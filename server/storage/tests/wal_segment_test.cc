/**
 * @file server/storage/tests/wal_segment_test.cc
 * What a crash can leave of the log's segments: a crash stands in as a copy of
 * the directory taken while a Debug Sync point holds the thread, and recovery
 * runs on the copy.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <map>
#include <msgpack.hpp>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/read.h"

#include "db_helper.h"
#include "sync_point.h"
#include "wal/crc32c.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/logger.h"
#include "wal/wal.h"

namespace {

namespace fs = std::filesystem;
using helios::storage::EpochNumber;
using helios::storage::wal::Crc32c;
using helios::storage::wal::EpochScanCheckpoint;
using helios::storage::wal::Logger;
using helios::storage::wal::LogRecord;
using helios::storage::wal::LogRecords;
using helios::storage::wal::PackedLogRecord;
using helios::storage::wal::PackedLogRecords;
using helios::storage::wal::Wal;
using helios::storage::wal::WalIo;
using helios::storage::wal::WalPosition;
using helios::storage::wal::WalScanResult;

constexpr auto kTestTimeout = std::chrono::seconds(10);
constexpr uint64_t kBlock = 4096;
constexpr uint64_t kSegment = 16 * kBlock;
constexpr uint16_t kFrameType = 1;
constexpr const char *kTable = "wal_segment_test";

// Writes enough release bytes on scope exit for every sync point that waits
// on `fd` afterwards.
struct ReleaseAll {
  int fd;
  ~ReleaseAll() {
    const std::string many(4096, 'r');
    [[maybe_unused]] const ssize_t rc = ::write(fd, many.data(), many.size());
  }
};

PackedLogRecord MakeRecord(EpochNumber epoch, const std::string &key) {
  LogRecord record;
  record.epoch = epoch;
  LogRecord::Write write;
  write.key = key;
  write.table_name = "t";
  record.writes.emplace_back(std::move(write));
  PackedLogRecord packed{epoch, {}};
  msgpack::pack(packed, record);
  return packed;
}

// A scan's result and what it replayed.
struct Recovered {
  WalScanResult scan;
  std::vector<EpochNumber> epochs;
  std::vector<std::string> keys;
};

Recovered Recover(Wal &wal, WalPosition start = {}) {
  Recovered out;
  out.scan = wal.Scan(
      0,
      [&out](const LogRecords &batch) {
        for (const auto &record : batch) {
          out.epochs.push_back(record.epoch);
          out.keys.push_back(record.writes.at(0).key);
        }
      },
      start);
  return out;
}

class WalSegmentTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { keep_sync_facility_armed(); }

  void SetUp() override {
    std::string pattern =
        (fs::temp_directory_path() / "helios_segment_XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
  }

  void TearDown() override {
    std::error_code ec;
    fs::remove_all(root_, ec);
  }

  std::string Dir(const std::string &name) const { return root_ + "/" + name; }

  static std::string SegmentPath(const std::string &dir, uint64_t number) {
    return dir + "/" + Wal::SegmentFileName(number);
  }

  // Appends one acknowledged group holding one epoch.
  static void Append(Wal &wal, EpochNumber epoch,
                     std::vector<EpochNumber> &acknowledged) {
    std::map<EpochNumber, PackedLogRecords> buckets;
    buckets[epoch] = {MakeRecord(epoch, "k" + std::to_string(epoch))};
    const auto result = wal.AppendGroup(buckets, epoch);
    ASSERT_TRUE(result.ok) << "errno " << result.error_number;
    acknowledged.push_back(epoch);
  }

  // Appends until one more single-block group would leave the active segment.
  static void FillSegment(Wal &wal, EpochNumber &epoch,
                          std::vector<EpochNumber> &acknowledged) {
    while (wal.write_offset() + 2 * kBlock <= kSegment) {
      Append(wal, ++epoch, acknowledged);
    }
  }

  static std::vector<uint8_t> ReadFile(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>((std::istreambuf_iterator<char>(in)),
                                std::istreambuf_iterator<char>());
  }

  static std::map<std::string, std::vector<uint8_t>> ReadSegments(
      const std::string &dir) {
    std::map<std::string, std::vector<uint8_t>> files;
    for (const auto &entry : fs::directory_iterator(dir)) {
      files[entry.path().filename().string()] = ReadFile(entry.path().string());
    }
    return files;
  }

  // Zeroes `[offset, end of the segment)`, which is what an unsynced write
  // losing all of its blocks looks like.
  static void LoseFrom(const std::string &dir, uint64_t number,
                       uint64_t offset) {
    const int fd = ::open(SegmentPath(dir, number).c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const std::vector<uint8_t> zeroes(kSegment - offset, 0);
    ASSERT_EQ(
        ::pwrite(fd, zeroes.data(), zeroes.size(), static_cast<off_t>(offset)),
        static_cast<ssize_t>(zeroes.size()));
    ::close(fd);
  }

  static void WriteAt(const std::string &path,
                      const std::vector<uint8_t> &bytes, uint64_t offset) {
    const int fd = ::open(path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(
        ::pwrite(fd, bytes.data(), bytes.size(), static_cast<off_t>(offset)),
        static_cast<ssize_t>(bytes.size()));
    ::close(fd);
  }

  // A valid frame record of `epoch`, naming the given segment and offset.
  static std::vector<uint8_t> MakeFrameRecord(uint64_t segment, uint64_t offset,
                                              EpochNumber epoch,
                                              const std::string &key) {
    msgpack::sbuffer payload;
    LogRecord record;
    record.epoch = epoch;
    LogRecord::Write write;
    write.key = key;
    write.table_name = "t";
    record.writes.emplace_back(std::move(write));
    msgpack::pack(payload, LogRecords{record});

    std::vector<uint8_t> out(Wal::kHeaderSize + payload.size());
    uint8_t *p = out.data();
    const uint32_t magic = Wal::kMagic;
    const auto length = static_cast<uint32_t>(payload.size());
    std::memcpy(p, &magic, 4);
    std::memcpy(p + 4, &kFrameType, 2);
    std::memcpy(p + 6, &segment, 8);
    std::memcpy(p + 14, &offset, 8);
    std::memcpy(p + 22, &length, 4);
    std::memcpy(p + 26, &epoch, 4);
    std::memcpy(p + Wal::kHeaderSize, payload.data(), payload.size());
    Crc32c crc;
    crc.Update(p, Wal::kHeaderSize - 4);
    crc.Update(payload.data(), payload.size());
    const uint32_t checksum = crc.Finish();
    std::memcpy(p + Wal::kHeaderSize - 4, &checksum, 4);
    return out;
  }

  std::string root_;
  ArmedSyncPoints points_;
};

// The rotation's steps, each with the writes it has not synced either kept or
// lost. Whatever the state, the log recovers what was acknowledged, resumes,
// and a further crash finds the resumed log whole.
TEST_F(WalSegmentTest, ACrashDuringRotationRecoversTheAcknowledgedPrefix) {
  struct Step {
    const char *point;
    int arrival;          // which arrival at the point is the crash
    uint64_t lost_from;   // 0: nothing lost; else the segment losing its tail
    bool in_flight_kept;  // the frame being written survives
  };
  constexpr const char *kBeforeSync = "HELIOS_DEBUG_SYNC_WAL_BEFORE_FDATASYNC";
  const Step steps[] = {
      {"HELIOS_DEBUG_SYNC_WAL_ROTATE_BEFORE_END_RECORD", 1, 0, false},
      {kBeforeSync, 1, 0, false},  // end record written, then kept
      {kBeforeSync, 1, 1, false},  // end record written, then lost
      {"HELIOS_DEBUG_SYNC_WAL_ROTATE_AFTER_END_SYNC", 1, 0, false},
      {kBeforeSync, 2, 0, true},   // first frame in the successor, kept
      {kBeforeSync, 2, 2, false},  // first frame in the successor, lost
  };

  int round = 0;
  for (const Step &step : steps) {
    SCOPED_TRACE(std::string(step.point) + " arrival " +
                 std::to_string(step.arrival) + " lost " +
                 std::to_string(step.lost_from));
    const std::string dir = Dir("live" + std::to_string(round));
    const std::string copy = Dir("crash" + std::to_string(round));
    ++round;

    Pipe arrived;
    Pipe release;
    std::vector<EpochNumber> acknowledged;
    EpochNumber epoch = 0;
    uint64_t lost_offset = 0;
    {
      Wal wal(dir, WalIo::Posix(), kSegment);
      ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
      FillSegment(wal, epoch, acknowledged);
      lost_offset = step.lost_from == 1 ? wal.write_offset() : kBlock;

      points_.arm(step.point,
                  "arrive_and_wait:" + std::to_string(arrived.write_fd()) +
                      ":" + std::to_string(release.read_fd()));
      std::map<EpochNumber, PackedLogRecords> buckets;
      const EpochNumber in_flight = ++epoch;
      buckets[in_flight] = {MakeRecord(in_flight, "in_flight")};
      auto append = std::async(std::launch::async, [&] {
        return wal.AppendGroup(buckets, in_flight);
      });
      ReleaseOnExit release_on_exit{release.write_fd()};

      char byte = 0;
      for (int i = 1; i < step.arrival; ++i) {
        ASSERT_TRUE(wait_readable(arrived.read_fd(), kTestTimeout));
        ASSERT_EQ(::read(arrived.read_fd(), &byte, 1), 1);
        ASSERT_EQ(::write(release.write_fd(), "r", 1), 1);
      }
      ASSERT_TRUE(wait_readable(arrived.read_fd(), kTestTimeout));
      ASSERT_EQ(::read(arrived.read_fd(), &byte, 1), 1);
      fs::copy(dir, copy, fs::copy_options::recursive);

      // Later arrivals of this group pass unheld.
      ::unsetenv(step.point);
      ASSERT_EQ(::write(release.write_fd(), "r", 1), 1);
      ASSERT_EQ(append.wait_for(kTestTimeout), std::future_status::ready);
      ASSERT_TRUE(append.get().ok);
    }
    if (step.lost_from != 0) LoseFrom(copy, step.lost_from, lost_offset);

    std::vector<EpochNumber> expected = acknowledged;
    if (step.in_flight_kept) expected.push_back(epoch);
    {
      Wal wal(copy, WalIo::Posix(), kSegment);
      const auto recovered = Recover(wal);
      ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk)
          << recovered.scan.detail;
      EXPECT_EQ(recovered.epochs, expected);
      Append(wal, epoch + 1, expected);
    }
    Wal wal(copy, WalIo::Posix(), kSegment);
    const auto again = Recover(wal);
    ASSERT_EQ(again.scan.status, WalScanResult::Status::kOk);
    EXPECT_EQ(again.epochs, expected);
  }
}

// Recycling's steps. The file being recycled is covered, so a crash at any step
// leaves everything from the redo start.
TEST_F(WalSegmentTest, ACrashDuringRecyclingRecoversTheAcknowledgedPrefix) {
  enum class Undo { kNone, kFinalRename, kTemporaryRename };
  struct Step {
    const char *point;
    Undo undo;
  };
  const Step steps[] = {
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_BEFORE_TEMP_RENAME", Undo::kNone},
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_AFTER_TEMP_RENAME", Undo::kNone},
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_AFTER_TEMP_RENAME",
       Undo::kTemporaryRename},
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_AFTER_HEADER_WRITE", Undo::kNone},
      // The new header is durable in a file whose rename did not last.
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_BEFORE_RENAME", Undo::kTemporaryRename},
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_BEFORE_DIRECTORY_SYNC", Undo::kNone},
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_BEFORE_DIRECTORY_SYNC",
       Undo::kFinalRename},
      {"HELIOS_DEBUG_SYNC_WAL_PREPARE_AFTER_DIRECTORY_SYNC", Undo::kNone},
  };

  int round = 0;
  for (const Step &step : steps) {
    SCOPED_TRACE(std::string(step.point) + " undo " +
                 std::to_string(static_cast<int>(step.undo)));
    const std::string dir = Dir("live" + std::to_string(round));
    const std::string copy = Dir("crash" + std::to_string(round));
    ++round;

    Pipe arrived;
    Pipe release;
    std::vector<EpochNumber> ignored;
    std::vector<EpochNumber> retained;
    EpochNumber epoch = 0;
    WalPosition start;
    uint64_t prepared = 0;
    {
      Wal wal(dir, WalIo::Posix(), kSegment);
      ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
      FillSegment(wal, epoch, ignored);
      Append(wal, ++epoch, ignored);  // leaves segment 1 for segment 2
      ASSERT_EQ(wal.active_segment(), 2u);
      start = wal.durable_end();
      Append(wal, ++epoch, retained);
      wal.MarkCovered(start.segment_number);
      prepared = wal.active_segment() + 1;

      points_.arm(step.point,
                  "arrive_and_wait:" + std::to_string(arrived.write_fd()) +
                      ":" + std::to_string(release.read_fd()));
      auto prepare = std::async(std::launch::async,
                                [&wal] { return wal.PrepareSegment(); });
      ReleaseOnExit release_on_exit{release.write_fd()};
      ASSERT_TRUE(wait_readable(arrived.read_fd(), kTestTimeout));
      char byte = 0;
      ASSERT_EQ(::read(arrived.read_fd(), &byte, 1), 1);
      fs::copy(dir, copy, fs::copy_options::recursive);
      ::unsetenv(step.point);
      ASSERT_EQ(::write(release.write_fd(), "r", 1), 1);
      ASSERT_EQ(prepare.wait_for(kTestTimeout), std::future_status::ready);
      ASSERT_TRUE(prepare.get());
    }

    std::string temporary = SegmentPath(copy, prepared);
    temporary.replace(temporary.size() - 4, 4, ".tmp");
    if (step.undo == Undo::kTemporaryRename) {
      fs::rename(temporary, SegmentPath(copy, 1));
    } else if (step.undo == Undo::kFinalRename) {
      fs::rename(SegmentPath(copy, prepared), temporary);
    }

    {
      Wal wal(copy, WalIo::Posix(), kSegment);
      const auto recovered = Recover(wal, start);
      ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk)
          << recovered.scan.detail;
      EXPECT_EQ(recovered.epochs, retained);
      EXPECT_FALSE(fs::exists(temporary));
      Append(wal, ++epoch, retained);
    }
    Wal wal(copy, WalIo::Posix(), kSegment);
    const auto again = Recover(wal, start);
    ASSERT_EQ(again.scan.status, WalScanResult::Status::kOk);
    EXPECT_EQ(again.epochs, retained);
  }
}

// Recycled bytes stay in place. The live tail ends where they begin, and none
// of them passes, even one crafted to carry the next epoch at the right offset.
TEST_F(WalSegmentTest, StaleFramesInARecycledSegmentAreRejected) {
  const std::string dir = Dir("live");
  std::vector<EpochNumber> ignored;
  std::vector<EpochNumber> retained;
  EpochNumber epoch = 0;
  WalPosition start;
  uint64_t recycled_number = 0;
  uint64_t live_tail = 0;
  {
    Wal wal(dir, WalIo::Posix(), kSegment);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    FillSegment(wal, epoch, ignored);
    Append(wal, ++epoch, ignored);
    start = wal.durable_end();
    wal.MarkCovered(start.segment_number);

    // Recycle segment 1 as the next segment and move into it.
    struct stat before {};
    ASSERT_EQ(::stat(SegmentPath(dir, 1).c_str(), &before), 0);
    ASSERT_TRUE(wal.PrepareSegment());
    recycled_number = wal.active_segment() + 1;
    struct stat after {};
    ASSERT_EQ(::stat(SegmentPath(dir, recycled_number).c_str(), &after), 0);
    ASSERT_EQ(before.st_ino, after.st_ino);
    EXPECT_FALSE(fs::exists(SegmentPath(dir, 1)));

    FillSegment(wal, epoch, retained);
    Append(wal, ++epoch, retained);
    ASSERT_EQ(wal.active_segment(), recycled_number);
    live_tail = wal.write_offset();
  }

  // A later block still holds an intact frame of the old segment.
  const auto bytes = ReadFile(SegmentPath(dir, recycled_number));
  uint32_t magic = 0;
  uint64_t number = 0;
  std::memcpy(&magic, bytes.data() + live_tail, 4);
  std::memcpy(&number, bytes.data() + live_tail + 6, 8);
  ASSERT_EQ(magic, Wal::kMagic);
  ASSERT_EQ(number, 1u);

  const auto recover_with = [&](const std::string &name,
                                const std::vector<uint8_t> &frame) {
    const std::string copy = Dir(name);
    fs::copy(dir, copy, fs::copy_options::recursive);
    if (!frame.empty()) {
      WriteAt(SegmentPath(copy, recycled_number), frame, live_tail);
    }
    Wal wal(copy, WalIo::Posix(), kSegment);
    return Recover(wal, start);
  };

  const auto plain = recover_with("plain", {});
  ASSERT_EQ(plain.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(plain.epochs, retained);

  const auto stale =
      recover_with("stale", MakeFrameRecord(1, live_tail, epoch + 1, "stale"));
  ASSERT_EQ(stale.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(stale.epochs, retained);

  // The same frame under the live number passes, so the number is what rejected
  // the one above.
  auto expected = retained;
  expected.push_back(epoch + 1);
  const auto live = recover_with(
      "live_number",
      MakeFrameRecord(recycled_number, live_tail, epoch + 1, "live"));
  ASSERT_EQ(live.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(live.epochs, expected);
}

// A torn group leaves its later frames intact. A new shorter group ending
// exactly where one of them begins must not make it reachable after a second
// crash, and its epoch would pass because epochs resume above what survived.
TEST_F(WalSegmentTest, TailRepairHidesTheTornGroupFromASecondCrash) {
  const std::string dir = Dir("live");

  // A frame exactly one block long, so nothing pads the group that holds it.
  std::string key(kBlock, 'x');
  const auto frame_bytes = [&key](EpochNumber epoch) {
    msgpack::sbuffer list;
    msgpack::packer<msgpack::sbuffer>(list).pack_array(1);
    return Wal::kHeaderSize + list.size() + MakeRecord(epoch, key).bytes.size();
  };
  key.resize(key.size() - (frame_bytes(5) - kBlock));
  ASSERT_EQ(frame_bytes(5), kBlock);

  std::vector<EpochNumber> acknowledged;
  uint64_t torn_offset = 0;
  {
    Wal wal(dir, WalIo::Posix(), kSegment);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    for (EpochNumber epoch = 1; epoch <= 4; ++epoch) {
      Append(wal, epoch, acknowledged);
    }
    torn_offset = wal.write_offset();
    std::map<EpochNumber, PackedLogRecords> buckets;
    buckets[5] = {MakeRecord(5, key)};
    buckets[6] = {MakeRecord(6, "stale6")};
    buckets[7] = {MakeRecord(7, "stale7")};
    ASSERT_TRUE(wal.AppendGroup(buckets, 7).ok);
  }
  // Tears epoch 5, leaving the frames after it whole.
  WriteAt(SegmentPath(dir, 1), {0xff, 0xff},
          torn_offset + Wal::kHeaderSize + 8);

  {
    Wal wal(dir, WalIo::Posix(), kSegment);
    const auto recovered = Recover(wal);
    ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
    EXPECT_EQ(recovered.epochs, acknowledged);
    EXPECT_TRUE(recovered.scan.tail_zeroed);
    ASSERT_EQ(wal.write_offset(), torn_offset);

    // The second crash follows this group.
    std::string new_key(key.size(), 'n');
    std::map<EpochNumber, PackedLogRecords> buckets;
    buckets[5] = {MakeRecord(5, new_key)};
    ASSERT_TRUE(wal.AppendGroup(buckets, 5).ok);
    EXPECT_EQ(wal.write_offset(), torn_offset + kBlock);
  }

  Wal wal(dir, WalIo::Posix(), kSegment);
  const auto recovered = Recover(wal);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  const std::vector<EpochNumber> expected{1, 2, 3, 4, 5};
  EXPECT_EQ(recovered.epochs, expected);
  EXPECT_EQ(recovered.keys.back()[0], 'n');
}

// Only a checkpoint frees segments, so a pool short of its spare target must
// not hold a due checkpoint back while a segment is ready.
TEST_F(WalSegmentTest, ADueCheckpointComesBeforeMoreSpares) {
  Wal wal(Dir("priority"), WalIo::Posix(), kSegment);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  EXPECT_EQ(wal.WaitForWork(), Wal::WorkerTask::kPrepare);
  wal.set_checkpoint_threshold(0);
  EXPECT_EQ(wal.WaitForWork(), Wal::WorkerTask::kCheckpoint);
  wal.StopWorker();
}

// The worker keeps two segments ready and prepares no more.
TEST_F(WalSegmentTest, TheWorkerKeepsTwoSegmentsReady) {
  const std::string dir = Dir("live");
  Wal wal(dir, WalIo::Posix(), kSegment);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  const uint64_t active = wal.active_segment();
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(wal.PrepareSegment());
  EXPECT_TRUE(fs::exists(SegmentPath(dir, active + 2)));
  EXPECT_FALSE(fs::exists(SegmentPath(dir, active + 3)));
}

// With no segment ready, the logger prepares one itself: it recycles a covered
// segment and leaves every retained byte alone.
TEST_F(WalSegmentTest, APoolMissRecyclesOnlyCoveredLog) {
  const std::string dir = Dir("live");
  std::vector<EpochNumber> ignored;
  std::vector<EpochNumber> retained;
  EpochNumber epoch = 0;
  Wal wal(dir, WalIo::Posix(), kSegment);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  FillSegment(wal, epoch, ignored);
  Append(wal, ++epoch, ignored);  // leaves segment 1 for segment 2
  ASSERT_EQ(wal.active_segment(), 2u);
  const WalPosition start = wal.durable_end();
  wal.MarkCovered(start.segment_number);
  FillSegment(wal, epoch, retained);

  const uint64_t written = wal.write_offset();
  auto before = ReadFile(SegmentPath(dir, 2));
  before.resize(written);
  struct stat covered {};
  ASSERT_EQ(::stat(SegmentPath(dir, 1).c_str(), &covered), 0);
  Append(wal, ++epoch, retained);
  ASSERT_EQ(wal.active_segment(), 3u);

  struct stat recycled {};
  ASSERT_EQ(::stat(SegmentPath(dir, 3).c_str(), &recycled), 0);
  EXPECT_EQ(recycled.st_ino, covered.st_ino);
  auto after = ReadFile(SegmentPath(dir, 2));
  after.resize(written);
  EXPECT_EQ(after, before);

  const auto recovered = Recover(wal, start);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, retained);
}

// A miss during a preparation waits for it and takes its segment, though the
// worker goes on to prepare another, and the logger prepares none itself.
TEST_F(WalSegmentTest, APoolMissTakesTheSegmentOfAPreparationUnderWay) {
  const std::string dir = Dir("live");
  std::vector<EpochNumber> acknowledged;
  EpochNumber epoch = 0;
  Pipe prepare_arrived;
  Pipe prepare_release;
  Pipe miss_arrived;
  Pipe miss_release;
  Wal wal(dir, WalIo::Posix(), kSegment);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  FillSegment(wal, epoch, acknowledged);
  Append(wal, ++epoch, acknowledged);
  ASSERT_EQ(wal.active_segment(), 2u);
  FillSegment(wal, epoch, acknowledged);

  // Both preparations of the worker park before their rename.
  points_.arm("HELIOS_DEBUG_SYNC_WAL_PREPARE_BEFORE_RENAME",
              "arrive_and_wait:" + std::to_string(prepare_arrived.write_fd()) +
                  ":" + std::to_string(prepare_release.read_fd()));
  std::map<EpochNumber, PackedLogRecords> buckets;
  const EpochNumber blocked = ++epoch;
  buckets[blocked] = {MakeRecord(blocked, "blocked")};
  std::future<bool> worker;
  std::future<helios::storage::wal::WalAppendResult> append;
  ReleaseOnExit release_first{prepare_release.write_fd()};
  ReleaseOnExit release_second{prepare_release.write_fd()};
  ReleaseOnExit release_miss{miss_release.write_fd()};
  worker = std::async(std::launch::async, [&] {
    const bool first = wal.PrepareSegment();
    return wal.PrepareSegment() && first;
  });
  char byte = 0;
  ASSERT_TRUE(wait_readable(prepare_arrived.read_fd(), kTestTimeout));
  ASSERT_EQ(::read(prepare_arrived.read_fd(), &byte, 1), 1);

  // The pool is empty and the first preparation is parked, so the append
  // misses while it is under way.
  points_.arm("HELIOS_DEBUG_SYNC_WAL_POOL_MISS",
              "arrive_and_wait:" + std::to_string(miss_arrived.write_fd()) +
                  ":" + std::to_string(miss_release.read_fd()));
  append = std::async(std::launch::async,
                      [&] { return wal.AppendGroup(buckets, blocked); });
  ASSERT_TRUE(wait_readable(miss_arrived.read_fd(), kTestTimeout));
  ::unsetenv("HELIOS_DEBUG_SYNC_WAL_POOL_MISS");
  ASSERT_EQ(::write(miss_release.write_fd(), "r", 1), 1);

  // The append completes while the second preparation is parked.
  ASSERT_EQ(::write(prepare_release.write_fd(), "r", 1), 1);
  ASSERT_TRUE(wait_readable(prepare_arrived.read_fd(), kTestTimeout));
  ASSERT_EQ(append.wait_for(kTestTimeout), std::future_status::ready);
  ASSERT_TRUE(append.get().ok);
  acknowledged.push_back(blocked);
  EXPECT_EQ(wal.active_segment(), 3u);

  ASSERT_EQ(::write(prepare_release.write_fd(), "r", 1), 1);
  ASSERT_EQ(worker.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_TRUE(worker.get());
  EXPECT_TRUE(fs::exists(SegmentPath(dir, 4)));
  EXPECT_FALSE(fs::exists(SegmentPath(dir, 5)));

  const auto recovered = Recover(wal);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, acknowledged);
}

// A failed preparation ahead of need is not retried ahead of need; a later miss
// may prepare the same segment, and that success lets the worker prepare ahead
// again. Only the failure of a miss's own preparation fails the append.
TEST_F(WalSegmentTest, OnlyAFailedPreparationOnAMissFailsTheAppend) {
  const std::string dir = Dir("live");
  std::atomic<bool> fail{false};
  WalIo io = WalIo::Posix();
  const auto real_pwrite = io.initialise_pwrite;
  io.initialise_pwrite = [&fail, real_pwrite](int fd, const void *data,
                                              size_t size,
                                              off_t offset) -> ssize_t {
    if (fail) {
      errno = ENOSPC;
      return -1;
    }
    return real_pwrite(fd, data, size, offset);
  };

  std::vector<EpochNumber> acknowledged;
  EpochNumber epoch = 0;
  Wal wal(dir, io, kSegment);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  FillSegment(wal, epoch, acknowledged);
  Append(wal, ++epoch, acknowledged);
  ASSERT_EQ(wal.active_segment(), 2u);

  fail = true;
  EXPECT_FALSE(wal.PrepareSegment());
  fail = false;
  EXPECT_FALSE(wal.PrepareSegment());
  FillSegment(wal, epoch, acknowledged);
  Append(wal, ++epoch, acknowledged);
  ASSERT_EQ(wal.active_segment(), 3u);
  EXPECT_EQ(wal.WaitForWork(), Wal::WorkerTask::kPrepare);
  EXPECT_TRUE(wal.PrepareSegment());
  const auto recovered = Recover(wal);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, acknowledged);

  // The rotation takes that spare, so the last append misses again.
  FillSegment(wal, epoch, acknowledged);
  Append(wal, ++epoch, acknowledged);
  ASSERT_EQ(wal.active_segment(), 4u);
  FillSegment(wal, epoch, acknowledged);
  fail = true;
  std::map<EpochNumber, PackedLogRecords> buckets;
  const EpochNumber failed = ++epoch;
  buckets[failed] = {MakeRecord(failed, "failed")};
  const auto result = wal.AppendGroup(buckets, failed);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, ENOSPC);
}

// A process crash leaves an unsynced group readable and the tail clean.
// Recovery counts that group as durable and syncs it through the log's seam,
// including the segment before the tail when the group was written there.
TEST_F(WalSegmentTest, RecoverySyncsWhatItAccepted) {
  const std::string dir = Dir("live");
  std::vector<EpochNumber> acknowledged;
  EpochNumber epoch = 0;
  std::set<std::string> synced;
  WalIo io = WalIo::Posix();
  io.fdatasync = [&synced](int fd) {
    synced.insert(fs::read_symlink("/proc/self/fd/" + std::to_string(fd))
                      .filename()
                      .string());
    return ::fdatasync(fd);
  };

  const auto reopen = [&] {
    synced.clear();
    Wal wal(dir, io, kSegment);
    const auto recovered = Recover(wal);
    EXPECT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
    EXPECT_FALSE(recovered.scan.tail_zeroed);
    EXPECT_EQ(recovered.epochs, acknowledged);
  };

  {
    Wal wal(dir, WalIo::Posix(), kSegment);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    Append(wal, ++epoch, acknowledged);
    ASSERT_EQ(wal.write_offset() % kBlock, 0u);
  }
  reopen();
  EXPECT_EQ(synced, (std::set<std::string>{Wal::SegmentFileName(1)}));

  {
    Wal wal(dir, WalIo::Posix(), kSegment);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    FillSegment(wal, epoch, acknowledged);
    Append(wal, ++epoch, acknowledged);
    ASSERT_EQ(wal.active_segment(), 2u);
  }
  reopen();
  EXPECT_EQ(synced, (std::set<std::string>{Wal::SegmentFileName(1),
                                           Wal::SegmentFileName(2)}));

  // A sync that fails leaves nothing to report as durable.
  WalIo failing = WalIo::Posix();
  failing.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };
  Wal wal(dir, failing, kSegment);
  const auto scan = wal.Scan();
  EXPECT_EQ(scan.status, WalScanResult::Status::kIoError);
  EXPECT_EQ(scan.error_number, EIO);
}

// A group spanning epochs goes as far as whole frames fit, syncs the old
// segment with its end record, and only then writes in the successor.
TEST_F(WalSegmentTest, AnEpochLargerThanTheRemainderRotatesWithoutSplitting) {
  const std::string dir = Dir("live");
  std::vector<std::pair<char, int>> calls;
  WalIo io = WalIo::Posix();
  io.pwritev = [&calls](int fd, const iovec *iov, int count, off_t offset) {
    calls.emplace_back('w', fd);
    return ::pwritev(fd, iov, count, offset);
  };
  io.fdatasync = [&calls](int fd) {
    calls.emplace_back('s', fd);
    return ::fdatasync(fd);
  };

  std::vector<EpochNumber> acknowledged;
  EpochNumber epoch = 0;
  Wal wal(dir, io, kSegment);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  for (; wal.write_offset() + 4 * kBlock <= kSegment;) {
    Append(wal, ++epoch, acknowledged);
  }
  const uint64_t remainder = kSegment - wal.write_offset();

  // The first frame fits; the second is larger than what is left after it.
  const EpochNumber small = ++epoch;
  const EpochNumber large = ++epoch;
  const std::string large_key(remainder + kBlock, 'l');
  std::map<EpochNumber, PackedLogRecords> buckets;
  buckets[small] = {MakeRecord(small, "small")};
  buckets[large] = {MakeRecord(large, large_key)};
  calls.clear();
  ASSERT_TRUE(wal.AppendGroup(buckets, large).ok);
  acknowledged.push_back(small);
  acknowledged.push_back(large);
  EXPECT_EQ(wal.active_segment(), 2u);

  ASSERT_EQ(calls.size(), 4u);
  EXPECT_EQ(calls[0].first, 'w');
  EXPECT_EQ(calls[1].first, 's');
  EXPECT_EQ(calls[1].second, calls[0].second);
  EXPECT_EQ(calls[2].first, 'w');
  EXPECT_NE(calls[2].second, calls[1].second);
  EXPECT_EQ(calls[3].first, 's');
  EXPECT_EQ(calls[3].second, calls[2].second);

  // The whole frame lies in the successor: the old segment holds none of its
  // key.
  const auto first = ReadFile(SegmentPath(dir, 1));
  EXPECT_EQ(std::string(first.begin(), first.end()).find("lllll"),
            std::string::npos);

  const auto recovered = Recover(wal);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, acknowledged);
  EXPECT_EQ(recovered.keys.back(), large_key);
}

// What recovery requires and does not find stops it, rather than letting it
// start from a log with a hole.
TEST_F(WalSegmentTest, MissingRecoveryMaterialFailsRecovery) {
  helios::storage::Config config;
  config.wal_segment_bytes = kSegment;
  config.work_dir = Dir("live");
  std::vector<EpochNumber> acknowledged;
  EpochNumber epoch = 0;
  {
    Wal wal(config.work_dir, WalIo::Posix(), kSegment);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    FillSegment(wal, epoch, acknowledged);
    Append(wal, ++epoch, acknowledged);
    ASSERT_EQ(wal.active_segment(), 2u);
    Append(wal, ++epoch, acknowledged);
  }

  // The successor an end record leads to is gone.
  {
    const std::string copy = Dir("no_successor");
    fs::copy(config.work_dir, copy, fs::copy_options::recursive);
    fs::remove(SegmentPath(copy, 2));
    auto damaged = config;
    damaged.work_dir = copy;
    Logger logger(damaged);
    EXPECT_EQ(logger.Recover().status, Logger::RecoveryStatus::kFailed);
  }
  // With no checkpoint the log must start at its first segment.
  {
    const std::string copy = Dir("no_first");
    fs::copy(config.work_dir, copy, fs::copy_options::recursive);
    fs::remove(SegmentPath(copy, 1));
    auto damaged = config;
    damaged.work_dir = copy;
    Logger logger(damaged);
    EXPECT_EQ(logger.Recover().status, Logger::RecoveryStatus::kFailed);
  }
  // A checkpoint that cannot be trusted has nothing to fall back on.
  {
    const std::string copy = Dir("bad_checkpoint");
    fs::copy(config.work_dir, copy, fs::copy_options::recursive);
    std::ofstream(copy + "/" + EpochScanCheckpoint::CheckpointFileName())
        << "not a checkpoint";
    auto damaged = config;
    damaged.work_dir = copy;
    Logger logger(damaged);
    EXPECT_EQ(logger.Recover().status, Logger::RecoveryStatus::kFailed);
  }
}

// A pwritev that checks what O_DIRECT requires of each group write.
WalIo DirectIo() {
  WalIo io = WalIo::Posix();
  io.pwritev = [](int fd, const iovec *iov, int count, off_t offset) {
    EXPECT_NE(::fcntl(fd, F_GETFL) & O_DIRECT, 0);
    EXPECT_EQ(count, 1);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(iov[0].iov_base) % kBlock, 0u);
    EXPECT_EQ(iov[0].iov_len % kBlock, 0u);
    EXPECT_EQ(static_cast<uint64_t>(offset) % kBlock, 0u);
    return ::pwritev(fd, iov, count, offset);
  };
  return io;
}

// Appends groups of two epochs whose sizes are not multiples of the block.
void AppendUnevenGroups(Wal &wal, int groups, EpochNumber &epoch,
                        std::vector<EpochNumber> &acknowledged,
                        std::vector<std::string> &keys) {
  for (int i = 0; i < groups; ++i) {
    std::map<EpochNumber, PackedLogRecords> buckets;
    for (int j = 0; j < 2; ++j) {
      ++epoch;
      const std::string key(epoch * 1237 % 9000 + 1, 'a' + epoch % 26);
      buckets[epoch] = {MakeRecord(epoch, key)};
      acknowledged.push_back(epoch);
      keys.push_back(key);
    }
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
  }
}

// Such groups, across rotations and a restart, all come back.
TEST_F(WalSegmentTest, DirectWritesRecoverGroupsOfAnySize) {
  const std::string dir = Dir("live");
  std::vector<EpochNumber> acknowledged;
  std::vector<std::string> keys;
  EpochNumber epoch = 0;
  {
    Wal wal(dir, DirectIo(), kSegment, true);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    AppendUnevenGroups(wal, 20, epoch, acknowledged, keys);
    EXPECT_GT(wal.active_segment(), 2u);
  }
  {
    Wal wal(dir, DirectIo(), kSegment, true);
    const auto recovered = Recover(wal);
    ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
    EXPECT_EQ(recovered.epochs, acknowledged);
    AppendUnevenGroups(wal, 3, epoch, acknowledged, keys);
  }
  Wal wal(dir, WalIo::Posix(), kSegment);
  const auto recovered = Recover(wal);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, acknowledged);
  EXPECT_EQ(recovered.keys, keys);
}

// A group that ends exactly on a block needs no padding, and the next group
// starts on the following block.
TEST_F(WalSegmentTest, DirectWritesRecoverAGroupEndingOnABlock) {
  const std::string dir = Dir("live");
  // The key that makes a one-record frame exactly one block.
  std::string exact;
  for (size_t size = 1; size < kBlock && exact.empty(); ++size) {
    const std::string key(size, 'b');
    if (Wal::kHeaderSize + 1 + MakeRecord(1, key).bytes.size() == kBlock) {
      exact = key;
    }
  }
  ASSERT_FALSE(exact.empty());

  {
    Wal wal(dir, DirectIo(), kSegment, true);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    const uint64_t before = wal.write_offset();
    std::map<EpochNumber, PackedLogRecords> buckets;
    buckets[1] = {MakeRecord(1, exact)};
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
    EXPECT_EQ(wal.write_offset(), before + kBlock);
    buckets.clear();
    buckets[2] = {MakeRecord(2, "next")};
    ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
  }
  Wal wal(dir, WalIo::Posix(), kSegment);
  const auto recovered = Recover(wal);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, (std::vector<EpochNumber>{1, 2}));
  EXPECT_EQ(recovered.keys, (std::vector<std::string>{exact, "next"}));
}

// Direct writes into a recycled segment leave its old frames behind the log's
// end, and recovery replays none of them.
TEST_F(WalSegmentTest, DirectWritesIntoARecycledSegmentReplayNoOldFrame) {
  const std::string dir = Dir("live");
  std::vector<EpochNumber> ignored;
  std::vector<EpochNumber> retained;
  EpochNumber epoch = 0;
  WalPosition start;
  uint64_t recycled_number = 0;
  uint64_t live_tail = 0;
  {
    Wal wal(dir, DirectIo(), kSegment, true);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    FillSegment(wal, epoch, ignored);
    Append(wal, ++epoch, ignored);
    start = wal.durable_end();
    wal.MarkCovered(start.segment_number);
    ASSERT_TRUE(wal.PrepareSegment());
    recycled_number = wal.active_segment() + 1;
    EXPECT_FALSE(fs::exists(SegmentPath(dir, 1)));

    FillSegment(wal, epoch, retained);
    Append(wal, ++epoch, retained);
    ASSERT_EQ(wal.active_segment(), recycled_number);
    live_tail = wal.write_offset();
  }

  // The block after the log's end still holds a frame of segment 1.
  const auto bytes = ReadFile(SegmentPath(dir, recycled_number));
  uint32_t magic = 0;
  uint64_t number = 0;
  std::memcpy(&magic, bytes.data() + live_tail, 4);
  std::memcpy(&number, bytes.data() + live_tail + 6, 8);
  ASSERT_EQ(magic, Wal::kMagic);
  ASSERT_EQ(number, 1u);

  Wal wal(dir, WalIo::Posix(), kSegment);
  const auto recovered = Recover(wal, start);
  ASSERT_EQ(recovered.scan.status, WalScanResult::Status::kOk);
  EXPECT_EQ(recovered.epochs, retained);
}

class WalRecyclingTest : public WalSegmentTest {
 protected:
  helios::storage::Config MakeConfig(const std::string &dir) const {
    helios::storage::Config config;
    config.epoch_duration_ms = 10;
    config.work_dir = dir;
    config.wal_segment_bytes = kRecyclingSegment;
    // A checkpoint is due after each segment of log.
    config.wal_budget_bytes = 2 * kRecyclingSegment;
    return config;
  }

  // Updates `key` and remembers what an acknowledged commit left there.
  static void Update(helios::storage::Database &db, const std::string &key,
                     std::map<std::string, std::string> &acknowledged,
                     helios::storage::CommitDurability durability =
                         helios::storage::CommitDurability::kSync) {
    // Nearly a block, so that eight checkpoints of these rows outlast the log
    // the tests write between two captures.
    const std::string value =
        key + "#" + std::to_string(++version_) + std::string(3000, 'x');
    std::string reason;
    ASSERT_TRUE(TestHelper::CommitRows(db, {},
                                       {{kTable, key, TestHelper::Row(value)}},
                                       {}, {}, reason, durability))
        << reason;
    acknowledged[key] = value;
  }

  // Asserts the database at `dir` holds what the commits acknowledged.
  void ExpectRecovers(const std::string &dir,
                      const std::map<std::string, std::string> &acknowledged) {
    helios::storage::Database db(MakeConfig(dir));
    for (const auto &[key, value] : acknowledged) {
      const auto row = db.Read(kTable, key);
      db.ReleaseThreadEpoch();
      ASSERT_TRUE(row.found) << key;
      EXPECT_EQ(TestHelper::RowPayload(row.value), value) << key;
    }
  }

  static constexpr uint64_t kRecyclingSegment = 64 * kBlock;
  inline static uint64_t version_ = 0;
};

// The capture holds its last row while updates land in `(S, T]`: the checkpoint
// keeps the older versions of the other rows, and only the log has the newer
// ones. A crash right after publication and recycling recovers them all.
TEST_F(WalRecyclingTest, RecyclingKeepsTheLogAfterTheStartEpoch) {
  const std::string dir = Dir("live");
  const std::string copy = Dir("crash");
  constexpr int kRows = 40;
  std::map<std::string, std::string> acknowledged;
  std::vector<std::string> keys;
  for (int i = 0; i < kRows; ++i)
    keys.push_back("key" + std::to_string(100 + i));

  Pipe scan_arrived;
  Pipe scan_release;
  Pipe prepare_arrived;
  Pipe prepare_release;
  std::future<void> releaser;
  // The releaser's read ends when this closes, before its future's destructor
  // would wait for it.
  struct CloseWriteOnExit {
    Pipe &pipe;
    ~CloseWriteOnExit() { pipe.close_write(); }
  } close_on_exit{scan_arrived};
  {
    helios::storage::Database db(MakeConfig(dir));
    ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

    // Past one segment, so the capture below has a segment before its redo
    // start. That volume also triggers a checkpoint of the worker's own, which
    // has to end first for the capture to be this thread's.
    for (uint64_t written = 0; written < 3 * kRecyclingSegment / 2;
         written += kBlock) {
      Update(db, keys[written / kBlock % kRows], acknowledged);
    }
    const std::string checkpoint =
        dir + "/" + EpochScanCheckpoint::CheckpointFileName();
    for (int i = 0; i < 1000 && !fs::exists(checkpoint); ++i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(fs::exists(checkpoint));

    points_.arm("HELIOS_DEBUG_SYNC_CHECKPOINT_BEFORE_ROW_COPY",
                "arrive_and_wait:" + std::to_string(scan_arrived.write_fd()) +
                    ":" + std::to_string(scan_release.read_fd()));
    auto capture = std::async(std::launch::async, [&db] {
      for (;;) {
        if (db.WriteCheckpoint()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });
    ReleaseOnExit release_scan{scan_release.write_fd()};

    // Let the scan copy every row but the last.
    char byte = 0;
    for (int i = 0; i < kRows; ++i) {
      ASSERT_TRUE(wait_readable(scan_arrived.read_fd(), kTestTimeout));
      ASSERT_EQ(::read(scan_arrived.read_fd(), &byte, 1), 1);
      if (i + 1 < kRows) {
        ASSERT_EQ(::write(scan_release.write_fd(), "r", 1), 1);
      }
    }

    // Updates inside `(S, T]`, spanning segments.
    for (uint64_t written = 0; written < 2 * kRecyclingSegment;
         written += kBlock) {
      Update(db, keys[written / kBlock % (kRows - 1)], acknowledged);
    }

    // From here every row copy passes, also those of later captures.
    releaser = std::async(std::launch::async, [&] {
      char arrival = 0;
      while (::read(scan_arrived.read_fd(), &arrival, 1) == 1) {
        ASSERT_EQ(::write(scan_release.write_fd(), "r", 1), 1);
      }
    });
    ASSERT_EQ(::write(scan_release.write_fd(), "r", 1), 1);
    ASSERT_EQ(capture.wait_for(kTestTimeout), std::future_status::ready);
    ASSERT_TRUE(capture.get());

    // Keep committing until the worker has recycled the first segment.
    points_.arm(
        "HELIOS_DEBUG_SYNC_WAL_PREPARE_AFTER_DIRECTORY_SYNC",
        "arrive_and_wait:" + std::to_string(prepare_arrived.write_fd()) + ":" +
            std::to_string(prepare_release.read_fd()));
    ReleaseOnExit release_prepare{prepare_release.write_fd()};
    bool recycled = false;
    for (int i = 0; i < 100000 && !recycled; ++i) {
      Update(db, keys[i % kRows], acknowledged);
      if (!wait_readable(prepare_arrived.read_fd(),
                         std::chrono::milliseconds(0))) {
        continue;
      }
      ASSERT_EQ(::read(prepare_arrived.read_fd(), &byte, 1), 1);
      recycled = !fs::exists(SegmentPath(dir, 1));
      if (recycled) {
        fs::copy(dir, copy, fs::copy_options::recursive);
        ::unsetenv("HELIOS_DEBUG_SYNC_WAL_PREPARE_AFTER_DIRECTORY_SYNC");
      }
      ASSERT_EQ(::write(prepare_release.write_fd(), "r", 1), 1);
    }
    ASSERT_TRUE(recycled);
  }
  scan_arrived.close_write();
  ASSERT_EQ(releaser.wait_for(kTestTimeout), std::future_status::ready);

  EXPECT_FALSE(fs::exists(SegmentPath(copy, 1)));
  ExpectRecovers(copy, acknowledged);
}

// A published checkpoint moves the trigger: the worker captures once the log
// appended after it reaches half the budget, and not before.
TEST_F(WalRecyclingTest, ACheckpointIsDueAtHalfTheBudgetOfNewLog) {
  const std::string dir = Dir("live");
  std::map<std::string, std::string> acknowledged;
  Pipe capture_arrived;
  Pipe capture_release;
  // Half the budget is half a segment, and each update takes one block of log.
  constexpr uint64_t kHalfBlocks = kRecyclingSegment / kBlock / 2;
  auto config = MakeConfig(dir);
  config.wal_budget_bytes = kRecyclingSegment;
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
  for (uint64_t i = 0; i < kHalfBlocks / 2; ++i) Update(db, "a", acknowledged);
  ASSERT_TRUE(db.WriteCheckpoint());

  // A capture parks on row "a", which comes first, so the updates of "k" keep
  // completing.
  points_.arm("HELIOS_DEBUG_SYNC_CHECKPOINT_BEFORE_ROW_COPY",
              "arrive_and_wait:" + std::to_string(capture_arrived.write_fd()) +
                  ":" + std::to_string(capture_release.read_fd()));
  // Releases every row copy before the database closes, also on a failure.
  ReleaseAll release_all{capture_release.write_fd()};
  for (uint64_t i = 0; i < kHalfBlocks - 8; ++i) {
    Update(db, "k", acknowledged);
  }
  EXPECT_FALSE(wait_readable(capture_arrived.read_fd(),
                             std::chrono::milliseconds(200)));
  for (uint64_t i = 0; i < 16; ++i) Update(db, "k", acknowledged);
  EXPECT_TRUE(wait_readable(capture_arrived.read_fd(), kTestTimeout));
}

// The worker is held inside its capture for good while the log needs more
// segments than are ready: the logger prepares one itself, and Sync commits
// keep completing.
TEST_F(WalRecyclingTest, CommitsCompleteWhileTheCaptureIsHeld) {
  const std::string dir = Dir("live");
  std::map<std::string, std::string> acknowledged;
  Pipe wait_arrived;
  Pipe wait_release;
  {
    helios::storage::Database db(MakeConfig(dir));
    ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
    std::future<void> updates;
    points_.arm("HELIOS_DEBUG_SYNC_CHECKPOINT_BEFORE_ROW_COPY",
                "arrive_and_wait:" + std::to_string(wait_arrived.write_fd()) +
                    ":" + std::to_string(wait_release.read_fd()));
    // Releases every row copy before `updates` is joined and the database
    // closes, also on a failure.
    ReleaseAll release_all{wait_release.write_fd()};

    // Writes until the worker's own checkpoint is under way.
    const std::string value(1024, 'v');
    bool held = false;
    std::string reason;
    for (int i = 0; i < 1000000 && !held; ++i) {
      ASSERT_TRUE(TestHelper::CommitRows(
          db, {},
          {{kTable, "k" + std::to_string(i % 50), TestHelper::Row(value)}}, {},
          {}, reason, helios::storage::CommitDurability::kAsync));
      // Paced so that no epoch outgrows these small segments.
      std::this_thread::sleep_for(std::chrono::microseconds(200));
      held =
          wait_readable(wait_arrived.read_fd(), std::chrono::milliseconds(0));
    }
    ASSERT_TRUE(held);

    // The held worker prepares nothing, so only the logger can add a segment
    // past the highest one on disk now.
    uint64_t highest = 0;
    for (const auto &entry : fs::directory_iterator(dir)) {
      if (entry.path().extension() != ".log") continue;
      highest = std::max<uint64_t>(
          highest, std::stoull(entry.path().stem().string().substr(4)));
    }
    const std::string prepared = SegmentPath(dir, highest + 1);
    updates = std::async(std::launch::async, [&] {
      for (uint64_t written = 0;
           written < 16 * kRecyclingSegment && !fs::exists(prepared);
           written += kBlock) {
        Update(db, "u" + std::to_string(written / kBlock % 50), acknowledged);
      }
      db.ReleaseThreadEpoch();
    });
    ASSERT_EQ(updates.wait_for(kTestTimeout), std::future_status::ready);
    updates.get();
    EXPECT_TRUE(fs::exists(prepared));
  }
  ExpectRecovers(dir, acknowledged);
}

// Enough log for many checkpoints: the worker takes them and recycles, so the
// directory stays small while segment numbers keep climbing.
TEST_F(WalRecyclingTest,
       LogGrowthTriggersCheckpointsAndKeepsTheDirectoryBounded) {
  const std::string dir = Dir("live");
  std::map<std::string, std::string> acknowledged;
  uint64_t highest = 0;
  {
    helios::storage::Database db(MakeConfig(dir));
    ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
    size_t most_segments = 0;
    for (uint64_t written = 0; written < 12 * kRecyclingSegment;
         written += kBlock) {
      Update(db, "k" + std::to_string(written / kBlock % 20), acknowledged);
      size_t segments = 0;
      for (const auto &entry : fs::directory_iterator(dir)) {
        if (entry.path().extension() != ".log") continue;
        ++segments;
        highest = std::max<uint64_t>(
            highest, std::stoull(entry.path().stem().string().substr(4)));
      }
      most_segments = std::max(most_segments, segments);
    }
    EXPECT_TRUE(
        fs::exists(dir + "/" + EpochScanCheckpoint::CheckpointFileName()));
    EXPECT_GE(highest, 10u);
    EXPECT_LE(most_segments, 8u);
  }
  ExpectRecovers(dir, acknowledged);
}

}  // namespace
