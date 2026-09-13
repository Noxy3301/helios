/**
 * @file server/storage/tests/wal_frame_test.cc
 * The log file itself: frame layout, group writes, and what the startup
 * scan makes of a corrupted or truncated tail.
 */

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <msgpack.hpp>
#include <string>

#include "wal/crc32c.h"
#include "wal/wal.h"

namespace {

// Offsets inside a frame header (magic, version, flags, payload length,
// epoch, checksum).
constexpr off_t kFlagsOffset = 6;

using helios::storage::EpochNumber;
using helios::storage::wal::Crc32c;
using helios::storage::wal::LogRecord;
using helios::storage::wal::LogRecords;
using helios::storage::wal::Wal;
using helios::storage::wal::WalScanResult;

LogRecords MakeRecords(EpochNumber epoch, const std::string &key) {
  LogRecord record;
  record.epoch = epoch;
  LogRecord::Write write;
  write.key = key;
  write.buffer = "value-of-" + key;
  write.table_name = "t";
  record.writes.emplace_back(std::move(write));
  return LogRecords{std::move(record)};
}

class WalFrameTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "helios_wal_XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    ASSERT_NE(::mkdtemp(buffer.data()), nullptr);
    root_ = buffer.data();
    work_dir_ = root_ + "/logs";
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  std::string WalPath() const { return work_dir_ + "/wal.log"; }

  off_t FileSize() const {
    struct stat file_stat {};
    EXPECT_EQ(::stat(WalPath().c_str(), &file_stat), 0);
    return file_stat.st_size;
  }

  /**
   * @brief Returns where the log ends, which is not where the file ends.
   *
   * @details Capacity beyond the last frame is written out with zeroes in
   * advance.
   */
  off_t EndOfLog() {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    EXPECT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    return wal.write_offset();
  }

  // Flips a bit at an absolute offset, standing in for a torn or damaged
  // write.
  void FlipByteAt(off_t offset) {
    const int fd = ::open(WalPath().c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    uint8_t byte = 0;
    const bool read_ok = ::pread(fd, &byte, 1, offset) == 1;
    byte = static_cast<uint8_t>(byte ^ 0xffu);
    const bool write_ok = read_ok && ::pwrite(fd, &byte, 1, offset) == 1;
    ::close(fd);
    ASSERT_TRUE(read_ok);
    ASSERT_TRUE(write_ok);
  }

  // Asserts every byte in [from, to) reads back zero: repair is expected to
  // have overwritten this range, not merely to have stopped trusting it.
  void AssertRangeIsZero(off_t from, off_t to) {
    const int fd = ::open(WalPath().c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> chunk(1 << 16);
    for (off_t at = from; at < to;) {
      const size_t want =
          static_cast<size_t>(std::min<off_t>(chunk.size(), to - at));
      ASSERT_EQ(::pread(fd, chunk.data(), want, at),
                static_cast<ssize_t>(want));
      for (size_t i = 0; i < want; ++i) {
        ASSERT_EQ(chunk[i], 0) << "offset " << (at + static_cast<off_t>(i));
      }
      at += static_cast<off_t>(want);
    }
    ASSERT_EQ(::close(fd), 0);
  }

  /** Offset one past the frame at `frame_offset`, taken from its own
   * length. */
  off_t FrameEnd(off_t frame_offset) {
    uint8_t header[Wal::kHeaderSize];
    const int fd = ::open(WalPath().c_str(), O_RDONLY);
    EXPECT_GE(fd, 0);
    EXPECT_EQ(::pread(fd, header, sizeof(header), frame_offset),
              static_cast<ssize_t>(sizeof(header)));
    EXPECT_EQ(::close(fd), 0);
    const uint32_t length = static_cast<uint32_t>(header[8]) |
                            (static_cast<uint32_t>(header[9]) << 8) |
                            (static_cast<uint32_t>(header[10]) << 16) |
                            (static_cast<uint32_t>(header[11]) << 24);
    return frame_offset + static_cast<off_t>(Wal::kHeaderSize) + length;
  }

  void AppendEpochs(const std::vector<EpochNumber> &epochs) {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    std::map<EpochNumber, LogRecords> buckets;
    for (const auto epoch : epochs) {
      buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    }
    const auto result = wal.AppendGroup(buckets, epochs.back());
    ASSERT_TRUE(result.ok) << "errno " << result.error_number;
    ASSERT_EQ(wal.extension_count(), 0u);
  }

  // Writes raw bytes at an absolute offset, standing in for the on-disk
  // state a torn or corrupted group write leaves behind. A settled-size log
  // must be addressed by offset rather than by appending: the file's own end
  // is capacity, not the log's end.
  void WriteRawBytesAt(off_t offset, const std::vector<uint8_t> &bytes) {
    const int fd = ::open(WalPath().c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const bool write_ok = ::pwrite(fd, bytes.data(), bytes.size(), offset) ==
                          static_cast<ssize_t>(bytes.size());
    ::close(fd);
    ASSERT_TRUE(write_ok);
  }

  static void PutLe16(std::vector<uint8_t> &out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xffu));
  }

  static void PutLe32(std::vector<uint8_t> &out, uint32_t value) {
    out.push_back(static_cast<uint8_t>(value & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 16) & 0xffu));
    out.push_back(static_cast<uint8_t>((value >> 24) & 0xffu));
  }

  // A syntactically complete header whose payload the file does not fully
  // carry: the remaining declared bytes read back as whatever the log's
  // reserved capacity already held.
  std::vector<uint8_t> MakeHeaderClaimingPayload(EpochNumber epoch,
                                                 uint32_t payload_len) {
    std::vector<uint8_t> header;
    PutLe32(header, Wal::kMagic);
    PutLe16(header, Wal::kVersion);
    PutLe16(header, Wal::kFlags);
    PutLe32(header, payload_len);
    PutLe32(header, epoch);
    PutLe32(header, 0);  // A placeholder; the real payload was never written.
    return header;
  }

  // A complete frame with a correct checksum over an arbitrary payload.
  std::vector<uint8_t> MakeFrame(EpochNumber epoch,
                                 const std::vector<uint8_t> &payload) {
    std::vector<uint8_t> frame;
    PutLe32(frame, Wal::kMagic);
    PutLe16(frame, Wal::kVersion);
    PutLe16(frame, Wal::kFlags);
    PutLe32(frame, static_cast<uint32_t>(payload.size()));
    PutLe32(frame, epoch);
    Crc32c crc;
    crc.Update(frame.data(), 16);
    crc.Update(payload.data(), payload.size());
    PutLe32(frame, crc.Finish());
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
  }

  static std::vector<uint8_t> PackRecords(const LogRecords &records) {
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, records);
    const auto *data = reinterpret_cast<const uint8_t *>(buffer.data());
    return std::vector<uint8_t>(data, data + buffer.size());
  }

  // Enough for every group these tests write, and small enough that writing
  // it out costs nothing worth measuring.
  static constexpr uint64_t kCapacity = 1ull << 20;

  std::string root_;
  std::string work_dir_;
};

TEST_F(WalFrameTest, ScanOfAFreshLogHasNoLastEpoch) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  EXPECT_EQ(result.status, WalScanResult::Status::kOk);
  EXPECT_EQ(result.last_epoch, 0u);
  EXPECT_TRUE(result.records.empty());
  EXPECT_FALSE(result.tail_zeroed);
}

TEST_F(WalFrameTest, ScanReturnsTheLastCompleteEpoch) {
  AppendEpochs({1, 3});
  // Frames may share an epoch, and empty epochs leave gaps in the log.
  WriteRawBytesAt(EndOfLog(), MakeFrame(3, PackRecords(MakeRecords(3, "again"))));

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk);
  EXPECT_EQ(result.last_epoch, 3u);
  EXPECT_FALSE(result.tail_zeroed);
  ASSERT_EQ(result.records.size(), 3u);
  EXPECT_EQ(result.records[0].epoch, 1u);
  EXPECT_EQ(result.records[1].epoch, 3u);
  EXPECT_EQ(result.records[2].epoch, 3u);
  EXPECT_EQ(result.records[0].writes.at(0).key, "k1");
  EXPECT_EQ(result.records[2].writes.at(0).key, "again");
}

TEST_F(WalFrameTest, AGroupIsOneWriteAndOneSync) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  int write_calls = 0;
  int sync_calls = 0;
  io.pwrite = [&write_calls](int fd, const void *data, size_t size,
                             off_t offset) {
    ++write_calls;
    return ::pwrite(fd, data, size, offset);
  };
  io.fdatasync = [&sync_calls](int fd) {
    ++sync_calls;
    return ::fdatasync(fd);
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  buckets[2] = MakeRecords(2, "k2");
  buckets[3] = MakeRecords(3, "k3");
  ASSERT_TRUE(wal.AppendGroup(buckets, 3).ok);
  EXPECT_EQ(write_calls, 1);
  EXPECT_EQ(sync_calls, 1);
}

TEST_F(WalFrameTest, GroupSkipsBucketsAboveTheTarget) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  buckets[2] = MakeRecords(2, "k2");
  buckets[3] = MakeRecords(3, "k3");
  ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);

  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk);
  EXPECT_EQ(result.last_epoch, 2u);
  EXPECT_EQ(result.records.size(), 2u);
}

TEST_F(WalFrameTest, TheFirstInvalidFrameEndsThePrefixAndAppendResumesThere) {
  AppendEpochs({1, 2, 3});
  const off_t damaged = FrameEnd(0);
  const off_t file_size = FileSize();

  // A complete third frame does not license reading past the broken second.
  FlipByteAt(damaged + static_cast<off_t>(Wal::kHeaderSize) + 1);
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan();
    ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
    EXPECT_TRUE(result.tail_zeroed);
    EXPECT_EQ(result.last_epoch, 1u);
    ASSERT_EQ(result.records.size(), 1u);
    EXPECT_EQ(result.records[0].writes.at(0).key, "k1");
    EXPECT_EQ(wal.write_offset(), damaged);
  }
  AssertRangeIsZero(damaged, file_size);
  EXPECT_EQ(FileSize(), file_size);

  // Repair keeps the reservation and restores the offset used by the next
  // process, even when its new group is shorter than the discarded suffix.
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan();
    ASSERT_EQ(result.status, WalScanResult::Status::kOk);
    EXPECT_FALSE(result.tail_zeroed);
    ASSERT_EQ(wal.write_offset(), damaged);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[4] = MakeRecords(4, "k4");
    ASSERT_TRUE(wal.AppendGroup(buckets, 4).ok);
    EXPECT_EQ(FileSize(), file_size);
  }
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 4u);
  ASSERT_EQ(result.records.size(), 2u);
  EXPECT_EQ(result.records[0].writes.at(0).key, "k1");
  EXPECT_EQ(result.records[1].writes.at(0).key, "k4");
}

TEST_F(WalFrameTest, PartialHeaderIsRepaired) {
  AppendEpochs({1, 2});
  const off_t full_end = EndOfLog();
  const off_t full_size = FileSize();

  // A write that stopped inside the next frame's header, which leaves a
  // prefix of the magic over the reserved zeroes.
  const std::vector<uint8_t> partial_header = {0x4c, 0x41, 0x57};
  WriteRawBytesAt(full_end, partial_header);

  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan();
    ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
    EXPECT_TRUE(result.tail_zeroed);
    EXPECT_EQ(result.last_epoch, 2u);
    EXPECT_EQ(wal.write_offset(), full_end);
  }
  // The bytes the torn write left behind are restored to zero, not just
  // logically ignored, and the file did not shrink.
  AssertRangeIsZero(full_end,
                    full_end + static_cast<off_t>(partial_header.size()));
  EXPECT_EQ(FileSize(), full_size);

  // A later scan reaches the same end with nothing left to repair.
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(wal.write_offset(), full_end);
}

TEST_F(WalFrameTest, PartialPayloadIsRepaired) {
  AppendEpochs({1, 2});
  const off_t log_end = EndOfLog();
  const off_t file_size = FileSize();

  // A full header with only ten payload bytes. The declared end may lie in
  // the reservation or beyond the file; both leave the same valid prefix.
  for (const uint32_t length : {50u, static_cast<uint32_t>(kCapacity)}) {
    SCOPED_TRACE(length);
    auto torn = MakeHeaderClaimingPayload(3, length);
    torn.insert(torn.end(), 10, 0xab);
    WriteRawBytesAt(log_end, torn);

    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan();
    ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
    EXPECT_TRUE(result.tail_zeroed);
    EXPECT_EQ(result.last_epoch, 2u);
    EXPECT_EQ(result.records.size(), 2u);
    EXPECT_EQ(wal.write_offset(), log_end);
    AssertRangeIsZero(log_end, file_size);
    EXPECT_EQ(FileSize(), file_size);
  }
}

TEST_F(WalFrameTest, AnInvalidHeaderEndsTheRecoveredPrefix) {
  AppendEpochs({1, 2});
  const off_t damaged = FrameEnd(0);
  WriteRawBytesAt(damaged + kFlagsOffset, {0x07, 0x00});

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_TRUE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 1u);
  ASSERT_EQ(result.records.size(), 1u);
  EXPECT_EQ(wal.write_offset(), damaged);
  AssertRangeIsZero(damaged, FileSize());
}

TEST_F(WalFrameTest, AnEmbeddedFrameDoesNotSurviveTheTornTail) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  const auto embedded = MakeFrame(5, PackRecords(MakeRecords(5, "k5")));
  auto torn = MakeHeaderClaimingPayload(6, 4096);
  torn.insert(torn.end(), embedded.begin(), embedded.end());
  WriteRawBytesAt(log_end, torn);

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_TRUE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 1u);
  ASSERT_EQ(result.records.size(), 1u);
  EXPECT_EQ(wal.write_offset(), log_end);
  AssertRangeIsZero(log_end, FileSize());
}

TEST_F(WalFrameTest, MalformedPayloadsEndTheRecoveredPrefix) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  auto trailing = PackRecords(MakeRecords(2, "k2"));
  trailing.push_back(0x00);
  const std::vector<std::vector<uint8_t>> payloads = {
      {0xc1},                            // invalid MessagePack
      trailing,                          // bytes after the record list
      PackRecords(LogRecords{}),         // empty record list
      PackRecords(MakeRecords(7, "k7")),  // disagrees with frame epoch 2
  };
  for (const auto &payload : payloads) {
    // Recompute the checksum so the decoder, not CRC, rejects the frame.
    WriteRawBytesAt(log_end, MakeFrame(2, payload));
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan();
    ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
    EXPECT_TRUE(result.tail_zeroed);
    EXPECT_EQ(result.last_epoch, 1u);
    ASSERT_EQ(result.records.size(), 1u);
    EXPECT_EQ(result.records[0].writes.at(0).key, "k1");
    EXPECT_EQ(wal.write_offset(), log_end);
    AssertRangeIsZero(log_end, FileSize());
  }
}

TEST_F(WalFrameTest, ZeroOrRegressedEpochEndsTheRecoveredPrefix) {
  AppendEpochs({3});
  const off_t log_end = EndOfLog();
  for (const EpochNumber epoch : {0u, 2u}) {
    SCOPED_TRACE(epoch);
    WriteRawBytesAt(
        log_end, MakeFrame(epoch, PackRecords(MakeRecords(epoch, "k"))));
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan();
    ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
    EXPECT_TRUE(result.tail_zeroed);
    EXPECT_EQ(result.last_epoch, 3u);
    EXPECT_EQ(result.records.size(), 1u);
    EXPECT_EQ(wal.write_offset(), log_end);
    AssertRangeIsZero(log_end, FileSize());
  }
}

TEST_F(WalFrameTest, AZeroHeaderBeforeNonzeroBytesEndsTheRecoveredPrefix) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();
  // Frame-like bytes after the reserved-zero header are outside the prefix.
  WriteRawBytesAt(log_end + 128, MakeFrame(2, PackRecords(MakeRecords(2, "k2"))));

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_TRUE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 1u);
  EXPECT_EQ(result.records.size(), 1u);
  EXPECT_EQ(wal.write_offset(), log_end);
  AssertRangeIsZero(log_end, FileSize());
}

TEST_F(WalFrameTest, AReadFailureDoesNotRepairTheLog) {
  AppendEpochs({1, 2});
  const off_t payload_offset = FrameEnd(0) + Wal::kHeaderSize;
  const off_t log_end = EndOfLog();
  const off_t file_size = FileSize();
  auto io = helios::storage::wal::WalIo::Posix();
  const auto real_pread = io.pread;
  io.pread = [real_pread, payload_offset](int fd, void *data, size_t size,
                                        off_t offset) -> ssize_t {
    if (offset == payload_offset) {
      errno = EIO;
      return -1;
    }
    return real_pread(fd, data, size, offset);
  };
  {
    Wal wal(work_dir_, io, kCapacity);
    const auto result = wal.Scan();
    EXPECT_EQ(result.status, WalScanResult::Status::kIoError);
    EXPECT_EQ(result.error_number, EIO);
    EXPECT_FALSE(result.tail_zeroed);
    EXPECT_EQ(wal.write_offset(), 0);
  }
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk);
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.records.size(), 2u);
  EXPECT_EQ(result.last_epoch, 2u);
  EXPECT_EQ(wal.write_offset(), log_end);
  EXPECT_EQ(FileSize(), file_size);
}

TEST_F(WalFrameTest, ARepairWriteFailureDoesNotReturnARecoveredPrefix) {
  AppendEpochs({1, 2});
  const off_t damaged = FrameEnd(0);
  FlipByteAt(damaged + Wal::kHeaderSize + 1);
  auto io = helios::storage::wal::WalIo::Posix();
  io.initialise_pwrite = [](int, const void *, size_t, off_t) -> ssize_t {
    errno = ENOSPC;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  const auto result = wal.Scan();
  EXPECT_EQ(result.status, WalScanResult::Status::kIoError);
  EXPECT_EQ(result.error_number, ENOSPC);
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(wal.write_offset(), 0);
}

TEST_F(WalFrameTest, AnAppendBelowTheLastEpochIsRefused) {
  AppendEpochs({3});

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[2] = MakeRecords(2, "k2");
  const auto result = wal.AppendGroup(buckets, 2);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EINVAL);
}

/**
 * @brief The rule that nothing reaches the log before its end is known holds
 *        even for an instance that was never scanned at all.
 *
 * The expected output is empty because the reason is logged through spdlog,
 * which writes to stdout, while a death test watches stderr.
 */
TEST_F(WalFrameTest, AnAppendBeforeTheScanIsRefused) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  EXPECT_DEATH(wal.AppendGroup(buckets, 1), "");
}

TEST_F(WalFrameTest, ShortWritesAreRetriedUntilTheGroupIsComplete) {
  {
    helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
    int write_calls = 0;
    io.pwrite = [&write_calls](int fd, const void *data, size_t,
                               off_t offset) -> ssize_t {
      ++write_calls;
      return ::pwrite(fd, data, 1, offset);  // one byte per call
    };

    Wal wal(work_dir_, io, kCapacity);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, "k1");
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
    EXPECT_GT(write_calls, 1);
  }

  // The exclusive flock is held only while `wal` is alive; the reader needs
  // its own instance, opened after that one is gone.
  Wal reader(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = reader.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_EQ(result.last_epoch, 1u);
  ASSERT_EQ(result.records.size(), 1u);
}

// A pwrite is allowed to write less than it was asked for. The group has to
// be carried to completion at the right offsets, not restarted or left
// short.
TEST_F(WalFrameTest, APartialWriteIsCarriedToCompletion) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  size_t calls = 0;
  io.pwrite = [&calls](int fd, const void *data, size_t size, off_t offset) {
    ++calls;
    return ::pwrite(fd, data, std::min<size_t>(size, 7), offset);
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  buckets[2] = MakeRecords(2, "k2");
  ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
  EXPECT_GT(calls, 1u);

  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 2u);
  EXPECT_EQ(result.records.size(), 2u);
}

TEST_F(WalFrameTest, WriteFailurePropagatesWithoutSyncing) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  bool synced = false;
  io.pwrite = [](int, const void *, size_t, off_t) -> ssize_t {
    errno = EIO;
    return -1;
  };
  io.fdatasync = [&synced](int) {
    synced = true;
    return 0;
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
  EXPECT_FALSE(synced);
}

// After a group whose own outcome is unknown, where the log ends is unknown
// too, and every later group is refused rather than written at a guessed
// offset.
//
// The expected output is empty because the reason is logged through spdlog,
// which writes to stdout, while a death test watches stderr.
TEST_F(WalFrameTest, AFailedAppendRefusesEveryLaterAppend) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> first;
  first[1] = MakeRecords(1, "k1");
  ASSERT_FALSE(wal.AppendGroup(first, 1).ok);

  std::map<EpochNumber, LogRecords> second;
  second[2] = MakeRecords(2, "k2");
  EXPECT_DEATH(wal.AppendGroup(second, 2), "");
}

TEST_F(WalFrameTest, ABucketTheScanWouldRejectIsRefused) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = LogRecords{};  // empty
    const auto result = wal.AppendGroup(buckets, 1);
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.error_number, EINVAL);
  }
  {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[2] = MakeRecords(7, "k7");  // record epoch disagrees
    const auto result = wal.AppendGroup(buckets, 2);
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.error_number, EINVAL);
  }
  EXPECT_EQ(wal.write_offset(), 0);
}

TEST_F(WalFrameTest, FdatasyncFailurePropagates) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  io.fdatasync = [](int) {
    errno = EIO;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  const auto result = wal.AppendGroup(buckets, 1);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);
}

TEST_F(WalFrameTest, ScanAcceptsTheMaximumEpoch) {
  // The scanner does not bound the epoch; the resume-epoch computation is
  // what has to refuse near the wrap. Recording that division here keeps a
  // later change from quietly moving the check into the scanner and leaving
  // startup to add one to UINT32_MAX.
  const EpochNumber near_wrap = 0xFFFFFFFFu;
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[near_wrap] = MakeRecords(near_wrap, "k");
    ASSERT_TRUE(wal.AppendGroup(buckets, near_wrap).ok);
  }
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk);
  EXPECT_EQ(result.last_epoch, near_wrap);
}

// The whole point of the design: the file's size is settled before the
// first group, so a group flush has no new size to persist.
TEST_F(WalFrameTest, CapacityIsWrittenOutAndGroupsDoNotChangeTheFileSize) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  ASSERT_EQ(FileSize(), static_cast<off_t>(kCapacity));

  off_t previous_end = 0;
  for (EpochNumber epoch = 1; epoch <= 4; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
    EXPECT_GT(wal.write_offset(), previous_end);
    previous_end = wal.write_offset();
  }
  EXPECT_EQ(wal.extension_count(), 0u);
}

// A log written before capacity existed ends where the file ends, with no
// zeroes after it. Opening it must find that end and reserve from there.
TEST_F(WalFrameTest, AGrownLogIsAdoptedWithoutLosingFrames) {
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, "k1");
    buckets[2] = MakeRecords(2, "k2");
    ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
    ASSERT_EQ(FileSize(), wal.write_offset());
  }
  const off_t grown_end = FileSize();

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 2u);
  EXPECT_EQ(result.records.size(), 2u);
  EXPECT_EQ(wal.write_offset(), grown_end);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));

  std::map<EpochNumber, LogRecords> buckets;
  buckets[3] = MakeRecords(3, "k3");
  ASSERT_TRUE(wal.AppendGroup(buckets, 3).ok);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
}

// A crash during an extension leaves a file whose size stopped partway,
// because the zeroes are what advances it. The next start reserves the
// rest.
TEST_F(WalFrameTest, AHalfWrittenCapacityIsCompleted) {
  AppendEpochs({1});
  const off_t log_end = EndOfLog();

  ASSERT_EQ(::truncate(WalPath().c_str(),
                       log_end + static_cast<off_t>(kCapacity) / 4),
            0);

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 1u);
  EXPECT_EQ(wal.write_offset(), log_end);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
}

// A reservation that fails partway leaves the file as far along as it got.
// The next start finishes it, and the log it already held is untouched.
TEST_F(WalFrameTest, AnInterruptedReservationIsCompletedOnTheNextStart) {
  // Written without preallocation, so that the file still ends at the log
  // and the reservation below has something to do.
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    std::map<EpochNumber, LogRecords> buckets;
    buckets[1] = MakeRecords(1, "k1");
    ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
  }
  const off_t log_end = FileSize();

  {
    off_t allowed = static_cast<off_t>(kCapacity) / 4;
    helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
    io.initialise_pwrite = [&allowed](int fd, const void *data, size_t size,
                                      off_t offset) -> ssize_t {
      if (allowed <= 0) {
        errno = EIO;
        return -1;
      }
      const ssize_t written =
          ::pwrite(fd, data, std::min<size_t>(size, 4096), offset);
      if (written > 0) allowed -= written;
      return written;
    };

    Wal wal(work_dir_, io, kCapacity);
    const auto result = wal.Scan();
    EXPECT_EQ(result.status, WalScanResult::Status::kIoError);
    EXPECT_EQ(wal.write_offset(), 0);
  }
  ASSERT_GT(FileSize(), log_end);
  ASSERT_LT(FileSize(), static_cast<off_t>(kCapacity));

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 1u);
  EXPECT_EQ(wal.write_offset(), log_end);
  EXPECT_EQ(FileSize(), static_cast<off_t>(kCapacity));
}

// A capacity of one byte reserves exactly what each group needs, which is
// the worst case for rounding the target up and the case that would expose
// a step per unit rather than a division.
TEST_F(WalFrameTest, ACapacityOfOneByteReservesPerGroup) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), 1);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);

  for (EpochNumber epoch = 1; epoch <= 3; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    EXPECT_EQ(FileSize(), wal.write_offset());
  }
  EXPECT_EQ(wal.extension_count(), 3u);

  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_EQ(result.last_epoch, 3u);
  EXPECT_EQ(result.records.size(), 3u);
}

// A log that outgrows its capacity is extended rather than refused, and the
// extension is counted: it is a synchronous write of a whole new region,
// which a measurement of the flush alone has to see is absent.
TEST_F(WalFrameTest, OutgrowingCapacityExtendsAndIsCounted) {
  constexpr uint64_t kTinyCapacity = 4096;
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kTinyCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  ASSERT_EQ(FileSize(), static_cast<off_t>(kTinyCapacity));

  for (EpochNumber epoch = 1; epoch <= 60; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] =
        MakeRecords(epoch, std::string(200, 'x') + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
  }
  ASSERT_GT(wal.write_offset(), static_cast<off_t>(kTinyCapacity));
  EXPECT_GT(wal.extension_count(), 0u);
  EXPECT_GE(FileSize(), wal.write_offset());
  EXPECT_EQ(FileSize() % static_cast<off_t>(kTinyCapacity), 0);

  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_EQ(result.last_epoch, 60u);
  EXPECT_EQ(result.records.size(), 60u);
}

// Offsets are this process's to assign once the file is no longer opened for
// appending, so a second holder would write over frames rather than after
// them.
TEST_F(WalFrameTest, ASecondHolderIsRefused) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  EXPECT_THROW(Wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity),
               std::system_error);
}

// A log written before capacity existed can be larger than the capacity a
// later start asks for. It is kept, not cut back to fit.
TEST_F(WalFrameTest, ALegacyLogLargerThanCapacityIsPreserved) {
  constexpr uint64_t kTinyCapacity = 4096;
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(),
            Wal::kNoPreallocation);
    ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
    for (EpochNumber epoch = 1; epoch <= 40; ++epoch) {
      std::map<EpochNumber, LogRecords> buckets;
      buckets[epoch] =
          MakeRecords(epoch, std::string(200, 'x') + std::to_string(epoch));
      ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    }
    ASSERT_GT(wal.write_offset(), static_cast<off_t>(kTinyCapacity));
  }
  const off_t grown = FileSize();

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kTinyCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_EQ(result.last_epoch, 40u);
  EXPECT_EQ(result.records.size(), 40u);
  EXPECT_EQ(FileSize(), grown);
  EXPECT_EQ(wal.write_offset(), grown);
}

// Without preallocation the file tracks the log exactly, which is what a
// database that writes no record at all should leave behind.
TEST_F(WalFrameTest, WithoutPreallocationTheFileTracksTheLog) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(),
          Wal::kNoPreallocation);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  EXPECT_EQ(FileSize(), 0);

  for (EpochNumber epoch = 1; epoch <= 3; ++epoch) {
    std::map<EpochNumber, LogRecords> buckets;
    buckets[epoch] = MakeRecords(epoch, "k" + std::to_string(epoch));
    ASSERT_TRUE(wal.AppendGroup(buckets, epoch).ok);
    EXPECT_EQ(FileSize(), wal.write_offset());
  }
  EXPECT_EQ(wal.extension_count(), 0u);
}

// Reserving capacity is a startup failure of its own, told apart from a
// group's failure by its own seam. Nothing may be published from a scan
// that hit one.
TEST_F(WalFrameTest, AFailureToReserveCapacityIsReported) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  io.initialise_pwrite = [](int, const void *, size_t, off_t) -> ssize_t {
    errno = ENOSPC;
    return -1;
  };

  Wal wal(work_dir_, io, kCapacity);
  const auto result = wal.Scan();
  EXPECT_EQ(result.status, WalScanResult::Status::kIoError);
  EXPECT_EQ(result.error_number, ENOSPC);
  EXPECT_EQ(wal.write_offset(), 0);
}

// A capacity that cannot be expressed as an offset is refused rather than
// turned into a write of that size.
TEST_F(WalFrameTest, ACapacityBeyondTheOffsetRangeIsRefused) {
  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), UINT64_MAX);
  const auto result = wal.Scan();
  EXPECT_EQ(result.status, WalScanResult::Status::kIoError);
  EXPECT_EQ(result.error_number, EFBIG);
  EXPECT_EQ(wal.write_offset(), 0);
  EXPECT_EQ(FileSize(), 0);
}

TEST_F(WalFrameTest, EmptyGroupNeitherWritesNorSyncs) {
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  bool wrote = false;
  bool synced = false;
  io.pwrite = [&wrote](int fd, const void *data, size_t size, off_t offset) {
    wrote = true;
    return ::pwrite(fd, data, size, offset);
  };
  io.fdatasync = [&synced](int fd) {
    synced = true;
    return ::fdatasync(fd);
  };

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[5] = MakeRecords(5, "k5");
  // Target below every bucket: nothing is eligible.
  const auto result = wal.AppendGroup(buckets, 4);
  EXPECT_TRUE(result.ok);
  EXPECT_FALSE(wrote);
  EXPECT_FALSE(synced);
  EXPECT_EQ(wal.write_offset(), 0);
}

// The skip's whole point: a covered frame costs one header read and nothing
// else, except the one frame right before the tail, which is read in full as
// a guard on the boundary the caller is trusting.
TEST_F(WalFrameTest, SkipReadsOnlyTheGuardAndTailPayloads) {
  AppendEpochs({1, 2, 3, 4, 5});

  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  auto header_reads = std::make_shared<int>(0);
  auto payload_reads = std::make_shared<int>(0);
  auto real_pread = io.pread;
  // A frame's payload here is a few dozen bytes; the only reads anywhere near
  // capacity-sized are the unrelated end-of-log scan this test does not mean
  // to count.
  io.pread = [real_pread, header_reads, payload_reads](
                 int fd, void *data, size_t size, off_t offset) -> ssize_t {
    if (size == Wal::kHeaderSize) {
      ++*header_reads;
    } else if (size > Wal::kHeaderSize && size < 4096) {
      ++*payload_reads;
    }
    return real_pread(fd, data, size, offset);
  };

  WalScanResult skipped;
  {
    Wal wal(work_dir_, io, kCapacity);
    skipped = wal.Scan(3);
  }
  ASSERT_EQ(skipped.status, WalScanResult::Status::kOk) << skipped.detail;
  EXPECT_EQ(skipped.frames_skipped, 3u);
  EXPECT_FALSE(skipped.tail_zeroed);
  ASSERT_EQ(skipped.records.size(), 2u);
  EXPECT_EQ(skipped.records[0].epoch, 4u);
  EXPECT_EQ(skipped.records[1].epoch, 5u);

  // Header reads: 3 skipped frames, +1 to see the frame after them is past
  // min_epoch, +3 more as the resuming scan re-reads that frame, the tail,
  // and the all-zero header ending the log. 4 + 3 = 7.
  EXPECT_EQ(*header_reads, 7);
  // One payload read per frame that is not a pure skip: the guard at epoch 3
  // plus the two tail frames.
  EXPECT_EQ(*payload_reads, 3);

  Wal full_wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto full = full_wal.Scan(0);
  ASSERT_EQ(full.status, WalScanResult::Status::kOk);
  EXPECT_EQ(skipped.last_epoch, full.last_epoch);
  // bytes_skipped covers exactly the three skipped frames: the offset one past
  // the third is where the first replayed frame, epoch 4, begins.
  const off_t third_frame_end = FrameEnd(FrameEnd(FrameEnd(0)));
  EXPECT_EQ(skipped.bytes_skipped, static_cast<uint64_t>(third_frame_end));
}

// The whole log at or below min_epoch: the guard is the true last frame, and
// end-of-log handling has to run exactly as it would without a skip.
TEST_F(WalFrameTest, SkippingTheWholeLogStillFinishesTheScan) {
  AppendEpochs({1, 2, 3});
  const off_t log_end = EndOfLog();

  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  auto payload_reads = std::make_shared<int>(0);
  auto real_pread = io.pread;
  // Locating the last nonzero byte can read reservation-sized chunks;
  // those reads are not frame payloads.
  io.pread = [real_pread, payload_reads](int fd, void *data, size_t size,
                                         off_t offset) -> ssize_t {
    if (size > Wal::kHeaderSize && size < 4096) ++*payload_reads;
    return real_pread(fd, data, size, offset);
  };

  Wal wal(work_dir_, io, kCapacity);
  const auto result = wal.Scan(3);
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_EQ(result.last_epoch, 3u);
  EXPECT_EQ(result.frames_skipped, 3u);
  EXPECT_TRUE(result.records.empty());
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(wal.write_offset(), log_end);
  // Only the guard, the true last frame of the log, has its payload read.
  EXPECT_EQ(*payload_reads, 1);
}

TEST_F(WalFrameTest, ATornEpochAtTheSkipBoundaryFallsBackToTheValidPrefix) {
  AppendEpochs({1, 2, 3});
  const off_t third = FrameEnd(FrameEnd(0));
  // Epoch 3 now looks covered by the checkpoint, but its checksum is stale.
  WriteRawBytesAt(third + 12, {2, 0, 0, 0});
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    const auto result = wal.Scan(2);
    ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
    EXPECT_EQ(result.last_epoch, 2u);
    EXPECT_EQ(result.frames_skipped, 2u);
    EXPECT_TRUE(result.records.empty());
    EXPECT_TRUE(result.tail_zeroed);
    EXPECT_EQ(wal.write_offset(), third);
  }
  AssertRangeIsZero(third, FileSize());

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
  const auto result = wal.Scan();
  ASSERT_EQ(result.status, WalScanResult::Status::kOk) << result.detail;
  EXPECT_FALSE(result.tail_zeroed);
  EXPECT_EQ(result.last_epoch, 2u);
  ASSERT_EQ(result.records.size(), 2u);
  EXPECT_EQ(result.records[0].writes.at(0).key, "k1");
  EXPECT_EQ(result.records[1].writes.at(0).key, "k2");
  EXPECT_EQ(wal.write_offset(), third);
}

// A corrupted length inside the covered region lands the skip's next header
// read on bytes that don't parse; it can't tell that apart from a lie only
// in the last covered frame, so it falls back to a full scan from offset 0.
TEST_F(WalFrameTest,
       ACorruptedLengthInTheCoveredRegionFallsBackAndStaysCorrect) {
  AppendEpochs({1, 2, 3, 4, 5});
  // Frame 1's declared length, shrunk so the skip's blind trust in it lands
  // mid-frame-1's own real payload rather than on frame 2's header.
  WriteRawBytesAt(8, {4, 0, 0, 0});

  WalScanResult skip_result;
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    skip_result = wal.Scan(4);
  }
  WalScanResult full_result;
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kCapacity);
    full_result = wal.Scan(0);
  }

  ASSERT_EQ(skip_result.status, WalScanResult::Status::kOk) << skip_result.detail;
  EXPECT_TRUE(skip_result.tail_zeroed);
  EXPECT_EQ(skip_result.last_epoch, 0u);
  EXPECT_TRUE(skip_result.records.empty());
  ASSERT_EQ(full_result.status, WalScanResult::Status::kOk) << full_result.detail;
  EXPECT_FALSE(full_result.tail_zeroed);
  EXPECT_EQ(full_result.last_epoch, 0u);
  EXPECT_TRUE(full_result.records.empty());
  AssertRangeIsZero(0, FileSize());
}

TEST_F(WalFrameTest, InjectedFdatasyncFailsAfterTheAllowedCalls) {
  ASSERT_EQ(::setenv("HELIOS_WAL_FDATASYNC_FAIL_AFTER", "2", 1), 0);
  helios::storage::wal::WalIo io = helios::storage::wal::WalIo::Posix();
  // The factory captured the count; the variable must not leak to later
  // tests.
  ASSERT_EQ(::unsetenv("HELIOS_WAL_FDATASYNC_FAIL_AFTER"), 0);

  Wal wal(work_dir_, io, kCapacity);
  ASSERT_EQ(wal.Scan().status, WalScanResult::Status::kOk);
  std::map<EpochNumber, LogRecords> buckets;
  buckets[1] = MakeRecords(1, "k1");
  ASSERT_TRUE(wal.AppendGroup(buckets, 1).ok);
  buckets.clear();
  buckets[2] = MakeRecords(2, "k2");
  ASSERT_TRUE(wal.AppendGroup(buckets, 2).ok);
  buckets.clear();
  buckets[3] = MakeRecords(3, "k3");
  const auto result = wal.AppendGroup(buckets, 3);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.error_number, EIO);

  // The failed sync poisons the instance exactly like a failed write: a
  // later append is refused before it touches the file.
  const off_t offset_after_failure = wal.write_offset();
  buckets.clear();
  buckets[4] = MakeRecords(4, "k4");
  EXPECT_DEATH(wal.AppendGroup(buckets, 4), "");
  EXPECT_EQ(wal.write_offset(), offset_after_failure);
}

TEST_F(WalFrameTest, AnUnparsableInjectionCountStopsStartup) {
  const char *const malformed[] = {
      "",                      // armed with nothing
      "abc",                   // not a number
      "1junk",                 // trailing garbage
      "-1",                    // sign
      "+1",                    // sign
      " 1",                    // leading whitespace
      "99999999999999999999",  // out of long range
  };
  for (const char *value : malformed) {
    ASSERT_EQ(::setenv("HELIOS_WAL_FDATASYNC_FAIL_AFTER", value, 1), 0);
    EXPECT_EXIT(helios::storage::wal::WalIo::Posix(),
                ::testing::ExitedWithCode(EXIT_FAILURE), "")
        << "value: " << value;
  }
  ASSERT_EQ(::unsetenv("HELIOS_WAL_FDATASYNC_FAIL_AFTER"), 0);
}

}  // namespace
