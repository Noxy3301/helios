/**
 * @file server/storage/tests/epoch_scan_checkpoint_test.cc
 * The checkpoint: what a scan captures, what recovery does with a damaged
 * or absent one, and that the log tail wins over it.
 */

#include "wal/epoch_scan_checkpoint.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/read.h"

#include "db_helper.h"
#include "sync_point.h"
#include "wal/logger.h"
#include "wal/wal.h"

namespace {

constexpr const char *kTable = "checkpoint_test";
constexpr const char *kIndex = "idx";
constexpr auto kTestTimeout = std::chrono::seconds(10);

// Closes the write end on scope exit. Declared after the future whose read
// loop owns the other end, so an early return delivers the EOF that ends it
// before that future's destructor would otherwise block joining it.
struct CloseWriteOnExit {
  Pipe &pipe;
  ~CloseWriteOnExit() { pipe.close_write(); }
};

using helios::storage::wal::EpochScanCheckpoint;
using helios::storage::wal::Wal;
using helios::storage::wal::WalScanResult;

class EpochScanCheckpointTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { keep_sync_facility_armed(); }

  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "helios_ckpt_XXXXXX")
            .string();
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

  helios::storage::Config MakeConfig(bool enable_recovery) const {
    helios::storage::Config config;
    config.epoch_duration_ms = 10;
    config.enable_recovery = enable_recovery;
    config.work_dir = work_dir_;
    config.wal_initial_capacity_bytes = 1ull << 20;
    return config;
  }

  static bool CommitWrite(helios::storage::Database &db, const std::string &key,
                          const std::string &value) {
    std::string commit_reason;
    return TestHelper::CommitRows(
        db, {}, {{kTable, key, TestHelper::Row(value)}}, {}, {}, commit_reason);
  }

  static bool CommitDelete(helios::storage::Database &db,
                           const std::string &key) {
    std::string commit_reason;
    return TestHelper::CommitRows(
        db, {}, {{kTable, key, "", helios::storage::RowOp::kDelete}}, {}, {},
        commit_reason);
  }

  static bool CommitIndexedWrite(helios::storage::Database &db,
                                 const std::string &key,
                                 const std::string &value,
                                 const std::string &secondary_key) {
    std::string commit_reason;
    return TestHelper::CommitRows(
        db, {}, {{kTable, key, TestHelper::Row(value)}},
        {{kTable, kIndex, secondary_key, key}}, {}, commit_reason);
  }

  static helios::storage::ReadResult Read(helios::storage::Database &db,
                                          const std::string &key) {
    auto result = db.Read(kTable, key);
    db.ReleaseThreadEpoch();
    if (result.found) result.value = TestHelper::RowPayload(result.value);
    return result;
  }

  // Every key's value, in key order, as the database currently holds it.
  static std::vector<std::string> ReadAliceBobCarol(
      helios::storage::Database &db) {
    std::vector<std::string> rows;
    for (const char *key : {"alice", "bob", "carol"}) {
      const auto row = Read(db, key);
      rows.emplace_back(std::string(key) + "=" + (row.found ? row.value : ""));
    }
    return rows;
  }

  // Every secondary-index hit, as `secondary_key/primary_key=value`.
  static std::vector<std::string> ReadIndex(helios::storage::Database &db) {
    auto result = db.ScanIndex(kTable, kIndex, "", "\xff", 0, false);
    db.ReleaseThreadEpoch();
    std::vector<std::string> hits;
    for (const auto &row : result.rows) {
      hits.emplace_back(row.secondary_key + "/" + row.primary_key + "=" +
                        TestHelper::RowPayload(row.value));
    }
    std::sort(hits.begin(), hits.end());
    return hits;
  }

  // The row value the checkpoint holds for `key`, if it holds one.
  static std::optional<std::string> RowInCheckpoint(
      const EpochScanCheckpoint::LoadResult &checkpoint,
      const std::string &key) {
    for (const auto &record : checkpoint.records) {
      for (const auto &write : record.writes) {
        if (!write.index_name.empty() || write.key != key) continue;
        return TestHelper::RowPayload(write.buffer);
      }
    }
    return std::nullopt;
  }

  // The primary keys the checkpoint lists under a secondary key.
  static std::vector<std::string> IndexEntryInCheckpoint(
      const EpochScanCheckpoint::LoadResult &checkpoint,
      const std::string &key) {
    for (const auto &record : checkpoint.records) {
      for (const auto &write : record.writes) {
        if (write.index_name != kIndex || write.key != key) continue;
        return write.primary_keys;
      }
    }
    return {};
  }

  std::string checkpoint_path() const {
    return (std::filesystem::path(work_dir_) /
            EpochScanCheckpoint::CheckpointFileName())
        .string();
  }

  std::string working_path() const {
    return (std::filesystem::path(work_dir_) /
            EpochScanCheckpoint::WorkingFileName())
        .string();
  }

  std::string root_;
  std::string work_dir_;
  helios::storage::EpochNumber last_epoch_ = 0;
  ArmedSyncPoints points_;
};

TEST_F(EpochScanCheckpointTest, ACheckpointHoldsWhatTheScanFound) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(db.CreateSecondaryIndex(
        kTable, kIndex, helios::storage::IndexConstraint::kNone));
    ASSERT_TRUE(CommitIndexedWrite(db, "alice", "one", "s"));
    ASSERT_TRUE(CommitIndexedWrite(db, "bob", "two", "s"));
    ASSERT_TRUE(db.WriteCheckpoint());
  }

  auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);
  EXPECT_EQ(RowInCheckpoint(checkpoint, "alice"), "one");
  EXPECT_EQ(RowInCheckpoint(checkpoint, "bob"), "two");
  auto primary_keys = IndexEntryInCheckpoint(checkpoint, "s");
  std::sort(primary_keys.begin(), primary_keys.end());
  EXPECT_EQ(primary_keys, (std::vector<std::string>{"alice", "bob"}));
  EXPECT_NE(checkpoint.start_epoch, 0u);
  EXPECT_GE(checkpoint.end_epoch, checkpoint.start_epoch);
  // The working file is renamed rather than left behind.
  EXPECT_FALSE(std::filesystem::exists(working_path()));
}

TEST_F(EpochScanCheckpointTest, ADeletedRowLeavesNoEntry) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "two"));
    ASSERT_TRUE(CommitDelete(db, "alice"));
    ASSERT_TRUE(db.WriteCheckpoint());
  }

  auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);
  EXPECT_EQ(RowInCheckpoint(checkpoint, "alice"), std::nullopt);
  EXPECT_EQ(RowInCheckpoint(checkpoint, "bob"), "two");
}

TEST_F(EpochScanCheckpointTest, AnAbsentCheckpointIsNotAFailure) {
  auto checkpoint = EpochScanCheckpoint::Load(work_dir_ + "/nowhere");
  EXPECT_EQ(checkpoint.status,
            EpochScanCheckpoint::LoadResult::Status::kAbsent);
}

TEST_F(EpochScanCheckpointTest, ADamagedCheckpointIsRefused) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(db.WriteCheckpoint());
  }

  // One byte inside the payload, which the checksum covers.
  {
    std::fstream file(checkpoint_path(),
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file.seekp(static_cast<std::streamoff>(EpochScanCheckpoint::kHeaderSize));
    char flipped = 0x7f;
    file.write(&flipped, 1);
  }

  auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  EXPECT_EQ(checkpoint.status,
            EpochScanCheckpoint::LoadResult::Status::kUnusable);
  EXPECT_TRUE(checkpoint.records.empty());
}

TEST_F(EpochScanCheckpointTest, TheLogTailWinsOverTheCheckpoint) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "one"));
    ASSERT_TRUE(CommitWrite(db, "carol", "one"));
    ASSERT_TRUE(db.WriteCheckpoint());
    // Written after the start epoch: the checkpoint holds the old version of
    // alice and no version of dave, and the tail has to supply both.
    ASSERT_TRUE(CommitWrite(db, "alice", "two"));
    ASSERT_TRUE(CommitDelete(db, "carol"));
    ASSERT_TRUE(CommitWrite(db, "dave", "two"));
  }

  auto config = MakeConfig(true);
  helios::storage::Database db(config);
  TestHelper::CreateTable(db, kTable);
  EXPECT_EQ(Read(db, "alice").value, "two");
  // Only the checkpoint holds this one: its record is in a frame the replay
  // skips.
  EXPECT_EQ(Read(db, "bob").value, "one");
  EXPECT_FALSE(Read(db, "carol").found);
  EXPECT_EQ(Read(db, "dave").value, "two");
}

TEST_F(EpochScanCheckpointTest,
       RecoveryWithTheCheckpointMatchesRecoveryWithout) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(db.CreateSecondaryIndex(
        kTable, kIndex, helios::storage::IndexConstraint::kNone));
    ASSERT_TRUE(CommitIndexedWrite(db, "alice", "one", "s"));
    ASSERT_TRUE(CommitIndexedWrite(db, "bob", "one", "t"));
    ASSERT_TRUE(db.WriteCheckpoint());
    ASSERT_TRUE(CommitWrite(db, "alice", "two"));
    ASSERT_TRUE(CommitDelete(db, "bob"));
    ASSERT_TRUE(CommitIndexedWrite(db, "carol", "two", "s"));
  }

  const auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);

  // What the replay leaves out, and that it leaves out something at all.
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), 1ull << 20);
    auto full = wal.Scan(0);
    ASSERT_EQ(full.status, WalScanResult::Status::kOk);
    EXPECT_EQ(full.frames_skipped, 0u);
    last_epoch_ = full.last_epoch;
  }
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), 1ull << 20);
    auto filtered = wal.Scan(checkpoint.start_epoch);
    ASSERT_EQ(filtered.status, WalScanResult::Status::kOk);
    EXPECT_GT(filtered.frames_skipped, 0u);
    EXPECT_GT(filtered.bytes_skipped, 0u);
    // The end of the log and how far it is durable come from every frame.
    EXPECT_EQ(filtered.last_epoch, last_epoch_);
    for (const auto &record : filtered.records) {
      EXPECT_GT(record.epoch, checkpoint.start_epoch);
    }
  }

  std::vector<std::string> with_checkpoint;
  std::vector<std::string> index_with_checkpoint;
  {
    auto config = MakeConfig(true);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    with_checkpoint = ReadAliceBobCarol(db);
    index_with_checkpoint = ReadIndex(db);
  }

  std::error_code ec;
  ASSERT_TRUE(std::filesystem::remove(checkpoint_path(), ec)) << ec.message();
  std::vector<std::string> without_checkpoint;
  std::vector<std::string> index_without_checkpoint;
  {
    auto config = MakeConfig(true);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    without_checkpoint = ReadAliceBobCarol(db);
    index_without_checkpoint = ReadIndex(db);
  }

  EXPECT_EQ(with_checkpoint, without_checkpoint);
  EXPECT_EQ(with_checkpoint,
            (std::vector<std::string>{"alice=two", "bob=", "carol=two"}));
  EXPECT_EQ(index_with_checkpoint, index_without_checkpoint);
  // The index reaches the row the tail rewrote and the one it added, and no
  // longer reaches the row the tail deleted.
  EXPECT_EQ(index_with_checkpoint,
            (std::vector<std::string>{"s/alice=two", "s/carol=two"}));
}

TEST_F(EpochScanCheckpointTest, AQuietTailAfterTheCheckpointIsAccepted) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(CommitWrite(db, "bob", "one"));
    // Nothing is written afterwards, so the scan ends past the epoch of the
    // last frame the log holds: the database went quiet before the checkpoint
    // did.
    ASSERT_TRUE(db.WriteCheckpoint());
  }

  const auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), 1ull << 20);
    auto scan = wal.Scan(0);
    ASSERT_EQ(scan.status, WalScanResult::Status::kOk);
    // Closed epochs with no writes need no frames of their own.
    ASSERT_LT(scan.last_epoch, checkpoint.end_epoch);
  }

  {
    auto config = MakeConfig(true);
    helios::storage::Database db(config);
    EXPECT_EQ(Read(db, "alice").value, "one");
    EXPECT_EQ(Read(db, "bob").value, "one");
    ASSERT_TRUE(CommitWrite(db, "alice", "two"));
  }

  // The new write must be newer than the recovered checkpoint, so its next
  // replay cannot skip it as a frame the checkpoint already covers.
  auto config = MakeConfig(true);
  helios::storage::Database db(config);
  EXPECT_EQ(Read(db, "alice").value, "two");
  EXPECT_EQ(Read(db, "bob").value, "one");
}

// A start that does not replay still takes the checkpoint's end epoch:
// publication waited for that epoch to become durable, so resuming below it
// would hand later commits epochs a replay treats as covered.
TEST_F(EpochScanCheckpointTest, AStartWithoutAReplayTakesTheCheckpointEpoch) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    // The database goes quiet before the checkpoint does, so the checkpoint
    // ends above the epoch of the log's last frame.
    ASSERT_TRUE(db.WriteCheckpoint());
  }

  const auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), 1ull << 20);
    auto scan = wal.Scan(0);
    ASSERT_EQ(scan.status, WalScanResult::Status::kOk);
    ASSERT_LT(scan.last_epoch, checkpoint.end_epoch);
  }

  helios::storage::wal::Logger logger(MakeConfig(false));
  const auto recovery = logger.Recover();
  ASSERT_EQ(recovery.status, helios::storage::wal::Logger::RecoveryStatus::kOk);
  EXPECT_EQ(recovery.durable_epoch, checkpoint.end_epoch);
}

TEST_F(EpochScanCheckpointTest, ARowLockedDuringTheScanIsRetried) {
  Pipe scan_arrived;
  Pipe scan_release;
  Pipe write_arrived;
  Pipe write_release;

  auto config = MakeConfig(false);
  helios::storage::Database db(config);
  TestHelper::CreateTable(db, kTable);
  ASSERT_TRUE(CommitWrite(db, "alice", std::string(64, 'a')));
  ASSERT_TRUE(CommitWrite(db, "bob", std::string(64, 'a')));

  points_.arm("HELIOS_DEBUG_SYNC_CHECKPOINT_BEFORE_ROW_COPY",
              "arrive_and_wait:" + std::to_string(scan_arrived.write_fd()) +
                  ":" + std::to_string(scan_release.read_fd()));
  points_.arm("HELIOS_DEBUG_SYNC_SILO_COMMIT_BETWEEN_ROW_INSTALLS",
              "arrive_and_wait:" + std::to_string(write_arrived.write_fd()) +
                  ":" + std::to_string(write_release.read_fd()));

  uint64_t version_retries = 0;
  auto scan = std::async(std::launch::async, [&db, &version_retries] {
    return db.WriteCheckpoint(&version_retries);
  });
  // See ReleaseOnExit: unblocks a scan parked at its current wait before
  // `scan`'s own destructor would otherwise join it forever.
  ReleaseOnExit release_scan_on_exit{scan_release.write_fd()};
  // Reverse destruction must close before the release byte fires below, or a
  // scan released back into an open pipe just re-arrives and parks again;
  // this guard covers the window before the releaser exists.
  CloseWriteOnExit close_scan_arrived_early{scan_arrived};

  // The scan has loaded a version and is about to copy the bytes it belongs
  // to; nothing has locked that row yet.
  char announcement = 0;
  ASSERT_TRUE(wait_readable(scan_arrived.read_fd(), kTestTimeout));
  ASSERT_EQ(::read(scan_arrived.read_fd(), &announcement, 1), 1);

  auto writer = std::async(std::launch::async, [&db] {
    std::string commit_reason;
    return TestHelper::CommitRows(
        db, {},
        {{kTable, "alice", TestHelper::Row(std::string(64, 'b'))},
         {kTable, "bob", TestHelper::Row(std::string(64, 'b'))}},
        {}, {}, commit_reason);
  });
  ReleaseOnExit release_write_on_exit{write_release.write_fd()};

  // One row is installed and published; the other remains locked.
  ASSERT_TRUE(wait_readable(write_arrived.read_fd(), kTestTimeout));
  ASSERT_EQ(::read(write_arrived.read_fd(), &announcement, 1), 1);
  // Releasing the scan here makes it copy bytes the writer is changing, which
  // its second version read has to reject.
  ASSERT_EQ(::write(scan_release.write_fd(), "r", 1), 1);

  // From here the scan may reach the point again on any retry, so arrivals are
  // answered by a thread of their own.
  auto releaser = std::async(std::launch::async, [&] {
    for (;;) {
      char arrived = 0;
      if (::read(scan_arrived.read_fd(), &arrived, 1) != 1) return;
      if (::write(scan_release.write_fd(), "r", 1) != 1) return;
    }
  });
  // See CloseWriteOnExit: an early return still delivers the EOF the
  // releaser's read loop above is waiting on.
  CloseWriteOnExit close_scan_arrived_on_exit{scan_arrived};

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  ASSERT_EQ(::write(write_release.write_fd(), "r", 1), 1);
  ASSERT_EQ(writer.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_TRUE(writer.get());

  ASSERT_EQ(scan.wait_for(kTestTimeout), std::future_status::ready);
  EXPECT_TRUE(scan.get());
  // The scan's old version must be rejected whether the row is already
  // published or is still locked by the writer.
  EXPECT_GT(version_retries, 0u);
  scan_arrived.close_write();
  ASSERT_EQ(releaser.wait_for(kTestTimeout), std::future_status::ready);
  releaser.get();

  auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);
  // Either version is a correct answer for a scan that runs alongside a
  // writer. A mixture of the two is not.
  for (const char *key : {"alice", "bob"}) {
    const auto value = RowInCheckpoint(checkpoint, key);
    ASSERT_TRUE(value.has_value()) << key;
    EXPECT_TRUE(*value == std::string(64, 'a') ||
                *value == std::string(64, 'b'))
        << key << " holds " << *value;
  }
}

TEST_F(EpochScanCheckpointTest, ALeftoverWorkingFileIsNotRead) {
  {
    auto config = MakeConfig(false);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "one"));
    ASSERT_TRUE(db.WriteCheckpoint());
  }

  // What a crash between the write and the rename leaves behind.
  {
    std::ofstream file(working_path(), std::ios::binary);
    ASSERT_TRUE(file.is_open());
    file << "half of a checkpoint";
  }

  auto checkpoint = EpochScanCheckpoint::Load(work_dir_);
  ASSERT_EQ(checkpoint.status, EpochScanCheckpoint::LoadResult::Status::kOk);
  EXPECT_EQ(RowInCheckpoint(checkpoint, "alice"), "one");
}

}  // namespace
