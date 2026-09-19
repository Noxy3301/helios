/**
 * @file server/storage/tests/recovery_test.cc
 * What a commit leaves in the log, and that a row recovered from it carries
 * an unlocked transaction id and accepts a further write.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <string>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/read.h"

#include "db_helper.h"
#include "sync_point.h"
#include "wal/wal.h"

namespace {

constexpr const char *kTable = "recovery_test";
constexpr const char *kIndex = "idx";
constexpr auto kTestTimeout = std::chrono::seconds(10);

using helios::storage::wal::Wal;
using helios::storage::wal::WalScanResult;

// The commit path is what query layer traffic takes, and what it writes
// to the log is only observable after the instance that wrote it is gone:
// the log is held under an exclusive lock while a Database is open.
class RecoveryTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { keep_sync_facility_armed(); }

  void SetUp() override {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "helios_recovery_XXXXXX")
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

  enum class Recovery { kOff, kOn };

  static constexpr uint64_t kWalCapacityBytes = 1ull << 20;

  helios::storage::Config MakeConfig(Recovery recovery) const {
    helios::storage::Config config;
    config.epoch_duration_ms = 10;
    config.enable_recovery = recovery == Recovery::kOn;
    config.work_dir = work_dir_;
    config.wal_initial_capacity_bytes = kWalCapacityBytes;
    return config;
  }

  static bool CommitWrite(helios::storage::Database &db, const std::string &key,
                          const std::string &value) {
    std::string commit_reason;
    return TestHelper::CommitRows(
        db, {}, {{kTable, key, TestHelper::Row(value)}}, {}, {}, commit_reason);
  }

  static bool CommitWriteWithIndexEntry(helios::storage::Database &db,
                                        const std::string &key,
                                        const std::string &value,
                                        const std::string &secondary_key) {
    std::string commit_reason;
    return TestHelper::CommitRows(
        db, {}, {{kTable, key, TestHelper::Row(value)}},
        {{kTable, kIndex, secondary_key, key}}, {}, commit_reason);
  }

  // Two primary keys under one secondary key: refused only by a UNIQUE index.
  static std::string DuplicateSecondaryKeyReason(
      helios::storage::Database &db) {
    std::string reason;
    EXPECT_FALSE(TestHelper::Commit(
        db, {}, {{kTable, "k1", "v1"}, {kTable, "k2", "v2"}},
        {{kTable, kIndex, "s", "k1"}, {kTable, kIndex, "s", "k2"}}, {},
        reason));
    return reason;
  }

  static helios::storage::ReadResult Read(helios::storage::Database &db,
                                          const std::string &key) {
    auto result = db.Read(kTable, key);
    db.ReleaseThreadEpoch();
    if (result.found) result.value = TestHelper::RowPayload(result.value);
    return result;
  }

  std::string root_;
  std::string work_dir_;
  ArmedSyncPoints points_;
};

TEST_F(RecoveryTest, ALoggedWriteCarriesTheUnlockedTid) {
  {
    auto config = MakeConfig(Recovery::kOff);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(db.CreateSecondaryIndex(
        kTable, kIndex, helios::storage::IndexConstraint::kNone));
    ASSERT_TRUE(CommitWriteWithIndexEntry(db, "k", "v1", "s"));
  }

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kWalCapacityBytes);
  auto scan = wal.Scan();
  ASSERT_EQ(scan.status, WalScanResult::Status::kOk);

  bool seen_row = false;
  bool seen_index_entry = false;
  for (const auto &record : scan.records) {
    for (const auto &write : record.writes) {
      if (write.index_name.empty()) {
        if (write.key != "k") continue;
        seen_row = true;
        EXPECT_EQ(write.buffer, TestHelper::Row("v1"));
      } else {
        if (write.key != "s") continue;
        seen_index_entry = true;
        EXPECT_EQ(write.secondary_primary_key, "k");
      }
      // A word with the lock bit is a write lock held by a transaction that
      // no longer exists; recovery installs it verbatim and every later reader
      // and writer of the key waits on it forever.
      EXPECT_FALSE(write.transaction_id.lock);
      // A published word always sets latest; the zero word never reaches here.
      EXPECT_TRUE(write.transaction_id.latest);
      // The log entry is taken before the unlock, so an epoch that advanced
      // under the lock would be recorded one epoch behind the frame.
      EXPECT_EQ(write.transaction_id.epoch, record.epoch);
    }
  }
  EXPECT_TRUE(seen_row);
  EXPECT_TRUE(seen_index_entry);
}

TEST_F(RecoveryTest, ARecoveredKeyAcceptsAFurtherWrite) {
  {
    auto config = MakeConfig(Recovery::kOff);
    helios::storage::Database db(config);
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  }

  // Guarded by the assertion above: a locked TID in the log makes the read
  // and the write below spin rather than fail.
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kWalCapacityBytes);
    auto scan = wal.Scan();
    ASSERT_EQ(scan.status, WalScanResult::Status::kOk);
    bool seen = false;
    for (const auto &record : scan.records) {
      for (const auto &write : record.writes) {
        if (write.key != "k") continue;
        seen = true;
        ASSERT_FALSE(write.transaction_id.lock);
      }
    }
    ASSERT_TRUE(seen) << "the log holds no record of the key";
  }

  auto config = MakeConfig(Recovery::kOn);
  helios::storage::Database db(config);
  TestHelper::CreateTable(db, kTable);
  const auto recovered = Read(db, "k");
  EXPECT_TRUE(recovered.found);
  EXPECT_EQ(recovered.value, "v1");

  ASSERT_TRUE(CommitWrite(db, "k", "v2"));
  const auto rewritten = Read(db, "k");
  EXPECT_TRUE(rewritten.found);
  EXPECT_EQ(rewritten.value, "v2");
}

TEST_F(RecoveryTest, AnIndexWithoutARecordKeepsItsConstraint) {
  {
    auto config = MakeConfig(Recovery::kOn);
    helios::storage::Database db(config);
    ASSERT_TRUE(TestHelper::CreateTable(db, kTable));
    ASSERT_TRUE(db.CreateSecondaryIndex(
        kTable, kIndex, helios::storage::IndexConstraint::kUnique));
  }

  // Nothing was written under the index, so only the catalog can restore it.
  auto config = MakeConfig(Recovery::kOn);
  helios::storage::Database db(config);
  const std::string reason = DuplicateSecondaryKeyReason(db);
  EXPECT_EQ(reason.rfind(helios::storage::kDuplicateSecondaryKeyAbortPrefix, 0),
            0u)
      << "abort reason: " << reason;
}

TEST_F(RecoveryTest, AnIndexDeclaredBeforeTheSchemaSurvives) {
  {
    auto config = MakeConfig(Recovery::kOn);
    helios::storage::Database db(config);
    ASSERT_TRUE(db.CreateTable(kTable));
    ASSERT_TRUE(db.CreateSecondaryIndex(
        kTable, kIndex, helios::storage::IndexConstraint::kUnique));
    ASSERT_TRUE(db.InstallPaxSchema(kTable, {1, 4096}));
    db.ReleaseThreadEpoch();
  }

  // The install is what writes the table's first catalog entry.
  auto config = MakeConfig(Recovery::kOn);
  helios::storage::Database db(config);
  const std::string reason = DuplicateSecondaryKeyReason(db);
  EXPECT_EQ(reason.rfind(helios::storage::kDuplicateSecondaryKeyAbortPrefix, 0),
            0u)
      << "abort reason: " << reason;
}

TEST_F(RecoveryTest, EarlyPublicationKeepsTheOriginalWalValue) {
  using namespace helios::storage;
  Pipe arrived;
  Pipe release;
  std::string first_key;
  Tidword first_commit_tid;
  {
    Database db(MakeConfig(Recovery::kOff));
    TestHelper::CreateTable(db, kTable);
    ASSERT_TRUE(CommitWrite(db, "alice", "old"));
    ASSERT_TRUE(CommitWrite(db, "bob", "old"));
    const auto rows = db.ScanPax(kTable, "", "\xff", 0, false);
    db.ReleaseThreadEpoch();
    ASSERT_TRUE(rows.ok);
    ASSERT_EQ(2u, rows.rows.size());

    points_.arm("HELIOS_DEBUG_SYNC_SILO_COMMIT_BETWEEN_ROW_INSTALLS",
                "arrive_and_wait:" + std::to_string(arrived.write_fd()) + ":" +
                    std::to_string(release.read_fd()));
    auto writer = std::async(std::launch::async, [&] {
      std::string reason;
      return TestHelper::CommitRows(
          db, {},
          {{kTable, "alice", TestHelper::Row("batch")},
           {kTable, "bob", TestHelper::Row("batch")}},
          {}, {}, reason);
    });
    ReleaseOnExit release_writer{release.write_fd()};
    char announcement;
    ASSERT_TRUE(wait_readable(arrived.read_fd(), kTestTimeout));
    ASSERT_EQ(::read(arrived.read_fd(), &announcement, 1), 1);

    // The map's pointer order is unspecified; locate the published row by its
    // lock.
    const Tidword left(CurrentTid(rows.rows[0]));
    const Tidword right(CurrentTid(rows.rows[1]));
    ASSERT_NE(left.lock, right.lock);
    const auto &published = left.lock ? rows.rows[1] : rows.rows[0];
    first_key = published.key;
    first_commit_tid = Tidword(CurrentTid(published));

    auto overwrite = std::async(std::launch::async, [&] {
      std::string reason;
      return TestHelper::CommitRows(
          db, {}, {{kTable, first_key, TestHelper::Row("later")}}, {}, {},
          reason, CommitDurability::kAsync);
    });
    ReleaseOnExit release_before_overwrite_join{release.write_fd()};
    ASSERT_EQ(overwrite.wait_for(kTestTimeout), std::future_status::ready);
    EXPECT_TRUE(overwrite.get());
    EXPECT_EQ(Read(db, first_key).value, "later");

    ASSERT_EQ(::write(release.write_fd(), "r", 1), 1);
    ASSERT_EQ(writer.wait_for(kTestTimeout), std::future_status::ready);
    EXPECT_TRUE(writer.get());
  }

  const auto scan = Wal(work_dir_).Scan();
  ASSERT_EQ(scan.status, WalScanResult::Status::kOk);
  size_t batch_values = 0;
  bool found_later = false;
  for (const auto &record : scan.records) {
    for (const auto &write : record.writes) {
      if (write.transaction_id == first_commit_tid) {
        EXPECT_EQ(write.buffer, TestHelper::Row("batch"));
        ++batch_values;
      }
      if (write.key == first_key && write.buffer == TestHelper::Row("later"))
        found_later = true;
    }
  }
  EXPECT_EQ(batch_values, 2u);
  EXPECT_TRUE(found_later);
}

}  // namespace
