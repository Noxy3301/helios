/**
 * @file server/storage/tests/recovery_test.cc
 * That a recovered row carries an unlocked transaction id and accepts a
 * further write.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <vector>

#include "storage/config.h"
#include "storage/database.h"
#include "storage/read.h"
#include "wal/wal.h"

namespace {

constexpr const char *kTable = "recovery_test";
constexpr const char *kIndex = "idx";

using helios::storage::wal::Wal;
using helios::storage::wal::WalScanResult;

// The commit path is what query layer traffic takes, and what it writes
// to the log is only observable after the instance that wrote it is gone:
// the log is held under an exclusive lock while a Database is open.
class RecoveryTest : public ::testing::Test {
 protected:
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
    const bool committed = db.Commit({}, {{kTable, key, value}}, {}, {},
                                     helios::storage::CommitDurability::kSync);
    db.ReleaseThreadEpoch();
    return committed;
  }

  static bool CommitWriteWithIndexEntry(helios::storage::Database &db,
                                        const std::string &key,
                                        const std::string &value,
                                        const std::string &secondary_key) {
    const bool committed = db.Commit({}, {{kTable, key, value}},
                                     {{kTable, kIndex, secondary_key, key}}, {},
                                     helios::storage::CommitDurability::kSync);
    db.ReleaseThreadEpoch();
    return committed;
  }

  static helios::storage::ReadResult Read(helios::storage::Database &db,
                                          const std::string &key) {
    auto result = db.Read(kTable, key);
    db.ReleaseThreadEpoch();
    return result;
  }

  std::string root_;
  std::string work_dir_;
};

TEST_F(RecoveryTest, ALoggedWriteCarriesTheUnlockedTid) {
  {
    auto config = MakeConfig(Recovery::kOff);
    helios::storage::Database db(config);
    db.CreateTable(kTable);
    ASSERT_TRUE(db.CreateSecondaryIndex(
        kTable, kIndex, helios::storage::IndexConstraint::kNone));
    ASSERT_TRUE(CommitWriteWithIndexEntry(db, "k", "v1", "s"));
  }

  Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kWalCapacityBytes);
  auto scan = wal.ScanAndRepair();
  ASSERT_EQ(scan.status, WalScanResult::Status::kOk);

  bool seen_row = false;
  bool seen_index_entry = false;
  for (const auto &record : scan.records) {
    for (const auto &write : record.writes) {
      if (write.index_name.empty()) {
        if (write.key != "k") continue;
        seen_row = true;
        EXPECT_EQ(write.buffer, "v1");
      } else {
        if (write.key != "s") continue;
        seen_index_entry = true;
        EXPECT_EQ(write.secondary_primary_key, "k");
      }
      // An odd tid is a write lock held by a transaction that no longer
      // exists; recovery installs it verbatim and every later reader and
      // writer of the key waits on it forever.
      EXPECT_EQ(write.transaction_id.tid % 2, 0u);
      EXPECT_NE(write.transaction_id.tid, 0u);
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
    db.CreateTable(kTable);
    ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  }

  // Guarded by the assertion above: a locked TID in the log makes the read
  // and the write below spin rather than fail.
  {
    Wal wal(work_dir_, helios::storage::wal::WalIo::Posix(), kWalCapacityBytes);
    auto scan = wal.ScanAndRepair();
    ASSERT_EQ(scan.status, WalScanResult::Status::kOk);
    bool seen = false;
    for (const auto &record : scan.records) {
      for (const auto &write : record.writes) {
        if (write.key != "k") continue;
        seen = true;
        ASSERT_EQ(write.transaction_id.tid % 2, 0u);
      }
    }
    ASSERT_TRUE(seen) << "the log holds no record of the key";
  }

  auto config = MakeConfig(Recovery::kOn);
  helios::storage::Database db(config);
  db.CreateTable(kTable);
  const auto recovered = Read(db, "k");
  EXPECT_TRUE(recovered.found);
  EXPECT_EQ(recovered.value, "v1");

  ASSERT_TRUE(CommitWrite(db, "k", "v2"));
  const auto rewritten = Read(db, "k");
  EXPECT_TRUE(rewritten.found);
  EXPECT_EQ(rewritten.value, "v2");
}

}  // namespace
