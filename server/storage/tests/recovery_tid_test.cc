/**
 * @file server/storage/tests/recovery_tid_test.cc
 * That the transaction id a recovered key carries admits the next write.
 */

#include <filesystem>
#include <string>

#include "gtest/gtest.h"
#include "storage/config.h"
#include "storage/database.h"

namespace {

constexpr const char *kTable = "recovery_tid_test";

helios::storage::Config MakeConfig() {
  helios::storage::Config config;
  config.epoch_duration_ms = 10;
  config.enable_recovery = true;
  config.work_dir = "./helios_recovery_tid_test_logs";
  return config;
}

// A write captures its log entry while the row lock is held.
// An entry persisted with the locked TID breaks recovery: the recovered
// row looks locked by a transaction that no longer exists, and every later
// access to the key spins or aborts forever.
TEST(RecoveryTidTest, ARecoveredKeyAcceptsTheNextWrite) {
  const auto config = MakeConfig();
  std::filesystem::remove_all(config.work_dir);

  {
    helios::storage::Database db(config);
    db.CreateTable(kTable);
    std::string reason;
    const bool committed =
        db.Commit({}, {{kTable, "alice", "v1"}}, {}, {},
                  helios::storage::CommitDurability::kSync, &reason);
    db.ReleaseThreadEpoch();
    ASSERT_TRUE(committed) << reason;
  }

  {
    helios::storage::Database db(config);
    // Recovery re-created the table, so this call is expected to find it.
    db.CreateTable(kTable);

    auto recovered = db.Read(kTable, "alice");
    db.ReleaseThreadEpoch();
    ASSERT_TRUE(recovered.found);
    EXPECT_EQ(recovered.tid % 2, 0u)
        << "the recovered TID still carries the lock bit";

    std::string reason;
    const bool committed =
        db.Commit({}, {{kTable, "alice", "v2"}}, {}, {},
                  helios::storage::CommitDurability::kSync, &reason);
    db.ReleaseThreadEpoch();
    EXPECT_TRUE(committed) << reason;
  }

  std::filesystem::remove_all(config.work_dir);
}

}  // namespace
