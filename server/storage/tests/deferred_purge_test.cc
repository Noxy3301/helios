/**
 * @file server/storage/tests/deferred_purge_test.cc
 * What a reader observes while a deleted slot waits for the reaper, and
 * what a re-insert of the same key does to a standing read.
 */

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/read.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {

constexpr const char *kTable = "purge_test";

helios::storage::Config MakeConfig(size_t epoch_duration_ms) {
  helios::storage::Config config;
  config.epoch_duration_ms = epoch_duration_ms;
  config.enable_recovery = false;
  config.work_dir = "./helios_deferred_purge_test_logs";
  std::filesystem::remove_all(config.work_dir);
  return config;
}

bool CommitWrite(helios::storage::Database &db, const std::string &key,
                 const std::string &value) {
  std::string commit_reason;
  const bool committed =
      db.Commit({}, {{kTable, key, TestHelper::Row(value)}}, {}, {},
                helios::storage::CommitDurability::kSync, commit_reason);
  db.ReleaseThreadEpoch();
  return committed;
}

bool CommitInsert(helios::storage::Database &db, const std::string &key,
                  const std::string &value, std::string &reason) {
  const bool committed = db.Commit(
      {},
      {{kTable, key, TestHelper::Row(value), helios::storage::RowOp::kInsert}},
      {}, {}, helios::storage::CommitDurability::kSync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

bool CommitDelete(helios::storage::Database &db, const std::string &key) {
  std::string commit_reason;
  const bool committed =
      db.Commit({}, {{kTable, key, "", helios::storage::RowOp::kDelete}}, {},
                {}, helios::storage::CommitDurability::kSync, commit_reason);
  db.ReleaseThreadEpoch();
  return committed;
}

helios::storage::ReadResult Read(helios::storage::Database &db,
                                 const std::string &key) {
  auto result = db.Read(kTable, key);
  db.ReleaseThreadEpoch();
  if (result.found) result.value = TestHelper::RowPayload(result.value);
  return result;
}

bool ValidateRead(helios::storage::Database &db,
                  const helios::storage::ReadResult &read,
                  const std::string &key, std::string &reason) {
  const bool committed =
      db.Commit({{kTable, key, read.tid, read.found}}, {}, {}, {},
                helios::storage::CommitDurability::kSync, reason);
  db.ReleaseThreadEpoch();
  return committed;
}

bool StartsWith(const std::string &value, const std::string &prefix) {
  return value.rfind(prefix, 0) == 0;
}

// The reaper runs on the epoch hook, so a purge needs epochs to pass with
// this thread outside of one.
void SleepAroundEpochRelease(helios::storage::Database &db,
                             std::chrono::milliseconds duration) {
  db.ReleaseThreadEpoch();
  std::this_thread::sleep_for(duration);
}

}  // namespace

TEST(DeferredPurgeTest, SameEpochDeleteReinsertInvalidatesStaleRead) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  const auto stale = Read(db, "k");
  ASSERT_TRUE(stale.found);
  ASSERT_EQ(stale.value, "v1");

  ASSERT_TRUE(CommitDelete(db, "k"));
  ASSERT_TRUE(CommitWrite(db, "k", "v2"));

  std::string reason;
  EXPECT_FALSE(ValidateRead(db, stale, "k", reason));
  EXPECT_TRUE(StartsWith(reason, "exact_read_tid_moved")) << reason;
}

TEST(DeferredPurgeTest, FoundReadAbortsAfterDeferredPurgeRemovesSlot) {
  auto config = MakeConfig(5);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  const auto stale = Read(db, "k");
  ASSERT_TRUE(stale.found);

  ASSERT_TRUE(CommitDelete(db, "k"));

  std::string reason;
  for (int i = 0; i < 200; ++i) {
    ASSERT_FALSE(ValidateRead(db, stale, "k", reason));
    if (StartsWith(reason, "exact_read_disappeared")) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  EXPECT_TRUE(StartsWith(reason, "exact_read_disappeared")) << reason;
}

TEST(DeferredPurgeTest, ReinsertBeforeReaperKeepsLiveRow) {
  auto config = MakeConfig(200);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));
  ASSERT_TRUE(CommitDelete(db, "k"));
  ASSERT_TRUE(CommitWrite(db, "k", "v2"));

  SleepAroundEpochRelease(db, std::chrono::milliseconds(600));

  const auto live = Read(db, "k");
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, AbsentReadStillAbortsWhenRowAppears) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  const auto absent = Read(db, "k");
  ASSERT_FALSE(absent.found);

  ASSERT_TRUE(CommitWrite(db, "k", "v1"));

  std::string reason;
  EXPECT_FALSE(ValidateRead(db, absent, "k", reason));
  EXPECT_TRUE(StartsWith(reason, "exact_read_appeared")) << reason;
}

TEST(DeferredPurgeTest, InsertAfterPurgeWaitSeesLiveRow) {
  std::string commit_reason;
  auto config = MakeConfig(5);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  const std::string key = "purged_then_reinserted_key";
  ASSERT_TRUE(CommitInsert(db, key, "v1", commit_reason));
  ASSERT_TRUE(CommitDelete(db, key));

  // Several epochs, so the reaper retires the tombstone's slot before the
  // insert below claims the key again.
  SleepAroundEpochRelease(db, std::chrono::milliseconds(200));

  ASSERT_TRUE(CommitInsert(db, key, "v2", commit_reason));
  const auto live = Read(db, key);
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, TwoInsertsOfOneKeyInARequestAreRefused) {
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  const std::string key = "twice_inserted_key";
  std::string reason;
  EXPECT_FALSE(db.Commit(
      {},
      {{kTable, key, TestHelper::Row("v1"), helios::storage::RowOp::kInsert},
       {kTable, key, TestHelper::Row("v2"), helios::storage::RowOp::kInsert}},
      {}, {}, helios::storage::CommitDurability::kSync, reason));
  db.ReleaseThreadEpoch();
  EXPECT_EQ(reason, helios::storage::kDuplicatePrimaryKeyAbortReason);
  EXPECT_FALSE(Read(db, key).found);

  // Deleted in between, the second insert is not a duplicate.
  reason.clear();
  EXPECT_TRUE(db.Commit(
      {},
      {{kTable, key, TestHelper::Row("v1"), helios::storage::RowOp::kInsert},
       {kTable, key, "", helios::storage::RowOp::kDelete},
       {kTable, key, TestHelper::Row("v2"), helios::storage::RowOp::kInsert}},
      {}, {}, helios::storage::CommitDurability::kSync, reason));
  db.ReleaseThreadEpoch();
  const auto live = Read(db, key);
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v2");
}

TEST(DeferredPurgeTest, InsertOntoALiveKeyIsRefused) {
  std::string commit_reason;
  auto config = MakeConfig(100);
  helios::storage::Database db(config);
  ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

  const std::string key = "live_key";
  ASSERT_TRUE(CommitInsert(db, key, "v1", commit_reason));

  std::string reason;
  EXPECT_FALSE(CommitInsert(db, key, "v2", reason));
  EXPECT_EQ(reason, helios::storage::kDuplicatePrimaryKeyAbortReason);

  const auto live = Read(db, key);
  EXPECT_TRUE(live.found);
  EXPECT_EQ(live.value, "v1");
}
