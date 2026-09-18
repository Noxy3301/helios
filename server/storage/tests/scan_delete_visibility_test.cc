// Modified for Helios.

/**
 * @file server/storage/tests/scan_delete_visibility_test.cc
 * That a scan does not return a key deleted in an earlier transaction.
 */

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {
constexpr const char *kTable = "users";
}  // namespace

class ScanDeleteVisibilityTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.work_dir = "./helios_scan_delete_visibility_test_logs";
    std::filesystem::remove_all(config_.work_dir);
    db_ = std::make_unique<helios::storage::Database>(config_);
    ASSERT_TRUE(TestHelper::CreateTable(*db_, kTable));
  }
};

TEST_F(ScanDeleteVisibilityTest, ScanShouldExcludeDeletedKeys) {
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{kTable, "alice", TestHelper::Pack<int>(1)},
             {kTable, "bob", TestHelper::Pack<int>(2)},
             {kTable, "carol", TestHelper::Pack<int>(3)}}));

  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, "bob"));

  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "carol");
  // bob is deleted and carol is the exclusive upper bound.
  ASSERT_EQ(rows.size(), size_t(1));
  EXPECT_EQ(rows[0].first, "alice");
  EXPECT_EQ(TestHelper::Unpack<int>(rows[0].second), 1);
}
