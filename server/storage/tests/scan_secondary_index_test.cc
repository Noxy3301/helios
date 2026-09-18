// Modified for Helios.

/**
 * @file server/storage/tests/scan_secondary_index_test.cc
 * Scanning a secondary index: order, bounds, and the keys an insert or a
 * delete adds or removes.
 */

#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lineairdb/config.h"
#include "lineairdb/database.h"
#include "lineairdb/index.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {
using helios::storage::IndexConstraint;
using SecondaryScanRows = std::vector<std::pair<std::string, std::string>>;
}  // namespace

class ScanSecondaryIndexTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.work_dir = "./helios_scan_secondary_index_test_logs";
    std::filesystem::remove_all(config_.work_dir);
    db_ = std::make_unique<helios::storage::Database>(config_);
  }
};

TEST_F(ScanSecondaryIndexTest, DeleteAndScan) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "alpha_index",
                                        IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice"},
       {"users", "user2", "Bob"},
       {"users", "user3", "Carol"}},
      {{"users", "alpha_index", "a", "user1", false},
       {"users", "alpha_index", "b", "user2", false},
       {"users", "alpha_index", "c", "user3", false}}));

  // The scan range is half-open: c is the exclusive upper bound.
  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "alpha_index", "a", "c"),
            (SecondaryScanRows{{"a", "user1"}, {"b", "user2"}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "alpha_index", "b", "user2", true}}));
  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "alpha_index", "b").empty());

  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "alpha_index", "a", "c"),
            (SecondaryScanRows{{"a", "user1"}}));
}

TEST_F(ScanSecondaryIndexTest, IncludeInsertedKeys) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "name_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}},
                               {{"users", "name_index", "alice", "user1"}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user2", "Bob"},
       {"users", "user3", "Carol"},
       {"users", "user4", "Erin"}},
      {{"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false},
       {"users", "name_index", "erin", "user4", false}}));

  // erin is the exclusive upper bound.
  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "name_index", "alice", "erin"),
            (SecondaryScanRows{
                {"alice", "user1"}, {"bob", "user2"}, {"carol", "user3"}}));
}

TEST_F(ScanSecondaryIndexTest, KeyOrder) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "name_index", IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", "user1", "Alice"}, {"users", "user4", "Diana"}},
      {{"users", "name_index", "alice", "user1", false},
       {"users", "name_index", "diana", "user4", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user2", "Bob"},
       {"users", "user3", "Carol"},
       {"users", "user5", "Erin"}},
      {{"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false},
       {"users", "name_index", "erin", "user5", false}}));

  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "name_index", "alice", "erin"),
            (SecondaryScanRows{{"alice", "user1"},
                               {"bob", "user2"},
                               {"carol", "user3"},
                               {"diana", "user4"}}));
}

TEST_F(ScanSecondaryIndexTest, ReverseScan) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "group_index",
                                        IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice"},
       {"users", "user2", "Bob"},
       {"users", "user3", "Carol"}},
      {{"users", "group_index", "g1", "user2", false},
       {"users", "group_index", "g1", "user1", false},
       {"users", "group_index", "g2", "user3", false}}));

  // Reverse walks the secondary keys backwards; the primary keys under one
  // secondary key keep their stored ascending order.
  EXPECT_EQ(
      TestHelper::ScanIndex(*db_, "users", "group_index", "g1", "g3", 0, true),
      (SecondaryScanRows{{"g2", "user3"}, {"g1", "user1"}, {"g1", "user2"}}));
}

TEST_F(ScanSecondaryIndexTest, StopScanning) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "name_index", IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice"},
       {"users", "user4", "Diana"},
       {"users", "user6", "Frank"}},
      {{"users", "name_index", "alice", "user1", false},
       {"users", "name_index", "diana", "user4", false},
       {"users", "name_index", "frank", "user6", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user2", "Bob"},
       {"users", "user3", "Carol"},
       {"users", "user5", "Erin"}},
      {{"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false},
       {"users", "name_index", "erin", "user5", false}}));

  // The row limit caps the scan at carol, well inside the range.
  EXPECT_EQ(
      TestHelper::ScanIndex(*db_, "users", "name_index", "alice", "zzz", 3),
      (SecondaryScanRows{
          {"alice", "user1"}, {"bob", "user2"}, {"carol", "user3"}}));
}

TEST_F(ScanSecondaryIndexTest, ExcludeDeletedKeys) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "name_index", IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_,
      {{"users", "user1", "Alice"},
       {"users", "user2", "Bob"},
       {"users", "user3", "Carol"}},
      {{"users", "name_index", "alice", "user1", false},
       {"users", "name_index", "bob", "user2", false},
       {"users", "name_index", "carol", "user3", false}}));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "name_index", "bob", "user2", true}}));

  // bob is deleted, carol is the exclusive upper bound.
  EXPECT_EQ(
      TestHelper::ScanIndex(*db_, "users", "name_index", "alice", "carol"),
      (SecondaryScanRows{{"alice", "user1"}}));
}
