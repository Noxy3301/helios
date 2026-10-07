// Modified for Helios.

/**
 * @file server/storage/tests/scan_secondary_index_test.cc
 * Scanning a secondary index: order, bounds, and the keys an insert or a
 * delete adds or removes.
 */

#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/index.h"

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

  // Reverse walks the (secondary key, primary key) pairs backwards.
  EXPECT_EQ(
      TestHelper::ScanIndex(*db_, "users", "group_index", "g1", "g3", 0, true),
      (SecondaryScanRows{{"g2", "user3"}, {"g1", "user2"}, {"g1", "user1"}}));
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

TEST_F(ScanSecondaryIndexTest, ManyRowsPerValue) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "group_index",
                                        IndexConstraint::kNone));

  // Four values of 100 rows each, interleaved by primary key and fed in
  // descending order. Fixed-width keys keep one value's pairs apart.
  SecondaryScanRows all;
  std::vector<TestHelper::RowWrite> rows;
  std::vector<TestHelper::IndexOp> ops;
  for (int i = 399; i >= 0; --i) {
    const std::string pk = "u" + std::to_string(1000 + i);
    const std::string sk = "g" + std::to_string(i % 4);
    rows.push_back({"users", pk, pk});
    ops.push_back({"users", "group_index", sk, pk});
    all.emplace_back(sk, pk);
  }
  ASSERT_TRUE(TestHelper::CommitWrites(*db_, rows, ops));
  std::sort(all.begin(), all.end());
  const SecondaryScanRows g1_g2(all.begin() + 100, all.begin() + 300);
  const SecondaryScanRows g2(all.begin() + 200, all.begin() + 300);

  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "group_index", "g1",
                                  TestHelper::prefix_end("g2")),
            g1_g2);
  // A start above g1 leaves every g1 pair out.
  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "group_index",
                                  TestHelper::prefix_end("g1"), "g3"),
            g2);
  EXPECT_EQ(
      TestHelper::ScanIndex(*db_, "users", "group_index", "g1", "g3", 0, true),
      SecondaryScanRows(g1_g2.rbegin(), g1_g2.rend()));

  // A limit counts pairs, not values, in either direction.
  EXPECT_EQ(
      TestHelper::ScanIndex(*db_, "users", "group_index", "g1", "g3", 150),
      SecondaryScanRows(g1_g2.begin(), g1_g2.begin() + 150));
  EXPECT_EQ(TestHelper::ScanIndex(*db_, "users", "group_index", "g1", "g3", 150,
                                  true),
            SecondaryScanRows(g1_g2.rbegin(), g1_g2.rbegin() + 150));

  // Removing half of g2's pairs leaves the other half and the other values.
  std::vector<TestHelper::IndexOp> removals;
  std::vector<std::string> kept;
  for (size_t i = 0; i < g2.size(); ++i) {
    if (i % 2 == 0) {
      removals.push_back({"users", "group_index", "g2", g2[i].second, true});
    } else {
      kept.push_back(g2[i].second);
    }
  }
  ASSERT_TRUE(TestHelper::CommitWrites(*db_, {}, removals));
  EXPECT_EQ(TestHelper::ReadIndex(*db_, "users", "group_index", "g2"), kept);
  EXPECT_EQ(TestHelper::ReadIndex(*db_, "users", "group_index", "g1").size(),
            100u);
}

TEST_F(ScanSecondaryIndexTest, APairPastTheKeyLimitIsRefused) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "name_index", IndexConstraint::kNone));

  // A non-unique record's key is both keys; a scan copies at most 255 bytes.
  const std::string secondary_key(200, 'a');
  const std::string primary_key(55, 'p');
  std::string reason;
  EXPECT_FALSE(TestHelper::Commit(
      *db_, {}, {}, {{"users", "name_index", secondary_key, primary_key + "p"}},
      {}, reason));
  EXPECT_EQ(reason, "secondary_key_too_long");

  // A pair of exactly 255 bytes is stored and scanned back.
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", primary_key, "v"}},
      {{"users", "name_index", secondary_key, primary_key}}));
  EXPECT_EQ(TestHelper::ReadIndex(*db_, "users", "name_index", secondary_key),
            std::vector<std::string>{primary_key});
}
