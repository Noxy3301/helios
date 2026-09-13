/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * @file server/storage/tests/manipulate_secondary_index_test.cc
 * Reading and writing through secondary indexes, including the moves and
 * removals an update makes.
 */

#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "db_helper.h"
#include "gtest/gtest.h"
#include "storage/config.h"
#include "storage/database.h"
#include "storage/index.h"

namespace {
using helios::storage::IndexConstraint;
}  // namespace
class ManipulateSecondaryIndexTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<helios::storage::Database>(config_);
  }
};

TEST_F(ManipulateSecondaryIndexTest, ReadWriteSecondaryIndex) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}},
                               {{"users", "age_index", "10", "user1"}}));

  const auto result = TestHelper::ReadIndex(*db_, "users", "age_index", "10");
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], "user1");
}

TEST_F(ManipulateSecondaryIndexTest, DuplicateAddDoesNotDuplicatePrimaryKey) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  // The repeated 30/user3 entry must not produce a duplicate primary key.
  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_,
                               {{"users", "user1", "Alice"},
                                {"users", "user2", "Bob"},
                                {"users", "user3", "Carol"}},
                               {{"users", "age_index", "25", "user1", false},
                                {"users", "age_index", "25", "user2", false},
                                {"users", "age_index", "30", "user3", false},
                                {"users", "age_index", "30", "user3", false}}));

  EXPECT_EQ(TestHelper::ReadIndex(*db_, "users", "age_index", "25").size(), 2u);
  EXPECT_EQ(TestHelper::ReadIndex(*db_, "users", "age_index", "30").size(), 1u);
}

TEST_F(ManipulateSecondaryIndexTest, ReadDataViaSecondaryIndex) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", "user1", "Alice"}, {"users", "user2", "Bob"}},
      {{"users", "age_index", "25", "user1", false},
       {"users", "age_index", "25", "user2", false}}));

  const auto primary_keys =
      TestHelper::ReadIndex(*db_, "users", "age_index", "25");
  ASSERT_EQ(primary_keys.size(), 2u);

  std::vector<std::string> names;
  for (const auto &primary_key : primary_keys) {
    const auto row = TestHelper::Read(*db_, "users", primary_key);
    ASSERT_TRUE(row.has_value());
    names.push_back(*row);
  }
  EXPECT_EQ(names, (std::vector<std::string>{"Alice", "Bob"}));
}

TEST_F(ManipulateSecondaryIndexTest, RemoveThenAddRelocatesMapping) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}},
                               {{"users", "age_index", "25", "user1"}}));

  // An update is the removal of the old entry and the add of the new one,
  // submitted together.
  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {},
                               {{"users", "age_index", "25", "user1", true},
                                {"users", "age_index", "30", "user1", false}}));

  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "age_index", "25").empty());
  const auto moved = TestHelper::ReadIndex(*db_, "users", "age_index", "30");
  ASSERT_EQ(moved.size(), 1u);
  EXPECT_EQ(moved[0], "user1");
}

TEST_F(ManipulateSecondaryIndexTest, ReAddExistingIndexEntryIsIdempotent) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}},
                               {{"users", "age_index", "25", "user1", false},
                                {"users", "age_index", "30", "user1", false}}));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {},
                               {{"users", "age_index", "25", "user1", true},
                                {"users", "age_index", "30", "user1", false}}));

  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "age_index", "25").empty());
  const auto moved = TestHelper::ReadIndex(*db_, "users", "age_index", "30");
  ASSERT_EQ(moved.size(), 1u);
  EXPECT_EQ(moved[0], "user1");
}

TEST_F(ManipulateSecondaryIndexTest, MissingDeleteDoesNotBlockInsert) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}}));

  // Removing an entry that was never there leaves the add unaffected.
  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {},
                               {{"users", "age_index", "99", "user1", true},
                                {"users", "age_index", "40", "user1", false}}));

  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "age_index", "99").empty());
  const auto inserted = TestHelper::ReadIndex(*db_, "users", "age_index", "40");
  ASSERT_EQ(inserted.size(), 1u);
  EXPECT_EQ(inserted[0], "user1");
}

TEST_F(ManipulateSecondaryIndexTest, DeleteSecondaryIndexRemovesPrimaryKey) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}},
                               {{"users", "age_index", "25", "user1"}}));
  ASSERT_EQ(TestHelper::ReadIndex(*db_, "users", "age_index", "25").size(), 1u);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "age_index", "25", "user1", true}}));

  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "age_index", "25").empty());
}

TEST_F(ManipulateSecondaryIndexTest, RemoveIndexEntryLeavesOtherPrimaryKeys) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_,
                               {{"users", "user1", "Alice"},
                                {"users", "user2", "Bob"},
                                {"users", "user3", "Carol"}},
                               {{"users", "age_index", "25", "user1", false},
                                {"users", "age_index", "25", "user2", false},
                                {"users", "age_index", "25", "user3", false}}));
  ASSERT_EQ(TestHelper::ReadIndex(*db_, "users", "age_index", "25").size(), 3u);

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {}, {{"users", "age_index", "25", "user2", true}}));

  const auto remaining =
      TestHelper::ReadIndex(*db_, "users", "age_index", "25");
  EXPECT_EQ(std::set<std::string>(remaining.begin(), remaining.end()),
            (std::set<std::string>{"user1", "user3"}));
}

TEST_F(ManipulateSecondaryIndexTest,
       MultipleUpdatesInSingleTransactionMaintainConsistency) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));

  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {{"users", "user1", "Alice"}},
                               {{"users", "age_index", "18", "user1"}}));

  // 18 -> 19 -> 20 in one request: the index ops are applied in order.
  ASSERT_TRUE(
      TestHelper::CommitWrites(*db_, {},
                               {{"users", "age_index", "18", "user1", true},
                                {"users", "age_index", "19", "user1", false},
                                {"users", "age_index", "19", "user1", true},
                                {"users", "age_index", "20", "user1", false}}));

  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "age_index", "18").empty());
  EXPECT_TRUE(TestHelper::ReadIndex(*db_, "users", "age_index", "19").empty());
  const auto current = TestHelper::ReadIndex(*db_, "users", "age_index", "20");
  ASSERT_EQ(current.size(), 1u);
  EXPECT_EQ(current[0], "user1");
}
