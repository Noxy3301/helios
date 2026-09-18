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

// Modified for Helios.

/**
 * @file server/storage/tests/create_secondary_index_test.cc
 * Declaring secondary indexes: key types, several per table, duplicates,
 * a table that does not exist, and the constraint a name stays bound to.
 */

#include <filesystem>
#include <memory>

#include "helios/config.h"
#include "helios/database.h"
#include "helios/index.h"

#include "db_helper.h"
#include "gtest/gtest.h"
#include "table/table.h"

namespace {
using helios::storage::IndexConstraint;
}  // namespace
class CreateSecondaryIndexTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<helios::storage::Database>(config_);
  }
};

TEST_F(CreateSecondaryIndexTest, CreateNonUniqueIndex) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));
}

TEST_F(CreateSecondaryIndexTest, CreateMultipleSecondaryIndexes) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "name_index", IndexConstraint::kNone));
  ASSERT_TRUE(db_->CreateSecondaryIndex("users", "created_at_index",
                                        IndexConstraint::kNone));
}

TEST_F(CreateSecondaryIndexTest, CreateDuplicateSecondaryIndex) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));
  ASSERT_TRUE(
      db_->CreateSecondaryIndex("users", "age_index", IndexConstraint::kNone));
  ASSERT_FALSE(db_->CreateSecondaryIndex("users", "age_index",
                                         IndexConstraint::kUnique));
}

TEST_F(CreateSecondaryIndexTest, CreateSecondaryIndexOnNonExistentTable) {
  ASSERT_FALSE(db_->CreateSecondaryIndex("non_existent_table", "index",
                                         IndexConstraint::kNone));
}

TEST(SecondaryIndexConstraintTest, GetOrCreateRefusesADifferentConstraint) {
  helios::storage::Table table("users");
  const auto unique = IndexConstraint::kUnique;
  const auto none = IndexConstraint::kNone;

  helios::storage::index::SecondaryIndex *index =
      table.GetOrCreateSecondaryIndex("age_index", none);
  ASSERT_NE(index, nullptr);

  EXPECT_EQ(table.GetOrCreateSecondaryIndex("age_index", unique), nullptr);
  EXPECT_EQ(table.GetOrCreateSecondaryIndex("age_index", none), index);
}
