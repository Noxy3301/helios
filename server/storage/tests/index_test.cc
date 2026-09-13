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
 * @file server/storage/tests/index_test.cc
 * Scanning the primary index: ordering, early stop, open upper bound, and
 * deletion.
 */

#include <filesystem>
#include <memory>

#include "lineairdb/config.h"
#include "lineairdb/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"

namespace {
constexpr const char *kTable = "users";
}  // namespace

class IndexTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    config_.enable_recovery = false;
    config_.work_dir = "./helios_index_test_logs";
    std::filesystem::remove_all(config_.work_dir);
    db_ = std::make_unique<helios::storage::Database>(config_);
    ASSERT_TRUE(TestHelper::CreateTable(*db_, kTable));

    ASSERT_TRUE(TestHelper::CommitWrites(
        *db_, {{kTable, "alice", TestHelper::Pack<int>(1)},
               {kTable, "bob", TestHelper::Pack<int>(2)},
               {kTable, "carol", TestHelper::Pack<int>(3)}}));
  }
};

TEST_F(IndexTest, Scan) {
  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "bob");
  ASSERT_EQ(size_t(1), rows.size());  // half-open: bob is excluded
  EXPECT_EQ(rows[0].first, "alice");
}

TEST_F(IndexTest, InvertedRange) {
  EXPECT_TRUE(TestHelper::Scan(*db_, kTable, "carol", "alice").empty());
}

TEST_F(IndexTest, AlphabeticalOrdering) {
  const auto rows = TestHelper::Scan(*db_, kTable, "carol", "zzz");
  ASSERT_EQ(size_t(1), rows.size());
  EXPECT_EQ(rows[0].first, "carol");
}

TEST_F(IndexTest, RowLimit) {
  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "carol", 1);
  ASSERT_EQ(size_t(1), rows.size());
  EXPECT_EQ(rows[0].first, "alice");
}

TEST_F(IndexTest, ScanToMaxKey) {
  const auto rows =
      TestHelper::Scan(*db_, kTable, "alice", TestHelper::kMaxKey);
  ASSERT_EQ(size_t(3), rows.size());
  EXPECT_EQ(rows[0].first, "alice");
  EXPECT_EQ(rows[1].first, "bob");
  EXPECT_EQ(rows[2].first, "carol");
}
