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
 * @file server/storage/tests/database_test.cc
 * The store as a whole: construction, reads and writes, scans, deletes,
 * and concurrent insertions.
 */

#include "storage/database.h"

#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db_helper.h"
#include "gtest/gtest.h"
#include "storage/config.h"

namespace {
constexpr const char *kTable = "users";
}  // namespace

class DatabaseTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<helios::storage::Database>(config_);
    ASSERT_TRUE(db_->CreateTable(kTable));
  }
};

TEST_F(DatabaseTest, Instantiate) {}

TEST_F(DatabaseTest, InstantiateWithConfig) {
  db_.reset(nullptr);
  helios::storage::Config conf;
  ASSERT_NO_THROW(db_ = std::make_unique<helios::storage::Database>(conf));
}

TEST_F(DatabaseTest, LargeSizeBuffer) {
  const std::string alice(2048, '\1');
  ASSERT_TRUE(TestHelper::Write(*db_, kTable, "alice", alice));

  const auto read = TestHelper::Read(*db_, kTable, "alice");
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read.value(), alice);
}

TEST_F(DatabaseTest, Scan) {
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{kTable, "alice", TestHelper::Pack<int>(1)},
             {kTable, "bob", TestHelper::Pack<int>(2)},
             {kTable, "carol", TestHelper::Pack<int>(3)}}));

  // Half-open: carol is the exclusive upper bound.
  const auto rows = TestHelper::Scan(*db_, kTable, "alice", "carol");
  ASSERT_EQ(rows.size(), size_t(2));
  EXPECT_EQ(rows[0].first, "alice");
  EXPECT_EQ(TestHelper::Unpack<int>(rows[0].second), 1);
  EXPECT_EQ(rows[1].first, "bob");
  EXPECT_EQ(TestHelper::Unpack<int>(rows[1].second), 2);

  const auto capped = TestHelper::Scan(*db_, kTable, "alice", "carol", 1);
  ASSERT_EQ(capped.size(), size_t(1));
  EXPECT_EQ(capped[0].first, "alice");
}

TEST_F(DatabaseTest, DeleteRemovesKeyAcrossTransactions) {
  const int value_of_alice = 123;
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "alice", value_of_alice));

  const auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
  ASSERT_TRUE(alice.has_value());
  ASSERT_EQ(value_of_alice, alice.value());

  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, "alice"));
  ASSERT_FALSE(TestHelper::Read<int>(*db_, kTable, "alice").has_value());
}

TEST_F(DatabaseTest, ThreadSafetyWrites) {
  constexpr int kValue = 0xBEEF;
  constexpr size_t kKeys = 11;

  std::vector<helios::storage::ExternalWriteEntry> writes;
  for (size_t idx = 0; idx < kKeys; idx++) {
    writes.push_back(
        {kTable, "alice" + std::to_string(idx), TestHelper::Pack<int>(kValue)});
  }

  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; i++) {
    threads.emplace_back([&]() {
      bool committed = false;
      for (size_t retry = 0; retry < 100 && !committed; retry++) {
        committed = TestHelper::CommitWrites(*db_, writes);
      }
      EXPECT_TRUE(committed);
    });
  }
  for (auto &thread : threads) thread.join();

  for (size_t idx = 0; idx < kKeys; idx++) {
    const auto alice =
        TestHelper::Read<int>(*db_, kTable, "alice" + std::to_string(idx));
    ASSERT_TRUE(alice.has_value());
    ASSERT_EQ(kValue, alice.value());
  }
}

TEST_F(DatabaseTest, PaxColumnSelectionPreservesNullEmptyAndHeapReads) {
  ASSERT_TRUE(db_->InstallPaxSchema(kTable, {1, 3, 3}));
  ASSERT_TRUE(db_->CreateSecondaryIndex(
      kTable, "name", helios::storage::IndexConstraint::kNone));

  // Row format: null flags, then two length-prefixed three-byte fields.
  const std::string null_flags("\1\1\0", 3);
  const std::string row = null_flags + "\1\3abc\1\3def";
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{kTable, "alice", row}}, {{kTable, "name", "a", "alice"}}));

  const std::vector<uint32_t> no_columns;
  const std::vector<uint32_t> second_column{1};
  const std::vector<const std::vector<uint32_t> *> selections{
      nullptr, &no_columns, &second_column};
  const std::vector<std::string> expected{
      row, null_flags + "\xff\xff", null_flags + "\xff\1\3def"};

  // Point, primary-range and secondary-range reads share column semantics.
  for (size_t i = 0; i < selections.size(); ++i) {
    const auto point = db_->Read(kTable, "alice", selections[i]);
    const auto primary =
        db_->Scan(kTable, "alice", "bob", 0, false, selections[i]);
    const auto secondary =
        db_->ScanIndex(kTable, "name", "a", "b", 0, false, selections[i]);
    db_->ReleaseThreadEpoch();

    ASSERT_TRUE(point.found);
    EXPECT_EQ(point.value, expected[i]);
    ASSERT_TRUE(primary.ok);
    ASSERT_EQ(primary.rows.size(), 1u);
    EXPECT_EQ(primary.rows[0].value, expected[i]);
    EXPECT_EQ(primary.rows[0].tid, point.tid);
    ASSERT_TRUE(secondary.ok);
    ASSERT_EQ(secondary.rows.size(), 1u);
    EXPECT_EQ(secondary.rows[0].value, expected[i]);
    EXPECT_EQ(secondary.rows[0].tid, point.tid);
  }

  // A field wider than its PAX cell falls back to a whole heap row.
  const std::string overflow = null_flags + "\1\4abcd\1\3def";
  ASSERT_TRUE(TestHelper::Write(*db_, kTable, "alice", overflow));
  for (const auto *columns : selections) {
    const auto point = db_->Read(kTable, "alice", columns);
    db_->ReleaseThreadEpoch();
    ASSERT_TRUE(point.found);
    EXPECT_EQ(point.value, overflow);
  }
}
