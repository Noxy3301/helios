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
 * @file server/storage/tests/durability_test.cc
 * Recovery from the log, and the difference the commit durability makes to
 * when a write is on the device.
 */

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>

#include "helios/config.h"
#include "helios/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"
#include "util/spdlog.h"

namespace {
constexpr const char *kTable = "users";

bool logged(const std::string &work_dir, const std::string &key) {
  std::ifstream file(work_dir + "/wal.log", std::ios::binary);
  const std::string bytes((std::istreambuf_iterator<char>(file)),
                          std::istreambuf_iterator<char>());
  return bytes.find(key) != std::string::npos;
}
}  // namespace

class DurabilityTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;

  /// Destroys the database and opens it again, which is what recovers it.
  void Restart(const helios::storage::Config &config) {
    db_.reset(nullptr);
    db_ = std::make_unique<helios::storage::Database>(config);
  }

  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.enable_recovery = true;
    db_ = std::make_unique<helios::storage::Database>(config_);
    ASSERT_TRUE(TestHelper::CreateTable(*db_, kTable));
  }
};

TEST_F(DurabilityTest, Recovery) {
  // Recovery logging is on by default.
  const helios::storage::Config config = db_->GetConfig();

  int initial_value = 1;
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "alice", initial_value));
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "bob", initial_value));

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    Restart(config);

    auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
    ASSERT_TRUE(alice.has_value());
    ASSERT_EQ(initial_value, alice.value());
    auto bob = TestHelper::Read<int>(*db_, kTable, "bob");
    ASSERT_TRUE(bob.has_value());
    ASSERT_EQ(initial_value, bob.value());
  }
}

TEST_F(DurabilityTest, RecoveryKeepsDeletedKeysAbsent) {
  // Recovery logging is on by default.
  const helios::storage::Config config = db_->GetConfig();

  int initial_value = 1;
  ASSERT_TRUE(TestHelper::Write<int>(*db_, kTable, "alice", initial_value));
  ASSERT_TRUE(TestHelper::Delete(*db_, kTable, "alice"));

  // Expect that recovery procedure has idempotence
  for (size_t i = 0; i < 3; i++) {
    Restart(config);

    auto alice = TestHelper::Read<int>(*db_, kTable, "alice");
    ASSERT_FALSE(alice.has_value());
  }
}

TEST_F(DurabilityTest, RecoveryLargeObject) {
  const helios::storage::Config config = db_->GetConfig();
  std::string initial_value(4096, 'a');
  ASSERT_TRUE(TestHelper::Write(*db_, kTable, "alice", initial_value));

  for (size_t i = 0; i < 3; i++) {
    Restart(config);

    auto alice = TestHelper::Read(*db_, kTable, "alice");
    ASSERT_TRUE(alice.has_value());
    ASSERT_EQ(initial_value, alice.value());
  }
}

TEST_F(DurabilityTest, RecoveryWithNamedTable) {
  const helios::storage::Config config = db_->GetConfig();
  const std::string table_name = "accounts";
  const std::string key = "user1";
  const int value = 12345;

  // 1. Create a table and write to it
  ASSERT_TRUE(TestHelper::CreateTable(*db_, table_name));
  ASSERT_TRUE(TestHelper::Write<int>(*db_, table_name, key, value));

  // 2. Restart DB to trigger recovery
  Restart(config);

  // 3. Verify data is recovered in the correct table
  auto data = TestHelper::Read<int>(*db_, table_name, key);
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), value);
}

// With an epoch timer far slower than the test, only a commit's own request
// closes an epoch: an Async commit makes none and leaves its record out of the
// log, and a Sync commit makes one and returns promptly with its record logged.
TEST(CommitDurabilityTest, SyncCommitClosesItsOwnEpoch) {
  constexpr size_t kEpochMs = 10000;
  constexpr long kPromptMs = 100;
  helios::storage::Config config;
  config.work_dir = "./helios_commit_policy_test_logs";
  std::filesystem::remove_all(config.work_dir);
  config.enable_recovery = false;
  config.epoch_duration_ms = kEpochMs;
  config.wal_initial_capacity_bytes = 1u << 20;

  {
    helios::storage::Database db(config);
    ASSERT_TRUE(TestHelper::CreateTable(db, kTable));

    const auto commit = [&db](const std::string &key,
                              helios::storage::CommitDurability durability) {
      std::string commit_reason;
      const auto started = std::chrono::steady_clock::now();
      const bool committed =
          TestHelper::CommitRows(db, {}, {{kTable, key, TestHelper::Row("v")}},
                                 {}, {}, commit_reason, durability);
      EXPECT_TRUE(committed);
      return std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::steady_clock::now() - started)
          .count();
    };

    EXPECT_LT(commit("lazy_key", helios::storage::CommitDurability::kAsync),
              kPromptMs);
    EXPECT_FALSE(logged(config.work_dir, "lazy_key"))
        << "an Async commit closed its epoch";

    EXPECT_LT(commit("durable_key", helios::storage::CommitDurability::kSync),
              kPromptMs)
        << "a Sync commit waited for the epoch timer";
    EXPECT_TRUE(logged(config.work_dir, "durable_key"))
        << "a Sync commit returned before its record was logged";
  }
  // After the database is destroyed: it holds the log open until then.
  std::filesystem::remove_all(config.work_dir);
}
