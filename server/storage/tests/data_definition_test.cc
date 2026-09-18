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
 * @file server/storage/tests/data_definition_test.cc
 * Tables as units of naming and of concurrency control: the same key in
 * two tables is two rows.
 */

#include <atomic>
#include <filesystem>
#include <memory>
#include <thread>

#include "lineairdb/config.h"
#include "lineairdb/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"

class DataDefinitionTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<helios::storage::Database>(config_);
  }
};

TEST_F(DataDefinitionTest, CreateTable) {
  bool success = TestHelper::CreateTable(*db_, "users");
  ASSERT_TRUE(success);
  bool created = TestHelper::CreateTable(*db_, "users");
  ASSERT_FALSE(created);
}

TEST_F(DataDefinitionTest, ReadWrite) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));

  ASSERT_TRUE(TestHelper::Write<int>(*db_, "users", "user1", 42));

  auto data = TestHelper::Read<int>(*db_, "users", "user1");
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), 42);
}

TEST_F(DataDefinitionTest, SeparateTableKeySpaces) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "accounts"));

  std::atomic<bool> tx1_ready = false;
  std::atomic<bool> tx2_ready = false;

  std::thread thread1([&]() {
    tx1_ready = true;
    while (!tx2_ready) std::this_thread::yield();  // Wait for tx2 to be ready
    EXPECT_TRUE(TestHelper::CommitWrites(
        *db_, {{"users", "user1", TestHelper::Pack<int>(42)},
               {"users", "user1_only_users", TestHelper::Pack<int>(42)}}));
  });

  std::thread thread2([&]() {
    tx2_ready = true;
    while (!tx1_ready) std::this_thread::yield();  // Wait for tx1 to be ready
    // The key tx1 writes into users never appears in accounts.
    EXPECT_FALSE(TestHelper::Read<int>(*db_, "accounts", "user1_only_users")
                     .has_value());
    EXPECT_TRUE(TestHelper::Write<int>(*db_, "accounts", "user1", 100));
  });

  thread1.join();
  thread2.join();

  // Check Results
  auto data = TestHelper::Read<int>(*db_, "users", "user1");
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), 42);

  data = TestHelper::Read<int>(*db_, "accounts", "user1");
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), 100);
}

TEST_F(DataDefinitionTest, WriteSameKeyIntoTwoTables) {
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "users"));
  ASSERT_TRUE(TestHelper::CreateTable(*db_, "accounts"));

  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", "user1", TestHelper::Pack<int>(42)},
             {"accounts", "user1", TestHelper::Pack<int>(100)}}));

  // Check Results
  auto data = TestHelper::Read<int>(*db_, "users", "user1");
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), 42);

  data = TestHelper::Read<int>(*db_, "accounts", "user1");
  ASSERT_TRUE(data.has_value());
  ASSERT_EQ(data.value(), 100);
}
