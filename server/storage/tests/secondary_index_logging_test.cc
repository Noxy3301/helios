/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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
 * @file server/storage/tests/secondary_index_logging_test.cc
 * That secondary index entries survive recovery without a checkpoint, and the
 * timing the log records for them.
 */

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <msgpack.hpp>
#include <set>
#include <string>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"

#include "db_helper.h"
#include "gtest/gtest.h"
#include "spdlog/spdlog.h"
#include "wal/logger.h"
#include "wal/wal.h"

namespace {

struct SecondaryLogStats {
  size_t secondary_entry_count = 0;
  size_t primary_keys_count = 0;
  size_t primary_keys_bytes = 0;
};

size_t GetLogDirectorySize(const helios::storage::Config &conf) {
  namespace fs = std::filesystem;
  size_t size = 0;
  for (const auto &entry : fs::directory_iterator(conf.work_dir)) {
    if (entry.path().filename().generic_string().find("working") !=
        std::string::npos)
      continue;
    size += fs::file_size(entry.path());
  }
  return size;
}

/**
 * @brief Reads the log's newest epoch.
 *
 * The caller must have destroyed the Database first: scanning opens the log and
 * truncates an incomplete tail, which would corrupt a log the flusher is still
 * appending to.
 */
SecondaryLogStats GetSecondaryIndexLogStatsForLatestEpoch(
    const helios::storage::Config &conf) {
  namespace fs = std::filesystem;
  SecondaryLogStats stats{};
  const bool log_exists = fs::exists(fs::path(conf.work_dir) / "wal.log");
  EXPECT_TRUE(log_exists) << conf.work_dir << ": no log to read";
  if (!log_exists) return stats;

  // Read the log through the writer's own format rather than re-deriving the
  // frame format here.
  helios::storage::wal::Wal wal(conf.work_dir);
  const auto scan = wal.Scan();
  EXPECT_EQ(scan.status, helios::storage::wal::WalScanResult::Status::kOk);
  if (scan.status != helios::storage::wal::WalScanResult::Status::kOk) {
    return stats;
  }

  helios::storage::EpochNumber max_epoch = 0;
  for (const auto &record : scan.records) {
    if (record.epoch > max_epoch) max_epoch = record.epoch;
  }

  for (const auto &record : scan.records) {
    if (record.epoch != max_epoch) continue;
    for (const auto &write : record.writes) {
      if (write.index_name.empty()) continue;
      stats.secondary_entry_count++;
      stats.primary_keys_count += write.primary_keys.size();
      for (const auto &pk : write.primary_keys) {
        stats.primary_keys_bytes += pk.size();
      }
    }
  }

  return stats;
}

std::string MakeFixedPrimaryKey(size_t index) {
  char buffer[17];
  std::snprintf(buffer, sizeof(buffer), "pk%014zu", index);
  return std::string(buffer, 16);
}

std::vector<std::string> MakePrimaryKeys(size_t count, size_t offset = 0) {
  std::vector<std::string> keys;
  keys.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    keys.emplace_back(MakeFixedPrimaryKey(offset + i));
  }
  return keys;
}

}  // namespace

class SecondaryIndexLoggingTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;

  void SetUp() override {
    spdlog::set_level(spdlog::level::info);
    std::filesystem::remove_all("helios_wal");
    config_.enable_recovery = true;
    db_ = std::make_unique<helios::storage::Database>(config_);
    TestHelper::CreateTable(*db_, "users");
    spdlog::set_level(spdlog::level::info);
  }
};

TEST_F(SecondaryIndexLoggingTest,
       SecondaryIndexDeltaLoggingAvoidsFullPrimaryKeyList) {
  helios::storage::Config config = db_->GetConfig();
  config.enable_recovery = false;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<helios::storage::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const size_t secondary_keys = 9;
  const size_t primary_keys_per_secondary = 300;
  std::vector<std::string> index_keys;
  index_keys.reserve(secondary_keys);
  for (size_t i = 0; i < secondary_keys; ++i) {
    index_keys.emplace_back("age:" + std::to_string(30 + i));
  }

  TestHelper::CreateTable(*db_, table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(
      table_name, index_name, helios::storage::IndexConstraint::kNone));

  // Preload 9 secondary keys, each with 300 primary keys (16 bytes each).
  {
    std::vector<TestHelper::RowWrite> writes;
    std::vector<TestHelper::IndexOp> index_ops;
    for (size_t s = 0; s < secondary_keys; ++s) {
      for (size_t i = 0; i < primary_keys_per_secondary; ++i) {
        const size_t pk_index = s * primary_keys_per_secondary + i;
        const std::string primary_key = MakeFixedPrimaryKey(pk_index);
        writes.push_back({table_name, primary_key, "value_" + primary_key});
        index_ops.push_back(
            {table_name, index_name, index_keys[s], primary_key, false});
      }
    }
    ASSERT_TRUE(TestHelper::CommitWrites(*db_, writes, index_ops));
  }

  // Trigger a single transaction that updates all 9 secondary keys.
  {
    std::vector<TestHelper::RowWrite> writes;
    std::vector<TestHelper::IndexOp> index_ops;
    for (size_t s = 0; s < secondary_keys; ++s) {
      const size_t pk_index = secondary_keys * primary_keys_per_secondary + s;
      const std::string primary_key = MakeFixedPrimaryKey(pk_index);
      writes.push_back({table_name, primary_key, "value_" + primary_key});
      index_ops.push_back(
          {table_name, index_name, index_keys[s], primary_key, false});
    }
    ASSERT_TRUE(TestHelper::CommitWrites(*db_, writes, index_ops));
  }

  // Close the database before reading the log: the destructor drains the
  // flusher, and scanning a log that is still being appended to would truncate
  // a frame in flight.
  db_.reset(nullptr);

  const auto stats = GetSecondaryIndexLogStatsForLatestEpoch(config);
  ASSERT_GT(stats.secondary_entry_count, 0u);
  EXPECT_EQ(stats.primary_keys_count, 0u);
  std::cout << "[SecondaryIndexLogStats] secondary_entries="
            << stats.secondary_entry_count
            << " secondary_pk_count=" << stats.primary_keys_count
            << " secondary_pk_bytes=" << stats.primary_keys_bytes << std::endl;
}

TEST_F(SecondaryIndexLoggingTest, RecoveryKeepsLastSecondaryDeltaWithSameTid) {
  ASSERT_TRUE(db_->CreateSecondaryIndex(
      "users", "idx", helios::storage::IndexConstraint::kNone));
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {{"users", "a", "value_a"}, {"users", "b", "value_b"}},
      {{"users", "idx", "keep", "a", false},
       {"users", "idx", "drop", "b", false}}));

  // Changes to the same secondary entry in one commit share a TID.
  ASSERT_TRUE(TestHelper::CommitWrites(
      *db_, {},
      {{"users", "idx", "drop", "a", false},
       {"users", "idx", "drop", "a", true},
       {"users", "idx", "keep", "a", true},
       {"users", "idx", "keep", "a", false}}));
  EXPECT_EQ(std::vector<std::string>({"b"}),
            TestHelper::ReadIndex(*db_, "users", "idx", "drop"));
  EXPECT_EQ(std::vector<std::string>({"a"}),
            TestHelper::ReadIndex(*db_, "users", "idx", "keep"));

  db_.reset();
  db_ = std::make_unique<helios::storage::Database>(config_);
  EXPECT_EQ(std::vector<std::string>({"b"}),
            TestHelper::ReadIndex(*db_, "users", "idx", "drop"));
  EXPECT_EQ(std::vector<std::string>({"a"}),
            TestHelper::ReadIndex(*db_, "users", "idx", "keep"));
}

TEST_F(SecondaryIndexLoggingTest, RecoveryWithSecondaryIndexWithoutCheckpoint) {
  helios::storage::Config config = db_->GetConfig();
  config.enable_recovery = true;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<helios::storage::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:30";
  const size_t primary_key_count = 300;
  const auto primary_keys = MakePrimaryKeys(primary_key_count);

  TestHelper::CreateTable(*db_, table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(
      table_name, index_name, helios::storage::IndexConstraint::kNone));

  {
    std::vector<TestHelper::RowWrite> writes;
    std::vector<TestHelper::IndexOp> index_ops;
    for (const auto &primary_key : primary_keys) {
      writes.push_back({table_name, primary_key, "value_" + primary_key});
      index_ops.push_back(
          {table_name, index_name, index_key, primary_key, false});
    }
    ASSERT_TRUE(TestHelper::CommitWrites(*db_, writes, index_ops));
  }

  db_.reset(nullptr);
  db_ = std::make_unique<helios::storage::Database>(config);

  const auto results =
      TestHelper::ReadIndex(*db_, table_name, index_name, index_key);
  const std::set<std::string> recovered(results.begin(), results.end());
  ASSERT_EQ(recovered.size(), primary_keys.size());
  for (const auto &expected : primary_keys) {
    ASSERT_TRUE(recovered.count(expected));
  }
}

TEST_F(SecondaryIndexLoggingTest, SecondaryIndexAddTimingRecorded) {
  helios::storage::Config config = db_->GetConfig();
  config.enable_recovery = false;
  // What this test reports per transaction is how many bytes of log one
  // secondary-index write costs, and it reads that from the file's size. A
  // preallocated log holds its size constant, which would report zero for
  // every transaction, so this one log grows as it is written.
  config.wal_initial_capacity_bytes = 0;

  db_.reset(nullptr);
  std::filesystem::remove_all(config.work_dir);
  db_ = std::make_unique<helios::storage::Database>(config);

  const std::string table_name = "users";
  const std::string index_name = "age_index";
  const std::string index_key = "age:30";
  const size_t initial_primary_keys = 300;
  const size_t iterations = 10;

  TestHelper::CreateTable(*db_, table_name);
  ASSERT_TRUE(db_->CreateSecondaryIndex(
      table_name, index_name, helios::storage::IndexConstraint::kNone));

  // Preload 300 primary keys for the same secondary key.
  {
    std::vector<TestHelper::RowWrite> writes;
    std::vector<TestHelper::IndexOp> index_ops;
    for (size_t i = 0; i < initial_primary_keys; ++i) {
      const std::string primary_key = MakeFixedPrimaryKey(i);
      writes.push_back({table_name, primary_key, "val_" + primary_key});
      index_ops.push_back(
          {table_name, index_name, index_key, primary_key, false});
    }
    ASSERT_TRUE(TestHelper::CommitWrites(*db_, writes, index_ops));
  }

  std::vector<long long> durations_us;
  std::vector<size_t> log_delta_bytes;
  durations_us.reserve(iterations);
  log_delta_bytes.reserve(iterations);

  // Measure adding one primary key to the same secondary key.
  for (size_t i = 0; i < iterations; ++i) {
    std::string commit_reason;
    const std::string primary_key =
        MakeFixedPrimaryKey(initial_primary_keys + i);
    const std::string value = "val_" + primary_key;
    const auto size_before = GetLogDirectorySize(config);

    const auto start = std::chrono::steady_clock::now();
    const bool committed = TestHelper::CommitRows(
        *db_, {}, {{table_name, primary_key, TestHelper::Row(value)}},
        {{table_name, index_name, index_key, primary_key, false}}, {},
        commit_reason);
    const auto end = std::chrono::steady_clock::now();
    ASSERT_TRUE(committed);

    const auto elapsed =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    durations_us.push_back(elapsed);

    const auto size_after = GetLogDirectorySize(config);
    log_delta_bytes.push_back(
        size_after >= size_before ? size_after - size_before : 0);

    // Cleanup to keep the secondary key size stable for the next iteration.
    ASSERT_TRUE(TestHelper::CommitWrites(
        *db_, {{table_name, primary_key, "", helios::storage::RowOp::kDelete}},
        {{table_name, index_name, index_key, primary_key, true}}));
  }

  std::vector<long long> sorted = durations_us;
  std::sort(sorted.begin(), sorted.end());
  const auto median = sorted[sorted.size() / 2];

  std::cout << "[SecondaryIndexTiming] iterations=" << iterations
            << " pk_bytes=16"
            << " initial_primary_keys=" << initial_primary_keys
            << " median_us=" << median << " samples_us=[";
  for (size_t i = 0; i < durations_us.size(); ++i) {
    if (i) std::cout << ",";
    std::cout << durations_us[i];
  }
  std::cout << "] log_delta_bytes=[";
  for (size_t i = 0; i < log_delta_bytes.size(); ++i) {
    if (i) std::cout << ",";
    std::cout << log_delta_bytes[i];
  }
  std::cout << "]" << std::endl;
}
