/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

// Modified for Helios.

/**
 * @file server/storage/tests/concurrent_create_table_test.cc
 * Table creation from several threads, across an epoch boundary.
 */

#include <array>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <thread>
#include <vector>

#include "helios/config.h"
#include "helios/database.h"

#include "gtest/gtest.h"

class ConcurrentCreateTableTest : public ::testing::Test {
 protected:
  helios::storage::Config config_;
  std::unique_ptr<helios::storage::Database> db_;
  virtual void SetUp() {
    std::filesystem::remove_all(config_.work_dir);
    config_.epoch_duration_ms = 100;
    db_ = std::make_unique<helios::storage::Database>(config_);
  }
};

// Holds its window open across several epochs and races CreateTable against
// the epoch tick.
TEST_F(ConcurrentCreateTableTest, ConcurrentCreateTableAcrossEpochs) {
  constexpr size_t kNumWorkers = 4;
  constexpr size_t kNumTables = 100;
  constexpr size_t kYieldInterval = 128;
  // Long enough for the epoch writer to tick across the race.
  constexpr size_t kNumEpochs = 3;

  std::atomic<bool> stop{false};
  // One name may be created once, whichever worker wins the race for it.
  std::vector<std::atomic<size_t>> created(kNumTables);
  for (auto &count : created) count.store(0);

  std::vector<std::thread> workers;
  for (size_t t = 0; t < kNumWorkers; ++t) {
    workers.emplace_back([&]() {
      size_t i = 0;
      while (!stop.load()) {
        const size_t slot = i % kNumTables;
        const std::string table_name = "table_" + std::to_string(slot);
        if (db_->CreateTable(table_name)) created[slot].fetch_add(1);
        ++i;
        if (i % kYieldInterval == 0) std::this_thread::yield();
      }
    });
  }

  std::this_thread::sleep_for(
      std::chrono::milliseconds(config_.epoch_duration_ms * kNumEpochs));

  stop.store(true);
  for (auto &w : workers) {
    w.join();
  }

  for (size_t slot = 0; slot < kNumTables; ++slot) {
    const std::string table_name = "table_" + std::to_string(slot);
    EXPECT_EQ(created[slot].load(), 1u) << table_name;
  }
}
