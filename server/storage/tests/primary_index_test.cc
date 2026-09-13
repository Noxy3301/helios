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
 * @file server/storage/tests/primary_index_test.cc
 * The primary index on its own: put, get, insert-if-absent, and
 * concurrent inserters of the same key.
 */

#include "index/primary_index.h"

#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "util/epoch.h"
#include "util/epoch_framework.h"
#include "util/spdlog.h"

TEST(PrimaryIndexTest, Instantiate) {
  ASSERT_NO_THROW(helios::storage::index::PrimaryIndex table);
}

TEST(PrimaryIndexTest, Put) {
  helios::storage::index::PrimaryIndex table;
  table.Put("alice", helios::storage::DataItem{});
}

TEST(PrimaryIndexTest, Get) {
  helios::storage::index::PrimaryIndex table;
  ASSERT_EQ(nullptr, table.Get("alice"));
  table.Put("alice", {});
  ASSERT_NE(nullptr, table.Get("alice"));
}

TEST(PrimaryIndexTest, GetOrInsert) {
  helios::storage::index::PrimaryIndex table;
  ASSERT_NE(nullptr, table.GetOrInsert("alice"));
}

TEST(PrimaryIndexTest, ConcurrentInserting) {
  std::vector<std::thread> threads;
  helios::storage::index::PrimaryIndex table;

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() { table.Put(std::to_string(i), {}); });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  for (size_t i = 0; i < 10; i++) {
    ASSERT_NE(nullptr, table.Get(std::to_string(i)));
  }
}

TEST(PrimaryIndexTest, ConcurrentPutSameKey) {
  std::vector<std::thread> threads;
  helios::storage::index::PrimaryIndex table;

  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&]() { table.Put("alice", {}); });
  }
  for (auto &thread : threads) {
    thread.join();
  }
  bool some_item_were_inserted = false;
  auto *item = table.Get("alice");
  for (size_t i = 0; i < 10; i++) {
    if (item != nullptr) some_item_were_inserted = true;
  }

  ASSERT_TRUE(some_item_were_inserted);
}

TEST(PrimaryIndexTest, Scan) {
  helios::storage::index::PrimaryIndex table;
  table.Put("alice", {});
  table.Put("bob", {});
  table.Put("carol", {});

  // Scan is half-open: carol is the exclusive upper bound.
  ASSERT_EQ(size_t(2),
            table.Scan("alice", "carol", [](auto) { return false; }));
  // A callback that cancels stops the walk at the first key.
  ASSERT_EQ(size_t(1), table.Scan("alice", "carol", [](auto) { return true; }));
}

TEST(PrimaryIndexTest, TremendousPut) {
  std::vector<std::thread> threads;
  helios::storage::index::PrimaryIndex table;

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

TEST(PrimaryIndexTest, TremendousGetAndPut) {
  std::vector<std::thread> threads;
  helios::storage::index::PrimaryIndex table;

  constexpr size_t working_set_size = 8192;
  for (size_t i = 0; i < 10; i++) {
    threads.emplace_back([&, i]() {
      for (size_t j = i * working_set_size; j < (i + 1) * working_set_size;
           j++) {
        table.Get(std::to_string(j - working_set_size));
        table.Put(std::to_string(j), {});
      }
    });
  }
  for (auto &thread : threads) {
    thread.join();
  }
}

TEST(PrimaryIndexTest, ReverseScanCountsOnlyTheKeysItEmits) {
  helios::storage::index::PrimaryIndex table;
  for (const char *key : {"a", "b", "c", "d"}) table.Put(key, {});

  std::vector<std::string> seen;
  const size_t count = table.ScanReverse(
      "b", std::optional<std::string_view>("d"), [&](std::string_view key) {
        seen.emplace_back(key);
        return false;
      });
  EXPECT_EQ(std::vector<std::string>({"c", "b"}), seen);
  EXPECT_EQ(seen.size(), count);
}
