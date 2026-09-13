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
 * @file server/storage/tests/masstree_index_test.cc
 * The shared index tree on its own: put, get, insert-if-absent, and
 * concurrent inserters of the same key.
 */

#include "index/masstree_index.h"

#include <atomic>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "index/reaper.h"
#include "pax/table.h"
#include "util/epoch.h"
#include "util/epoch_framework.h"
#include "util/spdlog.h"

TEST(MasstreeIndexTest, Instantiate) {
  ASSERT_NO_THROW(helios::storage::index::MasstreeIndex table);
}

TEST(MasstreeIndexTest, Put) {
  helios::storage::index::MasstreeIndex table;
  table.Put("alice", helios::storage::DataItem{});
}

TEST(MasstreeIndexTest, Get) {
  helios::storage::index::MasstreeIndex table;
  ASSERT_EQ(nullptr, table.Get("alice"));
  table.Put("alice", {});
  ASSERT_NE(nullptr, table.Get("alice"));
}

TEST(MasstreeIndexTest, GetOrInsert) {
  helios::storage::index::MasstreeIndex table;
  ASSERT_NE(nullptr, table.GetOrInsert("alice"));
}

TEST(MasstreeIndexTest, ConcurrentInserting) {
  std::vector<std::thread> threads;
  helios::storage::index::MasstreeIndex table;

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

TEST(MasstreeIndexTest, ConcurrentPutSameKey) {
  std::vector<std::thread> threads;
  helios::storage::index::MasstreeIndex table;

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

TEST(MasstreeIndexTest, Scan) {
  helios::storage::index::MasstreeIndex table;
  table.Put("alice", {});
  table.Put("bob", {});
  table.Put("carol", {});

  // Scan is half-open: carol is the exclusive upper bound.
  ASSERT_EQ(size_t(2),
            table.Scan("alice", "carol", [](auto) { return false; }));
  // A callback that cancels stops the walk at the first key.
  ASSERT_EQ(size_t(1), table.Scan("alice", "carol", [](auto) { return true; }));
}

TEST(MasstreeIndexTest, TremendousPut) {
  std::vector<std::thread> threads;
  helios::storage::index::MasstreeIndex table;

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

TEST(MasstreeIndexTest, TremendousGetAndPut) {
  std::vector<std::thread> threads;
  helios::storage::index::MasstreeIndex table;

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

TEST(MasstreeIndexTest, ReverseScanCountsOnlyTheKeysItEmits) {
  helios::storage::index::MasstreeIndex table;
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

TEST(MasstreeIndexTest, ConcurrentGetOrInsertReturnsTheSameEntry) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  std::vector<DataItem *> results(16);
  std::atomic<size_t> ready{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> threads;

  // Race insertions of one absent key, then compare the returned identities.
  for (size_t i = 0; i < results.size(); ++i) {
    threads.emplace_back([&tree, &results, &ready, &start, i]() {
      ready.fetch_add(1);
      while (!start.load()) std::this_thread::yield();
      results[i] = tree.GetOrInsert("shared");
      index::MasstreeReleaseThreadEpoch();
    });
  }
  while (ready.load() != results.size()) std::this_thread::yield();
  start.store(true);
  for (auto &thread : threads) thread.join();

  DataItem *item = tree.Get("shared");
  ASSERT_NE(nullptr, item);
  for (auto *result : results) EXPECT_EQ(item, result);
  EXPECT_EQ(item, tree.GetOrInsert("shared"));
  index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIndexTest, PrimaryEntryKeepsItsPaxTable) {
  using namespace helios::storage;
  pax::TableSchema schema;
  schema.field_max_bytes = {1, 32};
  pax::PaxTable store(schema);
  index::MasstreeIndex tree;
  tree.SetPaxTable(&store);

  DataItem *item = tree.GetOrInsert("row");
  ASSERT_NE(nullptr, item);
  EXPECT_FALSE(item->HasRow());
  ASSERT_TRUE(item->AllocateSlot());
  EXPECT_EQ(1u, store.slots_allocated());
  EXPECT_EQ(store.group(0), item->pax_group());
  EXPECT_EQ(item, tree.GetOrInsert("row"));
  index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIndexTest, ReaperPreservesReusedSecondaryEntry) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  index::Reaper reaper;
  const TransactionId deleted{10, 2};
  DataItem *item = tree.GetOrInsert("secondary");
  item->transaction_id.store(deleted);
  reaper.Enqueue(tree, "secondary", *item, deleted);

  // A delete cannot be purged until a full epoch has elapsed.
  reaper.Reap(11);
  EXPECT_EQ(item, tree.Get("secondary"));

  // A later insertion reuses the slot and publishes a newer TID.
  item->SetPrimaryKeys({"primary"});
  item->transaction_id.store(TransactionId{10, 4});
  reaper.Reap(12);
  EXPECT_EQ(item, tree.Get("secondary"));
  EXPECT_TRUE(item->IsLive());

  // Deleting that last posting allows the same tree to purge the slot.
  item->SetPrimaryKeys({});
  const TransactionId deleted_again{12, 6};
  item->transaction_id.store(deleted_again);
  reaper.Enqueue(tree, "secondary", *item, deleted_again);
  reaper.Reap(14);
  EXPECT_EQ(nullptr, tree.Get("secondary"));
  index::MasstreeReleaseThreadEpoch();
}

TEST(MasstreeIndexTest, PurgeRejectsAReplacementEntry) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  DataItem *old_item = tree.GetOrInsert("key");
  tree.Put("key", DataItem{});
  DataItem *replacement = tree.Get("key");
  ASSERT_NE(old_item, replacement);

  EXPECT_FALSE(tree.Purge("key", *old_item, TransactionId{10, 4}));
  EXPECT_EQ(replacement, tree.Get("key"));
  index::MasstreeReleaseThreadEpoch();
}
