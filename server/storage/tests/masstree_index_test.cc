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
 * Masstree insertion, scans, PAX slots, and deferred deletion.
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

namespace {

// A word a commit publishes: epoch, tid, and the absent bit.
helios::storage::Tidword make_tidword(helios::storage::EpochNumber epoch,
                                      uint32_t tid, bool absent) {
  helios::storage::Tidword word;
  word.epoch = epoch;
  word.tid = tid;
  word.latest = true;
  word.absent = absent;
  return word;
}

}  // namespace

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

TEST(MasstreeIndexTest, Scan) {
  helios::storage::index::MasstreeIndex table;
  table.Put("alice", {});
  table.Put("bob", {});
  table.Put("carol", {});

  // Scan is half-open: carol is the exclusive upper bound.
  ASSERT_EQ(size_t(2),
            table.Scan("alice", "carol", [](auto, auto &) { return false; }));
  // A callback that cancels stops the walk at the first key.
  ASSERT_EQ(size_t(1),
            table.Scan("alice", "carol", [](auto, auto &) { return true; }));
}

TEST(MasstreeIndexTest, ReverseScanCountsOnlyTheKeysItEmits) {
  helios::storage::index::MasstreeIndex table;
  for (const char *key : {"a", "b", "c", "d"}) table.Put(key, {});

  std::vector<std::string> seen;
  const size_t count =
      table.ScanReverse("b", std::optional<std::string_view>("d"),
                        [&](std::string_view key, helios::storage::DataItem &) {
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
      index::release_thread_epoch();
    });
  }
  while (ready.load() != results.size()) std::this_thread::yield();
  start.store(true);
  for (auto &thread : threads) thread.join();

  DataItem *item = tree.Get("shared");
  ASSERT_NE(nullptr, item);
  for (auto *result : results) EXPECT_EQ(item, result);
  EXPECT_EQ(item, tree.GetOrInsert("shared"));
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, PrimaryEntryTakesItsPaxSlotOnAllocation) {
  using namespace helios::storage;
  pax::TableSchema schema;
  schema.field_max_bytes = {1, 32};
  pax::PaxTable store(schema);
  index::MasstreeIndex tree;

  DataItem *item = tree.GetOrInsert("row");
  ASSERT_NE(nullptr, item);
  EXPECT_FALSE(item->IsLive());
  ASSERT_TRUE(item->AllocateSlot(store));
  EXPECT_EQ(1u, store.slots_allocated());
  EXPECT_EQ(store.group(0), item->pax_group());
  EXPECT_EQ(item, tree.GetOrInsert("row"));
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, ReaperPreservesReusedSecondaryEntry) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  index::Reaper reaper;
  const Tidword deleted = make_tidword(10, 2, /*absent=*/true);
  DataItem *item = tree.GetOrInsert("secondary");
  item->transaction_id.store(deleted);
  reaper.Enqueue(tree, "secondary", *item, deleted);

  // A delete cannot be purged until a full epoch has elapsed.
  reaper.Purge(9);
  EXPECT_EQ(item, tree.Get("secondary"));

  // A later insertion reuses the index record and publishes a newer TID.
  item->set_primary_keys({"primary"});
  item->transaction_id.store(make_tidword(10, 4, /*absent=*/false));
  reaper.Purge(10);
  EXPECT_EQ(item, tree.Get("secondary"));
  EXPECT_TRUE(item->IsLive());

  // Deleting that last posting allows the same tree to purge the record.
  item->set_primary_keys({});
  const Tidword deleted_again = make_tidword(12, 6, /*absent=*/true);
  item->transaction_id.store(deleted_again);
  reaper.Enqueue(tree, "secondary", *item, deleted_again);
  reaper.Purge(12);
  EXPECT_EQ(nullptr, tree.Get("secondary"));
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, PurgeRejectsAReplacementEntry) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  DataItem *old_item = tree.GetOrInsert("key");
  tree.Put("key", DataItem{});
  DataItem *replacement = tree.Get("key");
  ASSERT_NE(old_item, replacement);

  EXPECT_FALSE(
      tree.Purge("key", *old_item, make_tidword(10, 4, /*absent=*/true)));
  EXPECT_EQ(replacement, tree.Get("key"));
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, UnboundedReverseScanIncludesEveryKey) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  for (const char *key : {"", "a", "b"}) tree.GetOrInsert(key);
  std::vector<std::string> keys;
  const size_t count = tree.ScanReverse(
      "", std::nullopt, [&](std::string_view key, helios::storage::DataItem &) {
        keys.emplace_back(key);
        return false;
      });
  EXPECT_EQ(std::vector<std::string>({"b", "a", ""}), keys);
  EXPECT_EQ(3u, count);
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, ScanReturnsMatchingItemsInByteOrder) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  // Include empty, binary, prefix-related and maximum-length Masstree keys.
  const std::vector<std::string> keys = {"",
                                         std::string("\0", 1),
                                         "a",
                                         std::string("a\0", 2),
                                         std::string("a\xff", 2),
                                         "long-prefix-shared-a",
                                         "long-prefix-shared-b",
                                         std::string(1, '\xff'),
                                         std::string(255, '\xff')};
  for (const auto &key : keys) tree.GetOrInsert(key);

  std::vector<std::string> seen;
  auto collect = [&](std::string_view key, DataItem &item) {
    EXPECT_EQ(tree.Get(key), &item);
    seen.emplace_back(key);
    return false;
  };
  EXPECT_EQ(keys.size(), tree.Scan("", std::nullopt, collect));
  EXPECT_EQ(keys, seen);

  seen.clear();
  EXPECT_EQ(keys.size(), tree.ScanReverse("", std::nullopt, collect));
  EXPECT_EQ(std::vector<std::string>(keys.rbegin(), keys.rend()), seen);

  seen.clear();
  tree.ForEach(collect);
  EXPECT_EQ(keys, seen);
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, EmptyUpperBoundIsNotAnUnboundedScan) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  for (const char *key : {"", "a", "b"}) tree.GetOrInsert(key);
  auto unexpected = [](std::string_view, DataItem &) {
    ADD_FAILURE() << "An empty range must not invoke the callback";
    return true;
  };

  EXPECT_EQ(0u, tree.Scan("", std::string_view(), unexpected));
  EXPECT_EQ(0u, tree.ScanReverse("", std::string_view(), unexpected));
  EXPECT_EQ(0u, tree.Scan("a", "a", unexpected));
  EXPECT_EQ(0u, tree.ScanReverse("a", "a", unexpected));
  EXPECT_EQ(0u, tree.Scan("b", "a", unexpected));
  EXPECT_EQ(0u, tree.ScanReverse("b", "a", unexpected));
  index::release_thread_epoch();
}

TEST(MasstreeIndexTest, BoundedValueScanCountsTheCallbackThatStopsIt) {
  using namespace helios::storage;
  index::MasstreeIndex tree;
  for (const char *key : {"a", "b", "c", "d"}) tree.GetOrInsert(key);
  std::vector<std::string> seen;
  auto stop_after_two = [&](std::string_view key, DataItem &item) {
    EXPECT_EQ(tree.Get(key), &item);
    seen.emplace_back(key);
    return seen.size() == 2;
  };

  EXPECT_EQ(2u, tree.Scan("a", "d", stop_after_two));
  EXPECT_EQ(std::vector<std::string>({"a", "b"}), seen);
  seen.clear();
  EXPECT_EQ(2u, tree.ScanReverse("a", "d", stop_after_two));
  EXPECT_EQ(std::vector<std::string>({"c", "b"}), seen);
  index::release_thread_epoch();
}
