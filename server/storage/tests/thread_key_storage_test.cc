/**
 * @file server/storage/tests/thread_key_storage_test.cc
 * Slot takeover after thread exit, and distinct slots for live threads across
 * blocks.
 */

#include "util/thread_key_storage.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

using helios::storage::ThreadKeyStorage;

TEST(ThreadKeyStorageTest, AThreadTakesOverTheSlotOfAnExitedThread) {
  ThreadKeyStorage<std::atomic<unsigned>> slots;
  std::atomic<unsigned> *first = nullptr;
  std::thread([&] { first = slots.Get<unsigned>([] { return 7; }); }).join();

  std::atomic<unsigned> *second = nullptr;
  std::thread([&] { second = slots.Get<unsigned>([] { return 9; }); }).join();
  EXPECT_EQ(second, first);
  EXPECT_EQ(second->load(), 7u);

  unsigned count = 0;
  slots.ForEach([&](std::atomic<unsigned> *) { ++count; });
  EXPECT_EQ(count, 1u);
}

TEST(ThreadKeyStorageTest, LiveThreadsGetDistinctSlotsAcrossBlocks) {
  ThreadKeyStorage<std::atomic<unsigned>> slots;
  std::promise<void> release;
  const auto released = release.get_future().share();
  std::atomic<unsigned> entered{0};
  std::vector<std::thread> threads;
  for (unsigned i = 1; i <= 80; ++i) {
    threads.emplace_back([&, i] {
      slots.Get<unsigned>([i] { return i; });
      entered.fetch_add(1);
      released.wait();
    });
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (entered.load() != threads.size() &&
         std::chrono::steady_clock::now() < deadline) {
    slots.ForEach([](std::atomic<unsigned> *value) {
      EXPECT_GE(value->load(), 1u);
      EXPECT_LE(value->load(), 80u);
    });
  }
  EXPECT_EQ(entered.load(), threads.size());
  unsigned count = 0;
  unsigned sum = 0;
  slots.ForEach([&](std::atomic<unsigned> *value) {
    ++count;
    sum += value->load();
  });
  EXPECT_EQ(count, 80u);
  EXPECT_EQ(sum, 80u * 81u / 2u);
  release.set_value();
  for (auto &thread : threads) thread.join();
}
