/**
 * @file server/storage/tests/epoch_framework_test.cc
 * The epoch thread's lifetime around Start() and destruction, and how it
 * serves a forced advance.
 */

#include "util/epoch_framework.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <thread>
#include <utility>

namespace {

constexpr auto kTestTimeout = std::chrono::seconds(5);

// Builds and destroys one framework that is never started.
void DestroyUnstarted(std::promise<void> done) {
  { helios::storage::epoch::Framework framework; }
  done.set_value();
}

}  // namespace

// The constructor parks the epoch thread on the start signal, so a framework that
// is never started still has a thread for the destructor to wake and join.
// The thread is detached because a destructor that does not return would
// otherwise hang this test instead of failing it.
TEST(EpochFrameworkTest, DestroyingAnUnstartedFrameworkReturns) {
  std::promise<void> destroyed;
  auto finished = destroyed.get_future();
  std::thread(DestroyUnstarted, std::move(destroyed)).detach();

  EXPECT_EQ(finished.wait_for(kTestTimeout), std::future_status::ready);
}

// A forced advance that a thread still in the previous epoch refuses stays
// pending: with a tick far longer than the test, the epoch holds while that
// thread stays and advances soon after it leaves, not at the next tick.
TEST(EpochFrameworkTest, RefusedForcedAdvanceRetriesBeforeTheNextTick) {
  helios::storage::epoch::Framework framework(10000);
  framework.Start();
  std::promise<helios::storage::EpochNumber> joined;
  std::promise<void> release;
  std::thread straggler([&] {
    joined.set_value(framework.Join());
    release.get_future().wait();
    framework.Leave();
  });
  const auto epoch = joined.get_future().get();

  // One request for two advances: the first passes the straggler's epoch, the
  // second needs it gone.
  framework.RequestEpochAdvance(epoch + 2);
  EXPECT_TRUE(framework.WaitEpoch(epoch + 1, kTestTimeout));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(framework.GetGlobalEpoch(), epoch + 1)
      << "an advance passed a thread still in the previous epoch";

  // Poll rather than wait: WaitEpoch would send a request of its own.
  release.set_value();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (framework.GetGlobalEpoch() < epoch + 2 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_GE(framework.GetGlobalEpoch(), epoch + 2)
      << "the refused advance was not retried before the next tick";
  straggler.join();
}
