/**
 * @file server/storage/tests/epoch_framework_test.cc
 * The epoch thread's lifetime around Start() and destruction.
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
