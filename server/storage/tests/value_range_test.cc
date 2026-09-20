/**
 * @file server/storage/tests/value_range_test.cc
 * The column range a reader folds back together out of its per-thread copies.
 */

#include "pax/value_range.h"

#include <cstdint>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace {

using helios::storage::pax::ValueRange;

// The fold has to reach every copy, and every thread writes to its own. More
// threads than copies puts two of them on one copy, where the compare
// exchange is what keeps the value.
TEST(ValueRangeTest, CoversValuesEveryThreadAdds) {
  ValueRange range;
  const size_t writers = ValueRange::kCopies + 4;
  std::vector<std::thread> threads;
  threads.reserve(writers);
  for (size_t i = 0; i < writers; i++) {
    threads.emplace_back([&range, i] {
      // Values straddle zero, so a copy left at one sentinel is visible as a
      // bound that lost a side.
      range.add(static_cast<int64_t>(i) - 8);
    });
  }
  for (std::thread &writer : threads) writer.join();

  int64_t lo = 0, hi = 0;
  ASSERT_TRUE(range.read(&lo, &hi));
  EXPECT_EQ(lo, -8);
  EXPECT_EQ(hi, static_cast<int64_t>(writers) - 9);
}

// A reader folding while writers run may miss a value not yet added, and may
// not report one narrower than what the writers finished adding.
TEST(ValueRangeTest, FoldNeverNarrowsUnderConcurrentWrites) {
  ValueRange range;
  constexpr int64_t kPerWriter = 2000;
  const size_t writers = 8;
  std::vector<std::thread> threads;
  threads.reserve(writers);
  for (size_t i = 0; i < writers; i++) {
    threads.emplace_back([&range, i] {
      for (int64_t v = 0; v < kPerWriter; v++) {
        range.add(static_cast<int64_t>(i) * kPerWriter + v);
      }
    });
  }

  int64_t seen_lo = INT64_MAX, seen_hi = INT64_MIN;
  for (int pass = 0; pass < 2000; pass++) {
    int64_t lo = 0, hi = 0;
    if (!range.read(&lo, &hi)) continue;
    // Every read has to cover what the reads before it covered.
    EXPECT_LE(lo, seen_lo);
    EXPECT_GE(hi, seen_hi);
    seen_lo = lo;
    seen_hi = hi;
  }
  for (std::thread &writer : threads) writer.join();

  int64_t lo = 0, hi = 0;
  ASSERT_TRUE(range.read(&lo, &hi));
  EXPECT_EQ(lo, 0);
  EXPECT_EQ(hi, static_cast<int64_t>(writers) * kPerWriter - 1);
}

}  // namespace
