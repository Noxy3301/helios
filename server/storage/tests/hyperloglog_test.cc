/**
 * @file server/storage/tests/hyperloglog_test.cc
 * The distinct-value sketch over the range of counts a column reaches.
 */

#include "pax/hyperloglog.h"

#include <cmath>
#include <cstdint>

#include "gtest/gtest.h"

namespace {

using helios::storage::pax::HyperLogLog;

uint64_t EstimateOf(const HyperLogLog &sketch) {
  uint64_t ndv = 0;
  EXPECT_TRUE(sketch.estimate(&ndv));
  return ndv;
}

// The estimate has to hold across the counts a column reaches, not only the
// one a single case happens to pick. 1,024 registers put the standard error
// near 3%, and the small-range correction takes over below 2,560, so the
// counts below sit on both sides of that point.
TEST(HyperLogLogTest, EstimatesAcrossMagnitudes) {
  for (const int64_t distinct : {int64_t{10}, int64_t{1000}, int64_t{100000}}) {
    HyperLogLog sketch;
    for (int64_t i = 0; i < distinct; i++) sketch.add(i);
    const uint64_t ndv = EstimateOf(sketch);
    const double relative =
        (static_cast<double>(ndv) - static_cast<double>(distinct)) / distinct;
    EXPECT_LT(std::abs(relative), 0.15)
        << "distinct=" << distinct << " estimate=" << ndv;
  }
}

// Neighbouring keys must not crowd into neighbouring registers. Values spaced
// by a power of two are what an unhashed index would collapse.
TEST(HyperLogLogTest, StridedValuesSpreadAcrossRegisters) {
  HyperLogLog sketch;
  constexpr int64_t kDistinct = 20000;
  for (int64_t i = 0; i < kDistinct; i++) sketch.add(i * 4096);
  const double relative =
      (static_cast<double>(EstimateOf(sketch)) - kDistinct) / kDistinct;
  EXPECT_LT(std::abs(relative), 0.15);
}

}  // namespace
