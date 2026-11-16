/**
 * @file server/storage/tests/typed_cell_test.cc
 * The cell helpers of common/typed_cell.h: a 4-byte cell sign-extends, and a
 * DATE cell formats as MySQL's DATE val_str text.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>

#include "common/typed_cell.h"

namespace {

TEST(TypedCellTest, FourByteCellsSignExtend) {
  for (const int32_t v : {0, -1, INT32_MIN, INT32_MAX}) {
    EXPECT_EQ(helios::rpc::load_cell(&v, sizeof(v)), v);
  }
  for (const int64_t v : {int64_t{-1}, INT64_MIN, INT64_MAX}) {
    EXPECT_EQ(helios::rpc::load_cell(&v, sizeof(v)), v);
  }
}

TEST(TypedCellTest, DateCellsFormatAsDateText) {
  for (const auto &[ymd, text] :
       {std::pair{0, "0000-00-00"}, std::pair{10000101, "1000-01-01"},
        std::pair{99991231, "9999-12-31"}, std::pair{20240015, "2024-00-15"},
        std::pair{20240230, "2024-02-30"}}) {
    std::string out;
    helios::rpc::format_date(ymd, out);
    EXPECT_EQ(out, text);
  }
}

}  // namespace
