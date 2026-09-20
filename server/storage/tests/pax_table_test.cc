/**
 * @file server/storage/tests/pax_table_test.cc
 * Typed PAX cells whose declared width does not match their type, and the
 * column statistics a write feeds.
 */

#include <deque>
#include <string>
#include <string_view>
#include <vector>

#include "helios/pax.h"

#include "gtest/gtest.h"
#include "index/data_item.h"
#include "pax/table.h"

namespace {

using helios::storage::pax::FieldType;
using helios::storage::pax::PaxGroup;
using helios::storage::pax::TableSchema;

// One field in row format: the length-prefix width, the little-endian length
// and the payload, or the empty marker.
void PackField(std::string &out, std::string_view payload) {
  if (payload.empty()) {
    out.push_back(static_cast<char>(0xFF));
    return;
  }
  uint32_t prefix = 0;
  for (size_t len = payload.size(); len > 0; len /= 256) prefix++;
  out.push_back(static_cast<char>(prefix));
  for (uint32_t i = 0; i < prefix; i++) {
    out.push_back(static_cast<char>((payload.size() >> (8 * i)) & 0xFF));
  }
  out.append(payload);
}

std::string PackRow(const std::vector<std::string> &fields) {
  std::string row;
  for (const auto &field : fields) PackField(row, field);
  return row;
}

bool Scatter(PaxGroup &group, uint32_t slot, const std::string &row) {
  helios::storage::pax::Row unpacked;
  if (!helios::storage::pax::unpack_row(
          group.schema(), reinterpret_cast<const std::byte *>(row.data()),
          row.size(), unpacked))
    return false;
  group.ScatterRow(slot, unpacked);
  return true;
}

std::string Gather(const PaxGroup &group, uint32_t slot, size_t expected_size) {
  std::string out(expected_size, '\0');
  const size_t written = group.GatherRow(
      slot, reinterpret_cast<std::byte *>(out.data()), expected_size);
  out.resize(written);
  return out;
}

// The group PaxTable hands out first, holding slot 0.
PaxGroup &FirstGroup(helios::storage::pax::PaxTable &store) {
  return *store.AllocateSlot().first;
}

TableSchema MakeSchema(std::vector<uint32_t> widths,
                       std::vector<FieldType> types) {
  TableSchema schema;

  schema.field_max_bytes = std::move(widths);
  schema.field_type = std::move(types);
  schema.field_scale.assign(schema.field_type.size(), 0);
  return schema;
}

// A type whose declared width is not the width that type stores would be
// gathered by reading past the cell, into the next slot's length prefix.
TEST(PaxTableTest, TooNarrowTypedFieldStaysVerbatim) {
  const TableSchema schema =
      MakeSchema({1, 2}, {FieldType::kUntyped, FieldType::kInt32});
  EXPECT_EQ(schema.type_of(1), FieldType::kUntyped);

  helios::storage::pax::PaxTable store(schema);
  PaxGroup &group = FirstGroup(store);
  const std::string first = PackRow({std::string(1, '\0'), "42"});
  const std::string second = PackRow({std::string(1, '\0'), "99"});
  ASSERT_TRUE(Scatter(group, 0, first));
  ASSERT_TRUE(Scatter(group, 1, second));

  EXPECT_EQ(Gather(group, 0, first.size()), first);
  EXPECT_EQ(Gather(group, 1, second.size()), second);
}

// field_type is allowed to be shorter than the field count; the fields it
// does not reach are untyped rather than read past its end.
TEST(PaxTableTest, ShortTypeVectorLeavesTheRestUntyped) {
  const TableSchema schema = MakeSchema({1, 4}, {FieldType::kUntyped});
  EXPECT_EQ(schema.type_of(1), FieldType::kUntyped);

  helios::storage::pax::PaxTable store(schema);
  PaxGroup &group = FirstGroup(store);
  const std::string row = PackRow({std::string(1, '\0'), "7"});
  ASSERT_TRUE(Scatter(group, 0, row));
  EXPECT_EQ(Gather(group, 0, row.size()), row);
}

TEST(PaxTableTest, ScattersAndGathersFiveHundredColumns) {
  std::vector<uint32_t> widths(513, 1);
  widths[0] = 64;  // Null flags for 512 nullable data columns.
  const TableSchema schema = MakeSchema(widths, {});
  helios::storage::pax::PaxTable store(schema);
  PaxGroup &group = FirstGroup(store);
  std::vector<std::string> fields(513);
  fields[0] = std::string(64, '\0');
  for (size_t i = 1; i < fields.size(); i++) {
    fields[i] = std::string(1, static_cast<char>('a' + i % 26));
  }
  const auto row = PackRow(fields);
  ASSERT_TRUE(Scatter(group, 0, row));
  EXPECT_EQ(Gather(group, 0, row.size()), row);
}

TEST(PaxTableTest, CopyStaysWithinTheReadersBufferAfterRowGrowth) {
  const TableSchema schema = MakeSchema({1, 64}, {});
  helios::storage::pax::PaxTable store(schema);
  helios::storage::DataItem item;
  helios::storage::pax::Row unpacked;
  const auto small = PackRow({std::string(1, '\0'), "a"});
  const auto large = PackRow({std::string(1, '\0'), std::string(64, 'b')});
  ASSERT_TRUE(helios::storage::pax::unpack_row(
      schema, reinterpret_cast<const std::byte *>(small.data()), small.size(),
      unpacked));
  ASSERT_TRUE(item.AllocateSlot(store));
  item.InstallRow(unpacked, 1);

  // A reader sizes its output, then a writer grows the row before the copy.
  const size_t capacity = item.size();
  const std::byte marker{0x5a};
  std::vector<std::byte> buffer(large.size() + 8, marker);
  ASSERT_TRUE(helios::storage::pax::unpack_row(
      schema, reinterpret_cast<const std::byte *>(large.data()), large.size(),
      unpacked));
  item.InstallRow(unpacked, 1);
  const size_t copied = item.GatherInto(buffer.data(), capacity);
  EXPECT_LE(copied, capacity);
  bool guard_intact = true;
  for (size_t i = capacity; i < buffer.size(); ++i) {
    if (buffer[i] != marker) guard_intact = false;
  }
  EXPECT_TRUE(guard_intact);

  // Deletion between allocation and copying also keeps the same bound.
  item.DeleteRow(1);
  EXPECT_LE(item.GatherInto(buffer.data(), capacity), capacity);
}

helios::storage::DataItem &Install(
    std::deque<helios::storage::DataItem> &items,
    helios::storage::pax::PaxTable &store, const TableSchema &schema,
    const std::string &row) {
  helios::storage::pax::Row unpacked;
  EXPECT_TRUE(helios::storage::pax::unpack_row(
      schema, reinterpret_cast<const std::byte *>(row.data()), row.size(),
      unpacked));
  helios::storage::DataItem &item = items.emplace_back();
  EXPECT_TRUE(item.AllocateSlot(store));
  item.InstallRow(unpacked, 1);
  return item;
}

// The range covers every value written, and deleting the one row that holds
// the maximum leaves it wide: a planner reads it as a bound, not as the set
// of values present.
TEST(PaxTableTest, ColumnRangeCoversWrittenValues) {
  TableSchema schema =
      MakeSchema({1, 4, 8, 2}, {FieldType::kUntyped, FieldType::kInt32,
                                FieldType::kDecimal64, FieldType::kUntyped});
  schema.field_scale = {0, 0, 2, 0};
  helios::storage::pax::PaxTable store(schema);
  std::deque<helios::storage::DataItem> items;
  const std::string nulls(1, '\0');

  int64_t lo = 0, hi = 0;
  EXPECT_FALSE(helios::storage::pax::ColumnRange(&store, 1, &lo, &hi));

  Install(items, store, schema, PackRow({nulls, "-5", "1.25", "ab"}));
  Install(items, store, schema, PackRow({nulls, "7", "-9.00", "zz"}));

  ASSERT_TRUE(helios::storage::pax::ColumnRange(&store, 1, &lo, &hi));
  EXPECT_EQ(lo, -5);
  EXPECT_EQ(hi, 7);
  ASSERT_TRUE(helios::storage::pax::ColumnRange(&store, 2, &lo, &hi));
  EXPECT_EQ(lo, -900);  // scaled by 10^2
  EXPECT_EQ(hi, 125);
  EXPECT_FALSE(helios::storage::pax::ColumnRange(&store, 3, &lo, &hi));
  EXPECT_FALSE(helios::storage::pax::ColumnRange(&store, 9, &lo, &hi));

  // A row installed into a second group widens the same range.
  for (uint32_t i = 2; i < PaxGroup::kRows; i++) {
    Install(items, store, schema, PackRow({nulls, "0", "0.00", "x"}));
  }
  helios::storage::DataItem &peak =
      Install(items, store, schema, PackRow({nulls, "100", "0.00", "x"}));
  ASSERT_GE(helios::storage::pax::GroupCount(&store), 2u);
  ASSERT_TRUE(helios::storage::pax::ColumnRange(&store, 1, &lo, &hi));
  EXPECT_EQ(hi, 100);

  // 100 lives in that one row, and 7 is the highest value left after it goes.
  peak.DeleteRow(1);
  ASSERT_TRUE(helios::storage::pax::ColumnRange(&store, 1, &lo, &hi));
  EXPECT_EQ(lo, -5);
  EXPECT_EQ(hi, 100);
}

// The distinct estimate counts the values written, within the sketch's error,
// and an empty table still reports what it once held.
TEST(PaxTableTest, ColumnDistinctEstimatesWrittenValues) {
  TableSchema schema =
      MakeSchema({1, 4}, {FieldType::kUntyped, FieldType::kInt32});
  helios::storage::pax::PaxTable store(schema);
  std::deque<helios::storage::DataItem> items;
  const std::string nulls(1, '\0');

  uint64_t ndv = 0;
  EXPECT_FALSE(helios::storage::pax::ColumnDistinct(&store, 1, &ndv));

  // 2,000 distinct values, each written twice.
  constexpr int kDistinct = 2000;
  for (int pass = 0; pass < 2; pass++) {
    for (int i = 0; i < kDistinct; i++) {
      Install(items, store, schema, PackRow({nulls, std::to_string(i)}));
    }
  }
  ASSERT_TRUE(helios::storage::pax::ColumnDistinct(&store, 1, &ndv));
  // 1,024 registers put the standard error near 3%; allow four times that.
  EXPECT_GT(ndv, kDistinct * 0.88);
  EXPECT_LT(ndv, kDistinct * 1.12);

  // Every row goes, and the estimate stays where the writes put it.
  for (helios::storage::DataItem &item : items) item.DeleteRow(1);
  uint64_t after = 0;
  ASSERT_TRUE(helios::storage::pax::ColumnDistinct(&store, 1, &after));
  EXPECT_EQ(after, ndv);

  EXPECT_FALSE(helios::storage::pax::ColumnDistinct(&store, 0, &ndv));
}

}  // namespace
