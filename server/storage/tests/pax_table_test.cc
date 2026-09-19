/**
 * @file server/storage/tests/pax_table_test.cc
 * Typed PAX cells whose declared width does not match their type.
 */

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

  PaxGroup group(schema);
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

  PaxGroup group(schema);
  const std::string row = PackRow({std::string(1, '\0'), "7"});
  ASSERT_TRUE(Scatter(group, 0, row));
  EXPECT_EQ(Gather(group, 0, row.size()), row);
}

TEST(PaxTableTest, Stores512DataColumns) {
  std::vector<uint32_t> widths(513, 1);
  widths[0] = 64;  // Null flags for 512 nullable data columns.
  const TableSchema schema = MakeSchema(widths, {});
  PaxGroup group(schema);
  std::vector<std::string> fields(513, "x");
  fields[0] = std::string(64, '\0');
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

}  // namespace
