/**
 * @file server/storage/tests/pax_table_test.cc
 * Typed PAX cells whose declared width does not match their type.
 */

#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"
#include "pax/table.h"
#include "storage/pax.h"

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
  return group.ScatterRow(slot, reinterpret_cast<const std::byte *>(row.data()),
                          row.size());
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
  schema.table_name = "t";
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

  PaxGroup group(schema, nullptr);
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

  PaxGroup group(schema, nullptr);
  const std::string row = PackRow({std::string(1, '\0'), "7"});
  ASSERT_TRUE(Scatter(group, 0, row));
  EXPECT_EQ(Gather(group, 0, row.size()), row);
}

}  // namespace
