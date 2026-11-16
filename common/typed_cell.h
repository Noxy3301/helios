/**
 * @file common/typed_cell.h
 * The cell a typed PAX column carries in a row in place of its val_str text.
 * The query node converts a field to and from its cell; the storage copies
 * cells as bytes.
 */
#ifndef HELIOS_COMMON_TYPED_CELL_H
#define HELIOS_COMMON_TYPED_CELL_H

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

namespace helios::rpc {

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "cells are copied as host-order words");

// How a column is stored, with the values of helios::storage::pax::FieldType.
// A typed cell is cell_width(type) little-endian bytes.
enum class PaxType : uint8_t {
  kUntyped = 0,    // the val_str bytes themselves
  kInt32 = 1,      // int32
  kInt64 = 2,      // int64
  kDate = 3,       // YYYYMMDD as int32
  kDecimal64 = 4,  // value * 10^scale as int64
};

// Bytes of a cell of `type`, 0 for kUntyped.
constexpr uint32_t cell_width(PaxType type) {
  switch (type) {
    case PaxType::kInt32:
    case PaxType::kDate:
      return 4;
    case PaxType::kInt64:
    case PaxType::kDecimal64:
      return 8;
    default:
      return 0;
  }
}

// The value of a 4- or 8-byte cell at `cell`, a 4-byte one sign-extended.
inline int64_t load_cell(const void *cell, size_t width) {
  if (width == sizeof(int32_t)) {
    int32_t v;
    std::memcpy(&v, cell, sizeof(v));
    return v;
  }
  int64_t v;
  std::memcpy(&v, cell, sizeof(v));
  return v;
}

// Appends the "YYYY-MM-DD" text of the DATE cell value `ymd` (YYYYMMDD) to
// `out`, as MySQL's DATE val_str writes it.
inline void format_date(int64_t ymd, std::string &out) {
  char buf[16];  // an int32 YYYYMMDD prints in at most 15 characters
  const int n = std::snprintf(buf, sizeof(buf), "%04lld-%02lld-%02lld",
                              static_cast<long long>(ymd / 10000),
                              static_cast<long long>((ymd / 100) % 100),
                              static_cast<long long>(ymd % 100));
  if (n > 0) out.append(buf, static_cast<size_t>(n));
}

}  // namespace helios::rpc

#endif  // HELIOS_COMMON_TYPED_CELL_H
