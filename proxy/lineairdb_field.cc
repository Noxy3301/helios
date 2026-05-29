#include "lineairdb_field.hh"

#include <cassert>

/**
 * LineairDBField method definitions
 */

char LineairDBField::convert_numeric_to_a_byte(const size_t num) const {
  return convert_numeric_to_bytes(num)[0];
}

std::string LineairDBField::convert_numeric_to_bytes(const size_t num) const {
  size_t byteSizeOfNum = calculate_minimum_byte_size_required(num);
  std::string byteSequence;
  byteSequence.reserve(byteSizeOfNum);
  // Encode in little-endian order to match convert_bytes_to_numeric().
  for (size_t i = 0; i < byteSizeOfNum; i++) {
    byteSequence.push_back(static_cast<char>((num >> (CHAR_BIT * i)) & 0xFF));
  }
  return byteSequence;
}

size_t LineairDBField::convert_bytes_to_numeric(
    std::variant<const std::byte *, const uchar *> bytes,
    const size_t length) const {
  size_t n = 0;
  for (size_t i = 0; i < length; i++) {
    std::visit(
        [&](auto &&oneByte) {
          n = n | static_cast<uchar>(oneByte[i]) << CHAR_BIT * i;
        },
        bytes);
  }
  return n;
}

std::string LineairDBField::get_null_field() const {
  return get_lineairdb_field();
}

std::string LineairDBField::get_lineairdb_field() const {
  return std::move(byteSize + valueLength + value);
}

void LineairDBField::set_header(const size_t num) {
  if (num == 0) {
    byteSize = noValue;
    valueLength.clear();
    value.clear();
    return;
  }
  assert(num <= maxValueLength);
  valueLength = convert_numeric_to_bytes(num);
  byteSize = convert_numeric_to_a_byte(valueLength.size());
}

void LineairDBField::set_null_field(const uchar *const buf,
                                    const size_t null_byte_length) {
  set_lineairdb_field(buf, null_byte_length);
}

void LineairDBField::set_lineairdb_field(
    std::variant<const uchar *, const char *> const srcMysql,
    const size_t length) {
  set_header(length);
  std::visit(
      [&](auto &&src) {
        value.assign(reinterpret_cast<const char *>(src), length);
      },
      srcMysql);
}

// alloc-churn #5: little-endian byte decode WITHOUT std::variant/std::visit.
// make_mysql_table_row always reads from const std::byte*, and the original
// convert_bytes_to_numeric does a std::visit PER BYTE (~192M visits for Q21
// SF=1 = ~16 cols x ~2 decodes x 6M rows). This direct loop drops that overhead.
static inline size_t decode_le_bytes(const std::byte *p, size_t len) {
  size_t n = 0;
  for (size_t i = 0; i < len; ++i)
    n |= static_cast<size_t>(static_cast<unsigned char>(p[i])) << (CHAR_BIT * i);
  return n;
}

void LineairDBField::make_mysql_table_row(const std::byte *const ldbRawData,
                                          const size_t length) {
  // Zero-copy parse: record each field as a string_view pointing into
  // ldbRawData. No per-field allocations, no string copies.
  row.clear();
  nullFlagView = {};

  for (size_t offset = 0; offset < length;) {
    const auto ldbField = ldbRawData + offset;

    byteSize = static_cast<char>(decode_le_bytes(ldbField, sizeof(byteSize)));

    if (byteSize == noValue) {
      if (offset != 0) {
        row.emplace_back();  // empty field
      }
      offset += sizeof(byteSize);
      continue;
    }

    size_t byteSizeForRead =
        static_cast<size_t>(static_cast<unsigned char>(byteSize));

    const size_t valueLength =
        decode_le_bytes(ldbField + sizeof(byteSize), byteSizeForRead);

    assert(valueLength <= maxValueLength);
    const auto valueData = ldbField + byteSizeForRead + sizeof(byteSize);

    std::string_view field_view(reinterpret_cast<const char *>(valueData),
                                valueLength);
    if (offset == 0) {
      nullFlagView = field_view;
    } else {
      row.emplace_back(field_view);
    }
    offset += sizeof(byteSize) + byteSizeForRead + valueLength;
  }
}
