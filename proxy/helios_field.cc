#include "helios_field.hh"

#include <cassert>

/**
 * HeliosField method definitions
 */

char HeliosField::convert_numeric_to_a_byte(const size_t num) const {
  return convert_numeric_to_bytes(num)[0];
}

std::string HeliosField::convert_numeric_to_bytes(const size_t num) const {
  size_t byteSizeOfNum = calculate_minimum_byte_size_required(num);
  std::string byteSequence;
  byteSequence.reserve(byteSizeOfNum);
  // Encode in little-endian order to match convert_bytes_to_numeric().
  for (size_t i = 0; i < byteSizeOfNum; i++) {
    byteSequence.push_back(static_cast<char>((num >> (CHAR_BIT * i)) & 0xFF));
  }
  return byteSequence;
}

size_t HeliosField::convert_bytes_to_numeric(
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

std::string HeliosField::get_null_field() const {
  return get_helios_field();
}

std::string HeliosField::get_helios_field() const {
  return std::move(byteSize + valueLength + value);
}

void HeliosField::set_header(const size_t num) {
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

void HeliosField::set_null_field(const uchar *const buf,
                                    const size_t null_byte_length) {
  set_helios_field(buf, null_byte_length);
}

void HeliosField::set_helios_field(
    std::variant<const uchar *, const char *> const srcMysql,
    const size_t length) {
  set_header(length);
  std::visit(
      [&](auto &&src) {
        value.assign(reinterpret_cast<const char *>(src), length);
      },
      srcMysql);
}

void HeliosField::make_mysql_table_row(const std::byte *const raw_row,
                                       const size_t length) {
  // Zero-copy parse: record each field as a string_view pointing into
  // raw_row. No per-field allocations, no string copies.
  row.clear();
  nullFlagView = {};

  for (size_t offset = 0; offset < length;) {
    const auto field = raw_row + offset;

    byteSize =
        static_cast<char>(convert_bytes_to_numeric(field, sizeof(byteSize)));

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
        convert_bytes_to_numeric(field + sizeof(byteSize), byteSizeForRead);

    assert(valueLength <= maxValueLength);
    const auto valueData = field + byteSizeForRead + sizeof(byteSize);

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
