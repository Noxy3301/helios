#include "helios_field.hh"

#include <cassert>

/**
 * HeliosField method definitions
 */

std::string HeliosField::convert_numeric_to_bytes(const size_t num) const {
  // Bytes the value needs, one base-256 digit at a time.
  size_t byteSizeOfNum = 0;
  for (size_t n = num; n > 0; n /= 256) byteSizeOfNum++;
  std::string byteSequence;
  byteSequence.reserve(byteSizeOfNum);
  // Pack in little-endian order to match convert_bytes_to_numeric().
  for (size_t i = 0; i < byteSizeOfNum; i++) {
    byteSequence.push_back(static_cast<char>((num >> (CHAR_BIT * i)) & 0xFF));
  }
  return byteSequence;
}

size_t HeliosField::convert_bytes_to_numeric(const std::byte *const bytes,
                                             const size_t length) const {
  size_t n = 0;
  for (size_t i = 0; i < length; i++) {
    n = n | static_cast<uchar>(bytes[i]) << CHAR_BIT * i;
  }
  return n;
}

std::string HeliosField::get_helios_field() const {
  return std::move(byteSize + valueLength + value);
}

void HeliosField::set_helios_field(const char *const src, const size_t length) {
  if (length == 0) {
    byteSize = noValue;
    valueLength.clear();
    value.clear();
    return;
  }
  assert(length <= maxValueLength);
  valueLength = convert_numeric_to_bytes(length);
  byteSize = static_cast<char>(valueLength.size());
  value.assign(src, length);
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
