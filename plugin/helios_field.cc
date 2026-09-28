#include "helios_field.hh"

#include <cassert>

/**
 * HeliosField method definitions
 */

size_t HeliosField::convert_bytes_to_numeric(const std::byte *const bytes,
                                             const size_t length) const {
  size_t n = 0;
  for (size_t i = 0; i < length; i++) {
    n = n | static_cast<uchar>(bytes[i]) << CHAR_BIT * i;
  }
  return n;
}

void HeliosField::append_field(std::string &out, const char *src,
                               size_t length) {
  if (length == 0) {
    out.push_back(noValue);
    return;
  }
  assert(length <= maxValueLength);
  size_t width = 1;
  for (size_t n = length >> CHAR_BIT; n != 0; n >>= CHAR_BIT) ++width;
  out.push_back(static_cast<char>(width));
  for (size_t i = 0; i < width; ++i) {
    out.push_back(static_cast<char>(length >> (CHAR_BIT * i)));
  }
  out.append(src, length);
}

void HeliosField::make_mysql_table_row(const std::byte *const raw_row,
                                       const size_t length) {
  // Record each field as a string_view pointing into raw_row: the payloads
  // are not copied.
  row.clear();
  nullFlagView = {};

  for (size_t offset = 0; offset < length;) {
    const auto field = raw_row + offset;

    const char byteSize = static_cast<char>(*field);

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
