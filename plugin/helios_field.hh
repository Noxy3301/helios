#ifndef HELIOS_FIELD_HH
#define HELIOS_FIELD_HH

#include <climits>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "my_inttypes.h"

/**
 * @brief This class is responsible for the translation between
 * MySQL Field value and Helios Field.
 * @details
 * Helios field consists of the following 3 information:
 *  header1     header2
 * [byteSize][valueLength][value]
 * header info
 * - byteSize: number of bytes of `valueLength`
 *             always 1 byte, byteSize = UCHAR_MAX if valueLength = 0
 * - valueLength: length of value
 *                max 4 bytes
 * value: MySQL value shown to users
 *        max 4294967295 bytes, the LONGBLOB limit
 * Each row consists of multiple fields.
 * First field stores null flags.
 */
class HeliosField {
 public:
  size_t convert_bytes_to_numeric(const std::byte* const bytes,
                                  const size_t length) const;

  static void append_field(std::string &out, const char *src, size_t length);

  /**
   * @brief Parses a packed row into one view per field.
   *
   * Each field is recorded as a (pointer, length) pair into raw_row, so no
   * field payload is copied, and the caller MUST keep raw_row alive while
   * iterating via get_column_of_row().
   */
  void make_mysql_table_row(const std::byte *const raw_row,
                            const size_t length);
  std::string_view get_null_flags() const { return nullFlagView; }
  std::string_view get_column_of_row(const size_t i) const { return row[i]; }
  // Parsed column count of the unpacked value; readers size their mapping
  // by this, not by s->fields.
  size_t get_row_size() const { return row.size(); }

  HeliosField() = default;

 private:
  static constexpr char noValue = 0xff;
  static constexpr size_t maxValueLength = UINT_MAX;

  // Zero-copy row parsing: views point into the caller-owned raw_row.
  std::string_view nullFlagView;
  std::vector<std::string_view> row;
};

#endif /* HELIOS_FIELD_HH */