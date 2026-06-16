// lineairdb_keyenc.cc
// LineairDB Storage Engine: MySQL index key <-> LineairDB key/range encoding.
// Handler members delegate to the free functions in this module.

#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_field_types.h"
#include "lineairdb.pb.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "lineairdb_keyenc.hh"

namespace lineairdb_keyenc {

unsigned char key_part_type_tag(LineairDBFieldType type) {
  switch (type) {
  case LineairDBFieldType::LINEAIRDB_INT:
    return kKeyTypeInt;
  case LineairDBFieldType::LINEAIRDB_STRING:
    return kKeyTypeString;
  case LineairDBFieldType::LINEAIRDB_DATETIME:
    return kKeyTypeDatetime;
  case LineairDBFieldType::LINEAIRDB_OTHER:
  default:
    return kKeyTypeOther;
  }
}

void append_key_part_encoding(std::string &out, bool is_null,
                              LineairDBFieldType type,
                              const std::string &payload) {
  constexpr size_t kLengthFieldSize = 2;
  const size_t max_payload_length = std::numeric_limits<uint16_t>::max();
  size_t copy_length = std::min(payload.size(), max_payload_length);

  if (payload.size() > max_payload_length) {
    std::cerr << "[LineairDB][encode_key_part] payload truncated: length="
              << payload.size() << std::endl;
  }

  out.reserve(out.size() + 5 + copy_length);
  out.push_back(
      static_cast<char>(is_null ? kKeyMarkerNull : kKeyMarkerNotNull));
  out.push_back(static_cast<char>(key_part_type_tag(type)));

  if (type == LineairDBFieldType::LINEAIRDB_STRING) {
    // STRING: payload first, then terminator (0x00), then length
    if (copy_length > 0) {
      out.append(payload.data(), copy_length);
    }
    out.push_back('\0'); // terminator to ensure shorter strings sort before
                         // longer ones with same prefix
    uint16_t length_field = static_cast<uint16_t>(copy_length);
    out.push_back(static_cast<char>((length_field >> 8) & 0xFF));
    out.push_back(static_cast<char>(length_field & 0xFF));
  } else {
    // INT, DATETIME, OTHER: length first, then payload (fixed-length types)
    uint16_t length_field = static_cast<uint16_t>(copy_length);
    out.push_back(static_cast<char>((length_field >> 8) & 0xFF));
    out.push_back(static_cast<char>(length_field & 0xFF));

    if (copy_length > 0) {
      out.append(payload.data(), copy_length);
    }
  }
}

/**
 * @brief Generate the end key of a prefix range (the next lexicographic key)
 *
 * By returning the next lexicographic key, all keys that start with the prefix
 * are covered precisely in the form [prefix, end).
 *
 * Example:
 *   prefix = 01 02 FF -> end = 01 03
 *
 * If all bytes are 0xFF, there is no valid next lexicographic key. In that
 * case we return an empty string as a sentinel for "no upper bound", which the
 * caller treats as an open-ended range (e.g., converted to std::nullopt).
 */
std::string build_prefix_range_end(const std::string &prefix) {
  std::string end = prefix;
  for (size_t i = end.size(); i-- > 0;) {
    unsigned char byte = static_cast<unsigned char>(end[i]);
    if (byte != 0xFF) {
      end[i] = static_cast<char>(byte + 1);
      end.resize(i + 1);
      return end;
    }
  }
  // no upper bound
  return std::string();
}

}  // namespace lineairdb_keyenc

unsigned char ha_lineairdb::key_part_type_tag(LineairDBFieldType type) {
  return lineairdb_keyenc::key_part_type_tag(type);
}

void ha_lineairdb::append_key_part_encoding(std::string &out, bool is_null,
                                            LineairDBFieldType type,
                                            const std::string &payload) {
  lineairdb_keyenc::append_key_part_encoding(out, is_null, type, payload);
}

std::string ha_lineairdb::build_prefix_range_end(const std::string &prefix) {
  return lineairdb_keyenc::build_prefix_range_end(prefix);
}

/**
 * @brief Serialize a single field value to LineairDB key format
 *
 * This helper function converts a MySQL Field to LineairDB's sortable key
 * format based on its type. This eliminates code duplication across different
 * key handling functions.
 *
 * @param field MySQL Field object
 * @return Serialized key string
 */
std::string ha_lineairdb::serialize_key_from_field(Field *field) {
  const bool is_null = field->is_null();
  enum_field_types mysql_type = field->type();
  LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

  std::string payload;

  if (!is_null) {
    switch (ldb_type) {
    case LineairDBFieldType::LINEAIRDB_INT: {
      int64_t value = field->val_int();
      size_t field_len = field->pack_length();

      uchar buf[8] = {0};
      if (field_len == 1) {
        buf[0] = static_cast<uchar>(value & 0xFF);
      } else if (field_len == 2) {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
      } else if (field_len == 4) {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
        buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
        buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
      } else {
        buf[0] = static_cast<uchar>(value & 0xFF);
        buf[1] = static_cast<uchar>((value >> 8) & 0xFF);
        buf[2] = static_cast<uchar>((value >> 16) & 0xFF);
        buf[3] = static_cast<uchar>((value >> 24) & 0xFF);
        buf[4] = static_cast<uchar>((value >> 32) & 0xFF);
        buf[5] = static_cast<uchar>((value >> 40) & 0xFF);
        buf[6] = static_cast<uchar>((value >> 48) & 0xFF);
        buf[7] = static_cast<uchar>((value >> 56) & 0xFF);
        field_len = 8;
      }

      payload = encode_int_key(buf, field_len);
      break;
    }

    case LineairDBFieldType::LINEAIRDB_DATETIME: {
      size_t field_len = field->pack_length();
      std::string raw(field_len, '\0');
      field->get_key_image(reinterpret_cast<uchar *>(raw.data()), field_len,
                           Field::itRAW);
      payload = encode_datetime_key(reinterpret_cast<const uchar *>(raw.data()),
                                    field_len, mysql_type);
      break;
    }

    case LineairDBFieldType::LINEAIRDB_STRING: {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default: {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }
    }
  }

  std::string encoded;
  append_key_part_encoding(encoded, is_null, ldb_type, payload);
  return encoded;
}

std::string ha_lineairdb::build_secondary_key_from_row(const uchar *row_buffer,
                                                       const KEY &key_info) {
  // Temporarily set read_set to include all columns
  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);

  // Calculate the offset between row_buffer and record[0]
  ptrdiff_t offset = row_buffer - table->record[0];

  // Construct the secondary key
  std::string secondary_key;
  for (uint part_idx = 0; part_idx < key_info.user_defined_key_parts;
       part_idx++) {
    auto key_part = key_info.key_part[part_idx];
    Field *field = table->field[key_part.fieldnr - 1];

    // Adjust the Field pointer to match row_buffer
    field->move_field_offset(offset);

    // Serialize each key part and concatenate
    secondary_key += serialize_key_from_field(field);

    // Restore the Field pointer back to original position
    field->move_field_offset(-offset);
  }

  // Restore the original read_set
  tmp_restore_column_map(table->read_set, org_bitmap);

  return secondary_key;
}

void ha_lineairdb::store_primary_key_in_ref(const std::string &primary_key) {
  if (table == nullptr || table->s == nullptr || ref == nullptr) {
    return;
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t)) {
    return;
  }

  if (primary_key.size() > std::numeric_limits<uint16_t>::max()) {
    std::cerr << "[LineairDB][position] primary key length exceeds uint16_t: "
              << primary_key.size() << std::endl;
    return;
  }

  const size_t payload_capacity = ref_length_local - sizeof(uint16_t);
  if (primary_key.size() > payload_capacity) {
    std::cerr
        << "[LineairDB][position] primary key length exceeds ref capacity: "
        << primary_key.size() << " > " << payload_capacity << std::endl;
    return;
  }

  const uint16_t key_length = static_cast<uint16_t>(primary_key.size());
  std::memcpy(ref, &key_length, sizeof(uint16_t));

  if (key_length > 0) {
    std::memcpy(ref + sizeof(uint16_t), primary_key.data(), key_length);
  }

  const size_t remaining = payload_capacity - key_length;
  if (remaining > 0) {
    std::memset(ref + sizeof(uint16_t) + key_length, 0, remaining);
  }
}

std::string ha_lineairdb::extract_primary_key_from_ref(const uchar *pos) const {
  if (pos == nullptr || table == nullptr || table->s == nullptr) {
    return {};
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t)) {
    return {};
  }

  uint16_t key_length = 0;
  std::memcpy(&key_length, pos, sizeof(uint16_t));

  if (key_length == 0) {
    return {};
  }

  if (sizeof(uint16_t) + key_length > ref_length_local) {
    return {};
  }

  std::string key(reinterpret_cast<const char *>(pos + sizeof(uint16_t)),
                  key_length);

  return key;
}

bool ha_lineairdb::uses_hidden_primary_key() const {
  if (table == nullptr || table->s == nullptr) {
    return false;
  }
  return table->s->primary_key == MAX_KEY;
}

std::string ha_lineairdb::serialize_hidden_primary_key(uint64_t row_id) const {
  std::ostringstream oss;
  oss << std::hex << std::setw(16) << std::setfill('0') << row_id;
  return oss.str();
}

std::string ha_lineairdb::generate_hidden_primary_key() {
  if (share == nullptr) {
    share = get_share();
  }
  uint64_t row_id =
      share->next_hidden_pk.fetch_add(1, std::memory_order_relaxed);
  std::string key = serialize_hidden_primary_key(row_id);
  return key;
}

std::string ha_lineairdb::extract_key(const uchar *buf) {
  if (is_primary_key_exists()) {
    return extract_key_from_mysql(buf);
  } else {
    return autogenerate_key();
  }
}

std::string ha_lineairdb::extract_key_from_mysql(const uchar *row_buffer) {
  std::string complete_key;

  // Guard: return empty if no explicit primary key exists
  if (!is_primary_key_exists() || key_part == nullptr || num_key_parts == 0) {
    return complete_key;
  }

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  ptrdiff_t offset = row_buffer - table->record[0];

  for (size_t i = 0; i < num_key_parts; ++i) {
    auto field_index = key_part[i].fieldnr - 1;
    Field *field = table->field[field_index];

    field->move_field_offset(offset);
    complete_key += serialize_key_from_field(field);
    field->move_field_offset(-offset);
  }

  tmp_restore_column_map(table->read_set, org_bitmap);

  return complete_key;
}

std::string ha_lineairdb::autogenerate_key() {
  return generate_hidden_primary_key();
}

/**
 * @brief Encode INT key from MySQL format to LineairDB sortable format
 *
 * Converts little-endian integer to big-endian with sign bit flipped.
 * This ensures correct lexicographic ordering: negative < 0 < positive
 *
 * @param data MySQL key data (little-endian)
 * @param len Key length (1, 2, 4, or 8 bytes)
 * @return Big-endian binary string with sign bit flipped
 */
namespace lineairdb_keyenc {

std::string encode_int_key(const uchar *data, size_t len) {
  uint64_t value = 0;

  if (len == 1) {
    value = static_cast<uint8_t>(data[0]);
  } else if (len == 2) {
    value =
        static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8);
  } else if (len == 4) {
    value = static_cast<uint32_t>(data[0]) |
            (static_cast<uint32_t>(data[1]) << 8) |
            (static_cast<uint32_t>(data[2]) << 16) |
            (static_cast<uint32_t>(data[3]) << 24);
  } else if (len == 8) {
    value = static_cast<uint64_t>(data[0]) |
            (static_cast<uint64_t>(data[1]) << 8) |
            (static_cast<uint64_t>(data[2]) << 16) |
            (static_cast<uint64_t>(data[3]) << 24) |
            (static_cast<uint64_t>(data[4]) << 32) |
            (static_cast<uint64_t>(data[5]) << 40) |
            (static_cast<uint64_t>(data[6]) << 48) |
            (static_cast<uint64_t>(data[7]) << 56);
  } else {
    // Unsupported length
    return std::string();
  }

  // Flip sign bit for correct sorting
  // This makes: negative numbers < 0 < positive numbers
  if (len == 1) {
    value ^= 0x80ULL;
  } else if (len == 2) {
    value ^= 0x8000ULL;
  } else if (len == 4) {
    value ^= 0x80000000ULL;
  } else if (len == 8) {
    value ^= 0x8000000000000000ULL;
  }

  // Convert to big-endian
  char buf[8];
  size_t output_len = len;
  for (size_t i = 0; i < output_len; i++) {
    buf[i] = static_cast<char>((value >> ((output_len - 1 - i) * 8)) & 0xFF);
  }

  return std::string(buf, output_len);
}

/**
 * @brief Encode DATETIME key from MySQL format to LineairDB format
 *
 * MYSQL_TYPE_DATE / MYSQL_TYPE_NEWDATE: 3 bytes stored in little-endian.
 * Must be converted to big-endian for correct lexicographic sorting.
 * DATETIME2, TIMESTAMP2, TIME2: already in big-endian sortable format.
 */
std::string encode_datetime_key(const uchar *data, size_t len,
                                enum_field_types mysql_type) {
  if (mysql_type == MYSQL_TYPE_DATE || mysql_type == MYSQL_TYPE_NEWDATE) {
    char buf[3];
    buf[0] = static_cast<char>(data[2]);
    buf[1] = static_cast<char>(data[1]);
    buf[2] = static_cast<char>(data[0]);
    return std::string(buf, 3);
  }
  return std::string(reinterpret_cast<const char *>(data), len);
}

/**
 * @brief Encode VARCHAR key from MySQL format to LineairDB format
 *
 * MySQL stores VARCHAR keys with a 2-byte length prefix (little-endian).
 * We extract the actual string data without padding.
 *
 * @param data MySQL VARCHAR key data (length prefix + string + padding)
 * @param len Total key length
 * @return Actual string data without prefix or padding
 */
std::string encode_string_key(const uchar *data, size_t len) {
  if (len < 2)
    return std::string();

  // First 2 bytes are length (little-endian)
  uint16_t str_len =
      static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8);

  if (str_len == 0 || len < 2 + str_len) {
    // Invalid or empty string
    return std::string();
  }

  // Return actual string data (skip 2-byte prefix, exclude padding)
  return std::string(reinterpret_cast<const char *>(data + 2), str_len);
}

}  // namespace lineairdb_keyenc

std::string ha_lineairdb::encode_int_key(const uchar *data, size_t len) {
  return lineairdb_keyenc::encode_int_key(data, len);
}

std::string ha_lineairdb::encode_datetime_key(const uchar *data, size_t len,
                                              enum_field_types mysql_type) {
  return lineairdb_keyenc::encode_datetime_key(data, len, mysql_type);
}

std::string ha_lineairdb::encode_string_key(const uchar *data, size_t len) {
  return lineairdb_keyenc::encode_string_key(data, len);
}

/**
 * @brief Convert MySQL binary composite key format to LineairDB sortable key
 * format
 *
 * This function handles composite keys by processing each key part
 * sequentially:
 * - Reads key_part_map to determine which parts are used
 * - Converts each part to sortable format based on its type
 * - Concatenates all parts into a single sortable string
 *
 * Key formats by type:
 * - INT: Little-endian to big-endian + sign bit flip (for correct sorting)
 * - DATETIME: Pass through as-is (already sortable)
 * - STRING (VARCHAR): Extract actual data (remove length prefix and padding)
 *
 * @param key MySQL binary key data (concatenated byte array)
 * @param keypart_map Bitmap indicating which key parts are used
 * @return LineairDB formatted key string (concatenated sortable format)
 */
namespace lineairdb_keyenc {

std::string convert_key_to_ldbformat(TABLE *table, uint key_index,
                                     const uchar *key,
                                     key_part_map keypart_map) {
  KEY *key_info = &table->key_info[key_index];
  std::string result;
  const uchar *key_ptr = key;

  // Process each key part sequentially
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    // Check if this key part is used in the query
    if (!((keypart_map >> i) & 1)) {
      break; // Remaining parts are not used (prefix scan)
    }

    KEY_PART_INFO *kp = &key_info->key_part[i];
    Field *field = kp->field;
    bool is_null = false;
    if (kp->null_bit) {
      is_null = (*key_ptr != 0);
      key_ptr++; // Skip NULL flag byte

      if (is_null) {
        key_ptr += (kp->store_length - 1);
        append_key_part_encoding(result, true,
                                 convert_mysql_type_to_lineairdb(field->type()),
                                 std::string());
        continue;
      }
    }

    uint data_len = kp->length;
    const uchar *data_ptr = key_ptr;

    if (kp->key_part_flag & HA_VAR_LENGTH_PART) {
      data_len = uint2korr(data_ptr);
      data_ptr += 2; // Skip length prefix
      key_ptr = data_ptr;
    }

    enum_field_types mysql_type = field->type();
    LineairDBFieldType ldb_type = convert_mysql_type_to_lineairdb(mysql_type);

    std::string payload;
    switch (ldb_type) {
    case LineairDBFieldType::LINEAIRDB_INT:
      payload = encode_int_key(data_ptr, data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_DATETIME:
      payload = encode_datetime_key(data_ptr, data_len, mysql_type);
      break;

    case LineairDBFieldType::LINEAIRDB_STRING:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;

    case LineairDBFieldType::LINEAIRDB_OTHER:
    default:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;
    }

    append_key_part_encoding(result, false, ldb_type, payload);

    if (kp->key_part_flag & HA_VAR_LENGTH_PART) {
      key_ptr += kp->length;
    } else {
      key_ptr += kp->length;
    }
  }

  return result;
}

}  // namespace lineairdb_keyenc

std::string ha_lineairdb::convert_key_to_ldbformat(const uchar *key,
                                                   key_part_map keypart_map) {
  return lineairdb_keyenc::convert_key_to_ldbformat(table, active_index, key,
                                                    keypart_map);
}

/**
 * @brief This function only extracts the type of key for
 *        tables that have single key
 *
 * @return bytes Key type is int
 * @return 0 Key type is not int
 */
bool ha_lineairdb::is_primary_key_type_int() {
  ha_base_keytype integer_types[] = {
      HA_KEYTYPE_SHORT_INT, HA_KEYTYPE_USHORT_INT, HA_KEYTYPE_LONG_INT,
      HA_KEYTYPE_ULONG_INT, HA_KEYTYPE_LONGLONG,   HA_KEYTYPE_ULONGLONG,
      HA_KEYTYPE_INT24,     HA_KEYTYPE_UINT24,     HA_KEYTYPE_INT8};
  assert(table->s->keys == 1);
  ha_base_keytype key_type = primary_key_type;
  return std::find(std::begin(integer_types), std::end(integer_types),
                   key_type) != std::end(integer_types);
}
