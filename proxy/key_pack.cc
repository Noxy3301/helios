// key_pack.cc
// Helios Storage Engine: MySQL index key <-> Helios key/range packing.
// Handler members delegate to the free functions in this module.

#include "storage/helios/ha_helios.hh"
#include "../common/log.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "helios_field_types.h"
#include "helios.pb.h"
#include "my_base.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "key_pack.hh"

namespace key_pack {

unsigned char key_part_type_tag(HeliosFieldType type) {
  switch (type) {
  case HeliosFieldType::HELIOS_INT:
    return kKeyTypeInt;
  case HeliosFieldType::HELIOS_STRING:
    return kKeyTypeString;
  case HeliosFieldType::HELIOS_DATETIME:
    return kKeyTypeDatetime;
  case HeliosFieldType::HELIOS_OTHER:
  default:
    return kKeyTypeOther;
  }
}

void append_key_part_encoding(std::string &out, bool is_null,
                              HeliosFieldType type,
                              const std::string &payload) {
  constexpr size_t kLengthFieldSize = 2;
  const size_t max_payload_length = std::numeric_limits<uint16_t>::max();
  size_t copy_length = std::min(payload.size(), max_payload_length);

  if (payload.size() > max_payload_length) {
    std::cerr << "[Helios][pack_key] payload truncated: length="
              << payload.size() << std::endl;
  }

  out.reserve(out.size() + 5 + copy_length);
  out.push_back(
      static_cast<char>(is_null ? kKeyMarkerNull : kKeyMarkerNotNull));
  out.push_back(static_cast<char>(key_part_type_tag(type)));

  if (type == HeliosFieldType::HELIOS_STRING) {
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
 * @brief The end key of the prefix range [prefix, end): the next lexicographic
 * key, or empty when every byte is 0xFF and the range is unbounded above.
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

}  // namespace key_pack

unsigned char ha_helios::key_part_type_tag(HeliosFieldType type) {
  return key_pack::key_part_type_tag(type);
}

void ha_helios::append_key_part_encoding(std::string &out, bool is_null,
                                            HeliosFieldType type,
                                            const std::string &payload) {
  key_pack::append_key_part_encoding(out, is_null, type, payload);
}

std::string ha_helios::build_prefix_range_end(const std::string &prefix) {
  return key_pack::build_prefix_range_end(prefix);
}

std::string ha_helios::serialize_key_from_field(Field *field) {
  const bool is_null = field->is_null();
  enum_field_types mysql_type = field->type();
  HeliosFieldType helios_type = convert_mysql_type_to_helios(mysql_type);

  std::string payload;

  if (!is_null) {
    switch (helios_type) {
    case HeliosFieldType::HELIOS_INT: {
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

      payload = pack_int_key(buf, field_len);
      break;
    }

    case HeliosFieldType::HELIOS_DATETIME: {
      size_t field_len = field->pack_length();
      std::string raw(field_len, '\0');
      field->get_key_image(reinterpret_cast<uchar *>(raw.data()), field_len,
                           Field::itRAW);
      payload = pack_datetime_key(reinterpret_cast<const uchar *>(raw.data()),
                                    field_len, mysql_type);
      break;
    }

    case HeliosFieldType::HELIOS_STRING: {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }

    case HeliosFieldType::HELIOS_OTHER:
    default: {
      String buffer;
      field->val_str(&buffer, &buffer);
      payload.assign(buffer.c_ptr(), buffer.length());
      break;
    }
    }
  }

  std::string encoded;
  append_key_part_encoding(encoded, is_null, helios_type, payload);
  return encoded;
}

std::string ha_helios::build_secondary_key_from_row(const uchar *row_buffer,
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

void ha_helios::store_primary_key_in_ref(const std::string &primary_key) {
  if (table == nullptr || table->s == nullptr || ref == nullptr) {
    return;
  }

  const size_t ref_length_local = ref_length;
  if (ref_length_local < sizeof(uint16_t)) {
    return;
  }

  if (primary_key.size() > std::numeric_limits<uint16_t>::max()) {
    std::cerr << "[Helios][position] primary key length exceeds uint16_t: "
              << primary_key.size() << std::endl;
    return;
  }

  const size_t payload_capacity = ref_length_local - sizeof(uint16_t);
  if (primary_key.size() > payload_capacity) {
    std::cerr
        << "[Helios][position] primary key length exceeds ref capacity: "
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

std::string ha_helios::extract_primary_key_from_ref(const uchar *pos) const {
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

// Hidden primary keys are reserved a block at a time, sized to the statement
// when MySQL estimates its rows. NDB Cluster prefetches the same default.
static constexpr uint32_t kHiddenKeyRangeSize = 1000;
// Mirrors HiddenKeyAllocator::kMaxCount, which the proxy cannot include
static constexpr uint32_t kHiddenKeyMaxRange = 65536;

std::string ha_helios::serialize_hidden_primary_key(uint64_t row_id) const {
  std::ostringstream oss;
  oss << std::hex << std::setw(16) << std::setfill('0') << row_id;
  return oss.str();
}

namespace {

// A rejection the storage server will repeat: retrying can only spin on it
int reject_hidden_key(THD *thd, HeliosTransaction *tx,
                      const std::string &reason) {
  // The handler layer turns the return code into ER_AUTOINC_READ_FAILED, whose
  // text has no room for the cause, so the cause rides along as a warning.
  if (thd != nullptr) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_AUTOINC_READ_FAILED,
                        "Helios could not reserve a hidden primary key: %s",
                        reason.c_str());
    thd_mark_transaction_to_rollback(thd, 1);
  }
  if (tx != nullptr) tx->set_status_to_abort();
  return HA_ERR_AUTOINC_READ_FAILED;
}

}  // namespace

int ha_helios::generate_hidden_primary_key(HeliosTransaction *tx,
                                              std::string *key) {
  if (share == nullptr) {
    share = get_share();
  }

  auto *proxy = get_proxy();
  std::lock_guard<std::mutex> lock(share->hidden_keys.mutex);
  // A range from an earlier server run may overlap what the restarted server
  // hands out. A connection that does not know the run (token 0) or whose run
  // differs from the range's reserves again.
  const uint64_t token = proxy->storage_boot_token();
  const bool spent = share->hidden_keys.next >= share->hidden_keys.end;
  if (spent || token == 0 || token != share->hidden_keys.boot_token) {
    const uint64_t remaining = bulk_insert_rows_ > bulk_insert_generated_
                                   ? bulk_insert_rows_ - bulk_insert_generated_
                                   : 0;
    const uint32_t count = std::max<uint64_t>(
        kHiddenKeyRangeSize, std::min<uint64_t>(kHiddenKeyMaxRange, remaining));
    const auto reserved = proxy->db_allocate_hidden_keys(db_table_name, count);
    if (!reserved.ok) {
      if (reserved.permanent) {
        return reject_hidden_key(ha_thd(), tx, reserved.error);
      }
      if (tx != nullptr) {
        if (reserved.transport_error) {
          tx->mark_transport_error();
        } else {
          tx->set_status_to_abort();
        }
      }
      return abort_errno(tx);
    }
    // A live range of the same run keeps serving until it is spent: this
    // reservation was only needed to learn which run answered.
    if (share->hidden_keys.next >= share->hidden_keys.end ||
        reserved.boot_token != share->hidden_keys.boot_token) {
      share->hidden_keys.next = reserved.first_id;
      share->hidden_keys.end = reserved.first_id + count;
      share->hidden_keys.boot_token = reserved.boot_token;
    }
  }

  ++bulk_insert_generated_;
  *key = serialize_hidden_primary_key(share->hidden_keys.next++);
  return 0;
}

int ha_helios::extract_key(const uchar *buf, HeliosTransaction *tx,
                              std::string *key) {
  if (is_primary_key_exists()) {
    *key = extract_key_from_mysql(buf);
    return 0;
  }
  return autogenerate_key(tx, key);
}

std::string ha_helios::extract_key_from_mysql(const uchar *row_buffer) {
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

int ha_helios::autogenerate_key(HeliosTransaction *tx,
                                   std::string *key) {
  return generate_hidden_primary_key(tx, key);
}

/**
 * @brief Encode a 1, 2, 4 or 8 byte integer key part.
 *
 * @details Little-endian to big-endian with the sign bit flipped, so
 * lexicographic order matches signed integer order.
 */
namespace key_pack {

std::string pack_int_key(const uchar *data, size_t len) {
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
 * @brief Encode a temporal key part.
 *
 * @details DATE and NEWDATE are 3 little-endian bytes and get reversed;
 * DATETIME2, TIMESTAMP2 and TIME2 are already big-endian sortable.
 */
std::string pack_datetime_key(const uchar *data, size_t len,
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
 * @brief Encode a VARCHAR key part: the string without MySQL's 2-byte
 * little-endian length prefix and without padding.
 */
std::string pack_string_key(const uchar *data, size_t len) {
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

}  // namespace key_pack

std::string ha_helios::pack_int_key(const uchar *data, size_t len) {
  return key_pack::pack_int_key(data, len);
}

std::string ha_helios::pack_datetime_key(const uchar *data, size_t len,
                                              enum_field_types mysql_type) {
  return key_pack::pack_datetime_key(data, len, mysql_type);
}

std::string ha_helios::pack_string_key(const uchar *data, size_t len) {
  return key_pack::pack_string_key(data, len);
}

/**
 * @brief Convert a MySQL composite key into the sortable key format: the key
 * parts named by keypart_map, each encoded by its type, concatenated.
 */
namespace key_pack {

std::string pack_key(TABLE *table, uint key_index, const uchar *key,
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
                                 convert_mysql_type_to_helios(field->type()),
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
    HeliosFieldType helios_type = convert_mysql_type_to_helios(mysql_type);

    std::string payload;
    switch (helios_type) {
    case HeliosFieldType::HELIOS_INT:
      payload = pack_int_key(data_ptr, data_len);
      break;

    case HeliosFieldType::HELIOS_DATETIME:
      payload = pack_datetime_key(data_ptr, data_len, mysql_type);
      break;

    case HeliosFieldType::HELIOS_STRING:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;

    case HeliosFieldType::HELIOS_OTHER:
    default:
      payload.assign(reinterpret_cast<const char *>(data_ptr), data_len);
      break;
    }

    append_key_part_encoding(result, false, helios_type, payload);

    if (kp->key_part_flag & HA_VAR_LENGTH_PART) {
      key_ptr += kp->length;
    } else {
      key_ptr += kp->length;
    }
  }

  return result;
}

}  // namespace key_pack

std::string ha_helios::pack_key(const uchar *key, key_part_map keypart_map) {
  return key_pack::pack_key(table, active_index, key, keypart_map);
}

