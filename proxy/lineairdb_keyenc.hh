// lineairdb_keyenc.hh
// LineairDB Storage Engine: shared index key / range encoding.

#ifndef LINEAIRDB_KEYENC_HH
#define LINEAIRDB_KEYENC_HH

#include <cstddef>
#include <string>

#include "field_types.h"
#include "lineairdb_field_types.h"
#include "my_base.h"
#include "my_inttypes.h"

class Field;
struct TABLE;

constexpr unsigned char kKeyMarkerNotNull = 0x00;
constexpr unsigned char kKeyMarkerNull = 0x01;

constexpr unsigned char kKeyTypeInt = 0x10;
constexpr unsigned char kKeyTypeString = 0x20;
constexpr unsigned char kKeyTypeDatetime = 0x30;
constexpr unsigned char kKeyTypeOther = 0xF0;

namespace lineairdb_keyenc {

std::string encode_int_key(const uchar *data, size_t len);
std::string encode_datetime_key(const uchar *data, size_t len,
                                enum_field_types mysql_type);
std::string encode_string_key(const uchar *data, size_t len);

unsigned char key_part_type_tag(LineairDBFieldType type);
void append_key_part_encoding(std::string &out, bool is_null,
                              LineairDBFieldType type,
                              const std::string &payload);
std::string build_prefix_range_end(const std::string &prefix);

// "+infinity" end for an unbounded-upper range scan. 16 0xFF bytes sort after
// every key (real keys start with the 0x00/0x01 null marker). An empty end will
// not do: the server reads an empty plan-step end as a single-key range, and
// autogen and the consumer must stage identical bytes for the cache to match.
constexpr std::size_t kScanEndSentinelSize = 16;
inline std::string scan_end_sentinel() {
  return std::string(kScanEndSentinelSize, '\xff');
}

std::string convert_key_to_ldbformat(TABLE *table, uint key_index,
                                     const uchar *key,
                                     key_part_map keypart_map);

}  // namespace lineairdb_keyenc

#endif // LINEAIRDB_KEYENC_HH
