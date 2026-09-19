// key_pack.hh
// Helios Storage Engine: shared index key / range packing.

#ifndef HELIOS_KEY_PACK_HH
#define HELIOS_KEY_PACK_HH

#include <cstddef>
#include <string>

#include "field_types.h"
#include "helios_field_types.h"
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

namespace key_pack {

std::string pack_int_key(const uchar *data, size_t len);
std::string pack_datetime_key(const uchar *data, size_t len,
                              enum_field_types mysql_type);

void append_key_part(std::string &out, bool is_null, HeliosFieldType type,
                     const std::string &payload);
std::string build_prefix_range_end(const std::string &prefix);

// "+infinity" end for an unbounded-upper range scan. 16 0xFF bytes sort after
// every key (real keys start with the 0x00/0x01 null marker). An empty end will
// not do: the server reads an empty plan-step end as a single-key range, and
// the read-plan compiler and the consumer must pack identical bytes for the
// cache to match.
constexpr std::size_t kScanEndSentinelSize = 16;
inline std::string scan_end_sentinel() {
  return std::string(kScanEndSentinelSize, '\xff');
}

std::string pack_key(TABLE *table, uint key_index, const uchar *key,
                     key_part_map keypart_map);

}  // namespace key_pack

#endif // HELIOS_KEY_PACK_HH
