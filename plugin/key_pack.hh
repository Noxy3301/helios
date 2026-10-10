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
class KEY;
struct TABLE;

constexpr unsigned char kKeyMarkerNotNull = 0x00;
constexpr unsigned char kKeyMarkerNull = 0x01;

constexpr unsigned char kKeyTypeInt = 0x10;
constexpr unsigned char kKeyTypeString = 0x20;
constexpr unsigned char kKeyTypeDatetime = 0x30;
constexpr unsigned char kKeyTypeOther = 0xF0;

namespace key_pack {

// The longest key the storage holds, Masstree's MASSTREE_MAXKEYLEN. A longer
// index entry is refused, since cutting it would merge distinct keys.
constexpr std::size_t kMaxKeyLength = 255;

std::string pack_int_key(const uchar *data, size_t len);
std::string pack_datetime_key(const uchar *data, size_t len,
                              enum_field_types mysql_type);

void append_key_part(std::string &out, bool is_null, HeliosFieldType type,
                     const std::string &payload);
std::string build_prefix_range_end(const std::string &prefix);

/**
 * @brief Whether an index entry exceeds kMaxKeyLength.
 *
 * @details A UNIQUE index stores the secondary key alone, any other index the
 * secondary key followed by the primary key.
 *
 * @param key_info      The index the entry belongs to.
 * @param secondary_key The entry's packed secondary key.
 * @param primary_key   The packed primary key of its row.
 * @return True when the stored entry would be longer than kMaxKeyLength.
 */
bool index_key_too_long(const KEY &key_info, const std::string &secondary_key,
                        const std::string &primary_key);

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
