// key_pack.hh
// Helios Storage Engine: shared index key / range packing.

#ifndef HELIOS_KEY_PACK_HH
#define HELIOS_KEY_PACK_HH

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "field_types.h"
#include "helios_field_types.h"
#include "my_base.h"
#include "my_inttypes.h"

class Field;
class KEY_PART_INFO;
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

/**
 * @brief The width of a key part packed as its payload alone.
 *
 * @details A NOT NULL integer, DATE, DATETIME or TIMESTAMP part has a width
 * the schema fixes, so it needs no null marker, type or length to sort and to
 * stay apart from the next part. Every other part keeps them.
 *
 * @param kp The key part.
 * @return The part's width in bytes, or 0 when it keeps the header.
 */
std::size_t raw_part_width(const KEY_PART_INFO &kp);

void append_key_part(std::string &out, const KEY_PART_INFO &kp, bool is_null,
                     HeliosFieldType type, const std::string &payload);
std::string build_prefix_range_end(const std::string &prefix);

/**
 * @brief The weights of a string key part under its column's collation.
 *
 * @details The value is cut to the part's characters, a CHAR under a NO PAD
 * collation drops its trailing spaces, and a PAD SPACE collation pads the
 * weights to the part's full length, as Field_string::make_sort_key and
 * Field_varstring::make_sort_key do. Collation-equal values get equal bytes,
 * so the original text lives only in the row.
 *
 * @param kp  Key part whose field gives the collation and whose length gives
 *            the indexed characters.
 * @param s   The value, or its key image after the length prefix.
 * @param len Bytes at `s`.
 * @return The weight string; for ENUM and SET, the bytes at `s` unchanged.
 */
std::string string_weights(const KEY_PART_INFO &kp, const uchar *s, size_t len);

/**
 * @brief Whether an index entry exceeds kMaxKeyLength.
 *
 * @details A UNIQUE index stores the secondary key alone, any other index at
 * most the secondary key followed by the primary key.
 *
 * @param key_info      The index the entry belongs to.
 * @param secondary_key The entry's packed secondary key.
 * @param primary_key   The packed primary key of its row.
 * @return True when the stored entry could be longer than kMaxKeyLength.
 */
bool index_key_too_long(const KEY &key_info, const std::string &secondary_key,
                        const std::string &primary_key);

/**
 * @brief Where a non-unique index record's key holds the primary key.
 *
 * @details The storage keys a record by the secondary key followed by the
 * primary-key parts the secondary key does not hold. When every part of both
 * keys has a raw_part_width and the secondary key holds a primary-key part,
 * the key's layout is fixed and the result locates each primary-key part in
 * it; otherwise the storage appends the whole primary key.
 *
 * @param table The table, whose primary key the records point at.
 * @param key   The secondary index.
 * @return (offset, length) of each primary-key part in the record's key, in
 *         primary-key order; empty for a UNIQUE index, a table without a
 *         primary key, or a layout that is not fixed.
 */
std::vector<uint32_t> pk_slices(const TABLE &table, const KEY &key);

// "+infinity" end for an unbounded-upper range scan. It is one 0xFF byte longer
// than the longest key, so it sorts after every key. An empty end will not do:
// the server reads an empty plan-step end as a single-key range, and the
// read-plan compiler and the consumer must pack identical bytes for the cache
// to match.
constexpr std::size_t kScanEndSentinelSize = kMaxKeyLength + 1;
inline std::string scan_end_sentinel() {
  return std::string(kScanEndSentinelSize, '\xff');
}

std::string pack_key(TABLE *table, uint key_index, const uchar *key,
                     key_part_map keypart_map);

}  // namespace key_pack

#endif // HELIOS_KEY_PACK_HH
