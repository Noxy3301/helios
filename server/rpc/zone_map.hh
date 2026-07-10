#ifndef ZONE_MAP_HH
#define ZONE_MAP_HH

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include <lineairdb/pax_store.h>

namespace query_block {
namespace zone {

/**
 * @brief Typed value domain of a prunable column.
 *
 * @details Zone values live in an int64 domain. INT parses fully; DATE maps
 * "YYYY-MM-DD" to the YYYYMMDD ordinal, whose integer order equals the
 * fixed-width string order; DECIMAL maps digits at a uniform fractional scale
 * to a scaled integer. UNTYPED marks a column outside these rules; it never
 * prunes.
 */
enum class ZKind : uint8_t { UNTYPED = 0, INT, DATE, DECIMAL };

/**
 * @brief StripZone::state values.
 */
enum : uint8_t { ZS_ABSENT = 0, ZS_VALID = 1, ZS_INVALID = 2 };

/**
 * @brief Per-strip min/max side info for one column.
 *
 * @details One zone summarizes one PaxGroup strip (8192 rows). A scan filter
 * of the form `column CMP constant` (or BETWEEN, or column CMP column) skips
 * the whole strip when the zone proves no row can match. Zones are side info,
 * never a second copy of the data: a sorted projection would be a copy the
 * OLTP path must maintain, while zones are built lazily on first scan use and
 * cached keyed by the strip's write_counter snapshot, leaving the OLTP write
 * path untouched. Because the counter is bumped at modification start and end
 * and is monotone, an unchanged counter proves the strip's cells are unchanged
 * since the zone was built; a stale or torn zone can only mislead a prune when
 * the counter differs, and that same difference fails the executor's
 * quiescence check and discards the result. Pruning never changes results.
 */
struct StripZone {
    uint64_t wc = 0;            // write_counter this zone was built from
    int64_t zmin = 0;           // min over visible, typed, non-null cells
    int64_t zmax = 0;           // max over the same
    uint8_t state = ZS_ABSENT;  // VALID = min/max meaningful and prunable
};

/**
 * @brief Classifies one cell string into a typed int64 value.
 *
 * @details DATE requires exactly "YYYY-MM-DD" and yields YYYYMMDD. DECIMAL is
 * a cell with a '.' (scale = fractional digits) and yields the scaled
 * integer. INT is an optional sign plus digits. Empty cells, non-numeric
 * text, and magnitudes outside the int64 / exact-double reasoning (>= 19
 * integer digits, or a decimal with scale or total significant digits > 15)
 * are rejected.
 *
 * @param s Cell payload bytes.
 * @param k Classified kind on success.
 * @param scale Fractional-digit count (DECIMAL; 0 otherwise).
 * @param val Typed int64 value on success.
 * @return false when the cell does not classify; such a cell marks its strip
 * non-prunable.
 */
bool ClassifyCell(std::string_view s, ZKind* k, int* scale, int64_t* val);

/**
 * @brief Exact scaled-int lower boundary for a DECIMAL zone against a double
 * constant.
 *
 * @details `c` is the value the byte path folds and compares with strtod.
 * Returns the smallest scaled int I with f(I) >= c, where
 * f(I) = (double)I / 10^scale == strtod(cell).
 */
int64_t DecLower(double c, int scale);

/**
 * @brief Exact scaled-int upper boundary for a DECIMAL zone against a double
 * constant.
 *
 * @details Returns the largest scaled int I with f(I) <= c; see DecLower for
 * f.
 */
int64_t DecUpper(double c, int scale);

/**
 * @brief Builds or refreshes the per-strip zones for (store, column) and
 * copies a consistent snapshot into *out.
 *
 * @details Zones for groups [0, n_groups) are built lazily; strips whose
 * write counter moved are rebuilt. The snapshot in *out has n_groups entries
 * and is safe to read without locking afterward. Pruning applies only when a
 * strip is provably empty of matches; a strip whose cells fail typing, or
 * that mixes kinds or scales, is non-prunable.
 *
 * @param store PAX store of the scanned table.
 * @param column Zero-based MySQL column index.
 * @param n_groups Number of groups to cover.
 * @param out Destination snapshot (n_groups entries).
 * @param kind Column kind on success (!= UNTYPED).
 * @param scale Column DECIMAL scale on success.
 * @return false when the column is not prunable (untyped, or no typed data
 * yet); the caller then does not prune on this column.
 */
bool GetColumnZones(LineairDB::Pax::PaxStore* store, uint32_t column,
                    size_t n_groups, std::vector<StripZone>* out, ZKind* kind,
                    int* scale);

}  // namespace zone
}  // namespace query_block

#endif  // ZONE_MAP_HH
