#ifndef ZONE_MAP_HH
#define ZONE_MAP_HH

// E17 — zone maps (strip min/max pruning) for the server-side columnar scan.
//
// A scan filter of the form `column CMP constant` (or BETWEEN, or column CMP
// column) can skip an entire PaxGroup strip (8192 rows) when the strip's per-
// column value range provably contains no matching row. lineitem/orders are
// roughly date-clustered (rows are stored in orderkey/load order, and dates
// correlate with orderkey), so date-range filters prune large fractions.
//
// 1-copy-friendly by construction: a sorted projection would be a second copy
// the OLTP path must maintain. Instead we keep per-strip min/max SIDE INFO,
// built LAZILY on first scan use and cached keyed by the strip's write_counter
// snapshot (recomputed when the counter changed). The OLTP write path is never
// touched. Because write_counter is bumped at modification start and end and is
// monotone, an unchanged counter proves the strip's cells are unchanged since
// the zone was built; a stale/torn zone can only mislead a prune when the
// counter differs, and that same difference makes the executor's Quiesced()
// check fail and discard the result. So pruning never changes query results.
//
// Values live in an int64 domain per the E13/spike typing rules:
//   INT     : full from_chars parse.
//   DATE    : "YYYY-MM-DD" -> YYYYMMDD (int order == fixed-width string order).
//   DECIMAL : uniform fractional scale, digits as a scaled int.
// Any cell that fails typing (or a mixed kind/scale within a strip) marks that
// strip non-prunable — pruning is applied ONLY when provably empty.

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include <lineairdb/pax_store.h>

namespace qb {
namespace zone {

enum class ZKind : uint8_t { UNTYPED = 0, INT, DATE, DECIMAL };

// StripZone::state values.
enum : uint8_t { ZS_ABSENT = 0, ZS_VALID = 1, ZS_INVALID = 2 };

struct StripZone {
    uint64_t wc = 0;       // write_counter this zone was built from
    int64_t zmin = 0;      // min over visible, typed, non-null cells
    int64_t zmax = 0;      // max over the same
    uint8_t state = ZS_ABSENT;  // VALID = min/max meaningful & prunable
};

// Whether zone-map pruning is enabled (env LDBC_ZONEMAP; default on, "0" off).
bool Enabled();

// Classify one cell string into a typed int64 value.
//   DATE    : exactly "YYYY-MM-DD"          -> YYYYMMDD
//   DECIMAL : has a '.' (scale = frac digits) -> scaled int
//   INT     : optional sign + digits         -> int64
// Rejects (returns false) empty cells, non-numeric text, and magnitudes that
// would overflow the int64 / exact-double reasoning (>=19 int digits, or a
// decimal with scale or total significant digits > 15).
bool ClassifyCell(std::string_view s, ZKind* k, int* scale, int64_t* val);

// Exact scaled-int boundaries for a DECIMAL column of the given scale against a
// double constant c (the value the byte path folds and compares with strtod).
// DecLower = smallest scaled int I with f(I) >= c ; DecUpper = largest I with
// f(I) <= c, where f(I) = (double)I / 10^scale == strtod(cell). See the spike
// doc (2026-07-03-simd-spike.md) for the boundary-trap rationale.
int64_t DecLower(double c, int scale);
int64_t DecUpper(double c, int scale);

// Lazily build (or refresh stale strips of) the per-strip zones for
// (store, column) over groups [0, n_groups), then copy a consistent snapshot
// into *out (n_groups entries; safe to read without locking afterward).
// On success sets *kind (!= UNTYPED) and *scale. Returns false when the column
// is not prunable (untyped, or no typed data yet) — the caller then does not
// prune on this column.
bool GetColumnZones(LineairDB::Pax::PaxStore* store, uint32_t column,
                    size_t n_groups, std::vector<StripZone>* out, ZKind* kind,
                    int* scale);

}  // namespace zone
}  // namespace qb

#endif  // ZONE_MAP_HH
