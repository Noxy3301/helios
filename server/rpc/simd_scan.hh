#ifndef SIMD_SCAN_HH
#define SIMD_SCAN_HH

// SIMD/typed-cell scan feasibility spike (helios/pax-simd-spike).
//
// The row-at-a-time PredicateEvaluator re-parses length-prefixed byte cells
// for every scanned row; perf shows filter evaluation is 12-26% of filtered
// scans. This module measures the ceiling of the known endgame — typed cells
// as the receptacle for SIMD — by lazily building an int64 array per filter
// column (INT / DATE / uniform-scale DECIMAL), then evaluating simple
// comparison filters over those arrays with either a scalar typed loop or an
// AVX2 kernel. It is gated by env LDBC_SIMD and defaults off; the byte path
// is always the fallback, so results never change (only speed).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "lineairdb.pb.h"

#include <lineairdb/pax_store.h>

namespace qb {
namespace simd {

// Execution mode, chosen once from env LDBC_SIMD:
//   unset / "0" / ""      -> OFF     (byte path, i.e. baseline)
//   "scalar"              -> SCALAR  (typed int64 arrays, scalar loop)
//   "1" / "avx2"          -> AVX2    (typed int64 arrays, AVX2 kernel;
//                                     falls back to SCALAR if !avx2 cpu)
enum class Mode { OFF, SCALAR, AVX2 };
Mode CurrentMode();

// Per-column typing detected from the visible cells.
enum class Kind : uint8_t { UNTYPED, INT, DATE, DECIMAL };

struct TypedColumn {
    Kind kind = Kind::UNTYPED;
    int scale = 0;                 // DECIMAL: fractional digit count
    size_t n_groups = 0;           // group_count() at build time
    std::vector<int64_t> vals;     // n_groups * PaxGroup::kRows; only visible
                                   // slots are meaningful (rest are 0).
};

// A compiled typed predicate over one typed column (all preds in a Filter are
// ANDed). `vals` points into a TypedColumn kept alive by Filter::holders.
struct Pred {
    enum Op : uint8_t { LT, LE, GT, GE, EQ, BETWEEN } op;
    const int64_t* vals;
    int64_t a;   // threshold, or BETWEEN lo
    int64_t b;   // BETWEEN hi
};

struct Filter {
    std::vector<Pred> preds;
    std::vector<std::shared_ptr<const TypedColumn>> holders;
};

// Build (or fetch from cache) the typed int64 array for (store, column).
// Adds elapsed build nanoseconds to *build_ns when a (re)build happened.
// Returns a column whose kind is UNTYPED when any visible cell fails typing.
std::shared_ptr<const TypedColumn> GetTypedColumn(
    LineairDB::Pax::PaxStore* store, uint32_t column, uint64_t* build_ns);

// Total bytes of int64 arrays currently held by the process-wide cache.
size_t CacheBytes();

// Compile scan.filter().expr() into a fully-typed AND-of-comparisons Filter.
// Returns false (leaving *out untouched) when any conjunct is not a simple
// comparison between a typed column and a convertible constant — the caller
// then evaluates on the byte path. *build_ns accumulates typed-array build
// time for the columns it touched.
bool Compile(const LineairDB::Protocol::FilterExpr& expr,
             LineairDB::Pax::PaxStore* store, Filter* out, uint64_t* build_ns);

// Evaluate all preds over the 64 slots at absolute base `abs_base`
// (= group_idx * kRows + block_base). Returns a 64-bit mask: bit s set means
// slot abs_base+s satisfies every pred. Invisible slots are the caller's
// concern (AND the result with the visibility mask).
uint64_t EvalBlock64Scalar(const Filter& f, size_t abs_base);
uint64_t EvalBlock64Avx2(const Filter& f, size_t abs_base);  // needs avx2 cpu

}  // namespace simd
}  // namespace qb

#endif  // SIMD_SCAN_HH
