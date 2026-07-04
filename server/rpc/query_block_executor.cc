#include "query_block_executor.hh"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <set>
#include <type_traits>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include "predicate_evaluator.hh"
#include "zone_map.hh"

namespace qb {

// Sub-block execution with an externally-injected semi-join key filter.
void ExecuteQueryBlockFiltered(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteQueryBlock::Request& request,
    LineairDB::Protocol::TxExecuteQueryBlock::Response* response,
    const std::unordered_set<std::string>* ext_keys,
    uint32_t ext_filter_table, uint32_t ext_filter_column);

namespace {

using LineairDB::Pax::PaxGroup;
using LineairDB::Pax::PaxStore;
namespace pb = LineairDB::Protocol;

// ---------------------------------------------------------------------------
// Exact-decimal arithmetic. Mirrors the Dec machinery in lineairdb_rpc.cc
// (kept in sync by the md5 gates; the row format stores val_str() ASCII).
// ---------------------------------------------------------------------------
struct Dec {
    __int128 m = 0;  // mantissa
    int s = 0;       // scale
    bool null = true;
};

inline __int128 dec_pow10(int n) {
    __int128 v = 1;
    while (n-- > 0) v *= 10;
    return v;
}

Dec dec_parse(std::string_view text) {
    Dec d;
    if (text.empty()) return d;
    size_t i = 0;
    bool neg = false;
    if (text[0] == '-') {
        neg = true;
        i = 1;
    } else if (text[0] == '+') {
        i = 1;
    }
    __int128 m = 0;
    int scale = 0;
    bool seen_dot = false;
    bool any = false;
    for (; i < text.size(); ++i) {
        const char c = text[i];
        if (c == '.') {
            if (seen_dot) return d;
            seen_dot = true;
            continue;
        }
        if (c < '0' || c > '9') return d;
        m = m * 10 + (c - '0');
        if (seen_dot) scale++;
        any = true;
    }
    if (!any) return d;
    d.m = neg ? -m : m;
    d.s = scale;
    d.null = false;
    return d;
}

void dec_addsub(Dec& a, const Dec& b, bool sub) {
    const int s = a.s > b.s ? a.s : b.s;
    __int128 am = a.m * dec_pow10(s - a.s);
    __int128 bm = b.m * dec_pow10(s - b.s);
    a.m = sub ? am - bm : am + bm;
    a.s = s;
    a.null = a.null || b.null;
}

std::string dec_format(const Dec& d) {
    __int128 m = d.m;
    bool neg = m < 0;
    if (neg) m = -m;
    std::string digits;
    if (m == 0)
        digits = "0";
    else {
        while (m) {
            digits.push_back(char('0' + int(m % 10)));
            m /= 10;
        }
        std::reverse(digits.begin(), digits.end());
    }
    while (static_cast<int>(digits.size()) <= d.s)
        digits.insert(digits.begin(), '0');
    std::string out;
    if (neg) out.push_back('-');
    if (d.s == 0) {
        out += digits;
        return out;
    }
    const size_t ip = digits.size() - d.s;
    out.append(digits, 0, ip);
    out.push_back('.');
    out.append(digits, ip, std::string::npos);
    return out;
}

// MySQL decimal division: scale(a/b) = a.scale + div_precision_increment(4),
// round half up (validated by the md5 gates).
Dec dec_div(const Dec& a, const Dec& b, int out_scale) {
    Dec r;
    if (a.null || b.null || b.m == 0) return r;
    __int128 num = a.m;
    __int128 den = b.m;
    bool neg = false;
    if (num < 0) {
        num = -num;
        neg = !neg;
    }
    if (den < 0) {
        den = -den;
        neg = !neg;
    }
    // Scale numerator so quotient has out_scale + 1 digits, then round.
    const int shift = out_scale + b.s - a.s + 1;
    if (shift > 0)
        num *= dec_pow10(shift);
    else if (shift < 0)
        num /= dec_pow10(-shift);
    __int128 q = num / den;
    q = (q + 5) / 10;
    r.m = neg ? -q : q;
    r.s = out_scale;
    r.null = false;
    return r;
}

// MySQL AVG: scale = arg_scale + div_precision_increment(4), round half up.
Dec dec_divide(const Dec& sum, uint64_t count, int out_scale) {
    Dec r;
    if (sum.null || count == 0) return r;
    __int128 num = sum.m;
    bool neg = num < 0;
    if (neg) num = -num;
    // Scale numerator to out_scale + 1 digits for rounding.
    const int extra = out_scale - sum.s + 1;
    if (extra > 0)
        num *= dec_pow10(extra);
    else if (extra < 0)
        num /= dec_pow10(-extra);  // (never happens for AVG: out>arg scale)
    __int128 q = num / static_cast<__int128>(count);
    // Round half up on the extra digit.
    q = (q + 5) / 10;
    r.m = neg ? -q : q;
    r.s = out_scale;
    r.null = false;
    return r;
}

// ---------------------------------------------------------------------------
// Row references and cell access.
// ---------------------------------------------------------------------------
constexpr uint64_t kNullRef = ~uint64_t{0};  // LEFT-join no-match marker

// Typed-key fast path (E13): canonical INT cells parse losslessly to
// int64, and canonical forms are unique per value, so value equality on
// int64 is exactly byte equality on the cells. A set/table switches to
// the int64 representation only when EVERY key converts with a
// full-length parse; one failure (DECIMAL, empty/NULL, overflow) keeps
// the byte-comparison path, so semantics never change.
inline bool parse_i64(std::string_view v, int64_t* out) {
    if (v.empty()) return false;
    const char* first = v.data();
    const char* last = v.data() + v.size();
    auto res = std::from_chars(first, last, *out);
    return res.ec == std::errc() && res.ptr == last;
}

inline bool to_int_set(const std::unordered_set<std::string>& in,
                       std::unordered_set<int64_t>* out) {
    out->reserve(in.size());
    for (const auto& k : in) {
        int64_t v;
        if (!parse_i64(k, &v)) {
            out->clear();
            return false;
        }
        out->insert(v);
    }
    return true;
}

inline std::string_view cell_of(const PaxStore* store, uint64_t ref,
                                uint32_t column) {
    const PaxGroup* g = store->group(ref / PaxGroup::kRows);
    return g->cell(column + 1,
                   static_cast<uint32_t>(ref % PaxGroup::kRows));
}

// ---------------------------------------------------------------------------
// Typed cell decoding (M2). A cell view from a typed column holds fixed-width
// LE binary; these decode it to the numeric domain. Schema-driven: the caller
// always knows the column kind (never sniffs the bytes — a typed int whose LE
// bytes happen to be ASCII digits would otherwise mis-parse), so every typed
// read is exact. UNTYPED falls back to the ASCII parse (byte-identical to pre-M2).
// ---------------------------------------------------------------------------
namespace fk = LineairDB::Pax;  // FK_UNTYPED / FK_INT32 / ...

inline bool decode_cell_i64(std::string_view v, uint8_t kind, int64_t* out) {
    if (v.empty()) return false;  // SQL NULL
    switch (kind) {
        case fk::FK_INT32: {
            int32_t x;
            std::memcpy(&x, v.data(), 4);
            *out = x;
            return true;
        }
        case fk::FK_INT64: {
            int64_t x;
            std::memcpy(&x, v.data(), 8);
            *out = x;
            return true;
        }
        case fk::FK_UNTYPED:
            return parse_i64(v, out);  // UNTYPED ASCII integer
        default:
            // FK_DATE / FK_DEC64 are NOT int-key material: their canonical
            // ASCII ("YYYY-MM-DD", "1.50") does not parse_i64, so an UNTYPED
            // (derived/ASCII) copy of the same value would take the string
            // key_view path. Returning false here forces the typed cell down
            // the SAME string path — keeping hash-join build/probe int-ness
            // decisions symmetric (review finding 1: a typed-DATE build side
            // vs an ASCII-DATE probe side would otherwise drop matches). This
            // also covers M2b's DEC64 before it is activated.
            return false;
    }
}

// Append the exact val_str ASCII of a typed cell view to *out. Must match the
// engine's FormatTyped byte-for-byte (round-trip / md5 contract).
inline void format_typed_view(uint8_t kind, int scale, std::string_view v,
                              std::string* out) {
    switch (kind) {
        case fk::FK_INT32: {
            int32_t x;
            std::memcpy(&x, v.data(), 4);
            out->append(std::to_string(x));
            break;
        }
        case fk::FK_INT64: {
            int64_t x;
            std::memcpy(&x, v.data(), 8);
            out->append(std::to_string(x));
            break;
        }
        case fk::FK_DATE: {
            int32_t x;
            std::memcpy(&x, v.data(), 4);
            char b[16];
            int n = std::snprintf(b, sizeof(b), "%04d-%02d-%02d", x / 10000,
                                  (x / 100) % 100, x % 100);
            if (n > 0) out->append(b, static_cast<size_t>(n));
            break;
        }
        case fk::FK_DEC64: {
            int64_t x;
            std::memcpy(&x, v.data(), 8);
            Dec d;
            d.m = x;
            d.s = scale;
            d.null = false;
            out->append(dec_format(d));
            break;
        }
        default:
            out->append(v.data(), v.size());
            break;
    }
}

// Decode any numeric cell to an exact Dec (typed int/date -> scale 0; DEC64 ->
// scaled int + `scale`; UNTYPED -> dec_parse of the ASCII).
inline Dec decode_cell_dec(std::string_view v, uint8_t kind, int scale) {
    if (v.empty()) return {};  // NULL
    switch (kind) {
        case fk::FK_INT32:
        case fk::FK_DATE: {
            int32_t x;
            std::memcpy(&x, v.data(), 4);
            Dec d;
            d.m = x;
            d.s = 0;
            d.null = false;
            return d;
        }
        case fk::FK_INT64: {
            int64_t x;
            std::memcpy(&x, v.data(), 8);
            Dec d;
            d.m = x;
            d.s = 0;
            d.null = false;
            return d;
        }
        case fk::FK_DEC64: {
            int64_t x;
            std::memcpy(&x, v.data(), 8);
            Dec d;
            d.m = x;
            d.s = scale;
            d.null = false;
            return d;
        }
        default:
            return dec_parse(v);  // UNTYPED ASCII
    }
}

// Evaluate a FilterExpr arithmetic tree (COLUMN_REF/CONST/ADD/SUB/MUL/NEG).
// `store`/`ref` address the default table; a COLUMN_REF may target another
// table via (table_idx << 16 | column) when `ctx` row context is given.
struct ArithRowCtx {
    const std::vector<PaxStore*>* stores;
    const std::vector<uint32_t>* tables;      // table_idx per ref column
    const std::vector<std::vector<uint64_t>>* refs;
    size_t row;
};

Dec eval_arith(const pb::FilterExpr& e, const PaxStore* store, uint64_t ref,
               const ArithRowCtx* ctx = nullptr) {
    using FE = pb::FilterExpr;
    switch (e.op()) {
        case FE::COLUMN_REF: {
            const uint32_t enc = e.column_index();
            if (enc < (1u << 16) || ctx == nullptr)
                return decode_cell_dec(cell_of(store, ref, enc),
                                       store->schema().kind_of(enc + 1),
                                       store->schema().scale_of(enc + 1));
            const uint32_t t = enc >> 16;
            const uint32_t col = enc & 0xFFFF;
            for (size_t i = 0; i < ctx->tables->size(); ++i) {
                if ((*ctx->tables)[i] == t) {
                    const uint64_t r = (*ctx->refs)[i][ctx->row];
                    if (r == kNullRef) return {};
                    const PaxStore* s = (*ctx->stores)[t];
                    return decode_cell_dec(cell_of(s, r, col),
                                           s->schema().kind_of(col + 1),
                                           s->schema().scale_of(col + 1));
                }
            }
            return {};
        }
        case FE::CONST_INT: {
            Dec d;
            d.m = e.int_val();
            d.s = 0;
            d.null = false;
            return d;
        }
        case FE::CONST_UINT: {
            Dec d;
            d.m = static_cast<__int128>(e.uint_val());
            d.s = 0;
            d.null = false;
            return d;
        }
        case FE::OP_ADD:
        case FE::OP_SUB: {
            if (e.children_size() != 2) return {};
            Dec a = eval_arith(e.children(0), store, ref, ctx);
            Dec b = eval_arith(e.children(1), store, ref, ctx);
            if (a.null || b.null) return {};
            dec_addsub(a, b, e.op() == FE::OP_SUB);
            return a;
        }
        case FE::OP_MUL: {
            if (e.children_size() != 2) return {};
            Dec a = eval_arith(e.children(0), store, ref, ctx);
            Dec b = eval_arith(e.children(1), store, ref, ctx);
            if (a.null || b.null) return {};
            Dec r;
            r.m = a.m * b.m;
            r.s = a.s + b.s;
            r.null = false;
            return r;
        }
        case FE::OP_NEG: {
            if (e.children_size() != 1) return {};
            Dec a = eval_arith(e.children(0), store, ref, ctx);
            if (!a.null) a.m = -a.m;
            return a;
        }
        default:
            return {};
    }
}

// Append one field in the proxy row format ([byteSize][len LE][bytes]).
void emit_field(std::string& row, std::string_view payload, bool is_null) {
    if (is_null || payload.empty()) {
        row.push_back(static_cast<char>(0xFF));
        return;
    }
    uint32_t len = static_cast<uint32_t>(payload.size());
    uint32_t prefix = 0;
    for (uint32_t v = len; v > 0; v /= 256) prefix++;
    row.push_back(static_cast<char>(prefix));
    for (uint32_t i = 0; i < prefix; i++)
        row.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
    row.append(payload.data(), payload.size());
}

// ---------------------------------------------------------------------------
// E17 — zone-map strip pruning.
//
// A prunable AND-conjunct of a scan filter is lowered into a PruneClause that
// compares a strip's per-column [min,max] range (from zone_map) against a
// constant (or another column's range). A strip is skipped only when a clause
// proves NO row in it can match; when in doubt the strip is scanned. The
// constant is converted into the column's int64 domain so the range test is
// byte-path-equivalent (compare_type gates INT<->{0,1}, DECIMAL<->2, DATE<->3),
// and DECIMAL bounds are widened by +/-1 scaled unit (conservative).
// ---------------------------------------------------------------------------
struct PruneClause {
    int op;         // FilterExpr::OP_LT/LE/GT/GE/EQ/OP_BETWEEN
    int zi_a;       // zone-snapshot index for column A
    int zi_b = -1;  // zone-snapshot index for column B (col-vs-col), else -1
    int64_t a = 0;  // const threshold(s) in the column's int64 domain
    int64_t b = 0;
};

// compare_type must match the classified kind for the range test to mirror the
// byte path (e.g. a string column with numeric-looking cells is compare_type 3
// and must NOT be pruned with numeric ranges).
inline bool zone_ct_ok(zone::ZKind k, uint32_t ct) {
    switch (k) {
        case zone::ZKind::INT: return ct == 0 || ct == 1;
        case zone::ZKind::DECIMAL: return ct == 2;
        case zone::ZKind::DATE: return ct == 3;
        default: return false;
    }
}

inline int zone_flip_op(int op) {
    using FE = pb::FilterExpr;
    switch (op) {
        case FE::OP_LT: return FE::OP_GT;
        case FE::OP_LE: return FE::OP_GE;
        case FE::OP_GT: return FE::OP_LT;
        case FE::OP_GE: return FE::OP_LE;
        default: return op;  // EQ stays EQ
    }
}

// Convert a constant into an INT/DATE column's exact int64 value. INT accepts
// int/uint/integral-double/numeric-string; DATE accepts a "YYYY-MM-DD" string.
inline bool zone_const_int(const pb::FilterExpr& c, zone::ZKind kind,
                           int64_t* out) {
    using FE = pb::FilterExpr;
    if (kind == zone::ZKind::INT) {
        // Only exact-integer constants are byte-path-equivalent for an INT
        // column. A CONST_DOUBLE makes the byte path promote the INT cell to
        // double (predicate_evaluator compare()), which loses precision above
        // 2^53; a CONST_STRING makes the byte path compare as STRING, not
        // numeric. Neither can be mirrored by an integer zone, so we fall back
        // to the byte path (no prune) — reviewer I1 / Codex findings.
        switch (c.op()) {
            case FE::CONST_INT: *out = c.int_val(); return true;
            case FE::CONST_UINT:
                if (c.uint_val() > static_cast<uint64_t>(INT64_MAX))
                    return false;
                *out = static_cast<int64_t>(c.uint_val());
                return true;
            default: return false;
        }
    }
    // DATE: the constant is the date's string form (proxy serializes temporal
    // constants via val_str()).
    if (c.op() != FE::CONST_STRING) return false;
    zone::ZKind ck;
    int cs;
    int64_t v;
    if (!zone::ClassifyCell(std::string_view(c.string_val()), &ck, &cs, &v) ||
        ck != zone::ZKind::DATE)
        return false;
    *out = v;
    return true;
}

// Extract a DECIMAL constant as the double the byte path compares (strtod).
inline bool zone_const_double(const pb::FilterExpr& c, double* d) {
    using FE = pb::FilterExpr;
    switch (c.op()) {
        case FE::CONST_INT: *d = static_cast<double>(c.int_val()); return true;
        case FE::CONST_UINT: *d = static_cast<double>(c.uint_val()); return true;
        case FE::CONST_DOUBLE: *d = c.double_val(); return true;
        default: return false;  // string on a decimal column -> byte path
    }
}

// A DECIMAL constant is only convertible when c * 10^scale stays well inside
// int64, so DecLower/DecUpper's llround and +/-1 widening cannot overflow or
// diverge. TPC-H decimals are far inside this bound (reviewer S1 / Codex).
inline bool zone_dec_ok(double c, int scale) {
    double p = 1.0;
    for (int i = 0; i < scale; ++i) p *= 10.0;
    return std::isfinite(c) && std::fabs(c) * p < 9.0e18;
}

// Lower `column op const` bounds into PruneClause::a/b. For DECIMAL the exact
// scaled-int cut points (DecLower/DecUpper) are widened outward by 1 unit so
// pruning stays strictly conservative regardless of double rounding.
inline bool zone_lower_cmp(int op, const pb::FilterExpr& cst, zone::ZKind kind,
                           int scale, int64_t* a, int64_t* b) {
    using FE = pb::FilterExpr;
    if (kind == zone::ZKind::DECIMAL) {
        double c;
        if (!zone_const_double(cst, &c) || !zone_dec_ok(c, scale)) return false;
        switch (op) {
            case FE::OP_LT: *a = zone::DecLower(c, scale) + 1; return true;
            case FE::OP_LE: *a = zone::DecUpper(c, scale) + 1; return true;
            case FE::OP_GT: *a = zone::DecUpper(c, scale) - 1; return true;
            case FE::OP_GE: *a = zone::DecLower(c, scale) - 1; return true;
            case FE::OP_EQ:
                *a = zone::DecLower(c, scale) - 1;
                *b = zone::DecUpper(c, scale) + 1;
                return true;
            default: return false;
        }
    }
    int64_t c;
    if (!zone_const_int(cst, kind, &c)) return false;
    *a = c;
    if (op == FE::OP_EQ) *b = c;
    return true;
}

// Lower `column BETWEEN lo AND hi` bounds into PruneClause::a/b.
inline bool zone_lower_between(const pb::FilterExpr& lo,
                               const pb::FilterExpr& hi, zone::ZKind kind,
                               int scale, int64_t* a, int64_t* b) {
    if (kind == zone::ZKind::DECIMAL) {
        double dlo, dhi;
        if (!zone_const_double(lo, &dlo) || !zone_const_double(hi, &dhi))
            return false;
        if (!zone_dec_ok(dlo, scale) || !zone_dec_ok(dhi, scale)) return false;
        *a = zone::DecLower(dlo, scale) - 1;
        *b = zone::DecUpper(dhi, scale) + 1;
        return true;
    }
    return zone_const_int(lo, kind, a) && zone_const_int(hi, kind, b);
}

// Lower one comparison/BETWEEN conjunct. `getz` returns the zone-snapshot index
// for a column (building it on demand) and its kind/scale, or -1 when the
// column is not prunable. Returns true (and fills *pc) only for a fully
// convertible, compare_type-compatible conjunct.
using ZoneGetZ = std::function<int(uint32_t, zone::ZKind*, int*)>;
bool zone_lower_conjunct(const pb::FilterExpr& e, const ZoneGetZ& getz,
                         PruneClause* pc) {
    using FE = pb::FilterExpr;
    int op = e.op();
    if (op == FE::OP_BETWEEN) {
        if (e.negated() || e.children_size() != 3) return false;
        const FE& col = e.children(0);
        if (col.op() != FE::COLUMN_REF) return false;
        zone::ZKind k;
        int sc;
        const int zi = getz(col.column_index(), &k, &sc);
        if (zi < 0 || !zone_ct_ok(k, col.compare_type())) return false;
        int64_t a, b;
        if (!zone_lower_between(e.children(1), e.children(2), k, sc, &a, &b))
            return false;
        pc->op = FE::OP_BETWEEN;
        pc->zi_a = zi;
        pc->zi_b = -1;
        pc->a = a;
        pc->b = b;
        return true;
    }
    if (op != FE::OP_LT && op != FE::OP_LE && op != FE::OP_GT &&
        op != FE::OP_GE && op != FE::OP_EQ)
        return false;
    if (e.children_size() != 2) return false;
    const FE* lhs = &e.children(0);
    const FE* rhs = &e.children(1);
    // Normalize `const op column` -> `column op' const`.
    if (lhs->op() != FE::COLUMN_REF && rhs->op() == FE::COLUMN_REF) {
        std::swap(lhs, rhs);
        op = zone_flip_op(op);
    }
    if (lhs->op() != FE::COLUMN_REF) return false;
    zone::ZKind ka;
    int sca;
    const int zia = getz(lhs->column_index(), &ka, &sca);
    if (zia < 0 || !zone_ct_ok(ka, lhs->compare_type())) return false;

    if (rhs->op() == FE::COLUMN_REF) {
        // Column-vs-column: prunable only when both ranges live in the same
        // int64 domain (same kind+scale). DECIMAL col-vs-col is left to the
        // byte path (no widened boundary defined for a range-vs-range test).
        zone::ZKind kb;
        int scb;
        const int zib = getz(rhs->column_index(), &kb, &scb);
        if (zib < 0 || !zone_ct_ok(kb, rhs->compare_type())) return false;
        if (ka != kb || sca != scb || ka == zone::ZKind::DECIMAL) return false;
        pc->op = op;
        pc->zi_a = zia;
        pc->zi_b = zib;
        return true;
    }
    int64_t a = 0, b = 0;
    if (!zone_lower_cmp(op, *rhs, ka, sca, &a, &b)) return false;
    pc->op = op;
    pc->zi_a = zia;
    pc->zi_b = -1;
    pc->a = a;
    pc->b = b;
    return true;
}

// Lower the prunable conjuncts of a scan filter (a top-level AND, or a lone
// comparison). Non-prunable conjuncts (OR/IN/LIKE/NE/IS NULL/negated BETWEEN,
// or unconvertible ones) are simply omitted.
void zone_lower_clauses(const pb::FilterExpr& expr, const ZoneGetZ& getz,
                        std::vector<PruneClause>* out) {
    using FE = pb::FilterExpr;
    if (expr.op() == FE::OP_AND) {
        for (const auto& ch : expr.children()) {
            PruneClause pc;
            if (zone_lower_conjunct(ch, getz, &pc)) out->push_back(pc);
        }
    } else {
        PruneClause pc;
        if (zone_lower_conjunct(expr, getz, &pc)) out->push_back(pc);
    }
}

// True if strip `g` provably contains no row matching the filter: some clause's
// range test is empty over the strip. Invalid (non-typed) strip zones never
// prune. Reads immutable request-local snapshots — safe from worker threads.
inline bool zone_strip_pruned(
    size_t g, const std::vector<PruneClause>& prune,
    const std::vector<std::vector<zone::StripZone>>& zsnaps) {
    using FE = pb::FilterExpr;
    for (const PruneClause& c : prune) {
        const zone::StripZone& za = zsnaps[c.zi_a][g];
        if (za.state != zone::ZS_VALID) continue;
        if (c.zi_b < 0) {
            switch (c.op) {
                case FE::OP_LT: if (za.zmin >= c.a) return true; break;
                case FE::OP_LE: if (za.zmin > c.a) return true; break;
                case FE::OP_GT: if (za.zmax <= c.a) return true; break;
                case FE::OP_GE: if (za.zmax < c.a) return true; break;
                case FE::OP_EQ:
                    if (za.zmax < c.a || za.zmin > c.b) return true;
                    break;
                case FE::OP_BETWEEN:
                    if (za.zmax < c.a || za.zmin > c.b) return true;
                    break;
            }
        } else {
            const zone::StripZone& zb = zsnaps[c.zi_b][g];
            if (zb.state != zone::ZS_VALID) continue;
            switch (c.op) {
                case FE::OP_LT: if (za.zmin >= zb.zmax) return true; break;
                case FE::OP_LE: if (za.zmin > zb.zmax) return true; break;
                case FE::OP_GT: if (za.zmax <= zb.zmin) return true; break;
                case FE::OP_GE: if (za.zmax < zb.zmin) return true; break;
                case FE::OP_EQ:
                    if (za.zmax < zb.zmin || zb.zmax < za.zmin) return true;
                    break;
            }
        }
    }
    return false;
}

// ===========================================================================
// Vectorized scan filter (F6). The per-row scan path calls
// set_row_from_pax_cols (assigns num_columns string-views + copies the null
// flags for EVERY visible row of a wide table) and then walks the FilterExpr
// proto tree in PredicateEvaluator::evaluate, boxing a Val per node; perf
// attributes 25-50% of q1/q4/q6/q10/q19/q20/q21 to that chain. This compiles
// the pushed FilterExpr ONCE per scan into a plain struct tree with constants
// pre-decoded, then evaluates it column-at-a-time over a selection vector of
// slots (better strip locality; no per-row set_row / proto access).
//
// Correctness contract: byte-identical to the per-row path for EVERY pushable
// expression. Each compiled node is either (a) a specialized typed loop that
// PROVABLY mirrors extract_value + compare + evaluate for its exact shape, or
// (b) a per-slot FALLBACK that calls the reference PredicateEvaluator on the
// SAME proto subtree — so any unsupported shape stays identical by
// construction. When in doubt the compiler emits a FALLBACK; specialization is
// a pure optimization (a prior spike showed the typed loop cuts q6 by 42%).
//
// The compare()/date-helper logic below is a line-for-line DUPLICATE of
// PredicateEvaluator (predicate_evaluator.cc). It MUST stay in sync — any
// divergence in the promotion / NULL / DATE rules would break the contract.
// ===========================================================================
namespace vf {

using LineairDB::Pax::TableSchema;

enum class VT { NONE, INT, UINT, DOUBLE, STRING, DATE };
// Mirror of PredicateEvaluator::Val (same fields; STRING/const views point at
// the group cell arena or the persistent proto, both stable for the scan).
struct VVal {
    VT type = VT::NONE;
    int64_t i = 0;
    uint64_t u = 0;
    double d = 0.0;
    std::string_view s;
};

// --- DATE helpers: exact copies of PredicateEvaluator's. ---
inline bool date_operand_to_int(const VVal& v, int64_t* out) {
    if (v.type == VT::DATE || v.type == VT::INT) { *out = v.i; return true; }
    if (v.type == VT::UINT) { *out = static_cast<int64_t>(v.u); return true; }
    if (v.type == VT::STRING) {
        const std::string_view s = v.s;
        if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
            int64_t y = 0, m = 0, d = 0;
            auto dig = [](const char* p, int n, int64_t* o) {
                for (int i = 0; i < n; i++) {
                    if (p[i] < '0' || p[i] > '9') return false;
                    *o = *o * 10 + (p[i] - '0');
                }
                return true;
            };
            if (dig(s.data(), 4, &y) && dig(s.data() + 5, 2, &m) &&
                dig(s.data() + 8, 2, &d)) {
                *out = y * 10000 + m * 100 + d;
                return true;
            }
        }
        return false;
    }
    return false;
}
inline std::string_view date_operand_to_sv(const VVal& v, char* buf, size_t n) {
    if (v.type == VT::DATE) {
        const int32_t x = static_cast<int32_t>(v.i);
        int len = std::snprintf(buf, n, "%04d-%02d-%02d", x / 10000,
                                (x / 100) % 100, x % 100);
        return std::string_view(buf, len > 0 ? static_cast<size_t>(len) : 0);
    }
    return v.s;
}

// --- compare(): exact copy of PredicateEvaluator::compare. -1/0/1 for </==/>,
// -2 when either operand is NONE (NULL cmp X -> unknown). ---
inline int compare(const VVal& lhs, const VVal& rhs) {
    if (lhs.type == VT::NONE || rhs.type == VT::NONE) return -2;

    if (lhs.type == VT::DATE || rhs.type == VT::DATE) {
        int64_t li, ri;
        if (date_operand_to_int(lhs, &li) && date_operand_to_int(rhs, &ri))
            return (li < ri) ? -1 : (li > ri) ? 1 : 0;
        char lb[16], rb[16];
        const std::string_view ls = date_operand_to_sv(lhs, lb, sizeof(lb));
        const std::string_view rs = date_operand_to_sv(rhs, rb, sizeof(rb));
        const int c = ls.compare(rs);
        return (c < 0) ? -1 : (c > 0) ? 1 : 0;
    }

    if (lhs.type == VT::STRING || rhs.type == VT::STRING) {
        std::string_view ls = lhs.s, rs = rhs.s;
        if (lhs.type == VT::STRING && rhs.type == VT::STRING) {
            int r = ls.compare(rs);
            return (r < 0) ? -1 : (r > 0) ? 1 : 0;
        }
        return ls.compare(rs) < 0 ? -1 : ls.compare(rs) > 0 ? 1 : 0;
    }

    double dl, dr;
    if (lhs.type == VT::DOUBLE || rhs.type == VT::DOUBLE) {
        dl = (lhs.type == VT::DOUBLE) ? lhs.d
             : (lhs.type == VT::INT)  ? static_cast<double>(lhs.i)
                                      : static_cast<double>(lhs.u);
        dr = (rhs.type == VT::DOUBLE) ? rhs.d
             : (rhs.type == VT::INT)  ? static_cast<double>(rhs.i)
                                      : static_cast<double>(rhs.u);
        return (dl < dr) ? -1 : (dl > dr) ? 1 : 0;
    }

    if (lhs.type == VT::INT && rhs.type == VT::INT)
        return (lhs.i < rhs.i) ? -1 : (lhs.i > rhs.i) ? 1 : 0;
    if (lhs.type == VT::UINT && rhs.type == VT::UINT)
        return (lhs.u < rhs.u) ? -1 : (lhs.u > rhs.u) ? 1 : 0;
    int64_t li = (lhs.type == VT::INT) ? lhs.i : static_cast<int64_t>(lhs.u);
    int64_t ri = (rhs.type == VT::INT) ? rhs.i : static_cast<int64_t>(rhs.u);
    return (li < ri) ? -1 : (li > ri) ? 1 : 0;
}

// ---------------------------------------------------------------------------
// Compiled node tree. AND/OR/NOT recurse; the leaves are either a specialized
// column op (col vs pre-decoded const[s]) or a FALLBACK subtree pointer.
// ---------------------------------------------------------------------------
struct Node {
    enum K { AND, OR, NOT, CMP, BETWEEN, IN, IS_NULL, IS_NOT_NULL, FALLBACK };
    K kind = FALLBACK;
    // Specialized-column metadata (CMP/BETWEEN/IN/IS_NULL/IS_NOT_NULL):
    uint32_t col = 0;                 // 0-based column ordinal; cell field = +1
    uint8_t colkind = fk::FK_UNTYPED; // FK_* storage kind of the column
    int scale = 0;                    // DEC64 scale
    uint32_t ctype = 0;               // COLUMN_REF compare_type hint
    int op = 0;                       // pb::FilterExpr::Op (CMP only)
    bool negated = false;             // BETWEEN / IN
    std::vector<VVal> consts;         // CMP:1 (rhs) BETWEEN:2 (lo,hi) IN:N
    std::vector<Node> children;       // AND/OR/NOT
    const pb::FilterExpr* fb = nullptr;  // FALLBACK subtree (stable proto ref)
};

inline bool is_const(const pb::FilterExpr& e) {
    switch (e.op()) {
        case pb::FilterExpr::CONST_INT:
        case pb::FilterExpr::CONST_UINT:
        case pb::FilterExpr::CONST_DOUBLE:
        case pb::FilterExpr::CONST_STRING:
        case pb::FilterExpr::CONST_NULL:
            return true;
        default:
            return false;
    }
}

// Pre-decode a CONST_* node exactly as extract_value would (STRING view points
// into the persistent proto's string_val, stable for the scan's lifetime).
inline VVal const_val(const pb::FilterExpr& e) {
    VVal v;
    switch (e.op()) {
        case pb::FilterExpr::CONST_INT:
            v.type = VT::INT;
            v.i = e.int_val();
            break;
        case pb::FilterExpr::CONST_UINT:
            v.type = VT::UINT;
            v.u = e.uint_val();
            break;
        case pb::FilterExpr::CONST_DOUBLE:
            v.type = VT::DOUBLE;
            v.d = e.double_val();
            break;
        case pb::FilterExpr::CONST_STRING:
            v.type = VT::STRING;
            v.s = std::string_view(
                reinterpret_cast<const char*>(e.string_val().data()),
                e.string_val().size());
            break;
        default:  // CONST_NULL
            v.type = VT::NONE;
            break;
    }
    return v;
}

// Can extract_value's COLUMN_REF decode for (kind, compare_type) be reproduced
// by col_val() below? INT32/INT64 with compare_type==3 boxes a to_string(x)
// STRING (rotating fmtbuf) -> fall back. UNTYPED numeric (ct 0/1/2) runs
// strtoll/strtod on a non-terminated cell -> fall back. DATE/DEC64 ignore
// compare_type. UNTYPED string compare (ct 3) is a plain byte compare.
inline bool specializable(uint8_t kind, uint32_t ct) {
    switch (kind) {
        case fk::FK_INT32:
        case fk::FK_INT64:
            return ct != 3;
        case fk::FK_DATE:
        case fk::FK_DEC64:
            return true;
        case fk::FK_UNTYPED:
            return ct == 3;
        default:
            return false;
    }
}

// Decode one column cell into a VVal, mirroring extract_value(COLUMN_REF)
// exactly for the specialized shapes. Empty-cell handling is kind-independent
// (matches extract_value): null bit set -> NONE, else empty STRING.
inline VVal col_val(const Node& n, const PaxGroup& grp, uint32_t slot) {
    VVal v;
    const std::string_view col = grp.cell(n.col + 1, slot);
    if (col.empty()) {
        const std::string_view nf = grp.cell(0, slot);
        const uint32_t bp = n.col >> 3, bit = n.col & 7;
        if (bp < nf.size() &&
            (static_cast<uint8_t>(nf[bp]) & (1u << bit))) {
            v.type = VT::NONE;  // NULL
            return v;
        }
        v.type = VT::STRING;  // empty but not null
        v.s = col;
        return v;
    }
    switch (n.colkind) {
        case fk::FK_DATE: {
            int32_t x;
            std::memcpy(&x, col.data(), 4);
            v.type = VT::DATE;
            v.i = x;
            return v;
        }
        case fk::FK_INT32:
        case fk::FK_INT64: {
            int64_t x;
            if (n.colkind == fk::FK_INT32) {
                int32_t t;
                std::memcpy(&t, col.data(), 4);
                x = t;
            } else {
                std::memcpy(&x, col.data(), 8);
            }
            switch (n.ctype) {  // ct==3 is never compiled here (see specializable)
                case 1:
                    v.type = VT::UINT;
                    v.u = static_cast<uint64_t>(x);
                    return v;
                case 2:
                    v.type = VT::DOUBLE;
                    v.d = static_cast<double>(x);
                    return v;
                default:  // SIGNED_INT
                    v.type = VT::INT;
                    v.i = x;
                    return v;
            }
        }
        case fk::FK_DEC64: {
            int64_t m;
            std::memcpy(&m, col.data(), 8);
            double p = 1.0;  // same repeated-multiply loop as extract_value
            for (int k = 0; k < n.scale; ++k) p *= 10.0;
            v.type = VT::DOUBLE;
            v.d = static_cast<double>(m) / p;
            return v;
        }
        default:  // FK_UNTYPED, ct==3: raw-cell byte string compare
            v.type = VT::STRING;
            v.s = col;
            return v;
    }
}

// Per-call evaluation context (thread-local; the Node tree is read-only shared).
struct EvalCtx {
    const PaxGroup* grp;
    PredicateEvaluator* ev;
    uint32_t num_columns;
    const std::vector<uint32_t>* filter_cols;
    bool failed = false;
};

// Ascending sorted union of two ascending unique selections.
inline std::vector<uint16_t> merge_union(const std::vector<uint16_t>& a,
                                         const std::vector<uint16_t>& b) {
    if (a.empty()) return b;
    if (b.empty()) return a;
    std::vector<uint16_t> out;
    out.reserve(a.size() + b.size());
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i] < b[j]) out.push_back(a[i++]);
        else if (b[j] < a[i]) out.push_back(b[j++]);
        else { out.push_back(a[i++]); ++j; }
    }
    while (i < a.size()) out.push_back(a[i++]);
    while (j < b.size()) out.push_back(b[j++]);
    return out;
}

// Evaluate a node over the ascending selection `sel`; return the ascending
// subset that satisfies it (output order == input order, preserved exactly).
std::vector<uint16_t> eval_node(const Node& n, std::vector<uint16_t> sel,
                                EvalCtx& ctx) {
    switch (n.kind) {
        case Node::AND: {
            // Fold sequentially over the shrinking selection (short-circuits
            // the same rows as evaluate()'s per-row AND).
            for (const Node& c : n.children) {
                sel = eval_node(c, std::move(sel), ctx);
                if (ctx.failed || sel.empty()) return sel;
            }
            return sel;
        }
        case Node::OR: {
            // A row matches if ANY child matches -> union of per-child matches
            // over the input selection (ascending preserved by merge_union).
            std::vector<uint16_t> acc;
            for (const Node& c : n.children) {
                std::vector<uint16_t> m = eval_node(c, sel, ctx);
                if (ctx.failed) return sel;
                acc = merge_union(acc, m);
            }
            return acc;
        }
        case Node::NOT: {
            // sel_out = sel_in \ eval(child, sel_in): mirrors !evaluate(child)
            // exactly because the child evaluation is itself mirrored.
            std::vector<uint16_t> m = eval_node(n.children[0], sel, ctx);
            if (ctx.failed) return sel;
            size_t w = 0, j = 0;
            for (size_t r = 0; r < sel.size(); ++r) {
                while (j < m.size() && m[j] < sel[r]) ++j;
                if (j < m.size() && m[j] == sel[r]) { ++j; continue; }
                sel[w++] = sel[r];
            }
            sel.resize(w);
            return sel;
        }
        case Node::CMP: {
            const VVal& rhs = n.consts[0];
            size_t w = 0;
            for (size_t r = 0; r < sel.size(); ++r) {
                const VVal lhs = col_val(n, *ctx.grp, sel[r]);
                const int cmp = compare(lhs, rhs);
                bool keep;
                if (cmp == -2) {
                    keep = false;  // NULL -> unknown -> exclude
                } else {
                    switch (n.op) {
                        case pb::FilterExpr::OP_EQ: keep = cmp == 0; break;
                        case pb::FilterExpr::OP_NE: keep = cmp != 0; break;
                        case pb::FilterExpr::OP_LT: keep = cmp < 0; break;
                        case pb::FilterExpr::OP_LE: keep = cmp <= 0; break;
                        case pb::FilterExpr::OP_GT: keep = cmp > 0; break;
                        default: keep = cmp >= 0; break;  // OP_GE
                    }
                }
                if (keep) sel[w++] = sel[r];
            }
            sel.resize(w);
            return sel;
        }
        case Node::BETWEEN: {
            size_t w = 0;
            for (size_t r = 0; r < sel.size(); ++r) {
                const VVal v = col_val(n, *ctx.grp, sel[r]);
                const int cl = compare(v, n.consts[0]);
                const int ch = compare(v, n.consts[1]);
                bool keep;
                if (cl == -2 || ch == -2) {
                    keep = false;  // NULL -> false REGARDLESS of negated
                } else {
                    const bool res = (cl >= 0 && ch <= 0);
                    keep = n.negated ? !res : res;
                }
                if (keep) sel[w++] = sel[r];
            }
            sel.resize(w);
            return sel;
        }
        case Node::IN: {
            size_t w = 0;
            for (size_t r = 0; r < sel.size(); ++r) {
                const VVal v = col_val(n, *ctx.grp, sel[r]);
                bool keep;
                if (v.type == VT::NONE) {
                    keep = false;  // NULL IN (...) -> false (even if negated)
                } else {
                    bool matched = false;
                    for (const VVal& it : n.consts)
                        if (compare(v, it) == 0) { matched = true; break; }
                    keep = matched ? !n.negated : n.negated;
                }
                if (keep) sel[w++] = sel[r];
            }
            sel.resize(w);
            return sel;
        }
        case Node::IS_NULL:
        case Node::IS_NOT_NULL: {
            // NONE <=> empty cell AND null bit set (col < num_columns, so the
            // idx-out-of-range NONE case never applies) -> kind-independent.
            const bool want_null = (n.kind == Node::IS_NULL);
            size_t w = 0;
            for (size_t r = 0; r < sel.size(); ++r) {
                const std::string_view c = ctx.grp->cell(n.col + 1, sel[r]);
                bool isnull = false;
                if (c.empty()) {
                    const std::string_view nf = ctx.grp->cell(0, sel[r]);
                    const uint32_t bp = n.col >> 3, bit = n.col & 7;
                    isnull = bp < nf.size() &&
                             (static_cast<uint8_t>(nf[bp]) & (1u << bit));
                }
                if (isnull == want_null) sel[w++] = sel[r];
            }
            sel.resize(w);
            return sel;
        }
        case Node::FALLBACK: {
            // Reference path on the exact proto subtree: byte-identical by
            // construction. set_row false == schema mismatch -> fail the scan
            // (preserves the per-row path's failed[] semantics).
            size_t w = 0;
            for (size_t r = 0; r < sel.size(); ++r) {
                if (!ctx.ev->set_row_from_pax_cols(*ctx.grp, sel[r],
                                                   ctx.num_columns,
                                                   *ctx.filter_cols)) {
                    ctx.failed = true;
                    return sel;
                }
                if (ctx.ev->evaluate(*n.fb)) sel[w++] = sel[r];
            }
            sel.resize(w);
            return sel;
        }
    }
    return sel;
}

// Compile a FilterExpr subtree into a Node. `ncols` == filter num_columns
// (columns >= ncols decode to NONE in extract_value, so they can't be a
// specialized column -> FALLBACK reproduces that). Any shape not provably
// mirrored becomes a FALLBACK over its own proto node.
Node compile(const pb::FilterExpr& e, uint32_t ncols, const TableSchema& sch) {
    using FE = pb::FilterExpr;
    Node n;
    auto fallback = [&]() {
        Node f;
        f.kind = Node::FALLBACK;
        f.fb = &e;
        return f;
    };
    switch (e.op()) {
        case FE::OP_AND: {
            if (e.children_size() < 1) return fallback();
            n.kind = Node::AND;
            for (const auto& c : e.children())
                n.children.push_back(compile(c, ncols, sch));
            return n;
        }
        case FE::OP_OR: {
            if (e.children_size() < 1) return fallback();
            n.kind = Node::OR;
            for (const auto& c : e.children())
                n.children.push_back(compile(c, ncols, sch));
            return n;
        }
        case FE::OP_NOT: {
            if (e.children_size() < 1) return fallback();
            n.kind = Node::NOT;
            n.children.push_back(compile(e.children(0), ncols, sch));
            return n;
        }
        case FE::OP_EQ:
        case FE::OP_NE:
        case FE::OP_LT:
        case FE::OP_LE:
        case FE::OP_GT:
        case FE::OP_GE: {
            if (e.children_size() < 2) return fallback();
            const auto& c0 = e.children(0);
            if (c0.op() != FE::COLUMN_REF || !is_const(e.children(1)))
                return fallback();
            if (c0.column_index() >= ncols) return fallback();
            const uint32_t col = c0.column_index();
            const uint8_t kind = sch.kind_of(col + 1);
            const uint32_t ct = c0.compare_type();
            if (!specializable(kind, ct)) return fallback();
            n.kind = Node::CMP;
            n.col = col;
            n.colkind = kind;
            n.scale = sch.scale_of(col + 1);
            n.ctype = ct;
            n.op = e.op();
            n.consts.push_back(const_val(e.children(1)));
            return n;
        }
        case FE::OP_BETWEEN: {
            if (e.children_size() < 3) return fallback();
            const auto& c0 = e.children(0);
            if (c0.op() != FE::COLUMN_REF || !is_const(e.children(1)) ||
                !is_const(e.children(2)))
                return fallback();
            if (c0.column_index() >= ncols) return fallback();
            const uint32_t col = c0.column_index();
            const uint8_t kind = sch.kind_of(col + 1);
            const uint32_t ct = c0.compare_type();
            if (!specializable(kind, ct)) return fallback();
            n.kind = Node::BETWEEN;
            n.col = col;
            n.colkind = kind;
            n.scale = sch.scale_of(col + 1);
            n.ctype = ct;
            n.negated = e.negated();
            n.consts.push_back(const_val(e.children(1)));
            n.consts.push_back(const_val(e.children(2)));
            return n;
        }
        case FE::OP_IN: {
            if (e.children_size() < 2) return fallback();
            const auto& c0 = e.children(0);
            if (c0.op() != FE::COLUMN_REF) return fallback();
            if (c0.column_index() >= ncols) return fallback();
            for (int i = 1; i < e.children_size(); ++i)
                if (!is_const(e.children(i))) return fallback();
            const uint32_t col = c0.column_index();
            const uint8_t kind = sch.kind_of(col + 1);
            const uint32_t ct = c0.compare_type();
            if (!specializable(kind, ct)) return fallback();
            n.kind = Node::IN;
            n.col = col;
            n.colkind = kind;
            n.scale = sch.scale_of(col + 1);
            n.ctype = ct;
            n.negated = e.negated();
            for (int i = 1; i < e.children_size(); ++i)
                n.consts.push_back(const_val(e.children(i)));
            return n;
        }
        case FE::OP_IS_NULL:
        case FE::OP_IS_NOT_NULL: {
            if (e.children_size() < 1) return fallback();
            const auto& c0 = e.children(0);
            if (c0.op() != FE::COLUMN_REF) return fallback();
            if (c0.column_index() >= ncols) return fallback();
            n.kind = (e.op() == FE::OP_IS_NULL) ? Node::IS_NULL
                                                : Node::IS_NOT_NULL;
            n.col = c0.column_index();  // null check is kind-independent
            return n;
        }
        default:
            // OP_LIKE, column-on-RHS / column-op-column comparisons, arithmetic.
            return fallback();
    }
}

// A compiled filter program, built once per RunScan and shared read-only.
struct Program {
    Node root;
    uint32_t num_columns = 0;

    void build(const pb::FilterExpr& e, uint32_t ncols,
               const TableSchema& sch) {
        num_columns = ncols;
        // A schema too small for the filter fails the per-row path
        // (set_row_from_pax_cols returns false). Route the WHOLE expr to a
        // single FALLBACK so run() reproduces that failure AND never lets a
        // specialized loop read an out-of-range cell strip.
        if (sch.field_count() < static_cast<size_t>(ncols) + 1) {
            root.kind = Node::FALLBACK;
            root.fb = &e;
            return;
        }
        root = compile(e, ncols, sch);
    }

    // Filter `sel` (ascending slots). *failed set on a schema mismatch in a
    // fallback node (mirrors the per-row path's failed[w] on set_row false).
    std::vector<uint16_t> run(std::vector<uint16_t> sel, const PaxGroup& grp,
                              PredicateEvaluator& ev,
                              const std::vector<uint32_t>& fcols,
                              bool* failed) const {
        EvalCtx ctx{&grp, &ev, num_columns, &fcols, false};
        sel = eval_node(root, std::move(sel), ctx);
        *failed = ctx.failed;
        return sel;
    }
};

// Kill switch (read once): LDBC_VEC=0 forces the reference per-row scan path.
inline bool enabled() {
    static const bool on = [] {
        const char* e = std::getenv("LDBC_VEC");
        return !(e != nullptr && e[0] == '0' && e[1] == '\0');
    }();
    return on;
}

}  // namespace vf

// ---------------------------------------------------------------------------
// Executor state.
// ---------------------------------------------------------------------------

// Materialized operator output: tuple = one row-ref per participating table.
struct NodeResult {
    std::vector<uint32_t> tables;               // table_idx per ref column
    std::vector<std::vector<uint64_t>> refs;    // refs[i][row]
    size_t rows() const { return refs.empty() ? 0 : refs[0].size(); }
    int table_pos(uint32_t table_idx) const {
        for (size_t i = 0; i < tables.size(); ++i)
            if (tables[i] == table_idx) return static_cast<int>(i);
        return -1;
    }
};

struct Executor {
    LineairDB::Database* db;
    const pb::TxExecuteQueryBlock::Request& req;
    std::string error;

    std::vector<PaxStore*> stores;            // per request table
    std::vector<std::vector<uint64_t>> wc_snapshots;  // per table, per group
    std::vector<NodeResult> results;          // per node

    // Virtual tables: sub-block results in plan order. Virtual table k has
    // table_idx = req.tables_size() + k; refs are row numbers.
    struct VirtualTable {
        std::vector<std::vector<std::string>> vals;  // [row][col]
        std::vector<std::vector<bool>> nulls;
    };
    std::vector<VirtualTable> virtuals;

    // External semi-join filter injected by a parent executor: rows of
    // ext_filter_table must have ext_filter_column's cell in ext_keys.
    const std::unordered_set<std::string>* ext_keys = nullptr;
    uint32_t ext_filter_table = 0;
    uint32_t ext_filter_column = 0;

    // Build the key set of an executed node's output column.
    bool collect_keys(const pb::QbSemiFilter& semi,
                      std::unordered_set<std::string>* out) {
        if (semi.source_node() >= results.size())
            return fail("semi source out of range");
        const NodeResult& src = results[semi.source_node()];
        const int pos = src.table_pos(semi.source_column().table_idx());
        if (pos < 0) return fail("semi source table");
        const uint32_t tbl = semi.source_column().table_idx();
        const uint32_t col = semi.source_column().column();
        out->reserve(src.rows());
        std::string buf;  // typed -> canonical ASCII
        for (size_t r = 0; r < src.rows(); ++r) {
            const uint64_t ref = src.refs[pos][r];
            if (ref == kNullRef) continue;
            std::string_view v = key_view(tbl, ref, col, buf);
            if (!v.empty()) out->emplace(v);
        }
        return true;
    }

    // Typed key collection (E16): build the semi source's key set directly
    // in the int64 representation, skipping the intermediate
    // unordered_set<std::string> + separate to_int_set() re-parse that
    // dominated collect_keys in the semi-heavy queries (perf: q21/q10).
    // Every non-null cell is parsed once; the first cell that does not
    // convert (DECIMAL/non-canonical) makes the collection fall back to an
    // owned-string set (a second pass, rare — only for non-INT key columns),
    // so the probe semantics are byte-for-byte identical to collect_keys +
    // to_int_set. On success exactly one of *iout (when *is_int) or *sout is
    // populated.
    bool collect_keys_typed(const pb::QbSemiFilter& semi,
                            std::unordered_set<int64_t>* iout,
                            std::unordered_set<std::string>* sout,
                            bool* is_int) {
        if (semi.source_node() >= results.size())
            return fail("semi source out of range");
        const NodeResult& src = results[semi.source_node()];
        const uint32_t tbl = semi.source_column().table_idx();
        const uint32_t col = semi.source_column().column();
        const int pos = src.table_pos(tbl);
        if (pos < 0) return fail("semi source table");
        const uint8_t kind = column_kind(tbl, col);
        const int scale = column_scale(tbl, col);
        iout->reserve(src.rows());
        *is_int = true;
        std::string buf;
        for (size_t r = 0; r < src.rows(); ++r) {
            const uint64_t ref = src.refs[pos][r];
            if (ref == kNullRef) continue;
            const std::string_view v = value_of(tbl, ref, col);
            if (v.empty()) continue;
            int64_t iv;
            if (!decode_cell_i64(v, kind, &iv)) {
                // Non-INT key column: abandon the int set and re-collect as
                // owned canonical-ASCII strings (matches the byte-comparison
                // probe path; typed cells are formatted, not raw binary).
                *is_int = false;
                iout->clear();
                sout->reserve(src.rows());
                for (size_t r2 = 0; r2 < src.rows(); ++r2) {
                    const uint64_t ref2 = src.refs[pos][r2];
                    if (ref2 == kNullRef) continue;
                    const std::string_view v2 = value_of(tbl, ref2, col);
                    if (v2.empty()) continue;
                    if (kind == fk::FK_UNTYPED) {
                        sout->emplace(v2);
                    } else {
                        buf.clear();
                        format_typed_view(kind, scale, v2, &buf);
                        sout->emplace(buf);
                    }
                }
                return true;
            }
            iout->insert(iv);
        }
        return true;
    }

    bool is_virtual(uint32_t table_idx) const {
        return table_idx >= static_cast<uint32_t>(req.tables_size());
    }
    // Unified cell access across real PAX tables and virtual tables.
    std::string_view value_of(uint32_t table_idx, uint64_t ref,
                              uint32_t column) const {
        if (!is_virtual(table_idx))
            return cell_of(stores[table_idx], ref, column);
        const VirtualTable& vt =
            virtuals[table_idx - req.tables_size()];
        if (ref >= vt.vals.size() || column >= vt.vals[ref].size())
            return {};
        return vt.vals[ref][column];
    }
    bool null_of(uint32_t table_idx, uint64_t ref, uint32_t column) const {
        if (!is_virtual(table_idx)) return false;  // PAX: empty cell = null
        const VirtualTable& vt =
            virtuals[table_idx - req.tables_size()];
        if (ref >= vt.nulls.size() || column >= vt.nulls[ref].size())
            return true;
        return vt.nulls[ref][column];
    }

    // ----- Typed cell access (M2) ------------------------------------------
    // Storage kind / DECIMAL scale of a real-table column (virtual/derived
    // tables carry ASCII, so UNTYPED). `column` is 0-based; field index = +1.
    uint8_t column_kind(uint32_t t, uint32_t col) const {
        if (is_virtual(t)) return fk::FK_UNTYPED;
        return stores[t]->schema().kind_of(static_cast<size_t>(col) + 1);
    }
    int column_scale(uint32_t t, uint32_t col) const {
        if (is_virtual(t)) return 0;
        return stores[t]->schema().scale_of(static_cast<size_t>(col) + 1);
    }
    // Read an int/date column (or an UNTYPED ASCII integer) to int64. Returns
    // false for SQL NULL or a non-integer UNTYPED cell.
    bool read_i64(uint32_t t, uint64_t ref, uint32_t col, int64_t* out) const {
        return decode_cell_i64(value_of(t, ref, col), column_kind(t, col), out);
    }
    // Read any numeric column as an exact Dec.
    Dec read_dec(uint32_t t, uint64_t ref, uint32_t col) const {
        return decode_cell_dec(value_of(t, ref, col), column_kind(t, col),
                               column_scale(t, col));
    }
    // Canonical ASCII (== val_str) of a cell: verbatim for UNTYPED, formatted
    // for typed. `buf` backs the view for typed cells (own storage per call).
    std::string_view key_view(uint32_t t, uint64_t ref, uint32_t col,
                              std::string& buf) const {
        const std::string_view v = value_of(t, ref, col);
        const uint8_t k = column_kind(t, col);
        if (k == fk::FK_UNTYPED || v.empty()) return v;
        buf.clear();
        format_typed_view(k, column_scale(t, col), v, &buf);
        return buf;
    }

    unsigned workers() const {
        const unsigned hw = std::thread::hardware_concurrency();
        return std::min<unsigned>(hw ? hw : 8, 32);
    }

    bool fail(const std::string& why) {
        if (error.empty()) error = why;
        return false;
    }

    bool Prepare() {
        stores.resize(req.tables_size(), nullptr);
        wc_snapshots.resize(req.tables_size());
        for (int i = 0; i < req.tables_size(); ++i) {
            PaxStore* s = db->GetPaxStore(req.tables(i).table_name());
            if (s == nullptr) return fail("table has no PAX store");
            if (s->overflow_count() > 0) return fail("table has overflow rows");
            stores[i] = s;
            const size_t n = s->group_count();
            auto& snap = wc_snapshots[i];
            snap.resize(n);
            for (size_t g = 0; g < n; ++g) {
                PaxGroup* grp = s->group(g);
                snap[g] = grp ? grp->write_counter.load(
                                    std::memory_order_acquire)
                              : 0;
            }
        }
        return true;
    }

    bool Quiesced() {
        for (int i = 0; i < req.tables_size(); ++i) {
            // A row that overflows to the heap mid-scan retires its strip slot
            // (BumpOverflow -> RetireSlot); the visible-bit clear makes it
            // silently absent from a strip-direct pass. Prepare() proved
            // overflow_count()==0 at the start, so re-checking the monotonic
            // latch here rejects any 0->1 transition that raced the snapshot
            // and falls the block back to the TID-checked path. (String cells
            // sized to char_length() make this reachable for genuine multibyte
            // data; ASCII never overflows so this never fires.) The happens-
            // before is carried by the same write_counter release/acquire the
            // loop below already relies on: RetireSlot's release bump is
            // sequenced after BumpOverflow, so if the snapshot acquire-loaded
            // the post-retire counter (the only case the loop below passes),
            // this later load observes the overflow.
            if (stores[i]->overflow_count() > 0) return false;
            const auto& snap = wc_snapshots[i];
            for (size_t g = 0; g < snap.size(); ++g) {
                PaxGroup* grp = stores[i]->group(g);
                const uint64_t now =
                    grp ? grp->write_counter.load(std::memory_order_acquire)
                        : 0;
                if (now != snap[g]) return false;
            }
        }
        return true;
    }

    // ----- Scan ------------------------------------------------------------
    bool RunScan(const pb::QbScan& scan, NodeResult* out) {
        if (scan.table_idx() >= static_cast<uint32_t>(req.tables_size()))
            return fail("scan table out of range");
        PaxStore* store = stores[scan.table_idx()];
        const size_t n_groups = store->group_count();
        out->tables = {scan.table_idx()};
        out->refs.resize(1);
        if (n_groups == 0) return true;

        // Semi-join key filters: from the plan (earlier node's keys) and/or
        // injected by a parent executor (sub-block domain restriction).
        // The plan-side set is collected directly in its typed form (E16):
        // int64 when every key parses (semi_int), else an owned-string set.
        std::unordered_set<std::string> semi_keys;
        std::unordered_set<int64_t> semi_ikeys, ext_ikeys;
        const std::unordered_set<std::string>* semi_set = nullptr;
        bool semi_active = false;
        bool semi_int = false;
        uint32_t semi_col = 0;
        // Typed kind/scale of the probed column (M2): the probe decodes typed
        // cells directly, and formats them to canonical ASCII for a string set.
        uint8_t semi_col_kind = fk::FK_UNTYPED;
        int semi_col_scale = 0;
        if (scan.has_semi()) {
            bool is_int = false;
            if (!collect_keys_typed(scan.semi(), &semi_ikeys, &semi_keys,
                                    &is_int))
                return false;
            // A huge key set costs more to probe than it prunes (the
            // proxy's chained estimates can be wrong in either direction).
            const size_t ksz = is_int ? semi_ikeys.size() : semi_keys.size();
            if (ksz <= (size_t{4} << 20)) {
                semi_active = true;
                semi_int = is_int;
                if (!is_int) semi_set = &semi_keys;
                semi_col = scan.semi().my_column();
                semi_col_kind = column_kind(scan.table_idx(), semi_col);
                semi_col_scale = column_scale(scan.table_idx(), semi_col);
            }
        }
        const bool use_ext = ext_keys != nullptr &&
                             ext_filter_table == scan.table_idx();
        // Typed-key probes (E13): int64 set when every injected key converts.
        const bool ext_int = use_ext && to_int_set(*ext_keys, &ext_ikeys);
        const uint8_t ext_col_kind =
            use_ext ? column_kind(scan.table_idx(), ext_filter_column)
                    : fk::FK_UNTYPED;
        const int ext_col_scale =
            use_ext ? column_scale(scan.table_idx(), ext_filter_column) : 0;

        // Dense bitmap probes (F5): TPC-H int keys (orderkey/partkey/suppkey)
        // are dense in [min,max], so an exact bitset answers membership with
        // one range check + one bit test instead of a hash probe (LIP-style
        // sideways information passing, exact rather than Bloom). Falls back
        // to the hash set when the span exceeds the cap (32MB of bits).
        struct KeyBitmap {
            std::vector<uint64_t> bits;
            int64_t base = 0;
            bool ok = false;
            void build(const std::unordered_set<int64_t>& s) {
                if (s.empty()) return;
                int64_t kmin = INT64_MAX, kmax = INT64_MIN;
                for (int64_t v : s) {
                    kmin = std::min(kmin, v);
                    kmax = std::max(kmax, v);
                }
                const uint64_t span =
                    static_cast<uint64_t>(kmax) - static_cast<uint64_t>(kmin);
                if (span >= (uint64_t{256} << 20)) return;  // > 2^28 bits
                // Density gate (F5c3 regression lesson): semi key sets are
                // usually PRE-FILTERED and sparse (q18: 57 keys over a 6M
                // span). A sparse bitmap trades an L1-resident hash set for
                // L2-sized random bit probes and loses (+100ms on q18). Only
                // build when fill >= 1/64 — where the bitmap is at least as
                // compact as the set and probe locality wins (and where the
                // 4M-key SIP cap would otherwise bite at SF>=10).
                if (span > s.size() * 64) return;
                bits.assign((span + 64) / 64, 0);
                base = kmin;
                for (int64_t v : s) {
                    const uint64_t o = static_cast<uint64_t>(v) -
                                       static_cast<uint64_t>(base);
                    bits[o >> 6] |= uint64_t{1} << (o & 63);
                }
                ok = true;
            }
            bool test(int64_t v) const {
                const uint64_t o =
                    static_cast<uint64_t>(v) - static_cast<uint64_t>(base);
                return o < bits.size() * 64 &&
                       (bits[o >> 6] >> (o & 63)) & 1;
            }
        };
        KeyBitmap semi_bm, ext_bm;
        if (semi_active && semi_int) semi_bm.build(semi_ikeys);
        if (use_ext && ext_int) ext_bm.build(ext_ikeys);

        const bool has_filter =
            scan.has_filter() && scan.filter().has_expr();
        // Fetch only the cells the filter actually references — a wide
        // table's full-row load dominated filtered scans (perf: q20).
        std::vector<uint32_t> filter_cols;
        if (has_filter)
            PredicateEvaluator::collect_columns(scan.filter().expr(),
                                                &filter_cols);

        // E17: lower prunable filter conjuncts into per-strip range tests over
        // cached zone maps. Zone snapshots are request-local (immutable during
        // the parallel scan below); a strip proven empty is skipped entirely.
        std::vector<std::vector<zone::StripZone>> zsnaps;
        std::vector<zone::ZKind> col_kind;
        std::vector<int> col_scale;
        std::unordered_map<uint32_t, int> col_zi;
        std::vector<PruneClause> prune;
        if (has_filter && zone::Enabled()) {
            ZoneGetZ getz = [&](uint32_t col, zone::ZKind* k, int* sc) -> int {
                auto it = col_zi.find(col);
                if (it != col_zi.end()) {
                    if (col_kind[it->second] == zone::ZKind::UNTYPED) return -1;
                    *k = col_kind[it->second];
                    *sc = col_scale[it->second];
                    return it->second;
                }
                const int idx = static_cast<int>(zsnaps.size());
                std::vector<zone::StripZone> zs;
                zone::ZKind kk;
                int ss;
                if (!zone::GetColumnZones(store, col, n_groups, &zs, &kk, &ss)) {
                    zsnaps.emplace_back();  // placeholder (never indexed)
                    col_kind.push_back(zone::ZKind::UNTYPED);
                    col_scale.push_back(0);
                    col_zi[col] = idx;
                    return -1;
                }
                zsnaps.push_back(std::move(zs));
                col_kind.push_back(kk);
                col_scale.push_back(ss);
                col_zi[col] = idx;
                *k = kk;
                *sc = ss;
                return idx;
            };
            zone_lower_clauses(scan.filter().expr(), getz, &prune);
        }

        // F6: compile the pushed filter ONCE into a vectorized program shared
        // read-only by the workers (per-slot fallback covers any shape it can't
        // specialize, so this is always safe). LDBC_VEC=0 keeps the per-row
        // path. store->schema() is the shape every group in this table shares.
        const bool vec_active = has_filter && vf::enabled();
        vf::Program vprog;
        if (vec_active)
            vprog.build(scan.filter().expr(), scan.filter().num_columns(),
                        store->schema());

        const unsigned wc =
            static_cast<unsigned>(std::min<size_t>(workers(), n_groups));
        std::vector<std::vector<uint64_t>> locals(wc);
        std::vector<char> failed(wc, 0);
        std::vector<std::thread> pool;
        pool.reserve(wc);
        for (unsigned w = 0; w < wc; ++w) {
            pool.emplace_back([&, w] {
                PredicateEvaluator ev;
                auto& mine = locals[w];
                std::string probe;  // reused key buffer: no per-row alloc
                // F6: collect the visible + semi/ext-passing slots of a strip
                // into an ascending selection, then run the filter over it
                // column-at-a-time. Slots stay ascending within the strip, so
                // the emitted refs keep the exact per-row-path order.
                std::vector<uint16_t> sel;
                sel.reserve(PaxGroup::kRows);
                for (size_t g = w; g < n_groups; g += wc) {
                    PaxGroup* grp = store->group(g);
                    if (grp == nullptr) continue;
                    if (!prune.empty() &&
                        zone_strip_pruned(g, prune, zsnaps))
                        continue;  // E17: strip provably has no matching row
                    sel.clear();
                    for (uint32_t base = 0; base < PaxGroup::kRows;
                         base += 64) {
                        uint64_t bits = 0;
                        for (uint32_t s = 0; s < 64; ++s)
                            if (grp->IsVisible(base + s))
                                bits |= (uint64_t{1} << s);
                        while (bits != 0) {
                            const uint32_t s = static_cast<uint32_t>(
                                __builtin_ctzll(bits));
                            bits &= bits - 1;
                            const uint32_t slot = base + s;
                            if (semi_active) {
                                const std::string_view kv =
                                    grp->cell(semi_col + 1, slot);
                                if (semi_int) {
                                    int64_t v;
                                    if (!decode_cell_i64(kv, semi_col_kind,
                                                         &v))
                                        continue;
                                    if (semi_bm.ok ? !semi_bm.test(v)
                                                   : semi_ikeys.count(v) == 0)
                                        continue;
                                } else {
                                    probe.clear();
                                    if (semi_col_kind == fk::FK_UNTYPED)
                                        probe.assign(kv.data(), kv.size());
                                    else if (!kv.empty())
                                        format_typed_view(semi_col_kind,
                                                          semi_col_scale, kv,
                                                          &probe);
                                    if (semi_set->count(probe) == 0)
                                        continue;
                                }
                            }
                            if (use_ext) {
                                const std::string_view kv = grp->cell(
                                    ext_filter_column + 1, slot);
                                if (ext_int) {
                                    int64_t v;
                                    if (!decode_cell_i64(kv, ext_col_kind,
                                                         &v))
                                        continue;
                                    if (ext_bm.ok ? !ext_bm.test(v)
                                                  : ext_ikeys.count(v) == 0)
                                        continue;
                                } else {
                                    probe.clear();
                                    if (ext_col_kind == fk::FK_UNTYPED)
                                        probe.assign(kv.data(), kv.size());
                                    else if (!kv.empty())
                                        format_typed_view(ext_col_kind,
                                                          ext_col_scale, kv,
                                                          &probe);
                                    if (ext_keys->count(probe) == 0)
                                        continue;
                                }
                            }
                            sel.push_back(static_cast<uint16_t>(slot));
                        }
                    }
                    if (has_filter) {
                        if (vec_active) {
                            bool fail_flag = false;
                            sel = vprog.run(std::move(sel), *grp, ev,
                                            filter_cols, &fail_flag);
                            if (fail_flag) {  // schema mismatch (set_row false)
                                failed[w] = 1;
                                return;
                            }
                        } else {
                            // Reference per-row path (LDBC_VEC=0) over the
                            // collected selection — identical to the pre-F6
                            // inline test, order preserved.
                            size_t keep = 0;
                            for (uint16_t slot : sel) {
                                if (!ev.set_row_from_pax_cols(
                                        *grp, slot,
                                        scan.filter().num_columns(),
                                        filter_cols)) {
                                    failed[w] = 1;
                                    return;
                                }
                                if (ev.evaluate(scan.filter().expr()))
                                    sel[keep++] = slot;
                            }
                            sel.resize(keep);
                        }
                    }
                    for (uint16_t slot : sel)
                        mine.push_back(g * PaxGroup::kRows + slot);
                }
            });
        }
        for (auto& t : pool) t.join();
        for (char f : failed)
            if (f) return fail("scan filter unevaluable");
        size_t total = 0;
        for (auto& l : locals) total += l.size();
        out->refs[0].reserve(total);
        for (auto& l : locals)
            out->refs[0].insert(out->refs[0].end(), l.begin(), l.end());
        return true;
    }

    // ----- Sub-blocks (derived tables) --------------------------------------
    bool RunSubBlock(const pb::QbSubBlock& sub, NodeResult* out) {
        std::unordered_set<std::string> outer_keys;
        const std::unordered_set<std::string>* inject = nullptr;
        if (sub.has_semi()) {
            if (!collect_keys(sub.semi(), &outer_keys)) return false;
            // A huge key set costs more to probe than it prunes.
            if (outer_keys.size() <= (size_t{4} << 20))
                inject = &outer_keys;
        }
        pb::TxExecuteQueryBlock::Response resp;
        ExecuteQueryBlockFiltered(db, sub.block(), &resp, inject,
                                  sub.target_table(), sub.target_column());
        if (!resp.ok())
            return fail(resp.error().empty() ? "sub-block failed"
                                             : resp.error());
        VirtualTable vt;
        vt.vals.reserve(resp.rows_size());
        vt.nulls.reserve(resp.rows_size());
        for (const std::string& row : resp.rows()) {
            // Proxy row format: [null placeholder field][col fields...].
            std::vector<std::string> vals;
            std::vector<bool> nulls;
            size_t off = 0;
            bool first = true;
            while (off < row.size()) {
                const uint8_t prefix = static_cast<uint8_t>(row[off]);
                off += 1;
                if (prefix == 0xFF) {
                    if (!first) {
                        vals.emplace_back();
                        nulls.push_back(true);
                    }
                    first = false;
                    continue;
                }
                if (off + prefix > row.size()) return fail("sub row");
                uint32_t len = 0;
                for (uint32_t b = 0; b < prefix; ++b)
                    len |= static_cast<uint32_t>(
                               static_cast<uint8_t>(row[off + b]))
                           << (8 * b);
                off += prefix;
                if (off + len > row.size()) return fail("sub row");
                if (!first) {
                    vals.emplace_back(row.substr(off, len));
                    nulls.push_back(false);
                }
                first = false;
                off += len;
            }
            vt.vals.push_back(std::move(vals));
            vt.nulls.push_back(std::move(nulls));
        }
        const uint32_t vidx = static_cast<uint32_t>(
            req.tables_size() + virtuals.size());
        virtuals.push_back(std::move(vt));
        out->tables = {vidx};
        out->refs.resize(1);
        const size_t n = virtuals.back().vals.size();
        out->refs[0].reserve(n);
        for (size_t r = 0; r < n; ++r) out->refs[0].push_back(r);
        return true;
    }

    // ----- Tuple filters ----------------------------------------------------
    // Resolve a QbTupleFilter against one tuple of `nr` (row r), evaluating
    // FilterExpr ordinals over the mapped (table, column) cells. LEFT-join
    // misses become SQL NULLs.
    struct TupleFilterCtx {
        std::vector<int> pos;            // per filter column: ref column pos
        std::vector<std::string_view> cells;
        std::vector<bool> nulls;
        // Per-column storage kind/scale: raw typed cells go straight to the
        // evaluator (same decode as the PAX schema_ path), skipping the old
        // format-to-ASCII/re-parse round trip (perf: q19). Virtual tables are
        // FK_UNTYPED (canonical ASCII), matching the previous behavior.
        std::vector<uint8_t> kinds;
        std::vector<int> scales;
    };

    bool prep_tuple_filter(const pb::QbTupleFilter& tf, const NodeResult& nr,
                           TupleFilterCtx* ctx) {
        ctx->pos.resize(tf.columns_size());
        ctx->kinds.resize(tf.columns_size());
        ctx->scales.resize(tf.columns_size());
        for (int i = 0; i < tf.columns_size(); ++i) {
            ctx->pos[i] = nr.table_pos(tf.columns(i).table_idx());
            if (ctx->pos[i] < 0) return fail("tuple filter table");
            ctx->kinds[i] =
                column_kind(tf.columns(i).table_idx(), tf.columns(i).column());
            ctx->scales[i] =
                column_scale(tf.columns(i).table_idx(), tf.columns(i).column());
        }
        ctx->cells.resize(tf.columns_size());
        ctx->nulls.resize(tf.columns_size());
        return true;
    }

    bool eval_tuple_filter(const pb::QbTupleFilter& tf, const NodeResult& nr,
                           size_t row, TupleFilterCtx* ctx,
                           PredicateEvaluator* ev) {
        for (int i = 0; i < tf.columns_size(); ++i) {
            const uint64_t ref = nr.refs[ctx->pos[i]][row];
            if (ref == kNullRef) {
                ctx->cells[i] = {};
                ctx->nulls[i] = true;
            } else {
                ctx->cells[i] = value_of(tf.columns(i).table_idx(), ref,
                                         tf.columns(i).column());
                ctx->nulls[i] =
                    null_of(tf.columns(i).table_idx(), ref,
                            tf.columns(i).column());
            }
        }
        ev->set_row_from_views_typed(ctx->cells, ctx->nulls, ctx->kinds,
                                     ctx->scales);
        return ev->evaluate(tf.pred().expr());
    }

    bool RunFilter(const pb::QbTupleFilterNode& fn, NodeResult* out) {
        if (fn.input() >= results.size()) return fail("filter child");
        const NodeResult& in = results[fn.input()];
        out->tables = in.tables;
        out->refs.assign(in.tables.size(), {});
        const size_t n = in.rows();
        const unsigned wc = static_cast<unsigned>(
            std::min<size_t>(workers(), std::max<size_t>(n / 16384, 1)));
        struct ChunkOut {
            std::vector<std::vector<uint64_t>> refs;
        };
        std::vector<ChunkOut> chunks(wc);
        std::vector<char> failed(wc, 0);
        std::vector<std::thread> pool;
        pool.reserve(wc);
        for (unsigned w = 0; w < wc; ++w) {
            pool.emplace_back([&, w] {
                TupleFilterCtx ctx;
                if (!prep_tuple_filter(fn.filter(), in, &ctx)) {
                    failed[w] = 1;
                    return;
                }
                PredicateEvaluator ev;
                auto& mine = chunks[w].refs;
                mine.assign(in.tables.size(), {});
                const size_t begin = n * w / wc;
                const size_t end = n * (w + 1) / wc;
                for (size_t r = begin; r < end; ++r) {
                    if (!eval_tuple_filter(fn.filter(), in, r, &ctx, &ev))
                        continue;
                    for (size_t c = 0; c < in.tables.size(); ++c)
                        mine[c].push_back(in.refs[c][r]);
                }
            });
        }
        for (auto& t : pool) t.join();
        for (char f : failed)
            if (f) return false;
        size_t total = 0;
        for (auto& ch : chunks)
            total += ch.refs.empty() ? 0 : ch.refs[0].size();
        for (size_t c = 0; c < out->refs.size(); ++c) {
            out->refs[c].reserve(total);
            for (auto& ch : chunks)
                out->refs[c].insert(out->refs[c].end(), ch.refs[c].begin(),
                                    ch.refs[c].end());
        }
        return true;
    }

    // ----- Join ------------------------------------------------------------
    static void append_join_key(std::string& key, std::string_view cell) {
        const uint32_t l = static_cast<uint32_t>(cell.size());
        key.append(reinterpret_cast<const char*>(&l), sizeof(l));
        key.append(cell.data(), cell.size());
    }

    bool RunJoin(const pb::QbJoin& join, NodeResult* out) {
        if (join.build() >= results.size() || join.probe() >= results.size())
            return fail("join child out of range");
        // INNER is symmetric: hash the smaller side regardless of what the
        // plan called build/probe.
        const bool swap =
            join.type() == pb::QbJoin::INNER &&
            results[join.build()].rows() > results[join.probe()].rows();
        const NodeResult& build = results[swap ? join.probe() : join.build()];
        const NodeResult& probe = results[swap ? join.build() : join.probe()];
        const auto& build_key_refs =
            swap ? join.probe_keys() : join.build_keys();
        const auto& probe_key_refs =
            swap ? join.build_keys() : join.probe_keys();
        if (join.build_keys_size() != join.probe_keys_size())
            return fail("join key arity");

        // Resolve key columns to (ref column position, column).
        struct KeyCol {
            int pos;
            uint32_t column;
            uint32_t table_idx;
        };
        std::vector<KeyCol> bk(build_key_refs.size()), pk(probe_key_refs.size());
        for (int i = 0; i < build_key_refs.size(); ++i) {
            const auto& c = build_key_refs.Get(i);
            const int pos = build.table_pos(c.table_idx());
            if (pos < 0) return fail("build key table not in child");
            bk[i] = {pos, c.column(), c.table_idx()};
        }
        for (int i = 0; i < probe_key_refs.size(); ++i) {
            const auto& c = probe_key_refs.Get(i);
            const int pos = probe.table_pos(c.table_idx());
            if (pos < 0) return fail("probe key table not in child");
            pk[i] = {pos, c.column(), c.table_idx()};
        }

        // Residual predicate over the (probe ++ build) tuple, applied per
        // key match. Pre-resolve each referenced column to its side.
        struct ResidualCol {
            bool from_build;
            int pos;
            uint32_t table_idx;
            uint32_t column;
        };
        std::vector<ResidualCol> residual_cols;
        const bool has_residual =
            join.has_residual() && join.residual().pred().has_expr();
        if (has_residual) {
            for (const auto& c : join.residual().columns()) {
                int pos = probe.table_pos(c.table_idx());
                if (pos >= 0) {
                    residual_cols.push_back(
                        {false, pos, c.table_idx(), c.column()});
                    continue;
                }
                pos = build.table_pos(c.table_idx());
                if (pos < 0) return fail("residual column table");
                residual_cols.push_back(
                    {true, pos, c.table_idx(), c.column()});
            }
        }

        // Witness summaries (F5, Moerkotte/Neumann groupjoin lineage): a
        // SEMI/ANTI join whose entire residual is one integer `a <> b`
        // between a build column and a probe column only needs, per key,
        // whether ANY build row's value differs from the probe value —
        // which {count, min, max} answers exactly: exists differing value
        // <=> count>0 && !(min==max==probe_value). Replaces the per-key
        // build-row vectors AND the per-candidate evaluator walk with one
        // integer test (q21: l2/l3 self-joins on l_orderkey with
        // l_suppkey <> l1.l_suppkey). NULL semantics match the generic
        // path: a NULL build value never satisfies `<>` (excluded from the
        // summary), a NULL probe value satisfies nothing (matched=false).
        // Gated to typed INT columns compared as signed ints (ct==0), where
        // the evaluator's compare is exactly int64 inequality.
        struct Wit {
            uint32_t cnt = 0;
            int64_t mn = 0, mx = 0;
        };
        std::unordered_map<int64_t, Wit> iwit;
        int wit_b = -1, wit_p = -1;  // residual_cols ordinals per side
        bool witness = false;
        if ((join.type() == pb::QbJoin::SEMI ||
             join.type() == pb::QbJoin::ANTI) &&
            has_residual && bk.size() == 1) {
            const auto& e = join.residual().pred().expr();
            if (e.op() == pb::FilterExpr::OP_NE && e.children_size() == 2 &&
                e.children(0).op() == pb::FilterExpr::COLUMN_REF &&
                e.children(1).op() == pb::FilterExpr::COLUMN_REF &&
                e.children(0).compare_type() == 0 &&
                e.children(1).compare_type() == 0) {
                const uint32_t o0 = e.children(0).column_index();
                const uint32_t o1 = e.children(1).column_index();
                if (o0 < residual_cols.size() && o1 < residual_cols.size() &&
                    residual_cols[o0].from_build !=
                        residual_cols[o1].from_build) {
                    wit_b = residual_cols[o0].from_build ? o0 : o1;
                    wit_p = residual_cols[o0].from_build ? o1 : o0;
                    const uint8_t kb =
                        column_kind(residual_cols[wit_b].table_idx,
                                    residual_cols[wit_b].column);
                    const uint8_t kp =
                        column_kind(residual_cols[wit_p].table_idx,
                                    residual_cols[wit_p].column);
                    witness = (kb == fk::FK_INT32 || kb == fk::FK_INT64) &&
                              (kp == fk::FK_INT32 || kp == fk::FK_INT64);
                }
            }
        }

        // Build hash table: key bytes -> build row indexes. Single-column
        // INT keys switch to an int64 table (E13); one non-converting key
        // falls back to byte keys, so match semantics are unchanged.
        std::unordered_map<std::string, std::vector<uint32_t>> ht;
        std::unordered_map<int64_t, std::vector<uint32_t>> iht;
        bool int_join = bk.size() == 1;
        if (int_join && witness) {
            iwit.reserve(build.rows());
            for (size_t r = 0; r < build.rows(); ++r) {
                int64_t v;
                if (!read_i64(bk[0].table_idx, build.refs[bk[0].pos][r],
                              bk[0].column, &v)) {
                    int_join = false;
                    witness = false;
                    iwit.clear();
                    break;
                }
                int64_t wv;
                const auto& rc = residual_cols[wit_b];
                if (!read_i64(rc.table_idx, build.refs[rc.pos][r], rc.column,
                              &wv))
                    continue;  // NULL witness value never satisfies `<>`
                Wit& w = iwit[v];
                if (w.cnt == 0) {
                    w.mn = w.mx = wv;
                } else {
                    w.mn = std::min(w.mn, wv);
                    w.mx = std::max(w.mx, wv);
                }
                ++w.cnt;
            }
        }
        if (int_join && !witness) {
            iht.reserve(build.rows());
            for (size_t r = 0; r < build.rows(); ++r) {
                int64_t v;
                if (!read_i64(bk[0].table_idx, build.refs[bk[0].pos][r],
                              bk[0].column, &v)) {
                    int_join = false;
                    iht.clear();
                    break;
                }
                iht[v].push_back(static_cast<uint32_t>(r));
            }
        }
        if (!int_join) {
            ht.reserve(build.rows());
            std::string key, kbuf;
            for (size_t r = 0; r < build.rows(); ++r) {
                key.clear();
                bool null_key = false;
                for (const auto& k : bk) {
                    // Canonical ASCII so typed/UNTYPED columns join
                    // consistently (a typed int and an ASCII int with the same
                    // value share the same key bytes).
                    const std::string_view kv = key_view(
                        k.table_idx, build.refs[k.pos][r], k.column, kbuf);
                    // SQL equijoin: NULL (empty cell) never matches —
                    // mirror MySQL's null-rejecting hash join (review I1;
                    // byte keys used to match empty==empty, diverging
                    // from the semi filters' NULL-drop semantics).
                    if (kv.empty()) {
                        null_key = true;
                        break;
                    }
                    append_join_key(key, kv);
                }
                if (null_key) continue;
                ht[key].push_back(static_cast<uint32_t>(r));
            }
        }

        // Output schema: probe tables ++ build tables (SEMI/ANTI: probe only).
        const bool keep_build = join.type() == pb::QbJoin::INNER ||
                                join.type() == pb::QbJoin::LEFT;
        out->tables = probe.tables;
        if (keep_build)
            out->tables.insert(out->tables.end(), build.tables.begin(),
                               build.tables.end());
        out->refs.assign(out->tables.size(), {});

        // Probe, chunk-parallel.
        const size_t n = probe.rows();
        const unsigned wc = static_cast<unsigned>(
            std::min<size_t>(workers(), std::max<size_t>(n / 16384, 1)));
        struct ChunkOut {
            std::vector<std::vector<uint64_t>> refs;
        };
        std::vector<ChunkOut> chunks(wc);
        std::vector<std::thread> pool;
        pool.reserve(wc);
        for (unsigned w = 0; w < wc; ++w) {
            pool.emplace_back([&, w] {
                auto& mine = chunks[w].refs;
                mine.assign(out->tables.size(), {});
                std::string key, kbuf;
                PredicateEvaluator ev;
                std::vector<std::string_view> rcells(residual_cols.size());
                std::vector<bool> rnulls(residual_cols.size(), false);
                // Raw typed cells + per-column kind/scale (same decode as the
                // PAX schema_ path) — no per-row ASCII format/re-parse.
                std::vector<uint8_t> rkinds(residual_cols.size());
                std::vector<int> rscales(residual_cols.size());
                for (size_t i = 0; i < residual_cols.size(); ++i) {
                    rkinds[i] = column_kind(residual_cols[i].table_idx,
                                            residual_cols[i].column);
                    rscales[i] = column_scale(residual_cols[i].table_idx,
                                              residual_cols[i].column);
                }
                // True when build row `br` passes the residual predicate
                // against probe row `r`.
                auto residual_ok = [&](size_t r, uint32_t br) {
                    if (!has_residual) return true;
                    for (size_t i = 0; i < residual_cols.size(); ++i) {
                        const auto& rc = residual_cols[i];
                        const uint64_t ref = rc.from_build
                                                 ? build.refs[rc.pos][br]
                                                 : probe.refs[rc.pos][r];
                        rcells[i] = value_of(rc.table_idx, ref, rc.column);
                        rnulls[i] = null_of(rc.table_idx, ref, rc.column);
                    }
                    ev.set_row_from_views_typed(rcells, rnulls, rkinds,
                                                rscales);
                    return ev.evaluate(join.residual().pred().expr());
                };
                const size_t begin = n * w / wc;
                const size_t end = n * (w + 1) / wc;
                for (size_t r = begin; r < end; ++r) {
                    if (witness) {
                        // Summary probe: one map find + one integer test
                        // replaces the candidate walk (semantics proof at
                        // the Wit declaration).
                        bool wmatched = false;
                        int64_t v;
                        if (read_i64(pk[0].table_idx,
                                     probe.refs[pk[0].pos][r], pk[0].column,
                                     &v)) {
                            const auto wit = iwit.find(v);
                            if (wit != iwit.end() && wit->second.cnt > 0) {
                                const auto& rc = residual_cols[wit_p];
                                int64_t pv;
                                if (read_i64(rc.table_idx,
                                             probe.refs[rc.pos][r], rc.column,
                                             &pv))
                                    wmatched = !(wit->second.mn == pv &&
                                                 wit->second.mx == pv);
                            }
                        }
                        if (join.type() == pb::QbJoin::SEMI ? wmatched
                                                            : !wmatched)
                            for (size_t c = 0; c < probe.tables.size(); ++c)
                                mine[c].push_back(probe.refs[c][r]);
                        continue;
                    }
                    const std::vector<uint32_t>* mrows = nullptr;
                    if (int_join) {
                        int64_t v;
                        if (read_i64(pk[0].table_idx,
                                     probe.refs[pk[0].pos][r], pk[0].column,
                                     &v)) {
                            const auto iit = iht.find(v);
                            if (iit != iht.end()) mrows = &iit->second;
                        }
                    } else {
                        key.clear();
                        bool null_key = false;
                        for (const auto& k : pk) {
                            const std::string_view kv =
                                key_view(k.table_idx, probe.refs[k.pos][r],
                                         k.column, kbuf);
                            if (kv.empty()) {  // NULL never matches (I1)
                                null_key = true;
                                break;
                            }
                            append_join_key(key, kv);
                        }
                        if (!null_key) {
                            const auto it = ht.find(key);
                            if (it != ht.end()) mrows = &it->second;
                        }
                    }
                    bool matched = mrows != nullptr && !mrows->empty();
                    if (matched && has_residual) {
                        matched = false;
                        for (uint32_t br : *mrows)
                            if (residual_ok(r, br)) {
                                matched = true;
                                break;
                            }
                    }
                    switch (join.type()) {
                        case pb::QbJoin::INNER:
                            if (!matched) break;
                            for (uint32_t br : *mrows) {
                                if (!residual_ok(r, br)) continue;
                                for (size_t c = 0; c < probe.tables.size();
                                     ++c)
                                    mine[c].push_back(probe.refs[c][r]);
                                for (size_t c = 0; c < build.tables.size();
                                     ++c)
                                    mine[probe.tables.size() + c].push_back(
                                        build.refs[c][br]);
                            }
                            break;
                        case pb::QbJoin::LEFT:
                            if (matched) {
                                for (uint32_t br : *mrows) {
                                    if (!residual_ok(r, br)) continue;
                                    for (size_t c = 0;
                                         c < probe.tables.size(); ++c)
                                        mine[c].push_back(probe.refs[c][r]);
                                    for (size_t c = 0;
                                         c < build.tables.size(); ++c)
                                        mine[probe.tables.size() + c]
                                            .push_back(build.refs[c][br]);
                                }
                            } else {
                                for (size_t c = 0; c < probe.tables.size();
                                     ++c)
                                    mine[c].push_back(probe.refs[c][r]);
                                for (size_t c = 0; c < build.tables.size();
                                     ++c)
                                    mine[probe.tables.size() + c].push_back(
                                        kNullRef);
                            }
                            break;
                        case pb::QbJoin::SEMI:
                            if (matched)
                                for (size_t c = 0; c < probe.tables.size();
                                     ++c)
                                    mine[c].push_back(probe.refs[c][r]);
                            break;
                        case pb::QbJoin::ANTI:
                            if (!matched)
                                for (size_t c = 0; c < probe.tables.size();
                                     ++c)
                                    mine[c].push_back(probe.refs[c][r]);
                            break;
                        default:
                            break;
                    }
                }
            });
        }
        for (auto& t : pool) t.join();
        size_t total = 0;
        for (auto& ch : chunks) total += ch.refs.empty() ? 0 : ch.refs[0].size();
        // Fan-out safety valve: a blown-up intermediate means the join
        // order was wrong for this shape — fail and let the primary run it.
        if (total > (size_t{64} << 20))
            return fail("join intermediate too large");
        for (size_t c = 0; c < out->refs.size(); ++c) {
            out->refs[c].reserve(total);
            for (auto& ch : chunks)
                if (!ch.refs.empty())
                    out->refs[c].insert(out->refs[c].end(),
                                        ch.refs[c].begin(), ch.refs[c].end());
        }
        return true;
    }

    // ----- Aggregate + output ---------------------------------------------
    // One contiguous slot per aggregate: the previous
    // one-vector-per-field layout cost five heap blocks per group, which
    // dominated high-cardinality aggregation (q18: 1.5M groups).
    struct AggSlot {
        uint64_t count = 0;
        Dec acc{};          // SUM/AVG accumulator or MIN/MAX (numeric)
        std::string sval;   // MIN/MAX (binary string)
        bool has = false;   // MIN/MAX seen any
        // COUNT(DISTINCT): distinct value set (small groups at TPC-H
        // scale; lineage log #16). Empty std::set holds no heap.
        std::set<std::string> dset;
    };
    struct GroupState {
        std::vector<std::string> key_cols;
        std::vector<AggSlot> aggs;
        // Typed group-key fast paths (E15/E16b) defer key_cols formatting to
        // emit time (M3a): only groups surviving HAVING (when it references a
        // group column), the second stage, or the output ever pay the ASCII
        // format — q18's derived `GROUP BY l_orderkey` prunes ~1.5M groups to
        // a handful before any key string is built. key_ref holds the
        // representative row ref per group column so key_view reproduces the
        // exact canonical bytes on demand (byte-identical to eager formatting,
        // and immune to non-canonical spellings like ZEROFILL that a
        // to_string(int64) round-trip would corrupt). key_done marks key_cols
        // already materialized; the string path fills it eagerly, so it is
        // true there and this deferral is a pure no-op for string keys.
        uint64_t key_ref[2] = {kNullRef, kNullRef};
        bool key_done = true;
    };
    using GroupMap = std::unordered_map<std::string, GroupState>;
    // E16b: two INT group columns packed into a POD 128-bit key — the
    // two-column analogue of the E15 fast path (perf: q20 groups by
    // (l_partkey, l_suppkey), both INT). Same value<->byte 1:1 invariant,
    // so results are unchanged.
    struct Int2Key {
        int64_t a, b;
        bool operator==(const Int2Key& o) const {
            return a == o.a && b == o.b;
        }
    };
    struct Int2Hash {
        size_t operator()(const Int2Key& k) const {
            size_t h = std::hash<int64_t>()(k.a);
            h ^= std::hash<int64_t>()(k.b) + 0x9e3779b97f4a7c15ULL +
                 (h << 6) + (h >> 2);
            return h;
        }
    };

    // Flat open-addressing group map for POD keys (F5). Layout follows the
    // DuckDB GroupedAggregateHashTable shape: a power-of-2 probe table of
    // 16-byte {hash, payload index} entries plus a dense payload vector of
    // (key, GroupState). Compared to std::unordered_map this removes the
    // per-group node allocation (q18 inserts 1.5M groups per execution), makes
    // probes touch one cache line of entries where the stored hash filters
    // almost all non-matches before the payload is read, and turns the
    // HAVING/emit scans into dense sequential walks. Interface is the subset
    // of unordered_map the aggregation templates use. erase() tombstones the
    // payload slot (probe chains stay intact; only HAVING erases, after all
    // inserts). Not thread-safe; each radix partition owns one instance.
    template <typename K, typename HashF>
    struct FlatGroupMap {
        using key_type = K;
        using value_type = std::pair<K, GroupState>;

        // Payload is one dense vector. A blocked layout (1024-entry chunks,
        // zero growth moves) was tried and REVERTED: the extra dependent
        // block-pointer load on every payload access (find hits, merge,
        // HAVING, emit) cost q18 ~100ms — more than the ~55ms the avoided
        // realloc moves and page zeroing saved. Callers that know their row
        // count instead pre-size via reserve_payload (virtual memory only;
        // untouched pages never materialize).
        std::vector<value_type> payload_;
        std::vector<uint8_t> dead_;  // parallel to payload_
        struct Ent {
            uint64_t h;    // 0 = empty (hashes are forced odd)
            uint32_t idx;  // payload index
        };
        std::vector<Ent> table_;
        size_t mask_ = 0;
        size_t dead_n_ = 0;

        FlatGroupMap() = default;
        // Explicit moves: the defaults would leave the source's scalars
        // behind, making a moved-from map (MergeGroups dst=move(src)) report
        // empty()==false with zero payload.
        FlatGroupMap(FlatGroupMap&& o) noexcept
            : payload_(std::move(o.payload_)),
              dead_(std::move(o.dead_)),
              table_(std::move(o.table_)),
              mask_(o.mask_),
              dead_n_(o.dead_n_) {
            o.mask_ = 0;
            o.dead_n_ = 0;
        }
        FlatGroupMap& operator=(FlatGroupMap&& o) noexcept {
            payload_ = std::move(o.payload_);
            dead_ = std::move(o.dead_);
            table_ = std::move(o.table_);
            mask_ = o.mask_;
            dead_n_ = o.dead_n_;
            o.mask_ = 0;
            o.dead_n_ = 0;
            return *this;
        }

        struct iterator {
            FlatGroupMap* m;
            size_t i;
            void skip() {
                while (i < m->payload_.size() && m->dead_[i]) ++i;
            }
            iterator& operator++() {
                ++i;
                skip();
                return *this;
            }
            value_type& operator*() const { return m->payload_[i]; }
            value_type* operator->() const { return &m->payload_[i]; }
            bool operator==(const iterator& o) const { return i == o.i; }
            bool operator!=(const iterator& o) const { return i != o.i; }
        };
        iterator begin() {
            iterator it{this, 0};
            it.skip();
            return it;
        }
        iterator end() { return {this, payload_.size()}; }
        bool empty() const { return payload_.size() == dead_n_; }
        size_t size() const { return payload_.size() - dead_n_; }

        // Deliberately SHARES the mix with the radix partitioner: within one
        // partition every key then has identical low log2(P) bits, so probe
        // starts cluster on a stride-P subset of slots. A reviewer flagged
        // this as a probe-length pathology (simulated 6.7 vs 1.4 average
        // probes) and we tried decorrelating with a second mix64 — it made
        // q18 ~80ms WORSE at SF=1: the clustered starts keep the probe
        // working set L1-resident (cap/P distinct lines) and the +1 linear
        // chain walks adjacent lines, while decorrelated starts turn every
        // probe into a random L2 miss across the whole table. Probe length
        // is bounded by the 0.625 load cap either way. Measured, not
        // guessed — do not "fix" without re-measuring q18.
        // |1 keeps the stored hash nonzero (0 marks an empty entry).
        static uint64_t hv(const K& k) { return HashF{}(k) | 1; }

        // Rebuild the probe table for ~`want` live entries (load <= 0.625).
        void rehash(size_t want) {
            size_t cap = 16;
            while (cap * 10 < want * 16) cap <<= 1;
            table_.assign(cap, {0, 0});
            mask_ = cap - 1;
            for (size_t i = 0; i < payload_.size(); ++i) {
                if (dead_[i]) continue;
                const uint64_t h = hv(payload_[i].first);
                size_t j = h & mask_;
                while (table_[j].h != 0) j = (j + 1) & mask_;
                table_[j] = {h, static_cast<uint32_t>(i)};
            }
        }
        void reserve(size_t n) {
            if (n * 16 > table_.size() * 10) rehash(n);
            payload_.reserve(n);
            dead_.reserve(n);
        }
        // NOTE(F5): a payload-only pre-size from the caller's row share was
        // tried and REVERTED — at SF=5 the 1024 per-worker-per-partition
        // reservations (2.3MB each, mostly untouched) turned into mmap/
        // munmap churn and lock contention that cost q18 +336ms, far more
        // than the growth moves saved. Geometric growth stays.
        iterator find(const K& k) {
            if (table_.empty()) return end();
            const uint64_t h = hv(k);
            size_t j = h & mask_;
            while (table_[j].h != 0) {
                if (table_[j].h == h) {
                    const uint32_t idx = table_[j].idx;
                    if (!dead_[idx] && payload_[idx].first == k)
                        return {this, idx};
                }
                j = (j + 1) & mask_;
            }
            return end();
        }
        std::pair<iterator, bool> emplace(K k, GroupState&& v) {
            if (table_.empty() ||
                (payload_.size() + 1) * 16 > table_.size() * 10)
                rehash(std::max<size_t>((payload_.size() + 1) * 2, 16));
            const uint64_t h = hv(k);
            size_t j = h & mask_;
            while (table_[j].h != 0) {
                if (table_[j].h == h) {
                    const uint32_t idx = table_[j].idx;
                    if (!dead_[idx] && payload_[idx].first == k)
                        return {{this, idx}, false};
                }
                j = (j + 1) & mask_;
            }
            payload_.emplace_back(std::move(k), std::move(v));
            dead_.push_back(0);
            table_[j] = {h, static_cast<uint32_t>(payload_.size() - 1)};
            return {{this, payload_.size() - 1}, true};
        }
        iterator erase(iterator it) {
            dead_[it.i] = 1;
            ++dead_n_;
            iterator nx{this, it.i + 1};
            nx.skip();
            return nx;
        }
    };
    struct I64Mix {
        size_t operator()(int64_t v) const {
            return mix64(static_cast<uint64_t>(v));
        }
    };
    struct I2Mix {
        size_t operator()(const Int2Key& k) const {
            return mix64(static_cast<uint64_t>(k.a) * 0x9e3779b97f4a7c15ULL +
                         static_cast<uint64_t>(k.b));
        }
    };
    // E15: int64-keyed variant for the single-INT-group-column fast path.
    using IntGroupMap = FlatGroupMap<int64_t, I64Mix>;
    using Int2GroupMap = FlatGroupMap<Int2Key, I2Mix>;

    // Radix partition index for a group key (M3b). A well-mixed finalizer keeps
    // partition sizes balanced even for sequentially-clustered keys (l_orderkey
    // runs 1..N with gaps), which libstdc++'s identity std::hash<int64_t> would
    // otherwise bucket unevenly. Pure function of the key, so every worker maps
    // a given key to the same partition — that disjointness is what lets the P
    // merge tasks run without cross-partition contention.
    static inline uint64_t mix64(uint64_t x) {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }
    static inline unsigned part_of_i64(int64_t v, unsigned P) {
        return static_cast<unsigned>(mix64(static_cast<uint64_t>(v)) % P);
    }
    static inline unsigned part_of_i2(const Int2Key& k, unsigned P) {
        return static_cast<unsigned>(
            mix64(static_cast<uint64_t>(k.a) * 0x9e3779b97f4a7c15ULL +
                  static_cast<uint64_t>(k.b)) % P);
    }
    static inline unsigned part_of_str(std::string_view k, unsigned P) {
        return static_cast<unsigned>(
            std::hash<std::string_view>{}(k) % P);
    }

    // Accumulates rows [begin,end) into `parts` — a radix fan-out of P group
    // maps keyed by part_of_*(key) (M3b). Generic over the map key: the
    // string-keyed GroupMap builds length-prefixed byte keys as before, while
    // the int64-keyed IntGroupMap (E15) parses the single group cell as int64.
    // In the int path the first cell that does not parse full-length sets
    // *parse_fail and returns false, so the caller discards the partial result
    // and re-runs the whole aggregation on the string path — semantics stay
    // identical (only pure-INT single group columns ever take the fast path;
    // canonical INT cells have value==byte equality, mirroring the E13 join-key
    // convention). P==1 collapses to a single map (small inputs, and the
    // per-worker range is a strict row subset either way).
    template <typename MapT>
    bool AccumulateRangeT(const pb::QbAggregate& agg, const NodeResult& in,
                          size_t begin, size_t end,
                          std::vector<MapT>& parts, unsigned P,
                          std::atomic<bool>* parse_fail) {
        constexpr bool IntKey =
            std::is_same_v<typename MapT::key_type, int64_t>;
        constexpr bool Int2 =
            std::is_same_v<typename MapT::key_type, Int2Key>;
        const int n_agg = agg.aggs_size();
        const int n_grp = agg.group_columns_size();
        // Resolve positions once.
        std::vector<int> gpos(n_grp);
        for (int g = 0; g < n_grp; ++g) {
            gpos[g] = in.table_pos(agg.group_columns(g).table_idx());
            if (gpos[g] < 0) return fail("group table not in input");
        }
        std::vector<int> apos(n_agg, -1);
        std::vector<int> fpos(n_agg, -1);
        std::vector<std::vector<uint32_t>> fcols(n_agg);
        for (int a = 0; a < n_agg; ++a) {
            if (agg.aggs(a).has_arg() || agg.aggs(a).has_filter()) {
                apos[a] = in.table_pos(agg.aggs(a).arg_table());
                if (apos[a] < 0) return fail("agg table not in input");
            }
            if (agg.aggs(a).has_filter()) {
                fpos[a] = in.table_pos(agg.aggs(a).filter_table());
                if (fpos[a] < 0) return fail("agg filter table not in input");
                if (agg.aggs(a).filter().has_expr())
                    PredicateEvaluator::collect_columns(
                        agg.aggs(a).filter().expr(), &fcols[a]);
            }
        }
        PredicateEvaluator ev;
        [[maybe_unused]] std::string keybuf;
        std::string aggbuf;  // typed -> canonical ASCII (agg arg values)
        [[maybe_unused]] std::vector<std::string_view> gv(n_grp);
        [[maybe_unused]] std::vector<std::string> gbufs(n_grp);
        // Run cache (F5, clustered-run streaming aggregation tier 1): rows
        // arrive in physical order and lineitem is clustered by l_orderkey
        // (run length ~4 at TPC-H scale), so consecutive rows usually hit the
        // same group. Equal key => same radix partition (part_of_* is a pure
        // key function), and the pointer is refreshed on every row, so no
        // emplace into this partition can intervene between set and reuse —
        // the pointer is never stale when it is actually dereferenced.
        [[maybe_unused]] int64_t last_key = 0;
        [[maybe_unused]] Int2Key last_key2{0, 0};
        GroupState* last_gs = nullptr;
        for (size_t r = begin; r < end; ++r) {
            GroupState* gs;
            if constexpr (IntKey) {
                // Single group column, prefix_len==0 (enforced by caller).
                const auto& c = agg.group_columns(0);
                const uint64_t ref = in.refs[gpos[0]][r];
                int64_t iv;
                if (ref == kNullRef ||
                    !read_i64(c.table_idx(), ref, c.column(), &iv)) {
                    // Non-INT (or NULL) key: abandon the int path; the
                    // caller re-runs this aggregation over string keys.
                    parse_fail->store(true, std::memory_order_relaxed);
                    return false;
                }
                if (last_gs != nullptr && iv == last_key) {
                    gs = last_gs;
                } else {
                    MapT& groups = parts[P == 1 ? 0 : part_of_i64(iv, P)];
                    auto it = groups.find(iv);
                    if (it == groups.end()) {
                        GroupState st;
                        // Defer the canonical-ASCII format to emit (M3a):
                        // stash the representative ref; key_cols is built
                        // lazily only for groups that survive
                        // HAVING/second-stage/output.
                        st.key_ref[0] = ref;
                        st.key_done = false;
                        st.aggs.resize(n_agg);
                        gs = &groups.emplace(iv, std::move(st)).first->second;
                    } else {
                        gs = &it->second;
                    }
                    last_key = iv;
                    last_gs = gs;
                }
            } else if constexpr (Int2) {
                // Two group columns, both prefix_len==0 (enforced by
                // caller). Parse each cell to int64; any failure abandons
                // the packed path and the caller re-runs on string keys.
                const auto& c0 = agg.group_columns(0);
                const auto& c1 = agg.group_columns(1);
                const uint64_t r0 = in.refs[gpos[0]][r];
                const uint64_t r1 = in.refs[gpos[1]][r];
                Int2Key key;
                if (r0 == kNullRef ||
                    !read_i64(c0.table_idx(), r0, c0.column(), &key.a) ||
                    r1 == kNullRef ||
                    !read_i64(c1.table_idx(), r1, c1.column(), &key.b)) {
                    parse_fail->store(true, std::memory_order_relaxed);
                    return false;
                }
                if (last_gs != nullptr && key == last_key2) {
                    gs = last_gs;
                } else {
                    MapT& groups = parts[P == 1 ? 0 : part_of_i2(key, P)];
                    auto it = groups.find(key);
                    if (it == groups.end()) {
                        GroupState st;
                        // Defer both cells' canonical-ASCII format to emit
                        // (M3a).
                        st.key_ref[0] = r0;
                        st.key_ref[1] = r1;
                        st.key_done = false;
                        st.aggs.resize(n_agg);
                        gs = &groups.emplace(key, std::move(st)).first->second;
                    } else {
                        gs = &it->second;
                    }
                    last_key2 = key;
                    last_gs = gs;
                }
            } else {
                keybuf.clear();
                for (int g = 0; g < n_grp; ++g) {
                    const auto& c = agg.group_columns(g);
                    const uint64_t ref = in.refs[gpos[g]][r];
                    // Canonical ASCII so grouping, HAVING and output all see
                    // the val_str text (typed cells formatted once here).
                    gv[g] = ref == kNullRef
                                ? std::string_view()
                                : key_view(c.table_idx(), ref, c.column(),
                                           gbufs[g]);
                    if (c.prefix_len() > 0 && gv[g].size() > c.prefix_len())
                        gv[g] = gv[g].substr(0, c.prefix_len());
                    const uint32_t l = static_cast<uint32_t>(gv[g].size());
                    keybuf.append(reinterpret_cast<const char*>(&l),
                                  sizeof(l));
                    keybuf.append(gv[g].data(), gv[g].size());
                }
                MapT& groups = parts[P == 1 ? 0 : part_of_str(keybuf, P)];
                auto it = groups.find(keybuf);
                if (it == groups.end()) {
                    GroupState st;
                    st.key_cols.resize(n_grp);
                    for (int g = 0; g < n_grp; ++g)
                        st.key_cols[g] = std::string(gv[g]);
                    st.aggs.resize(n_agg);
                    gs = &groups.emplace(std::move(keybuf), std::move(st))
                              .first->second;
                    keybuf.clear();
                } else {
                    gs = &it->second;
                }
            }
            ArithRowCtx arith_ctx{&stores, &in.tables, &in.refs, r};
            for (int a = 0; a < n_agg; ++a) {
                const auto& af = agg.aggs(a);
                const uint64_t ref =
                    apos[a] >= 0 ? in.refs[apos[a]][r] : 0;
                if (apos[a] >= 0 && ref == kNullRef) continue;  // LEFT null
                if (af.has_filter() && af.filter().has_expr()) {
                    if (is_virtual(af.filter_table()))
                        return fail("agg filter on virtual table");
                    const uint64_t fref = in.refs[fpos[a]][r];
                    if (fref == kNullRef) continue;
                    const PaxStore* st = stores[af.filter_table()];
                    const PaxGroup* grp =
                        st->group(fref / PaxGroup::kRows);
                    if (!ev.set_row_from_pax_cols(
                            *grp,
                            static_cast<uint32_t>(fref % PaxGroup::kRows),
                            af.filter().num_columns(), fcols[a]))
                        return fail("agg filter unevaluable");
                    if (!ev.evaluate(af.filter().expr())) continue;
                }
                // Virtual-table aggregate arguments (one-row derived
                // scalars, q11): read through value_of, not stores[].
                const bool arg_virtual = is_virtual(af.arg_table());
                switch (af.kind()) {
                    case pb::QbAggFunc::COUNT:
                        if (af.distinct()) {
                            // Canonical ASCII so DISTINCT counts by value
                            // (typed cells formatted; distinct-by-value).
                            const std::string_view dv = key_view(
                                af.arg_table(), ref,
                                af.arg().column_index(), aggbuf);
                            if (!dv.empty())
                                gs->aggs[a].dset.emplace(dv);
                            break;
                        }
                        gs->aggs[a].count += 1;
                        break;
                    case pb::QbAggFunc::SUM:
                    case pb::QbAggFunc::AVG: {
                        Dec v =
                            arg_virtual
                                ? read_dec(af.arg_table(), ref,
                                           af.arg().column_index())
                                : eval_arith(af.arg(),
                                             stores[af.arg_table()], ref,
                                             &arith_ctx);
                        if (v.null) break;  // NULL input skipped
                        if (gs->aggs[a].count == 0)
                            gs->aggs[a].acc = v;
                        else
                            dec_addsub(gs->aggs[a].acc, v, false);
                        gs->aggs[a].count += 1;
                        break;
                    }
                    case pb::QbAggFunc::MIN:
                    case pb::QbAggFunc::MAX: {
                        const bool want_max =
                            af.kind() == pb::QbAggFunc::MAX;
                        if (af.cmp_kind() == 1) {
                            // Canonical ASCII: string order == value order for
                            // strings and (via YYYY-MM-DD) for typed DATE.
                            std::string_view cv = key_view(
                                af.arg_table(), ref,
                                af.arg().column_index(), aggbuf);
                            if (cv.empty()) break;
                            if (!gs->aggs[a].has ||
                                (want_max ? cv > std::string_view(gs->aggs[a].sval)
                                          : cv < std::string_view(gs->aggs[a].sval)))
                                gs->aggs[a].sval = std::string(cv);
                            gs->aggs[a].has = true;
                        } else {
                            Dec v = arg_virtual
                                        ? read_dec(af.arg_table(), ref,
                                                   af.arg().column_index())
                                        : eval_arith(af.arg(),
                                                     stores[af.arg_table()],
                                                     ref);
                            if (v.null) break;
                            if (!gs->aggs[a].has) {
                                gs->aggs[a].acc = v;
                            } else {
                                Dec diff = v;  // diff = v - acc
                                dec_addsub(diff, gs->aggs[a].acc, true);
                                if (want_max ? diff.m > 0 : diff.m < 0)
                                    gs->aggs[a].acc = v;
                            }
                            gs->aggs[a].has = true;
                        }
                        break;
                    }
                    default:
                        return fail("unsupported aggregate kind");
                }
            }
        }
        return true;
    }

    template <typename MapT>
    static void MergeGroups(MapT& dst, MapT& src,
                            const pb::QbAggregate& agg) {
        const int n_agg = agg.aggs_size();
        if (dst.empty()) {
            dst = std::move(src);
            return;
        }
        for (auto& kv : src) {
            auto it = dst.find(kv.first);
            if (it == dst.end()) {
                dst.emplace(kv.first, std::move(kv.second));
                continue;
            }
            GroupState& d = it->second;
            GroupState& s = kv.second;
            for (int a = 0; a < n_agg; ++a) {
                switch (agg.aggs(a).kind()) {
                    case pb::QbAggFunc::COUNT:
                        if (agg.aggs(a).distinct()) {
                            d.aggs[a].dset.merge(s.aggs[a].dset);
                            break;
                        }
                        d.aggs[a].count += s.aggs[a].count;
                        break;
                    case pb::QbAggFunc::SUM:
                    case pb::QbAggFunc::AVG:
                        if (s.aggs[a].count > 0) {
                            if (d.aggs[a].count == 0)
                                d.aggs[a].acc = s.aggs[a].acc;
                            else
                                dec_addsub(d.aggs[a].acc, s.aggs[a].acc, false);
                            d.aggs[a].count += s.aggs[a].count;
                        }
                        break;
                    case pb::QbAggFunc::MIN:
                    case pb::QbAggFunc::MAX: {
                        if (!s.aggs[a].has) break;
                        const bool want_max =
                            agg.aggs(a).kind() == pb::QbAggFunc::MAX;
                        if (agg.aggs(a).cmp_kind() == 1) {
                            if (!d.aggs[a].has ||
                                (want_max ? s.aggs[a].sval > d.aggs[a].sval
                                          : s.aggs[a].sval < d.aggs[a].sval))
                                d.aggs[a].sval = std::move(s.aggs[a].sval);
                        } else {
                            if (!d.aggs[a].has) {
                                d.aggs[a].acc = s.aggs[a].acc;
                            } else {
                                Dec diff = d.aggs[a].acc;
                                dec_addsub(diff, s.aggs[a].acc, true);
                                const bool s_bigger = diff.m < 0;
                                if (want_max ? s_bigger : diff.m > 0)
                                    d.aggs[a].acc = s.aggs[a].acc;
                            }
                        }
                        d.aggs[a].has = true;
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    }

    // Evaluate an output expression (source=EXPR): COLUMN_REF ordinals
    // address [group columns..., aggregates...]; OP_DIV follows MySQL
    // decimal division. Returns a null Dec on failure.
    Dec EvalOutExpr(const pb::FilterExpr& e, const pb::QbAggregate& agg,
                    const GroupState& gs) {
        using FE = pb::FilterExpr;
        const int n_grp = agg.group_columns_size();
        switch (e.op()) {
            case FE::COLUMN_REF: {
                const int ord = static_cast<int>(e.column_index());
                if (ord < n_grp) return dec_parse(gs.key_cols[ord]);
                const int a = ord - n_grp;
                if (a >= agg.aggs_size()) return {};
                bool is_null = false;
                const std::string v =
                    AggValue(agg.aggs(a), gs, a, &is_null);
                if (is_null) return {};
                return dec_parse(v);
            }
            case FE::CONST_INT: {
                Dec d;
                d.m = e.int_val();
                d.s = 0;
                d.null = false;
                return d;
            }
            case FE::CONST_STRING:
                return dec_parse(std::string_view(e.string_val()));
            case FE::OP_ADD:
            case FE::OP_SUB: {
                if (e.children_size() != 2) return {};
                Dec a = EvalOutExpr(e.children(0), agg, gs);
                Dec b = EvalOutExpr(e.children(1), agg, gs);
                if (a.null || b.null) return {};
                dec_addsub(a, b, e.op() == FE::OP_SUB);
                return a;
            }
            case FE::OP_MUL: {
                if (e.children_size() != 2) return {};
                Dec a = EvalOutExpr(e.children(0), agg, gs);
                Dec b = EvalOutExpr(e.children(1), agg, gs);
                if (a.null || b.null) return {};
                Dec r;
                r.m = a.m * b.m;
                r.s = a.s + b.s;
                r.null = false;
                return r;
            }
            case FE::OP_DIV: {
                if (e.children_size() != 2) return {};
                Dec a = EvalOutExpr(e.children(0), agg, gs);
                Dec b = EvalOutExpr(e.children(1), agg, gs);
                return dec_div(a, b, a.null ? 0 : a.s + 4);
            }
            case FE::OP_NEG: {
                if (e.children_size() != 1) return {};
                Dec a = EvalOutExpr(e.children(0), agg, gs);
                if (!a.null) a.m = -a.m;
                return a;
            }
            default:
                return {};
        }
    }

    // Format one aggregate's final value ("" + is_null for SQL NULL).
    std::string AggValue(const pb::QbAggFunc& af, const GroupState& gs, int a,
                         bool* is_null) {
        *is_null = false;
        switch (af.kind()) {
            case pb::QbAggFunc::COUNT:
                return std::to_string(af.distinct() ? gs.aggs[a].dset.size()
                                                    : gs.aggs[a].count);
            case pb::QbAggFunc::SUM:
                if (gs.aggs[a].count == 0) {
                    if (af.zero_if_empty()) {
                        Dec z;
                        z.m = 0;
                        z.s = static_cast<int>(af.arg_scale());
                        z.null = false;
                        return dec_format(z);
                    }
                    *is_null = true;
                    return {};
                }
                return dec_format(gs.aggs[a].acc);
            case pb::QbAggFunc::AVG: {
                if (gs.aggs[a].count == 0) {
                    *is_null = true;
                    return {};
                }
                const int out_scale =
                    static_cast<int>(af.arg_scale()) + 4;
                return dec_format(
                    dec_divide(gs.aggs[a].acc, gs.aggs[a].count, out_scale));
            }
            case pb::QbAggFunc::MIN:
            case pb::QbAggFunc::MAX:
                if (!gs.aggs[a].has) {
                    *is_null = true;
                    return {};
                }
                return af.cmp_kind() == 1 ? gs.aggs[a].sval
                                          : dec_format(gs.aggs[a].acc);
            default:
                *is_null = true;
                return {};
        }
    }

    // Row-returning blocks (q2/q15/q20): emit output columns straight from
    // the final tuple set, then sort/limit.
    bool RunEmitRows(const NodeResult& in,
                     pb::TxExecuteQueryBlock::Response* response) {
        struct OutRow {
            std::vector<std::string> vals;
            std::vector<bool> nulls;
        };
        std::vector<OutRow> rows;
        rows.reserve(in.rows());
        std::vector<int> pos(req.output_size(), -1);
        for (int i = 0; i < req.output_size(); ++i) {
            const auto& oe = req.output(i);
            if (oe.source() != pb::QbOutputExpr::COLUMN)
                return fail("row output source");
            pos[i] = in.table_pos(oe.column().table_idx());
            if (pos[i] < 0) return fail("row output table");
        }
        std::string vbuf;  // typed cell -> canonical ASCII for output
        for (size_t r = 0; r < in.rows(); ++r) {
            OutRow row;
            row.vals.reserve(req.output_size());
            row.nulls.reserve(req.output_size());
            for (int i = 0; i < req.output_size(); ++i) {
                const auto& oe = req.output(i);
                const uint64_t ref = in.refs[pos[i]][r];
                if (ref == kNullRef ||
                    null_of(oe.column().table_idx(), ref,
                            oe.column().column())) {
                    row.vals.emplace_back();
                    row.nulls.push_back(true);
                    continue;
                }
                std::string_view v = key_view(oe.column().table_idx(), ref,
                                              oe.column().column(), vbuf);
                if (oe.column().prefix_len() > 0 &&
                    v.size() > oe.column().prefix_len())
                    v = v.substr(0, oe.column().prefix_len());
                row.vals.emplace_back(v);
                row.nulls.push_back(v.empty());
            }
            rows.push_back(std::move(row));
        }

        if (req.order_by_size() > 0) {
            std::sort(rows.begin(), rows.end(), [&](const OutRow& a,
                                                    const OutRow& b) {
                for (const auto& k : req.order_by()) {
                    const uint32_t o = k.output_ordinal();
                    const bool an = a.nulls[o];
                    const bool bn = b.nulls[o];
                    if (an != bn) return k.descending() ? bn : an;
                    if (an) continue;
                    int c;
                    if (k.cmp_kind() == 1) {
                        c = a.vals[o].compare(b.vals[o]);
                    } else {
                        Dec da = dec_parse(a.vals[o]);
                        Dec db = dec_parse(b.vals[o]);
                        Dec diff = da;
                        dec_addsub(diff, db, true);
                        c = diff.m < 0 ? -1 : (diff.m > 0 ? 1 : 0);
                    }
                    if (c != 0) return k.descending() ? c > 0 : c < 0;
                }
                return false;
            });
        }
        size_t begin = std::min<size_t>(req.offset(), rows.size());
        size_t end = req.limit() > 0
                         ? std::min<size_t>(begin + req.limit(), rows.size())
                         : rows.size();
        std::string rowbuf;
        for (size_t r = begin; r < end; ++r) {
            rowbuf.clear();
            emit_field(rowbuf, {}, true);
            for (size_t c = 0; c < rows[r].vals.size(); ++c)
                emit_field(rowbuf, rows[r].vals[c], rows[r].nulls[c]);
            response->add_rows(rowbuf);
        }
        return true;
    }

    bool RunAggregateAndEmit(const pb::QbAggregate& agg,
                             pb::TxExecuteQueryBlock::Response* response) {
        const NodeResult& in = results[agg.input()];
        const int n_grp = agg.group_columns_size();
        const size_t n = in.rows();
        const unsigned wc = static_cast<unsigned>(std::min<size_t>(
            workers(), std::max<size_t>(n / 65536, 1)));
        // Radix partitions (M3b) = worker count. Each accumulate worker fans
        // its groups into P partitions by part_of_*(key); P merge tasks then
        // fold partition p from every worker in parallel (disjoint keys → no
        // contention), replacing the single-threaded MergeGroups chain into one
        // giant map that bottlenecked q18/q20 at ~1.5M groups. HAVING and the
        // output row build also run per-partition in parallel; the second stage
        // and ORDER BY/LIMIT stay single-threaded. P==1 (small inputs) collapses
        // to the pre-M3b single-map path with identical emit order.
        const unsigned P = wc;

        // Post-accumulation pipeline (HAVING → second stage / output →
        // ORDER BY / LIMIT / serialize), generic over the map key type so
        // the int64 fast path (E15) and the string path share it verbatim.
        // Lazily materialize a group's key_cols from its representative refs
        // (M3a): a no-op on the string path (key_done already true) and on the
        // typed fast paths for groups whose keys are never read. key_view on
        // the stored ref reproduces the exact canonical bytes, so the deferred
        // format is byte-identical to eager formatting. Reentrant (local buf)
        // so parallel per-partition emit stages can call it concurrently on
        // disjoint groups.
        auto ensure_keys = [&](GroupState& gs) {
            if (gs.key_done) return;
            gs.key_cols.resize(n_grp);
            std::string buf;
            for (int g = 0; g < n_grp; ++g) {
                const auto& c = agg.group_columns(g);
                gs.key_cols[g] = std::string(
                    key_view(c.table_idx(), gs.key_ref[g], c.column(), buf));
            }
            gs.key_done = true;
        };

        // Output rows: one per surviving group. Hoisted above emit so the
        // per-partition parallel builders below can produce vectors of it.
        struct OutRow {
            std::vector<std::string> vals;
            std::vector<bool> nulls;
        };

        auto emit = [&](auto& parts) -> bool {
        using MapT = typename std::decay_t<decltype(parts)>::value_type;
        using KeyT = typename MapT::key_type;
        const unsigned np = static_cast<unsigned>(parts.size());

        // Implicit grouping emits one row over zero input. Reachable only for
        // the string map — the int paths always have >=1 group column. Inject
        // into partition 0 when every partition is empty.
        if constexpr (std::is_same_v<KeyT, std::string>) {
            if (n_grp == 0) {
                bool any = false;
                for (auto& pt : parts)
                    if (!pt.empty()) { any = true; break; }
                if (!any) {
                    GroupState st;
                    st.aggs.resize(agg.aggs_size());
                    parts[0].emplace(std::string(), std::move(st));
                }
            }
        }

        // Does HAVING actually reference a group column (ordinal < n_grp)?
        // If not (q18: `sum(l_quantity) > 300` reads only the aggregate), the
        // 1.5M-group HAVING scan never materializes a key — the whole point of
        // the M3a deferral.
        bool having_uses_group = false;
        if (agg.has_having() && agg.having().has_expr()) {
            std::vector<uint32_t> hcols;
            PredicateEvaluator::collect_columns(agg.having().expr(), &hcols);
            for (uint32_t o : hcols)
                if (o < static_cast<uint32_t>(n_grp)) {
                    having_uses_group = true;
                    break;
                }
        }

        // HAVING: drop groups failing the predicate over the stage-1 value
        // layout [group values..., aggregate values...]. Partitions hold
        // disjoint keys, so each is pruned independently — this is q18's
        // dominant post-merge cost (a 1.5M-group scan), so run it per-partition
        // in parallel (M3b), each thread with its own evaluator/scratch.
        if (agg.has_having() && agg.having().has_expr()) {
            auto do_having = [&](MapT& groups) {
                PredicateEvaluator hev;
                std::vector<std::string> hv;
                std::vector<std::string_view> hcells;
                std::vector<bool> hnulls;
                for (auto it = groups.begin(); it != groups.end();) {
                    GroupState& gs = it->second;
                    if (having_uses_group) ensure_keys(gs);
                    hv.clear();
                    hnulls.clear();
                    for (int g = 0; g < n_grp; ++g) {
                        // Placeholder when HAVING never reads group columns;
                        // the evaluator only touches ordinals it names.
                        hv.push_back(having_uses_group ? gs.key_cols[g]
                                                       : std::string());
                        hnulls.push_back(false);
                    }
                    for (int a = 0; a < agg.aggs_size(); ++a) {
                        bool is_null = false;
                        hv.push_back(AggValue(agg.aggs(a), gs, a, &is_null));
                        hnulls.push_back(is_null);
                    }
                    hcells.assign(hv.begin(), hv.end());
                    hev.set_row_from_views(hcells, hnulls);
                    if (hev.evaluate(agg.having().expr()))
                        ++it;
                    else
                        it = groups.erase(it);
                }
            };
            if (np <= 1) {
                do_having(parts[0]);
            } else {
                std::vector<std::thread> pool;
                pool.reserve(np);
                for (unsigned p = 0; p < np; ++p) {
                    if (parts[p].empty()) continue;  // no work, no thread
                    pool.emplace_back([&, p] { do_having(parts[p]); });
                }
                for (auto& t : pool) t.join();
            }
        }

        // Validate the NON-second output ordinals once (data-independent) so
        // the parallel row builder below has no failure path. The second-stage
        // path is single-threaded and its output ordinals live in a different
        // space (the composite gvals, sized by group_value_ordinals, checked
        // inline against gvals.size()), so it keeps its own inline validation
        // and is skipped here — validating it against n_grp/aggs_size would
        // spuriously reject a valid `GROUP ordinal >= n_grp` second-stage
        // output (Codex M3b review).
        if (!agg.has_second()) {
            for (const auto& oe : req.output()) {
                switch (oe.source()) {
                    case pb::QbOutputExpr::GROUP:
                        if (oe.ordinal() >= static_cast<uint32_t>(n_grp))
                            return fail("group ordinal out of range");
                        break;
                    case pb::QbOutputExpr::AGG:
                        if (oe.ordinal() >=
                            static_cast<uint32_t>(agg.aggs_size()))
                            return fail("agg ordinal out of range");
                        break;
                    case pb::QbOutputExpr::EXPR:
                        break;
                    default:
                        return fail("unsupported output source");
                }
            }
        }

        std::vector<OutRow> rows;
        if (agg.has_second()) {
            // Second-stage re-aggregation (q13 shape): re-group stage-1
            // values and COUNT(*) per group. Parallel since F5: stage-1
            // partitions hold disjoint groups, so each is folded into a
            // radix fan-out of partial second maps (part_of_str on the
            // composite value key), and the np2 second partitions merge and
            // emit concurrently — the same M3b pattern as stage 1. Ordinal
            // validation is hoisted (data-independent) so workers have no
            // failure path. ensure_keys mutates only this partition's
            // groups (disjoint; reentrant per M3a).
            if (!agg.second().count_star())
                return fail("second stage must count");
            for (uint32_t ord : agg.second().group_value_ordinals())
                if (ord >= static_cast<uint32_t>(n_grp) &&
                    static_cast<int>(ord) - n_grp >= agg.aggs_size())
                    return fail("second stage ordinal");
            for (const auto& oe : req.output()) {
                if (oe.source() == pb::QbOutputExpr::GROUP) {
                    if (oe.ordinal() >= static_cast<uint32_t>(
                                            agg.second()
                                                .group_value_ordinals_size()))
                        return fail("second output ordinal");
                } else if (oe.source() != pb::QbOutputExpr::AGG) {
                    return fail("second output source");
                }
            }
            using SecondMap = std::unordered_map<std::string, uint64_t>;
            const unsigned np2 = np;
            // [stage1 partition][second partition] partial counts.
            std::vector<std::vector<SecondMap>> sl(
                np, std::vector<SecondMap>(np2));
            auto fold_part = [&](unsigned p) {
                std::string key;
                for (auto& kv : parts[p]) {
                    GroupState& gs = kv.second;
                    key.clear();
                    for (uint32_t ord : agg.second().group_value_ordinals()) {
                        std::string v;
                        bool is_null = false;
                        if (ord < static_cast<uint32_t>(n_grp)) {
                            ensure_keys(gs);
                            v = gs.key_cols[ord];
                        } else {
                            const int a = static_cast<int>(ord) - n_grp;
                            v = AggValue(agg.aggs(a), gs, a, &is_null);
                        }
                        const uint32_t l = static_cast<uint32_t>(v.size());
                        key.append(reinterpret_cast<const char*>(&l),
                                   sizeof(l));
                        key.append(v);
                    }
                    sl[p][np2 == 1 ? 0 : part_of_str(key, np2)]
                        .emplace(key, 0)
                        .first->second += 1;
                }
            };
            // Merge second partition s across stage-1 partials, then emit
            // its rows (order within/across partitions is arbitrary — the
            // final result is ORDER BY-sorted or order-free, same contract
            // as the stage-1 emit concatenation).
            std::vector<std::vector<OutRow>> prows(np2);
            auto emit_second = [&](unsigned s) {
                SecondMap merged;
                for (unsigned p = 0; p < np; ++p) {
                    if (merged.empty()) {
                        merged = std::move(sl[p][s]);
                        continue;
                    }
                    for (auto& kv : sl[p][s])
                        merged.emplace(kv.first, 0).first->second +=
                            kv.second;
                }
                auto& out = prows[s];
                out.reserve(merged.size());
                for (auto& kv : merged) {
                    // Re-split the composite key back into values.
                    OutRow row;
                    std::vector<std::string> gvals;
                    {
                        const std::string& k = kv.first;
                        size_t off = 0;
                        while (off + 4 <= k.size()) {
                            uint32_t l;
                            std::memcpy(&l, k.data() + off, 4);
                            off += 4;
                            gvals.emplace_back(k.data() + off, l);
                            off += l;
                        }
                    }
                    for (const auto& oe : req.output()) {
                        if (oe.source() == pb::QbOutputExpr::GROUP) {
                            row.vals.push_back(
                                oe.ordinal() < gvals.size()
                                    ? gvals[oe.ordinal()]
                                    : std::string());
                            row.nulls.push_back(false);
                        } else {  // AGG (validated above)
                            row.vals.push_back(std::to_string(kv.second));
                            row.nulls.push_back(false);
                        }
                    }
                    out.push_back(std::move(row));
                }
            };
            if (np <= 1) {
                fold_part(0);
                emit_second(0);
            } else {
                std::vector<std::thread> pool;
                pool.reserve(np);
                for (unsigned p = 0; p < np; ++p) {
                    if (parts[p].empty()) continue;
                    pool.emplace_back([&, p] { fold_part(p); });
                }
                for (auto& t : pool) t.join();
                pool.clear();
                for (unsigned s = 0; s < np2; ++s)
                    pool.emplace_back([&, s] { emit_second(s); });
                for (auto& t : pool) t.join();
            }
            size_t tot = 0;
            for (auto& pr : prows) tot += pr.size();
            rows.reserve(tot);
            for (auto& pr : prows)
                for (auto& r : pr) rows.push_back(std::move(r));
        } else {
            // Normal output: build each partition's rows independently, then
            // concatenate in partition order (M3b). Ordinals were validated
            // above, so this path cannot fail — safe to run per-partition in
            // parallel. The concatenation order differs from the pre-M3b single
            // map, but every final result is ORDER BY-sorted or a scalar row and
            // derived blocks feed order-independent joins, so md5 is preserved.
            auto build_part = [&](MapT& groups, std::vector<OutRow>& out) {
                out.reserve(groups.size());
                for (auto& kv : groups) {
                    GroupState& gs = kv.second;
                    ensure_keys(gs);  // survivors only (M3a)
                    OutRow row;
                    row.vals.reserve(req.output_size());
                    row.nulls.reserve(req.output_size());
                    for (const auto& oe : req.output()) {
                        switch (oe.source()) {
                            case pb::QbOutputExpr::GROUP:
                                row.vals.push_back(gs.key_cols[oe.ordinal()]);
                                row.nulls.push_back(false);
                                break;
                            case pb::QbOutputExpr::AGG: {
                                bool is_null = false;
                                row.vals.push_back(AggValue(
                                    agg.aggs(oe.ordinal()), gs,
                                    static_cast<int>(oe.ordinal()), &is_null));
                                row.nulls.push_back(is_null);
                                break;
                            }
                            case pb::QbOutputExpr::EXPR: {
                                Dec v = EvalOutExpr(oe.expr(), agg, gs);
                                if (!v.null && v.s > static_cast<int>(
                                                         oe.result_scale())) {
                                    // Round to the declared output scale.
                                    const int drop = v.s - oe.result_scale();
                                    __int128 m = v.m;
                                    const bool neg = m < 0;
                                    if (neg) m = -m;
                                    m = (m + dec_pow10(drop) / 2) /
                                        dec_pow10(drop);
                                    v.m = neg ? -m : m;
                                    v.s = oe.result_scale();
                                }
                                row.vals.push_back(v.null ? std::string()
                                                          : dec_format(v));
                                row.nulls.push_back(v.null);
                                break;
                            }
                            default:
                                break;  // validated above; unreachable
                        }
                    }
                    out.push_back(std::move(row));
                }
            };
            if (np <= 1) {
                build_part(parts[0], rows);
            } else {
                std::vector<std::vector<OutRow>> prows(np);
                std::vector<std::thread> pool;
                pool.reserve(np);
                for (unsigned p = 0; p < np; ++p) {
                    if (parts[p].empty()) continue;
                    pool.emplace_back(
                        [&, p] { build_part(parts[p], prows[p]); });
                }
                for (auto& t : pool) t.join();
                size_t tot = 0;
                for (auto& pr : prows) tot += pr.size();
                rows.reserve(tot);
                for (auto& pr : prows)
                    for (auto& r : pr) rows.push_back(std::move(r));
            }
        }

        // ORDER BY over output ordinals.
        if (req.order_by_size() > 0) {
            std::sort(rows.begin(), rows.end(), [&](const OutRow& a,
                                                    const OutRow& b) {
                for (const auto& k : req.order_by()) {
                    const uint32_t o = k.output_ordinal();
                    const bool an = a.nulls[o];
                    const bool bn = b.nulls[o];
                    if (an != bn)  // MySQL: NULLs first ASC, last DESC
                        return k.descending() ? bn : an;
                    if (an) continue;
                    int c;
                    if (k.cmp_kind() == 1) {
                        c = a.vals[o].compare(b.vals[o]);
                    } else {
                        Dec da = dec_parse(a.vals[o]);
                        Dec db = dec_parse(b.vals[o]);
                        Dec diff = da;
                        dec_addsub(diff, db, true);
                        c = diff.m < 0 ? -1 : (diff.m > 0 ? 1 : 0);
                    }
                    if (c != 0) return k.descending() ? c > 0 : c < 0;
                }
                return false;
            });
        }

        // OFFSET / LIMIT, then serialize.
        size_t begin = std::min<size_t>(req.offset(), rows.size());
        size_t end = req.limit() > 0
                         ? std::min<size_t>(begin + req.limit(), rows.size())
                         : rows.size();
        std::string rowbuf;
        for (size_t r = begin; r < end; ++r) {
            rowbuf.clear();
            emit_field(rowbuf, {}, true);  // null-flags field placeholder
            for (size_t c = 0; c < rows[r].vals.size(); ++c)
                emit_field(rowbuf, rows[r].vals[c], rows[r].nulls[c]);
            response->add_rows(rowbuf);
        }
        return true;
        };  // emit

        // Radix accumulate + parallel P-way merge into `parts` (pre-sized to
        // P). Phase 1: each worker fans its row range into its own P local
        // partitions. Phase 2: one merge task per partition folds that
        // partition from every worker — disjoint keys, so no contention, which
        // replaces the single-threaded MergeGroups chain into one giant map
        // that dominated high-cardinality aggregation (q18/q20). Returns 0 on
        // success (parts ready to emit), 1 on parse fallback (int paths only —
        // a non-INT cell was seen), 2 on structural failure.
        auto run_typed = [&](auto& parts) -> int {
            using MapT = typename std::decay_t<decltype(parts)>::value_type;
            std::atomic<bool> parse_fail{false};
            if (P <= 1) {
                if (!AccumulateRangeT(agg, in, 0, n, parts, 1u, &parse_fail))
                    return parse_fail.load() ? 1 : 2;
                return 0;
            }
            std::vector<std::vector<MapT>> locals(wc);
            std::vector<char> failed(wc, 0);
            {
                std::vector<std::thread> pool;
                pool.reserve(wc);
                for (unsigned w = 0; w < wc; ++w) {
                    pool.emplace_back([&, w] {
                        locals[w].resize(P);
                        if (!AccumulateRangeT(agg, in, n * w / wc,
                                              n * (w + 1) / wc, locals[w], P,
                                              &parse_fail))
                            failed[w] = 1;
                    });
                }
                for (auto& t : pool) t.join();
            }
            for (char f : failed)
                if (f) return parse_fail.load() ? 1 : 2;
            {
                std::vector<std::thread> pool;
                pool.reserve(P);
                for (unsigned p = 0; p < P; ++p) {
                    size_t ptotal = 0;
                    for (unsigned w = 0; w < wc; ++w)
                        ptotal += locals[w][p].size();
                    if (ptotal == 0) continue;  // empty partition, no thread
                    pool.emplace_back([&, p, ptotal] {
                        bool first = true;
                        for (unsigned w = 0; w < wc; ++w) {
                            MergeGroups(parts[p], locals[w][p], agg);
                            if (first && !parts[p].empty()) {
                                parts[p].reserve(ptotal);  // one rehash (E8b)
                                first = false;
                            }
                        }
                    });
                }
                for (auto& t : pool) t.join();
            }
            return 0;
        };

        // The int fast paths only take numeric group columns (cmp_kind==0):
        // a STRING column could carry non-canonical numeric spellings
        // ("01" vs "1") that byte-group apart but parse to the same int64,
        // whereas numeric columns store canonical val_str so parse-success
        // is value<->byte 1:1. The proxy always sets cmp_kind (1 for STRING
        // result type, 0 for numeric) on every group column.
        const bool grp0_int = n_grp >= 1 &&
                              agg.group_columns(0).prefix_len() == 0 &&
                              agg.group_columns(0).cmp_kind() == 0;
        const bool grp1_int = n_grp >= 2 &&
                              agg.group_columns(1).prefix_len() == 0 &&
                              agg.group_columns(1).cmp_kind() == 0;
        if (n_grp == 1 && grp0_int) {
            std::vector<IntGroupMap> parts(P);
            const int st = run_typed(parts);
            if (st == 0) return emit(parts);     // every key was int64
            if (st == 2) return false;           // structural failure
            // st == 1: a cell did not parse → fall through to string path.
        } else if (n_grp == 2 && grp0_int && grp1_int) {
            std::vector<Int2GroupMap> parts(P);
            const int st = run_typed(parts);
            if (st == 0) return emit(parts);     // both keys were int64
            if (st == 2) return false;
            // st == 1: fall through to the string path.
        }

        std::vector<GroupMap> parts(P);
        const int st = run_typed(parts);  // string keys never set parse_fail
        if (st != 0) return false;
        return emit(parts);
    }

    bool Run(pb::TxExecuteQueryBlock::Response* response) {
        if (!Prepare()) return false;
        if (req.nodes_size() == 0) return fail("empty plan");

        results.resize(req.nodes_size());
        const pb::QbAggregate* final_agg = nullptr;
        for (int i = 0; i < req.nodes_size(); ++i) {
            const auto& node = req.nodes(i);
            if (node.has_scan()) {
                if (!RunScan(node.scan(), &results[i])) return false;
            } else if (node.has_join()) {
                if (node.join().build() >= static_cast<uint32_t>(i) ||
                    node.join().probe() >= static_cast<uint32_t>(i)) {
                    if (node.join().build() >= static_cast<uint32_t>(i) ||
                        node.join().probe() >= static_cast<uint32_t>(i))
                        return fail("join child order");
                }
                if (!RunJoin(node.join(), &results[i])) return false;
            } else if (node.has_sub_block()) {
                if (!RunSubBlock(node.sub_block(), &results[i]))
                    return false;
            } else if (node.has_filter()) {
                if (node.filter().input() >= static_cast<uint32_t>(i))
                    return fail("filter child order");
                if (!RunFilter(node.filter(), &results[i])) return false;
            } else if (node.has_aggregate()) {
                if (i != req.nodes_size() - 1)
                    return fail("aggregate must be the root");
                if (node.aggregate().input() >= static_cast<uint32_t>(i))
                    return fail("aggregate child order");
                final_agg = &node.aggregate();
            } else {
                return fail("unknown node");
            }
        }
        if (final_agg == nullptr) {
            if (!RunEmitRows(results[req.nodes_size() - 1], response))
                return false;
        } else if (!RunAggregateAndEmit(*final_agg, response)) {
            return false;
        }
        if (!Quiesced()) return fail("concurrent modification");
        return true;
    }
};

}  // namespace

void ExecuteQueryBlockFiltered(
    LineairDB::Database* db,
    const pb::TxExecuteQueryBlock::Request& request,
    pb::TxExecuteQueryBlock::Response* response,
    const std::unordered_set<std::string>* ext_keys,
    uint32_t ext_filter_table, uint32_t ext_filter_column) {
    Executor ex{db, request, {}, {}, {}, {}};
    ex.ext_keys = ext_keys;
    ex.ext_filter_table = ext_filter_table;
    ex.ext_filter_column = ext_filter_column;
    bool ok = false;
    try {
        ok = ex.Run(response);
    } catch (const std::exception& e) {
        ex.error = e.what();
    } catch (...) {
        ex.error = "query block execution failed";
    }
    if (!ok) {
        response->set_ok(false);
        response->set_error(ex.error.empty() ? "query block failed"
                                             : ex.error);
        response->clear_rows();
        return;
    }
    response->set_ok(true);
}

void ExecuteQueryBlock(
    LineairDB::Database* db,
    const pb::TxExecuteQueryBlock::Request& request,
    pb::TxExecuteQueryBlock::Response* response) {
    Executor ex{db, request, {}, {}, {}, {}};
    bool ok = false;
    try {
        ok = ex.Run(response);
    } catch (const std::exception& e) {
        ex.error = e.what();
    } catch (...) {
        ex.error = "query block execution failed";
    }
    if (!ok) {
        response->set_ok(false);
        response->set_error(ex.error.empty() ? "query block failed"
                                             : ex.error);
        response->clear_rows();
        return;
    }
    response->set_ok(true);
}

}  // namespace qb
