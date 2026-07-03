#include "simd_scan.hh"

#include <immintrin.h>

#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string_view>
#include <unordered_map>

namespace qb {
namespace simd {

using LineairDB::Pax::PaxGroup;
using LineairDB::Pax::PaxStore;
namespace pb = LineairDB::Protocol;

namespace {

int64_t ipow10(int n) {
    int64_t v = 1;
    while (n-- > 0) v *= 10;
    return v;
}

// Classify one cell string into a typed value. Kinds are structural:
//   DATE    : exactly "YYYY-MM-DD"        -> YYYYMMDD int
//   DECIMAL : has a '.' (scale = frac digits) -> all digits as scaled int
//   INT     : all digits (optional sign) -> int64
// Returns false when the cell fits none (empty, letters, malformed).
bool classify_cell(std::string_view s, Kind* k, int* scale, int64_t* val) {
    if (s.empty()) return false;

    // DATE: fixed "YYYY-MM-DD".
    if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        int64_t y = 0, m = 0, d = 0;
        auto digits = [](std::string_view t, int64_t* o) {
            for (char c : t) {
                if (c < '0' || c > '9') return false;
                *o = *o * 10 + (c - '0');
            }
            return true;
        };
        if (digits(s.substr(0, 4), &y) && digits(s.substr(5, 2), &m) &&
            digits(s.substr(8, 2), &d)) {
            *k = Kind::DATE;
            *scale = 0;
            *val = y * 10000 + m * 100 + d;
            return true;
        }
        return false;
    }

    // DECIMAL / INT: optional sign, digits, at most one dot.
    size_t i = 0;
    bool neg = false;
    if (s[i] == '-') {
        neg = true;
        ++i;
    } else if (s[i] == '+') {
        ++i;
    }
    int64_t m = 0;
    int sc = 0;
    bool seen_dot = false;
    bool any = false;
    for (; i < s.size(); ++i) {
        const char c = s[i];
        if (c == '.') {
            if (seen_dot) return false;
            seen_dot = true;
            continue;
        }
        if (c < '0' || c > '9') return false;
        m = m * 10 + (c - '0');
        if (seen_dot) ++sc;
        any = true;
    }
    if (!any) return false;
    *val = neg ? -m : m;
    *scale = sc;
    *k = seen_dot ? Kind::DECIMAL : Kind::INT;
    return true;
}

std::shared_ptr<const TypedColumn> Build(PaxStore* store, uint32_t column) {
    auto tc = std::make_shared<TypedColumn>();
    const size_t n_groups = store->group_count();
    tc->n_groups = n_groups;
    tc->vals.assign(n_groups * PaxGroup::kRows, 0);

    Kind kind = Kind::UNTYPED;  // UNTYPED here means "not yet determined"
    int scale = -1;
    bool determined = false;
    bool ok = true;
    for (size_t g = 0; g < n_groups && ok; ++g) {
        PaxGroup* grp = store->group(g);
        if (grp == nullptr) continue;
        int64_t* base = tc->vals.data() + g * PaxGroup::kRows;
        for (uint32_t slot = 0; slot < PaxGroup::kRows; ++slot) {
            if (!grp->IsVisible(slot)) continue;
            Kind ck;
            int cs;
            int64_t v;
            if (!classify_cell(grp->cell(column + 1, slot), &ck, &cs, &v)) {
                ok = false;
                break;
            }
            if (!determined) {
                // DECIMAL boundary math needs 10^scale exactly representable as
                // a double; cap the scale (all TPC-H decimals are scale 2).
                if (ck == Kind::DECIMAL && cs > 15) {
                    ok = false;
                    break;
                }
                kind = ck;
                scale = cs;
                determined = true;
            } else if (ck != kind || cs != scale) {
                ok = false;
                break;
            }
            base[slot] = v;
        }
    }
    if (!ok || !determined) {
        tc->kind = Kind::UNTYPED;
        tc->vals.clear();
        tc->vals.shrink_to_fit();
        return tc;
    }
    tc->kind = kind;
    tc->scale = scale;
    return tc;
}

struct CacheKey {
    PaxStore* store;
    uint32_t column;
    bool operator==(const CacheKey& o) const {
        return store == o.store && column == o.column;
    }
};
struct CacheKeyHash {
    size_t operator()(const CacheKey& k) const {
        return std::hash<const void*>()(k.store) * 1000003u + k.column;
    }
};

std::mutex g_cache_mu;
std::unordered_map<CacheKey, std::shared_ptr<const TypedColumn>, CacheKeyHash>
    g_cache;

// Convert a filter constant into the typed column's int64 domain. Returns
// false when the constant cannot be represented exactly enough to preserve
// the byte path's comparison result (caller then rejects the fast path).
//
// INT / DATE convert exactly (byte path compares int-vs-int / string-vs-string
// of fixed-width dates, both order-equivalent to the int representation).
//
// DECIMAL is subtle: the byte path parses the cell with strtod and compares as
// double against the (folded) double constant, e.g. q6's `'0.06'+0.01` folds to
// 0.0699999999999999983 which EXCLUDES 0.07 (double 0.0700000000000000067).
// Scaling by llround would snap 0.06999..*100 to 7 and wrongly include 0.07. We
// instead compute the exact integer boundary: f(I) = (double)I / 10^s equals
// strtod(cell) (both correctly-rounded), and f is monotone in the scaled int I,
// so a small search around llround(c*10^s) finds the true cut point.

// Extract a filter constant as a double (the value the byte path compares).
bool const_double(const pb::FilterExpr& c, double* d) {
    using FE = pb::FilterExpr;
    switch (c.op()) {
        case FE::CONST_INT: *d = static_cast<double>(c.int_val()); return true;
        case FE::CONST_UINT: *d = static_cast<double>(c.uint_val()); return true;
        case FE::CONST_DOUBLE: *d = c.double_val(); return true;
        default: return false;  // string on a decimal column -> byte path
    }
}

// Smallest scaled int I with f(I) >= c  (boundary for LT / GE / BETWEEN-lo).
int64_t dec_lower(double c, int scale) {
    const double P = static_cast<double>(ipow10(scale));
    auto f = [P](int64_t I) { return static_cast<double>(I) / P; };
    int64_t I = llround(c * P);
    while (f(I) >= c) --I;   // back down until f(I) < c
    while (f(I) < c) ++I;    // step up to the first f(I) >= c
    return I;
}
// Largest scaled int I with f(I) <= c  (boundary for GT / LE / BETWEEN-hi).
int64_t dec_upper(double c, int scale) {
    const double P = static_cast<double>(ipow10(scale));
    auto f = [P](int64_t I) { return static_cast<double>(I) / P; };
    int64_t I = llround(c * P);
    while (f(I) <= c) ++I;   // up until f(I) > c
    while (f(I) > c) --I;    // step down to the last f(I) <= c
    return I;
}

bool convert_int_date(const pb::FilterExpr& c, Kind kind, int64_t* out) {
    using FE = pb::FilterExpr;
    if (kind == Kind::INT) {
        switch (c.op()) {
            case FE::CONST_INT:
                *out = c.int_val();
                return true;
            case FE::CONST_UINT:
                if (c.uint_val() > static_cast<uint64_t>(INT64_MAX))
                    return false;
                *out = static_cast<int64_t>(c.uint_val());
                return true;
            case FE::CONST_DOUBLE: {
                const double d = c.double_val();
                if (d != static_cast<double>(static_cast<int64_t>(d)))
                    return false;  // non-integral vs INT: leave to bytes
                *out = static_cast<int64_t>(d);
                return true;
            }
            case FE::CONST_STRING: {
                const std::string& s = c.string_val();
                int64_t v;
                auto r = std::from_chars(s.data(), s.data() + s.size(), v);
                if (r.ec != std::errc() || r.ptr != s.data() + s.size())
                    return false;
                *out = v;
                return true;
            }
            default:
                return false;
        }
    }
    // DATE
    if (c.op() != FE::CONST_STRING) return false;
    Kind ck;
    int cs;
    int64_t v;
    if (!classify_cell(std::string_view(c.string_val()), &ck, &cs, &v) ||
        ck != Kind::DATE)
        return false;
    *out = v;
    return true;
}

// Try to lower one AND-conjunct (a comparison / BETWEEN over COLUMN_REF op
// CONST) into a typed Pred. Returns false if the conjunct is not of that form
// or a referenced column is untyped / a constant is not convertible.
bool lower_conjunct(const pb::FilterExpr& e, PaxStore* store, Filter* out,
                    uint64_t* build_ns) {
    using FE = pb::FilterExpr;
    Pred::Op op;
    bool is_between = false;
    switch (e.op()) {
        case FE::OP_LT: op = Pred::LT; break;
        case FE::OP_LE: op = Pred::LE; break;
        case FE::OP_GT: op = Pred::GT; break;
        case FE::OP_GE: op = Pred::GE; break;
        case FE::OP_EQ: op = Pred::EQ; break;
        case FE::OP_BETWEEN:
            op = Pred::BETWEEN;
            is_between = true;
            break;
        default:
            return false;
    }
    if (is_between) {
        if (e.negated() || e.children_size() != 3) return false;
    } else if (e.children_size() != 2) {
        return false;
    }
    const pb::FilterExpr& col = e.children(0);
    if (col.op() != FE::COLUMN_REF) return false;  // require column-first form

    auto tc = GetTypedColumn(store, col.column_index(), build_ns);
    if (!tc || tc->kind == Kind::UNTYPED) return false;

    Pred p;
    p.op = op;
    p.b = 0;
    if (tc->kind == Kind::DECIMAL) {
        // Operator-aware exact-boundary conversion (see const_double comment).
        if (is_between) {
            double lo, hi;
            if (!const_double(e.children(1), &lo) ||
                !const_double(e.children(2), &hi))
                return false;
            p.a = dec_lower(lo, tc->scale);   // I >= lo
            p.b = dec_upper(hi, tc->scale);   // I <= hi
        } else {
            double c;
            if (!const_double(e.children(1), &c)) return false;
            switch (op) {
                case Pred::LT:
                case Pred::GE:
                    p.a = dec_lower(c, tc->scale);
                    break;
                case Pred::GT:
                case Pred::LE:
                    p.a = dec_upper(c, tc->scale);
                    break;
                default: {  // EQ: only when the constant lands on an exact cell
                    const int64_t cand =
                        llround(c * static_cast<double>(ipow10(tc->scale)));
                    if (static_cast<double>(cand) /
                            static_cast<double>(ipow10(tc->scale)) !=
                        c)
                        return false;  // no exact match -> byte path
                    p.a = cand;
                    break;
                }
            }
        }
    } else if (is_between) {
        if (!convert_int_date(e.children(1), tc->kind, &p.a) ||
            !convert_int_date(e.children(2), tc->kind, &p.b))
            return false;
    } else {
        if (!convert_int_date(e.children(1), tc->kind, &p.a)) return false;
    }
    p.vals = tc->vals.data();
    out->holders.push_back(std::move(tc));
    out->preds.push_back(p);
    return true;
}

}  // namespace

Mode CurrentMode() {
    static const Mode m = [] {
        const char* v = std::getenv("LDBC_SIMD");
        if (v == nullptr || v[0] == '\0' || std::strcmp(v, "0") == 0)
            return Mode::OFF;
        if (std::strcmp(v, "scalar") == 0) return Mode::SCALAR;
        return Mode::AVX2;  // "1", "avx2", anything else
    }();
    return m;
}

std::shared_ptr<const TypedColumn> GetTypedColumn(PaxStore* store,
                                                  uint32_t column,
                                                  uint64_t* build_ns) {
    const CacheKey key{store, column};
    {
        std::lock_guard<std::mutex> lk(g_cache_mu);
        auto it = g_cache.find(key);
        if (it != g_cache.end() &&
            it->second->n_groups == store->group_count())
            return it->second;
    }
    const auto t0 = std::chrono::steady_clock::now();
    auto tc = Build(store, column);
    const auto t1 = std::chrono::steady_clock::now();
    if (build_ns != nullptr)
        *build_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                         t1 - t0)
                         .count();
    {
        std::lock_guard<std::mutex> lk(g_cache_mu);
        g_cache[key] = tc;
    }
    return tc;
}

size_t CacheBytes() {
    std::lock_guard<std::mutex> lk(g_cache_mu);
    size_t bytes = 0;
    for (const auto& kv : g_cache)
        bytes += kv.second->vals.capacity() * sizeof(int64_t);
    return bytes;
}

bool Compile(const pb::FilterExpr& expr, PaxStore* store, Filter* out,
             uint64_t* build_ns) {
    using FE = pb::FilterExpr;
    Filter f;
    if (expr.op() == FE::OP_AND) {
        if (expr.children_size() == 0) return false;
        for (const auto& ch : expr.children())
            if (!lower_conjunct(ch, store, &f, build_ns)) return false;
    } else {
        if (!lower_conjunct(expr, store, &f, build_ns)) return false;
    }
    if (f.preds.empty()) return false;
    *out = std::move(f);
    return true;
}

uint64_t EvalBlock64Scalar(const Filter& f, size_t abs_base) {
    uint64_t match = ~uint64_t{0};
    for (const Pred& p : f.preds) {
        const int64_t* col = p.vals + abs_base;
        uint64_t m = 0;
        for (uint32_t s = 0; s < 64; ++s) {
            const int64_t x = col[s];
            bool ok;
            switch (p.op) {
                case Pred::LT: ok = x < p.a; break;
                case Pred::LE: ok = x <= p.a; break;
                case Pred::GT: ok = x > p.a; break;
                case Pred::GE: ok = x >= p.a; break;
                case Pred::EQ: ok = x == p.a; break;
                default: ok = x >= p.a && x <= p.b; break;  // BETWEEN
            }
            if (ok) m |= (uint64_t{1} << s);
        }
        match &= m;
        if (match == 0) break;
    }
    return match;
}

__attribute__((target("avx2"))) uint64_t EvalBlock64Avx2(const Filter& f,
                                                         size_t abs_base) {
    uint64_t match = ~uint64_t{0};
    for (const Pred& p : f.preds) {
        const int64_t* col = p.vals + abs_base;
        uint64_t m = 0;
        if (p.op == Pred::BETWEEN) {
            const __m256i lo = _mm256_set1_epi64x(p.a);
            const __m256i hi = _mm256_set1_epi64x(p.b);
            for (int q = 0; q < 16; ++q) {
                const __m256i x = _mm256_loadu_si256(
                    reinterpret_cast<const __m256i*>(col + q * 4));
                // x >= lo  ==  !(lo > x)   ;   x <= hi  ==  !(x > hi)
                const int lt_lo =
                    _mm256_movemask_pd(_mm256_castsi256_pd(
                        _mm256_cmpgt_epi64(lo, x)));
                const int gt_hi =
                    _mm256_movemask_pd(_mm256_castsi256_pd(
                        _mm256_cmpgt_epi64(x, hi)));
                const uint64_t r = (~lt_lo & ~gt_hi) & 0xF;
                m |= r << (q * 4);
            }
        } else {
            const __m256i t = _mm256_set1_epi64x(p.a);
            for (int q = 0; q < 16; ++q) {
                const __m256i x = _mm256_loadu_si256(
                    reinterpret_cast<const __m256i*>(col + q * 4));
                int bits;
                switch (p.op) {
                    case Pred::LT:  // x < t  ==  t > x
                        bits = _mm256_movemask_pd(_mm256_castsi256_pd(
                            _mm256_cmpgt_epi64(t, x)));
                        break;
                    case Pred::GT:  // x > t
                        bits = _mm256_movemask_pd(_mm256_castsi256_pd(
                            _mm256_cmpgt_epi64(x, t)));
                        break;
                    case Pred::LE:  // x <= t  ==  !(x > t)
                        bits = (~_mm256_movemask_pd(_mm256_castsi256_pd(
                                   _mm256_cmpgt_epi64(x, t)))) & 0xF;
                        break;
                    case Pred::GE:  // x >= t  ==  !(t > x)
                        bits = (~_mm256_movemask_pd(_mm256_castsi256_pd(
                                   _mm256_cmpgt_epi64(t, x)))) & 0xF;
                        break;
                    default:  // EQ
                        bits = _mm256_movemask_pd(_mm256_castsi256_pd(
                            _mm256_cmpeq_epi64(x, t)));
                        break;
                }
                m |= static_cast<uint64_t>(bits & 0xF) << (q * 4);
            }
        }
        match &= m;
        if (match == 0) break;
    }
    return match;
}

}  // namespace simd
}  // namespace qb
