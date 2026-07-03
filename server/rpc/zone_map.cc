#include "zone_map.hh"

#include <cmath>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace qb {
namespace zone {

using LineairDB::Pax::PaxGroup;
using LineairDB::Pax::PaxStore;

namespace {

int64_t ipow10(int n) {
    int64_t v = 1;
    while (n-- > 0) v *= 10;
    return v;
}

// ---------------------------------------------------------------------------
// Process-wide cache: one ZoneColumn per (PaxStore*, column).
// ---------------------------------------------------------------------------
struct ZoneColumn {
    std::mutex mu;
    ZKind kind = ZKind::UNTYPED;
    int scale = 0;
    bool kind_locked = false;  // kind/scale decided (UNTYPED = never prunable)
    std::vector<StripZone> strips;
};

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

std::mutex g_mu;
std::unordered_map<CacheKey, std::unique_ptr<ZoneColumn>, CacheKeyHash> g_cache;

// The cache is keyed by the raw PaxStore* and is never evicted. PAX stores are
// database-lifetime objects (a table reload requires a server restart, per the
// bench recipe), so a pointer is never freed and re-allocated under a live
// process; even if it were, a mismatched locked kind would make cells fail
// ClassifyCell -> ZS_INVALID -> no prune (never a wrong result), and the
// write_counter check would force a rebuild.
ZoneColumn* get_col(PaxStore* store, uint32_t column) {
    std::lock_guard<std::mutex> lk(g_mu);
    auto& slot = g_cache[CacheKey{store, column}];
    if (!slot) slot = std::make_unique<ZoneColumn>();
    return slot.get();  // heap object; stable across rehash and never erased
}

// Compute one strip's zone over its visible, typed, non-null cells. A cell that
// fails typing (or mismatches the column's locked kind/scale) marks the strip
// non-prunable (ZS_INVALID). An all-null / all-empty strip is also INVALID:
// those rows never satisfy a comparison, so scanning them is correct (just not
// optimized), and we have no min/max to prune with.
StripZone build_strip(PaxGroup* grp, uint32_t column, ZKind kind, int scale,
                      uint64_t wc) {
    StripZone z;
    z.wc = wc;
    z.state = ZS_INVALID;
    bool any = false;
    int64_t mn = 0, mx = 0;
    for (uint32_t slot = 0; slot < PaxGroup::kRows; ++slot) {
        if (!grp->IsVisible(slot)) continue;
        const std::string_view cv = grp->cell(column + 1, slot);
        // Empty cell: for a numeric column (the only prunable kind) an empty
        // cell is always a NULL, which the byte path evaluates to unknown and
        // excludes from any comparison — so leaving it out of min/max cannot
        // over-prune. (A non-null empty STRING cell is possible only for string
        // columns, which never reach a prunable zone: zone_ct_ok rejects them.)
        if (cv.empty()) continue;
        ZKind ck;
        int cs;
        int64_t v;
        if (!ClassifyCell(cv, &ck, &cs, &v) || ck != kind || cs != scale) {
            z.state = ZS_INVALID;
            return z;
        }
        if (!any) {
            mn = mx = v;
            any = true;
        } else {
            if (v < mn) mn = v;
            if (v > mx) mx = v;
        }
    }
    if (any) {
        z.zmin = mn;
        z.zmax = mx;
        z.state = ZS_VALID;
    }
    return z;
}

}  // namespace

bool Enabled() {
    static const bool on = [] {
        const char* v = std::getenv("LDBC_ZONEMAP");
        return !(v != nullptr && v[0] == '0' && v[1] == '\0');
    }();
    return on;
}

bool ClassifyCell(std::string_view s, ZKind* k, int* scale, int64_t* val) {
    if (s.empty()) return false;

    // DATE: fixed "YYYY-MM-DD" -> YYYYMMDD (int order == fixed-width str order).
    if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
        int64_t y = 0, m = 0, d = 0;
        auto digs = [](std::string_view t, int64_t* o) {
            for (char c : t) {
                if (c < '0' || c > '9') return false;
                *o = *o * 10 + (c - '0');
            }
            return true;
        };
        if (digs(s.substr(0, 4), &y) && digs(s.substr(5, 2), &m) &&
            digs(s.substr(8, 2), &d)) {
            *k = ZKind::DATE;
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
    int ndig = 0;
    bool seen_dot = false;
    for (; i < s.size(); ++i) {
        const char c = s[i];
        if (c == '.') {
            if (seen_dot) return false;
            seen_dot = true;
            continue;
        }
        if (c < '0' || c > '9') return false;
        ++ndig;
        if (ndig > 18) return false;  // bound |m| < 10^18 BEFORE the multiply,
                                      // so the int64 accumulation never overflows
        m = m * 10 + (c - '0');
        if (seen_dot) ++sc;
    }
    if (ndig == 0) return false;
    if (seen_dot) {
        // Bound the scaled int so |m| < 10^15 (exact double) and 10^scale is
        // exactly representable — enough for all TPC-H decimals (scale 2).
        if (sc > 15 || ndig > 15) return false;
        *k = ZKind::DECIMAL;
    } else {
        *k = ZKind::INT;  // ndig <= 18 already guaranteed above
    }
    *scale = sc;
    *val = neg ? -m : m;
    return true;
}

// f(I) = (double)I / 10^scale == strtod(cell); monotone in I, so a small search
// around llround(c * 10^scale) finds the exact integer cut point.
int64_t DecLower(double c, int scale) {
    const double P = static_cast<double>(ipow10(scale));
    auto f = [P](int64_t I) { return static_cast<double>(I) / P; };
    int64_t I = static_cast<int64_t>(std::llround(c * P));
    while (f(I) >= c) --I;  // back down until f(I) < c
    while (f(I) < c) ++I;   // step up to the first f(I) >= c
    return I;
}
int64_t DecUpper(double c, int scale) {
    const double P = static_cast<double>(ipow10(scale));
    auto f = [P](int64_t I) { return static_cast<double>(I) / P; };
    int64_t I = static_cast<int64_t>(std::llround(c * P));
    while (f(I) <= c) ++I;  // up until f(I) > c
    while (f(I) > c) --I;   // step down to the last f(I) <= c
    return I;
}

bool GetColumnZones(PaxStore* store, uint32_t column, size_t n_groups,
                    std::vector<StripZone>* out, ZKind* kind, int* scale) {
    if (!Enabled() || n_groups == 0) return false;
    // Typed cells (M2) hold fixed-width LE binary that ClassifyCell would
    // mis-read as ASCII (e.g. a typed int whose bytes are digit characters),
    // yielding a bogus min/max and an incorrect prune. Zone maps are
    // perf-neutral on TPC-H, so typed columns are simply never pruned (skipping
    // a prune is always correct). Field index for MySQL column c is c+1.
    const auto& fkinds = store->schema().field_kind;
    if (static_cast<size_t>(column) + 1 < fkinds.size() &&
        fkinds[column + 1] != LineairDB::Pax::FK_UNTYPED)
        return false;
    ZoneColumn* zc = get_col(store, column);
    std::lock_guard<std::mutex> lk(zc->mu);

    // Lock the column kind/scale from the first visible, non-null cell. A typed
    // first cell fixes the kind; a non-numeric first cell locks UNTYPED (string
    // column, never prunable). No visible data yet -> retry on a later scan.
    if (!zc->kind_locked) {
        for (size_t g = 0; g < n_groups && !zc->kind_locked; ++g) {
            PaxGroup* grp = store->group(g);
            if (grp == nullptr) continue;
            for (uint32_t slot = 0; slot < PaxGroup::kRows; ++slot) {
                if (!grp->IsVisible(slot)) continue;
                const std::string_view cv = grp->cell(column + 1, slot);
                if (cv.empty()) continue;  // skip nulls, keep looking
                ZKind ck;
                int cs;
                int64_t v;
                if (ClassifyCell(cv, &ck, &cs, &v)) {
                    zc->kind = ck;
                    zc->scale = cs;
                } else {
                    zc->kind = ZKind::UNTYPED;
                }
                zc->kind_locked = true;
                break;
            }
        }
        if (!zc->kind_locked) return false;
    }
    if (zc->kind == ZKind::UNTYPED) return false;

    // Refresh: rebuild strips that are absent or whose write_counter changed.
    // Reading the current counter (not the executor snapshot) is equivalent in
    // the success case (Quiesced() proves they match) and keeps this module
    // independent of the executor's snapshot layout.
    if (zc->strips.size() < n_groups) zc->strips.resize(n_groups);
    for (size_t g = 0; g < n_groups; ++g) {
        PaxGroup* grp = store->group(g);
        if (grp == nullptr) {
            zc->strips[g].state = ZS_INVALID;
            zc->strips[g].wc = 0;
            continue;
        }
        const uint64_t wc1 =
            grp->write_counter.load(std::memory_order_acquire);
        StripZone& z = zc->strips[g];
        if (z.state != ZS_ABSENT && z.wc == wc1) continue;  // cached & fresh
        StripZone nz = build_strip(grp, column, zc->kind, zc->scale, wc1);
        // If the counter moved during the build the cells may be torn: mark the
        // strip non-prunable and force a rebuild on a later (quiescent) scan.
        // A VALID cached zone therefore always reflects a consistent cell
        // snapshot at counter wc1, and is reused only while the live counter
        // still equals wc1 (monotone bump => no modification since). This is a
        // strict complement to the executor's Prepare/Quiesced check, not a
        // replacement for it.
        const uint64_t wc2 =
            grp->write_counter.load(std::memory_order_acquire);
        if (wc2 != wc1) {
            nz.state = ZS_INVALID;
            nz.wc = 0;  // != any live counter => rebuilt on the next scan
        }
        z = nz;
    }

    // Snapshot into the caller's request-local vector so the hot scan loop
    // reads immutable data with no further locking.
    out->assign(zc->strips.begin(), zc->strips.begin() + n_groups);
    *kind = zc->kind;
    *scale = zc->scale;
    return true;
}

}  // namespace zone
}  // namespace qb
