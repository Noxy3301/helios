#include "query_block_executor.hh"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include "predicate_evaluator.hh"

namespace qb {
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

inline std::string_view cell_of(const PaxStore* store, uint64_t ref,
                                uint32_t column) {
    const PaxGroup* g = store->group(ref / PaxGroup::kRows);
    return g->cell(column + 1,
                   static_cast<uint32_t>(ref % PaxGroup::kRows));
}

// Evaluate a FilterExpr arithmetic tree (COLUMN_REF/CONST/ADD/SUB/MUL/NEG)
// over one row of one table.
Dec eval_arith(const pb::FilterExpr& e, const PaxStore* store, uint64_t ref) {
    using FE = pb::FilterExpr;
    switch (e.op()) {
        case FE::COLUMN_REF:
            return dec_parse(cell_of(store, ref, e.column_index()));
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
            Dec a = eval_arith(e.children(0), store, ref);
            Dec b = eval_arith(e.children(1), store, ref);
            if (a.null || b.null) return {};
            dec_addsub(a, b, e.op() == FE::OP_SUB);
            return a;
        }
        case FE::OP_MUL: {
            if (e.children_size() != 2) return {};
            Dec a = eval_arith(e.children(0), store, ref);
            Dec b = eval_arith(e.children(1), store, ref);
            if (a.null || b.null) return {};
            Dec r;
            r.m = a.m * b.m;
            r.s = a.s + b.s;
            r.null = false;
            return r;
        }
        case FE::OP_NEG: {
            if (e.children_size() != 1) return {};
            Dec a = eval_arith(e.children(0), store, ref);
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

        const bool has_filter =
            scan.has_filter() && scan.filter().has_expr();
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
                for (size_t g = w; g < n_groups; g += wc) {
                    PaxGroup* grp = store->group(g);
                    if (grp == nullptr) continue;
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
                            if (has_filter) {
                                if (!ev.set_row_from_pax(
                                        *grp, slot,
                                        scan.filter().num_columns())) {
                                    failed[w] = 1;
                                    return;
                                }
                                if (!ev.evaluate(scan.filter().expr()))
                                    continue;
                            }
                            mine.push_back(g * PaxGroup::kRows + slot);
                        }
                    }
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
        if (join.build_keys_size() != join.probe_keys_size() ||
            join.build_keys_size() == 0)
            return fail("join key arity");

        // Resolve key columns to (ref column position, column).
        struct KeyCol {
            int pos;
            uint32_t column;
            const PaxStore* store;
        };
        std::vector<KeyCol> bk(build_key_refs.size()), pk(probe_key_refs.size());
        for (int i = 0; i < build_key_refs.size(); ++i) {
            const auto& c = build_key_refs.Get(i);
            const int pos = build.table_pos(c.table_idx());
            if (pos < 0) return fail("build key table not in child");
            bk[i] = {pos, c.column(), stores[c.table_idx()]};
        }
        for (int i = 0; i < probe_key_refs.size(); ++i) {
            const auto& c = probe_key_refs.Get(i);
            const int pos = probe.table_pos(c.table_idx());
            if (pos < 0) return fail("probe key table not in child");
            pk[i] = {pos, c.column(), stores[c.table_idx()]};
        }

        // Build hash table: key bytes -> build row indexes.
        std::unordered_map<std::string, std::vector<uint32_t>> ht;
        ht.reserve(build.rows());
        {
            std::string key;
            for (size_t r = 0; r < build.rows(); ++r) {
                key.clear();
                for (const auto& k : bk) {
                    append_join_key(key,
                                    cell_of(k.store, build.refs[k.pos][r],
                                            k.column));
                }
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
                std::string key;
                const size_t begin = n * w / wc;
                const size_t end = n * (w + 1) / wc;
                for (size_t r = begin; r < end; ++r) {
                    key.clear();
                    for (const auto& k : pk) {
                        append_join_key(
                            key, cell_of(k.store, probe.refs[k.pos][r],
                                         k.column));
                    }
                    const auto it = ht.find(key);
                    const bool matched =
                        it != ht.end() && !it->second.empty();
                    switch (join.type()) {
                        case pb::QbJoin::INNER:
                            if (!matched) break;
                            for (uint32_t br : it->second) {
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
                                for (uint32_t br : it->second) {
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
    struct GroupState {
        std::vector<std::string> key_cols;
        std::vector<uint64_t> count;   // per agg
        std::vector<Dec> acc;          // SUM/AVG accumulator or MIN/MAX (numeric)
        std::vector<std::string> sval; // MIN/MAX (binary string)
        std::vector<bool> has;         // MIN/MAX seen any
    };
    using GroupMap = std::unordered_map<std::string, GroupState>;

    bool AccumulateRange(const pb::QbAggregate& agg, const NodeResult& in,
                         size_t begin, size_t end, GroupMap& groups) {
        const int n_agg = agg.aggs_size();
        const int n_grp = agg.group_columns_size();
        // Resolve positions once.
        std::vector<int> gpos(n_grp);
        for (int g = 0; g < n_grp; ++g) {
            gpos[g] = in.table_pos(agg.group_columns(g).table_idx());
            if (gpos[g] < 0) return fail("group table not in input");
        }
        std::vector<int> apos(n_agg, -1);
        for (int a = 0; a < n_agg; ++a) {
            if (agg.aggs(a).has_arg() || agg.aggs(a).has_filter()) {
                apos[a] = in.table_pos(agg.aggs(a).arg_table());
                if (apos[a] < 0) return fail("agg table not in input");
            }
        }
        PredicateEvaluator ev;
        std::string keybuf;
        std::vector<std::string_view> gv(n_grp);
        for (size_t r = begin; r < end; ++r) {
            keybuf.clear();
            for (int g = 0; g < n_grp; ++g) {
                const auto& c = agg.group_columns(g);
                const uint64_t ref = in.refs[gpos[g]][r];
                gv[g] = ref == kNullRef
                            ? std::string_view()
                            : cell_of(stores[c.table_idx()], ref, c.column());
                const uint32_t l = static_cast<uint32_t>(gv[g].size());
                keybuf.append(reinterpret_cast<const char*>(&l), sizeof(l));
                keybuf.append(gv[g].data(), gv[g].size());
            }
            auto it = groups.find(keybuf);
            GroupState* gs;
            if (it == groups.end()) {
                GroupState st;
                st.key_cols.resize(n_grp);
                for (int g = 0; g < n_grp; ++g)
                    st.key_cols[g] = std::string(gv[g]);
                st.count.assign(n_agg, 0);
                st.acc.assign(n_agg, Dec{});
                st.sval.assign(n_agg, {});
                st.has.assign(n_agg, false);
                gs = &groups.emplace(std::move(keybuf), std::move(st))
                          .first->second;
                keybuf.clear();
            } else {
                gs = &it->second;
            }
            for (int a = 0; a < n_agg; ++a) {
                const auto& af = agg.aggs(a);
                const uint64_t ref =
                    apos[a] >= 0 ? in.refs[apos[a]][r] : 0;
                if (apos[a] >= 0 && ref == kNullRef) continue;  // LEFT null
                if (af.has_filter() && af.filter().has_expr()) {
                    const PaxStore* st = stores[af.arg_table()];
                    const PaxGroup* grp =
                        st->group(ref / PaxGroup::kRows);
                    if (!ev.set_row_from_pax(
                            *grp,
                            static_cast<uint32_t>(ref % PaxGroup::kRows),
                            af.filter().num_columns()))
                        return fail("agg filter unevaluable");
                    if (!ev.evaluate(af.filter().expr())) continue;
                }
                switch (af.kind()) {
                    case pb::QbAggFunc::COUNT:
                        gs->count[a] += 1;
                        break;
                    case pb::QbAggFunc::SUM:
                    case pb::QbAggFunc::AVG: {
                        Dec v = eval_arith(af.arg(),
                                           stores[af.arg_table()], ref);
                        if (v.null) break;  // NULL input skipped
                        if (gs->count[a] == 0)
                            gs->acc[a] = v;
                        else
                            dec_addsub(gs->acc[a], v, false);
                        gs->count[a] += 1;
                        break;
                    }
                    case pb::QbAggFunc::MIN:
                    case pb::QbAggFunc::MAX: {
                        const bool want_max =
                            af.kind() == pb::QbAggFunc::MAX;
                        if (af.cmp_kind() == 1) {
                            std::string_view cv = cell_of(
                                stores[af.arg_table()], ref,
                                af.arg().column_index());
                            if (cv.empty()) break;
                            if (!gs->has[a] ||
                                (want_max ? cv > std::string_view(gs->sval[a])
                                          : cv < std::string_view(gs->sval[a])))
                                gs->sval[a] = std::string(cv);
                            gs->has[a] = true;
                        } else {
                            Dec v = eval_arith(af.arg(),
                                               stores[af.arg_table()], ref);
                            if (v.null) break;
                            if (!gs->has[a]) {
                                gs->acc[a] = v;
                            } else {
                                Dec diff = v;  // diff = v - acc
                                dec_addsub(diff, gs->acc[a], true);
                                if (want_max ? diff.m > 0 : diff.m < 0)
                                    gs->acc[a] = v;
                            }
                            gs->has[a] = true;
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

    static void MergeGroups(GroupMap& dst, GroupMap& src,
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
                        d.count[a] += s.count[a];
                        break;
                    case pb::QbAggFunc::SUM:
                    case pb::QbAggFunc::AVG:
                        if (s.count[a] > 0) {
                            if (d.count[a] == 0)
                                d.acc[a] = s.acc[a];
                            else
                                dec_addsub(d.acc[a], s.acc[a], false);
                            d.count[a] += s.count[a];
                        }
                        break;
                    case pb::QbAggFunc::MIN:
                    case pb::QbAggFunc::MAX: {
                        if (!s.has[a]) break;
                        const bool want_max =
                            agg.aggs(a).kind() == pb::QbAggFunc::MAX;
                        if (agg.aggs(a).cmp_kind() == 1) {
                            if (!d.has[a] ||
                                (want_max ? s.sval[a] > d.sval[a]
                                          : s.sval[a] < d.sval[a]))
                                d.sval[a] = std::move(s.sval[a]);
                        } else {
                            if (!d.has[a]) {
                                d.acc[a] = s.acc[a];
                            } else {
                                Dec diff = d.acc[a];
                                dec_addsub(diff, s.acc[a], true);
                                const bool s_bigger = diff.m < 0;
                                if (want_max ? s_bigger : diff.m > 0)
                                    d.acc[a] = s.acc[a];
                            }
                        }
                        d.has[a] = true;
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    }

    // Format one aggregate's final value ("" + is_null for SQL NULL).
    std::string AggValue(const pb::QbAggFunc& af, const GroupState& gs, int a,
                         bool* is_null) {
        *is_null = false;
        switch (af.kind()) {
            case pb::QbAggFunc::COUNT:
                return std::to_string(gs.count[a]);
            case pb::QbAggFunc::SUM:
                if (gs.count[a] == 0) {
                    *is_null = true;
                    return {};
                }
                return dec_format(gs.acc[a]);
            case pb::QbAggFunc::AVG: {
                if (gs.count[a] == 0) {
                    *is_null = true;
                    return {};
                }
                const int out_scale =
                    static_cast<int>(af.arg_scale()) + 4;
                return dec_format(
                    dec_divide(gs.acc[a], gs.count[a], out_scale));
            }
            case pb::QbAggFunc::MIN:
            case pb::QbAggFunc::MAX:
                if (!gs.has[a]) {
                    *is_null = true;
                    return {};
                }
                return af.cmp_kind() == 1 ? gs.sval[a]
                                          : dec_format(gs.acc[a]);
            default:
                *is_null = true;
                return {};
        }
    }

    bool RunAggregateAndEmit(const pb::QbAggregate& agg,
                             pb::TxExecuteQueryBlock::Response* response) {
        const NodeResult& in = results[agg.input()];
        const int n_grp = agg.group_columns_size();
        const size_t n = in.rows();

        GroupMap groups;
        const unsigned wc = static_cast<unsigned>(std::min<size_t>(
            workers(), std::max<size_t>(n / 65536, 1)));
        if (wc <= 1) {
            if (!AccumulateRange(agg, in, 0, n, groups)) return false;
        } else {
            std::vector<GroupMap> locals(wc);
            std::vector<char> failed(wc, 0);
            std::vector<std::thread> pool;
            pool.reserve(wc);
            for (unsigned w = 0; w < wc; ++w) {
                pool.emplace_back([&, w] {
                    if (!AccumulateRange(agg, in, n * w / wc,
                                         n * (w + 1) / wc, locals[w]))
                        failed[w] = 1;
                });
            }
            for (auto& t : pool) t.join();
            for (char f : failed)
                if (f) return false;
            for (auto& l : locals) MergeGroups(groups, l, agg);
        }

        // Implicit grouping emits one row over zero input.
        if (n_grp == 0 && groups.empty()) {
            GroupState st;
            st.count.assign(agg.aggs_size(), 0);
            st.acc.assign(agg.aggs_size(), Dec{});
            st.sval.assign(agg.aggs_size(), {});
            st.has.assign(agg.aggs_size(), false);
            groups.emplace(std::string(), std::move(st));
        }

        // Build output rows: value strings per output expression.
        struct OutRow {
            std::vector<std::string> vals;
            std::vector<bool> nulls;
        };
        std::vector<OutRow> rows;
        if (agg.has_second()) {
            // Second-stage re-aggregation (q13 shape): re-group stage-1
            // values and COUNT(*) per group. Stage-1 value ordinal space:
            // [group columns..., aggregates...].
            if (!agg.second().count_star())
                return fail("second stage must count");
            std::unordered_map<std::string, uint64_t> second;
            std::string key;
            std::vector<std::string> kvals;
            for (auto& kv : groups) {
                GroupState& gs = kv.second;
                key.clear();
                kvals.clear();
                for (uint32_t ord : agg.second().group_value_ordinals()) {
                    std::string v;
                    bool is_null = false;
                    if (ord < static_cast<uint32_t>(n_grp)) {
                        v = gs.key_cols[ord];
                    } else {
                        const int a = static_cast<int>(ord) - n_grp;
                        if (a >= agg.aggs_size())
                            return fail("second stage ordinal");
                        v = AggValue(agg.aggs(a), gs, a, &is_null);
                    }
                    const uint32_t l = static_cast<uint32_t>(v.size());
                    key.append(reinterpret_cast<const char*>(&l), sizeof(l));
                    key.append(v);
                    kvals.push_back(std::move(v));
                }
                auto ins = second.emplace(key, 0);
                ins.first->second += 1;
            }
            rows.reserve(second.size());
            for (auto& kv : second) {
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
                    switch (oe.source()) {
                        case pb::QbOutputExpr::GROUP:
                            if (oe.ordinal() >= gvals.size())
                                return fail("second output ordinal");
                            row.vals.push_back(gvals[oe.ordinal()]);
                            row.nulls.push_back(false);
                            break;
                        case pb::QbOutputExpr::AGG:
                            row.vals.push_back(
                                std::to_string(kv.second));
                            row.nulls.push_back(false);
                            break;
                        default:
                            return fail("second output source");
                    }
                }
                rows.push_back(std::move(row));
            }
        } else {
        rows.reserve(groups.size());
        for (auto& kv : groups) {
            GroupState& gs = kv.second;
            OutRow row;
            row.vals.reserve(req.output_size());
            row.nulls.reserve(req.output_size());
            for (const auto& oe : req.output()) {
                switch (oe.source()) {
                    case pb::QbOutputExpr::GROUP: {
                        if (oe.ordinal() >=
                            static_cast<uint32_t>(n_grp))
                            return fail("group ordinal out of range");
                        row.vals.push_back(gs.key_cols[oe.ordinal()]);
                        row.nulls.push_back(false);
                        break;
                    }
                    case pb::QbOutputExpr::AGG: {
                        if (oe.ordinal() >=
                            static_cast<uint32_t>(agg.aggs_size()))
                            return fail("agg ordinal out of range");
                        bool is_null = false;
                        row.vals.push_back(
                            AggValue(agg.aggs(oe.ordinal()), gs,
                                     static_cast<int>(oe.ordinal()),
                                     &is_null));
                        row.nulls.push_back(is_null);
                        break;
                    }
                    default:
                        return fail("unsupported output source");
                }
            }
            rows.push_back(std::move(row));
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
        if (final_agg == nullptr) return fail("root must aggregate (B0)");
        if (!RunAggregateAndEmit(*final_agg, response)) return false;
        if (!Quiesced()) return fail("concurrent modification");
        return true;
    }
};

}  // namespace

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
