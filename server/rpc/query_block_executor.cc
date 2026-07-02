#include "query_block_executor.hh"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <set>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include "predicate_evaluator.hh"

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

inline std::string_view cell_of(const PaxStore* store, uint64_t ref,
                                uint32_t column) {
    const PaxGroup* g = store->group(ref / PaxGroup::kRows);
    return g->cell(column + 1,
                   static_cast<uint32_t>(ref % PaxGroup::kRows));
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
                return dec_parse(cell_of(store, ref, enc));
            const uint32_t t = enc >> 16;
            const uint32_t col = enc & 0xFFFF;
            for (size_t i = 0; i < ctx->tables->size(); ++i) {
                if ((*ctx->tables)[i] == t) {
                    const uint64_t r = (*ctx->refs)[i][ctx->row];
                    if (r == kNullRef) return {};
                    return dec_parse(cell_of((*ctx->stores)[t], r, col));
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
        out->reserve(src.rows());
        for (size_t r = 0; r < src.rows(); ++r) {
            const uint64_t ref = src.refs[pos][r];
            if (ref == kNullRef) continue;
            std::string_view v =
                value_of(semi.source_column().table_idx(), ref,
                         semi.source_column().column());
            if (!v.empty()) out->emplace(v);
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

        // Semi-join key filters: from the plan (earlier node's keys) and/or
        // injected by a parent executor (sub-block domain restriction).
        std::unordered_set<std::string> semi_keys;
        const std::unordered_set<std::string>* semi_set = nullptr;
        uint32_t semi_col = 0;
        if (scan.has_semi()) {
            if (!collect_keys(scan.semi(), &semi_keys)) return false;
            semi_set = &semi_keys;
            semi_col = scan.semi().my_column();
        }
        const bool use_ext = ext_keys != nullptr &&
                             ext_filter_table == scan.table_idx();

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
                            if (semi_set != nullptr) {
                                const std::string_view kv =
                                    grp->cell(semi_col + 1, slot);
                                if (semi_set->count(std::string(kv)) == 0)
                                    continue;
                            }
                            if (use_ext) {
                                const std::string_view kv = grp->cell(
                                    ext_filter_column + 1, slot);
                                if (ext_keys->count(std::string(kv)) == 0)
                                    continue;
                            }
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
    };

    bool prep_tuple_filter(const pb::QbTupleFilter& tf, const NodeResult& nr,
                           TupleFilterCtx* ctx) {
        ctx->pos.resize(tf.columns_size());
        for (int i = 0; i < tf.columns_size(); ++i) {
            ctx->pos[i] = nr.table_pos(tf.columns(i).table_idx());
            if (ctx->pos[i] < 0) return fail("tuple filter table");
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
        ev->set_row_from_views(ctx->cells, ctx->nulls);
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

        // Build hash table: key bytes -> build row indexes.
        std::unordered_map<std::string, std::vector<uint32_t>> ht;
        ht.reserve(build.rows());
        {
            std::string key;
            for (size_t r = 0; r < build.rows(); ++r) {
                key.clear();
                for (const auto& k : bk) {
                    append_join_key(key,
                                    value_of(k.table_idx,
                                             build.refs[k.pos][r],
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
                PredicateEvaluator ev;
                std::vector<std::string_view> rcells(residual_cols.size());
                std::vector<bool> rnulls(residual_cols.size(), false);
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
                    ev.set_row_from_views(rcells, rnulls);
                    return ev.evaluate(join.residual().pred().expr());
                };
                const size_t begin = n * w / wc;
                const size_t end = n * (w + 1) / wc;
                for (size_t r = begin; r < end; ++r) {
                    key.clear();
                    for (const auto& k : pk) {
                        append_join_key(
                            key, value_of(k.table_idx, probe.refs[k.pos][r],
                                          k.column));
                    }
                    const auto it = ht.find(key);
                    bool matched = it != ht.end() && !it->second.empty();
                    if (matched && has_residual) {
                        matched = false;
                        for (uint32_t br : it->second)
                            if (residual_ok(r, br)) {
                                matched = true;
                                break;
                            }
                    }
                    switch (join.type()) {
                        case pb::QbJoin::INNER:
                            if (!matched) break;
                            for (uint32_t br : it->second) {
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
                                for (uint32_t br : it->second) {
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
    struct GroupState {
        std::vector<std::string> key_cols;
        std::vector<uint64_t> count;   // per agg
        std::vector<Dec> acc;          // SUM/AVG accumulator or MIN/MAX (numeric)
        std::vector<std::string> sval; // MIN/MAX (binary string)
        std::vector<bool> has;         // MIN/MAX seen any
        // COUNT(DISTINCT): per-agg distinct value sets (small groups at
        // TPC-H scale; lineage log #16).
        std::vector<std::set<std::string>> dset;
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
        std::vector<int> fpos(n_agg, -1);
        for (int a = 0; a < n_agg; ++a) {
            if (agg.aggs(a).has_arg() || agg.aggs(a).has_filter()) {
                apos[a] = in.table_pos(agg.aggs(a).arg_table());
                if (apos[a] < 0) return fail("agg table not in input");
            }
            if (agg.aggs(a).has_filter()) {
                fpos[a] = in.table_pos(agg.aggs(a).filter_table());
                if (fpos[a] < 0) return fail("agg filter table not in input");
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
                            : value_of(c.table_idx(), ref, c.column());
                if (c.prefix_len() > 0 && gv[g].size() > c.prefix_len())
                    gv[g] = gv[g].substr(0, c.prefix_len());
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
                st.dset.resize(n_agg);
                gs = &groups.emplace(std::move(keybuf), std::move(st))
                          .first->second;
                keybuf.clear();
            } else {
                gs = &it->second;
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
                    if (!ev.set_row_from_pax(
                            *grp,
                            static_cast<uint32_t>(fref % PaxGroup::kRows),
                            af.filter().num_columns()))
                        return fail("agg filter unevaluable");
                    if (!ev.evaluate(af.filter().expr())) continue;
                }
                // Virtual-table aggregate arguments (one-row derived
                // scalars, q11): read through value_of, not stores[].
                const bool arg_virtual = is_virtual(af.arg_table());
                switch (af.kind()) {
                    case pb::QbAggFunc::COUNT:
                        if (af.distinct()) {
                            const std::string_view dv = value_of(
                                af.arg_table(), ref,
                                af.arg().column_index());
                            if (!dv.empty())
                                gs->dset[a].emplace(dv);
                            break;
                        }
                        gs->count[a] += 1;
                        break;
                    case pb::QbAggFunc::SUM:
                    case pb::QbAggFunc::AVG: {
                        Dec v =
                            arg_virtual
                                ? dec_parse(value_of(
                                      af.arg_table(), ref,
                                      af.arg().column_index()))
                                : eval_arith(af.arg(),
                                             stores[af.arg_table()], ref,
                                             &arith_ctx);
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
                            std::string_view cv = value_of(
                                af.arg_table(), ref,
                                af.arg().column_index());
                            if (cv.empty()) break;
                            if (!gs->has[a] ||
                                (want_max ? cv > std::string_view(gs->sval[a])
                                          : cv < std::string_view(gs->sval[a])))
                                gs->sval[a] = std::string(cv);
                            gs->has[a] = true;
                        } else {
                            Dec v = arg_virtual
                                        ? dec_parse(value_of(
                                              af.arg_table(), ref,
                                              af.arg().column_index()))
                                        : eval_arith(af.arg(),
                                                     stores[af.arg_table()],
                                                     ref);
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
                        if (agg.aggs(a).distinct()) {
                            d.dset[a].merge(s.dset[a]);
                            break;
                        }
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
                return std::to_string(af.distinct() ? gs.dset[a].size()
                                                    : gs.count[a]);
            case pb::QbAggFunc::SUM:
                if (gs.count[a] == 0) {
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
                std::string_view v = value_of(oe.column().table_idx(), ref,
                                              oe.column().column());
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
            st.dset.resize(agg.aggs_size());
            groups.emplace(std::string(), std::move(st));
        }

        // HAVING: drop groups failing the predicate over the stage-1
        // value layout [group values..., aggregate values...].
        if (agg.has_having() && agg.having().has_expr()) {
            PredicateEvaluator hev;
            std::vector<std::string> hv;
            std::vector<std::string_view> hcells;
            std::vector<bool> hnulls;
            for (auto it = groups.begin(); it != groups.end();) {
                GroupState& gs = it->second;
                hv.clear();
                hnulls.clear();
                for (int g = 0; g < n_grp; ++g) {
                    hv.push_back(gs.key_cols[g]);
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
                    case pb::QbOutputExpr::EXPR: {
                        Dec v = EvalOutExpr(oe.expr(), agg, gs);
                        if (!v.null && v.s > static_cast<int>(
                                                 oe.result_scale())) {
                            // Round to the declared output scale.
                            const int drop = v.s - oe.result_scale();
                            __int128 m = v.m;
                            const bool neg = m < 0;
                            if (neg) m = -m;
                            m = (m + dec_pow10(drop) / 2) / dec_pow10(drop);
                            v.m = neg ? -m : m;
                            v.s = oe.result_scale();
                        }
                        row.vals.push_back(v.null ? std::string()
                                                  : dec_format(v));
                        row.nulls.push_back(v.null);
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
