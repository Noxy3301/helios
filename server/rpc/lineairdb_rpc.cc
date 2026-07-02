#include "lineairdb_rpc.hh"
#include "predicate_evaluator.hh"
#include "../../common/log.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <unordered_set>

#include "lineairdb.pb.h"
#include <lineairdb/pax_store.h>
#include <map>
#include <set>
#include <thread>
#include <type_traits>

namespace {

// Bump the byte string to its lexicographic successor; returns empty on overflow.
std::string next_lexicographic_key(std::string key) {
    for (size_t i = key.size(); i-- > 0;) {
        auto byte = static_cast<unsigned char>(key[i]);
        if (byte != 0xFF) {
            key[i] = static_cast<char>(byte + 1);
            key.resize(i + 1);
            return key;
        }
    }
    return {};
}

/**
 * @brief Trim a serialized row value to the projected columns.
 *
 * @details Row format is [null_flags][col_0]..[col_n], where each field is
 * [byteSize:1B][len:byteSize B][bytes] and byteSize==0xFF means NULL. The
 * output keeps the full null_flags field and emits only the kept columns.
 * Returns false on malformed input; the caller then fails the read-plan
 * response instead of shipping a mismatched row layout.
 */
bool trim_row_value(const std::string& full,
                    const google::protobuf::RepeatedField<uint32_t>& kept,
                    uint32_t num_columns, std::string& out) {
    out.clear();
    const char* end = full.data() + full.size();
    auto read_field = [&](const char*& q, const char*& fstart,
                          size_t& flen) -> bool {
        fstart = q;
        if (q >= end) return false;
        uint8_t bs = static_cast<uint8_t>(*q);
        if (bs == 0xFF) { flen = 1; q += 1; return true; }
        if (q + 1 + bs > end) return false;
        size_t len = 0;
        for (uint8_t i = 0; i < bs; i++)
            len |= static_cast<size_t>(static_cast<uint8_t>(q[1 + i])) << (8 * i);
        if (q + 1 + bs + len > end) return false;
        flen = 1 + bs + len;
        q += flen;
        return true;
    };
    const char* q = full.data();
    const char* fs;
    size_t fl;
    if (!read_field(q, fs, fl)) return false;  // field 0 = null_flags
    out.append(fs, fl);
    int ki = 0;
    for (uint32_t c = 0; c < num_columns; c++) {  // column c is field index c+1
        const char* cs;
        size_t cl;
        if (!read_field(q, cs, cl)) return false;
        if (ki < kept.size() &&
            kept.Get(ki) == static_cast<uint32_t>(c)) {
            out.append(cs, cl);
            ki++;
        }
    }
    return ki == kept.size();  // every requested column was present
}

// Return the bytes of column `column_index` from a serialized MySQL row payload.
std::string_view extract_value_column(const std::string& row,
                                      int column_index) {
    size_t offset = 0;
    int field_index = 0;
    const int target_field = column_index + 1; // field 0 is null flags.

    while (offset < row.size()) {
        const auto byte_size = static_cast<unsigned char>(row[offset]);
        ++offset;
        if (byte_size == 0xFF) {
            if (field_index == target_field) return {};
            ++field_index;
            continue;
        }
        if (offset + byte_size > row.size()) return {};

        size_t value_length = 0;
        for (unsigned int i = 0; i < byte_size; ++i) {
            value_length |= static_cast<size_t>(
                static_cast<unsigned char>(row[offset + i])) << (8 * i);
        }
        offset += byte_size;
        if (offset + value_length > row.size()) return {};

        if (field_index == target_field) {
            return std::string_view(row.data() + offset, value_length);
        }
        offset += value_length;
        ++field_index;
    }
    return {};
}

/**
 * @brief Append one LineairDBField-format field to a synthetic group row.
 *
 * Format: [byteSize][valueLength][value]. A null field is encoded as 0xFF.
 */
void agg_emit_field(std::string& out, std::string_view v, bool is_null) {
    if (is_null) { out.push_back(static_cast<char>(0xFF)); return; }
    size_t len = v.size();
    size_t bs = 1;
    for (size_t t = len >> 8; t; t >>= 8) ++bs;
    out.push_back(static_cast<char>(bs));
    for (size_t i = 0; i < bs; ++i)
        out.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
    out.append(v.data(), v.size());
}

/**
 * @brief Exact fixed-point decimal used for server-side SUM/AVG partials.
 */
struct Dec { __int128 m = 0; int s = 0; bool null = false; };

/**
 * @brief Return 10^n as an exact integer.
 */
static __int128 dec_pow10(int n) {
    __int128 r = 1;
    while (n-- > 0) r *= 10;
    return r;
}

/**
 * @brief Parse an ASCII decimal string into Dec.
 */
static Dec dec_parse(std::string_view v) {
    Dec d;
    if (v.empty()) { d.null = true; return d; }
    size_t i = 0; bool neg = false;
    if (v[0] == '-') { neg = true; i = 1; } else if (v[0] == '+') i = 1;
    __int128 m = 0; int scale = 0; bool dot = false;
    for (; i < v.size(); ++i) {
        char c = v[i];
        if (c == '.') { dot = true; continue; }
        if (c < '0' || c > '9') { d.null = true; return d; }
        m = m * 10 + (c - '0');
        if (dot) ++scale;
    }
    d.m = neg ? -m : m; d.s = scale;
    return d;
}

/**
 * @brief Add or subtract two Dec values after aligning their scales.
 */
static void dec_addsub(Dec& a, const Dec& b, bool sub) {
    const int s = a.s > b.s ? a.s : b.s;
    __int128 am = a.m * dec_pow10(s - a.s);
    __int128 bm = b.m * dec_pow10(s - b.s);
    a.m = sub ? am - bm : am + bm;
    a.s = s;
    a.null = a.null || b.null;
}

// Collect every column a FilterExpr tree references.
static void collect_filter_columns(const LineairDB::Protocol::FilterExpr& e,
                                   std::set<uint32_t>& out) {
    if (e.op() == LineairDB::Protocol::FilterExpr::COLUMN_REF) {
        out.insert(e.column_index());
    }
    for (const auto& c : e.children()) collect_filter_columns(c, out);
}

// Columns a step actually consumes server- or proxy-side. Non-empty ONLY
// when the step carries a RowProjection (the proxy's read_set bound): the
// scan may then materialize just these columns for PAX rows, leaving the
// rest as 1-byte placeholders (full row shape). Without a projection the
// proxy may read any column, so the full row is required.
static std::vector<uint32_t> build_sparse_columns(
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step) {
    if (!step.has_projection()) return {};
    std::set<uint32_t> cols(step.projection().field_indexes().begin(),
                            step.projection().field_indexes().end());
    if (step.has_filter() && step.filter().has_expr()) {
        collect_filter_columns(step.filter().expr(), cols);
    }
    for (const auto& sj : step.semijoins()) cols.insert(sj.probe_column());
    return std::vector<uint32_t>(cols.begin(), cols.end());
}

// Row-source abstraction for aggregate-argument evaluation: a materialized
// row string walks the field headers per access; a PAX row reads the cell
// straight from its column strip.
struct PaxRowRef {
    const LineairDB::Pax::PaxGroup* group;
    uint32_t slot;
};
static inline std::string_view row_column(const std::string& row,
                                          uint32_t idx) {
    return extract_value_column(row, idx);
}
static inline std::string_view row_column(const PaxRowRef& row, uint32_t idx) {
    return row.group->cell(idx + 1, row.slot);  // field 0 = null flags
}

/**
 * @brief Evaluate an aggregate argument expression against one base row.
 */
template <typename Row>
static Dec dec_eval(const LineairDB::Protocol::FilterExpr& e, const Row& row) {
    using FE = LineairDB::Protocol::FilterExpr;
    switch (e.op()) {
        case FE::COLUMN_REF:
            return dec_parse(row_column(row, e.column_index()));
        case FE::CONST_INT:  { Dec d; d.m = e.int_val();  d.s = 0; return d; }
        case FE::CONST_UINT: { Dec d; d.m = (__int128)e.uint_val(); d.s = 0; return d; }
        case FE::OP_ADD: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            dec_addsub(a, dec_eval(e.children(1), row), false);
            return a;
        }
        case FE::OP_SUB: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            dec_addsub(a, dec_eval(e.children(1), row), true);
            return a;
        }
        case FE::OP_MUL: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            Dec b = dec_eval(e.children(1), row);
            Dec r; r.m = a.m * b.m; r.s = a.s + b.s; r.null = a.null || b.null;
            return r;
        }
        case FE::OP_NEG: {
            if (e.children_size() != 1) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row); a.m = -a.m; return a;
        }
        default: { Dec d; d.null = true; return d; }
    }
}

/**
 * @brief Format Dec back to its decimal string representation.
 */
static std::string dec_format(const Dec& d) {
    __int128 m = d.m; bool neg = m < 0; if (neg) m = -m;
    std::string digits;
    if (m == 0) digits = "0";
    else { while (m) { digits.push_back(char('0' + int(m % 10))); m /= 10; }
           std::reverse(digits.begin(), digits.end()); }
    while (static_cast<int>(digits.size()) <= d.s) digits.insert(digits.begin(), '0');
    std::string out;
    if (neg) out.push_back('-');
    if (d.s == 0) { out += digits; return out; }
    const size_t ip = digits.size() - d.s;
    out.append(digits, 0, ip); out.push_back('.'); out.append(digits, ip, std::string::npos);
    return out;
}

static constexpr uint32_t kAggregateHavingFilterColumns =
    std::numeric_limits<uint32_t>::max();

static bool is_aggregate_having_filter(
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step) {
    return step.has_aggregate() && step.has_filter() &&
           step.filter().has_expr() &&
           step.filter().num_columns() == kAggregateHavingFilterColumns;
}

static bool dec_from_filter_const(const LineairDB::Protocol::FilterExpr& e,
                                  Dec& out) {
    using FE = LineairDB::Protocol::FilterExpr;
    switch (e.op()) {
        case FE::CONST_INT:
            out.m = e.int_val();
            out.s = 0;
            out.null = false;
            return true;
        case FE::CONST_UINT:
            out.m = static_cast<__int128>(e.uint_val());
            out.s = 0;
            out.null = false;
            return true;
        case FE::CONST_STRING:
            out = dec_parse(e.string_val());
            return !out.null;
        default:
            return false;
    }
}

static bool aggregate_having_row_passes(
    const LineairDB::Protocol::PushedPredicate* pred,
    const std::string& group_row) {
    if (pred == nullptr) return true;
    const auto& e = pred->expr();
    using FE = LineairDB::Protocol::FilterExpr;
    switch (e.op()) {
        case FE::OP_GT:
        case FE::OP_GE:
        case FE::OP_LT:
        case FE::OP_LE:
        case FE::OP_EQ:
        case FE::OP_NE:
            break;
        default:
            return false;
    }
    if (e.children_size() != 2 ||
        e.children(0).op() != FE::COLUMN_REF)
        return false;

    Dec lhs = dec_parse(
        extract_value_column(group_row, e.children(0).column_index()));
    Dec rhs;
    if (lhs.null || !dec_from_filter_const(e.children(1), rhs)) return false;

    __int128 a = lhs.m;
    __int128 b = rhs.m;
    if (lhs.s < rhs.s) {
        a *= dec_pow10(rhs.s - lhs.s);
    } else if (rhs.s < lhs.s) {
        b *= dec_pow10(lhs.s - rhs.s);
    }

    switch (e.op()) {
        case FE::OP_GT: return a > b;
        case FE::OP_GE: return a >= b;
        case FE::OP_LT: return a < b;
        case FE::OP_LE: return a <= b;
        case FE::OP_EQ: return a == b;
        case FE::OP_NE: return a != b;
        default: return false;
    }
}

/**
 * @brief Accumulators for one GROUP BY key.
 */
struct AggGroupState {
    std::vector<std::string> key_cols;   // captured group-by column values
    std::vector<uint64_t> count;         // per agg: COUNT / non-null counter
    std::vector<Dec> sum;                // per agg: SUM/AVG accumulator
};

/**
 * @brief Accumulate rows in [begin, end) into a caller-owned group map.
 */
static void aggregate_rows_range(
    const LineairDB::Protocol::AggregateSpec& spec,
    const std::vector<LineairDB::StatelessScanRow>& rows,
    size_t begin, size_t end,
    std::unordered_map<std::string, AggGroupState>& groups) {
    using AF = LineairDB::Protocol::AggFunc;
    const int n_agg = spec.aggs_size();
    const int n_grp = spec.group_columns_size();
    std::string keybuf;
    std::vector<std::string_view> gv(n_grp);
    for (size_t row_index = begin; row_index < end; ++row_index) {
        const auto& row = rows[row_index];
        // Build an unambiguous binary key from the group column bytes.
        keybuf.clear();
        for (int g = 0; g < n_grp; ++g) {
            gv[g] = extract_value_column(row.value, spec.group_columns(g));
            const uint32_t l = static_cast<uint32_t>(gv[g].size());
            keybuf.append(reinterpret_cast<const char*>(&l), sizeof(l));
            keybuf.append(gv[g].data(), gv[g].size());
        }
        auto it = groups.find(keybuf);
        AggGroupState* gs;
        if (it == groups.end()) {
            AggGroupState s;
            s.key_cols.resize(n_grp);
            for (int g = 0; g < n_grp; ++g) s.key_cols[g] = std::string(gv[g]);
            s.count.assign(n_agg, 0);
            s.sum.assign(n_agg, Dec{});
            gs = &groups.emplace(std::move(keybuf), std::move(s)).first->second;
            keybuf.clear();
        } else {
            gs = &it->second;
        }

        // Apply each aggregate to this row.
        for (int a = 0; a < n_agg; ++a) {
            const auto& af = spec.aggs(a);
            if (af.kind() == AF::AGG_COUNT) { gs->count[a] += 1; continue; }
            // SUM / AVG: evaluate the exact decimal arg, accumulate non-nulls.
            Dec v = dec_eval(af.arg(), row.value);
            if (!v.null) { dec_addsub(gs->sum[a], v, false); gs->count[a] += 1; }
        }
    }
}

/**
 * @brief Merge one worker-local aggregate map into another.
 */
static void merge_agg_groups(
    std::unordered_map<std::string, AggGroupState>& dst,
    std::unordered_map<std::string, AggGroupState>& src,
    int n_agg) {
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

        AggGroupState& d = it->second;
        AggGroupState& s = kv.second;
        for (int a = 0; a < n_agg; ++a) {
            d.count[a] += s.count[a];
            dec_addsub(d.sum[a], s.sum[a], false);
        }
    }
}

/**
 * @brief Serialize aggregate groups as synthetic scan rows for the proxy.
 */
static void emit_agg_groups(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::unordered_map<std::string, AggGroupState>& groups,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result,
    const LineairDB::Protocol::PushedPredicate* group_having = nullptr) {
    using AF = LineairDB::Protocol::AggFunc;
    const int n_agg = spec.aggs_size();
    const int n_grp = spec.group_columns_size();

    // Implicit grouping must emit one row even over zero input rows:
    // COUNT(*) => 0, SUM/AVG => NULL.
    if (n_grp == 0 && groups.empty()) {
        AggGroupState s;
        s.count.assign(n_agg, 0);
        s.sum.assign(n_agg, Dec{});
        groups.emplace(std::string(), std::move(s));
    }

    // Serialize final group states as synthetic rows for the proxy.
    for (auto& kv : groups) {
        AggGroupState& s = kv.second;
        std::string row;
        agg_emit_field(row, std::string_view(), false);  // null_flags (empty)
        for (int g = 0; g < n_grp; ++g) agg_emit_field(row, s.key_cols[g], false);
        for (int a = 0; a < n_agg; ++a) {
            const auto& af = spec.aggs(a);
            const std::string cnt = std::to_string(s.count[a]);
            if (af.kind() == AF::AGG_COUNT) {
                agg_emit_field(row, cnt, false);
                agg_emit_field(row, cnt, false);
            } else {  // SUM / AVG: (exact decimal sum | null) , non-null count
                if (s.count[a] == 0) agg_emit_field(row, std::string_view(), true);
                else agg_emit_field(row, dec_format(s.sum[a]), false);
                agg_emit_field(row, cnt, false);
            }
        }
        if (!aggregate_having_row_passes(group_having, row)) continue;
        step_result->add_scan_keys(std::string());
        step_result->add_scan_values(std::move(row));
        step_result->add_scan_tids(0);
    }
}

/**
 * @brief Aggregate scan rows and emit one synthetic group row per group.
 *
 * Each output row is [null_flags][group columns][value,count per aggregate].
 */
bool server_aggregate_scan(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::vector<LineairDB::StatelessScanRow>& rows,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result,
    const LineairDB::Protocol::PushedPredicate* group_having = nullptr) {
    const int n_agg = spec.aggs_size();
    std::unordered_map<std::string, AggGroupState> groups;

    const size_t morsel_rows = 100000;  // FIXME: make configurable
    const unsigned nproc = std::thread::hardware_concurrency();
    const unsigned max_threads = nproc ? nproc : 4;  // FIXME: make configurable
    const size_t desired_workers =
        morsel_rows > 0 ? (rows.size() + morsel_rows - 1) / morsel_rows : 1;
    const unsigned worker_count =
        desired_workers > 1 && max_threads > 1
            ? static_cast<unsigned>(
                  std::min<size_t>(desired_workers, max_threads))
            : 1;

    if (worker_count <= 1) {
        aggregate_rows_range(spec, rows, 0, rows.size(), groups);
    } else {
        // Each worker writes a private group map; the parent merges them.
        std::vector<std::unordered_map<std::string, AggGroupState>> locals(
            worker_count);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);
        const size_t chunk = (rows.size() + worker_count - 1) / worker_count;
        for (unsigned worker_index = 0; worker_index < worker_count;
             ++worker_index) {
            const size_t begin = static_cast<size_t>(worker_index) * chunk;
            const size_t end = std::min(rows.size(), begin + chunk);
            if (begin >= end) break;
            workers.emplace_back([&, worker_index, begin, end] {
                aggregate_rows_range(spec, rows, begin, end,
                                     locals[worker_index]);
            });
        }
        for (auto& worker : workers) worker.join();
        for (auto& local : locals) merge_agg_groups(groups, local, n_agg);
    }

    emit_agg_groups(spec, groups, step_result, group_having);
    return true;
}

// Build the int-keyed primary key bytes. Mirrors the layout produced by
// ha_lineairdb::append_key_part_encoding, so server-side read-plan scan
// boundaries match what the proxy wrote into LineairDB:
//   [0x00 not-null marker | 0x10 INT type tag | 2-byte big-endian length=4
//    | 4-byte signed int with top bit flipped]
// Flipping the top bit makes byte-wise lexicographic sort agree with signed
// integer order, so Masstree can sort without knowing the column type.
// TODO: factor this and ha_lineairdb's encoder into a shared encoder so the
// two ends cannot drift.
std::string encode_int_key_part(int64_t value) {
    const auto encoded = static_cast<uint32_t>(static_cast<int32_t>(value)) ^
                         0x80000000U;
    std::string out;
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x10));
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x04));
    out.push_back(static_cast<char>((encoded >> 24) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 16) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 8) & 0xFF));
    out.push_back(static_cast<char>(encoded & 0xFF));
    return out;
}

/**
 * @brief Decode the leading integer key part produced by encode_int_key_part.
 */
static bool decode_leading_int_key(std::string_view key, int64_t& out) {
    if (key.size() < 8) return false;
    if (static_cast<uint8_t>(key[0]) != 0x00 ||
        static_cast<uint8_t>(key[1]) != 0x10 ||
        static_cast<uint8_t>(key[2]) != 0x00 ||
        static_cast<uint8_t>(key[3]) != 0x04) {
        return false;
    }

    const uint32_t encoded =
        (static_cast<uint32_t>(static_cast<uint8_t>(key[4])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(key[5])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(key[6])) << 8) |
        static_cast<uint32_t>(static_cast<uint8_t>(key[7]));
    out = static_cast<int32_t>(encoded ^ 0x80000000U);
    return true;
}

// Parse a decimal-string column and encode it as int-keyed primary key bytes.
std::string encode_column_as_int_key(std::string_view column,
                                     int64_t int_delta) {
    std::string tmp(column);
    int64_t value = std::strtoll(tmp.c_str(), nullptr, 10);
    return encode_int_key_part(value + int_delta);
}

/**
 * @brief Scan integer primary-key slices in parallel and aggregate locally.
 *
 * @return true when group rows were emitted; false lets the caller use the
 * serial scan path.
 */
/**
 * @brief Fold the visible rows of one PAX group into a group map,
 * evaluating the filter and aggregate arguments straight on column strips
 * (no row materialization).
 *
 * Returns false when a row's filter columns are unparsable (caller falls
 * back to the materializing path, which fails the plan the same way the
 * row-store path does).
 */
static bool aggregate_pax_group(
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const LineairDB::Pax::PaxGroup& group,
    std::unordered_map<std::string, AggGroupState>& groups) {
    using AF = LineairDB::Protocol::AggFunc;
    const auto& spec = step.aggregate();
    // A HAVING carrier (num_columns == UINT32_MAX marker) is a group-level
    // predicate applied at emit time, never a per-row filter.
    const bool has_filter = !is_aggregate_having_filter(step) &&
                            step.has_filter() && step.filter().has_expr();
    const int n_agg = spec.aggs_size();
    const int n_grp = spec.group_columns_size();
    PredicateEvaluator evaluator;
    std::string keybuf;
    std::vector<std::string_view> gv(n_grp);
    for (uint32_t base = 0; base < LineairDB::Pax::PaxGroup::kRows;
         base += 64) {
        // Visibility is checked per 64-slot word so fully-empty regions
        // (e.g. the unfilled tail group) cost one load.
        uint64_t bits = 0;
        for (uint32_t s = 0; s < 64; ++s) {
            if (group.IsVisible(base + s)) bits |= (uint64_t{1} << s);
        }
        while (bits != 0) {
            const uint32_t s = static_cast<uint32_t>(__builtin_ctzll(bits));
            bits &= bits - 1;
            const uint32_t slot = base + s;
            if (has_filter) {
                if (!evaluator.set_row_from_pax(group, slot,
                                                step.filter().num_columns())) {
                    return false;
                }
                if (!evaluator.evaluate(step.filter().expr())) continue;
            }
            const PaxRowRef row{&group, slot};
            keybuf.clear();
            for (int g = 0; g < n_grp; ++g) {
                gv[g] = row_column(row, spec.group_columns(g));
                const uint32_t l = static_cast<uint32_t>(gv[g].size());
                keybuf.append(reinterpret_cast<const char*>(&l), sizeof(l));
                keybuf.append(gv[g].data(), gv[g].size());
            }
            auto it = groups.find(keybuf);
            AggGroupState* gs;
            if (it == groups.end()) {
                AggGroupState st;
                st.key_cols.resize(n_grp);
                for (int g = 0; g < n_grp; ++g)
                    st.key_cols[g] = std::string(gv[g]);
                st.count.assign(n_agg, 0);
                st.sum.assign(n_agg, Dec{});
                gs = &groups.emplace(std::move(keybuf), std::move(st))
                          .first->second;
                keybuf.clear();
            } else {
                gs = &it->second;
            }
            for (int a = 0; a < n_agg; ++a) {
                const auto& af = spec.aggs(a);
                if (af.kind() == AF::AGG_COUNT) {
                    gs->count[a] += 1;
                    continue;
                }
                Dec v = dec_eval(af.arg(), row);
                if (!v.null) {
                    dec_addsub(gs->sum[a], v, false);
                    gs->count[a] += 1;
                }
            }
        }
    }
    return true;
}

/**
 * @brief Full-table aggregate scan over PAX column strips.
 *
 * Applies to unvalidated full-range primary scans of a fully-PAX table
 * (aggregate steps emit synthetic group rows with no keys/tids, so no OCC
 * evidence is lost). Consistency: group write counters are snapshotted
 * before and re-checked after the fold; any concurrent modification falls
 * back to the row-materializing path.
 */
static bool parallel_primary_pax_aggregate_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
    if (db == nullptr) return false;
    // Full-table scans only: PAX strips carry no encoded PK to bound a range.
    // The proxy encodes "whole table" as an empty start plus the 16x0xFF
    // scan-end sentinel (lineairdb_keyenc::scan_end_sentinel).
    const bool end_is_sentinel =
        end_key.size() == 16 &&
        end_key.find_first_not_of('\xff') == std::string::npos;
    if (!start_key.empty() || !end_is_sentinel) return false;
    auto* store = db->GetPaxStore(step.table_name());
    if (store == nullptr) return false;
    // Heap-fallback rows are invisible to strips: never strip-scan then.
    if (store->overflow_count() > 0) return false;

    const size_t n_groups = store->group_count();
    std::unordered_map<std::string, AggGroupState> groups;
    if (n_groups > 0) {
        std::vector<uint64_t> wc_before(n_groups, 0);
        for (size_t g = 0; g < n_groups; ++g) {
            auto* grp = store->group(g);
            wc_before[g] =
                grp ? grp->write_counter.load(std::memory_order_acquire) : 0;
        }

        const unsigned nproc = std::thread::hardware_concurrency();
        const unsigned max_threads = std::min<unsigned>(nproc ? nproc : 4, 8);
        const unsigned worker_count = static_cast<unsigned>(
            std::min<size_t>(std::max<size_t>(n_groups / 8, 1), max_threads));
        const int n_agg = step.aggregate().aggs_size();

        std::vector<std::unordered_map<std::string, AggGroupState>> locals(
            worker_count);
        std::vector<char> failed(worker_count, 0);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);
        for (unsigned w = 0; w < worker_count; ++w) {
            workers.emplace_back([&, w] {
                const size_t begin = n_groups * w / worker_count;
                const size_t end = n_groups * (w + 1) / worker_count;
                for (size_t g = begin; g < end; ++g) {
                    auto* grp = store->group(g);
                    if (grp == nullptr) continue;
                    if (!aggregate_pax_group(step, *grp, locals[w])) {
                        failed[w] = 1;
                        return;
                    }
                }
            });
        }
        for (auto& worker : workers) worker.join();
        for (char worker_failed : failed) {
            if (worker_failed) return false;
        }
        // Re-check quiescence: any concurrent scatter/retire in the scanned
        // window poisons cell reads — fall back to the TID-checked path.
        for (size_t g = 0; g < n_groups; ++g) {
            auto* grp = store->group(g);
            const uint64_t wc_now =
                grp ? grp->write_counter.load(std::memory_order_acquire) : 0;
            if (wc_now != wc_before[g]) return false;
        }
        for (auto& local : locals) merge_agg_groups(groups, local, n_agg);
    }
    emit_agg_groups(step.aggregate(), groups, step_result,
                    is_aggregate_having_filter(step) ? &step.filter()
                                                     : nullptr);
    return true;
}

// Membership set for one hoisted semijoin reduction (built from an earlier
// step's rows); probe rows whose join-key column is absent are dropped.
struct FeSemijoin {
    std::unordered_set<std::string> keys;
    uint32_t probe_column;
};

/**
 * @brief Row-returning primary scan over PAX refs: evaluate the pushed
 * filter on column strips, gather only surviving rows (projected when the
 * step carries a RowProjection), and re-check each row's TID after its cell
 * reads — rows touched by concurrent writers are re-read through
 * StatelessRead, so validated (non-ro_novalidate) plans stay correct.
 *
 * Integer-keyed ranges without LIMIT are sliced across worker threads
 * (same slicing as parallel_primary_filter_scan), each worker running its
 * own ref-scan + strip evaluation; chunk outputs are appended in key order.
 *
 * Returns false (emitting nothing) when the table is not fully
 * PAX-resident or the schema is too narrow — caller uses the materializing
 * path. `projection_failed` mirrors the trim_row_value contract.
 */
static bool pax_ref_scan_emit(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result,
    bool& projection_failed, const std::vector<FeSemijoin>& fe_semijoins) {
    if (db == nullptr) return false;
    const bool has_filter = step.has_filter() && step.filter().has_expr();
    const bool has_projection = step.has_projection();
    // With a pushed filter, LIMIT applies after filter evaluation.
    const uint64_t ref_scan_limit = has_filter ? 0 : step.scan_limit();

    std::vector<uint32_t> kept;
    if (has_projection) {
        kept.assign(step.projection().field_indexes().begin(),
                    step.projection().field_indexes().end());
    }

    struct RefChunkOut {
        std::vector<std::string> scan_keys, scan_values, filtered_keys;
        std::vector<uint64_t> tids;
        std::atomic<bool> bail{false};
        bool not_pax = false;
        bool projection_failed = false;
    };

    // Per-chunk worker: ref-scan [s, e), evaluate on strips, gather
    // survivors, TID-recheck. `release_epoch` for extra threads only.
    auto run_range = [&](const std::string& s, const std::string& e,
                         RefChunkOut& out, bool release_epoch) {
        auto refs = db->StatelessPaxRefScan(step.table_name(), s, e,
                                            ref_scan_limit,
                                            step.reverse_scan());
        if (!refs.ok) {
            out.not_pax = true;
            if (release_epoch) db->ReleaseMasstreeThreadEpoch();
            return;
        }
        PredicateEvaluator evaluator;
        for (auto& r : refs.rows) {
            const auto* grp =
                static_cast<const LineairDB::Pax::PaxGroup*>(r.group);
            bool pass = true;
            if (has_filter) {
                if (!evaluator.set_row_from_pax(
                        *grp, r.slot, step.filter().num_columns())) {
                    out.bail.store(true, std::memory_order_relaxed);
                    break;  // schema narrower than the filter: fall back
                }
                pass = evaluator.evaluate(step.filter().expr());
            }
            // Hoisted semijoin reduction: drop rows without a join partner.
            // Distinct from `pass` — rejected rows are NOT negative-cache
            // material (membership is plan-local, not a row predicate).
            bool sj_drop = false;
            if (pass && !fe_semijoins.empty()) {
                for (const auto& fsj : fe_semijoins) {
                    const std::string_view col =
                        grp->cell(fsj.probe_column + 1, r.slot);
                    if (fsj.keys.find(std::string(col)) == fsj.keys.end()) {
                        sj_drop = true;
                        break;
                    }
                }
            }
            std::string value;
            if (pass && !sj_drop) {
                if (has_projection) {
                    if (!grp->GatherRowProjected(r.slot, kept.data(),
                                                 kept.size(), value)) {
                        out.bail.store(true, std::memory_order_relaxed);
                        break;  // projected column outside schema
                    }
                } else {
                    value.resize(r.row_size);
                    const size_t got = grp->GatherRow(
                        r.slot, reinterpret_cast<std::byte*>(value.data()),
                        r.row_size);
                    if (got != r.row_size) value.resize(got);
                }
            }
            // The cells just read are only trustworthy if no writer touched
            // the row since the scan observed its TID.
            if (LineairDB::PaxRefCurrentTid(r) != r.tid) {
                auto rr = db->StatelessRead(step.table_name(), r.key);
                if (!rr.found) continue;  // deleted meanwhile: skip
                bool spass = true;
                if (has_filter) {
                    PredicateEvaluator ev;
                    if (ev.parse_row(rr.value.data(), rr.value.size(),
                                     step.filter().num_columns())) {
                        spass = ev.evaluate(step.filter().expr());
                    }  // parse failure -> keep for MySQL to re-check
                }
                if (!spass) {
                    out.filtered_keys.push_back(std::move(r.key));
                    continue;
                }
                bool ssj_drop = false;
                for (const auto& fsj : fe_semijoins) {
                    auto col = extract_value_column(rr.value, fsj.probe_column);
                    if (fsj.keys.find(std::string(col)) == fsj.keys.end()) {
                        ssj_drop = true;
                        break;
                    }
                }
                if (ssj_drop) continue;
                std::string sout;
                if (has_projection) {
                    if (!trim_row_value(rr.value,
                                        step.projection().field_indexes(),
                                        step.projection().num_columns(),
                                        sout)) {
                        out.projection_failed = true;
                        sout = std::move(rr.value);
                    }
                } else {
                    sout = std::move(rr.value);
                }
                out.scan_keys.push_back(std::move(r.key));
                out.scan_values.push_back(std::move(sout));
                out.tids.push_back(rr.tid);
                continue;
            }
            if (!pass) {
                out.filtered_keys.push_back(std::move(r.key));
                continue;
            }
            if (sj_drop) continue;
            out.scan_keys.push_back(std::move(r.key));
            out.scan_values.push_back(std::move(value));
            out.tids.push_back(r.tid);
        }
        if (release_epoch) db->ReleaseMasstreeThreadEpoch();
    };

    // Try to slice integer-keyed, LIMIT-less forward scans across threads.
    std::vector<RefChunkOut> chunks;
    bool parallel_done = false;
    if (step.scan_limit() == 0 && !step.reverse_scan()) {
        const unsigned nproc = std::thread::hardware_concurrency();
        const unsigned max_threads = std::min<unsigned>(nproc ? nproc : 4, 8);
        auto first = db->StatelessRangeScan(step.table_name(), start_key,
                                            end_key, 1, false);
        auto last = db->StatelessRangeScan(step.table_name(), start_key,
                                           end_key, 1, true);
        int64_t lo = 0, hi = 0;
        if (max_threads > 1 && first.ok && !first.rows.empty() && last.ok &&
            !last.rows.empty() &&
            decode_leading_int_key(first.rows.front().key, lo) &&
            decode_leading_int_key(last.rows.front().key, hi) && hi > lo &&
            static_cast<uint64_t>(hi - lo) + 1 >= 500000) {
            const uint64_t span = static_cast<uint64_t>(hi - lo) + 1;
            const unsigned worker_count = static_cast<unsigned>(
                std::min<size_t>((span + 127999) / 128000, max_threads));
            if (worker_count > 1) {
                std::vector<std::string> starts(worker_count);
                std::vector<std::string> ends(worker_count);
                for (unsigned i = 0; i < worker_count; ++i) {
                    const int64_t begin_value =
                        lo + static_cast<int64_t>((span * i) / worker_count);
                    const int64_t end_value =
                        (i + 1 == worker_count)
                            ? hi + 1
                            : lo + static_cast<int64_t>((span * (i + 1)) /
                                                        worker_count);
                    starts[i] = (i == 0) ? start_key
                                         : encode_int_key_part(begin_value);
                    ends[i] = (i + 1 == worker_count)
                                  ? end_key
                                  : encode_int_key_part(end_value);
                }
                chunks = std::vector<RefChunkOut>(worker_count);
                std::vector<std::thread> workers;
                workers.reserve(worker_count);
                for (unsigned w = 0; w < worker_count; ++w) {
                    workers.emplace_back([&, w] {
                        run_range(starts[w], ends[w], chunks[w], true);
                    });
                }
                for (auto& worker : workers) worker.join();
                parallel_done = true;
            }
        }
    }
    if (!parallel_done) {
        chunks = std::vector<RefChunkOut>(1);
        run_range(start_key, end_key, chunks[0], false);
    }
    for (auto& c : chunks) {
        if (c.not_pax || c.bail.load(std::memory_order_relaxed)) return false;
        if (c.projection_failed) projection_failed = true;
    }

    uint64_t emitted = 0;
    for (auto& c : chunks) {
        for (auto& fk : c.filtered_keys) {
            step_result->add_filtered_keys(std::move(fk));
        }
        for (size_t i = 0; i < c.scan_keys.size(); ++i) {
            step_result->add_scan_keys(std::move(c.scan_keys[i]));
            step_result->add_scan_values(std::move(c.scan_values[i]));
            step_result->add_scan_tids(c.tids[i]);
            if (step.scan_limit() > 0 && ++emitted >= step.scan_limit()) {
                return true;
            }
        }
    }
    return true;
}

static bool parallel_primary_aggregate_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
    if (db == nullptr) return false;

    const size_t morsel_rows = 128000;  // FIXME: make configurable
    const size_t min_rows = 500000;     // FIXME: make configurable
    const unsigned nproc = std::thread::hardware_concurrency();
    const unsigned max_threads =
        std::min<unsigned>(nproc ? nproc : 4, 8);  // FIXME: make configurable
    if (max_threads <= 1) return false;

    // Probe the actual key span before creating split boundaries.
    auto first = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                        1, false);
    if (!first.ok || first.rows.empty()) return false;
    auto last = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                       1, true);
    if (!last.ok || last.rows.empty()) return false;

    int64_t lo = 0;
    int64_t hi = 0;
    if (!decode_leading_int_key(first.rows.front().key, lo) ||
        !decode_leading_int_key(last.rows.front().key, hi)) {
        return false;
    }
    if (hi <= lo) return false;

    const uint64_t span = static_cast<uint64_t>(hi - lo) + 1;
    if (span < min_rows) return false;
    const size_t desired_workers =
        morsel_rows > 0 ? (span + morsel_rows - 1) / morsel_rows : 1;
    const unsigned worker_count =
        static_cast<unsigned>(std::min<size_t>(desired_workers, max_threads));
    if (worker_count <= 1) return false;

    // Interior bounds are integer key prefixes; each slice is [start, end).
    std::vector<std::string> starts(worker_count);
    std::vector<std::string> ends(worker_count);
    for (unsigned i = 0; i < worker_count; ++i) {
        const int64_t begin_value =
            lo + static_cast<int64_t>((span * i) / worker_count);
        const int64_t end_value =
            (i + 1 == worker_count)
                ? hi + 1
                : lo + static_cast<int64_t>((span * (i + 1)) / worker_count);
        starts[i] = (i == 0) ? start_key : encode_int_key_part(begin_value);
        ends[i] = (i + 1 == worker_count) ? end_key
                                          : encode_int_key_part(end_value);
    }

    const bool group_having = is_aggregate_having_filter(step);
    const bool has_filter =
        step.has_filter() && step.filter().has_expr() && !group_having;
    const auto& spec = step.aggregate();
    const int n_agg = spec.aggs_size();
    std::vector<std::unordered_map<std::string, AggGroupState>> locals(
        worker_count);
    std::vector<char> failed(worker_count, 0);
    std::vector<std::thread> workers;
    workers.reserve(worker_count);

    for (unsigned worker_index = 0; worker_index < worker_count;
         ++worker_index) {
        workers.emplace_back([&, worker_index] {
            auto scan_result =
                db->StatelessRangeScan(step.table_name(), starts[worker_index],
                                       ends[worker_index], 0, false);
            if (!scan_result.ok) {
                failed[worker_index] = 1;
                db->ReleaseMasstreeThreadEpoch();
                return;
            }

            // Aggregate filters cannot be rechecked after rows are grouped.
            if (has_filter && !scan_result.rows.empty()) {
                PredicateEvaluator evaluator;
                std::vector<LineairDB::StatelessScanRow> kept;
                kept.reserve(scan_result.rows.size());
                for (auto& row : scan_result.rows) {
                    if (!evaluator.parse_row(row.value.data(),
                                             row.value.size(),
                                             step.filter().num_columns())) {
                        failed[worker_index] = 1;
                        db->ReleaseMasstreeThreadEpoch();
                        return;
                    }
                    if (evaluator.evaluate(step.filter().expr())) {
                        kept.push_back(std::move(row));
                    }
                }
                scan_result.rows = std::move(kept);
            }

            aggregate_rows_range(spec, scan_result.rows, 0,
                                 scan_result.rows.size(),
                                 locals[worker_index]);
            db->ReleaseMasstreeThreadEpoch();
        });
    }
    for (auto& worker : workers) worker.join();
    for (char worker_failed : failed) {
        if (worker_failed) return false;
    }

    std::unordered_map<std::string, AggGroupState> groups;
    for (auto& local : locals) merge_agg_groups(groups, local, n_agg);
    emit_agg_groups(spec, groups, step_result,
                    group_having ? &step.filter() : nullptr);
    return true;
}

/**
 * @brief Scan integer primary-key slices in parallel for filtered base rows.
 *
 * @return true when rows were emitted; false lets the caller use the serial
 * scan path.
 */
static bool parallel_primary_filter_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
    if (db == nullptr) return false;

    const size_t morsel_rows = 128000;  // FIXME: make configurable
    const size_t min_rows = 500000;     // FIXME: make configurable
    const unsigned nproc = std::thread::hardware_concurrency();
    const unsigned max_threads =
        std::min<unsigned>(nproc ? nproc : 4, 8);  // FIXME: make configurable
    if (max_threads <= 1) return false;

    // Probe the actual key span before creating split boundaries.
    auto first = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                        1, false);
    if (!first.ok || first.rows.empty()) return false;
    auto last = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                       1, true);
    if (!last.ok || last.rows.empty()) return false;

    int64_t lo = 0;
    int64_t hi = 0;
    if (!decode_leading_int_key(first.rows.front().key, lo) ||
        !decode_leading_int_key(last.rows.front().key, hi)) {
        return false;
    }
    if (hi <= lo) return false;

    const uint64_t span = static_cast<uint64_t>(hi - lo) + 1;
    if (span < min_rows) return false;
    const size_t desired_workers =
        morsel_rows > 0 ? (span + morsel_rows - 1) / morsel_rows : 1;
    const unsigned worker_count =
        static_cast<unsigned>(std::min<size_t>(desired_workers, max_threads));
    if (worker_count <= 1) return false;

    // Interior bounds are integer key prefixes; each slice is [start, end).
    std::vector<std::string> starts(worker_count);
    std::vector<std::string> ends(worker_count);
    for (unsigned i = 0; i < worker_count; ++i) {
        const int64_t begin_value =
            lo + static_cast<int64_t>((span * i) / worker_count);
        const int64_t end_value =
            (i + 1 == worker_count)
                ? hi + 1
                : lo + static_cast<int64_t>((span * (i + 1)) / worker_count);
        starts[i] = (i == 0) ? start_key : encode_int_key_part(begin_value);
        ends[i] = (i + 1 == worker_count) ? end_key
                                          : encode_int_key_part(end_value);
    }

    struct WorkerOut {
        std::vector<std::string> keys;
        std::vector<std::string> values;
        std::vector<std::string> filtered_keys;
        std::vector<uint64_t> tids;
    };
    std::vector<WorkerOut> outputs(worker_count);
    std::vector<char> failed(worker_count, 0);
    const bool has_projection = step.has_projection();
    std::vector<std::thread> workers;
    workers.reserve(worker_count);

    for (unsigned worker_index = 0; worker_index < worker_count;
         ++worker_index) {
        workers.emplace_back([&, worker_index] {
            auto scan_result =
                db->StatelessRangeScan(step.table_name(),
                                       starts[worker_index],
                                       ends[worker_index], 0, false);
            if (!scan_result.ok) {
                failed[worker_index] = 1;
                db->ReleaseMasstreeThreadEpoch();
                return;
            }

            PredicateEvaluator evaluator;
            WorkerOut& out = outputs[worker_index];
            out.keys.reserve(scan_result.rows.size());
            for (auto& row : scan_result.rows) {
                bool pass = true;
                if (evaluator.parse_row(row.value.data(), row.value.size(),
                                        step.filter().num_columns())) {
                    pass = evaluator.evaluate(step.filter().expr());
                }
                // Unparseable rows are shipped so MySQL can re-check them.
                if (!pass) {
                    out.filtered_keys.push_back(std::move(row.key));
                    continue;
                }

                if (has_projection && !row.value.empty()) {
                    std::string trimmed;
                    if (!trim_row_value(row.value,
                                        step.projection().field_indexes(),
                                        step.projection().num_columns(),
                                        trimmed)) {
                        failed[worker_index] = 1;
                        break;
                    }
                    row.value = std::move(trimmed);
                }
                out.keys.push_back(std::move(row.key));
                out.values.push_back(std::move(row.value));
                out.tids.push_back(row.tid);
            }
            db->ReleaseMasstreeThreadEpoch();
        });
    }
    for (auto& worker : workers) worker.join();
    for (char worker_failed : failed) {
        if (worker_failed) return false;
    }

    // Append worker chunks in range order to match the serial scan output.
    for (WorkerOut& out : outputs) {
        for (size_t i = 0; i < out.keys.size(); ++i) {
            step_result->add_scan_keys(std::move(out.keys[i]));
            step_result->add_scan_values(std::move(out.values[i]));
            step_result->add_scan_tids(out.tids[i]);
        }
        for (auto& filtered_key : out.filtered_keys) {
            step_result->add_filtered_keys(std::move(filtered_key));
        }
    }
    return true;
}

// Pick the source byte string for a binding (from a step's scan key, scan value, or value).
const std::string* select_source_bytes(
    const LineairDB::Protocol::TxExecuteReadPlan::StepResult& source,
    const LineairDB::Protocol::TxExecuteReadPlan::KeyBinding& binding,
    bool from_key, int row_override) {
    if (from_key) {
        if (source.scan_keys_size() == 0) return nullptr;
        int row = row_override >= 0 ? row_override : binding.source_row();
        if (binding.use_midpoint()) row = (source.scan_keys_size() - 1) / 2;
        row = std::min(row, source.scan_keys_size() - 1);
        return &source.scan_keys(row);
    }

    if (source.scan_values_size() > 0) {
        int row = row_override >= 0 ? row_override : binding.source_row();
        if (binding.use_midpoint()) row = (source.scan_values_size() - 1) / 2;
        row = std::min(row, source.scan_values_size() - 1);
        return &source.scan_values(row);
    }
    return &source.value();
}

// Compose a read-plan key prefix plus all bindings into the actual scan key.
std::string build_plan_key(
    const std::string& prefix,
    const google::protobuf::RepeatedPtrField<
        LineairDB::Protocol::TxExecuteReadPlan::KeyBinding>& bindings,
    const std::vector<LineairDB::Protocol::TxExecuteReadPlan::StepResult*>&
        previous_results,
    int row_override = -1,
    bool *complete = nullptr) {
    if (complete != nullptr) *complete = true;
    std::string key = prefix;
    for (const auto& binding : bindings) {
        const int source_step = static_cast<int>(binding.source_step());
        if (source_step < 0 ||
            source_step >= static_cast<int>(previous_results.size())) {
            if (complete != nullptr) *complete = false;
            continue;
        }

        const auto& source = *previous_results[source_step];
        std::string scratch;
        std::string_view extracted;
        if (binding.source_column() > 0) {
            const std::string* bytes =
                select_source_bytes(source, binding, false, row_override);
            if (bytes != nullptr) {
                extracted =
                    extract_value_column(*bytes, binding.source_column() - 1);
                if (binding.column_as_int_key()) {
                    scratch =
                        encode_column_as_int_key(extracted,
                                                 binding.int_delta());
                    extracted = scratch;
                }
            } else if (complete != nullptr) {
                *complete = false;
            }
        } else {
            const std::string* bytes =
                select_source_bytes(source, binding, binding.from_key(),
                                    row_override);
            if (bytes != nullptr) {
                uint32_t offset = binding.source_offset();
                uint32_t length = binding.source_length();
                if (offset < bytes->size()) {
                    if (length == 0) length = bytes->size() - offset;
                    length = std::min<uint32_t>(
                        length, static_cast<uint32_t>(bytes->size() - offset));
                    extracted = std::string_view(bytes->data() + offset,
                                                 length);
                } else if (complete != nullptr) {
                    *complete = false;
                }
            } else if (complete != nullptr) {
                *complete = false;
            }
        }
        if (bindings.size() > 0 && extracted.empty() && complete != nullptr) {
            *complete = false;
        }
        key.append(extracted.data(), extracted.size());
    }
    return key;
}

}  // namespace

std::mutex LineairDBRpc::ndv_cache_mu_;
std::unordered_map<std::string, std::pair<bool, std::vector<uint64_t>>>
    LineairDBRpc::ndv_cache_;
std::unordered_map<std::string, LineairDBRpc::HistEntry>
    LineairDBRpc::hist_cache_;

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager,
                           std::shared_ptr<TableRowCounts> row_counts)
    : db_manager_(db_manager), tx_manager_(tx_manager), row_counts_(row_counts) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type,
                             const std::string& message, std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));

    // Close this thread's masstree RCU critical section after each
    // self-contained handler (stateless RPCs and DB_END_TRANSACTION) so
    // RCU can reclaim retired leaves; the next masstree op re-opens it.
    auto release_masstree_thread_epoch = [this]() {
        if (db_manager_) {
            auto db = db_manager_->get_database();
            if (db) db->ReleaseMasstreeThreadEpoch();
        }
    };
    switch(message_type) {
        // Transaction lifecycle
        case MessageType::TX_BEGIN_TRANSACTION:
            handleTxBeginTransaction(message, result);
            return;
        case MessageType::TX_ABORT:
            handleTxAbort(message, result);
            return;

        // Primary key operations
        case MessageType::TX_READ:
            handleTxRead(message, result);
            return;
        case MessageType::TX_BATCH_READ:
            handleTxBatchRead(message, result);
            return;
        case MessageType::TX_BATCH_WRITE:
            handleTxBatchWrite(message, result);
            return;
        case MessageType::TX_STATELESS_READ:
            handleTxStatelessRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_BATCH_READ:
            handleTxStatelessBatchRead(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_EXECUTE_READ_PLAN:
            handleTxExecuteReadPlan(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_GET_TABLE_STATS:
            handleTxGetTableStats(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_VALIDATE_AND_COMMIT:
            handleTxValidateAndCommit(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_WRITE:
            handleTxWrite(message, result);
            return;
        case MessageType::TX_DELETE:
            handleTxDelete(message, result);
            return;

        // Secondary index operations
        case MessageType::TX_READ_SECONDARY_INDEX:
            handleTxReadSecondaryIndex(message, result);
            return;
        case MessageType::TX_WRITE_SECONDARY_INDEX:
            handleTxWriteSecondaryIndex(message, result);
            return;
        case MessageType::TX_DELETE_SECONDARY_INDEX:
            handleTxDeleteSecondaryIndex(message, result);
            return;
        case MessageType::TX_UPDATE_SECONDARY_INDEX:
            handleTxUpdateSecondaryIndex(message, result);
            return;

        // Primary key scan operations
        case MessageType::TX_GET_MATCHING_KEYS_IN_RANGE:
            handleTxGetMatchingKeysInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE:
            handleTxGetMatchingKeysAndValuesInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX:
            handleTxGetMatchingKeysAndValuesFromPrefix(message, result);
            return;
        case MessageType::TX_FETCH_LAST_KEY_IN_RANGE:
            handleTxFetchLastKeyInRange(message, result);
            return;
        case MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX:
            handleTxFetchFirstKeyWithPrefix(message, result);
            return;
        case MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX:
            handleTxFetchNextKeyWithPrefix(message, result);
            return;

        // Secondary index scan operations
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE:
            handleTxGetMatchingPrimaryKeysInRange(message, result);
            return;
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX:
            handleTxGetMatchingPrimaryKeysFromPrefix(message, result);
            return;
        case MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE:
            handleTxFetchLastPrimaryKeyInSecondaryRange(message, result);
            return;
        case MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE:
            handleTxFetchLastSecondaryEntryInRange(message, result);
            return;

        // Database operations
        case MessageType::DB_FENCE:
            handleDbFence(message, result);
            return;
        case MessageType::DB_END_TRANSACTION:
            handleDbEndTransaction(message, result);
            release_masstree_thread_epoch();
            return;
        case MessageType::DB_CREATE_TABLE:
            handleDbCreateTable(message, result);
            return;
        case MessageType::DB_SET_TABLE:
            handleDbSetTable(message, result);
            return;
        case MessageType::DB_CREATE_SECONDARY_INDEX:
            handleDbCreateSecondaryIndex(message, result);
            return;

        default:
            LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
            return;
    }
}

bool LineairDBRpc::key_prefix_is_matching(const std::string& key_prefix, const std::string& key) {
    if (key.substr(0, key_prefix.size()) != key_prefix) return false;
    return true;
}

void LineairDBRpc::handleTxBeginTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxBeginTransaction");

    LineairDB::Protocol::TxBeginTransaction::Request request;
    LineairDB::Protocol::TxBeginTransaction::Response response;

    request.ParseFromString(message);

    auto& tx = db_manager_->get_database()->BeginTransaction();
    int64_t tx_id = tx_manager_->generate_tx_id();
    tx_manager_->store_transaction(tx_id, &tx);

    response.set_transaction_id(tx_id);

    // Piggyback current table row counts so proxy has fresh stats.
    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();

    LOG_DEBUG("Created transaction: %ld", tx_id);
}

void LineairDBRpc::handleTxGetTableStats(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::GetTableStats::Request request;
    request.ParseFromString(message);
    LineairDB::Protocol::GetTableStats::Response response;

    if (row_counts_) {
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    }

    if (!request.ndv_table().empty() && db_manager_) {
        auto db = db_manager_->get_database();
        for (const auto& desc : request.ndv_indexes()) {
            auto* out = response.add_index_ndv();
            out->set_index_name(desc.index_name());

            std::string cache_key = request.ndv_table();
            cache_key.push_back('\0');
            cache_key.append(desc.index_name());
            cache_key.push_back('\0');
            cache_key.append(std::to_string(desc.num_key_parts()));

            bool cached = false;
            bool available = false;
            std::vector<uint64_t> ndv;
            {
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                if (request.ndv_force_recompute()) ndv_cache_.erase(cache_key);
                auto it = ndv_cache_.find(cache_key);
                if (it != ndv_cache_.end()) {
                    cached = true;
                    available = it->second.first;
                    ndv = it->second.second;
                }
            }

            if (!cached) {
                available = db && db->ComputeIndexNdvInt(
                                      request.ndv_table(), desc.index_name(),
                                      desc.num_key_parts(), ndv);
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                ndv_cache_[cache_key] = {available, ndv};
            }

            out->set_available(available);
            if (available) {
                for (uint64_t value : ndv) out->add_ndv(value);
            }

            constexpr uint32_t kHistogramBuckets = 64;
            HistEntry hist;
            bool hist_cached = false;
            {
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                if (request.ndv_force_recompute()) hist_cache_.erase(cache_key);
                auto it = hist_cache_.find(cache_key);
                if (it != hist_cache_.end()) {
                    hist_cached = true;
                    hist = it->second;
                }
            }

            if (!hist_cached) {
                hist.available = db && db->ComputeIndexHistogram(
                                            request.ndv_table(),
                                            desc.index_name(),
                                            kHistogramBuckets,
                                            hist.bounds,
                                            hist.cum);
                if (!hist.available) {
                    hist.bounds.clear();
                    hist.cum.clear();
                }
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                hist_cache_[cache_key] = hist;
            }

            out->set_hist_available(hist.available);
            if (hist.available) {
                for (const auto& bound : hist.bounds)
                    out->add_hist_bounds(bound);
                for (uint64_t value : hist.cum)
                    out->add_hist_cum(value);
            }
        }
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxAbort(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxAbort");

    LineairDB::Protocol::TxAbort::Request request;
    LineairDB::Protocol::TxAbort::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        tx->Abort();
    } else {
        LOG_WARNING("Transaction not found for abort: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxRead(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxRead");

    LineairDB::Protocol::TxRead::Request request;
    LineairDB::Protocol::TxRead::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto read_result = tx->Read(request.key());
        response.set_is_aborted(tx->IsAborted());

        if (read_result.first != nullptr) {
            response.set_found(true);
            std::string value(reinterpret_cast<const char*>(read_result.first), read_result.second);
            response.set_value(value);
        } else {
            response.set_found(false);
        }

        LOG_DEBUG("Read key '%s' from transaction %ld: %s", request.key().c_str(), tx_id, (read_result.first != nullptr ? "found" : "not found"));
    } else {
        response.set_found(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxBatchRead(const std::string& message, std::string& result) {
    LineairDB::Protocol::TxBatchRead::Request request;
    LineairDB::Protocol::TxBatchRead::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        for (int i = 0; i < request.keys_size(); i++) {
            auto* read_result = response.add_results();
            auto pair = tx->Read(request.keys(i));
            if (pair.first != nullptr) {
                read_result->set_found(true);
                read_result->set_value(
                    reinterpret_cast<const char*>(pair.first), pair.second);
            } else {
                read_result->set_found(false);
            }
        }
        response.set_is_aborted(tx->IsAborted());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for batch_read: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxBatchWrite(const std::string& message, std::string& result) {
    LineairDB::Protocol::TxBatchWrite::Request request;
    LineairDB::Protocol::TxBatchWrite::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }

        if (!tx->IsAborted()) {
            for (int i = 0; i < request.ops_size(); i++) {
                const auto& op = request.ops(i);
                const std::string& op_table =
                    op.table_name().empty() ? request.table_name() : op.table_name();
                if (!op_table.empty()) {
                    tx->SetTable(op_table);
                }
                switch (op.type()) {
                    case LineairDB::Protocol::BATCH_OP_WRITE: {
                        const std::string& value_str = op.value();
                        tx->Write(op.key(),
                                  reinterpret_cast<const std::byte*>(value_str.c_str()),
                                  value_str.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_DELETE:
                        tx->Delete(op.key());
                        break;
                    case LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_WRITE: {
                        const std::string& pk = op.primary_key();
                        tx->WriteSecondaryIndex(
                            op.index_name(), op.secondary_key(),
                            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_SECONDARY_INDEX_DELETE: {
                        const std::string& pk = op.primary_key();
                        tx->DeleteSecondaryIndex(
                            op.index_name(), op.secondary_key(),
                            reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
                        break;
                    }
                    case LineairDB::Protocol::BATCH_OP_UNKNOWN:
                    default:
                        break;
                }
                if (tx->IsAborted()) break;
            }
        }

        response.set_success(!tx->IsAborted());
        response.set_is_aborted(tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for batch_write: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessRead(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::TxStatelessRead::Request request;
    LineairDB::Protocol::TxStatelessRead::Response response;

    request.ParseFromString(message);

    auto read_result =
        db_manager_->get_database()->StatelessRead(request.table_name(),
                                                   request.key());
    response.set_found(read_result.found);
    response.set_tid(read_result.tid);
    if (read_result.found) {
        response.set_value(std::move(read_result.value));
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxStatelessBatchRead(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::TxStatelessBatchRead::Request request;
    LineairDB::Protocol::TxStatelessBatchRead::Response response;

    request.ParseFromString(message);

    std::vector<std::pair<std::string, std::string>> keys;
    keys.reserve(request.ops_size());
    for (const auto& op : request.ops()) {
        keys.emplace_back(op.table_name(), op.key());
    }

    auto read_results = db_manager_->get_database()->StatelessBatchRead(keys);
    for (auto& read_result : read_results) {
        auto* out = response.add_results();
        out->set_found(read_result.found);
        out->set_tid(read_result.tid);
        if (read_result.found) {
            out->set_value(std::move(read_result.value));
        }
    }

    result = response.SerializeAsString();
}

namespace flat_plan {
// Native-endian bytes spell "LDBFLATP" (LineairDB flat payload).
static constexpr uint64_t kMagic = 0x5054414C4642444Cull;
static constexpr uint8_t kVersion = 2;

template <class Sink>
void w_u8(Sink& out, uint8_t v) {
    const char c = static_cast<char>(v);
    out.append(&c, 1);
}

template <class Sink>
void w_u64(Sink& out, uint64_t v) {
    char bytes[8];
    std::memcpy(bytes, &v, 8);
    out.append(bytes, 8);
}

template <class Sink>
void w_bytes(Sink& out, const std::string& s) {
    w_u64(out, static_cast<uint64_t>(s.size()));
    out.append(s.data(), s.size());
}

struct CountSink {
    uint64_t n = 0;
    void append(const char*, size_t k) { n += k; }
};

template <class Sink>
void encode_step(
    const LineairDB::Protocol::TxExecuteReadPlan::StepResult& s, Sink& out) {
    w_u8(out, s.found() ? 1 : 0);
    w_u64(out, s.tid());
    w_bytes(out, s.value());
    w_bytes(out, s.actual_key());
    w_bytes(out, s.actual_start_key());
    w_bytes(out, s.actual_end_key());
    w_u64(out, static_cast<uint64_t>(s.scan_keys_size()));
    for (const auto& k : s.scan_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.scan_values_size()));
    for (const auto& v : s.scan_values()) w_bytes(out, v);
    w_u64(out, static_cast<uint64_t>(s.scan_tids_size()));
    for (const auto t : s.scan_tids()) w_u64(out, t);
    w_u64(out, static_cast<uint64_t>(s.secondary_keys_size()));
    for (const auto& k : s.secondary_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.group_sizes_size()));
    for (const auto g : s.group_sizes()) w_u64(out, g);
    w_u64(out, static_cast<uint64_t>(s.group_start_keys_size()));
    for (const auto& k : s.group_start_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.group_end_keys_size()));
    for (const auto& k : s.group_end_keys()) w_bytes(out, k);
    w_u64(out, static_cast<uint64_t>(s.filtered_keys_size()));
    for (const auto& k : s.filtered_keys()) w_bytes(out, k);
}

// Destructive: release each StepResult after encoding it so large read-plan
// responses do not keep both protobuf rows and the flat payload alive.
void encode_to_string(LineairDB::Protocol::TxExecuteReadPlan::Response& r,
                      std::string& out) {
    CountSink count;
    count.n = 8 + 1 + 1 + 8;  // magic + version + ok + result count
    for (const auto& s : r.results()) encode_step(s, count);

    out.clear();
    out.reserve(count.n);
    w_u64(out, kMagic);
    w_u8(out, kVersion);
    w_u8(out, r.ok() ? 1 : 0);
    w_u64(out, static_cast<uint64_t>(r.results_size()));
    for (int i = 0; i < r.results_size(); ++i) {
        encode_step(r.results(i), out);
        r.mutable_results(i)->Clear();
    }
}
}  // namespace flat_plan

void LineairDBRpc::handleTxExecuteReadPlan(const std::string& message,
                                           std::string& result) {
    LineairDB::Protocol::TxExecuteReadPlan::Request request;
    LineairDB::Protocol::TxExecuteReadPlan::Response response;
    request.ParseFromString(message);
    response.set_ok(true);

    std::vector<LineairDB::Protocol::TxExecuteReadPlan::StepResult*>
        previous_results;
    previous_results.reserve(request.steps_size());

    for (const auto& step : request.steps()) {
        auto* step_result = response.add_results();
        previous_results.push_back(step_result);

        bool start_complete = true;
        bool end_complete = true;
        const std::string start_key =
            build_plan_key(step.key_prefix(), step.bindings(),
                           previous_results, -1, &start_complete);
        std::string end_key =
            build_plan_key(step.end_key_prefix(), step.end_bindings(),
                           previous_results, -1, &end_complete);
        if (!start_complete || !end_complete) continue;
        if (step.is_scan() && end_key.empty()) {
            end_key = next_lexicographic_key(start_key);
        }

        // Step-level row filter: parseable non-matches are dropped; rows the
        // evaluator cannot parse are returned for MySQL to re-check.
        const bool step_has_group_having = is_aggregate_having_filter(step);
        const bool step_has_filter =
            step.has_filter() && step.filter().has_expr() &&
            !step_has_group_having;
        const auto* step_filter =
            step_has_filter ? &step.filter().expr() : nullptr;
        const uint32_t step_filter_cols =
            step_has_filter ? step.filter().num_columns() : 0;
        PredicateEvaluator step_eval;
        auto row_passes = [&](const std::string& value) {
            if (step_filter == nullptr) return true;
            if (!step_eval.parse_row(value.data(), value.size(),
                                     step_filter_cols)) {
                return true;
            }
            return step_eval.evaluate(*step_filter);
        };

        // PAX sparse materialization: when a projection bounds this step's
        // read set, scans need only these columns' strips; other fields come
        // back as placeholders (heap rows are unaffected).
        const std::vector<uint32_t> sparse_cols = build_sparse_columns(step);
        const std::vector<uint32_t>* sparse =
            sparse_cols.empty() ? nullptr : &sparse_cols;

        // Filters read the full row. Projection then trims emitted VALUES to
        // the kept columns; malformed rows fail the plan instead of mixing
        // full and projected layouts.
        const bool step_has_projection = step.has_projection();
        bool projection_failed = false;
        auto project_value = [&](std::string&& v) -> std::string {
            if (!step_has_projection || v.empty()) return std::move(v);
            std::string out;
            if (trim_row_value(v, step.projection().field_indexes(),
                               step.projection().num_columns(), out)) {
                return out;
            }
            projection_failed = true;
            return std::move(v);
        };

        std::vector<FeSemijoin> fe_semijoins;
        {
            const int this_step_idx =
                static_cast<int>(previous_results.size()) - 1;
            for (const auto& sj : step.semijoins()) {
                const int ss = static_cast<int>(sj.source_step());
                if (ss < 0 || ss >= this_step_idx) continue;
                FeSemijoin fsj;
                fsj.probe_column = sj.probe_column();
                const bool sf_on =
                    sj.has_source_filter() && sj.source_filter().has_expr();
                const uint32_t sf_cols =
                    sf_on ? sj.source_filter().num_columns() : 0;
                for (const auto& v : previous_results[ss]->scan_values()) {
                    if (v.empty()) continue;
                    if (sf_on) {
                        PredicateEvaluator se;
                        if (se.parse_row(v.data(), v.size(), sf_cols) &&
                            !se.evaluate(sj.source_filter().expr()))
                            continue;
                    }
                    auto col = extract_value_column(v, sj.source_column());
                    if (!col.empty()) fsj.keys.emplace(col);
                }
                fe_semijoins.push_back(std::move(fsj));
            }
        }
        auto sj_reject = [&](const std::string& value) -> bool {
            for (const auto& fsj : fe_semijoins) {
                auto col = extract_value_column(value, fsj.probe_column);
                if (fsj.keys.find(std::string(col)) == fsj.keys.end())
                    return true;
            }
            return false;
        };

        if (step.for_each()) {
            // Anti-join probe: it only needs a match to exist. Stop after the
            // first surviving row. No scan_limit -- index_next reports a clean
            // EOF (see PlanStep.existence_only).
            const bool existence_only = step.existence_only();
            int source_step = -1;
            if (step.bindings_size() > 0) {
                source_step = static_cast<int>(step.bindings(0).source_step());
            }
            if (source_step < 0 ||
                source_step >= static_cast<int>(previous_results.size()) - 1) {
                continue;
            }

            const auto* source = previous_results[source_step];
            const int row_count =
                std::max(source->scan_keys_size(), source->scan_values_size());
            // Dedup probes: many source rows share a join key, and the proxy
            // serves every runtime probe of one key from the single staged
            // result, so re-executing the probe only inflates the response.
            std::vector<std::string> probe_keys;
            probe_keys.reserve(static_cast<size_t>(row_count));
            {
                std::unordered_set<std::string> seen_probe_keys;
                seen_probe_keys.reserve(static_cast<size_t>(row_count));
                for (int row = 0; row < row_count; ++row) {
                    bool row_complete = true;
                    const std::string row_key =
                        build_plan_key(step.key_prefix(), step.bindings(),
                                       previous_results, row, &row_complete);
                    if (!row_complete) continue;
                    if (!seen_probe_keys.insert(row_key).second) continue;
                    probe_keys.push_back(row_key);
                }
            }

            const size_t min_parallel_probes = 4096;  // FIXME: make configurable
            const unsigned nproc = std::thread::hardware_concurrency();
            const unsigned max_probe_threads =
                std::min<unsigned>(nproc ? nproc : 4, 8);  // FIXME: make configurable
            if (probe_keys.size() >= min_parallel_probes &&
                max_probe_threads > 1) {
                const size_t probe_count = probe_keys.size();
                const unsigned worker_count = static_cast<unsigned>(
                    std::min<size_t>(max_probe_threads, probe_count));
                struct ProbeOut {
                    std::vector<std::string> keys;
                    std::vector<std::string> values;
                    std::vector<std::string> secondary_keys;
                    std::vector<uint64_t> tids;
                    std::vector<uint32_t> group_rows;
                };
                std::vector<ProbeOut> outputs(worker_count);
                std::vector<char> failed(worker_count, 0);
                auto* db = db_manager_->get_database().get();
                const bool scan_probe = step.is_scan();
                const bool has_projection = step.has_projection();
                std::vector<std::thread> workers;
                workers.reserve(worker_count);

                // Workers execute disjoint probe slices into local buffers.
                for (unsigned worker_index = 0; worker_index < worker_count;
                     ++worker_index) {
                    workers.emplace_back([&, worker_index] {
                        const size_t begin =
                            probe_count * worker_index / worker_count;
                        const size_t end =
                            probe_count * (worker_index + 1) / worker_count;
                        PredicateEvaluator evaluator;
                        auto worker_row_passes = [&](const std::string& value) {
                            if (step_filter == nullptr) return true;
                            if (!evaluator.parse_row(value.data(),
                                                     value.size(),
                                                     step_filter_cols)) {
                                return true;
                            }
                            return evaluator.evaluate(*step_filter);
                        };
                        auto worker_project = [&](std::string&& value) {
                            if (!has_projection || value.empty())
                                return std::move(value);
                            std::string trimmed;
                            if (trim_row_value(
                                    value, step.projection().field_indexes(),
                                    step.projection().num_columns(),
                                    trimmed)) {
                                return trimmed;
                            }
                            failed[worker_index] = 1;
                            return std::move(value);
                        };

                        ProbeOut& out = outputs[worker_index];
                        for (size_t probe_index = begin;
                             probe_index < end && !failed[worker_index];
                             ++probe_index) {
                            const std::string& row_key =
                                probe_keys[probe_index];
                            if (scan_probe) {
                                const std::string row_end =
                                    next_lexicographic_key(row_key);
                                uint32_t group_rows = 0;
                                if (step.index_name().empty()) {
                                    auto scan_result =
                                        db->StatelessRangeScan(
                                            step.table_name(), row_key,
                                            row_end, step.scan_limit(),
                                            step.reverse_scan(), sparse);
                                    if (!scan_result.ok) {
                                        failed[worker_index] = 1;
                                        break;
                                    }
                                    for (auto& r : scan_result.rows) {
                                        if (!worker_row_passes(r.value))
                                            continue;
                                        if (!fe_semijoins.empty() &&
                                            sj_reject(r.value))
                                            continue;
                                        out.keys.push_back(std::move(r.key));
                                        out.values.push_back(worker_project(
                                            std::move(r.value)));
                                        out.tids.push_back(r.tid);
                                        ++group_rows;
                                        if (existence_only) break;
                                    }
                                } else {
                                    auto scan_result =
                                        db->StatelessSecondaryRangeScan(
                                            step.table_name(),
                                            step.index_name(), row_key,
                                            row_end, step.scan_limit(),
                                            step.reverse_scan(), sparse);
                                    if (!scan_result.ok) {
                                        failed[worker_index] = 1;
                                        break;
                                    }
                                    for (auto& r : scan_result.rows) {
                                        if (!worker_row_passes(r.value))
                                            continue;
                                        if (!fe_semijoins.empty() &&
                                            sj_reject(r.value))
                                            continue;
                                        out.secondary_keys.push_back(
                                            std::move(r.secondary_key));
                                        out.keys.push_back(
                                            std::move(r.primary_key));
                                        out.values.push_back(worker_project(
                                            std::move(r.value)));
                                        out.tids.push_back(r.tid);
                                        ++group_rows;
                                        if (existence_only) break;
                                    }
                                }
                                out.group_rows.push_back(group_rows);
                            } else {
                                auto read_result =
                                    db->StatelessRead(step.table_name(),
                                                      row_key, sparse);
                                out.keys.push_back(row_key);
                                out.tids.push_back(read_result.tid);
                                if (read_result.found &&
                                    !(!fe_semijoins.empty() &&
                                      sj_reject(read_result.value))) {
                                    out.values.push_back(worker_project(
                                        std::move(read_result.value)));
                                } else {
                                    out.values.push_back("");
                                }
                            }
                        }
                        db->ReleaseMasstreeThreadEpoch();
                    });
                }
                for (auto& worker : workers) worker.join();

                bool any_failed = false;
                for (char worker_failed : failed) {
                    if (worker_failed) any_failed = true;
                }
                if (!any_failed) {
                    // Append worker chunks in probe order.
                    for (unsigned worker_index = 0;
                         worker_index < worker_count; ++worker_index) {
                        ProbeOut& out = outputs[worker_index];
                        for (size_t i = 0; i < out.keys.size(); ++i) {
                            if (!out.secondary_keys.empty()) {
                                step_result->add_secondary_keys(
                                    std::move(out.secondary_keys[i]));
                            }
                            step_result->add_scan_keys(
                                std::move(out.keys[i]));
                            step_result->add_scan_values(
                                std::move(out.values[i]));
                            step_result->add_scan_tids(out.tids[i]);
                        }
                        if (scan_probe) {
                            const size_t begin =
                                probe_count * worker_index / worker_count;
                            const size_t end =
                                probe_count * (worker_index + 1) /
                                worker_count;
                            for (size_t probe_index = begin;
                                 probe_index < end; ++probe_index) {
                                const std::string& row_key =
                                    probe_keys[probe_index];
                                step_result->add_group_sizes(
                                    out.group_rows[probe_index - begin]);
                                step_result->add_group_start_keys(row_key);
                                step_result->add_group_end_keys(
                                    next_lexicographic_key(row_key));
                            }
                        }
                    }
                    continue;
                }
                // Nothing was emitted yet; use the serial loop below.
            }

            for (const std::string& row_key : probe_keys) {
                if (step.is_scan()) {
                    // Per-probe range scan: [row_key, next(row_key)).
                    const std::string row_end = next_lexicographic_key(row_key);
                    int group_rows = 0;
                    if (step.index_name().empty()) {
                        auto scan_result =
                            db_manager_->get_database()->StatelessRangeScan(
                                step.table_name(), row_key, row_end,
                                step.scan_limit(), step.reverse_scan(),
                                sparse);
                        if (!scan_result.ok) {
                            response.set_ok(false);
                            flat_plan::encode_to_string(response, result);
                            return;
                        }
                        for (auto& r : scan_result.rows) {
                            if (!row_passes(r.value)) continue;
                            if (!fe_semijoins.empty() && sj_reject(r.value))
                                continue;
                            step_result->add_scan_keys(std::move(r.key));
                            step_result->add_scan_values(
                                project_value(std::move(r.value)));
                            step_result->add_scan_tids(r.tid);
                            ++group_rows;
                            if (existence_only) break;
                        }
                    } else {
                        auto scan_result =
                            db_manager_->get_database()
                                ->StatelessSecondaryRangeScan(
                                    step.table_name(), step.index_name(),
                                    row_key, row_end, step.scan_limit(),
                                    step.reverse_scan(), sparse);
                        if (!scan_result.ok) {
                            response.set_ok(false);
                            flat_plan::encode_to_string(response, result);
                            return;
                        }
                        for (auto& r : scan_result.rows) {
                            if (!row_passes(r.value)) continue;
                            if (!fe_semijoins.empty() && sj_reject(r.value))
                                continue;
                            step_result->add_secondary_keys(
                                std::move(r.secondary_key));
                            step_result->add_scan_keys(std::move(r.primary_key));
                            step_result->add_scan_values(
                                project_value(std::move(r.value)));
                            step_result->add_scan_tids(r.tid);
                            ++group_rows;
                            if (existence_only) break;
                        }
                    }
                    step_result->add_group_sizes(
                        static_cast<uint32_t>(group_rows));
                    step_result->add_group_start_keys(row_key);
                    step_result->add_group_end_keys(row_end);
                    continue;
                }

                auto read_result =
                    db_manager_->get_database()->StatelessRead(
                        step.table_name(), row_key, sparse);
                step_result->add_scan_keys(row_key);
                step_result->add_scan_tids(read_result.tid);
                if (read_result.found &&
                    !(!fe_semijoins.empty() &&
                      sj_reject(read_result.value))) {
                    step_result->add_scan_values(
                        project_value(std::move(read_result.value)));
                } else {
                    // Semijoin-rejected point probes are covered as not-found.
                    step_result->add_scan_values("");
                }
            }
            if (projection_failed) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            continue;
        }

        if (!step.is_scan()) {
            auto read_result =
                db_manager_->get_database()->StatelessRead(
                    step.table_name(), start_key, sparse);
            step_result->set_actual_key(start_key);
            step_result->set_actual_start_key(start_key);
            step_result->set_found(read_result.found);
            step_result->set_tid(read_result.tid);
            if (read_result.found) {
                step_result->set_value(project_value(std::move(read_result.value)));
            }
            if (projection_failed) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            continue;
        }

        if (step.index_name().empty()) {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            if (!step.for_each() && step.scan_limit() == 0 &&
                !step.reverse_scan() && step.has_aggregate() &&
                step.aggregate().aggs_size() > 0) {
                if (parallel_primary_pax_aggregate_scan(
                        db_manager_->get_database().get(), step, start_key,
                        end_key, step_result)) {
                    continue;
                }
                if (parallel_primary_aggregate_scan(
                        db_manager_->get_database().get(), step, start_key,
                        end_key, step_result)) {
                    continue;
                }
            }
            if (!(step.has_aggregate() && step.aggregate().aggs_size() > 0)) {
                // Strip-direct row scan: filter on PAX cells, gather
                // survivors only (projected). Falls back when the table is
                // not fully PAX-resident.
                if (pax_ref_scan_emit(db_manager_->get_database().get(), step,
                                      start_key, end_key, step_result,
                                      projection_failed, fe_semijoins)) {
                    if (projection_failed) {
                        response.set_ok(false);
                        flat_plan::encode_to_string(response, result);
                        return;
                    }
                    continue;
                }
            }
            if (!step.for_each() && step.scan_limit() == 0 &&
                !step.reverse_scan() &&
                !(step.has_aggregate() && step.aggregate().aggs_size() > 0) &&
                step.has_filter() && step.filter().has_expr()) {
                if (parallel_primary_filter_scan(
                        db_manager_->get_database().get(), step, start_key,
                        end_key, step_result)) {
                    continue;
                }
            }

            // With a pushed filter, LIMIT must apply after filter evaluation.
            const bool limit_after_filter =
                step.has_filter() && step.filter().has_expr();
            const uint64_t scan_limit_for_lineairdb =
                limit_after_filter ? 0 : step.scan_limit();
            auto scan_result =
                db_manager_->get_database()->StatelessRangeScan(
                    step.table_name(), start_key, end_key,
                    scan_limit_for_lineairdb, step.reverse_scan());
            if (!scan_result.ok) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            // Aggregation emits synthetic group rows instead of base rows.
            bool aggregated = false;
            if (step.has_aggregate() && step.aggregate().aggs_size() > 0) {
                if (step_filter != nullptr) {
                    // Aggregate filters fail closed: once rows are folded into
                    // groups, MySQL cannot recheck an unparseable base row.
                    PredicateEvaluator agg_eval;
                    bool parse_failed = false;
                    std::remove_reference_t<decltype(scan_result.rows)> filtered;
                    filtered.reserve(scan_result.rows.size());
                    for (auto& row : scan_result.rows) {
                        if (!agg_eval.parse_row(row.value.data(),
                                                row.value.size(),
                                                step_filter_cols)) {
                            parse_failed = true;
                            break;
                        }
                        if (agg_eval.evaluate(*step_filter))
                            filtered.push_back(std::move(row));
                    }
                    if (parse_failed) {
                        response.set_ok(false);
                        flat_plan::encode_to_string(response, result);
                        return;
                    }
                    scan_result.rows = std::move(filtered);
                }
                aggregated = server_aggregate_scan(step.aggregate(),
                                                   scan_result.rows,
                                                   step_result,
                                                   step_has_group_having
                                                       ? &step.filter()
                                                       : nullptr);
            }
            if (!aggregated) {
                uint64_t emitted = 0;
                for (auto& row : scan_result.rows) {
                    if (!row_passes(row.value)) {
                        // Negative coverage for point probes into this scan.
                        step_result->add_filtered_keys(std::move(row.key));
                        continue;
                    }
                    if (!fe_semijoins.empty() && sj_reject(row.value))
                        continue;
                    step_result->add_scan_keys(std::move(row.key));
                    step_result->add_scan_values(project_value(std::move(row.value)));
                    step_result->add_scan_tids(row.tid);
                    if (step.scan_limit() > 0 &&
                        ++emitted >= step.scan_limit()) {
                        break;
                    }
                }
            }
        } else {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            auto scan_result =
                db_manager_->get_database()->StatelessSecondaryRangeScan(
                    step.table_name(), step.index_name(), start_key, end_key,
                    step.scan_limit(), step.reverse_scan(), sparse);
            if (!scan_result.ok) {
                response.set_ok(false);
                flat_plan::encode_to_string(response, result);
                return;
            }
            for (auto& row : scan_result.rows) {
                if (!row_passes(row.value)) {
                    // Secondary scans report rejected rows by primary key.
                    step_result->add_filtered_keys(std::move(row.primary_key));
                    continue;
                }
                if (!fe_semijoins.empty() && sj_reject(row.value))
                    continue;
                step_result->add_secondary_keys(std::move(row.secondary_key));
                step_result->add_scan_keys(std::move(row.primary_key));
                step_result->add_scan_values(project_value(std::move(row.value)));
                step_result->add_scan_tids(row.tid);
            }
        }
        if (projection_failed) {
            response.set_ok(false);
            flat_plan::encode_to_string(response, result);
            return;
        }
    }

    flat_plan::encode_to_string(response, result);
}

void LineairDBRpc::handleTxValidateAndCommit(const std::string& message,
                                             std::string& result) {
    LineairDB::Protocol::TxValidateAndCommit::Request request;
    LineairDB::Protocol::TxValidateAndCommit::Response response;

    request.ParseFromString(message);

    std::vector<LineairDB::ExternalReadEntry> reads;
    reads.reserve(request.reads_size());
    for (const auto& read : request.reads()) {
        reads.push_back({read.table_name(), read.key(), read.tid(),
                         read.found()});
    }

    std::vector<LineairDB::ExternalWriteEntry> writes;
    writes.reserve(request.writes_size() + request.deletes_size());
    for (const auto& write : request.writes()) {
        writes.push_back({write.table_name(), write.key(), write.value(),
                          write.is_delete()});
    }
    for (const auto& del : request.deletes()) {
        writes.push_back({del.table_name(), del.key(), "", true});
    }

    std::vector<LineairDB::ExternalSecondaryIndexEntry> si_ops;
    si_ops.reserve(request.secondary_index_ops_size());
    for (const auto& op : request.secondary_index_ops()) {
        si_ops.push_back({op.table_name(), op.index_name(),
                          op.secondary_key(), op.primary_key(),
                          op.is_delete()});
    }

    std::vector<LineairDB::ExternalRangeReadEntry> range_reads;
    range_reads.reserve(request.range_reads_size());
    for (const auto& range : request.range_reads()) {
        LineairDB::ExternalRangeReadEntry entry;
        entry.table_name = range.table_name();
        entry.index_name = range.index_name();
        entry.start_key = range.start_key();
        entry.end_key = range.end_key();
        entry.row_limit = range.row_limit();
        entry.reverse_scan = range.reverse_scan();
        entry.result_keys.reserve(range.result_keys_size());
        for (const auto& key : range.result_keys()) {
            entry.result_keys.push_back(key);
        }
        entry.result_primary_keys.reserve(range.result_primary_keys_size());
        for (const auto& key : range.result_primary_keys()) {
            entry.result_primary_keys.push_back(key);
        }
        range_reads.push_back(std::move(entry));
    }

    std::string abort_reason;
    const bool committed =
        db_manager_->get_database()->ValidateAndCommit(reads, writes, si_ops,
                                                       range_reads,
                                                       &abort_reason);
    response.set_committed(committed);
    if (!committed && !abort_reason.empty()) {
        response.set_abort_reason(abort_reason);
    }

    if (committed && request.row_deltas_size() > 0) {
        row_counts_->apply_deltas(request.row_deltas());
    }
    if (committed && request.fence()) {
        db_manager_->get_database()->Fence();
    }

    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWrite(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWrite");

    LineairDB::Protocol::TxWrite::Request request;
    LineairDB::Protocol::TxWrite::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& value_str = request.value();
        tx->Write(request.key(), reinterpret_cast<const std::byte*>(value_str.c_str()), value_str.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("Wrote key '%s' to transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDelete(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxDelete");

    LineairDB::Protocol::TxDelete::Request request;
    LineairDB::Protocol::TxDelete::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        tx->Delete(request.key());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("Deleted key '%s' from transaction %ld", request.key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxReadSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxReadSecondaryIndex");

    LineairDB::Protocol::TxReadSecondaryIndex::Request request;
    LineairDB::Protocol::TxReadSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        auto results = tx->ReadSecondaryIndex(request.index_name(), request.secondary_key());
        response.set_is_aborted(tx->IsAborted());

        for (const auto& [ptr, size] : results) {
            std::string value(reinterpret_cast<const char*>(ptr), size);
            response.add_values(value);
        }
        LOG_DEBUG("ReadSecondaryIndex index='%s' key='%s' tx=%ld: %d values",
                  request.index_name().c_str(), request.secondary_key().c_str(), tx_id, response.values_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for read_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxWriteSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxWriteSecondaryIndex");

    LineairDB::Protocol::TxWriteSecondaryIndex::Request request;
    LineairDB::Protocol::TxWriteSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->WriteSecondaryIndex(request.index_name(), request.secondary_key(),
                                reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for write_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxDeleteSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxDeleteSecondaryIndex");

    LineairDB::Protocol::TxDeleteSecondaryIndex::Request request;
    LineairDB::Protocol::TxDeleteSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->DeleteSecondaryIndex(request.index_name(), request.secondary_key(),
                                 reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("DeleteSecondaryIndex index='%s' key='%s' tx=%ld",
                  request.index_name().c_str(), request.secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for delete_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxUpdateSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxUpdateSecondaryIndex");

    LineairDB::Protocol::TxUpdateSecondaryIndex::Request request;
    LineairDB::Protocol::TxUpdateSecondaryIndex::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        const std::string& pk = request.primary_key();
        tx->UpdateSecondaryIndex(request.index_name(),
                                 request.old_secondary_key(), request.new_secondary_key(),
                                 reinterpret_cast<const std::byte*>(pk.c_str()), pk.size());
        response.set_is_aborted(tx->IsAborted());
        response.set_success(!tx->IsAborted());
        LOG_DEBUG("UpdateSecondaryIndex index='%s' old='%s' new='%s' tx=%ld",
                  request.index_name().c_str(), request.old_secondary_key().c_str(),
                  request.new_secondary_key().c_str(), tx_id);
    } else {
        response.set_success(false);
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for update_secondary_index: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingKeysInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysInRange");

    LineairDB::Protocol::TxGetMatchingKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingKeysInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        auto scan_result = tx->Scan(
            start_key, end_opt, [&response](auto key, auto) {
                response.add_keys(std::string(key));
                return false;
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
        }
        LOG_DEBUG("GetMatchingKeysInRange tx=%ld: %d keys", tx_id, response.keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_keys_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingKeysAndValuesInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysAndValuesInRange");

    LineairDB::Protocol::TxGetMatchingKeysAndValuesInRange::Request request;
    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);

    // Respond with flat binary instead of protobuf to avoid per-entry overhead.
    // Format: [is_aborted:1B] [key_len:4B][key][val_len:4B][val]... [sentinel:key_len=0]
    result.clear();
    result.reserve(4096);
    result.push_back(0);   // is_aborted placeholder (updated after Scan completes)

    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();
        const uint64_t row_limit = request.row_limit();
        const bool reverse_scan = request.reverse_scan();
        // Count rows actually returned after tombstone and predicate checks.
        uint64_t returned_rows = 0;

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        // Predicate pushdown: prepare filter if present
        const bool has_filter = request.has_filter() && request.filter().has_expr();
        const auto* filter_expr = has_filter ? &request.filter().expr() : nullptr;
        uint32_t filter_num_cols = has_filter ? request.filter().num_columns() : 0;
        PredicateEvaluator evaluator;

        // Scan callback: value is pair<const void*, size_t> from LineairDB
        auto append_matching_row =
            [&result, row_limit, &returned_rows, filter_expr, filter_num_cols,
             &evaluator](auto key, auto value) {
                // Skip tombstones (deleted rows)
                if (value.first == nullptr || value.second == 0) { return false; }
                // Predicate pushdown: evaluate filter if present
                if (filter_expr) {
                    if (evaluator.parse_row(static_cast<const char*>(value.first),
                                            value.second, filter_num_cols)) {
                        if (!evaluator.evaluate(*filter_expr)) {
                            return false;  // filter rejected → skip row, continue scanning
                        }
                    } else {
                        // Return parse-failed rows for MySQL to check, but do
                        // not count them toward a pushed LIMIT.
                        uint32_t klen = static_cast<uint32_t>(key.size());
                        uint32_t vlen = static_cast<uint32_t>(value.second);
                        result.append(reinterpret_cast<const char*>(&klen), 4);
                        result.append(key.data(), key.size());
                        result.append(reinterpret_cast<const char*>(&vlen), 4);
                        result.append(static_cast<const char*>(value.first), value.second);
                        return false;
                    }
                }
                // Append key-value entry in flat binary format
                uint32_t klen = static_cast<uint32_t>(key.size());
                uint32_t vlen = static_cast<uint32_t>(value.second);
                result.append(reinterpret_cast<const char*>(&klen), 4);
                result.append(key.data(), key.size());
                result.append(reinterpret_cast<const char*>(&vlen), 4);
                result.append(static_cast<const char*>(value.first), value.second);
                returned_rows++;
                // Stop the LineairDB scan once the pushed LIMIT is satisfied.
                return row_limit > 0 && returned_rows >= row_limit;
            };

        std::optional<size_t> scan_result;
        if (reverse_scan) {
            scan_result = tx->ScanReverse(start_key, end_opt, append_matching_row);
        } else {
            scan_result = tx->Scan(start_key, end_opt, append_matching_row);
        }

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            result[0] = 1;  // update is_aborted placeholder
        } else if (tx->IsAborted()) {
            result[0] = 1;
        }
    } else {
        result[0] = 1;
        LOG_WARNING("Transaction not found for get_matching_keys_and_values_in_range: %ld", tx_id);
    }

    uint32_t sentinel = 0;
    result.append(reinterpret_cast<const char*>(&sentinel), 4);  // sentinel: key_len=0 marks end of entries
}

void LineairDBRpc::handleTxGetMatchingKeysAndValuesFromPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingKeysAndValuesFromPrefix");

    LineairDB::Protocol::TxGetMatchingKeysAndValuesFromPrefix::Request request;
    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);

    // Same flat binary format as handleTxGetMatchingKeysAndValuesInRange
    result.clear();
    result.reserve(4096);
    result.push_back(0);   // is_aborted placeholder

    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string prefix = request.prefix();
        bool first_key_checked = false;
        bool prefix_miss = false;

        // Predicate pushdown: prepare filter if present
        const bool has_filter = request.has_filter() && request.filter().has_expr();
        const auto* filter_expr = has_filter ? &request.filter().expr() : nullptr;
        uint32_t filter_num_cols = has_filter ? request.filter().num_columns() : 0;
        PredicateEvaluator evaluator;

        // Scan callback: value is pair<const void*, size_t> from LineairDB
        auto scan_result = tx->Scan(
            prefix, std::nullopt,
            [&result, &first_key_checked, &prefix_miss, &prefix,
             filter_expr, filter_num_cols, &evaluator, this](auto key, auto value) {
                // Check if first key matches the prefix; if not, abort scan early
                if (!first_key_checked) {
                    first_key_checked = true;
                    std::string key_str(key);
                    if (!key_prefix_is_matching(prefix, key_str)) { prefix_miss = true; return true; }
                }
                // Skip tombstones (deleted rows)
                if (value.first == nullptr || value.second == 0) { return false; }
                // Predicate pushdown: evaluate filter if present
                if (filter_expr) {
                    if (evaluator.parse_row(static_cast<const char*>(value.first),
                                            value.second, filter_num_cols)) {
                        if (!evaluator.evaluate(*filter_expr)) {
                            return false;  // filter rejected → skip row, continue scanning
                        }
                    }
                    // parse_row failure → include row (safe fallback)
                }
                // Append key-value entry in flat binary format
                uint32_t klen = static_cast<uint32_t>(key.size());
                uint32_t vlen = static_cast<uint32_t>(value.second);
                result.append(reinterpret_cast<const char*>(&klen), 4);
                result.append(key.data(), key.size());
                result.append(reinterpret_cast<const char*>(&vlen), 4);
                result.append(static_cast<const char*>(value.first), value.second);
                return false;  // continue scanning
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            result[0] = 1;
        } else if (tx->IsAborted()) {
            result[0] = 1;
        }
        if (prefix_miss) {
            // No matching keys found; discard entries, keep just header + sentinel
            result.resize(1);
        }
    } else {
        result[0] = 1;
        LOG_WARNING("Transaction not found for get_matching_keys_and_values_from_prefix: %ld", tx_id);
    }

    uint32_t sentinel = 0;
    result.append(reinterpret_cast<const char*>(&sentinel), 4);  // sentinel
}

void LineairDBRpc::handleTxFetchLastKeyInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastKeyInRange");

    LineairDB::Protocol::TxFetchLastKeyInRange::Request request;
    LineairDB::Protocol::TxFetchLastKeyInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        std::optional<std::string> result;
        auto scan_result = tx->ScanReverse(
            start_key, end_opt, [&result](auto key, auto) {
                result = std::string(key);
                return true;
            });

        // Phantom detection: if ScanReverse returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchLastKeyInRange tx=%ld: found=%s", tx_id, result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_key_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchFirstKeyWithPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchFirstKeyWithPrefix");

    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchFirstKeyWithPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string prefix = request.prefix();
        std::string prefix_end = request.prefix_end();

        std::optional<std::string_view> end_opt;
        if (!prefix_end.empty()) { end_opt = prefix_end; }

        std::optional<std::string> result;
        auto scan_result = tx->Scan(
            prefix, end_opt, [&result, &prefix_end](auto key, auto value) {
                if (!prefix_end.empty() && key == prefix_end) {
                    return true; // exclusive end
                }
                // Skip tombstones
                if (value.first == nullptr || value.second == 0) {
                    return false; // Continue scanning
                }
                result = std::string(key);
                return true; // Stop after first valid key
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchFirstKeyWithPrefix tx=%ld prefix='%s': found=%s",
                  tx_id, prefix.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_first_key_with_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchNextKeyWithPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchNextKeyWithPrefix");

    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Request request;
    LineairDB::Protocol::TxFetchNextKeyWithPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string last_key = request.last_key();
        std::string prefix_end = request.prefix_end();
        bool skip_first = true;

        std::optional<std::string_view> end_opt;
        if (!prefix_end.empty()) { end_opt = prefix_end; }

        std::optional<std::string> result;
        auto scan_result = tx->Scan(
            last_key, end_opt,
            [&result, &skip_first, &last_key, &prefix_end](auto key, auto value) {
                // Skip the last_key itself (we want the next one)
                if (skip_first && key == last_key) {
                    skip_first = false;
                    return false; // Continue scanning
                }
                if (!prefix_end.empty() && key == prefix_end) {
                    return true; // exclusive end
                }
                // Skip tombstones
                if (value.first == nullptr || value.second == 0) {
                    return false; // Continue scanning
                }
                result = std::string(key);
                return true; // Stop after first valid key
            });

        // Phantom detection: if Scan returns nullopt, the transaction is in an abort state
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchNextKeyWithPrefix tx=%ld last_key='%s': found=%s",
                  tx_id, last_key.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_next_key_with_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingPrimaryKeysInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingPrimaryKeysInRange");

    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        auto scan_result = tx->ScanSecondaryIndex(
            index_name, start_key, end_opt,
            [&response]([[maybe_unused]] std::string_view secondary_key,
                        const std::vector<std::string>& primary_keys) {
                for (const auto& pk : primary_keys) { response.add_primary_keys(pk); }
                return false;
            });

        // Phantom detection: ScanSecondaryIndex returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
        }
        LOG_DEBUG("GetMatchingPrimaryKeysInRange tx=%ld index='%s': %d keys",
                  tx_id, index_name.c_str(), response.primary_keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_primary_keys_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxGetMatchingPrimaryKeysFromPrefix(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxGetMatchingPrimaryKeysFromPrefix");

    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Request request;
    LineairDB::Protocol::TxGetMatchingPrimaryKeysFromPrefix::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string prefix = request.prefix();
        bool first_key_checked = false;
        bool prefix_miss = false;

        auto scan_result = tx->ScanSecondaryIndex(
            index_name, prefix, std::nullopt,
            [&response, &first_key_checked, &prefix_miss, &prefix, this]
            (std::string_view secondary_key, const std::vector<std::string>& primary_keys) {
                if (!first_key_checked) {
                    first_key_checked = true;
                    std::string key_str(secondary_key);
                    if (!key_prefix_is_matching(prefix, key_str)) { prefix_miss = true; return true; }
                }
                for (const auto& pk : primary_keys) { response.add_primary_keys(pk); }
                return false;
            });

        // Phantom detection: ScanSecondaryIndex returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (prefix_miss) { response.clear_primary_keys(); }
        }
        LOG_DEBUG("GetMatchingPrimaryKeysFromPrefix tx=%ld index='%s' prefix='%s': %d keys",
                  tx_id, index_name.c_str(), prefix.c_str(), response.primary_keys_size());
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for get_matching_primary_keys_from_prefix: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchLastPrimaryKeyInSecondaryRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastPrimaryKeyInSecondaryRange");

    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Request request;
    LineairDB::Protocol::TxFetchLastPrimaryKeyInSecondaryRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        std::optional<std::string> result;
        auto scan_result = tx->ScanSecondaryIndexReverse(
            index_name, start_key, end_opt,
            [&result]([[maybe_unused]] std::string_view secondary_key,
                      const std::vector<std::string>& primary_keys) {
                if (primary_keys.empty()) { return false; }
                result = primary_keys.back();
                return true;
            });

        // Phantom detection: ScanSecondaryIndexReverse returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            if (result.has_value()) {
                response.set_found(true);
                response.set_primary_key(result.value());
            } else {
                response.set_found(false);
            }
        }
        LOG_DEBUG("FetchLastPrimaryKeyInSecondaryRange tx=%ld index='%s': found=%s",
                  tx_id, index_name.c_str(), result.has_value() ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_primary_key_in_secondary_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleTxFetchLastSecondaryEntryInRange(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling TxFetchLastSecondaryEntryInRange");

    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Request request;
    LineairDB::Protocol::TxFetchLastSecondaryEntryInRange::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        if (!request.table_name().empty()) {
            tx->SetTable(request.table_name());
        }
        std::string index_name = request.index_name();
        std::string start_key = request.start_key();
        std::string end_key = request.end_key();

        std::optional<std::string_view> end_opt;
        if (!end_key.empty()) { end_opt = end_key; }

        bool found = false;
        auto scan_result = tx->ScanSecondaryIndexReverse(
            index_name, start_key, end_opt,
            [&response, &found](std::string_view secondary_key,
                                const std::vector<std::string>& primary_keys) {
                if (primary_keys.empty()) { return false; }
                found = true;
                auto* entry = response.mutable_entry();
                entry->set_secondary_key(std::string(secondary_key));
                for (const auto& pk : primary_keys) { entry->add_primary_keys(pk); }
                return true;
            });

        // Phantom detection: ScanSecondaryIndexReverse returns nullopt if aborted
        if (!scan_result.has_value()) {
            tx->Abort();
            response.set_is_aborted(true);
            response.set_found(false);
        } else {
            response.set_is_aborted(tx->IsAborted());
            response.set_found(found);
        }
        LOG_DEBUG("FetchLastSecondaryEntryInRange tx=%ld index='%s': found=%s",
                  tx_id, index_name.c_str(), found ? "true" : "false");
    } else {
        response.set_is_aborted(true);
        response.set_found(false);
        LOG_WARNING("Transaction not found for fetch_last_secondary_entry_in_range: %ld", tx_id);
    }

    result = response.SerializeAsString();
}
void LineairDBRpc::handleDbFence(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbFence");

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;

    request.ParseFromString(message);

    db_manager_->get_database()->Fence();
    LOG_DEBUG("Database fence completed");

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbEndTransaction(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbEndTransaction");

    LineairDB::Protocol::DbEndTransaction::Request request;
    LineairDB::Protocol::DbEndTransaction::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool fence = request.fence();
        bool committed = db_manager_->get_database()->EndTransaction(
            *tx, [fence, tx_id](LineairDB::TxStatus status) {
                LOG_DEBUG("Transaction %ld ended with status: %d, fence=%s", tx_id, static_cast<int>(status), fence ? "true" : "false");
            });
        bool aborted = !committed;
        response.set_is_aborted(aborted);
        tx_manager_->remove_transaction(tx_id);

        // Apply row-count deltas on successful commit
        if (committed && request.row_deltas_size() > 0) {
            row_counts_->apply_deltas(request.row_deltas());
        }

        LOG_DEBUG("Ended transaction %ld with fence=%s (committed=%s)", tx_id, fence ? "true" : "false", committed ? "true" : "false");
        // Piggyback updated table row counts for the proxy's next transaction.
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    } else {
        response.set_is_aborted(true);
        LOG_WARNING("Transaction not found for end: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateTable(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbCreateTable");

    LineairDB::Protocol::DbCreateTable::Request request;
    LineairDB::Protocol::DbCreateTable::Response response;

    request.ParseFromString(message);

    bool success = db_manager_->get_database()->CreateTable(request.table_name());
    response.set_success(success);
    LOG_DEBUG("CreateTable '%s': %s", request.table_name().c_str(), success ? "success" : "already exists");

    // PAX single-copy storage: shred this table's rows into column strips.
    // Gated on the server env so gate-off runs are byte-identical row-store.
    static const bool pax_storage_enabled = []() {
        const char* v = std::getenv("HELIOS_PAX_STORAGE");
        return v != nullptr && v[0] == '1';
    }();
    if (pax_storage_enabled && request.pax_field_max_bytes_size() > 0) {
        std::vector<uint32_t> widths(request.pax_field_max_bytes().begin(),
                                     request.pax_field_max_bytes().end());
        const bool installed = db_manager_->get_database()->InstallPaxSchema(
            request.table_name(), widths);
        LOG_INFO("PAX schema for '%s': %zu fields, %s",
                 request.table_name().c_str(), widths.size(),
                 installed ? "installed" : "skipped (exists or unsupported)");
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbSetTable(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbSetTable");

    LineairDB::Protocol::DbSetTable::Request request;
    LineairDB::Protocol::DbSetTable::Response response;

    request.ParseFromString(message);

    int64_t tx_id = request.transaction_id();
    auto* tx = tx_manager_->get_transaction(tx_id);
    if (tx) {
        bool success = tx->SetTable(request.table_name());
        response.set_success(success);
        LOG_DEBUG("SetTable '%s' for tx=%ld: %s", request.table_name().c_str(), tx_id, success ? "success" : "failed");
    } else {
        response.set_success(false);
        LOG_WARNING("Transaction not found for set_table: %ld", tx_id);
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateSecondaryIndex(const std::string& message, std::string& result) {
    LOG_DEBUG("Handling DbCreateSecondaryIndex");

    LineairDB::Protocol::DbCreateSecondaryIndex::Request request;
    LineairDB::Protocol::DbCreateSecondaryIndex::Response response;

    request.ParseFromString(message);

    bool success = db_manager_->get_database()->CreateSecondaryIndex(
        request.table_name(), request.index_name(), request.index_type());
    response.set_success(success);

    result = response.SerializeAsString();
}
