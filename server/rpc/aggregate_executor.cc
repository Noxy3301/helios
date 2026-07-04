#include "aggregate_executor.hh"

#include <algorithm>
#include <string_view>
#include <thread>
#include <utility>

#include "row_codec.hh"

/**
 * @brief Append one LineairDBField-format field to a synthetic group row.
 *
 * Format: [byteSize][valueLength][value]. A null field is encoded as 0xFF.
 */
static void agg_emit_field(std::string& out, std::string_view v, bool is_null) {
    if (is_null) { out.push_back(static_cast<char>(0xFF)); return; }
    size_t len = v.size();
    size_t bs = 1;
    for (size_t t = len >> 8; t; t >>= 8) ++bs;
    out.push_back(static_cast<char>(bs));
    for (size_t i = 0; i < bs; ++i)
        out.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
    out.append(v.data(), v.size());
}

void aggregate_rows_range(
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

void merge_agg_groups(
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

void emit_agg_groups(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::unordered_map<std::string, AggGroupState>& groups,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
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
        step_result->add_scan_keys(std::string());
        step_result->add_scan_values(std::move(row));
        step_result->add_scan_tids(0);
    }
}

bool server_aggregate_scan(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::vector<LineairDB::StatelessScanRow>& rows,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
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

    emit_agg_groups(spec, groups, step_result);
    return true;
}
