#include "parallel_scan.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aggregate_executor.hh"
#include "predicate_evaluator.hh"
#include "row_codec.hh"

bool parallel_primary_aggregate_scan(
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

    const bool has_filter = step.has_filter() && step.filter().has_expr();
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
    emit_agg_groups(spec, groups, step_result);
    return true;
}

bool parallel_primary_filter_scan(
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
