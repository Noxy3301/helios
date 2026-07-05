#include "parallel_scan.hh"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "aggregate_executor.hh"
#include "predicate_evaluator.hh"
#include "row_codec.hh"

namespace {

bool is_scan_end_sentinel(const std::string& key) {
    return key.size() == 16 &&
           key.find_first_not_of(static_cast<char>(0xFF)) == std::string::npos;
}

void collect_filter_columns(
    const LineairDB::Protocol::FilterExpr& expr,
    std::vector<uint32_t>& columns) {
    if (expr.op() == LineairDB::Protocol::FilterExpr::COLUMN_REF) {
        columns.push_back(expr.column_index());
    }
    for (const auto& child : expr.children()) {
        collect_filter_columns(child, columns);
    }
}

void sort_unique_columns(std::vector<uint32_t>& columns) {
    std::sort(columns.begin(), columns.end());
    columns.erase(std::unique(columns.begin(), columns.end()), columns.end());
}

const std::vector<uint32_t>* selected_columns_for_projected_read(
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    std::vector<uint32_t>& selected_columns) {
    selected_columns.clear();
    if (!step.has_projection() ||
        step.projection().field_indexes_size() == 0) {
        return nullptr;
    }
    selected_columns.assign(step.projection().field_indexes().begin(),
                            step.projection().field_indexes().end());
    if (step.has_filter() && step.filter().has_expr()) {
        collect_filter_columns(step.filter().expr(), selected_columns);
    }
    sort_unique_columns(selected_columns);
    if (step.projection().num_columns() > 0 &&
        selected_columns.size() >= step.projection().num_columns()) {
        return nullptr;
    }
    return &selected_columns;
}

const std::vector<uint32_t>* selected_columns_for_aggregate_read(
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    std::vector<uint32_t>& selected_columns) {
    selected_columns.clear();
    if (!step.has_aggregate() || step.aggregate().aggs_size() == 0) {
        return nullptr;
    }
    if (step.has_filter() && step.filter().has_expr()) {
        collect_filter_columns(step.filter().expr(), selected_columns);
    }
    const auto& spec = step.aggregate();
    for (uint32_t column : spec.group_columns()) {
        selected_columns.push_back(column);
    }
    for (const auto& aggregate_func : spec.aggs()) {
        if (aggregate_func.kind() != LineairDB::Protocol::AggFunc::AGG_COUNT) {
            collect_filter_columns(aggregate_func.arg(), selected_columns);
        }
    }
    sort_unique_columns(selected_columns);
    if (spec.num_columns() > 0 &&
        selected_columns.size() >= spec.num_columns()) {
        return nullptr;
    }
    return &selected_columns;
}

bool aggregate_pax_group(
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const LineairDB::Pax::PaxGroup& group,
    std::unordered_map<std::string, AggGroupState>& groups) {
    using AF = LineairDB::Protocol::AggFunc;

    const auto& spec = step.aggregate();
    const bool has_filter = step.has_filter() && step.filter().has_expr();
    const int n_agg = spec.aggs_size();
    const int n_grp = spec.group_columns_size();

    PredicateEvaluator evaluator;
    std::string keybuf;
    std::vector<std::string_view> group_values(n_grp);

    for (uint32_t base = 0; base < LineairDB::Pax::PaxGroup::kRows;
         base += 64) {
        // Gather one visibility word at a time; empty tail regions become one
        // cheap zero-word check.
        uint64_t visible_slots = 0;
        for (uint32_t offset = 0; offset < 64; ++offset) {
            if (group.IsVisible(base + offset)) {
                visible_slots |= uint64_t{1} << offset;
            }
        }

        while (visible_slots != 0) {
            const uint32_t bit =
                static_cast<uint32_t>(__builtin_ctzll(visible_slots));
            visible_slots &= visible_slots - 1;
            const uint32_t slot = base + bit;

            if (has_filter) {
                if (!evaluator.set_row_from_pax(
                        group, slot, step.filter().num_columns())) {
                    return false;
                }
                if (!evaluator.evaluate(step.filter().expr())) continue;
            }

            const PaxRowRef row{&group, slot};
            keybuf.clear();
            for (int g = 0; g < n_grp; ++g) {
                group_values[g] = extract_value_column(row,
                                                       spec.group_columns(g));
                const uint32_t length =
                    static_cast<uint32_t>(group_values[g].size());
                keybuf.append(reinterpret_cast<const char*>(&length),
                              sizeof(length));
                keybuf.append(group_values[g].data(), group_values[g].size());
            }

            auto it = groups.find(keybuf);
            AggGroupState* group_state;
            if (it == groups.end()) {
                AggGroupState state;
                state.key_cols.resize(n_grp);
                for (int g = 0; g < n_grp; ++g) {
                    state.key_cols[g] = std::string(group_values[g]);
                }
                state.count.assign(n_agg, 0);
                state.sum.assign(n_agg, DecimalValue{});
                group_state =
                    &groups.emplace(std::move(keybuf), std::move(state))
                         .first->second;
                keybuf.clear();
            } else {
                group_state = &it->second;
            }

            for (int a = 0; a < n_agg; ++a) {
                const auto& aggregate_func = spec.aggs(a);
                if (aggregate_func.kind() == AF::AGG_COUNT) {
                    group_state->count[a] += 1;
                    continue;
                }

                DecimalValue value =
                    evaluate_decimal_expression(aggregate_func.arg(), row);
                if (!value.is_null) {
                    add_decimal_value(group_state->sum[a], value);
                    group_state->count[a] += 1;
                }
            }
        }
    }
    return true;
}

}  // namespace

bool parallel_primary_pax_aggregate_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result) {
    if (db == nullptr) return false;

    // PAX strips do not store primary-key bytes, so this path is only valid
    // for the whole-table primary scan shape produced by the proxy.
    if (!start_key.empty() || !is_scan_end_sentinel(end_key)) return false;

    auto* store = db->GetPaxStore(step.table_name());
    if (store == nullptr) return false;

    const uint64_t overflow_before = store->overflow_count();
    if (overflow_before > 0) return false;
    const uint64_t slots_before = store->slots_allocated();
    const size_t n_groups = store->group_count();

    std::unordered_map<std::string, AggGroupState> groups;
    if (n_groups > 0) {
        std::vector<uint64_t> write_counters(n_groups, 0);
        for (size_t group_index = 0; group_index < n_groups; ++group_index) {
            auto* group = store->group(group_index);
            write_counters[group_index] =
                group != nullptr
                    ? group->write_counter.load(std::memory_order_acquire)
                    : 0;
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

        for (unsigned worker_index = 0; worker_index < worker_count;
             ++worker_index) {
            workers.emplace_back([&, worker_index] {
                const size_t begin = n_groups * worker_index / worker_count;
                const size_t end =
                    n_groups * (worker_index + 1) / worker_count;
                for (size_t group_index = begin; group_index < end;
                     ++group_index) {
                    auto* group = store->group(group_index);
                    if (group == nullptr) continue;
                    if (!aggregate_pax_group(step, *group,
                                             locals[worker_index])) {
                        failed[worker_index] = 1;
                        return;
                    }
                }
            });
        }
        for (auto& worker : workers) worker.join();

        for (char worker_failed : failed) {
            if (worker_failed) return false;
        }

        // A changed counter means a writer scattered or retired a slot while
        // this scan was reading cells; retry through the TID-checked row path.
        for (size_t group_index = 0; group_index < n_groups; ++group_index) {
            auto* group = store->group(group_index);
            const uint64_t current_counter =
                group != nullptr
                    ? group->write_counter.load(std::memory_order_acquire)
                    : 0;
            if (current_counter != write_counters[group_index]) return false;
        }

        for (auto& local : locals) merge_agg_groups(groups, local, n_agg);
    }

    if (store->slots_allocated() != slots_before ||
        store->overflow_count() != overflow_before) {
        return false;
    }

    emit_agg_groups(step.aggregate(), groups, step_result);
    return true;
}

bool parallel_primary_pax_row_ref_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result,
    bool& projection_failed) {
    if (db == nullptr) return false;

    const bool has_filter = step.has_filter() && step.filter().has_expr();
    const bool has_projection = step.has_projection();
    // With a pushed filter, LIMIT applies after filter evaluation.
    const uint64_t ref_scan_limit = has_filter ? 0 : step.scan_limit();

    std::vector<uint32_t> kept_columns;
    if (has_projection) {
        kept_columns.assign(step.projection().field_indexes().begin(),
                            step.projection().field_indexes().end());
    }
    std::vector<uint32_t> selected_columns;
    const std::vector<uint32_t>* selected_columns_for_reread =
        selected_columns_for_projected_read(step, selected_columns);

    struct RefChunkOut {
        struct Event {
            bool filtered_key = false;
            size_t index = 0;
        };

        // Preserve the scan observation order between accepted rows and
        // filtered keys. LIMIT is applied while replaying this event stream,
        // matching the materialized-row path's key-set coverage.
        std::vector<std::string> scan_keys;
        std::vector<std::string> scan_values;
        std::vector<std::string> filtered_keys;
        std::vector<uint64_t> tids;
        std::vector<Event> events;
        bool materialized_path_required = false;
        bool projection_failed = false;
    };

    auto run_range = [&](const std::string& chunk_start,
                         const std::string& chunk_end, RefChunkOut& out,
                         bool release_epoch) {
        auto refs = db->StatelessPaxRowRefScan(
            step.table_name(), chunk_start, chunk_end, ref_scan_limit,
            step.reverse_scan());
        if (!refs.ok) {
            out.materialized_path_required = true;
            if (release_epoch) db->ReleaseMasstreeThreadEpoch();
            return;
        }

        PredicateEvaluator evaluator;
        for (auto& row_ref : refs.rows) {
            const auto* group =
                static_cast<const LineairDB::Pax::PaxGroup*>(row_ref.group);
            bool pass = true;
            if (has_filter) {
                if (!evaluator.set_row_from_pax(
                        *group, row_ref.slot, step.filter().num_columns())) {
                    out.materialized_path_required = true;
                    break;
                }
                pass = evaluator.evaluate(step.filter().expr());
            }

            std::string value;
            if (pass) {
                if (has_projection) {
                    if (!group->GatherRowProjected(row_ref.slot,
                                                   kept_columns.data(),
                                                   kept_columns.size(),
                                                   value)) {
                        out.materialized_path_required = true;
                        break;
                    }
                } else {
                    value.resize(row_ref.row_size);
                    const size_t gathered = group->GatherRow(
                        row_ref.slot,
                        reinterpret_cast<std::byte*>(value.data()),
                        row_ref.row_size);
                    if (gathered != row_ref.row_size) value.resize(gathered);
                }
            }

            // The cells just read are valid only if the row TID still matches
            // the ref-scan observation. Otherwise re-read a stable row copy.
            if (LineairDB::PaxRowRefCurrentTid(row_ref) != row_ref.tid) {
                auto reread = db->StatelessRead(
                    step.table_name(), row_ref.key,
                    selected_columns_for_reread);
                if (!reread.found) continue;

                bool reread_pass = true;
                if (has_filter) {
                    PredicateEvaluator reread_eval;
                    if (reread_eval.parse_row(reread.value.data(),
                                              reread.value.size(),
                                              step.filter().num_columns())) {
                        reread_pass =
                            reread_eval.evaluate(step.filter().expr());
                    }
                }
                if (!reread_pass) {
                    out.events.push_back({true, out.filtered_keys.size()});
                    out.filtered_keys.push_back(std::move(row_ref.key));
                    continue;
                }

                std::string reread_value;
                if (has_projection) {
                    if (!trim_row_value(reread.value,
                                        step.projection().field_indexes(),
                                        step.projection().num_columns(),
                                        reread_value)) {
                        out.projection_failed = true;
                        reread_value = std::move(reread.value);
                    }
                } else {
                    reread_value = std::move(reread.value);
                }
                out.events.push_back({false, out.scan_keys.size()});
                out.scan_keys.push_back(std::move(row_ref.key));
                out.scan_values.push_back(std::move(reread_value));
                out.tids.push_back(reread.tid);
                continue;
            }

            if (!pass) {
                out.events.push_back({true, out.filtered_keys.size()});
                out.filtered_keys.push_back(std::move(row_ref.key));
                continue;
            }
            out.events.push_back({false, out.scan_keys.size()});
            out.scan_keys.push_back(std::move(row_ref.key));
            out.scan_values.push_back(std::move(value));
            out.tids.push_back(row_ref.tid);
        }

        if (release_epoch) db->ReleaseMasstreeThreadEpoch();
    };

    std::vector<RefChunkOut> chunks;
    bool ran_parallel = false;
    if (step.scan_limit() == 0 && !step.reverse_scan()) {
        const unsigned nproc = std::thread::hardware_concurrency();
        const unsigned max_threads = std::min<unsigned>(nproc ? nproc : 4, 8);
        const std::vector<uint32_t> key_only_columns;
        auto first = db->StatelessRangeScan(step.table_name(), start_key,
                                            end_key, 1, false,
                                            &key_only_columns);
        auto last = db->StatelessRangeScan(step.table_name(), start_key,
                                           end_key, 1, true,
                                           &key_only_columns);
        int64_t lo = 0;
        int64_t hi = 0;
        if (max_threads > 1 && first.ok && !first.rows.empty() && last.ok &&
            !last.rows.empty() &&
            decode_leading_int_key(first.rows.front().key, lo) &&
            decode_leading_int_key(last.rows.front().key, hi) && hi > lo) {
            const uint64_t span = static_cast<uint64_t>(hi - lo) + 1;
            constexpr uint64_t kMinParallelRows = 500000;
            constexpr uint64_t kMorselRows = 128000;
            if (span >= kMinParallelRows) {
                const unsigned worker_count = static_cast<unsigned>(
                    std::min<uint64_t>(
                        (span + kMorselRows - 1) / kMorselRows,
                        max_threads));
                if (worker_count > 1) {
                    std::vector<std::string> starts(worker_count);
                    std::vector<std::string> ends(worker_count);
                    for (unsigned i = 0; i < worker_count; ++i) {
                        const int64_t begin_value =
                            lo + static_cast<int64_t>((span * i) /
                                                      worker_count);
                        const int64_t end_value =
                            (i + 1 == worker_count)
                                ? hi + 1
                                : lo + static_cast<int64_t>(
                                           (span * (i + 1)) / worker_count);
                        starts[i] = i == 0 ? start_key
                                           : encode_int_key_part(begin_value);
                        ends[i] = i + 1 == worker_count
                                      ? end_key
                                      : encode_int_key_part(end_value);
                    }

                    chunks = std::vector<RefChunkOut>(worker_count);
                    std::vector<std::thread> workers;
                    workers.reserve(worker_count);
                    for (unsigned worker_index = 0;
                         worker_index < worker_count; ++worker_index) {
                        workers.emplace_back([&, worker_index] {
                            run_range(starts[worker_index],
                                      ends[worker_index],
                                      chunks[worker_index], true);
                        });
                    }
                    for (auto& worker : workers) worker.join();
                    ran_parallel = true;
                }
            }
        }
    }
    if (!ran_parallel) {
        chunks = std::vector<RefChunkOut>(1);
        run_range(start_key, end_key, chunks[0], false);
    }

    for (const auto& chunk : chunks) {
        if (chunk.materialized_path_required) return false;
        if (chunk.projection_failed) projection_failed = true;
    }

    uint64_t emitted = 0;
    for (auto& chunk : chunks) {
        // Replay the mixed stream in scan order so filtered keys observed
        // before LIMIT cutoff remain visible to range validation.
        for (const auto& event : chunk.events) {
            if (event.filtered_key) {
                step_result->add_filtered_keys(
                    std::move(chunk.filtered_keys[event.index]));
                continue;
            }
            step_result->add_scan_keys(
                std::move(chunk.scan_keys[event.index]));
            step_result->add_scan_values(
                std::move(chunk.scan_values[event.index]));
            step_result->add_scan_tids(chunk.tids[event.index]);
            if (step.scan_limit() > 0 && ++emitted >= step.scan_limit()) {
                return true;
            }
        }
    }
    return true;
}

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
    const std::vector<uint32_t> key_only_columns;
    auto first = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                        1, false, &key_only_columns);
    if (!first.ok || first.rows.empty()) return false;
    auto last = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                       1, true, &key_only_columns);
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
    std::vector<uint32_t> selected_columns;
    const std::vector<uint32_t>* selected_columns_for_reads =
        selected_columns_for_aggregate_read(step, selected_columns);
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
                                       ends[worker_index], 0, false,
                                       selected_columns_for_reads);
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
    const std::vector<uint32_t> key_only_columns;
    auto first = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                        1, false, &key_only_columns);
    if (!first.ok || first.rows.empty()) return false;
    auto last = db->StatelessRangeScan(step.table_name(), start_key, end_key,
                                       1, true, &key_only_columns);
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
    std::vector<uint32_t> selected_columns;
    const std::vector<uint32_t>* selected_columns_for_reads =
        selected_columns_for_projected_read(step, selected_columns);
    std::vector<std::thread> workers;
    workers.reserve(worker_count);

    for (unsigned worker_index = 0; worker_index < worker_count;
         ++worker_index) {
        workers.emplace_back([&, worker_index] {
            auto scan_result =
                db->StatelessRangeScan(step.table_name(),
                                       starts[worker_index],
                                       ends[worker_index], 0, false,
                                       selected_columns_for_reads);
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
