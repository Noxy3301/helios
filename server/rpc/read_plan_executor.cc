#include "lineairdb_rpc.hh"

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../../common/log.h"
#include "lineairdb.pb.h"

#include "aggregate_executor.hh"
#include "flat_plan_encode.hh"
#include "parallel_scan.hh"
#include "predicate_evaluator.hh"
#include "row_codec.hh"

// Read-plan execution: the TX_EXECUTE_READ_PLAN handler and its plan-key
// binding glue. Runs staged scans, semijoin probes, and server aggregation.

namespace {

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

const std::vector<uint32_t>* selected_columns_for_materialization(
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
    for (const auto& semijoin : step.semijoins()) {
        selected_columns.push_back(semijoin.probe_column());
    }
    std::sort(selected_columns.begin(), selected_columns.end());
    selected_columns.erase(
        std::unique(selected_columns.begin(), selected_columns.end()),
        selected_columns.end());
    if (step.projection().num_columns() > 0 &&
        selected_columns.size() >= step.projection().num_columns()) {
        return nullptr;
    }
    return &selected_columns;
}

}  // namespace

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
        const bool step_has_filter =
            step.has_filter() && step.filter().has_expr();
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

        const bool step_has_projection = step.has_projection();
        std::vector<uint32_t> selected_columns;
        const std::vector<uint32_t>* selected_columns_for_reads =
            selected_columns_for_materialization(step, selected_columns);

        // PAX-backed reads can materialize only the selected payloads needed
        // for server-side filtering and final projection while keeping the
        // full row shape. Projection then trims emitted VALUES to the kept
        // columns; malformed rows fail the plan instead of mixing full and
        // projected layouts.
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

            // Build membership sets from earlier source steps, then drop
            // probe rows whose join key is absent.
            struct FeSemijoin {
                std::unordered_set<std::string> keys;
                uint32_t probe_column;
            };
            std::vector<FeSemijoin> fe_semijoins;
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
            auto sj_reject = [&](const std::string& value) -> bool {
                for (const auto& fsj : fe_semijoins) {
                    auto col = extract_value_column(value, fsj.probe_column);
                    if (fsj.keys.find(std::string(col)) == fsj.keys.end())
                        return true;  // no join partner -> drop
                }
                return false;
            };

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
                                            step.reverse_scan(),
                                            selected_columns_for_reads);
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
                                            step.reverse_scan(),
                                            selected_columns_for_reads);
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
                                                      row_key,
                                                      selected_columns_for_reads);
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
                                selected_columns_for_reads);
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
                                    step.reverse_scan(),
                                    selected_columns_for_reads);
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
                        step.table_name(), row_key,
                        selected_columns_for_reads);
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
                    step.table_name(), start_key, selected_columns_for_reads);
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
                if (parallel_primary_pax_row_ref_scan(
                        db_manager_->get_database().get(), step, start_key,
                        end_key, step_result, projection_failed)) {
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
                    scan_limit_for_lineairdb, step.reverse_scan(),
                    selected_columns_for_reads);
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
                                                   scan_result.rows, step_result);
            }
            if (!aggregated) {
                uint64_t emitted = 0;
                for (auto& row : scan_result.rows) {
                    if (!row_passes(row.value)) {
                        // Negative coverage for point probes into this scan.
                        step_result->add_filtered_keys(std::move(row.key));
                        continue;
                    }
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
                    step.scan_limit(), step.reverse_scan(),
                    selected_columns_for_reads);
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
