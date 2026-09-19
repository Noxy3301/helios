// Executes a read plan: one RPC runs a statement's prefetched point reads and
// scans, where a later step builds its keys from what an earlier one read.

#include "helios_rpc.hh"

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "helios.pb.h"

#include "flat_plan_encode.hh"
#include "parallel_scan.hh"
#include "row_codec.hh"

// Read-plan execution: the tx_execute_read_plan handler and its plan-key
// binding glue.

namespace {

// Pick the source byte string for a binding (from a step's scan key, scan value, or value).
const std::string* select_source_bytes(
    const Helios::Protocol::TxExecuteReadPlan::StepResult& source,
    const Helios::Protocol::TxExecuteReadPlan::KeyBinding& binding,
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
        Helios::Protocol::TxExecuteReadPlan::KeyBinding>& bindings,
    const std::vector<Helios::Protocol::TxExecuteReadPlan::StepResult*>&
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
                    unpack_row_field(*bytes, binding.source_column() - 1);
                if (binding.column_as_int_key()) {
                    scratch =
                        pack_column_as_int_key(extracted,
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

void HeliosRpc::handleTxExecuteReadPlan(
    const Helios::Protocol::TxExecuteReadPlan::Request& request,
    std::string* result) {
    Helios::Protocol::TxExecuteReadPlan::Response response;
    response.set_ok(true);

    std::vector<Helios::Protocol::TxExecuteReadPlan::StepResult*>
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

        if (step.for_each()) {
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
            // Dedup probes: many source rows share a join key, and the plugin
            // serves every runtime probe of one key from the single cached
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
                                  auto scan_result = db->Scan(
                                      step.table_name(), row_key, row_end,
                                      step.scan_limit(), step.reverse_scan(),
                                      nullptr);
                                  if (!scan_result.ok) {
                                    failed[worker_index] = 1;
                                    break;
                                  }
                                    for (auto& r : scan_result.rows) {
                                        out.keys.push_back(std::move(r.key));
                                        out.values.push_back(std::move(r.value));
                                        out.tids.push_back(r.tid);
                                        ++group_rows;
                                    }
                                } else {
                                  auto scan_result = db->ScanIndex(
                                      step.table_name(), step.index_name(),
                                      row_key, row_end, step.scan_limit(),
                                      step.reverse_scan(), nullptr);
                                  if (!scan_result.ok) {
                                    failed[worker_index] = 1;
                                    break;
                                  }
                                    for (auto& r : scan_result.rows) {
                                        out.secondary_keys.push_back(
                                            std::move(r.secondary_key));
                                        out.keys.push_back(
                                            std::move(r.primary_key));
                                        out.values.push_back(std::move(r.value));
                                        out.tids.push_back(r.tid);
                                        ++group_rows;
                                    }
                                }
                                out.group_rows.push_back(group_rows);
                            } else {
                              auto read_result =
                                  db->Read(step.table_name(), row_key, nullptr);
                              out.keys.push_back(row_key);
                              out.tids.push_back(read_result.tid);
                              out.values.push_back(
                                  read_result.found
                                      ? std::move(read_result.value)
                                      : std::string());
                            }
                        }
                        db->ReleaseThreadEpoch();
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
                      auto scan_result = db_manager_->get_database()->Scan(
                          step.table_name(), row_key, row_end,
                          step.scan_limit(), step.reverse_scan(), nullptr);
                      if (!scan_result.ok) {
                        response.set_ok(false);
                        flat_plan::pack(response, *result);
                        return;
                      }
                        for (auto& r : scan_result.rows) {
                            step_result->add_scan_keys(std::move(r.key));
                            step_result->add_scan_values(std::move(r.value));
                            step_result->add_scan_tids(r.tid);
                            ++group_rows;
                        }
                    } else {
                      auto scan_result = db_manager_->get_database()->ScanIndex(
                          step.table_name(), step.index_name(), row_key,
                          row_end, step.scan_limit(), step.reverse_scan(),
                          nullptr);
                      if (!scan_result.ok) {
                        response.set_ok(false);
                        flat_plan::pack(response, *result);
                        return;
                      }
                        for (auto& r : scan_result.rows) {
                            step_result->add_secondary_keys(
                                std::move(r.secondary_key));
                            step_result->add_scan_keys(std::move(r.primary_key));
                            step_result->add_scan_values(std::move(r.value));
                            step_result->add_scan_tids(r.tid);
                            ++group_rows;
                        }
                    }
                    step_result->add_group_sizes(
                        static_cast<uint32_t>(group_rows));
                    step_result->add_group_start_keys(row_key);
                    step_result->add_group_end_keys(row_end);
                    continue;
                }

                auto read_result = db_manager_->get_database()->Read(
                    step.table_name(), row_key, nullptr);
                step_result->add_scan_keys(row_key);
                step_result->add_scan_tids(read_result.tid);
                step_result->add_scan_values(
                    read_result.found ? std::move(read_result.value)
                                      : std::string());
            }
            continue;
        }

        if (!step.is_scan()) {
          auto read_result = db_manager_->get_database()->Read(
              step.table_name(), start_key, nullptr);
          step_result->set_actual_key(start_key);
          step_result->set_actual_start_key(start_key);
          step_result->set_found(read_result.found);
          step_result->set_tid(read_result.tid);
          if (read_result.found) {
            step_result->set_value(std::move(read_result.value));
          }
          continue;
        }

        if (step.index_name().empty()) {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            if (parallel_primary_pax_row_ref_scan(
                    db_manager_->get_database().get(), step, start_key,
                    end_key, step_result)) {
                continue;
            }

            auto scan_result = db_manager_->get_database()->Scan(
                step.table_name(), start_key, end_key, step.scan_limit(),
                step.reverse_scan(), nullptr);
            if (!scan_result.ok) {
                response.set_ok(false);
                flat_plan::pack(response, *result);
                return;
            }
            for (auto& row : scan_result.rows) {
                step_result->add_scan_keys(std::move(row.key));
                step_result->add_scan_values(std::move(row.value));
                step_result->add_scan_tids(row.tid);
            }
        } else {
            step_result->set_actual_start_key(start_key);
            step_result->set_actual_end_key(end_key);
            auto scan_result = db_manager_->get_database()->ScanIndex(
                step.table_name(), step.index_name(), start_key, end_key,
                step.scan_limit(), step.reverse_scan(), nullptr);
            if (!scan_result.ok) {
                response.set_ok(false);
                flat_plan::pack(response, *result);
                return;
            }
            for (auto& row : scan_result.rows) {
                step_result->add_secondary_keys(std::move(row.secondary_key));
                step_result->add_scan_keys(std::move(row.primary_key));
                step_result->add_scan_values(std::move(row.value));
                step_result->add_scan_tids(row.tid);
            }
        }
    }

    flat_plan::pack(response, *result);
}
