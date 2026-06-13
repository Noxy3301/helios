#include "lineairdb_transaction.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "lineairdb_keyenc.hh"
#include "../common/log.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

#include <thread>
#include <unordered_set>

namespace {

// Composite key for the scan-cache exact-start lookup indexes.
inline std::string scan_cache_index_key(const std::string& table,
                                        const std::string& index,
                                        const std::string& start) {
  std::string k;
  k.reserve(table.size() + index.size() + start.size() + 2);
  k += table;
  k.push_back('\x01');
  k += index;
  k.push_back('\x01');
  k += start;
  return k;
}

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

std::string trace_count_event(const char* kind, const std::string& table_name,
                              size_t count) {
  return std::string(kind) + ":" + table_name + ":n=" +
         std::to_string(count);
}

std::string trace_plan_scan_event(const std::string& table_name,
                                  const std::string& index_name,
                                  size_t keys, size_t values,
                                  uint64_t limit, bool for_each) {
  std::string event = "plan_fetch:S:" + table_name;
  if (!index_name.empty()) event += ":" + index_name;
  event += ":keys=" + std::to_string(keys);
  event += ":vals=" + std::to_string(values);
  if (limit > 0) event += ":lim=" + std::to_string(limit);
  if (for_each) event += ":each";
  return event;
}

}  // namespace

LineairDBTransaction::LineairDBTransaction(THD* thd, 
                                            LineairDBProxy* lineairdb_proxy,
                                            handlerton* lineairdb_hton,
                                            bool isFence) 
    : tx_id(-1), 
      lineairdb_proxy(lineairdb_proxy),
      thread(thd), 
      isTransaction(false), 
      hton(lineairdb_hton),
      isFence(isFence),
      is_aborted_(false)
    {}

std::string LineairDBTransaction::get_selected_table_name() { return db_table_key; }

void LineairDBTransaction::choose_table(std::string db_table_name) {
  if (db_table_key != db_table_name) {
    pushed_filter_.clear();
  }
  db_table_key = db_table_name;
}

bool LineairDBTransaction::table_is_not_chosen() {
  if (db_table_key.size() == 0) {
    LOG_WARNING("Database and Table is not chosen in LineairDBTransaction");
    return true;
  }
  return false;
}

const std::pair<const std::byte *const, const size_t>
LineairDBTransaction::read(std::string key) {
  if (table_is_not_chosen()) return std::pair<const std::byte *const, const size_t>{nullptr, 0};

  // Silo-style local view: own writes are visible before remote reads
  if (auto entry = lookup_write_set(db_table_key, key)) {
    rpc_trace_.record_local_view("read_write_hit");
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Repeat exact-key reads can use the local read set
  if (auto entry = lookup_row_cache(db_table_key, key)) {
    rpc_trace_.record_local_view("read_cache_hit");
    // A consumed cache hit appends to the point read set (staged rows only).
    if (entry->validate_on_use) {
      rpc_trace_.record_local_view(
          trace_count_event("use_point_read", entry->table_name, 1));
      append_base_row_read(entry->table_name, entry->key,
                                   entry->found, entry->tid);
    }
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Normal path misses go to the server; prefetch plans must prefetch them
  rpc_trace_.record_local_view("read_miss");
  if (prefetch_mode_) {
    abort_prefetch_cache_miss("read");
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  last_read_value_ = lineairdb_proxy->tx_read(this, key);
  if (last_read_value_.empty()) {
    record_row_cache(db_table_key, key, false, ""); // value unused when not found
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  record_row_cache(db_table_key, key, true, last_read_value_);

  return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
}

std::vector<std::pair<bool, std::string>>
LineairDBTransaction::batch_read(const std::vector<std::string>& keys) {
  if (table_is_not_chosen()) return {};

  std::vector<std::pair<bool, std::string>> pairs;
  pairs.resize(keys.size());

  std::vector<std::string> rpc_keys;
  std::vector<size_t> rpc_positions;
  rpc_keys.reserve(keys.size());
  rpc_positions.reserve(keys.size());

  // Resolve keys covered by the local read/write sets first
  for (size_t i = 0; i < keys.size(); ++i) {
    if (auto entry = lookup_write_set(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_write_hit");
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    if (auto entry = lookup_row_cache(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_cache_hit");
      if (entry->validate_on_use) {
        rpc_trace_.record_local_view(
            trace_count_event("use_point_read", entry->table_name, 1));
        append_base_row_read(entry->table_name, entry->key,
                                     entry->found, entry->tid);
      }
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    rpc_trace_.record_local_view("batch_miss");
    rpc_positions.push_back(i);
    rpc_keys.push_back(keys[i]);
  }

  // Prefetch plans must fetch every key up front; misses mean the plan is short
  if (prefetch_mode_ && !rpc_keys.empty()) {
    abort_prefetch_cache_miss("batch_read");
    return pairs;
  }

  // Fetch only cache misses; tx_batch_read() returns rows in rpc_keys order
  //   Example: keys=[A,B,C], B is local -> rpc_keys=[A,C],
  //            rpc_positions=[0,2], so RPC results fill pairs[0] and pairs[2].
  if (!rpc_keys.empty()) {
    auto results = lineairdb_proxy->tx_batch_read(this, rpc_keys);
    if (results.size() != rpc_keys.size()) {
      rpc_trace_.record_local_view("abort_batch_size_mismatch");
      is_aborted_ = true;
      return pairs;
    }
    for (size_t i = 0; i < results.size(); ++i) {
      // Map each RPC result back to the original keys[] position
      const size_t pos = rpc_positions[i];
      pairs[pos] = {results[i].found, std::move(results[i].value)};
      record_row_cache(db_table_key, keys[pos], pairs[pos].first, pairs[pos].second);
    }
  }
  return pairs;
}

void LineairDBTransaction::prefetch_stateless_reads(
    const std::vector<LineairDBProxy::StatelessReadKey>& reads) {
  if (!prefetch_mode_ || reads.empty()) return;

  std::vector<LineairDBProxy::StatelessReadKey> rpc_reads;
  rpc_reads.reserve(reads.size());
  std::unordered_set<std::string> seen;
  seen.reserve(reads.size());

  // Keep the plan prefetch to rows not already covered by the local view
  for (const auto& read : reads) {
    const std::string seen_key = read.table_name + '\0' + read.key;
    if (!seen.insert(seen_key).second) continue;
    if (lookup_write_set(read.table_name, read.key)) continue;
    if (lookup_row_cache(read.table_name, read.key)) continue;
    rpc_reads.push_back(read);
  }

  if (rpc_reads.empty()) return;

  auto results = lineairdb_proxy->tx_stateless_batch_read(rpc_reads);
  if (results.size() != rpc_reads.size()) {
    rpc_trace_.record_local_view("abort_prefetch_size_mismatch");
    is_aborted_ = true;
    return;
  }

  // Store prefetched rows in the local cache; validate them only if MySQL reads them
  for (size_t i = 0; i < rpc_reads.size(); ++i) {
    const auto& read = rpc_reads[i];
    auto& result = results[i];
    if (!result.ok) {
      rpc_trace_.record_local_view("abort_prefetch_rpc");
      is_aborted_ = true;
      return;
    }
    if (result.found) {
      record_row_cache(read.table_name, read.key, true, result.value,
                        result.tid, true);
    } else {
      record_row_cache(read.table_name, read.key, false, "",
                        result.tid, true); // value unused when not found
    }
  }
}

void LineairDBTransaction::execute_read_plan(
    const std::vector<LineairDBProxy::ReadPlanStep>& full_steps) {
  if (!prefetch_mode_ || full_steps.empty()) return;

  // Drop exact point steps whose row is already in the local view: the
  // SELECT ... FOR UPDATE -> UPDATE pattern re-stages the same row once per
  // statement otherwise (TPC-C: one redundant RPC per stock/district/
  // warehouse/customer write). Serving the later statement from the cached
  // row is exactly what the runtime cache-hit path does anyway, and its TID
  // is validated at commit. Bound steps resolve keys at runtime and scans
  // have coverage semantics, so only constant-key scalar reads are dropped.
  // Steps referenced by a later step's bindings or semijoins must survive
  // the covered-drop below AND keep resolvable indices: dropping shifts the
  // array, so source_step references are remapped at the end. (Latent in the
  // original F1: TPC-C/TPC-H plans never dropped a referenced step, but the
  // semijoin planner makes cross-references common.)
  std::vector<bool> referenced(full_steps.size(), false);
  for (const auto& step : full_steps) {
    for (const auto& b : step.bindings) {
      if (b.source_step < referenced.size()) referenced[b.source_step] = true;
    }
    for (const auto& b : step.end_bindings) {
      if (b.source_step < referenced.size()) referenced[b.source_step] = true;
    }
    for (const auto& sj : step.semijoins) {
      if (sj.source_step < referenced.size())
        referenced[sj.source_step] = true;
    }
  }

  std::vector<LineairDBProxy::ReadPlanStep> steps;
  steps.reserve(full_steps.size());
  std::vector<uint32_t> new_index(full_steps.size(), 0);
  size_t covered = 0;
  for (size_t si = 0; si < full_steps.size(); ++si) {
    const auto& step = full_steps[si];
    const bool constant_point_read = !step.is_scan && !step.for_each &&
                                     step.bindings.empty() &&
                                     step.end_bindings.empty() &&
                                     !step.key_prefix.empty();
    if (constant_point_read && !referenced[si] &&
        (lookup_write_set(step.table_name, step.key_prefix) ||
         lookup_row_cache(step.table_name, step.key_prefix))) {
      ++covered;
      continue;
    }
    new_index[si] = static_cast<uint32_t>(steps.size());
    steps.push_back(step);
  }
  if (covered > 0) {
    for (auto& step : steps) {
      for (auto& b : step.bindings) b.source_step = new_index[b.source_step];
      for (auto& b : step.end_bindings)
        b.source_step = new_index[b.source_step];
      for (auto& sj : step.semijoins)
        sj.source_step = new_index[sj.source_step];
    }
  }
  if (covered > 0) {
    rpc_trace_.record_section_count("plan_steps_covered", covered);
  }
  if (steps.empty()) return;  // everything already staged: no RPC needed

  // Aggregation pushdown: stamp the spec onto the matching primary scan and
  // disable that step's projection — the spec addresses group/arg columns by
  // ORIGINAL field index, so the server must parse full rows.
  if (!pushed_aggregate_.empty()) {
    for (auto& s : steps) {
      if (s.is_scan && !s.for_each && s.index_name.empty() &&
          s.table_name == db_table_key) {
        s.aggregate_serialized = pushed_aggregate_;
        // The statement's WHERE rides as the step filter so the server
        // aggregates exactly the rows MySQL would have kept.
        if (s.serialized_filter.empty() && !pushed_filter_.empty()) {
          s.serialized_filter = pushed_filter_;
        }
        s.projection.clear();
        s.projection_num_columns = 0;
      }
    }
  }

  // Negative-coverage suppression: a filtered scan ships filtered_keys (the
  // rejected rows' keys) ONLY so a later point probe into that same table can
  // answer not-found locally. If the table is read by no other step, no such
  // probe exists, and those keys are pure transfer/decode waste (q14: lineitem
  // ships 593k rejected keys / ~9.5MB nobody consumes). Skipping can never make
  // a filtered-out row visible; worst case, a same-table point consumer the
  // planner failed to foresee aborts the prefetch (fail-closed, loud) instead
  // of answering not-found locally. The single-step guard means no such
  // consumer exists. An aggregate-stamped step ships group rows, not base
  // rows, and never emits filtered_keys, so leave it alone.
  {
    std::unordered_map<std::string, int> table_step_count;
    for (const auto& s : steps) table_step_count[s.table_name]++;
    for (auto& s : steps) {
      if (s.is_scan && s.aggregate_serialized.empty() &&
          !s.serialized_filter.empty() &&
          table_step_count[s.table_name] == 1) {
        s.suppress_filtered_keys = true;
      }
    }
  }

  rpc_trace_.record_local_view("plan_request:steps=" +
                               std::to_string(steps.size()));
  LineairDBProxy::ReadPlanResult result;
  {
    // Overlapping span: contains the TX_EXECUTE_READ_PLAN RPC (recorded
    // separately in summary_by_type) plus request build + flat-codec decode.
    // Aggregators must not add this into a non-RPC sections sum; decode-only
    // time = this section minus the RPC entry.
    SectionTimer rpc_decode_timer(&rpc_trace_, "stage_rpc_and_decode");
    result = lineairdb_proxy->tx_execute_read_plan(steps);
  }
  if (!result.ok || result.steps.size() != steps.size()) {
    rpc_trace_.record_local_view("abort_read_plan_rpc");
    is_aborted_ = true;
    return;
  }

  SectionTimer stage_local_timer(&rpc_trace_, "stage_local");
  if (rpc_trace_.active()) {
    uint64_t staged_rows = 0;
    for (size_t i = 0; i < result.steps.size(); ++i) {
      // Scalar point steps carry their row in found/value, not scan_keys.
      staged_rows += (!steps[i].is_scan && !steps[i].for_each)
                         ? 1
                         : static_cast<uint64_t>(
                               result.steps[i].scan_keys.size());
    }
    rpc_trace_.record_section_count("staged_rows", staged_rows);
  }

  // Staging consumes `result` destructively: each step's payload is moved
  // into the caches and released before the next step is staged, so the
  // transient footprint is one copy of a step, not two copies of the whole
  // response (multi-GB at TPC-H SF=1).
  for (size_t i = 0; i < result.steps.size() && i < steps.size(); ++i) {
    const auto& step = steps[i];
    auto& step_result = result.steps[i];

    if (!step.is_scan && !step.for_each) {
      rpc_trace_.record_local_view(trace_count_event(
          step_result.found ? "plan_fetch:R:hit" : "plan_fetch:R:miss",
          step.table_name, 1));
      if (step_result.found) {
        record_row_cache(step.table_name, step_result.actual_key, true,
                          step_result.value, step_result.tid, true);
      } else {
        record_row_cache(step.table_name, step_result.actual_key, false, "",
                          step_result.tid, true);
      }
      continue;
    }

    rpc_trace_.record_local_view(trace_plan_scan_event(
        step.table_name, step.index_name, step_result.scan_keys.size(),
        step_result.scan_values.size(), step.scan_limit, step.for_each));

    if (step.for_each && step.is_scan) {
      // FER/FES probe groups: stage one bounded scan entry per (deduplicated)
      // probe so the runtime per-outer-row range lookups hit the cache.
      size_t flat = 0;  // index into the flat arrays
      for (size_t g = 0; g < step_result.group_sizes.size(); ++g) {
        const size_t n = step_result.group_sizes[g];
        const std::string& gstart = g < step_result.group_start_keys.size()
                                        ? step_result.group_start_keys[g]
                                        : std::string();
        const std::string& gend = g < step_result.group_end_keys.size()
                                      ? step_result.group_end_keys[g]
                                      : std::string();
        if (step.index_name.empty()) {
          LocalRangeScanEntry entry;
          entry.table_name = step.table_name;
          entry.start_key = gstart;
          entry.end_key = gend;
          entry.reverse_scan = step.reverse_scan;
          entry.row_limit = step.scan_limit;
          for (size_t j = flat; j < flat + n && j < step_result.scan_keys.size();
               ++j) {
            std::string key = std::move(step_result.scan_keys[j]);
            std::string value = j < step_result.scan_values.size()
                                    ? std::move(step_result.scan_values[j])
                                    : std::string();
            const uint64_t tid =
                j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
            const bool found = !value.empty();
            record_row_cache(step.table_name, key, found, value, tid, true);
            if (found) {
              entry.rows.emplace_back(std::move(key), std::move(value));
              entry.row_tids.push_back(tid);
            }
          }
          push_range_scan_cache(std::move(entry));
        } else {
          LocalSecondaryScanEntry entry;
          entry.table_name = step.table_name;
          entry.index_name = step.index_name;
          entry.start_key = gstart;
          entry.end_key = gend;
          entry.reverse_scan = step.reverse_scan;
          entry.row_limit = step.scan_limit;
          for (size_t j = flat; j < flat + n && j < step_result.scan_keys.size();
               ++j) {
            std::string key = std::move(step_result.scan_keys[j]);
            std::string value = j < step_result.scan_values.size()
                                    ? std::move(step_result.scan_values[j])
                                    : std::string();
            const uint64_t tid =
                j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
            record_row_cache(step.table_name, key, !value.empty(), value, tid,
                             true);
            if (j < step_result.secondary_keys.size()) {
              entry.secondary_keys.push_back(
                  std::move(step_result.secondary_keys[j]));
            }
            entry.primary_keys.push_back(std::move(key));
          }
          push_secondary_scan_cache(std::move(entry));
        }
        flat += n;
      }
      step_result = LineairDBProxy::ReadPlanStepResult{};
      continue;
    }

    if (step.for_each) {
      // Point probes: rows go to the row cache only.
      for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
        const std::string& key = step_result.scan_keys[j];
        const std::string value = j < step_result.scan_values.size()
                                      ? std::move(step_result.scan_values[j])
                                      : std::string();
        const uint64_t tid =
            j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
        record_row_cache(step.table_name, key, !value.empty(), value, tid,
                         true);
      }
      step_result = LineairDBProxy::ReadPlanStepResult{};
      continue;
    }

    if (step.index_name.empty()) {
      // An aggregate-stamped step returns synthetic GROUP rows (empty keys):
      // they must never enter the ROW cache (their keys are not base keys,
      // and the step's filter is the INNER unit's WHERE — its rejections are
      // no statement about what other consumers of the table may read), and
      // the range entry is flagged so only the aggregate's own consuming
      // scan can be served from it.
      const bool agg_step = !step.aggregate_serialized.empty();
      // q18 unification: if this agg step is a grouped-semijoin's source over
      // table T, cache its group rows so the inner GroupedSummary serving of T
      // reuses them instead of re-aggregating (one full scan, not two).
      bool gs_agg_step = false;
      if (agg_step) {
        for (const auto& g : grouped_semijoins_)
          if (g.inner_table_key == step.table_name) { gs_agg_step = true; break; }
      }
      std::vector<std::string> gs_group_cache;
      std::vector<std::pair<std::string, std::string>> rows;
      std::vector<uint64_t> row_tids;
      rows.reserve(step_result.scan_keys.size());
      row_tids.reserve(step_result.scan_keys.size());
      for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
        std::string key = std::move(step_result.scan_keys[j]);
        std::string value = j < step_result.scan_values.size()
                                ? std::move(step_result.scan_values[j])
                                : std::string();
        const uint64_t tid =
            j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
        const bool found = !value.empty();
        if (!agg_step) {
          record_row_cache(step.table_name, key, found, value, tid, true);
        }
        if (found) {
          if (gs_agg_step) gs_group_cache.push_back(value);
          rows.emplace_back(std::move(key), std::move(value));
          row_tids.push_back(tid);
        }
      }
      if (gs_agg_step)
        cache_grouped_semijoin_groups(step.table_name, std::move(gs_group_cache));
      LocalRangeScanEntry scan_entry{
          step.table_name, step_result.actual_start_key,
          step_result.actual_end_key, step.reverse_scan, step.scan_limit,
          std::move(rows), std::move(row_tids)};
      scan_entry.aggregate_rows = agg_step;
      push_range_scan_cache(std::move(scan_entry));
      // Negative coverage for the step filter's rejected rows: point probes
      // into this filtered scan resolve to not-found locally instead of
      // aborting on a cache miss (sound per alias — the filter is that
      // alias's WHERE conjunct, so MySQL would discard the row anyway).
      if (!agg_step) {
        for (auto& fk : step_result.filtered_keys) {
          record_row_cache(step.table_name, fk, false, "", 0, true);
        }
      }
    } else {
      // Secondary scan: primary_keys must stay 1:1 aligned with
      // secondary_keys (including not-found base rows), as the cache slicing
      // walks the pairs together.
      LocalSecondaryScanEntry cached;
      cached.table_name = step.table_name;
      cached.index_name = step.index_name;
      cached.start_key = step_result.actual_start_key;
      cached.end_key = step_result.actual_end_key;
      cached.reverse_scan = step.reverse_scan;
      cached.row_limit = step.scan_limit;
      cached.secondary_keys.reserve(step_result.secondary_keys.size());
      for (auto& key : step_result.secondary_keys) {
        cached.secondary_keys.push_back(std::move(key));
      }
      cached.primary_keys.reserve(step_result.scan_keys.size());
      for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
        std::string key = std::move(step_result.scan_keys[j]);
        std::string value = j < step_result.scan_values.size()
                                ? std::move(step_result.scan_values[j])
                                : std::string();
        const uint64_t tid =
            j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
        record_row_cache(step.table_name, key, !value.empty(), value, tid,
                         true);
        cached.primary_keys.push_back(std::move(key));
      }
      push_secondary_scan_cache(std::move(cached));
      // See the primary branch: negative coverage by primary key.
      for (auto& fk : step_result.filtered_keys) {
        record_row_cache(step.table_name, fk, false, "", 0, true);
      }
    }
    step_result = LineairDBProxy::ReadPlanStepResult{};
  }
}

bool LineairDBTransaction::batch_write(
    const std::string& table_name,
    const std::vector<LineairDBProxy::BatchOp>& ops) {
  if (prefetch_mode_) {
    for (auto op : ops) {
      if (op.table_name.empty()) op.table_name = table_name;
      if (op.type == LineairDBProxy::BatchOp::Type::Write) {
        record_write(op.table_name, op.key, true, op.value);
      } else if (op.type == LineairDBProxy::BatchOp::Type::Delete) {
        record_write(op.table_name, op.key, false, ""); // value unused when not found
      } else if (op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexWrite ||
                 op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexDelete) {
        drop_secondary_scan_cache(op.table_name, op.index_name);
      }
      write_buffer_ops_.push_back(std::move(op));
    }
    return true;
  }

  return lineairdb_proxy->tx_batch_write(this, table_name, ops);
}

std::vector<std::string>
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};
  if (!fallback_to_normal_transaction("get_all_keys")) return {};
  flush_write_buffer_for_table(db_table_key);

  auto key_value_pairs = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, "");

  std::vector<std::string> keyList;
  for (const auto& kv : key_value_pairs) {
    keyList.push_back(kv.key);
  }

  return keyList;
}

std::vector<std::string>
LineairDBTransaction::get_matching_keys(std::string first_key_part) {
  if (table_is_not_chosen()) return {};
  if (!fallback_to_normal_transaction("get_matching_keys")) return {};
  flush_write_buffer_for_table(db_table_key);

  auto key_value_pairs = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, first_key_part);

  std::vector<std::string> keyList;
  for (const auto& kv : key_value_pairs) {
    keyList.push_back(kv.key);
  }

  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;
  if (prefetch_mode_) {
    buffer_write(db_table_key, key, value);
    return true;
  }

  const bool ok = lineairdb_proxy->tx_write(this, key, value);
  if (ok) record_write(db_table_key, key, true, value);
  return ok;
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  if (prefetch_mode_) {
    buffer_delete(db_table_key, key);
    return true;
  }

  const bool ok = lineairdb_proxy->tx_delete(this, key);
  if (ok) record_write(db_table_key, key, false, ""); // value unused when not found
  return ok;
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key) {
  if (table_is_not_chosen()) return {};
  if (prefetch_mode_) {
    const std::string end_key = next_lexicographic_key(secondary_key);
    if (end_key.empty()) {
      abort_prefetch_cache_miss("secondary point range end");
      return {};
    }
    return get_matching_primary_keys_in_range(index_name, secondary_key,
                                              end_key);
  }

  if (!fallback_to_normal_transaction("read_secondary_index")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_read_secondary_index(this, index_name, secondary_key);
}

bool LineairDBTransaction::write_secondary_index(std::string index_name,
                                                 std::string secondary_key,
                                                 const std::string primary_key) {
  if (table_is_not_chosen()) return false;
  if (prefetch_mode_) {
    buffer_write_secondary_index(db_table_key, index_name, secondary_key,
                                 primary_key);
    return true;
  }

  return lineairdb_proxy->tx_write_secondary_index(this, index_name, secondary_key, primary_key);
}

bool LineairDBTransaction::delete_secondary_index(std::string index_name,
                                                  std::string secondary_key,
                                                  const std::string primary_key) {
  if (table_is_not_chosen()) return false;
  if (prefetch_mode_) {
    buffer_delete_secondary_index(db_table_key, index_name, secondary_key,
                                  primary_key);
    return true;
  }

  return lineairdb_proxy->tx_delete_secondary_index(this, index_name, secondary_key, primary_key);
}

bool LineairDBTransaction::update_secondary_index(std::string index_name,
                                                  std::string old_secondary_key,
                                                  std::string new_secondary_key,
                                                  const std::string primary_key) {
  if (table_is_not_chosen()) return false;
  if (prefetch_mode_) {
    buffer_delete_secondary_index(db_table_key, index_name, old_secondary_key,
                                  primary_key);
    buffer_write_secondary_index(db_table_key, index_name, new_secondary_key,
                                 primary_key);
    return true;
  }

  return lineairdb_proxy->tx_update_secondary_index(this, index_name, old_secondary_key, new_secondary_key, primary_key);
}

// Primary key scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_keys_in_range(std::string start_key,
                                                 std::string end_key) {
  if (table_is_not_chosen()) return {};
  if (prefetch_mode_) {
    if (auto cached =
            lookup_range_scan_cache(db_table_key, start_key, end_key, false, 0)) {
      std::vector<std::pair<std::string, std::string>> rows = cached->rows;
      rpc_trace_.record_local_view(
          trace_count_event("use_pk_key_scan", db_table_key, rows.size()));
      // Keys-only consumption: membership/order is guarded by the range
      // replay below, and key bytes cannot change without a delete+insert
      // (which the replay catches). Row VALUES are not returned here; a later
      // value read goes through read(), whose cache hit appends the per-row
      // TID validation on use. Per-row base appends here would re-validate
      // every row in the range for no extra guarantee.
      //
      // Assemble the range read from the pre-merge cached rows: server-side
      // re-walk at commit cannot see this tx's pending writes, and
      // validating against the post-merge view would false-abort on every
      // own-insert / own-delete in range.
      append_range_read(*cached);

      merge_pending_rows_into_range_scan(rows, start_key, end_key, false);
      std::vector<std::string> keys;
      keys.reserve(rows.size());
      for (const auto& row : rows) keys.push_back(row.first);
      return keys;
    }

    abort_prefetch_cache_miss("primary key scan");
    return {};
  }

  if (!fallback_to_normal_transaction("get_matching_keys_in_range")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_keys_in_range(this, start_key, end_key);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_range(std::string start_key,
                                                            std::string end_key,
                                                            uint64_t row_limit,
                                                            bool reverse_scan,
                                                            bool *served_truncated) {
  if (served_truncated != nullptr) *served_truncated = false;
  if (table_is_not_chosen()) return {};
  if (prefetch_mode_) {
    // Unbounded-upper: map an empty end to the sentinel the scan was staged with
    // so the [start, sentinel) slice keeps every row (see scan_end_sentinel).
    if (end_key.empty()) end_key = lineairdb_keyenc::scan_end_sentinel();
    if (auto cached = lookup_range_scan_cache(
            db_table_key, start_key, end_key, reverse_scan, row_limit,
            /*allow_truncated=*/served_truncated != nullptr)) {
      if (served_truncated != nullptr) *served_truncated = cached->truncated;
      std::vector<std::pair<std::string, std::string>> pairs = cached->rows;
      // Values are returned (and thus read) only for rows inside the limit
      // window; rows sliced away below were never observed by the statement,
      // so their TIDs need no validation — the range replay still guards
      // membership/order of the whole staged range.
      const size_t validated_rows =
          (row_limit > 0)
              ? std::min<size_t>(row_limit, cached->rows.size())
              : cached->rows.size();
      for (size_t i = 0; i < validated_rows && i < cached->row_tids.size();
           ++i) {
        append_base_row_read(db_table_key, cached->rows[i].first, true,
                              cached->row_tids[i]);
      }
      // See get_matching_keys_in_range above for the rationale.
      append_range_read(*cached);

      merge_pending_rows_into_range_scan(pairs, start_key, end_key,
                                         reverse_scan);
      if (row_limit > 0 && pairs.size() > row_limit) {
        pairs.resize(static_cast<size_t>(row_limit));
      }
      rpc_trace_.record_local_view(
          trace_count_event("use_pk_value_scan", db_table_key, pairs.size()));
      return pairs;
    }

    abort_prefetch_cache_miss("primary value scan");
    return {};
  }

  if (!fallback_to_normal_transaction("get_matching_keys_and_values_in_range")) return {};
  const bool can_merge_local_rows = (row_limit == 0 && pushed_filter_.empty());
  // LIMIT / pushed filter scans must see only server-filtered rows
  if (!can_merge_local_rows) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_in_range(
      this, start_key, end_key, row_limit, reverse_scan);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  // Merge unflushed own writes after the server has validated the range
  if (can_merge_local_rows) {
    merge_pending_rows_into_range_scan(pairs, start_key, end_key, reverse_scan);
  }
  return pairs;
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_from_prefix(std::string prefix) {
  if (table_is_not_chosen()) return {};
  if (prefetch_mode_) {
    if (prefix.empty()) {
      // Full table scan: serve from a staged ["", sentinel) range
      // (in_range maps the empty end key to the sentinel).
      return get_matching_keys_and_values_in_range("", std::string());
    }
    const std::string prefix_end = next_lexicographic_key(prefix);
    if (prefix_end.empty()) {
      abort_prefetch_cache_miss("primary prefix range end");
      return {};
    }
    return get_matching_keys_and_values_in_range(prefix, prefix_end);
  }

  if (!fallback_to_normal_transaction("get_matching_keys_and_values_from_prefix")) return {};
  const bool can_merge_local_rows = pushed_filter_.empty();
  // Pushed filter scans must see only server-filtered rows
  if (!can_merge_local_rows) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, prefix);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  // Merge unflushed own writes after the server has validated the prefix
  if (can_merge_local_rows) {
    merge_pending_rows_into_prefix_scan(pairs, prefix);
  }
  return pairs;
}

std::optional<std::string>
LineairDBTransaction::fetch_last_key_in_range(const std::string &start_key,
                                              const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (!fallback_to_normal_transaction("fetch_last_key_in_range")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_key_in_range(this, start_key, end_key);
}

std::optional<std::string>
LineairDBTransaction::fetch_first_key_with_prefix(const std::string &prefix,
                                                  const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  if (!fallback_to_normal_transaction("fetch_first_key_with_prefix")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_first_key_with_prefix(this, prefix, prefix_end);
}

std::optional<std::string>
LineairDBTransaction::fetch_next_key_with_prefix(const std::string &last_key,
                                                 const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  if (!fallback_to_normal_transaction("fetch_next_key_with_prefix")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_next_key_with_prefix(this, last_key, prefix_end);
}

// Secondary index scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_in_range(std::string index_name,
                                                         std::string start_key,
                                                         std::string end_key,
                                                         uint64_t row_limit,
                                                         bool reverse_scan) {
  if (table_is_not_chosen()) return {};
  if (prefetch_mode_) {
    if (has_pending_secondary_ops_for_index(db_table_key, index_name)) {
      abort_prefetch_cache_miss("secondary scan after secondary write");
      return {};
    }

    if (end_key.empty()) end_key = lineairdb_keyenc::scan_end_sentinel();
    auto cached = lookup_secondary_scan_cache(
        db_table_key, index_name, start_key, end_key, reverse_scan, row_limit);
    if (!cached && row_limit != 0) {
      // A limited request (e.g. DESC LIMIT 1 prefix-last) is also covered by
      // an unlimited staged scan of the same range: the full key set contains
      // the limited one and the caller positions on the tail itself.
      cached = lookup_secondary_scan_cache(db_table_key, index_name, start_key,
                                           end_key, false, 0);
    }
    if (cached) {
      rpc_trace_.record_local_view("use_si_scan:" + db_table_key + ":" +
                                   index_name + ":n=" +
                                   std::to_string(cached->primary_keys.size()));
      append_secondary_range_read(*cached);
      return cached->primary_keys;
    }

    abort_prefetch_cache_miss("secondary scan");
    return {};
  }

  if (!fallback_to_normal_transaction("get_matching_primary_keys_in_range")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_primary_keys_in_range(this, index_name, start_key, end_key);
}

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_from_prefix(std::string index_name,
                                                            std::string prefix) {
  if (table_is_not_chosen()) return {};
  if (prefetch_mode_) {
    const std::string prefix_end = next_lexicographic_key(prefix);
    if (prefix_end.empty()) {
      abort_prefetch_cache_miss("secondary prefix range end");
      return {};
    }
    return get_matching_primary_keys_in_range(index_name, prefix, prefix_end);
  }

  if (!fallback_to_normal_transaction("get_matching_primary_keys_from_prefix")) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_primary_keys_from_prefix(this, index_name, prefix);
}

std::optional<std::string>
LineairDBTransaction::fetch_last_primary_key_in_secondary_range(const std::string &index_name,
                                                                const std::string &start_key,
                                                                const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (!fallback_to_normal_transaction("fetch_last_primary_key_in_secondary_range")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_primary_key_in_secondary_range(this, index_name, start_key, end_key);
}

std::optional<SecondaryIndexEntry>
LineairDBTransaction::fetch_last_secondary_entry_in_range(const std::string &index_name,
                                                          const std::string &start_key,
                                                          const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (!fallback_to_normal_transaction("fetch_last_secondary_entry_in_range")) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_secondary_entry_in_range(this, index_name, start_key, end_key);
}

// Row count delta tracking

void LineairDBTransaction::add_rowcount_delta(LineairDB_share *share,
                                              const std::string &table_name,
                                              int64_t delta) {
  if (share == nullptr || delta == 0) return;

  for (auto &entry : rowcount_deltas_) {
    if (entry.share == share) {
      entry.delta += delta;
      return;
    }
  }

  rowcount_deltas_.push_back({share, table_name, delta});
}

int64_t
LineairDBTransaction::peek_rowcount_delta(const LineairDB_share *share) const {
  if (share == nullptr) return 0;

  for (const auto &entry : rowcount_deltas_) {
    if (entry.share == share)
      return entry.delta;
  }

  return 0;
}

void LineairDBTransaction::buffer_write(const std::string& table_name,
                                        const std::string& key,
                                        const std::string& value) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::Write;
  op.key = key;
  op.value = value;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  record_write(table_name, key, true, value);

  if (!prefetch_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_write_secondary_index(const std::string& table_name,
                                                        const std::string& index_name,
                                                        const std::string& secondary_key,
                                                        const std::string& primary_key) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
  op.index_name = index_name;
  op.secondary_key = secondary_key;
  op.primary_key = primary_key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  drop_secondary_scan_cache(table_name, index_name);

  if (!prefetch_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_delete(const std::string& table_name,
                                         const std::string& key) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::Delete;
  op.key = key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  record_write(table_name, key, false, ""); // value unused when not found

  if (!prefetch_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_delete_secondary_index(
    const std::string& table_name,
    const std::string& index_name,
    const std::string& secondary_key,
    const std::string& primary_key) {
  LineairDBProxy::BatchOp op;
  op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexDelete;
  op.index_name = index_name;
  op.secondary_key = secondary_key;
  op.primary_key = primary_key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  drop_secondary_scan_cache(table_name, index_name);

  if (!prefetch_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

bool LineairDBTransaction::flush_write_buffer() {
  if (write_buffer_ops_.empty()) return true;
  if (prefetch_mode_) return true;
  if (is_aborted_) {
    write_buffer_ops_.clear();
    return false;
  }

  bool ok = lineairdb_proxy->tx_batch_write(this, "", write_buffer_ops_);
  write_buffer_ops_.clear();
  return ok;
}

bool LineairDBTransaction::flush_write_buffer_for_table(
    const std::string& table_name) {
  if (write_buffer_ops_.empty()) return true;
  if (prefetch_mode_) return true;
  if (is_aborted_) {
    write_buffer_ops_.clear();
    return false;
  }

  bool has_table_ops = false;
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name == table_name) {
      has_table_ops = true;
      break;
    }
  }
  if (!has_table_ops) return true;

  std::vector<LineairDBProxy::BatchOp> flush_ops;
  std::vector<LineairDBProxy::BatchOp> keep_ops;
  flush_ops.reserve(write_buffer_ops_.size());
  keep_ops.reserve(write_buffer_ops_.size());

  for (auto& op : write_buffer_ops_) {
    if (op.table_name == table_name) {
      flush_ops.push_back(std::move(op));
    } else {
      keep_ops.push_back(std::move(op));
    }
  }

  bool ok = lineairdb_proxy->tx_batch_write(this, table_name, flush_ops);
  if (!ok) {
    write_buffer_ops_.clear();
    return false;
  }

  write_buffer_ops_ = std::move(keep_ops);
  return true;
}

std::optional<LineairDBTransaction::LocalRowEntry>
LineairDBTransaction::lookup_write_set(
    const std::string& table_name, const std::string& key) const {
  for (auto it = own_writes_.rbegin(); it != own_writes_.rend(); ++it) {
    if (it->table_name == table_name && it->key == key) {
      return *it;
    }
  }
  return std::nullopt;
}

std::optional<LineairDBTransaction::LocalRowEntry>
LineairDBTransaction::lookup_row_cache(
    const std::string& table_name, const std::string& key) const {
  auto it = row_cache_.find(make_row_cache_key(table_name, key));
  if (it == row_cache_.end()) return std::nullopt;
  return it->second;
}

void LineairDBTransaction::drop_row_cache(const std::string& table_name,
                                           const std::string& key) {
  row_cache_.erase(make_row_cache_key(table_name, key));
}

bool LineairDBTransaction::key_is_in_range(const std::string& key,
                                           const std::string& start_key,
                                           const std::string& end_key) const {
  // LineairDB ranges are [start_key, end_key)
  if (key < start_key) return false;
  if (!end_key.empty() && key >= end_key) return false;
  return true;
}

bool LineairDBTransaction::key_starts_with(const std::string& key,
                                           const std::string& prefix) const {
  // Prefix scans use the encoded primary-key prefix
  if (key.size() < prefix.size()) return false;
  return key.compare(0, prefix.size(), prefix) == 0;
}

void LineairDBTransaction::remove_scan_row(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& key) const {
  // Local write/delete replaces any server row with the same key
  for (auto it = rows.begin(); it != rows.end(); ++it) {
    if (it->first == key) {
      rows.erase(it);
      return;
    }
  }
}

void LineairDBTransaction::insert_scan_row_in_order(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& key, const std::string& value,
    bool reverse_scan) const {
  // Keep the materialized scan result in key order
  for (auto it = rows.begin(); it != rows.end(); ++it) {
    if ((!reverse_scan && key < it->first) || (reverse_scan && key > it->first)) {
      rows.insert(it, {key, value});
      return;
    }
  }
  rows.emplace_back(key, value);
}

void LineairDBTransaction::merge_pending_rows_into_range_scan(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& start_key, const std::string& end_key,
    bool reverse_scan) const {
  // Server scan validates the range; proxy only adds its unflushed row ops
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != db_table_key) continue;
    if (op.type != LineairDBProxy::BatchOp::Type::Write &&
        op.type != LineairDBProxy::BatchOp::Type::Delete) {
      continue;
    }
    if (!key_is_in_range(op.key, start_key, end_key)) continue;

    remove_scan_row(rows, op.key);
    if (op.type == LineairDBProxy::BatchOp::Type::Write) {
      insert_scan_row_in_order(rows, op.key, op.value, reverse_scan);
    }
  }
}

void LineairDBTransaction::merge_pending_rows_into_prefix_scan(
    std::vector<std::pair<std::string, std::string>>& rows,
    const std::string& prefix) const {
  // Prefix scans are ASC, so inserted local rows keep ASC key order
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != db_table_key) continue;
    if (op.type != LineairDBProxy::BatchOp::Type::Write &&
        op.type != LineairDBProxy::BatchOp::Type::Delete) {
      continue;
    }
    if (!key_starts_with(op.key, prefix)) continue;

    remove_scan_row(rows, op.key);
    if (op.type == LineairDBProxy::BatchOp::Type::Write) {
      insert_scan_row_in_order(rows, op.key, op.value, false);
    }
  }
}

bool LineairDBTransaction::has_pending_ops_for_table(
    const std::string& table_name) const {
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name == table_name) return true;
  }
  return false;
}

bool LineairDBTransaction::has_pending_row_ops_in_range(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key) const {
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != table_name) continue;
    if (op.type != LineairDBProxy::BatchOp::Type::Write &&
        op.type != LineairDBProxy::BatchOp::Type::Delete) {
      continue;
    }
    if (op.key >= start_key && op.key < end_key) return true;
  }
  return false;
}

bool LineairDBTransaction::has_pending_secondary_ops_for_index(
    const std::string& table_name,
    const std::string& index_name) const {
  for (const auto& op : write_buffer_ops_) {
    const bool same_index =
        op.table_name == table_name && op.index_name == index_name;
    if (!same_index) continue;

    if (op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexWrite ||
        op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexDelete) {
      return true;
    }
  }
  return false;
}

void LineairDBTransaction::drop_secondary_scan_cache(
    const std::string& table_name,
    const std::string& index_name) {
  std::vector<LocalSecondaryScanEntry> kept;
  kept.reserve(secondary_scan_cache_.size());

  // A buffered SI write/delete can make old scan results incomplete
  for (const auto& entry : secondary_scan_cache_) {
    if (entry.table_name == table_name && entry.index_name == index_name) {
      continue;
    }
    kept.push_back(entry);
  }
  secondary_scan_cache_.swap(kept);
  // Vector indices shifted: rebuild the exact-start lookup index.
  secondary_scan_start_index_.clear();
  for (size_t i = 0; i < secondary_scan_cache_.size(); ++i) {
    const auto& e = secondary_scan_cache_[i];
    secondary_scan_start_index_[scan_cache_index_key(e.table_name,
                                                     e.index_name,
                                                     e.start_key)]
        .push_back(i);
  }
}

void LineairDBTransaction::record_write(const std::string& table_name,
                                              const std::string& key,
                                              bool found,
                                              const std::string& value) {
  // A later write/delete replaces any cached read for the same key
  drop_row_cache(table_name, key);

  for (auto& entry : own_writes_) {
    if (entry.table_name == table_name && entry.key == key) {
      entry.found = found;
      entry.value = value;
      return;
    }
  }
  own_writes_.push_back({table_name, key, found, value});
}

void LineairDBTransaction::record_row_cache(
    const std::string& table_name, const std::string& key, bool found,
    const std::string& value, uint64_t tid, bool validate_on_use) {
  // Overwrite with the latest staged row. Staging runs once per statement, so
  // the cached value stays stable while the statement consumes it (repeatable
  // within the statement); the next statement re-stages and overwrites. Each
  // consume appends to base_row_read_set_, so an overwrite never loses
  // a prior observation's TID.
  row_cache_[make_row_cache_key(table_name, key)] =
      LocalRowEntry{table_name, key, found, value, tid, validate_on_use};
}

void LineairDBTransaction::append_base_row_read(
    const std::string& table_name, const std::string& key, bool found,
    uint64_t tid) {
  // Append every observation, like Silo's read set (txn_impl.h: read_set is
  // emplace_back-only, no dedup). Repeats carry the cached value's TID, so a
  // key read N times validates that same TID N times -- redundant but never
  // wrong. Commit aborts if any entry's TID no longer matches the server.
  if (ro_novalidate_) return;  // commit skips validation; don't accumulate
  base_row_read_set_.push_back({table_name, key, tid, found});
}

void LineairDBTransaction::append_range_read(
    const LocalRangeScanEntry& cached) {
  // Assemble the commit-side range read from the cached scan: the bounds
  // describe the replay and result_keys is the observed key list in scan
  // order. Append, like the point and Silo read sets; a scan consumed twice
  // is revalidated twice -- redundant but never wrong.
  if (ro_novalidate_) return;  // commit skips validation; don't accumulate
  LineairDBProxy::RangeReadEntry entry;
  entry.table_name = cached.table_name;
  entry.start_key = cached.start_key;
  entry.end_key = cached.end_key;
  entry.row_limit = cached.row_limit;
  entry.reverse_scan = cached.reverse_scan;
  entry.result_keys.reserve(cached.rows.size());
  for (const auto& row : cached.rows) {
    entry.result_keys.push_back(row.first);
  }
  range_read_set_.push_back(std::move(entry));
}

void LineairDBTransaction::append_secondary_range_read(
    const LocalSecondaryScanEntry& cached) {
  if (ro_novalidate_) return;  // commit skips validation; don't accumulate
  LineairDBProxy::RangeReadEntry entry;
  entry.table_name = cached.table_name;
  entry.index_name = cached.index_name;
  entry.start_key = cached.start_key;
  entry.end_key = cached.end_key;
  entry.row_limit = cached.row_limit;
  entry.reverse_scan = cached.reverse_scan;
  entry.result_keys = cached.secondary_keys;
  entry.result_primary_keys = cached.primary_keys;
  range_read_set_.push_back(std::move(entry));
}

void LineairDBTransaction::abort_prefetch_cache_miss(
    const std::string& reason) {
  rpc_trace_.record_local_view("abort_prefetch_cache_miss:" + reason);
  LOG_WARNING("Prefetch cache miss: %s table=%s", reason.c_str(),
              db_table_key.c_str());
  is_aborted_ = true;
  aborted_by_cache_miss_ = true;
  thd_mark_transaction_to_rollback(thread, 1);
}

bool LineairDBTransaction::execute_read_plan_raw(
    const std::vector<LineairDBProxy::ReadPlanStep>& steps,
    std::vector<std::string>* values) {
  if (values == nullptr || steps.empty()) return false;
  if (!ro_novalidate_) return false;  // GS is read-only no-validate scope
  LineairDBProxy::ReadPlanResult result =
      lineairdb_proxy->tx_execute_read_plan(steps);
  if (!result.ok || result.steps.size() != steps.size()) {
    is_aborted_ = true;
    return false;
  }
  *values = std::move(result.steps[0].scan_values);
  return true;
}

void LineairDBTransaction::push_range_scan_cache(LocalRangeScanEntry entry) {
  range_scan_start_index_[scan_cache_index_key(entry.table_name, "",
                                               entry.start_key)]
      .push_back(range_scan_cache_.size());
  range_scan_cache_.push_back(std::move(entry));
}

void LineairDBTransaction::push_secondary_scan_cache(
    LocalSecondaryScanEntry entry) {
  secondary_scan_start_index_[scan_cache_index_key(
                                  entry.table_name, entry.index_name,
                                  entry.start_key)]
      .push_back(secondary_scan_cache_.size());
  secondary_scan_cache_.push_back(std::move(entry));
}

std::optional<LineairDBTransaction::LocalRangeScanEntry>
LineairDBTransaction::lookup_range_scan_cache(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key, bool reverse_scan, uint64_t row_limit,
    bool allow_truncated) const {
  SectionTimer section_timer(&rpc_trace_, "lookup_range");
  // A LIMITED staged entry holds only its first-N window. Own pending row
  // writes inside the requested range can change which rows belong to that
  // window (an own delete of row 1 makes the serial answer row N+1, which
  // the entry never fetched), so limited entries must not serve such ranges
  // (Codex F3). Unbounded entries are safe: the merge sees the full window.
  const bool pending_in_range =
      has_pending_row_ops_in_range(table_name, start_key, end_key);

  // GROUP-row entries (aggregate-stamped steps) are visible ONLY to the
  // aggregate's own consuming scan, and that scan sees ONLY them: while the
  // consume window for this table is open the consumer must never match a
  // raw same-table entry (its parser expects group rows), and no other
  // reader may match the group rows (Codex P1-3 — exclusivity is enforced
  // BOTH directions, per table).
  const bool agg_consumer =
      !pushed_aggregate_.empty() && pushed_aggregate_table_ == table_name;

  // Fast path: exact-start staged entry (the FER probe pattern).
  auto idx_it = range_scan_start_index_.find(
      scan_cache_index_key(table_name, "", start_key));
  if (idx_it != range_scan_start_index_.end()) {
    for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
         ++rit) {
      const auto& e = range_scan_cache_[*rit];
      if (e.aggregate_rows != agg_consumer) continue;
      if (e.row_limit != 0 && pending_in_range) continue;
      if (e.reverse_scan == reverse_scan && e.row_limit == row_limit &&
          end_key <= e.end_key) {
        LocalRangeScanEntry cached = e;
        cached.start_key = start_key;
        cached.end_key = end_key;
        cached.row_limit = row_limit;
        std::vector<std::pair<std::string, std::string>> rows;
        std::vector<uint64_t> row_tids;
        rows.reserve(cached.rows.size());
        row_tids.reserve(cached.row_tids.size());
        for (size_t i = 0; i < cached.rows.size(); ++i) {
          const auto& row = cached.rows[i];
          if (row.first >= start_key && row.first < end_key) {
            rows.push_back(row);
            if (i < cached.row_tids.size())
              row_tids.push_back(cached.row_tids[i]);
          }
        }
        cached.rows = std::move(rows);
        cached.row_tids = std::move(row_tids);
        return cached;
      }
    }
  }

  for (auto it = range_scan_cache_.rbegin();
       it != range_scan_cache_.rend(); ++it) {
    if (it->aggregate_rows != agg_consumer) continue;
    if (it->row_limit != 0 && pending_in_range) continue;
    const bool same_table = it->table_name == table_name;
    const bool same_direction = it->reverse_scan == reverse_scan;
    const bool same_limit = it->row_limit == row_limit;
    const bool covers_range =
        it->start_key <= start_key && end_key <= it->end_key;
    if (same_table && same_direction && same_limit && covers_range) {
      LocalRangeScanEntry cached = *it;
      cached.start_key = start_key;
      cached.end_key = end_key;
      cached.row_limit = row_limit;
      std::vector<std::pair<std::string, std::string>> rows;
      std::vector<uint64_t> row_tids;
      rows.reserve(cached.rows.size());
      row_tids.reserve(cached.row_tids.size());
      for (size_t i = 0; i < cached.rows.size(); ++i) {
        const auto& row = cached.rows[i];
        if (row.first >= start_key && row.first < end_key) {
          rows.push_back(row);
          if (i < cached.row_tids.size()) row_tids.push_back(cached.row_tids[i]);
        }
      }
      cached.rows = std::move(rows);
      cached.row_tids = std::move(row_tids);
      return cached;
    }
  }

  // Limit-staged fallback (autogen LIMIT pushdown): an unbounded request can
  // be served from a forward limit-N entry of the EXACT same range — the
  // entry holds the first N rows; the caller opted in (allow_truncated) to
  // abort if anything reads past them. Pending own row writes INSIDE this
  // range make the merged order ambiguous against a truncated prefix, so
  // reject those (ops on other ranges of the table — e.g. TPC-C Delivery's
  // earlier-district deletes — cannot affect this window).
  if (allow_truncated && row_limit == 0 && !reverse_scan &&
      !pending_in_range) {
    auto idx_it = range_scan_start_index_.find(
        scan_cache_index_key(table_name, "", start_key));
    if (idx_it != range_scan_start_index_.end()) {
      for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
           ++rit) {
        const auto& e = range_scan_cache_[*rit];
        if (e.aggregate_rows != agg_consumer) continue;
        if (e.row_limit > 0 && !e.reverse_scan && e.start_key == start_key &&
            e.end_key == end_key) {
          LocalRangeScanEntry cached = e;
          cached.truncated = (e.rows.size() >= e.row_limit);
          return cached;
        }
      }
    }
  }
  return std::nullopt;
}

std::optional<LineairDBTransaction::LocalSecondaryScanEntry>
LineairDBTransaction::lookup_secondary_scan_cache(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    bool reverse_scan, uint64_t row_limit) const {
  SectionTimer section_timer(&rpc_trace_, "lookup_secondary");
  // Direction only matters for truncated scans: an unlimited entry holds the
  // whole range whichever way it was produced, but a limit-N entry holds the
  // first N in ITS direction, so serving a DESC request from an ASC-limited
  // entry would return the wrong end of the range (Codex P0).
  const auto direction_compatible = [&](const LocalSecondaryScanEntry& e) {
    return e.row_limit == 0 || e.reverse_scan == reverse_scan;
  };

  // Fast path: exact-start staged entry (the FES probe pattern).
  auto idx_it = secondary_scan_start_index_.find(
      scan_cache_index_key(table_name, index_name, start_key));
  if (idx_it != secondary_scan_start_index_.end()) {
    for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
         ++rit) {
      const auto& e = secondary_scan_cache_[*rit];
      if (e.row_limit == row_limit && end_key <= e.end_key &&
          direction_compatible(e)) {
        LocalSecondaryScanEntry cached = e;
        cached.start_key = start_key;
        cached.end_key = end_key;
        cached.row_limit = row_limit;
        if (cached.secondary_keys.size() == cached.primary_keys.size()) {
          std::vector<std::string> secondary_keys;
          std::vector<std::string> primary_keys;
          secondary_keys.reserve(cached.secondary_keys.size());
          primary_keys.reserve(cached.primary_keys.size());
          for (size_t i = 0; i < cached.secondary_keys.size(); ++i) {
            if (cached.secondary_keys[i] >= start_key &&
                cached.secondary_keys[i] < end_key) {
              secondary_keys.push_back(cached.secondary_keys[i]);
              primary_keys.push_back(cached.primary_keys[i]);
            }
          }
          cached.secondary_keys = std::move(secondary_keys);
          cached.primary_keys = std::move(primary_keys);
        }
        return cached;
      }
    }
  }

  for (auto it = secondary_scan_cache_.rbegin();
       it != secondary_scan_cache_.rend(); ++it) {
    const bool same_index =
        it->table_name == table_name && it->index_name == index_name;
    const bool same_limit = it->row_limit == row_limit;
    const bool covers_range =
        it->start_key <= start_key && end_key <= it->end_key;
    if (same_index && same_limit && covers_range &&
        direction_compatible(*it)) {
      LocalSecondaryScanEntry cached = *it;
      cached.start_key = start_key;
      cached.end_key = end_key;
      cached.row_limit = row_limit;
      if (cached.secondary_keys.size() == cached.primary_keys.size()) {
        std::vector<std::string> secondary_keys;
        std::vector<std::string> primary_keys;
        secondary_keys.reserve(cached.secondary_keys.size());
        primary_keys.reserve(cached.primary_keys.size());
        for (size_t i = 0; i < cached.secondary_keys.size(); ++i) {
          if (cached.secondary_keys[i] >= start_key &&
              cached.secondary_keys[i] < end_key) {
            secondary_keys.push_back(cached.secondary_keys[i]);
            primary_keys.push_back(cached.primary_keys[i]);
          }
        }
        cached.secondary_keys = std::move(secondary_keys);
        cached.primary_keys = std::move(primary_keys);
      }
      return cached;
    }
  }
  return std::nullopt;
}

bool LineairDBTransaction::fallback_to_normal_transaction(const char* reason) {
  if (!prefetch_mode_) return true;

  abort_prefetch_cache_miss(std::string("unstaged read surface: ") + reason);
  return false;
}

bool LineairDBTransaction::prefetch_validate_and_commit() {
  bool was_aborted = is_aborted_;

  // Read-only no-validation fast path: nothing to install, validation opted
  // out -> the transaction ends locally with no commit RPC (1-RPC SELECT).
  if (!was_aborted && ro_novalidate_ && write_buffer_ops_.empty() &&
      rowcount_deltas_.empty()) {
    if (rpc_trace_.active()) {
      rpc_trace_.record_local_view("ro_novalidate_commit");
      RpcTraceLogger::instance().log_line(rpc_trace_.finalize_jsonl(true));
    }
    lineairdb_proxy->set_current_trace(nullptr);
    delete this;
    return true;
  }

  // Single-key read-only fast path (Phase-20): a read-only transaction whose
  // ENTIRE read-set is a single point read is serializable at its read
  // timestamp regardless of concurrent writes — a one-item read-only
  // transaction can always be placed in the serial order immediately after the
  // write that produced the version it read, so OCC validation can only ever
  // pass. There is nothing to install and nothing another transaction's
  // validation depends on, so end locally with no commit RPC (halves the RPC
  // count of single-row OLTP reads: TATP GetSubscriberData/GetAccessData).
  // Soundness needs the read to be a single key: a multi-key read can observe
  // an inconsistent cross-key state (the server's plan reads are stateless, not
  // a snapshot), which only validation rules out; range reads are excluded for
  // the phantom concern. This is NOT ro_novalidate (which blanket-skips
  // validation for any read set and is unsafe under concurrency) — it is the
  // provably-always-serializable single-item read-only case. Default ON;
  // HELIOS_RO_SINGLEKEY_COMMIT=0 disables (off-switch / A-B).
  static const char* sk_env = std::getenv("HELIOS_RO_SINGLEKEY_COMMIT");
  static const bool single_key_ro_on = sk_env == nullptr || sk_env[0] != '0';
  if (single_key_ro_on && !was_aborted && write_buffer_ops_.empty() &&
      rowcount_deltas_.empty() && range_read_set_.empty() &&
      base_row_read_set_.size() <= 1) {
    if (rpc_trace_.active()) {
      rpc_trace_.record_local_view("ro_singlekey_commit");
      RpcTraceLogger::instance().log_line(rpc_trace_.finalize_jsonl(true));
    }
    lineairdb_proxy->set_current_trace(nullptr);
    delete this;
    return true;
  }

  if (rpc_trace_.active()) {
    rpc_trace_.record_section_count("commit_base_rows",
                                    base_row_read_set_.size());
    rpc_trace_.record_section_count("commit_range_entries",
                                    range_read_set_.size());
    uint64_t range_keys = 0;
    for (const auto& e : range_read_set_) range_keys += e.result_keys.size();
    rpc_trace_.record_section_count("commit_range_keys", range_keys);
    rpc_trace_.record_section_count("commit_write_ops",
                                    write_buffer_ops_.size());
  }

  std::vector<LineairDBProxy::StatelessReadKey> reads;
  std::vector<uint64_t> read_tids;
  std::vector<bool> read_found;
  if (!was_aborted) {
    reads.reserve(base_row_read_set_.size());
    read_tids.reserve(base_row_read_set_.size());
    read_found.reserve(base_row_read_set_.size());
    for (const auto& entry : base_row_read_set_) {
      reads.push_back({entry.table_name, entry.key});
      read_tids.push_back(entry.tid);
      read_found.push_back(entry.found);
    }
  }

  std::vector<std::pair<std::string, int64_t>> server_deltas;
  if (!was_aborted && !rowcount_deltas_.empty()) {
    server_deltas.reserve(rowcount_deltas_.size());
    for (const auto& entry : rowcount_deltas_) {
      if (entry.share != nullptr && entry.delta != 0)
        server_deltas.emplace_back(entry.table_name, entry.delta);
    }
  }

  bool committed = false;
  std::string abort_reason;
  if (!was_aborted) {
    committed = lineairdb_proxy->tx_validate_and_commit(
        reads, read_tids, read_found, range_read_set_,
        write_buffer_ops_, server_deltas, isFence, &abort_reason);
    if (!committed && !abort_reason.empty()) {
      rpc_trace_.record_local_view("abort_validate_" + abort_reason);
    }
  }
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
  }

  if (!was_aborted && committed && !rowcount_deltas_.empty()) {
    const uint64_t tid = static_cast<uint64_t>(thread->thread_id());
    const size_t shard =
        static_cast<size_t>(tid) & (LineairDB_share::kRowCountShards - 1);

    for (const auto& entry : rowcount_deltas_) {
      if (entry.share == nullptr || entry.delta == 0)
        continue;

      entry.share->rowcount_shards[shard].delta.fetch_add(
          entry.delta, std::memory_order_relaxed);
    }
  }

  if (rpc_trace_.active()) {
    RpcTraceLogger::instance().log_line(
        rpc_trace_.finalize_jsonl(committed && !was_aborted));
  }
  lineairdb_proxy->set_current_trace(nullptr);

  delete this;
  return committed;
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  rpc_trace_.start(-1, std::this_thread::get_id());
  lineairdb_proxy->set_current_trace(&rpc_trace_);

  if (prefetch_mode_) {
    prefetch_registered_ = true;
    is_aborted_ = false;
    if (thd_is_transaction()) {
      isTransaction = true;
      register_transaction_to_mysql();
    }
    else {
      // Autocommit single-statement plain SELECT: when the operator enabled
      // lineairdb_prefetch_ro_novalidate, skip read-set accumulation and the
      // commit-time validation RPC entirely (sound without concurrent
      // writers; see the sysvar help text). Locking reads (FOR UPDATE/SHARE)
      // keep full validation: every referenced table must be opened with a
      // plain read lock (Codex P1).
      extern bool srv_prefetch_ro_novalidate;
      bool plain_read_only = srv_prefetch_ro_novalidate && thread != nullptr &&
                             thd_sql_command(thread) == SQLCOM_SELECT &&
                             thread->lex != nullptr;
      if (plain_read_only) {
        for (Table_ref *t = thread->lex->query_tables; t != nullptr;
             t = t->next_global) {
          if (t->lock_descriptor().type > TL_READ) {
            plain_read_only = false;
            break;
          }
        }
      }
      ro_novalidate_ = plain_read_only;
      register_single_statement_to_mysql();
    }
    return;
  }

  tx_id = lineairdb_proxy->tx_begin_transaction();
  // TODO: maybe need error handling when tx_id == -1
  assert(tx_id != -1);
  rpc_trace_.set_tx_id(tx_id);
  is_aborted_ = false;

  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  }
  else {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() {
  if (prefetch_mode_ || tx_id == -1) {
    is_aborted_ = true;
    return;
  }
  // Skip TX_ABORT RPC if the server already knows (is_aborted_ was set from an RPC response).
  if (!is_aborted_) {
    lineairdb_proxy->tx_abort(tx_id);
  }
  is_aborted_ = true;
}

bool LineairDBTransaction::end_transaction() {
  if (prefetch_mode_) {
    return prefetch_validate_and_commit();
  }

  assert(tx_id != -1);
  flush_write_buffer();
  bool was_aborted = is_aborted_;

  // Build row-delta pairs for the server (table_name, delta).
  std::vector<std::pair<std::string, int64_t>> server_deltas;
  if (!was_aborted && !rowcount_deltas_.empty()) {
    server_deltas.reserve(rowcount_deltas_.size());
    for (const auto &entry : rowcount_deltas_) {
      if (entry.share != nullptr && entry.delta != 0)
        server_deltas.emplace_back(entry.table_name, entry.delta);
    }
  }

  bool committed = lineairdb_proxy->db_end_transaction(tx_id, isFence, server_deltas);
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
  }

  // Flush committed row-count deltas to local shards (for this proxy's info()).
  if (!was_aborted && committed && !rowcount_deltas_.empty()) {
    const uint64_t tid = static_cast<uint64_t>(thread->thread_id());
    const size_t shard =
        static_cast<size_t>(tid) & (LineairDB_share::kRowCountShards - 1);

    for (const auto &entry : rowcount_deltas_) {
      if (entry.share == nullptr || entry.delta == 0)
        continue;

      entry.share->rowcount_shards[shard].delta.fetch_add(
          entry.delta, std::memory_order_relaxed);
    }
  }

  if (isFence && !was_aborted && committed) {
    lineairdb_proxy->db_fence();
  }

  if (rpc_trace_.active()) {
    RpcTraceLogger::instance().log_line(
        rpc_trace_.finalize_jsonl(committed && !was_aborted));
  }
  lineairdb_proxy->set_current_trace(nullptr);

  delete this;
  return committed;
}

void LineairDBTransaction::fence() const { lineairdb_proxy->db_fence(); }




bool LineairDBTransaction::thd_is_transaction() const {
  return ::thd_test_options(thread, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN | OPTION_TABLE_LOCK);
}

void LineairDBTransaction::register_transaction_to_mysql() {
  const ulonglong threadID = static_cast<ulonglong>(thread->thread_id());
  ::trans_register_ha(thread, isTransaction, hton, &threadID);
}

void LineairDBTransaction::register_single_statement_to_mysql() {
  register_transaction_to_mysql();
}
