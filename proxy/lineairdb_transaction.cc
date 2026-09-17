#include "lineairdb_transaction.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "lineairdb_keyenc.hh"
#include "../common/log.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

#include <algorithm>
#include <thread>

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

// Scope of one secondary index in the pending-entry map.
inline std::string index_scope_key(const std::string& table,
                                   const std::string& index) {
  std::string k;
  k.reserve(table.size() + index.size() + 1);
  k += table;
  k.push_back('\x01');
  k += index;
  return k;
}

// Exclusive upper bound of a prefix range, or the sentinel when the prefix is
// all 0xff and has no successor.
std::string prefix_range_end(const std::string& prefix) {
  std::string end = lineairdb_keyenc::build_prefix_range_end(prefix);
  if (end.empty()) return lineairdb_keyenc::scan_end_sentinel();
  return end;
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
                                            handlerton* lineairdb_hton)
    : lineairdb_proxy(lineairdb_proxy),
      thread(thd),
      isTransaction(false),
      hton(lineairdb_hton),
      is_aborted_(false)
    {}

void LineairDBTransaction::choose_table(std::string db_table_name) {
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
    rpc_trace_.record_local_view("write_set_hit");
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Repeat exact-key reads can use the local read set
  if (auto entry = lookup_row_cache(db_table_key, key)) {
    rpc_trace_.record_local_view("read_cache_hit");
    // A consumed cache hit appends to the point read set.
    rpc_trace_.record_local_view(
        trace_count_event("use_point_read", entry->table_name, 1));
    append_base_row_read(entry->table_name, entry->key, entry->tid);
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  rpc_trace_.record_local_view("read_miss");
  auto result = lineairdb_proxy->tx_read(db_table_key, key);
  if (!result.ok) {
    mark_transport_error();
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  record_row_cache(db_table_key, key, result.found, result.value, result.tid);
  append_base_row_read(db_table_key, key, result.tid);
  if (!result.found) {
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  last_read_value_ = std::move(result.value);
  return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
}

std::vector<std::pair<bool, std::string>>
LineairDBTransaction::batch_read(const std::vector<std::string>& keys) {
  if (table_is_not_chosen()) return {};

  std::vector<std::pair<bool, std::string>> pairs;
  pairs.resize(keys.size());

  std::vector<LineairDBProxy::ReadKey> rpc_keys;
  std::vector<size_t> rpc_positions;
  rpc_keys.reserve(keys.size());
  rpc_positions.reserve(keys.size());

  // Resolve keys covered by the local read/write sets first
  for (size_t i = 0; i < keys.size(); ++i) {
    if (auto entry = lookup_write_set(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("write_set_hit");
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    if (auto entry = lookup_row_cache(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_cache_hit");
      rpc_trace_.record_local_view(
          trace_count_event("use_point_read", entry->table_name, 1));
      append_base_row_read(entry->table_name, entry->key, entry->tid);
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    rpc_trace_.record_local_view("batch_miss");
    rpc_positions.push_back(i);
    rpc_keys.push_back({db_table_key, keys[i]});
  }

  // Fetch only the misses; tx_batch_read() answers in rpc_keys order
  //   Example: keys=[A,B,C], B is local -> rpc_keys=[A,C],
  //            rpc_positions=[0,2], so RPC results fill pairs[0] and pairs[2].
  if (!rpc_keys.empty()) {
    auto results = lineairdb_proxy->tx_batch_read(rpc_keys);
    if (results.size() != rpc_keys.size()) {
      rpc_trace_.record_local_view("abort_batch_size_mismatch");
      mark_transport_error();
      return pairs;
    }
    for (size_t i = 0; i < results.size(); ++i) {
      // Map each RPC result back to the original keys[] position
      const size_t pos = rpc_positions[i];
      record_row_cache(db_table_key, keys[pos], results[i].found,
                       results[i].value, results[i].tid);
      append_base_row_read(db_table_key, keys[pos], results[i].tid);
      pairs[pos] = {results[i].found, std::move(results[i].value)};
    }
  }
  return pairs;
}

LineairDBTransaction::KeyState LineairDBTransaction::insert_key_state(
    const std::string& table_name, const std::string& key) const {
  // A row this transaction still holds is a duplicate of its own making; a key
  // it deleted is free, and the commit checks the absence again under the
  // lock. A cached read does not answer: the row may have moved since.
  if (auto own = lookup_write_set(table_name, key)) {
    return own->found ? KeyState::Taken : KeyState::Free;
  }
  return KeyState::Unknown;
}

bool LineairDBTransaction::index_key_taken_by_own_write(
    const std::string& table_name, const std::string& index_name,
    const std::string& secondary_key, const std::string& primary_key) const {
  const auto scope =
      pending_index_entries_.find(index_scope_key(table_name, index_name));
  if (scope == pending_index_entries_.end()) return false;
  const auto entry = scope->second.find(secondary_key);
  if (entry == scope->second.end()) return false;
  for (const auto& [pk, still_there] : entry->second) {
    if (still_there && pk != primary_key) return true;
  }
  return false;
}

void LineairDBTransaction::record_index_op(
    const LineairDBProxy::WriteOp& op) {
  // The last op for one (secondary key, primary key) pair decides.
  pending_index_entries_[index_scope_key(op.table_name, op.index_name)]
                        [op.secondary_key][op.primary_key] =
      op.type == LineairDBProxy::WriteOp::Type::SecondaryIndexWrite;
}

bool LineairDBTransaction::reads_still_valid() {
  if (!base_row_read_set_.empty()) {
    std::vector<LineairDBProxy::ReadKey> keys;
    keys.reserve(base_row_read_set_.size());
    for (const auto& entry : base_row_read_set_) {
      keys.push_back({entry.table_name, entry.key});
    }
    auto results = lineairdb_proxy->tx_batch_read(keys);
    if (results.size() != keys.size()) {
      mark_transport_error();
      return false;
    }
    for (size_t i = 0; i < results.size(); ++i) {
      if (results[i].tid != base_row_read_set_[i].tid) return false;
    }
  }

  for (const auto& range : range_read_set_) {
    if (range.index_name.empty()) {
      auto now = lineairdb_proxy->tx_scan(range.table_name, range.start_key,
                                          range.end_key, range.row_limit,
                                          range.reverse_scan,
                                          /*keys_only=*/true);
      if (!now.ok) {
        if (now.transport_error) mark_transport_error();
        return false;
      }
      if (now.rows.size() != range.result_keys.size()) return false;
      for (size_t i = 0; i < now.rows.size(); ++i) {
        if (now.rows[i].key != range.result_keys[i]) return false;
      }
      continue;
    }

    auto now = lineairdb_proxy->tx_scan_index(
        range.table_name, range.index_name, range.start_key, range.end_key,
        range.row_limit, range.reverse_scan, /*keys_only=*/true);
    if (!now.ok) {
      if (now.transport_error) mark_transport_error();
      return false;
    }
    if (now.rows.size() != range.result_keys.size() ||
        now.rows.size() != range.result_primary_keys.size()) {
      return false;
    }
    for (size_t i = 0; i < now.rows.size(); ++i) {
      if (now.rows[i].secondary_key != range.result_keys[i] ||
          now.rows[i].primary_key != range.result_primary_keys[i]) {
        return false;
      }
    }
  }
  return true;
}

bool LineairDBTransaction::probe_insert_keys(
    const std::string& table_name, const std::vector<std::string>& keys) {
  if (keys.empty()) return false;

  std::vector<LineairDBProxy::ReadKey> reads;
  reads.reserve(keys.size());
  for (const auto& key : keys) reads.push_back({table_name, key});

  auto results = lineairdb_proxy->tx_batch_read(reads);
  if (results.size() != reads.size()) {
    rpc_trace_.record_local_view("abort_probe_size_mismatch");
    mark_transport_error();
    return false;
  }

  bool taken = false;
  for (size_t i = 0; i < results.size(); ++i) {
    // The absence is what the commit has to revalidate; the row this
    // statement writes at the same key is not a cached read.
    append_base_row_read(table_name, keys[i], results[i].tid);
    if (results[i].found) taken = true;
  }
  return taken;
}

void LineairDBTransaction::execute_read_plan(
    const std::vector<LineairDBProxy::ReadPlanStep>& full_steps) {
  if (full_steps.empty()) return;

  // Exact point reads already covered by the local view need no staging RPC.
  // Referenced steps must survive because later bindings point at
  // them by source_step index.
  std::vector<bool> referenced(full_steps.size(), false);
  for (const auto& step : full_steps) {
    for (const auto& b : step.bindings) {
      if (b.source_step < referenced.size()) referenced[b.source_step] = true;
    }
    for (const auto& b : step.end_bindings) {
      if (b.source_step < referenced.size()) referenced[b.source_step] = true;
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
    // Remap source_step after dropping covered point reads. The DSL supplies
    // it, so an index past the plan is an invalid plan, not a step.
    for (auto& step : steps) {
      for (auto& b : step.bindings) {
        if (b.source_step >= new_index.size()) return;
        b.source_step = new_index[b.source_step];
      }
      for (auto& b : step.end_bindings) {
        if (b.source_step >= new_index.size()) return;
        b.source_step = new_index[b.source_step];
      }
    }
  }
  if (steps.empty()) return;

  rpc_trace_.record_local_view("plan_request:steps=" +
                               std::to_string(steps.size()));
  auto result = lineairdb_proxy->tx_execute_read_plan(steps);
  if (!result.ok || result.steps.size() != steps.size()) {
    rpc_trace_.record_local_view("abort_read_plan_rpc");
    if (result.transport_error) {
      mark_transport_error();
    } else {
      is_aborted_ = true;
    }
    return;
  }

  // Consume each decoded step destructively: move strings into local caches,
  // then release the step before staging the next one.
  for (size_t i = 0; i < result.steps.size() && i < steps.size(); ++i) {
    const auto& step = steps[i];
    auto& step_result = result.steps[i];
    // Move row j out of the wire result into the row cache; an empty value is
    // a not-found answer.
    auto take_row = [&](size_t j, std::string& key, std::string& value,
                        uint64_t& tid) {
      key = std::move(step_result.scan_keys[j]);
      value = j < step_result.scan_values.size()
                  ? std::move(step_result.scan_values[j])
                  : std::string();
      tid = j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
      const bool found = !value.empty();
      record_row_cache(step.table_name, key, found, value, tid);
      return found;
    };

    if (!step.is_scan && !step.for_each) {
      rpc_trace_.record_local_view(trace_count_event(
          step_result.found ? "plan_fetch:R:hit" : "plan_fetch:R:miss",
          step.table_name, 1));
      if (step_result.found) {
        record_row_cache(step.table_name, step_result.actual_key, true,
                          step_result.value, step_result.tid);
      } else {
        record_row_cache(step.table_name, step_result.actual_key, false, "",
                          step_result.tid);
      }
      step_result = LineairDBProxy::ReadPlanStepResult{};
      continue;
    }

    rpc_trace_.record_local_view(trace_plan_scan_event(
        step.table_name, step.index_name, step_result.scan_keys.size(),
        step_result.scan_values.size(), step.scan_limit, step.for_each));

    if (step.for_each && step.is_scan) {
      // Stage grouped for_each range results as ordinary scan-cache entries.
      // Each group corresponds to one deduplicated probe key.
      if (step_result.group_start_keys.size() !=
              step_result.group_sizes.size() ||
          step_result.group_end_keys.size() != step_result.group_sizes.size()) {
        // A group without its bounds cannot serve anything; the plan failed.
        rpc_trace_.record_local_view("abort_read_plan_groups");
        is_aborted_ = true;
        return;
      }
      size_t flat = 0;  // Offset into the flat scan arrays across all groups.
      for (size_t g = 0; g < step_result.group_sizes.size(); ++g) {
        const size_t n = step_result.group_sizes[g];
        const std::string& gstart = step_result.group_start_keys[g];
        const std::string& gend = step_result.group_end_keys[g];
        if (step.index_name.empty()) {
          // Primary range group: cache row values by primary key.
          LocalRangeScanEntry entry;
          entry.table_name = step.table_name;
          entry.start_key = gstart;
          entry.end_key = gend;
          entry.reverse_scan = step.reverse_scan;
          entry.row_limit = step.scan_limit;
          for (size_t j = flat; j < flat + n && j < step_result.scan_keys.size();
               ++j) {
            std::string key;
            std::string value;
            uint64_t tid = 0;
            if (take_row(j, key, value, tid)) {
              entry.rows.emplace_back(std::move(key), std::move(value));
              entry.row_tids.push_back(tid);
            }
          }
          push_range_scan_cache(std::move(entry));
        } else {
          // Secondary range group: cache secondary keys and their primary keys.
          LocalSecondaryScanEntry entry;
          entry.table_name = step.table_name;
          entry.index_name = step.index_name;
          entry.start_key = gstart;
          entry.end_key = gend;
          entry.reverse_scan = step.reverse_scan;
          entry.row_limit = step.scan_limit;
          for (size_t j = flat; j < flat + n && j < step_result.scan_keys.size();
               ++j) {
            std::string key;
            std::string value;
            uint64_t tid = 0;
            take_row(j, key, value, tid);
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
      // Point probes only populate the row cache.
      for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
        std::string key;
        std::string value;
        uint64_t tid = 0;
        take_row(j, key, value, tid);
      }
      step_result = LineairDBProxy::ReadPlanStepResult{};
      continue;
    }

    if (step.index_name.empty()) {
      // Primary scan: cache row values and stage one primary range entry.
      std::vector<std::pair<std::string, std::string>> rows;
      std::vector<uint64_t> row_tids;
      rows.reserve(step_result.scan_keys.size());
      row_tids.reserve(step_result.scan_keys.size());
      for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
        std::string key;
        std::string value;
        uint64_t tid = 0;
        if (take_row(j, key, value, tid)) {
          rows.emplace_back(std::move(key), std::move(value));
          row_tids.push_back(tid);
        }
      }
      LocalRangeScanEntry entry{
          step.table_name, step_result.actual_start_key,
          step_result.actual_end_key, step.reverse_scan, step.scan_limit,
          std::move(rows), std::move(row_tids)};
      push_range_scan_cache(std::move(entry));
    } else {
      // Keep secondary_keys and primary_keys aligned; lookup walks the pairs.
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
        std::string key;
        std::string value;
        uint64_t tid = 0;
        take_row(j, key, value, tid);
        cached.primary_keys.push_back(std::move(key));
      }
      push_secondary_scan_cache(std::move(cached));
    }
    step_result = LineairDBProxy::ReadPlanStepResult{};
  }
}

void LineairDBTransaction::buffer_writes(
    const std::string& table_name,
    const std::vector<LineairDBProxy::WriteOp>& ops) {
  for (auto op : ops) {
    if (op.table_name.empty()) op.table_name = table_name;
    if (op.type == LineairDBProxy::WriteOp::Type::Write) {
      record_write(op.table_name, op.key, true, op.value);
    } else if (op.type == LineairDBProxy::WriteOp::Type::Delete) {
      record_write(op.table_name, op.key, false, ""); // value unused when not found
    } else {
      record_index_op(op);
    }
    write_buffer_ops_.push_back(std::move(op));
  }
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key,
                                           bool keys_only) {
  if (table_is_not_chosen()) return {};

  // The smallest key above secondary_key bounds the range to that one key.
  std::string end_key = secondary_key;
  end_key.push_back('\0');

  if (keys_only) {
    return scan_index_range(index_name, secondary_key, end_key, 0, false, true)
        .primary_keys;
  }
  return get_matching_primary_keys_in_range(index_name, secondary_key, end_key);
}

void LineairDBTransaction::update_secondary_index(std::string index_name,
                                                  std::string old_secondary_key,
                                                  std::string new_secondary_key,
                                                  const std::string primary_key) {
  buffer_delete_secondary_index(db_table_key, index_name, old_secondary_key,
                                primary_key);
  buffer_write_secondary_index(db_table_key, index_name, new_secondary_key,
                               primary_key);
}

// Primary key scan operations

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_range(std::string start_key,
                                                            std::string end_key,
                                                            uint64_t row_limit,
                                                            bool reverse_scan,
                                                            bool *served_truncated) {
  if (served_truncated != nullptr) *served_truncated = false;
  if (table_is_not_chosen()) return {};
  // An empty end is not a range the server answers; real encoded keys begin
  // with a null marker and sort below the sentinel.
  if (end_key.empty()) end_key = lineairdb_keyenc::scan_end_sentinel();

  if (auto cached = lookup_range_scan_cache(
          db_table_key, start_key, end_key, reverse_scan, row_limit,
          /*allow_truncated=*/served_truncated != nullptr)) {
    if (served_truncated != nullptr) *served_truncated = cached->truncated;
    std::vector<std::pair<std::string, std::string>> pairs = cached->rows;
    for (size_t i = 0; i < cached->rows.size(); ++i) {
      append_base_row_read(db_table_key, cached->rows[i].first,
                           cached->row_tids[i]);
    }
    // Record the pre-merge staged rows: commit-side replay cannot see this
    // transaction's pending writes.
    append_range_read(*cached);

    merge_pending_rows_into_range_scan(pairs, start_key, end_key, reverse_scan);
    if (row_limit > 0 && pairs.size() > row_limit) {
      pairs.resize(static_cast<size_t>(row_limit));
    }
    rpc_trace_.record_local_view(
        trace_count_event("use_pk_value_scan", db_table_key, pairs.size()));
    return pairs;
  }

  return scan_range(start_key, end_key, row_limit, reverse_scan);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::scan_range(const std::string& start_key,
                                 const std::string& end_key,
                                 uint64_t row_limit, bool reverse_scan) {
  // A pending write of this transaction inside the range changes which rows a
  // limit selects, so ask for the whole range and cut it here instead.
  const uint64_t sent_limit =
      has_pending_row_ops_in_range(db_table_key, start_key, end_key)
          ? 0
          : row_limit;

  auto result = lineairdb_proxy->tx_scan(db_table_key, start_key, end_key,
                                         sent_limit, reverse_scan, false);
  if (!result.ok) {
    if (result.transport_error) {
      mark_transport_error();
    } else {
      abort_server_refused("scan");
    }
    return {};
  }

  LocalRangeScanEntry scanned;
  scanned.table_name = db_table_key;
  scanned.start_key = start_key;
  scanned.end_key = end_key;
  scanned.reverse_scan = reverse_scan;
  scanned.row_limit = sent_limit;
  scanned.rows.reserve(result.rows.size());
  scanned.row_tids.reserve(result.rows.size());
  for (auto& row : result.rows) {
    record_row_cache(db_table_key, row.key, true, row.value, row.tid);
    append_base_row_read(db_table_key, row.key, row.tid);
    scanned.rows.emplace_back(std::move(row.key), std::move(row.value));
    scanned.row_tids.push_back(row.tid);
  }
  append_range_read(scanned);

  auto pairs = std::move(scanned.rows);
  merge_pending_rows_into_range_scan(pairs, start_key, end_key, reverse_scan);
  if (row_limit > 0 && pairs.size() > row_limit) {
    pairs.resize(static_cast<size_t>(row_limit));
  }
  return pairs;
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_from_prefix(std::string prefix) {
  if (table_is_not_chosen()) return {};
  if (prefix.empty()) {
    return get_matching_keys_and_values_in_range("", std::string());
  }
  return get_matching_keys_and_values_in_range(prefix, prefix_range_end(prefix));
}

// Secondary index scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_in_range(std::string index_name,
                                                         std::string start_key,
                                                         std::string end_key,
                                                         uint64_t row_limit,
                                                         bool reverse_scan) {
  if (table_is_not_chosen()) return {};
  if (end_key.empty()) end_key = lineairdb_keyenc::scan_end_sentinel();

  auto cached = lookup_secondary_scan_cache(
      db_table_key, index_name, start_key, end_key, reverse_scan, row_limit);
  if (!cached && row_limit != 0) {
    // An unlimited staged scan of the same range covers a limited request;
    // the limit is applied to the merged result below.
    cached = lookup_secondary_scan_cache(db_table_key, index_name, start_key,
                                         end_key, reverse_scan, 0);
  }
  if (cached) {
    append_secondary_range_read(*cached);
    std::map<std::string, std::vector<std::string>> groups;
    for (size_t i = 0; i < cached->secondary_keys.size(); ++i) {
      groups[cached->secondary_keys[i]].push_back(cached->primary_keys[i]);
    }
    auto merged = merge_index_scan(index_name, start_key, end_key, row_limit,
                                   reverse_scan, groups);
    rpc_trace_.record_local_view("use_si_scan:" + db_table_key + ":" +
                                 index_name + ":n=" +
                                 std::to_string(merged.primary_keys.size()));
    return merged.primary_keys;
  }

  return scan_index_range(index_name, start_key, end_key, row_limit,
                          reverse_scan, false)
      .primary_keys;
}

LineairDBTransaction::SecondaryScan LineairDBTransaction::scan_index_range(
    const std::string& index_name, const std::string& start_key,
    const std::string& end_key, uint64_t row_limit, bool reverse_scan,
    bool keys_only) {
  SecondaryScan out;
  // A pending index op of this transaction inside the range changes which
  // entries a limit selects, so ask for the whole range and cut it here.
  const uint64_t sent_limit =
      has_pending_secondary_ops_in_range(db_table_key, index_name, start_key,
                                         end_key)
          ? 0
          : row_limit;

  auto result =
      lineairdb_proxy->tx_scan_index(db_table_key, index_name, start_key,
                                     end_key, sent_limit, reverse_scan,
                                     keys_only);
  if (!result.ok) {
    if (result.transport_error) {
      mark_transport_error();
    } else {
      abort_server_refused("scan_index");
    }
    return out;
  }

  LocalSecondaryScanEntry scanned;
  scanned.table_name = db_table_key;
  scanned.index_name = index_name;
  scanned.start_key = start_key;
  scanned.end_key = end_key;
  scanned.reverse_scan = reverse_scan;
  scanned.row_limit = sent_limit;
  // Within one secondary key the primary keys come in key order whichever the
  // direction, so each group stays sorted for the merge below.
  std::map<std::string, std::vector<std::string>> groups;
  for (auto& row : result.rows) {
    if (!keys_only) {
      record_row_cache(db_table_key, row.primary_key, !row.value.empty(),
                       row.value, row.tid);
    }
    scanned.secondary_keys.push_back(row.secondary_key);
    groups[row.secondary_key].push_back(row.primary_key);
    scanned.primary_keys.push_back(std::move(row.primary_key));
  }
  append_secondary_range_read(scanned);

  return merge_index_scan(index_name, start_key, end_key, row_limit,
                          reverse_scan, groups);
}

LineairDBTransaction::SecondaryScan LineairDBTransaction::merge_index_scan(
    const std::string& index_name, const std::string& start_key,
    const std::string& end_key, uint64_t row_limit, bool reverse_scan,
    std::map<std::string, std::vector<std::string>>& groups) const {
  merge_pending_index_ops(index_name, start_key, end_key, groups);

  SecondaryScan out;
  out.ok = true;
  const auto emit = [&](const std::string& secondary_key,
                        const std::vector<std::string>& primary_keys) {
    for (const auto& pk : primary_keys) {
      out.secondary_keys.push_back(secondary_key);
      out.primary_keys.push_back(pk);
    }
  };
  if (reverse_scan) {
    for (auto it = groups.rbegin(); it != groups.rend(); ++it) {
      emit(it->first, it->second);
    }
  } else {
    for (const auto& [secondary_key, primary_keys] : groups) {
      emit(secondary_key, primary_keys);
    }
  }
  if (row_limit > 0 && out.primary_keys.size() > row_limit) {
    out.secondary_keys.resize(static_cast<size_t>(row_limit));
    out.primary_keys.resize(static_cast<size_t>(row_limit));
  }
  return out;
}

std::optional<LineairDBTransaction::SecondaryEntry>
LineairDBTransaction::fetch_last_secondary_entry_in_range(const std::string &index_name,
                                                          const std::string &start_key,
                                                          const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;

  const std::string effective_end =
      end_key.empty() ? lineairdb_keyenc::scan_end_sentinel() : end_key;
  auto scan = scan_index_range(index_name, start_key, effective_end, 0,
                               /*reverse_scan=*/true, /*keys_only=*/true);
  if (!scan.ok || scan.secondary_keys.empty()) return std::nullopt;

  // A reverse scan puts the highest secondary key first; take its whole group.
  SecondaryEntry entry;
  entry.secondary_key = scan.secondary_keys.front();
  for (size_t i = 0; i < scan.secondary_keys.size(); ++i) {
    if (scan.secondary_keys[i] != entry.secondary_key) break;
    entry.primary_keys.push_back(scan.primary_keys[i]);
  }
  return entry;
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
                                        const std::string& value,
                                        bool is_insert) {
  LineairDBProxy::WriteOp op;
  op.type = LineairDBProxy::WriteOp::Type::Write;
  op.key = key;
  op.value = value;
  op.table_name = table_name;
  op.is_insert = is_insert;
  write_buffer_ops_.push_back(std::move(op));
  record_write(table_name, key, true, value);
}

void LineairDBTransaction::buffer_write_secondary_index(const std::string& table_name,
                                                        const std::string& index_name,
                                                        const std::string& secondary_key,
                                                        const std::string& primary_key) {
  LineairDBProxy::WriteOp op;
  op.type = LineairDBProxy::WriteOp::Type::SecondaryIndexWrite;
  op.index_name = index_name;
  op.secondary_key = secondary_key;
  op.primary_key = primary_key;
  op.table_name = table_name;
  record_index_op(op);
  write_buffer_ops_.push_back(std::move(op));
}

void LineairDBTransaction::buffer_delete(const std::string& table_name,
                                         const std::string& key) {
  LineairDBProxy::WriteOp op;
  op.type = LineairDBProxy::WriteOp::Type::Delete;
  op.key = key;
  op.table_name = table_name;
  write_buffer_ops_.push_back(std::move(op));
  record_write(table_name, key, false, ""); // value unused when not found
}

void LineairDBTransaction::buffer_delete_secondary_index(
    const std::string& table_name,
    const std::string& index_name,
    const std::string& secondary_key,
    const std::string& primary_key) {
  LineairDBProxy::WriteOp op;
  op.type = LineairDBProxy::WriteOp::Type::SecondaryIndexDelete;
  op.index_name = index_name;
  op.secondary_key = secondary_key;
  op.primary_key = primary_key;
  op.table_name = table_name;
  record_index_op(op);
  write_buffer_ops_.push_back(std::move(op));
}

std::optional<LineairDBTransaction::LocalRowEntry>
LineairDBTransaction::lookup_write_set(
    const std::string& table_name, const std::string& key) const {
  auto it = own_writes_index_.find(make_row_cache_key(table_name, key));
  if (it == own_writes_index_.end()) return std::nullopt;
  return own_writes_[it->second];
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
  return key >= start_key && key < end_key;
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
  // The recorded range is what the server saw; this only adds the rows this
  // transaction has written but not yet installed.
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != db_table_key) continue;
    if (op.type != LineairDBProxy::WriteOp::Type::Write &&
        op.type != LineairDBProxy::WriteOp::Type::Delete) {
      continue;
    }
    if (!key_is_in_range(op.key, start_key, end_key)) continue;

    remove_scan_row(rows, op.key);
    if (op.type == LineairDBProxy::WriteOp::Type::Write) {
      insert_scan_row_in_order(rows, op.key, op.value, reverse_scan);
    }
  }
}

bool LineairDBTransaction::has_pending_row_ops_in_range(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key) const {
  for (const auto& op : write_buffer_ops_) {
    if (op.table_name != table_name) continue;
    if (op.type != LineairDBProxy::WriteOp::Type::Write &&
        op.type != LineairDBProxy::WriteOp::Type::Delete) {
      continue;
    }
    if (op.key >= start_key && op.key < end_key) return true;
  }
  return false;
}

bool LineairDBTransaction::has_pending_secondary_ops_in_range(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key) const {
  const auto scope =
      pending_index_entries_.find(index_scope_key(table_name, index_name));
  if (scope == pending_index_entries_.end()) return false;
  const auto it = scope->second.lower_bound(start_key);
  return it != scope->second.end() && it->first < end_key;
}

void LineairDBTransaction::merge_pending_index_ops(
    const std::string& index_name, const std::string& start_key,
    const std::string& end_key,
    std::map<std::string, std::vector<std::string>>& groups) const {
  const auto scope =
      pending_index_entries_.find(index_scope_key(db_table_key, index_name));
  if (scope != pending_index_entries_.end()) {
    for (auto it = scope->second.lower_bound(start_key);
         it != scope->second.end() && it->first < end_key; ++it) {
      auto& primary_keys = groups[it->first];
      for (const auto& [pk, still_there] : it->second) {
        const auto at =
            std::lower_bound(primary_keys.begin(), primary_keys.end(), pk);
        const bool present = at != primary_keys.end() && *at == pk;
        if (!still_there) {
          if (present) primary_keys.erase(at);
        } else if (!present) {
          primary_keys.insert(at, pk);
        }
      }
    }
  }
  for (auto it = groups.begin(); it != groups.end();) {
    it = it->second.empty() ? groups.erase(it) : std::next(it);
  }
}

void LineairDBTransaction::record_write(const std::string& table_name,
                                              const std::string& key,
                                              bool found,
                                              const std::string& value) {
  // A later write/delete replaces any cached read for the same key
  drop_row_cache(table_name, key);

  std::string index_key = make_row_cache_key(table_name, key);
  auto it = own_writes_index_.find(index_key);
  if (it != own_writes_index_.end()) {
    LocalRowEntry& entry = own_writes_[it->second];
    entry.found = found;
    entry.value = value;
    return;
  }
  // Append first so a throwing push_back leaves both containers untouched; the
  // index entry then points at the element that is already in place.
  own_writes_.push_back({table_name, key, found, value});
  own_writes_index_.emplace(std::move(index_key), own_writes_.size() - 1);
}

void LineairDBTransaction::record_row_cache(
    const std::string& table_name, const std::string& key, bool found,
    const std::string& value, uint64_t tid) {
  // Re-staging overwrites the cached row; every consume has already appended
  // its TID to the read set, so no observation is lost.
  row_cache_[make_row_cache_key(table_name, key)] =
      LocalRowEntry{table_name, key, found, value, tid};
}

void LineairDBTransaction::append_base_row_read(
    const std::string& table_name, const std::string& key, uint64_t tid) {
  // Append every observation, no dedup (Silo read_set style): a repeated read
  // validates the same TID again.
  base_row_read_set_.push_back({table_name, key, tid});
}

void LineairDBTransaction::append_range_read(
    const LocalRangeScanEntry& scanned) {
  // The bounds describe the replay and result_keys is the observed key list in
  // scan order. Append, like the point and Silo read sets; a scan consumed
  // twice is revalidated twice: redundant but never wrong.
  LineairDBProxy::RangeReadEntry entry;
  entry.table_name = scanned.table_name;
  entry.start_key = scanned.start_key;
  entry.end_key = scanned.end_key;
  entry.row_limit = scanned.row_limit;
  entry.reverse_scan = scanned.reverse_scan;
  entry.result_keys.reserve(scanned.rows.size());
  for (const auto& row : scanned.rows) {
    entry.result_keys.push_back(row.first);
  }
  range_read_set_.push_back(std::move(entry));
}

void LineairDBTransaction::append_secondary_range_read(
    const LocalSecondaryScanEntry& scanned) {
  LineairDBProxy::RangeReadEntry entry;
  entry.table_name = scanned.table_name;
  entry.index_name = scanned.index_name;
  entry.start_key = scanned.start_key;
  entry.end_key = scanned.end_key;
  entry.row_limit = scanned.row_limit;
  entry.reverse_scan = scanned.reverse_scan;
  entry.result_keys = scanned.secondary_keys;
  entry.result_primary_keys = scanned.primary_keys;
  range_read_set_.push_back(std::move(entry));
}

void LineairDBTransaction::abort_server_refused(const char* what) {
  rpc_trace_.record_local_view(std::string("abort_server_refused:") + what);
  LOG_WARNING("Storage server refused %s table=%s", what, db_table_key.c_str());
  is_aborted_ = true;
  thd_mark_transaction_to_rollback(thread, 1);
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
  const bool pending_in_range =
      has_pending_row_ops_in_range(table_name, start_key, end_key);

  // Grouped for_each range probes are staged by exact start key. Try that
  // index before falling back to the wider range-cache scan below.
  auto idx_it = range_scan_start_index_.find(
      scan_cache_index_key(table_name, "", start_key));
  if (idx_it != range_scan_start_index_.end()) {
    for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
         ++rit) {
      const auto& e = range_scan_cache_[*rit];
      if (e.row_limit != 0 && pending_in_range) continue;
      // A limited window holds K rows adjacent to one endpoint: forward from
      // start_key (equal here by index key), reverse before end_key. Serving a
      // reverse window at a different end reads as EOF over rows it never held.
      if (e.row_limit != 0 && e.reverse_scan && end_key != e.end_key) continue;
      if (e.reverse_scan == reverse_scan && e.row_limit == row_limit &&
          end_key <= e.end_key) {
        // Copy and trim because the staged group may cover a wider range.
        LocalRangeScanEntry cached = e;
        cached.start_key = start_key;
        cached.end_key = end_key;
        cached.row_limit = row_limit;
        trim_range_entry(cached, start_key, end_key);
        return cached;
      }
    }
  }

  for (auto it = range_scan_cache_.rbegin();
       it != range_scan_cache_.rend(); ++it) {
    if (it->row_limit != 0 && pending_in_range) continue;
    const bool same_table = it->table_name == table_name;
    const bool same_direction = it->reverse_scan == reverse_scan;
    const bool same_limit = it->row_limit == row_limit;
    // A limited window is anchored at one endpoint and cannot serve a request
    // that moves it. Forward needs the same start and may narrow the end,
    // reverse the same end and may raise the start.
    const bool anchored =
        it->row_limit == 0 || (it->reverse_scan ? end_key == it->end_key
                                                : start_key == it->start_key);
    const bool covers_range =
        it->start_key <= start_key && end_key <= it->end_key;
    if (same_table && same_direction && same_limit && anchored && covers_range) {
      LocalRangeScanEntry cached = *it;
      cached.start_key = start_key;
      cached.end_key = end_key;
      cached.row_limit = row_limit;
      trim_range_entry(cached, start_key, end_key);
      return cached;
    }
  }

  // An unlimited request may be served by a limited window only over the same
  // bounds and only when the caller can fetch the rest; the window's own limit
  // is what gets recorded.
  if (allow_truncated && row_limit == 0 && !reverse_scan && !pending_in_range) {
    auto limited = range_scan_start_index_.find(
        scan_cache_index_key(table_name, "", start_key));
    if (limited != range_scan_start_index_.end()) {
      for (auto rit = limited->second.rbegin(); rit != limited->second.rend();
           ++rit) {
        const auto& e = range_scan_cache_[*rit];
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

void LineairDBTransaction::trim_range_entry(
    LocalRangeScanEntry& entry, const std::string& start_key,
    const std::string& end_key) {
  std::vector<std::pair<std::string, std::string>> rows;
  std::vector<uint64_t> row_tids;
  rows.reserve(entry.rows.size());
  row_tids.reserve(entry.rows.size());
  for (size_t i = 0; i < entry.rows.size(); ++i) {
    if (entry.rows[i].first >= start_key && entry.rows[i].first < end_key) {
      rows.push_back(entry.rows[i]);
      row_tids.push_back(entry.row_tids[i]);
    }
  }
  entry.rows = std::move(rows);
  entry.row_tids = std::move(row_tids);
}

void LineairDBTransaction::trim_secondary_entry(
    LocalSecondaryScanEntry& entry, const std::string& start_key,
    const std::string& end_key) {
  std::vector<std::string> secondary_keys;
  std::vector<std::string> primary_keys;
  secondary_keys.reserve(entry.secondary_keys.size());
  primary_keys.reserve(entry.secondary_keys.size());
  for (size_t i = 0; i < entry.secondary_keys.size(); ++i) {
    if (entry.secondary_keys[i] >= start_key &&
        entry.secondary_keys[i] < end_key) {
      secondary_keys.push_back(entry.secondary_keys[i]);
      primary_keys.push_back(entry.primary_keys[i]);
    }
  }
  entry.secondary_keys = std::move(secondary_keys);
  entry.primary_keys = std::move(primary_keys);
}

std::optional<LineairDBTransaction::LocalSecondaryScanEntry>
LineairDBTransaction::lookup_secondary_scan_cache(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    bool reverse_scan, uint64_t row_limit) const {
  // A limited window holds the first entries the storage had, so an entry of
  // this transaction inside the range could belong in it. An unlimited one
  // merges cleanly.
  const bool pending_in_range =
      row_limit == 0 ? false
                     : has_pending_secondary_ops_in_range(
                           table_name, index_name, start_key, end_key);

  // Grouped for_each secondary probes are staged by exact start key. Try that
  // index before falling back to the wider secondary-cache scan below.
  auto idx_it = secondary_scan_start_index_.find(
      scan_cache_index_key(table_name, index_name, start_key));
  if (idx_it != secondary_scan_start_index_.end()) {
    for (auto rit = idx_it->second.rbegin(); rit != idx_it->second.rend();
         ++rit) {
      const auto& e = secondary_scan_cache_[*rit];
      if (e.row_limit != 0 && pending_in_range) continue;
      // A limited window holds K entries adjacent to one endpoint, so a
      // reverse window at another end would report an end it never held.
      if (e.row_limit != 0 && e.reverse_scan && end_key != e.end_key) continue;
      // A window holds its keys in its own direction, so only a request of
      // that direction can consume it in order.
      if (e.row_limit == row_limit && end_key <= e.end_key &&
          e.reverse_scan == reverse_scan) {
        LocalSecondaryScanEntry cached = e;
        cached.start_key = start_key;
        cached.end_key = end_key;
        cached.row_limit = row_limit;
        trim_secondary_entry(cached, start_key, end_key);
        return cached;
      }
    }
  }

  for (auto it = secondary_scan_cache_.rbegin();
       it != secondary_scan_cache_.rend(); ++it) {
    if (it->row_limit != 0 && pending_in_range) continue;
    const bool same_index =
        it->table_name == table_name && it->index_name == index_name;
    const bool same_limit = it->row_limit == row_limit;
    // A limited window is anchored at one endpoint and cannot serve a request
    // that moves it. Forward needs the same start and may narrow the end,
    // reverse the same end and may raise the start.
    const bool anchored =
        it->row_limit == 0 || (it->reverse_scan ? end_key == it->end_key
                                                : start_key == it->start_key);
    const bool covers_range =
        it->start_key <= start_key && end_key <= it->end_key;
    if (same_index && same_limit && anchored && covers_range &&
        it->reverse_scan == reverse_scan) {
      LocalSecondaryScanEntry cached = *it;
      cached.start_key = start_key;
      cached.end_key = end_key;
      cached.row_limit = row_limit;
      trim_secondary_entry(cached, start_key, end_key);
      return cached;
    }
  }
  return std::nullopt;
}

bool LineairDBTransaction::end_transaction(bool *transport_error,
                                           bool *duplicate_key) {
  if (transport_error != nullptr) *transport_error = transport_error_;
  if (duplicate_key != nullptr) *duplicate_key = duplicate_key_abort_;
  const bool was_aborted = is_aborted_;

  std::vector<std::pair<std::string, int64_t>> server_deltas;
  if (!was_aborted && !rowcount_deltas_.empty()) {
    server_deltas.reserve(rowcount_deltas_.size());
    for (const auto& entry : rowcount_deltas_) {
      if (entry.share != nullptr && entry.delta != 0)
        server_deltas.emplace_back(entry.table_name, entry.delta);
    }
  }

  bool committed = false;
  std::string abort_detail;
  if (!was_aborted) {
    bool commit_transport_error = false;
    bool commit_duplicate_key = false;
    committed = lineairdb_proxy->tx_commit(
        base_row_read_set_, range_read_set_, write_buffer_ops_, server_deltas,
        &abort_detail, &commit_duplicate_key, &commit_transport_error);
    if (transport_error != nullptr) {
      *transport_error = transport_error_ || commit_transport_error;
    }
    if (commit_duplicate_key) {
      duplicate_key_abort_ = true;
      if (duplicate_key != nullptr) *duplicate_key = true;
    }
    if (!committed && !abort_detail.empty()) {
      rpc_trace_.record_local_view("abort_validate_" + abort_detail);
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
        rpc_trace_.finalize_jsonl(committed));
  }
  lineairdb_proxy->set_current_trace(nullptr);

  delete this;
  return committed;
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  rpc_trace_.start(std::this_thread::get_id());
  lineairdb_proxy->set_current_trace(&rpc_trace_);

  registered_ = true;
  is_aborted_ = false;
  if (thd_is_transaction()) {
    isTransaction = true;
    register_transaction_to_mysql();
  }
  else {
    register_single_statement_to_mysql();
  }
}

void LineairDBTransaction::set_status_to_abort() { is_aborted_ = true; }

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
