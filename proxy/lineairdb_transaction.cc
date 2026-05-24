#include "lineairdb_transaction.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <thread>
#include <unordered_set>

namespace {

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

bool range_validation_is_logical(
    const LineairDBProxy::RangeValidationEntry& entry) {
  return !entry.end_key.empty();
}

bool range_validation_can_be_sliced(
    const std::vector<LineairDBProxy::RangeValidationEntry>& ranges,
    const std::vector<LineairDBProxy::IndexValidationEntry>& indexes) {
  if (!indexes.empty()) return false;
  for (const auto& range : ranges) {
    if (!range_validation_is_logical(range)) return false;
  }
  return !ranges.empty();
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
  if (auto entry = lookup_local_write_set(db_table_key, key)) {
    rpc_trace_.record_local_view("read_write_hit");
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Repeat exact-key reads can use the local read set
  if (auto entry = lookup_local_read_set(db_table_key, key)) {
    rpc_trace_.record_local_view("read_cache_hit");
    activate_local_read(*entry);
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Normal path misses go to the server; oneshot plans must prefetch them
  rpc_trace_.record_local_view("read_miss");
  if (oneshot_mode_) {
    rpc_trace_.record_local_view("abort_oneshot_read_miss");
    is_aborted_ = true;
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  last_read_value_ = lineairdb_proxy->tx_read(this, key);
  if (last_read_value_.empty()) {
    record_local_read(db_table_key, key, false, ""); // value unused when not found
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  record_local_read(db_table_key, key, true, last_read_value_);

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
    if (auto entry = lookup_local_write_set(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_write_hit");
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    if (auto entry = lookup_local_read_set(db_table_key, keys[i])) {
      rpc_trace_.record_local_view("batch_cache_hit");
      activate_local_read(*entry);
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    rpc_trace_.record_local_view("batch_miss");
    rpc_positions.push_back(i);
    rpc_keys.push_back(keys[i]);
  }

  // Oneshot plans must fetch every key up front; misses mean the plan is short
  if (oneshot_mode_ && !rpc_keys.empty()) {
    rpc_trace_.record_local_view("abort_oneshot_batch_miss");
    is_aborted_ = true;
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
      record_local_read(db_table_key, keys[pos], pairs[pos].first, pairs[pos].second);
    }
  }
  return pairs;
}

void LineairDBTransaction::prefetch_stateless_reads(
    const std::vector<LineairDBProxy::StatelessReadKey>& reads) {
  if (!oneshot_mode_ || reads.empty()) return;

  std::vector<LineairDBProxy::StatelessReadKey> rpc_reads;
  rpc_reads.reserve(reads.size());
  std::unordered_set<std::string> seen;
  seen.reserve(reads.size());

  // Keep the plan prefetch to rows not already covered by the local view
  for (const auto& read : reads) {
    const std::string seen_key = read.table_name + '\0' + read.key;
    if (!seen.insert(seen_key).second) continue;
    if (lookup_local_write_set(read.table_name, read.key)) continue;
    if (lookup_local_read_set(read.table_name, read.key)) continue;
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
      record_local_read(read.table_name, read.key, true, result.value,
                        result.tid, true);
    } else {
      record_local_read(read.table_name, read.key, false, "",
                        result.tid, true); // value unused when not found
    }
  }
}

void LineairDBTransaction::execute_read_plan(
    const std::vector<LineairDBProxy::ReadPlanStep>& steps) {
  if (!oneshot_mode_ || steps.empty()) return;

  rpc_trace_.record_local_view("plan_request:steps=" +
                               std::to_string(steps.size()));
  auto result = lineairdb_proxy->tx_execute_read_plan(steps);
  if (!result.ok || result.steps.size() != steps.size()) {
    rpc_trace_.record_local_view("abort_read_plan_rpc");
    is_aborted_ = true;
    return;
  }

  for (size_t i = 0; i < result.steps.size() && i < steps.size(); ++i) {
    const auto& step = steps[i];
    const auto& step_result = result.steps[i];

    if (!step.is_scan && !step.for_each) {
      rpc_trace_.record_local_view(trace_count_event(
          step_result.found ? "plan_fetch:R:hit" : "plan_fetch:R:miss",
          step.table_name, 1));
      if (step_result.found) {
        record_local_read(step.table_name, step_result.actual_key, true,
                          step_result.value, step_result.tid, true);
      } else {
        record_local_read(step.table_name, step_result.actual_key, false, "",
                          step_result.tid, true);
      }
      continue;
    }

    rpc_trace_.record_local_view(trace_plan_scan_event(
        step.table_name, step.index_name, step_result.scan_keys.size(),
        step_result.scan_values.size(), step.scan_limit, step.for_each));

    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint64_t> row_tids;
    rows.reserve(step_result.scan_keys.size());
    row_tids.reserve(step_result.scan_keys.size());
    for (size_t j = 0; j < step_result.scan_keys.size(); ++j) {
      const std::string& key = step_result.scan_keys[j];
      const std::string value =
          j < step_result.scan_values.size() ? step_result.scan_values[j] : "";
      const uint64_t tid =
          j < step_result.scan_tids.size() ? step_result.scan_tids[j] : 0;
      const bool found = !value.empty();
      record_local_read(step.table_name, key, found, value, tid, true);
      if (found) {
        rows.emplace_back(key, value);
        row_tids.push_back(tid);
      }
    }

    if (step.for_each) continue;

    if (step.index_name.empty()) {
      local_range_scans_.push_back(
          {step.table_name, step_result.actual_start_key,
           step_result.actual_end_key, step.reverse_scan, step.scan_limit,
           std::move(rows), std::move(row_tids), step_result.range_versions,
           step_result.index_reads});
    } else {
      LocalSecondaryScanEntry cached;
      cached.table_name = step.table_name;
      cached.index_name = step.index_name;
      cached.start_key = step_result.actual_start_key;
      cached.end_key = step_result.actual_end_key;
      cached.reverse_scan = step.reverse_scan;
      cached.row_limit = step.scan_limit;
      cached.secondary_keys.reserve(step_result.secondary_keys.size());
      cached.primary_keys.reserve(step_result.scan_keys.size());
      for (const auto& key : step_result.secondary_keys) {
        cached.secondary_keys.push_back(key);
      }
      for (const auto& key : step_result.scan_keys) {
        cached.primary_keys.push_back(key);
      }
      cached.range_versions = step_result.range_versions;
      cached.index_reads = step_result.index_reads;
      local_secondary_scans_.push_back(std::move(cached));
    }
  }
}

bool LineairDBTransaction::batch_write(
    const std::string& table_name,
    const std::vector<LineairDBProxy::BatchOp>& ops) {
  if (oneshot_mode_) {
    for (auto op : ops) {
      if (op.table_name.empty()) op.table_name = table_name;
      if (op.type == LineairDBProxy::BatchOp::Type::Write) {
        record_local_write(op.table_name, op.key, true, op.value);
      } else if (op.type == LineairDBProxy::BatchOp::Type::Delete) {
        record_local_write(op.table_name, op.key, false, ""); // value unused when not found
      } else if (op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexWrite ||
                 op.type == LineairDBProxy::BatchOp::Type::SecondaryIndexDelete) {
        drop_local_secondary_scans(op.table_name, op.index_name);
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
  if (oneshot_mode_) {
    buffer_write(db_table_key, key, value);
    return true;
  }

  const bool ok = lineairdb_proxy->tx_write(this, key, value);
  if (ok) record_local_write(db_table_key, key, true, value);
  return ok;
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;
  if (oneshot_mode_) {
    buffer_delete(db_table_key, key);
    return true;
  }

  const bool ok = lineairdb_proxy->tx_delete(this, key);
  if (ok) record_local_write(db_table_key, key, false, ""); // value unused when not found
  return ok;
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key) {
  if (table_is_not_chosen()) return {};
  if (oneshot_mode_) {
    const std::string end_key = next_lexicographic_key(secondary_key);
    if (end_key.empty()) {
      rpc_trace_.record_local_view("abort_secondary_key_end");
      is_aborted_ = true;
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
  if (oneshot_mode_) {
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
  if (oneshot_mode_) {
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
  if (oneshot_mode_) {
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
  if (oneshot_mode_) {
    if (auto cached =
            lookup_local_range_scan(db_table_key, start_key, end_key, false, 0)) {
      std::vector<std::pair<std::string, std::string>> rows = cached->rows;
      rpc_trace_.record_local_view(
          trace_count_event("use_pk_key_scan", db_table_key, rows.size()));
      for (size_t i = 0; i < rows.size() && i < cached->row_tids.size(); ++i) {
        record_stateless_read(db_table_key, rows[i].first, true,
                              cached->row_tids[i]);
      }
      // Activate validation against the pre-merge key list in
      // cached->range_versions: server-side re-walk at commit cannot see
      // this tx's pending writes, so validating against the post-merge
      // view would false-abort on every own-insert / own-delete in range.
      activate_range_validation(cached->range_versions, cached->index_reads);

      merge_pending_rows_into_range_scan(rows, start_key, end_key, false);
      std::vector<std::string> keys;
      keys.reserve(rows.size());
      for (const auto& row : rows) keys.push_back(row.first);
      return keys;
    }

    rpc_trace_.record_local_view("abort_oneshot_pk_key_scan_miss");
    is_aborted_ = true;
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
                                                            bool reverse_scan) {
  if (table_is_not_chosen()) return {};
  if (oneshot_mode_) {
    if (auto cached = lookup_local_range_scan(
            db_table_key, start_key, end_key, reverse_scan, row_limit)) {
      std::vector<std::pair<std::string, std::string>> pairs = cached->rows;
      for (size_t i = 0; i < cached->rows.size() && i < cached->row_tids.size();
           ++i) {
        record_stateless_read(db_table_key, cached->rows[i].first, true,
                              cached->row_tids[i]);
      }
      // See get_matching_keys_in_range above for the rationale.
      activate_range_validation(cached->range_versions, cached->index_reads);

      merge_pending_rows_into_range_scan(pairs, start_key, end_key,
                                         reverse_scan);
      if (row_limit > 0 && pairs.size() > row_limit) {
        pairs.resize(static_cast<size_t>(row_limit));
      }
      rpc_trace_.record_local_view(
          trace_count_event("use_pk_value_scan", db_table_key, pairs.size()));
      return pairs;
    }

    rpc_trace_.record_local_view("abort_oneshot_pk_value_scan_miss:" +
                                 std::to_string(start_key.size()) + ":" +
                                 std::to_string(end_key.size()));
    is_aborted_ = true;
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
  if (oneshot_mode_) {
    const std::string prefix_end = next_lexicographic_key(prefix);
    if (prefix_end.empty()) {
      rpc_trace_.record_local_view("abort_pk_prefix_end");
      is_aborted_ = true;
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
                                                         std::string end_key) {
  if (table_is_not_chosen()) return {};
  if (oneshot_mode_) {
    if (has_pending_secondary_ops_for_index(db_table_key, index_name)) {
      rpc_trace_.record_local_view("abort_secondary_scan_after_secondary_write:" +
                                   index_name);
      is_aborted_ = true;
      return {};
    }

    if (auto cached = lookup_local_secondary_scan(
            db_table_key, index_name, start_key, end_key, false, 0)) {
      rpc_trace_.record_local_view("use_si_scan:" + db_table_key + ":" +
                                   index_name + ":n=" +
                                   std::to_string(cached->primary_keys.size()));
      activate_range_validation(cached->range_versions, cached->index_reads);
      return cached->primary_keys;
    }

    rpc_trace_.record_local_view("abort_oneshot_secondary_scan_miss:" +
                                 index_name + ":" +
                                 std::to_string(start_key.size()) + ":" +
                                 std::to_string(end_key.size()));
    is_aborted_ = true;
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
  if (oneshot_mode_) {
    const std::string prefix_end = next_lexicographic_key(prefix);
    if (prefix_end.empty()) {
      rpc_trace_.record_local_view("abort_secondary_prefix_end");
      is_aborted_ = true;
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
  record_local_write(table_name, key, true, value);

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
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
  drop_local_secondary_scans(table_name, index_name);

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
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
  record_local_write(table_name, key, false, ""); // value unused when not found

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
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
  drop_local_secondary_scans(table_name, index_name);

  if (!oneshot_mode_ && write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

bool LineairDBTransaction::flush_write_buffer() {
  if (write_buffer_ops_.empty()) return true;
  if (oneshot_mode_) return true;
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
  if (oneshot_mode_) return true;
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
LineairDBTransaction::lookup_local_write_set(
    const std::string& table_name, const std::string& key) const {
  for (auto it = local_write_set_.rbegin(); it != local_write_set_.rend(); ++it) {
    if (it->table_name == table_name && it->key == key) {
      return *it;
    }
  }
  return std::nullopt;
}

std::optional<LineairDBTransaction::LocalRowEntry>
LineairDBTransaction::lookup_local_read_set(
    const std::string& table_name, const std::string& key) const {
  for (auto it = local_read_set_.rbegin(); it != local_read_set_.rend(); ++it) {
    if (it->table_name == table_name && it->key == key) return *it;
  }
  return std::nullopt;
}

void LineairDBTransaction::drop_local_read(const std::string& table_name,
                                           const std::string& key) {
  for (auto it = local_read_set_.begin(); it != local_read_set_.end(); ++it) {
    if (it->table_name == table_name && it->key == key) {
      local_read_set_.erase(it);
      return;
    }
  }
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

void LineairDBTransaction::drop_local_secondary_scans(
    const std::string& table_name,
    const std::string& index_name) {
  std::vector<LocalSecondaryScanEntry> kept;
  kept.reserve(local_secondary_scans_.size());

  // A buffered SI write/delete can make old scan results incomplete
  for (const auto& entry : local_secondary_scans_) {
    if (entry.table_name == table_name && entry.index_name == index_name) {
      continue;
    }
    kept.push_back(entry);
  }
  local_secondary_scans_.swap(kept);
}

void LineairDBTransaction::record_local_write(const std::string& table_name,
                                              const std::string& key,
                                              bool found,
                                              const std::string& value) {
  // A later write/delete replaces any cached read for the same key
  drop_local_read(table_name, key);

  for (auto& entry : local_write_set_) {
    if (entry.table_name == table_name && entry.key == key) {
      entry.found = found;
      entry.value = value;
      return;
    }
  }
  local_write_set_.push_back({table_name, key, found, value});
}

void LineairDBTransaction::record_local_read(const std::string& table_name,
                                             const std::string& key,
                                             bool found,
                                             const std::string& value,
                                             uint64_t tid,
                                             bool validate_on_use) {
  for (auto& entry : local_read_set_) {
    if (entry.table_name == table_name && entry.key == key) {
      entry.found = found;
      entry.value = value;
      entry.tid = tid;
      entry.validate_on_use = validate_on_use;
      return;
    }
  }
  local_read_set_.push_back(
      {table_name, key, found, value, tid, validate_on_use});
}

void LineairDBTransaction::record_stateless_read(const std::string& table_name,
                                                 const std::string& key,
                                                 bool found,
                                                 uint64_t tid) {
  for (auto& entry : stateless_read_set_) {
    if (entry.table_name == table_name && entry.key == key) {
      entry.found = found;
      entry.tid = tid;
      return;
    }
  }
  stateless_read_set_.push_back({table_name, key, tid, found});
}

void LineairDBTransaction::activate_local_read(const LocalRowEntry& entry) {
  if (!entry.validate_on_use) return;
  rpc_trace_.record_local_view(
      trace_count_event("use_point_read", entry.table_name, 1));
  record_stateless_read(entry.table_name, entry.key, entry.found, entry.tid);
}

void LineairDBTransaction::activate_range_validation(
    const std::vector<LineairDBProxy::RangeValidationEntry>& ranges,
    const std::vector<LineairDBProxy::IndexValidationEntry>& indexes) {
  for (const auto& range : ranges) {
    bool found = false;
    for (const auto& active : range_validation_set_) {
      const bool same_node =
          active.table_name == range.table_name &&
          active.index_name == range.index_name &&
          active.owner_ptr == range.owner_ptr &&
          active.node_ptr == range.node_ptr &&
          active.version == range.version;
      const bool same_logical =
          active.start_key == range.start_key &&
          active.end_key == range.end_key &&
          active.row_limit == range.row_limit &&
          active.reverse_scan == range.reverse_scan &&
          active.result_keys == range.result_keys &&
          active.result_primary_keys == range.result_primary_keys;
      if (same_node && same_logical) {
        found = true;
        break;
      }
    }
    if (!found) range_validation_set_.push_back(range);
  }

  for (const auto& index : indexes) {
    bool found = false;
    for (const auto& active : index_validation_set_) {
      if (active.table_name == index.table_name &&
          active.index_name == index.index_name &&
          active.key == index.key &&
          active.tid == index.tid &&
          active.found == index.found) {
        found = true;
        break;
      }
    }
    if (!found) index_validation_set_.push_back(index);
  }
}

std::optional<LineairDBTransaction::LocalRangeScanEntry>
LineairDBTransaction::lookup_local_range_scan(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key, bool reverse_scan, uint64_t row_limit) const {
  for (auto it = local_range_scans_.rbegin();
       it != local_range_scans_.rend(); ++it) {
    const bool same_table = it->table_name == table_name;
    const bool same_direction = it->reverse_scan == reverse_scan;
    const bool same_limit = it->row_limit == row_limit;
    const bool covers_range =
        it->start_key <= start_key && end_key <= it->end_key;
    const bool exact_range =
        it->start_key == start_key && it->end_key == end_key;
    const bool can_slice_validation =
        range_validation_can_be_sliced(it->range_versions, it->index_reads);
    if (same_table && same_direction && same_limit && covers_range &&
        (exact_range || can_slice_validation)) {
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
      for (auto& range : cached.range_versions) {
        if (!range_validation_is_logical(range)) continue;
        range.start_key = start_key;
        range.end_key = end_key;
        range.result_keys.clear();
        range.result_primary_keys.clear();
        for (const auto& row : cached.rows) {
          range.result_keys.push_back(row.first);
        }
      }
      return cached;
    }
  }
  return std::nullopt;
}

std::optional<LineairDBTransaction::LocalSecondaryScanEntry>
LineairDBTransaction::lookup_local_secondary_scan(
    const std::string& table_name, const std::string& index_name,
    const std::string& start_key, const std::string& end_key,
    bool reverse_scan, uint64_t row_limit) const {
  (void)reverse_scan; // SI cache keeps the query-specific order from the plan

  for (auto it = local_secondary_scans_.rbegin();
       it != local_secondary_scans_.rend(); ++it) {
    const bool same_index =
        it->table_name == table_name && it->index_name == index_name;
    const bool same_limit = it->row_limit == row_limit;
    const bool covers_range =
        it->start_key <= start_key && end_key <= it->end_key;
    const bool exact_range =
        it->start_key == start_key && it->end_key == end_key;
    const bool can_slice_validation =
        range_validation_can_be_sliced(it->range_versions, it->index_reads);
    if (same_index && same_limit && covers_range &&
        (exact_range || can_slice_validation)) {
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
      for (auto& range : cached.range_versions) {
        if (!range_validation_is_logical(range)) continue;
        range.start_key = start_key;
        range.end_key = end_key;
        range.result_keys = cached.secondary_keys;
        range.result_primary_keys = cached.primary_keys;
      }
      return cached;
    }
  }
  return std::nullopt;
}

bool LineairDBTransaction::fallback_to_normal_transaction(const char* reason) {
  if (!oneshot_mode_) return true;

  // A clean transaction can still switch to the normal scan-capable path
  const bool has_oneshot_state =
      !stateless_read_set_.empty() || !write_buffer_ops_.empty() ||
      !rowcount_deltas_.empty() || !local_read_set_.empty() ||
      !local_write_set_.empty() || !range_validation_set_.empty() ||
      !index_validation_set_.empty() || !local_range_scans_.empty() ||
      !local_secondary_scans_.empty();
  if (!has_oneshot_state) {
    oneshot_mode_ = false;
    oneshot_registered_ = false;
    begin_transaction();
    return !is_aborted_;
  }

  // Mixing stateless point reads with scans would need phantom tracking
  LOG_WARNING("Oneshot fallback blocked by prior local state: %s", reason);
  rpc_trace_.record_local_view(std::string("abort_fallback_") + reason);
  is_aborted_ = true;
  return false;
}

bool LineairDBTransaction::oneshot_validate_and_commit() {
  bool was_aborted = is_aborted_;

  std::vector<LineairDBProxy::StatelessReadKey> reads;
  std::vector<uint64_t> read_tids;
  std::vector<bool> read_found;
  if (!was_aborted) {
    reads.reserve(stateless_read_set_.size());
    read_tids.reserve(stateless_read_set_.size());
    read_found.reserve(stateless_read_set_.size());
    for (const auto& entry : stateless_read_set_) {
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
        reads, read_tids, read_found, range_validation_set_,
        index_validation_set_,
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

  if (oneshot_mode_) {
    oneshot_registered_ = true;
    is_aborted_ = false;
    if (thd_is_transaction()) {
      isTransaction = true;
      register_transaction_to_mysql();
    }
    else {
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
  if (oneshot_mode_ || tx_id == -1) {
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
  if (oneshot_mode_) {
    return oneshot_validate_and_commit();
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
