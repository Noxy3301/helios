#include "lineairdb_transaction.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <thread>

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
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // Repeat exact-key reads can use the local read set
  if (auto entry = lookup_local_read_set(db_table_key, key)) {
    if (!entry->found) return {nullptr, 0};
    last_read_value_ = entry->value;
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }

  // First exact-key read goes to the server and enters the local read set
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
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    if (auto entry = lookup_local_read_set(db_table_key, keys[i])) {
      pairs[i] = {entry->found, entry->value};
      continue;
    }
    rpc_positions.push_back(i);
    rpc_keys.push_back(keys[i]);
  }

  // Fetch only cache misses; tx_batch_read() returns rows in rpc_keys order
  //   Example: keys=[A,B,C], B is local -> rpc_keys=[A,C],
  //            rpc_positions=[0,2], so RPC results fill pairs[0] and pairs[2].
  if (!rpc_keys.empty()) {
    auto results = lineairdb_proxy->tx_batch_read(this, rpc_keys);
    for (size_t i = 0; i < results.size(); ++i) {
      // Map each RPC result back to the original keys[] position
      const size_t pos = rpc_positions[i];
      pairs[pos] = {results[i].found, std::move(results[i].value)};
      record_local_read(db_table_key, keys[pos], pairs[pos].first, pairs[pos].second);
    }
  }
  return pairs;
}

bool LineairDBTransaction::batch_write(
    const std::string& table_name,
    const std::vector<LineairDBProxy::BatchOp>& ops) {
  return lineairdb_proxy->tx_batch_write(this, table_name, ops);
}

std::vector<std::string>
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};
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

  const bool ok = lineairdb_proxy->tx_write(this, key, value);
  if (ok) record_local_write(db_table_key, key, true, value);
  return ok;
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;

  const bool ok = lineairdb_proxy->tx_delete(this, key);
  if (ok) record_local_write(db_table_key, key, false, ""); // value unused when not found
  return ok;
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_read_secondary_index(this, index_name, secondary_key);
}

bool LineairDBTransaction::write_secondary_index(std::string index_name,
                                                 std::string secondary_key,
                                                 const std::string primary_key) {
  if (table_is_not_chosen()) return false;

  return lineairdb_proxy->tx_write_secondary_index(this, index_name, secondary_key, primary_key);
}

bool LineairDBTransaction::delete_secondary_index(std::string index_name,
                                                  std::string secondary_key,
                                                  const std::string primary_key) {
  if (table_is_not_chosen()) return false;

  return lineairdb_proxy->tx_delete_secondary_index(this, index_name, secondary_key, primary_key);
}

bool LineairDBTransaction::update_secondary_index(std::string index_name,
                                                  std::string old_secondary_key,
                                                  std::string new_secondary_key,
                                                  const std::string primary_key) {
  if (table_is_not_chosen()) return false;

  return lineairdb_proxy->tx_update_secondary_index(this, index_name, old_secondary_key, new_secondary_key, primary_key);
}

// Primary key scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_keys_in_range(std::string start_key,
                                                 std::string end_key) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_keys_in_range(this, start_key, end_key);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_range(std::string start_key,
                                                            std::string end_key,
                                                            uint64_t row_limit,
                                                            bool reverse_scan) {
  if (table_is_not_chosen()) return {};
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
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_key_in_range(this, start_key, end_key);
}

std::optional<std::string>
LineairDBTransaction::fetch_first_key_with_prefix(const std::string &prefix,
                                                  const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_first_key_with_prefix(this, prefix, prefix_end);
}

std::optional<std::string>
LineairDBTransaction::fetch_next_key_with_prefix(const std::string &last_key,
                                                 const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_next_key_with_prefix(this, last_key, prefix_end);
}

// Secondary index scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_in_range(std::string index_name,
                                                         std::string start_key,
                                                         std::string end_key) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_primary_keys_in_range(this, index_name, start_key, end_key);
}

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_from_prefix(std::string index_name,
                                                            std::string prefix) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_get_matching_primary_keys_from_prefix(this, index_name, prefix);
}

std::optional<std::string>
LineairDBTransaction::fetch_last_primary_key_in_secondary_range(const std::string &index_name,
                                                                const std::string &start_key,
                                                                const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer_for_table(db_table_key);

  return lineairdb_proxy->tx_fetch_last_primary_key_in_secondary_range(this, index_name, start_key, end_key);
}

std::optional<SecondaryIndexEntry>
LineairDBTransaction::fetch_last_secondary_entry_in_range(const std::string &index_name,
                                                          const std::string &start_key,
                                                          const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
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

  if (write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
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

  if (write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
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

  if (write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
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

  if (write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

bool LineairDBTransaction::flush_write_buffer() {
  if (write_buffer_ops_.empty()) return true;
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
                                             const std::string& value) {
  for (auto& entry : local_read_set_) {
    if (entry.table_name == table_name && entry.key == key) {
      entry.found = found;
      entry.value = value;
      return;
    }
  }
  local_read_set_.push_back({table_name, key, found, value});
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  rpc_trace_.start(-1, std::this_thread::get_id());
  lineairdb_proxy->set_current_trace(&rpc_trace_);

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
  // Skip TX_ABORT RPC if the server already knows (is_aborted_ was set from an RPC response).
  if (!is_aborted_) {
    lineairdb_proxy->tx_abort(tx_id);
  }
  is_aborted_ = true;
}

bool LineairDBTransaction::end_transaction() {
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
