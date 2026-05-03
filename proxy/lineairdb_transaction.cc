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

  std::string cache_key = make_pk_cache_key(db_table_key, key);

  // Cache hit: return the previously-fetched value without an RPC.
  auto hit = read_cache_.find(cache_key);
  if (hit != read_cache_.end()) {
    last_read_value_ = hit->second;
    if (last_read_value_.empty()) {
      return std::pair<const std::byte *const, const size_t>{nullptr, 0};
    }
    return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
  }
  // Negative cache hit: this key was previously confirmed to not exist.
  if (read_cache_misses_.count(cache_key) > 0) {
    last_read_value_.clear();
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }

  // Cache miss: issue the RPC and memoize the result for the rest of the tx.
  if (table_has_pending_write_for_key(db_table_key, key)) {
    flush_write_buffer_for_table(db_table_key);
  }
  last_read_value_ = lineairdb_proxy->tx_read(this, key);

  if (last_read_value_.empty()) {
    read_cache_misses_.insert(std::move(cache_key));
    return std::pair<const std::byte *const, const size_t>{nullptr, 0};
  }
  read_cache_.emplace(std::move(cache_key), last_read_value_);
  return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
}

std::vector<std::pair<bool, std::string>>
LineairDBTransaction::batch_read(const std::vector<std::string>& keys) {
  if (table_is_not_chosen()) return {};

  // Resolve hits up front; only RPC for the misses. The handler-side
  // caller (MRR / SI fetch) needs results in input order, so we maintain
  // index mappings to splice fetched rows back in the right slots.
  std::vector<std::pair<bool, std::string>> pairs(keys.size());
  std::vector<std::string> miss_keys;
  std::vector<size_t> miss_indices;
  miss_keys.reserve(keys.size());
  miss_indices.reserve(keys.size());

  // Pass 1: classify each input key as cache hit / negative hit / miss.
  for (size_t i = 0; i < keys.size(); i++) {
    std::string cache_key = make_pk_cache_key(db_table_key, keys[i]);
    // Cache hit
    auto hit = read_cache_.find(cache_key);
    if (hit != read_cache_.end()) {
      pairs[i] = {!hit->second.empty(), hit->second};
      continue;
    }
    // Negative cache hit
    if (read_cache_misses_.count(cache_key) > 0) {
      pairs[i] = {false, std::string()};
      continue;
    }
    // Miss: collect for the batched server RPC issued in Pass 2 below.
    miss_keys.push_back(keys[i]);
    miss_indices.push_back(i);
  }

  // All keys were served from the cache; no RPC needed.
  if (miss_keys.empty()) return pairs;

  // Pass 2: one RPC for the misses, then splice results back into pairs[]
  // at their original input indices and memoize each result.
  for (const auto& key : miss_keys) {
    if (table_has_pending_write_for_key(db_table_key, key)) {
      flush_write_buffer_for_table(db_table_key);
      break;
    }
  }
  auto fetched = lineairdb_proxy->tx_batch_read(this, miss_keys);

  const size_t n = std::min(fetched.size(), miss_indices.size());
  for (size_t j = 0; j < n; j++) {
    const size_t idx = miss_indices[j];
    pairs[idx] = {fetched[j].found, fetched[j].value};
    std::string cache_key = make_pk_cache_key(db_table_key, miss_keys[j]);
    if (fetched[j].found) {
      read_cache_.emplace(std::move(cache_key), fetched[j].value);
    } else {
      read_cache_misses_.insert(std::move(cache_key));
    }
  }

  return pairs;
}

bool LineairDBTransaction::batch_write(
    const std::string& table_name,
    const std::vector<LineairDBProxy::BatchWriteOp>& writes,
    const std::vector<LineairDBProxy::BatchSecondaryIndexOp>& si_writes) {
  for (const auto& w : writes) {
    invalidate_pk_cache_entry(table_name, w.key);
  }
  return lineairdb_proxy->tx_batch_write(this, table_name, writes, si_writes);
}

std::vector<std::string>
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

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
  if (table_has_pending_write_with_prefix(db_table_key, first_key_part)) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto key_value_pairs = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, first_key_part);

  std::vector<std::string> keyList;
  for (const auto& kv : key_value_pairs) {
    keyList.push_back(kv.key);
  }

  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;

  invalidate_pk_cache_entry(db_table_key, key);
  flush_write_buffer_for_table(db_table_key);
  return lineairdb_proxy->tx_write(this, key, value);
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;

  invalidate_pk_cache_entry(db_table_key, key);
  flush_write_buffer_for_table(db_table_key);
  return lineairdb_proxy->tx_delete(this, key);
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key) {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

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
  if (table_has_pending_write_in_range(db_table_key, start_key, end_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_get_matching_keys_in_range(this, start_key, end_key);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_range(std::string start_key,
                                                            std::string end_key,
                                                            uint32_t limit) {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_write_in_range(db_table_key, start_key, end_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_in_range(
      this, start_key, end_key, limit);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  return pairs;
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_from_prefix(std::string prefix) {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_write_with_prefix(db_table_key, prefix)) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, prefix);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  return pairs;
}

std::optional<std::string>
LineairDBTransaction::fetch_last_key_in_range(const std::string &start_key,
                                              const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (table_has_pending_write_in_range(db_table_key, start_key, end_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_fetch_last_key_in_range(this, start_key, end_key);
}

std::optional<std::string>
LineairDBTransaction::fetch_first_key_with_prefix(const std::string &prefix,
                                                  const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  if (table_has_pending_write_in_range(db_table_key, prefix, prefix_end)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_fetch_first_key_with_prefix(this, prefix, prefix_end);
}

std::optional<std::string>
LineairDBTransaction::fetch_next_key_with_prefix(const std::string &last_key,
                                                 const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  if (table_has_pending_write_in_range(db_table_key, last_key, prefix_end)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_fetch_next_key_with_prefix(this, last_key, prefix_end);
}

// Secondary index scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_in_range(std::string index_name,
                                                         std::string start_key,
                                                         std::string end_key) {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_get_matching_primary_keys_in_range(this, index_name, start_key, end_key);
}

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_from_prefix(std::string index_name,
                                                            std::string prefix) {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_get_matching_primary_keys_from_prefix(this, index_name, prefix);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_index_range(std::string index_name,
                                                                  std::string start_key,
                                                                  std::string end_key) {
  if (table_is_not_chosen()) return {};
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_in_index_range(
      this, index_name, start_key, end_key);

  std::vector<std::pair<std::string, std::string>> pairs;
  pairs.reserve(results.size());
  for (auto& kv : results) {
    // Populate per-tx PK row cache so a subsequent read() on any of these
    // PKs hits cache instead of issuing a TX_READ. Empty value means the
    // server saw the SI entry but Read() found no base row (dangling SI),
    // which is a negative-cache hit for the same tx.
    auto cache_key = make_pk_cache_key(db_table_key, kv.key);
    if (!kv.value.empty()) {
      read_cache_.emplace(std::move(cache_key), kv.value);
    } else {
      read_cache_misses_.insert(std::move(cache_key));
    }
    pairs.emplace_back(std::move(kv.key), std::move(kv.value));
  }
  return pairs;
}

std::optional<std::string>
LineairDBTransaction::fetch_last_primary_key_in_secondary_range(const std::string &index_name,
                                                                const std::string &start_key,
                                                                const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

  return lineairdb_proxy->tx_fetch_last_primary_key_in_secondary_range(this, index_name, start_key, end_key);
}

std::optional<SecondaryIndexEntry>
LineairDBTransaction::fetch_last_secondary_entry_in_range(const std::string &index_name,
                                                          const std::string &start_key,
                                                          const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  if (table_has_pending_writes(db_table_key)) {
    flush_write_buffer_for_table(db_table_key);
  }

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
  auto &buffer = write_buffers_[table_name];
  buffer.writes.push_back({key, value});

  // Drop stale cache entry so a subsequent same-table read flushes and returns
  // post-write state from the server.
  invalidate_pk_cache_entry(table_name, key);

  if (buffer.writes.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer_for_table(table_name);
  }
}

void LineairDBTransaction::buffer_write_secondary_index(const std::string& table_name,
                                                        const std::string& index_name,
                                                        const std::string& secondary_key,
                                                        const std::string& primary_key) {
  auto &buffer = write_buffers_[table_name];
  buffer.secondary_index_writes.push_back(
      {index_name, secondary_key, primary_key});
}

bool LineairDBTransaction::flush_write_buffer() {
  if (write_buffers_.empty()) return true;
  if (is_aborted_) {
    write_buffers_.clear();
    return false;
  }

  while (!write_buffers_.empty()) {
    const std::string table_name = write_buffers_.begin()->first;
    if (!flush_write_buffer_for_table(table_name)) {
      write_buffers_.clear();
      return false;
    }
  }

  return true;
}

bool LineairDBTransaction::flush_write_buffer_for_table(
    const std::string& table_name) {
  auto it = write_buffers_.find(table_name);
  if (it == write_buffers_.end()) return true;
  if (is_aborted_) {
    write_buffers_.clear();
    return false;
  }

  PendingWriteBuffer buffer = std::move(it->second);
  write_buffers_.erase(it);
  if (buffer.writes.empty() && buffer.secondary_index_writes.empty()) {
    return true;
  }

  bool ok = lineairdb_proxy->tx_batch_write(
      this, table_name, buffer.writes, buffer.secondary_index_writes);
  if (!ok) {
    write_buffers_.clear();
  }
  return ok;
}

std::string LineairDBTransaction::make_pk_cache_key(const std::string& table,
                                                    const std::string& key) {
  std::string out;
  out.reserve(8 + table.size() + key.size());
  uint32_t tlen = static_cast<uint32_t>(table.size());
  uint32_t klen = static_cast<uint32_t>(key.size());
  out.append(reinterpret_cast<const char*>(&tlen), sizeof(tlen));
  out.append(table);
  out.append(reinterpret_cast<const char*>(&klen), sizeof(klen));
  out.append(key);
  return out;
}

void LineairDBTransaction::invalidate_pk_cache_entry(const std::string& table,
                                                     const std::string& key) {
  if (read_cache_.empty() && read_cache_misses_.empty()) return;
  std::string cache_key = make_pk_cache_key(table, key);
  read_cache_.erase(cache_key);
  read_cache_misses_.erase(cache_key);
}

bool LineairDBTransaction::table_has_pending_writes(
    const std::string& table_name) const {
  auto it = write_buffers_.find(table_name);
  if (it == write_buffers_.end()) return false;
  return !it->second.writes.empty() ||
         !it->second.secondary_index_writes.empty();
}

bool LineairDBTransaction::table_has_pending_write_for_key(
    const std::string& table_name, const std::string& key) const {
  auto it = write_buffers_.find(table_name);
  if (it == write_buffers_.end()) return false;
  for (const auto& w : it->second.writes) {
    if (w.key == key) return true;
  }
  return false;
}

bool LineairDBTransaction::table_has_pending_write_in_range(
    const std::string& table_name, const std::string& start_key,
    const std::string& end_key) const {
  auto it = write_buffers_.find(table_name);
  if (it == write_buffers_.end()) return false;
  for (const auto& w : it->second.writes) {
    if (w.key >= start_key && (end_key.empty() || w.key < end_key)) {
      return true;
    }
  }
  return false;
}

bool LineairDBTransaction::table_has_pending_write_with_prefix(
    const std::string& table_name, const std::string& prefix) const {
  auto it = write_buffers_.find(table_name);
  if (it == write_buffers_.end()) return false;
  for (const auto& w : it->second.writes) {
    if (w.key.size() >= prefix.size() &&
        w.key.compare(0, prefix.size(), prefix) == 0) {
      return true;
    }
  }
  return false;
}

void LineairDBTransaction::clear_read_cache() {
  read_cache_.clear();
  read_cache_misses_.clear();
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  // Activate trace before begin RPC so TX_BEGIN_TRANSACTION appears in
  // the recorded sequence. tx_id is patched once we have it.
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
  clear_read_cache();
}

bool LineairDBTransaction::end_transaction(bool* out_transport_error) {
  assert(tx_id != -1);
  if (out_transport_error != nullptr) *out_transport_error = false;
  // Snapshot is_aborted_ before flushing so we can tell whether flush itself
  // flipped it. tx_batch_write sets is_aborted_ only on a clean OCC abort
  // (response.is_aborted=true); transport failures (no connection / send
  // failure) leave is_aborted_ unchanged. So if flush returns false but
  // is_aborted_ did not change, the failure was transport, not OCC.
  bool was_aborted_before_flush = is_aborted_;
  bool flush_ok = flush_write_buffer();
  // transport iff flush failed AND nothing in this tx was aborted before
  // flush AND flush itself did not produce a new OCC abort. (Skip the
  // "unchanged" shortcut: an already-aborted tx makes flush early-return
  // false without that being a transport failure.)
  bool flush_transport_error =
      !flush_ok && !was_aborted_before_flush && !is_aborted_;
  bool was_aborted = is_aborted_;

  // Pick up any transport failure that happened earlier in the tx
  // (auto-flush in buffer_write, direct SI RPC in update_row, etc.).
  // Once latched, that classification wins over a later "clean" END RPC.
  bool prior_transport_error = transport_error_seen_;

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
  // A prior mid-tx transport failure (auto-flush, direct SI RPC, etc.)
  // means the server-side state is incomplete: at least one buffered op
  // never reached the server. Force the commit to fail so the SQL layer
  // gets a 1815 instead of silently appearing to succeed on a partial tx.
  if (prior_transport_error) committed = false;
  if (!committed) {
    thd_mark_transaction_to_rollback(thread, 1);
    if (out_transport_error != nullptr) {
      *out_transport_error = prior_transport_error ||
                             flush_transport_error ||
                             lineairdb_proxy->last_end_was_transport_error();
    }
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

  // Finalize trace + clear proxy pointer before suicide. Logger gates
  // the file write; finalize_jsonl is otherwise a small allocation.
  if (rpc_trace_.active()) {
    auto line = rpc_trace_.finalize_jsonl(committed && !was_aborted);
    RpcTraceLogger::instance().log_line(line);
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
