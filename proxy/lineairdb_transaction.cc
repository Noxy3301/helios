#include "lineairdb_transaction.hh"
#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

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

  flush_write_buffer();

  last_read_value_ = lineairdb_proxy->tx_read(this, key);
  if (last_read_value_.empty()) return std::pair<const std::byte *const, const size_t>{nullptr, 0};

  return {reinterpret_cast<const std::byte*>(last_read_value_.data()), last_read_value_.size()};
}

std::vector<std::pair<bool, std::string>>
LineairDBTransaction::batch_read(const std::vector<std::string>& keys) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

  auto results = lineairdb_proxy->tx_batch_read(this, keys);
  std::vector<std::pair<bool, std::string>> pairs;
  pairs.reserve(results.size());
  for (auto& r : results) {
    pairs.emplace_back(r.found, std::move(r.value));
  }
  return pairs;
}

bool LineairDBTransaction::batch_write(
    const std::string& table_name,
    const std::vector<LineairDBProxy::BatchWriteOp>& writes,
    const std::vector<LineairDBProxy::BatchSecondaryIndexOp>& si_writes) {
  return lineairdb_proxy->tx_batch_write(this, table_name, writes, si_writes);
}

std::vector<std::string>
LineairDBTransaction::get_all_keys() {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

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
  flush_write_buffer();

  auto key_value_pairs = lineairdb_proxy->tx_get_matching_keys_and_values_from_prefix(this, first_key_part);

  std::vector<std::string> keyList;
  for (const auto& kv : key_value_pairs) {
    keyList.push_back(kv.key);
  }

  return keyList;
}

bool LineairDBTransaction::write(std::string key, const std::string value) {
  if (table_is_not_chosen()) return false;

  return lineairdb_proxy->tx_write(this, key, value);
}

bool LineairDBTransaction::delete_value(std::string key) {
  if (table_is_not_chosen()) return false;

  return lineairdb_proxy->tx_delete(this, key);
}

// Secondary index operations

std::vector<std::string>
LineairDBTransaction::read_secondary_index(std::string index_name,
                                           std::string secondary_key) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

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
  flush_write_buffer();

  return lineairdb_proxy->tx_get_matching_keys_in_range(this, start_key, end_key);
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_in_range(std::string start_key,
                                                            std::string end_key) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

  auto results = lineairdb_proxy->tx_get_matching_keys_and_values_in_range(this, start_key, end_key);

  std::vector<std::pair<std::string, std::string>> pairs;
  for (const auto& kv : results) {
    pairs.emplace_back(kv.key, kv.value);
  }
  return pairs;
}

std::vector<std::pair<std::string, std::string>>
LineairDBTransaction::get_matching_keys_and_values_from_prefix(std::string prefix) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

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
  flush_write_buffer();

  return lineairdb_proxy->tx_fetch_last_key_in_range(this, start_key, end_key);
}

std::optional<std::string>
LineairDBTransaction::fetch_first_key_with_prefix(const std::string &prefix,
                                                  const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer();

  return lineairdb_proxy->tx_fetch_first_key_with_prefix(this, prefix, prefix_end);
}

std::optional<std::string>
LineairDBTransaction::fetch_next_key_with_prefix(const std::string &last_key,
                                                 const std::string &prefix_end) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer();

  return lineairdb_proxy->tx_fetch_next_key_with_prefix(this, last_key, prefix_end);
}

// Secondary index scan operations

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_in_range(std::string index_name,
                                                         std::string start_key,
                                                         std::string end_key) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

  return lineairdb_proxy->tx_get_matching_primary_keys_in_range(this, index_name, start_key, end_key);
}

std::vector<std::string>
LineairDBTransaction::get_matching_primary_keys_from_prefix(std::string index_name,
                                                            std::string prefix) {
  if (table_is_not_chosen()) return {};
  flush_write_buffer();

  return lineairdb_proxy->tx_get_matching_primary_keys_from_prefix(this, index_name, prefix);
}

std::optional<std::string>
LineairDBTransaction::fetch_last_primary_key_in_secondary_range(const std::string &index_name,
                                                                const std::string &start_key,
                                                                const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer();

  return lineairdb_proxy->tx_fetch_last_primary_key_in_secondary_range(this, index_name, start_key, end_key);
}

std::optional<SecondaryIndexEntry>
LineairDBTransaction::fetch_last_secondary_entry_in_range(const std::string &index_name,
                                                          const std::string &start_key,
                                                          const std::string &end_key) {
  if (table_is_not_chosen()) return std::nullopt;
  flush_write_buffer();

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
  // If table changed, flush the current buffer first
  if (!write_buffer_ops_.empty() && write_buffer_table_ != table_name) {
    flush_write_buffer();
  }
  write_buffer_table_ = table_name;
  write_buffer_ops_.push_back({key, value});

  if (write_buffer_ops_.size() >= WRITE_BATCH_SIZE) {
    flush_write_buffer();
  }
}

void LineairDBTransaction::buffer_write_secondary_index(const std::string& table_name,
                                                        const std::string& index_name,
                                                        const std::string& secondary_key,
                                                        const std::string& primary_key) {
  write_buffer_si_ops_.push_back({index_name, secondary_key, primary_key});
}

bool LineairDBTransaction::flush_write_buffer() {
  if (write_buffer_ops_.empty() && write_buffer_si_ops_.empty()) return true;
  if (is_aborted_) {
    write_buffer_ops_.clear();
    write_buffer_si_ops_.clear();
    return false;
  }

  bool ok = lineairdb_proxy->tx_batch_write(
      this, write_buffer_table_, write_buffer_ops_, write_buffer_si_ops_);
  write_buffer_ops_.clear();
  write_buffer_si_ops_.clear();
  return ok;
}

void LineairDBTransaction::begin_transaction() {
  assert(is_not_started());
  tx_id = lineairdb_proxy->tx_begin_transaction();
  // TODO: maybe need error handling when tx_id == -1
  assert(tx_id != -1);
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
