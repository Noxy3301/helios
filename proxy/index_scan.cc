#include "storage/helios/ha_helios.hh"

#include <algorithm>
#include <string>
#include <utility>

#include "helios_prefetch.hh"
#include "my_dbug.h"
#include "sql/key.h"
#include "sql/table.h"

// Handler index-access entry points. These methods translate MySQL index
// cursor operations into Helios primary/secondary range reads and consume
// the index scan cache populated by the search planner.

void ha_helios::reset_index_search_buffers() {
  secondary_index_results_.clear();
  secondary_index_payloads_.clear();
  current_position_in_index_ = 0;
  index_scan_is_partial_ = false;
  index_scan_end_key_.clear();
  index_cursor_active_ = false;
  index_cursor_reverse_ = false;
  index_cursor_secondary_ = false;
  index_cursor_at_eof_ = false;
  index_cursor_start_key_.clear();
  index_cursor_end_key_.clear();
}

bool ha_helios::refill_index_cursor(HeliosTransaction *tx) {
  secondary_index_results_.clear();
  secondary_index_payloads_.clear();
  current_position_in_index_ = 0;

  if (index_cursor_at_eof_) return false;

  if (index_cursor_secondary_) {
    // One batch of complete secondary-key groups at a time. The lowest group
    // in it is the next exclusive end, which preserves the original
    // (secondary key, primary key) order without reading the full index.
    auto batch = tx->fetch_secondary_batch_below(
        current_index_name, index_cursor_start_key_, index_cursor_end_key_,
        INDEX_CURSOR_BATCH_SIZE);
    if (!batch.has_value()) {
      index_cursor_at_eof_ = true;
      return false;
    }
    // The cursor consumes the vector from its tail, so flatten the groups the
    // other way round: ascending by secondary key, ascending within a group.
    index_cursor_end_key_ = batch->groups.back().secondary_key;
    index_cursor_at_eof_ = !batch->more_below;
    for (auto group = batch->groups.rbegin(); group != batch->groups.rend();
         ++group) {
      for (auto &primary_key : group->primary_keys) {
        secondary_index_results_.push_back(std::move(primary_key));
      }
    }
    batch_fetch_secondary_payloads(tx);
  } else {
    auto key_values = tx->get_matching_keys_and_values_in_range(
        index_cursor_start_key_, index_cursor_end_key_,
        INDEX_CURSOR_BATCH_SIZE, index_cursor_reverse_);
    if (key_values.empty()) {
      index_cursor_at_eof_ = true;
      return false;
    }

    const size_t fetched = key_values.size();
    if (index_cursor_reverse_) {
      // ScanReverse returns descending rows. The handler's established
      // index_prev() cursor consumes an ascending vector from its tail.
      std::reverse(key_values.begin(), key_values.end());
      index_cursor_end_key_ = key_values.front().first;  // exclusive resume
    } else {
      index_cursor_start_key_ = key_values.back().first;
      // Helios ranges are [start,end). Appending NUL is the smallest bound
      // strictly greater than this complete serialized index key.
      index_cursor_start_key_.push_back('\0');
    }
    index_cursor_at_eof_ = fetched < INDEX_CURSOR_BATCH_SIZE;

    secondary_index_results_.reserve(fetched);
    secondary_index_payloads_.reserve(fetched);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(std::move(kv.first));
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  }

  if (tx->is_aborted() || secondary_index_results_.empty()) return false;
  current_position_in_index_ =
      index_cursor_reverse_
          ? static_cast<uint>(secondary_index_results_.size() - 1)
          : 0;
  return true;
}

bool ha_helios::refill_index_scan(HeliosTransaction *tx) {
  index_scan_is_partial_ = false;
  if (secondary_index_results_.empty()) return false;

  // The cached scan stopped at its last key; ask the storage for the rest of the
  // range the statement wanted. Helios ranges are [start, end), so the
  // smallest key above a complete serialized key is that key plus NUL.
  std::string start_key = secondary_index_results_.back();
  start_key.push_back('\0');

  auto key_values =
      tx->get_matching_keys_and_values_in_range(start_key, index_scan_end_key_);
  if (tx->is_aborted() || key_values.empty()) return false;

  secondary_index_results_.clear();
  secondary_index_payloads_.clear();
  current_position_in_index_ = 0;
  secondary_index_results_.reserve(key_values.size());
  secondary_index_payloads_.reserve(key_values.size());
  for (auto &kv : key_values) {
    secondary_index_results_.push_back(std::move(kv.first));
    secondary_index_payloads_.push_back(std::move(kv.second));
  }
  return true;
}

int ha_helios::change_active_index(uint keynr) {
  DBUG_TRACE;
  active_index = keynr;

  if (table && table->s && keynr < table->s->keys) {
    current_index_name = std::string(table->key_info[keynr].name);
  } else {
    current_index_name.clear();
  }

  return 0;
}

int ha_helios::index_init(uint idx, bool sorted [[maybe_unused]]) {
  DBUG_TRACE;
  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  return change_active_index(idx);
}

int ha_helios::index_end() {
  DBUG_TRACE;
  active_index = MAX_KEY;
  mrr_use_batch_ = false;
  mrr_buffer_.clear();
  mrr_buffer_pos_ = 0;
  return 0;
}

int ha_helios::index_read(uchar *buf, const uchar *key,
                             uint key_len [[maybe_unused]],
                             enum ha_rkey_function find_flag) {
  DBUG_TRACE;
  return index_read_map(buf, key, HA_WHOLE_KEY, find_flag);
}

int ha_helios::index_read_last(uchar *buf, const uchar *key, uint key_len) {
  DBUG_TRACE;

  if (key == nullptr || key_len == 0) {
    return index_last(buf);
  }

  KEY *key_info = &table->key_info[active_index];
  uint total_len = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    total_len += key_info->key_part[i].store_length;
  }

  if (key_len >= total_len) {
    return index_read_map(buf, key, HA_WHOLE_KEY, HA_READ_PREFIX_LAST);
  }

  key_part_map keypart_map = 0;
  uint consumed = 0;
  bool aligned = false;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    const uint part_len = key_info->key_part[i].store_length;
    if (consumed + part_len > key_len) {
      break;
    }
    consumed += part_len;
    keypart_map |= (static_cast<key_part_map>(1) << i);
    if (consumed == key_len) {
      aligned = true;
      break;
    }
  }

  if (!aligned) {
    return HA_ERR_WRONG_COMMAND;
  }

  return index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST);
}

int ha_helios::index_read_map(uchar *buf, const uchar *key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag) {
  DBUG_TRACE;

  stats.records = 0;
  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);

  KEY *key_info = &table->key_info[active_index];

  // MySQL runs single-table UPDATE/DELETE through the old executor
  // (sql_update.cc/sql_delete.cc), which has no JOIN/access path. Derive its
  // plan from the optimizer-selected handler access instead.
  if (prefetch_needs_single_table_dml_handler(ha_thd(), tx)) {
    build_search_plan(key, keypart_map, find_flag, key_info);
    if (int err = maybe_prefetch_for_single_table_dml_handler(
            ha_thd(), tx, table, active_index, current_plan_)) {
      return err;
    }
    return execute_plan(buf, tx);
  }

  // The optimizer has run, so the SELECT/generic-DML QEP is available.
  if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table)) return err;

  build_search_plan(key, keypart_map, find_flag, key_info);

  return execute_plan(buf, tx);
}

int ha_helios::index_next(uchar *buf) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  if (index_cursor_active_ && !index_cursor_reverse_ &&
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (!refill_index_cursor(tx)) {
      return tx->is_aborted() ? abort_errno(tx) : HA_ERR_END_OF_FILE;
    }
  }

  // Consume the index scan cache.
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (index_scan_is_partial_ && !refill_index_scan(tx)) {
      return tx->is_aborted() ? abort_errno(tx) : HA_ERR_END_OF_FILE;
    }
    if (current_position_in_index_ >= secondary_index_results_.size()) {
      return HA_ERR_END_OF_FILE;
    }
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::index_next_same(uchar *buf, const uchar *key [[maybe_unused]],
                                  uint key_len [[maybe_unused]]) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // Consume the index scan cache.
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (index_scan_is_partial_ && !refill_index_scan(tx)) {
      return tx->is_aborted() ? abort_errno(tx) : HA_ERR_END_OF_FILE;
    }
    if (current_position_in_index_ >= secondary_index_results_.size()) {
      return HA_ERR_END_OF_FILE;
    }
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::index_prev(uchar *buf) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  if (index_cursor_active_ && index_cursor_reverse_ &&
      (secondary_index_results_.empty() || current_position_in_index_ < 2)) {
    if (!refill_index_cursor(tx)) {
      return tx->is_aborted() ? abort_errno(tx) : HA_ERR_END_OF_FILE;
    }
    return fetch_and_set_current_result(buf, tx);
  }

  // Consume the index scan cache.
  if (secondary_index_results_.empty() || current_position_in_index_ < 2) {
    return HA_ERR_END_OF_FILE;
  }

  current_position_in_index_ -= 2;
  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::index_first(uchar *buf) {
  DBUG_TRACE;
  int error = index_read(buf, nullptr, 0, HA_READ_AFTER_KEY);

  // MySQL does not seem to allow this to return HA_ERR_KEY_NOT_FOUND.
  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }

  return error;
}

int ha_helios::index_last(uchar *buf) {
  DBUG_TRACE;

  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);

  // A key-less tail seek carries no range for the read-plan compiler (an
  // unbounded MAX reaches it straight from the optimizer), so cache the
  // index tail fetch on demand. A secondary tail walks the index cursor
  // instead.
  if (active_index == table->s->primary_key) {
    if (int err = maybe_prefetch_for_index_tail(ha_thd(), tx, db_table_name,
                                                INDEX_CURSOR_BATCH_SIZE)) {
      return err;
    }
  }

  if (active_index == table->s->primary_key) {
    index_cursor_active_ = true;
    index_cursor_reverse_ = true;
    index_cursor_secondary_ = false;
  } else {
    index_cursor_active_ = true;
    index_cursor_reverse_ = true;
    index_cursor_secondary_ = true;
  }

  (void)refill_index_cursor(tx);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  int error = fetch_and_set_current_result(buf, tx);
  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }
  return error;
}
