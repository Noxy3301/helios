#include "storage/lineairdb/ha_lineairdb.hh"

#include <string>
#include <utility>

#include "lineairdb_prefetch.hh"
#include "my_dbug.h"
#include "sql/key.h"
#include "sql/table.h"

// Handler index-access entry points. These methods translate MySQL index
// cursor operations into LineairDB primary/secondary range reads and consume
// the materialized result buffers populated by the search planner.

void ha_lineairdb::reset_index_search_buffers() {
  secondary_index_results_.clear();
  secondary_index_payloads_.clear();
  current_position_in_index_ = 0;
  materialized_scan_truncated_ = false;
}

int ha_lineairdb::change_active_index(uint keynr) {
  DBUG_TRACE;
  active_index = keynr;

  if (table && table->s && keynr < table->s->keys) {
    current_index_name = std::string(table->key_info[keynr].name);
  } else {
    current_index_name.clear();
  }

  return 0;
}

int ha_lineairdb::index_init(uint idx, bool sorted [[maybe_unused]]) {
  DBUG_TRACE;
  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  return change_active_index(idx);
}

int ha_lineairdb::index_end() {
  DBUG_TRACE;
  active_index = MAX_KEY;
  mrr_use_batch_ = false;
  mrr_buffer_.clear();
  mrr_buffer_pos_ = 0;
  return 0;
}

int ha_lineairdb::index_read(uchar *buf, const uchar *key,
                             uint key_len [[maybe_unused]],
                             enum ha_rkey_function find_flag) {
  DBUG_TRACE;
  return index_read_map(buf, key, HA_WHOLE_KEY, find_flag);
}

int ha_lineairdb::index_read_last(uchar *buf, const uchar *key, uint key_len) {
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

int ha_lineairdb::index_read_map(uchar *buf, const uchar *key,
                                 key_part_map keypart_map,
                                 enum ha_rkey_function find_flag) {
  DBUG_TRACE;

  stats.records = 0;
  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);
  if (!pushed_filter_serialized_.empty()) {
    tx->set_pushed_filter(pushed_filter_serialized_);
  } else if (!tx->has_pushed_aggregate()) {
    // See rnd_init: a pending aggregation pushdown owns the tx filter.
    tx->clear_pushed_filter();
  }

  KEY *key_info = &table->key_info[active_index];

  // MySQL runs single-table UPDATE/DELETE through the old executor
  // (sql_update.cc/sql_delete.cc), which has no JOIN/access path. Derive its
  // autogen plan from the optimizer-selected handler access instead.
  if (prefetch_needs_legacy_dml_handler(ha_thd(), tx)) {
    build_search_plan(key, keypart_map, find_flag, key_info);
    if (int err = maybe_prefetch_for_legacy_dml_handler(
            ha_thd(), tx, table, active_index, current_plan_)) {
      return err;
    }
    return execute_plan(buf, tx);
  }

  // A legacy single-table DML staged its plan on the first handler access; a
  // second handler access here means the statement spans multiple index ranges
  // (e.g. index merge over different indexes), which the single staged plan
  // cannot cover. Reject loudly (no-fallback) rather than let the read miss the
  // cache and surface as a retryable deadlock, which would livelock on retry.
  if (tx->is_prefetch_mode() && !tx->tx_plan_used() &&
      tx->is_autogen_stmt_handler_deferred()) {
    return prefetch_reject_unsupported(
        ha_thd(), tx, "legacy DML multi-index access (index merge)");
  }

  // The optimizer has run, so the SELECT/generic-DML QEP is available.
  if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table)) return err;

  build_search_plan(key, keypart_map, find_flag, key_info);

  return execute_plan(buf, tx);
}

int ha_lineairdb::index_next(uchar *buf) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // Consume materialized index results.
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (materialized_scan_truncated_ && !secondary_index_results_.empty()) {
      tx->fallback_to_normal_transaction("read past limit-staged scan");
      return abort_errno(tx);
    }
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_lineairdb::index_next_same(uchar *buf, const uchar *key [[maybe_unused]],
                                  uint key_len [[maybe_unused]]) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // Consume materialized index results.
  if (secondary_index_results_.empty() ||
      current_position_in_index_ >= secondary_index_results_.size()) {
    if (materialized_scan_truncated_ && !secondary_index_results_.empty()) {
      tx->fallback_to_normal_transaction("read past limit-staged scan");
      return abort_errno(tx);
    }
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_lineairdb::index_prev(uchar *buf) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  tx->choose_table(db_table_name);

  // Consume materialized index results.
  if (secondary_index_results_.empty() || current_position_in_index_ < 2) {
    return HA_ERR_END_OF_FILE;
  }

  current_position_in_index_ -= 2;
  return fetch_and_set_current_result(buf, tx);
}

int ha_lineairdb::index_first(uchar *buf) {
  DBUG_TRACE;
  int error = index_read(buf, nullptr, 0, HA_READ_AFTER_KEY);

  // MySQL does not seem to allow this to return HA_ERR_KEY_NOT_FOUND.
  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }

  return error;
}

int ha_lineairdb::index_last(uchar *buf) {
  DBUG_TRACE;

  reset_index_search_buffers();
  last_fetched_primary_key_.clear();

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);

  // index_last seeks the index tail with no search key: it does not route
  // through index_read_map and carries no range to build an autogen plan from,
  // and a statement-scoped plan keyed by the QEP's range cannot cover it.
  // Reject loudly under no-fallback rather than silently miss the local view.
  // Range-driven reverse scans (ORDER BY ... DESC LIMIT) are supported; only
  // this key-less tail seek is not.
  // TODO: support index_last under prefetch (autogen reverse tail-scan).
  if (tx->is_prefetch_mode()) {
    return prefetch_reject_unsupported(ha_thd(), tx, "index_last access");
  }

  if (active_index == table->s->primary_key) {
    auto key_values = tx->get_matching_keys_and_values_in_range("", "");
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ =
        tx->get_matching_primary_keys_in_range(current_index_name, "", "");
    batch_fetch_secondary_payloads(tx);
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  current_position_in_index_ =
      static_cast<uint>(secondary_index_results_.size() - 1);
  int error = fetch_and_set_current_result(buf, tx);
  if (error == HA_ERR_KEY_NOT_FOUND) {
    error = HA_ERR_END_OF_FILE;
  }
  return error;
}
