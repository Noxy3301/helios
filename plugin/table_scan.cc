#include "storage/helios/ha_helios.hh"

#include <cstddef>
#include <string>

#include "prefetch.hh"
#include "my_dbug.h"
#include "sql/sql_class.h"
#include "sql/table.h"

// Handler full-table-scan entry points. These methods back MySQL's rnd_* scan
// callbacks with Helios range reads and maintain the local row cache used
// for sorted re-reads through rnd_pos().

int ha_helios::rnd_init(bool) {
  DBUG_ENTER("ha_helios::rnd_init");
  scanned_keys_.clear();
  scanned_values_.clear();
  scan_buffer_.clear();
  buffer_position_ = 0;
  scan_exhausted_ = false;
  last_fetched_primary_key_.clear();
  stats.records = 0;

  change_active_index(table->s->primary_key);

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    DBUG_RETURN(abort_errno(tx));
  }

  tx->choose_table(db_table_name);

  // The optimizer has run, so the QEP is available. Statement-scoped prefetch
  // runs the read plan once per statement; a shape it cannot compile reads
  // from the storage server row by row.
  if (int err = maybe_prefetch_for_statement(ha_thd(), tx, table)) {
    DBUG_RETURN(err);
  }

  DBUG_RETURN(0);
}

int ha_helios::rnd_end() {
  DBUG_TRACE;
  // Do not clear scan_buffer_ or scanned_values_ here. MySQL can call rnd_end()
  // after scanning and then call rnd_pos() to re-read rows in sorted order.
  // Clear them at the start of the next rnd_init() instead.
  buffer_position_ = 0;
  scan_exhausted_ = false;
  blobroot.Clear();
  return 0;
}

bool ha_helios::fill_scan_buffer() {
  DBUG_ENTER("ha_helios::fill_scan_buffer");

  auto tx = get_transaction(ha_thd());
  if (tx->is_aborted()) {
    DBUG_RETURN(false);
  }

  tx->choose_table(db_table_name);

  scanned_keys_.clear();
  scanned_values_.clear();
  scan_buffer_.clear();
  buffer_position_ = 0;

  auto key_value_pairs = tx->get_matching_keys_and_values_from_prefix("");

  for (auto &kv : key_value_pairs) {
    if (kv.second.empty()) continue;

    // Store row data and build scan_buffer_ so rnd_pos() can reuse it later.
    size_t idx = scanned_keys_.size();
    scanned_keys_.push_back(kv.first);
    const auto &val = kv.second;
    scanned_values_.emplace_back(
        reinterpret_cast<const std::byte *>(val.data()),
        reinterpret_cast<const std::byte *>(val.data()) + val.size());
    scan_buffer_[kv.first] = idx;
  }

  if (tx->is_aborted()) {
    thd_mark_transaction_to_rollback(ha_thd(), 1);
    DBUG_RETURN(false);
  }

  if (scanned_keys_.empty()) {
    DBUG_RETURN(false);
  }

  // The plugin fetches all rows in one RPC.
  scan_exhausted_ = true;

  DBUG_RETURN(true);
}

int ha_helios::rnd_next(uchar *buf) {
  DBUG_ENTER("ha_helios::rnd_next");
  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  if (buffer_position_ >= scanned_keys_.size()) {
    if (scan_exhausted_) {
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }

    if (!fill_scan_buffer()) {
      auto tx = get_transaction(ha_thd());
      if (tx->is_aborted()) {
        DBUG_RETURN(abort_errno(tx));
      }
      scan_exhausted_ = true;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }

  auto &key = scanned_keys_[buffer_position_];
  auto &value = scanned_values_[buffer_position_];
  buffer_position_++;

  int error = set_fields_from_helios(buf, value.data(), value.size());
  if (error == 0) {
    last_fetched_primary_key_ = key;
  }
  DBUG_RETURN(error);
}

void ha_helios::position(const uchar *) {
  DBUG_TRACE;

  if (last_fetched_primary_key_.empty()) {
    return;
  }

  store_primary_key_in_ref(last_fetched_primary_key_);
}

int ha_helios::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_TRACE;

  std::string primary_key = extract_primary_key_from_ref(pos);

  if (primary_key.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  // Return from scan_buffer_ if available. Without this, each sorted re-read
  // would require a separate RPC to Helios.
  auto cache_it = scan_buffer_.find(primary_key);
  if (cache_it != scan_buffer_.end()) {
    auto &value = scanned_values_[cache_it->second];
    if (set_fields_from_helios(buf, value.data(), value.size())) {
      return HA_ERR_OUT_OF_MEM;
    }
    last_fetched_primary_key_ = primary_key;
    return 0;
  }

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);
  auto result = tx->read(primary_key);

  // read() aborts the tx when the request did not reach the server; catch it
  // before the empty result below reads as a missing key.
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }
  if (result.first == nullptr || result.second == 0) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  if (set_fields_from_helios(buf, result.first, result.second)) {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }

  last_fetched_primary_key_ = primary_key;

  return 0;
}
