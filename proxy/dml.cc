#include "storage/lineairdb/ha_lineairdb.hh"

#include <string>
#include <utility>
#include <vector>

#include "lineairdb_prefetch.hh"
#include "my_dbug.h"
#include "sql/table.h"

// Handler DML entry points. These methods stage base-row mutations and their
// secondary-index side effects in the current LineairDB transaction.

int ha_lineairdb::duplicate_or_conflict(LineairDBTransaction *tx, uint index) {
  if (!tx->reads_still_valid()) {
    tx->set_status_to_abort();
    return abort_errno(tx);
  }
  duplicate_key_index_ = index;
  return HA_ERR_FOUND_DUPP_KEY;
}

int ha_lineairdb::flush_insert_probe(LineairDBTransaction *tx) {
  if (insert_probe_keys_.empty()) return 0;
  std::vector<std::string> keys;
  keys.swap(insert_probe_keys_);
  if (!tx->probe_insert_keys(db_table_name, keys)) return 0;
  return duplicate_or_conflict(tx, table_share->primary_key);
}

int ha_lineairdb::write_row(uchar *buf) {
  DBUG_TRACE;

  set_write_buffer(buf);

  auto tx = get_transaction(ha_thd());

  // A transaction already gone at row entry is one no statement can resolve
  // against, so a duplicate it carries must not be offered as ignorable.
  if (tx->is_aborted()) {
    return abort_errno(tx, /*duplicate_is_conflict=*/true);
  }

  // A hidden primary key is reserved from the storage server, so it can fail.
  // Writing the row anyway would put it under a key another query layer owns.
  std::string key;
  if (const int error = extract_key(buf, tx, &key); error != 0) {
    return error;
  }

  tx->choose_table(db_table_name);

  const bool checks_unique_keys =
      !::thd_test_options(ha_thd(), OPTION_RELAXED_UNIQUE_CHECKS);

  // REPLACE overwrites whatever the key holds, so the entries the old row put
  // in the secondary indexes leave with it and the row count does not change.
  // The read is recorded, so a row that appears after it fails the commit.
  bool replaced_existing_row = false;
  if (insert_can_replace_ && is_primary_key_exists()) {
    const auto old_row = tx->read(key);
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
    if (old_row.first != nullptr && old_row.second != 0) {
      // record[1] is MySQL's buffer for the row being replaced.
      if (set_fields_from_lineairdb(table->record[1], old_row.first,
                                    old_row.second)) {
        return HA_ERR_OUT_OF_MEM;
      }
      replaced_existing_row = true;
    }
  }

  // IGNORE and ON DUPLICATE KEY UPDATE need the answer at this row, so read
  // the key here.
  const bool resolve_duplicate_at_row =
      !insert_can_replace_ && insert_peeks_duplicates_ &&
      is_primary_key_exists();
  if (resolve_duplicate_at_row) {
    const bool key_taken = tx->read(key).first != nullptr;
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
    if (key_taken) {
      duplicate_key_index_ = table_share->primary_key;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  } else if (!insert_can_replace_ && is_primary_key_exists()) {
    switch (tx->insert_key_state(db_table_name, key)) {
      case LineairDBTransaction::KeyState::Taken:
        // A row this transaction wrote itself: no request settles it, and
        // unique_checks does not make it acceptable either.
        duplicate_key_index_ = table_share->primary_key;
        return HA_ERR_FOUND_DUPP_KEY;
      case LineairDBTransaction::KeyState::Free:
        break;
      case LineairDBTransaction::KeyState::Unknown:
        // Under unique_checks the statement owes ER_DUP_ENTRY, so the keys it
        // inserts are resolved before it returns: in one batch at
        // end_bulk_insert, or at this row without that bracket. Without it the
        // commit is what refuses the key.
        if (checks_unique_keys) {
          insert_probe_keys_.push_back(key);
          if (!bulk_insert_active_ ||
              insert_probe_keys_.size() >= kInsertProbeBatch) {
            if (const int error = flush_insert_probe(tx); error != 0) {
              return error;
            }
          }
        }
        break;
    }
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
  }

  // The commit installs the row and refuses an INSERT whose key is taken.
  tx->buffer_write(db_table_name, key, write_buffer_,
                   !insert_can_replace_ || !replaced_existing_row);

  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    if (i == table->s->primary_key) continue;

    std::string secondary_key = build_secondary_key_from_row(buf, key_info);

    if (replaced_existing_row) {
      const std::string old_secondary_key =
          build_secondary_key_from_row(table->record[1], key_info);
      if (old_secondary_key != secondary_key) {
        tx->buffer_delete_secondary_index(db_table_name, key_info.name,
                                          old_secondary_key, key);
      }
    }

    if (key_info.flags & HA_NOSAME) {
      // An index entry this transaction wrote itself is a duplicate whatever
      // unique_checks says: no request settles it.
      if (tx->index_key_taken_by_own_write(db_table_name, key_info.name,
                                           secondary_key, key)) {
        duplicate_key_index_ = i;
        return HA_ERR_FOUND_DUPP_KEY;
      }
      // A UNIQUE secondary key is the statement's to report too. The probe
      // records its range, so a duplicate another transaction installs after
      // it still fails this transaction at commit.
      if (checks_unique_keys) {
        const auto owners =
            tx->read_secondary_index(key_info.name, secondary_key, true);
        if (tx->is_aborted()) {
          return abort_errno(tx, resolve_duplicate_at_row);
        }
        for (const auto &owner : owners) {
          if (owner != key) {
            return duplicate_or_conflict(tx, i);
          }
        }
      }
    }

    tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                     secondary_key, key);
  }

  if (tx->is_aborted()) {
    return abort_errno(tx, resolve_duplicate_at_row);
  }

  if (!replaced_existing_row) {
    tx->add_rowcount_delta(share, db_table_name, +1);
  }

  return 0;
}

int ha_lineairdb::update_row(const uchar *old_data, uchar *new_data) {
  DBUG_TRACE;

  auto tx = get_transaction(ha_thd());
  auto key = extract_key_from_mysql(old_data);
  const auto new_key = extract_key_from_mysql(new_data);

  // FIXME: reject a PK-changing UPDATE. update_row overwrites in place at the
  // old key and cannot move a row, so executing one would store new_data under
  // the old key with nothing at the new key -- silent corruption. A real move
  // (delete old + insert new + secondary-index rewrite) is not implemented.
  if (key != new_key) {
    return reject_unsupported_statement(ha_thd(), tx,
                                       "primary-key-changing UPDATE");
  }

  if (key.empty()) {
    key = last_fetched_primary_key_;
  }

  if (key.empty()) {
    key = extract_primary_key_from_ref(ref);
  }

  last_fetched_primary_key_ = key;

  set_write_buffer(new_data);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  // Buffer the base-row update; the commit installs it.
  tx->buffer_write(db_table_name, key, write_buffer_);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->choose_table(db_table_name);

  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];

    if (i == table->s->primary_key) {
      continue;
    }

    std::string old_secondary_key =
        build_secondary_key_from_row(old_data, key_info);
    std::string new_secondary_key =
        build_secondary_key_from_row(new_data, key_info);

    if (old_secondary_key == new_secondary_key) {
      continue;
    }

    tx->update_secondary_index(key_info.name, old_secondary_key,
                               new_secondary_key, key);

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
  }

  return 0;
}

int ha_lineairdb::delete_row(const uchar *buf) {
  DBUG_TRACE;

  auto key = extract_key_from_mysql(buf);

  if (key.empty()) {
    key = last_fetched_primary_key_;
  }

  if (key.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  last_fetched_primary_key_ = key;

  auto tx = get_transaction(ha_thd());

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  // Buffer the base-row delete; the commit installs it.
  tx->buffer_delete(db_table_name, key);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    if (i != table->s->primary_key) {
      std::string secondary_key = build_secondary_key_from_row(buf, key_info);

      tx->buffer_delete_secondary_index(db_table_name, key_info.name,
                                        secondary_key, key);
    }
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  tx->add_rowcount_delta(share, db_table_name, -1);

  return 0;
}
