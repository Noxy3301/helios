#include "storage/lineairdb/ha_lineairdb.hh"

#include <string>

#include "lineairdb_prefetch.hh"
#include "my_dbug.h"
#include "sql/table.h"

// Handler DML entry points. These methods stage base-row mutations and their
// secondary-index side effects in the current LineairDB transaction.

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

  // REPLACE stays a blind write; IGNORE and ON DUPLICATE KEY UPDATE need the
  // answer at this row, so read the key here. A plain INSERT is refused by
  // the storage server when the buffer is sent.
  const bool resolve_duplicate_at_row =
      !insert_can_replace_ && insert_peeks_duplicates_ &&
      is_primary_key_exists();
  if (resolve_duplicate_at_row) {
    tx->choose_table(db_table_name);
    if (tx->is_prefetch_mode()) {
      tx->prefetch_stateless_reads({{db_table_name, key}});
    }
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
    const bool key_taken = tx->read(key).first != nullptr;
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
    if (key_taken) {
      duplicate_key_index_ = table_share->primary_key;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }

  // buffer_write appends to a local buffer (no RPC yet), so no error check
  // needed. The actual RPC is sent at flush time.
  tx->buffer_write(db_table_name, key, write_buffer_, !insert_can_replace_);

  // Write secondary index entries. Normal transactions check UNIQUE indexes
  // immediately. Prefetch sends UNIQUE index writes to validate-and-commit with
  // row writes.
  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    if (i == table->s->primary_key) continue;

    std::string secondary_key = build_secondary_key_from_row(buf, key_info);

    if (key_info.flags & HA_NOSAME) {
      if (tx->is_prefetch_mode()) {
        tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                         secondary_key, key);
      } else {
        tx->flush_write_buffer();
        tx->choose_table(db_table_name);
        bool ok = tx->write_secondary_index(key_info.name, secondary_key, key);
        if (!ok || tx->is_aborted()) {
          // A duplicate this flush surfaces is a lost race for a statement
          // that already gave its row-time answer.
          return abort_errno(tx, resolve_duplicate_at_row);
        }
      }
    } else {
      tx->buffer_write_secondary_index(db_table_name, key_info.name,
                                       secondary_key, key);
    }
  }

  if (tx->is_aborted()) {
    return abort_errno(tx, resolve_duplicate_at_row);
  }

  // Such a statement cannot wait for the statement-end flush: by then MySQL
  // has moved past this row. Send it now, as NDB does by turning batching
  // off for these statements; prefetch defers everything to its commit.
  if (resolve_duplicate_at_row) {
    tx->flush_write_buffer();
    if (tx->is_aborted()) {
      // The key was taken between the read above and this flush, so this
      // transaction is gone and the statement's own duplicate handling can no
      // longer run. A retry sees the row and resolves it.
      return abort_errno(tx, /*duplicate_is_conflict=*/true);
    }
  }

  tx->add_rowcount_delta(share, db_table_name, +1);

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
    return prefetch_reject_unsupported(ha_thd(), tx,
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

  // Buffer the base-row update; read/scan paths and commit flush it later.
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

  // Buffer the base-row delete; read/scan paths and commit flush it later.
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
