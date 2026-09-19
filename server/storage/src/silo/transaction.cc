/**
 * @file server/storage/src/silo/transaction.cc
 * The feed calls of one commit attempt and the three phases of the Silo
 * protocol that validate, install and publish it.
 */

#include "helios/transaction.h"

#include <xmmintrin.h>
#include <algorithm>
#include <utility>

#include "helios/database.h"

#include "index/masstree_index.h"
#include "index/reaper.h"
#include "index/secondary_index.h"
#include "table/table_dictionary.h"
#include "util/debug_sync.h"
#include "util/epoch_framework.h"
#include "wal/logger.h"

namespace helios::storage {
namespace silo {

namespace {

Tidword PublishedTid(Tidword commit_tid, const DataItem &item) {
  commit_tid.absent = !item.IsLive();
  return commit_tid;
}

}  // namespace

Transaction::Transaction(TableDictionary &tables, epoch::Framework &epoch,
                         index::Reaper &reaper, wal::Logger &logger,
                         Tidword &last_commit_tid)
    : tables_(tables),
      epoch_(epoch),
      reaper_(reaper),
      logger_(logger),
      last_commit_tid_(last_commit_tid) {}

Transaction::Transaction(Database &db)
    : Transaction(db.table_dictionary_, db.epoch_framework_, db.reaper_,
                  db.logger_, *db.last_commit_tids_.Get()) {}

void Transaction::Read(std::string_view table, std::string_view key,
                       Tidword observed) {
  read_set_.push_back({table, key, observed});
}

void Transaction::RangeRead(std::string_view table, std::string_view index,
                            std::string_view begin, std::string_view end,
                            uint64_t limit, bool reverse,
                            std::vector<std::string_view> keys,
                            std::vector<std::string_view> primary_keys) {
  range_set_.push_back({table, index, begin, end, limit, reverse,
                        std::move(keys), std::move(primary_keys)});
}

bool Transaction::Write(std::string_view table_name, std::string_view key,
                        std::string_view row_bytes, RowOp op,
                        std::string &reason) {
  Table *table = tables_.GetTable(table_name);
  if (table == nullptr) {
    reason = "write_table_missing";
    return false;
  }
  pax::PaxTable *store = table->GetPaxTable();
  if (store == nullptr) {
    reason = "pax_schema_missing";
    return false;
  }
  pax::Row row;
  if (op != RowOp::kDelete &&
      !pax::DecodeRow(store->schema(),
                      reinterpret_cast<const std::byte *>(row_bytes.data()),
                      row_bytes.size(), row)) {
    reason = "pax_row_decode_failed";
    return false;
  }

  auto &index = table->GetPrimaryIndex();
  DataItem *item = index.GetOrInsert(key);
  auto entry = write_set_.find(item);
  if (entry == write_set_.end()) {
    write_set_.emplace(item, WriteEntry{table,
                                        {},
                                        key,
                                        &index,
                                        false,
                                        RowUpdate{std::move(row), store, op,
                                                  op == RowOp::kInsert}});
    return true;
  }
  // The record already has a pending update; the last value wins, and the
  // absence an initial INSERT requires stays with it.
  auto &update = std::get<RowUpdate>(entry->second.update);
  if (op == RowOp::kInsert && update.op != RowOp::kDelete) {
    reason = kDuplicatePrimaryKeyAbortReason;
    return false;
  }
  update.row = std::move(row);
  update.op = op;
  return true;
}

bool Transaction::IndexWrite(std::string_view table_name,
                             std::string_view index_name,
                             std::string_view secondary_key,
                             std::string_view primary_key, bool remove,
                             std::string &reason) {
  Table *table = tables_.GetTable(table_name);
  if (table == nullptr) {
    reason = "secondary_index_table_missing";
    return false;
  }
  // Recovery refuses a log entry whose table has no schema, so a record on
  // one must not reach the log.
  if (table->GetPaxTable() == nullptr) {
    reason = "pax_schema_missing";
    return false;
  }
  index::SecondaryIndex *index = table->GetSecondaryIndex(index_name);
  if (index == nullptr) {
    reason = "secondary_index_missing";
    return false;
  }
  DataItem *item = index->tree.GetOrInsert(secondary_key);
  auto entry =
      write_set_
          .try_emplace(
              item, WriteEntry{table, index_name, secondary_key, &index->tree,
                               false, IndexUpdate{index->constraint, {}, {}}})
          .first;
  auto &update = std::get<IndexUpdate>(entry->second.update);
  update.deltas.push_back({primary_key, remove
                                            ? wal::SecondaryIndexOp::kDelete
                                            : wal::SecondaryIndexOp::kInsert});
  return true;
}

bool Transaction::Commit(CommitDurability durability, std::string &reason) {
  reason.clear();
  // A range without its end bound cannot be revalidated; refuse before
  // locking.
  for (const auto &range : range_set_) {
    if (range.end.empty()) {
      reason = "range_end_key_missing";
      return false;
    }
  }

  // A writer stays within kMaxLagEpochs of the durable epoch; wait here, before
  // any lock and before joining the epoch.
  if (!write_set_.empty()) logger_.WaitMaxLag(epoch_);

  // The largest word this attempt read or found under a lock (Silo §4.2).
  Tidword max_tid;

  // Phase 1: lock the write set in pointer order.
  for (auto &[item, entry] : write_set_) {
    if (!Lock(*item, entry, max_tid, reason)) {
      UnlockAll();
      return false;
    }
  }
  // Silo reads the epoch after the write locks. Nothing before this point
  // changes a stored value, and a lock failure returns before the join.
  const EpochNumber commit_epoch = epoch_.Join();

  // Phase 2: validate the read set, then choose the commit TID.
  if (!ValidateReads(max_tid, reason)) return Abort();
  // Constraints run under the locks after the reads, so a stale read is
  // reported as a conflict and not as a duplicate.
  for (auto &[item, entry] : write_set_) {
    if (!Prepare(*item, entry, reason)) return Abort();
  }

  const Tidword highest = std::max(max_tid, last_commit_tid_);
  // Recovery must not retain this commit while dropping the later state it
  // read.
  if (highest.epoch > commit_epoch) {
    reason = "commit_epoch_stale";
    return Abort();
  }
  // A newer epoch already orders after the observed TIDs; its tid starts at 0.
  Tidword commit_tid;
  commit_tid.epoch = commit_epoch;
  if (highest.epoch == commit_epoch) {
    if (highest.tid >= kMaxTid) {
      reason = "commit_tid_exhausted";
      return Abort();
    }
    commit_tid.tid = highest.tid + 1;
  }
  commit_tid.latest = true;

  // Reserve every destination before changing the first stored value.
  for (auto &[item, entry] : write_set_) {
    const auto *row = std::get_if<RowUpdate>(&entry.update);
    if (row == nullptr || row->op == RowOp::kDelete) continue;
    if (!item->AllocateSlot(*row->store)) {
      reason = "pax_slots_exhausted";
      return Abort();
    }
  }

  // Phase 3: install each record, copy its log, then publish and unlock it.
  last_commit_tid_ = commit_tid;
  wal::LogRecord record;
  record.epoch = commit_tid.epoch;
  bool row_applied = false;
  for (auto &[item, entry] : write_set_) {
    const bool is_row = std::holds_alternative<RowUpdate>(entry.update);
    if (is_row && row_applied)
      HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
    Apply(*item, entry, commit_tid.epoch);
    // Once unlocked, another writer may replace this record immediately.
    AppendLog(record, *item, entry, commit_tid);
    Publish(*item, entry, commit_tid);
    row_applied = row_applied || is_row;
  }

  // Buffer the record before leaving, so a flush cannot pass this commit.
  const bool logged = !record.writes.empty();
  if (logged) logger_.Enqueue(std::move(record));
  epoch_.Leave();
  // Leave before waiting, so this worker does not hold back epoch advancement.
  logger_.AwaitCommitDurability(
      commit_tid.epoch, logged && durability == CommitDurability::kSync);
  return true;
}

bool Transaction::OwnsLock(DataItem *item) const {
  const auto entry = write_set_.find(item);
  return entry != write_set_.end() && entry->second.owns_lock;
}

bool Transaction::Lock(DataItem &item, WriteEntry &entry, Tidword &max_tid,
                       std::string &reason) {
  for (;;) {
    Tidword current = item.transaction_id.load();
    if (current.lock) {
      _mm_pause();
      continue;
    }
    Tidword locked = current;
    locked.lock = true;
    if (!item.transaction_id.compare_exchange_weak(current, locked)) continue;
    entry.owns_lock = true;
    max_tid = std::max(max_tid, current);
    // A purge may detach the record while we wait for its lock.
    if (entry.index->Get(entry.key) != &item) {
      reason = "write_target_detached";
      return false;
    }
    return true;
  }
}

void Transaction::UnlockAll() {
  for (auto &[item, entry] : write_set_) {
    if (!entry.owns_lock) continue;
    Tidword tid = item->transaction_id.load();
    tid.lock = false;
    item->transaction_id.store(tid);
    entry.owns_lock = false;
  }
}

bool Transaction::Abort() {
  UnlockAll();
  epoch_.Leave();
  return false;
}

bool Transaction::Prepare(DataItem &item, WriteEntry &entry,
                          std::string &reason) {
  if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
    if (row->check_absent && !item.transaction_id.load().absent) {
      reason = kDuplicatePrimaryKeyAbortReason;
      return false;
    }
    return true;
  }

  // Apply the request's changes to the list this commit publishes.
  auto &index = std::get<IndexUpdate>(entry.update);
  index.primary_keys = std::atomic_load(&item.primary_keys);
  for (const auto &delta : index.deltas) {
    if (delta.op == wal::SecondaryIndexOp::kDelete) {
      index.primary_keys =
          PrimaryKeyList::Delete(index.primary_keys, delta.primary_key);
      continue;
    }
    // A UNIQUE key holds one primary key; adding that same key again is not
    // a second one.
    const PrimaryKeyList::View keys(index.primary_keys);
    if (index.constraint == IndexConstraint::kUnique && !keys.empty() &&
        !(keys.size() == 1 && keys.contains(delta.primary_key))) {
      reason =
          std::string(kDuplicateSecondaryKeyAbortPrefix) + "exists_after_lock";
      return false;
    }
    index.primary_keys =
        PrimaryKeyList::Insert(index.primary_keys, delta.primary_key);
  }
  return true;
}

bool Transaction::ValidateReads(Tidword &max_tid, std::string &reason) {
  for (const auto &read : read_set_) {
    Table *table = tables_.GetTable(read.table);
    if (table == nullptr) {
      // A read of a missing table returns the word 0 and nothing else.
      if (read.tid.obj != 0) {
        reason = "read_table_missing";
        return false;
      }
      continue;
    }
    DataItem *item = table->GetPrimaryIndex().Get(read.key);
    // A key with no record reads as the absent word.
    const Tidword current =
        item ? item->transaction_id.load() : Tidword::Absent();
    // Only our own lock may differ from the word the read observed.
    Tidword expected = read.tid;
    if (item != nullptr && OwnsLock(item)) expected.lock = true;
    if (current != expected) {
      reason = "exact_read_tid_moved";
      return false;
    }
    max_tid = std::max(max_tid, read.tid);
  }

  for (const auto &range : range_set_) {
    const bool ok = range.index.empty() ? RevalidateRange(range, max_tid)
                                        : RevalidateSecondaryRange(range, max_tid);
    if (!ok) {
      reason = range.index.empty() ? "primary_range_result_changed"
                                   : "secondary_range_result_changed";
      return false;
    }
  }
  return true;
}

bool Transaction::RevalidateRange(const RangeEntry &range, Tidword &max_tid) {
  auto table = tables_.GetTable(range.table);
  if (table == nullptr) return false;

  size_t result_pos = 0;
  bool aborted = false;
  bool matches = true;
  auto collect_key = [&](std::string_view key, DataItem &item) {
    const Tidword tid = item.transaction_id.load();
    if (tid.lock && !OwnsLock(&item)) {
      aborted = true;
      return true;
    }
    // Include tombstones too: their delete TIDs determine the state we saw.
    max_tid = std::max(max_tid, tid);
    if (!tid.absent) {
      if (result_pos >= range.keys.size() || range.keys[result_pos] != key) {
        matches = false;
        return true;
      }
      ++result_pos;
    }
    return range.limit > 0 && result_pos >= range.limit;
  };

  if (range.reverse) {
    table->GetPrimaryIndex().ScanReverse(range.begin, range.end, collect_key);
  } else {
    table->GetPrimaryIndex().Scan(range.begin, range.end, collect_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.keys.size();
}

bool Transaction::RevalidateSecondaryRange(const RangeEntry &range, Tidword &max_tid) {
  auto table = tables_.GetTable(range.table);
  if (table == nullptr) return false;
  auto *index = table->GetSecondaryIndex(range.index);
  if (index == nullptr) return false;

  size_t result_pos = 0;
  bool aborted = false;
  bool matches = true;
  auto collect_base_row = [&](std::string_view secondary_key,
                              std::string_view primary_key) {
    DataItem *item = table->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) return false;
    const Tidword tid = item->transaction_id.load();
    if (tid.lock && !OwnsLock(item)) {
      aborted = true;
      return true;
    }
    max_tid = std::max(max_tid, tid);
    if (!tid.absent) {
      if (result_pos >= range.keys.size() ||
          result_pos >= range.primary_keys.size() ||
          range.keys[result_pos] != secondary_key ||
          range.primary_keys[result_pos] != primary_key) {
        matches = false;
        return true;
      }
      ++result_pos;
    }
    return range.limit > 0 && result_pos >= range.limit;
  };

  auto collect_secondary_key = [&](std::string_view key, DataItem &) {
    // Find the current entry; the item supplied by the scan may be stale.
    DataItem *item = index->tree.Get(key);
    if (item == nullptr) return false;

    const Tidword tid = item->transaction_id.load();
    if (tid.lock && !OwnsLock(item)) {
      aborted = true;
      return true;
    }

    // The key list is a separate load; abort if the word moved around it.
    auto primary_keys = std::atomic_load(&item->primary_keys);
    if (item->transaction_id.load() != tid) {
      aborted = true;
      return true;
    }
    max_tid = std::max(max_tid, tid);

    for (std::string_view primary_key : PrimaryKeyList::View(primary_keys)) {
      if (collect_base_row(key, primary_key)) return true;
    }
    return false;
  };

  if (range.reverse) {
    index->tree.ScanReverse(range.begin, range.end, collect_secondary_key);
  } else {
    index->tree.Scan(range.begin, range.end, collect_secondary_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.keys.size() &&
         result_pos == range.primary_keys.size();
}

void Transaction::Apply(DataItem &item, const WriteEntry &entry,
                        EpochNumber epoch) {
  if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
    if (row->op == RowOp::kDelete)
      item.DeleteRow(epoch);
    else
      item.InstallRow(row->row, epoch);
    return;
  }
  std::atomic_store(&item.primary_keys,
                    std::get<IndexUpdate>(entry.update).primary_keys);
}

void Transaction::AppendLog(wal::LogRecord &record, const DataItem &item,
                            const WriteEntry &entry, Tidword commit_tid) {
  const Tidword published = PublishedTid(commit_tid, item);
  if (std::holds_alternative<RowUpdate>(entry.update)) {
    wal::LogRecord::Write write;
    write.key = entry.key;
    write.buffer = item.CopyValue();
    write.transaction_id = published;
    write.table_name = entry.table->Name();
    record.writes.emplace_back(std::move(write));
    return;
  }

  // Preserve all changes in order, including repeated primary keys.
  const auto &index = std::get<IndexUpdate>(entry.update);
  for (const auto &delta : index.deltas) {
    wal::LogRecord::Write write;
    write.key = entry.key;
    write.transaction_id = published;
    write.table_name = entry.table->Name();
    write.index_name = entry.index_name;
    write.index_type = static_cast<uint32_t>(index.constraint);
    write.secondary_op = delta.op;
    write.secondary_primary_key = delta.primary_key;
    record.writes.emplace_back(std::move(write));
  }
}

void Transaction::Publish(DataItem &item, WriteEntry &entry,
                          Tidword commit_tid) {
  const Tidword published = PublishedTid(commit_tid, item);
  item.transaction_id.store(published);
  entry.owns_lock = false;
  if (published.absent)
    reaper_.Enqueue(*entry.index, entry.key, item, published);
}

}  // namespace silo
}  // namespace helios::storage
