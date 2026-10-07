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

// Upper bounds on the msgpack bytes a record adds around its strings: the
// record, epoch and write-list headers, and per write its array header, five
// string headers, the TID, index type, key list and op.
constexpr size_t kLogRecordFraming = 11;
constexpr size_t kLogWriteFraming = 44;

// Packs one write as LogRecord::Write's MSGPACK_DEFINE does. A commit logs no
// primary-key list, which packs as an empty array.
void pack_write(msgpack::packer<wal::PackedLogRecord> &pk, std::string_view key,
                std::string_view buffer, Tidword tid,
                std::string_view table_name, std::string_view index_name,
                uint32_t index_type, wal::SecondaryIndexOp op,
                std::string_view primary_key) {
  pk.pack_array(9);
  pk.pack(key);
  pk.pack(buffer);
  pk.pack(tid);
  pk.pack(table_name);
  pk.pack(index_name);
  pk.pack(index_type);
  pk.pack_array(0);
  pk.pack(op);
  pk.pack(primary_key);
}

// True when every record from `pos` on is a tombstone the re-scan did not
// reach, which the reaper purged as Transaction::match_record describes.
bool rest_purged(const std::vector<Transaction::RangeRecord> &records,
                 size_t pos) {
  return std::all_of(records.begin() + pos, records.end(),
                     [](const auto &record) { return record.tid.absent; });
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

void Transaction::reserve(size_t reads, size_t ranges) {
  read_set_.reserve(reads);
  range_set_.reserve(ranges);
}

void Transaction::Read(std::string_view table, std::string_view key,
                       Tidword observed, uint64_t column_mask) {
  read_set_.push_back({table, key, observed, column_mask});
}

void Transaction::RangeRead(std::string_view table, std::string_view index,
                            std::string_view begin, std::string_view end,
                            uint64_t limit, bool reverse,
                            std::vector<RangeRecord> rows,
                            std::vector<RangeRecord> visited) {
  range_set_.push_back({table, index, begin, end, limit, reverse,
                        std::move(rows), std::move(visited)});
}

bool Transaction::Write(std::string_view table_name, std::string_view key,
                        std::string_view row_bytes, RowOp op,
                        std::string &reason, uint64_t column_mask) {
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
      !pax::unpack_row(store->schema(),
                      reinterpret_cast<const std::byte *>(row_bytes.data()),
                      row_bytes.size(), row)) {
    reason = "pax_row_unpack_failed";
    return false;
  }

  auto &index = table->GetPrimaryIndex();
  // An inserted key is usually new.
  const bool expect_new = op == RowOp::kInsert;
  DataItem *item = index.GetOrInsert(key, expect_new);
  const std::string_view bytes =
      op == RowOp::kDelete ? std::string_view() : row_bytes;
  auto entry = write_set_.find(item);
  if (entry == write_set_.end()) {
    RowUpdate row_update{std::move(row), store, op, op == RowOp::kInsert,
                         bytes, column_mask};
    write_set_.emplace(
        item, WriteEntry{table, {}, key, &index, false, std::move(row_update)});
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
  update.bytes = bytes;
  update.column_mask |= column_mask;
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
  // A UNIQUE index keeps one record per secondary key, the lock its check
  // runs under. A non-unique one keeps one record per (secondary key, primary
  // key), under the two concatenated.
  std::string composite;
  std::string_view key = secondary_key;
  if (index->constraint != IndexConstraint::kUnique) {
    if (secondary_key.size() + primary_key.size() > index::kMaxKeyLength) {
      reason = "secondary_key_too_long";
      return false;
    }
    composite.append(secondary_key).append(primary_key);
    key = composite;
  }
  // An added key is usually new.
  DataItem *item = index->tree.GetOrInsert(key, !remove);
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

  // Phase 3: install each record, pack its log, then publish and unlock it.
  last_commit_tid_ = commit_tid;
  // Count the logged writes and bound their packed size, so the record packs
  // without regrowing.
  size_t log_writes = 0;
  size_t log_bytes = kLogRecordFraming;
  for (const auto &[item, entry] : write_set_) {
    const size_t names = kLogWriteFraming + entry.key.size() +
                         entry.table->Name().size() + entry.index_name.size();
    if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
      ++log_writes;
      log_bytes += names + row->bytes.size();
      continue;
    }
    for (const auto &delta : std::get<IndexUpdate>(entry.update).deltas) {
      ++log_writes;
      log_bytes += names + delta.primary_key.size();
    }
  }
  // The record packs as LogRecord's MSGPACK_DEFINE does: [epoch, [write...]].
  wal::PackedLogRecord record;
  record.epoch = commit_tid.epoch;
  msgpack::packer<wal::PackedLogRecord> pk(record);
  if (log_writes != 0) {
    record.bytes.reserve(log_bytes);
    pk.pack_array(2);
    pk.pack(record.epoch);
    pk.pack_array(static_cast<uint32_t>(log_writes));
  }
  bool row_applied = false;
  for (auto &[item, entry] : write_set_) {
    const bool is_row = std::holds_alternative<RowUpdate>(entry.update);
    if (is_row && row_applied)
      HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
    Apply(*item, entry, commit_tid);
    // Once unlocked, another writer may replace this record immediately.
    AppendLog(pk, *item, entry, commit_tid);
    Publish(*item, entry, commit_tid);
    row_applied = row_applied || is_row;
  }

  // Buffer the record before leaving, so a flush cannot pass this commit.
  const bool logged = log_writes != 0;
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
    // The reaper clears latest when it unlinks the record.
    if (!current.latest) {
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
          PrimaryKeyList::erase(index.primary_keys, delta.primary_key);
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
        PrimaryKeyList::insert(index.primary_keys, delta.primary_key);
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
      // A masked read outlives a moved word only on a live row nobody locks.
      const bool maskable =
          read.column_mask != 0 &&  // the plugin named the fields it read
          item != nullptr &&
          !current.lock &&    // no install in flight, this attempt's too
          !current.absent &&  // the row exists now,
          current.latest &&   // is still the key's record,
          !read.tid.absent;   // and existed when read
      bool held =
          maskable &&
          // no install since changed or assigned a used field or the null flags
          item->pax_group()->max_column_tid(read.column_mask | 1) <=
              read.tid.obj;
      // The word unchanged across both loads means no install came between.
      if (held) {
        std::atomic_thread_fence(std::memory_order_acquire);
        held = item->transaction_id.load() == current;
      }
      if (!held) {
        reason = "exact_read_tid_moved";
        return false;
      }
      max_tid = std::max(max_tid, current);
      continue;
    }
    max_tid = std::max(max_tid, read.tid);
  }

  for (const auto &range : range_set_) {
    HELIOS_DEBUG_SYNC("silo_commit.before_range_revalidation");
    const bool ok = range.index.empty()
                        ? RevalidateRange(range, max_tid)
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

  size_t row = 0;
  size_t visited = 0;
  uint64_t live = 0;
  bool matches = true;
  // Match each record the re-scan reaches, up to its limit-th live row.
  auto match_key = [&](std::string_view key, DataItem &item) {
    const Tidword tid = item.transaction_id.load();
    if (!match_record(range, row, visited, key, &item, tid, false, max_tid)) {
      matches = false;
      return true;
    }
    if (!tid.absent) ++live;
    return range.limit > 0 && live >= range.limit;
  };

  if (range.reverse) {
    table->GetPrimaryIndex().ScanReverse(range.begin, range.end, match_key);
  } else {
    table->GetPrimaryIndex().Scan(range.begin, range.end, match_key);
  }
  return matches && row == range.rows.size() &&
         rest_purged(range.visited, visited);
}

bool Transaction::RevalidateSecondaryRange(const RangeEntry &range,
                                           Tidword &max_tid) {
  auto table = tables_.GetTable(range.table);
  if (table == nullptr) return false;
  auto *index = table->GetSecondaryIndex(range.index);
  if (index == nullptr) return false;

  size_t row = 0;
  size_t visited = 0;
  uint64_t live = 0;
  bool matches = true;
  auto match_entry = [&](std::string_view key, DataItem &) {
    // Find the current entry; the item supplied by the scan may be stale.
    DataItem *item = index->tree.Get(key);
    if (item == nullptr) return false;

    // The key list is a separate load; abort if the word moved around it.
    const Tidword tid = item->transaction_id.load();
    auto primary_keys = std::atomic_load(&item->primary_keys);
    if (item->transaction_id.load() != tid ||
        !match_record(range, row, visited, key, item, tid, true, max_tid)) {
      matches = false;
      return true;
    }

    // The entry's word fixes its list, so its base records follow in order. A
    // missing base record reads as no record, as the scan skipped it.
    for (std::string_view primary_key : PrimaryKeyList::View(primary_keys)) {
      DataItem *base = table->GetPrimaryIndex().Get(primary_key);
      if (base == nullptr) continue;
      const Tidword base_tid = base->transaction_id.load();
      if (!match_record(range, row, visited, primary_key, base, base_tid, false,
                        max_tid)) {
        matches = false;
        return true;
      }
      if (!base_tid.absent) ++live;
      if (range.limit > 0 && live >= range.limit) return true;
    }
    return false;
  };

  if (range.reverse) {
    index->tree.ScanReverse(range.begin, range.end, match_entry);
  } else {
    index->tree.Scan(range.begin, range.end, match_entry);
  }
  return matches && row == range.rows.size() &&
         rest_purged(range.visited, visited);
}

bool Transaction::match_record(const RangeEntry &range, size_t &row,
                               size_t &visited, std::string_view key,
                               DataItem *item, Tidword tid, bool entry,
                               Tidword &max_tid) const {
  if (tid.lock) {
    if (!OwnsLock(item)) return false;
    tid.lock = false;
  }
  // A record the reaper unlinked (latest clear) counts as not reached.
  if (!tid.latest) return true;
  if (tid == Tidword::Absent()) return true;

  // A skipped tombstone's deleter serializes before this attempt: the reaper
  // purges only deletes of epoch <= E - 2, and E <= e + 1 while this attempt
  // is joined at e.
  const bool live = !entry && !tid.absent;
  const auto &records = live ? range.rows : range.visited;
  size_t &pos = live ? row : visited;
  while (pos < records.size() && records[pos].key != key &&
         records[pos].tid.absent) {
    ++pos;
  }
  if (pos >= records.size() || records[pos].key != key ||
      records[pos].tid != tid) {
    return false;
  }
  ++pos;
  max_tid = std::max(max_tid, tid);
  return true;
}

void Transaction::Apply(DataItem &item, const WriteEntry &entry,
                        Tidword commit_tid) {
  if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
    if (row->op == RowOp::kDelete) {
      item.DeleteRow(commit_tid.epoch);
      return;
    }
    const bool inserted = item.size() == 0;
    uint64_t fields =
        item.InstallRow(row->row, commit_tid.epoch) | row->column_mask;

    // Every masked read validates field 0, so raising it for an insert or a
    // null-flag change fails every older masked read in the group. The raise
    // precedes Publish, so a validator that loads the new word sees it.
    if (inserted) fields = 1;
    item.pax_group()->update_max_column_tid(fields, commit_tid.obj);
    return;
  }
  std::atomic_store(&item.primary_keys,
                    std::get<IndexUpdate>(entry.update).primary_keys);
}

void Transaction::AppendLog(msgpack::packer<wal::PackedLogRecord> &pk,
                            const DataItem &item, const WriteEntry &entry,
                            Tidword commit_tid) {
  const Tidword published = PublishedTid(commit_tid, item);
  const std::string &table_name = entry.table->Name();
  if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
    // The bytes the install unpacked; a delete logs an empty value.
    pack_write(pk, entry.key, row->bytes, published, table_name, {}, 0,
               wal::SecondaryIndexOp::kNone, {});
    return;
  }

  // Preserve all changes in order, including repeated primary keys.
  const auto &index = std::get<IndexUpdate>(entry.update);
  for (const auto &delta : index.deltas) {
    pack_write(pk, entry.key, {}, published, table_name, entry.index_name,
               static_cast<uint32_t>(index.constraint), delta.op,
               delta.primary_key);
  }
}

void Transaction::Publish(DataItem &item, WriteEntry &entry,
                          Tidword commit_tid) {
  const Tidword published = PublishedTid(commit_tid, item);
  item.transaction_id.store(published);
  entry.owns_lock = false;
  if (!published.absent) return;

  // A non-unique record sits under its secondary key and its one primary key.
  const auto *index = std::get_if<IndexUpdate>(&entry.update);
  if (index == nullptr || index->constraint == IndexConstraint::kUnique) {
    reaper_.Enqueue(*entry.index, entry.key, item, published);
    return;
  }
  std::string key(entry.key);
  key += index->deltas.front().primary_key;
  reaper_.Enqueue(*entry.index, key, item, published);
}

}  // namespace silo
}  // namespace helios::storage
