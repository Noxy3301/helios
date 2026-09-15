/**
 * @file server/storage/src/silo/write_set.cc
 * Registration of pending updates and operations on individual records.
 */

#include "silo/write_set.h"

#include <xmmintrin.h>
#include <algorithm>
#include <utility>

#include "index/masstree_index.h"
#include "index/reaper.h"
#include "silo/commit.h"

namespace helios::storage::silo {

namespace {
Tidword PublishedTid(Tidword commit_tid, const DataItem &item) {
  commit_tid.absent = !item.IsLive();
  return commit_tid;
}
}  // namespace

WriteSet::Entry::Entry(std::string table_name, std::string index_name,
                       std::string key, index::MasstreeIndex &index,
                       std::variant<RowUpdate, IndexUpdate> update)
    : table_name_(std::move(table_name)),
      index_name_(std::move(index_name)),
      key_(std::move(key)),
      index_(index),
      update_(std::move(update)) {}

bool WriteSet::AddRow(index::MasstreeIndex &index, const Write &write,
                      std::string &reason) {
  DataItem *item = index.GetOrInsert(write.key);
  auto [entry, inserted] = entries_.try_emplace(
      item, Entry{std::string(write.table_name),
                  {},
                  std::string(write.key),
                  index,
                  Entry::RowUpdate{&write.value, write.op,
                                   write.op == RowOp::kInsert}});
  auto &row = std::get<Entry::RowUpdate>(entry->second.update_);
  if (!inserted && write.op == RowOp::kInsert && row.op != RowOp::kDelete) {
    reason = kDuplicatePrimaryKeyAbortReason;
    return false;
  }
  row.value = &write.value;
  row.op = write.op;
  return true;
}

bool WriteSet::AddIndex(index::MasstreeIndex &index, IndexConstraint constraint,
                        const ExternalSecondaryIndexEntry &change,
                        std::string &reason) {
  DataItem *item = index.GetOrInsert(change.secondary_key);
  auto entry =
      entries_
          .try_emplace(item, Entry{change.table_name, change.index_name,
                                   change.secondary_key, index,
                                   Entry::IndexUpdate{constraint, {}, {}}})
          .first;
  auto &update = std::get<Entry::IndexUpdate>(entry->second.update_);
  if (!change.is_delete && constraint == IndexConstraint::kUnique &&
      std::any_of(update.changes.begin(), update.changes.end(),
                  [](const auto &previous) {
                    return previous.op == wal::SecondaryIndexOp::kInsert;
                  })) {
    reason =
        std::string(kDuplicateSecondaryKeyAbortPrefix) + "duplicate_in_request";
    return false;
  }
  update.changes.push_back(
      {change.primary_key, change.is_delete ? wal::SecondaryIndexOp::kDelete
                                            : wal::SecondaryIndexOp::kInsert});
  return true;
}

bool WriteSet::OwnsLock(DataItem *item) const {
  const auto entry = entries_.find(item);
  return entry != entries_.end() && entry->second.owns_lock_;
}

bool WriteSet::Entry::Lock(DataItem &item, Tidword &observed_tid,
                           std::string &reason) {
  for (;;) {
    Tidword current = item.transaction_id.load();
    if (current.lock) {
      _mm_pause();
      continue;
    }
    Tidword locked = current;
    locked.lock = true;
    if (item.transaction_id.compare_exchange_weak(current, locked)) {
      owns_lock_ = true;
      observed_tid = current;
      // A purge may detach the record while we wait for its lock.
      if (index_.Get(key_) != &item) {
        reason = "write_target_detached";
        return false;
      }
      return true;
    }
  }
}

void WriteSet::Entry::Unlock(DataItem &item) {
  if (!owns_lock_) return;
  Tidword tid = item.transaction_id.load();
  tid.lock = false;
  item.transaction_id.store(tid);
  owns_lock_ = false;
}

bool WriteSet::Entry::PrepareUpdate(DataItem &item, std::string &reason) {
  if (const auto *row = std::get_if<RowUpdate>(&update_)) {
    if (row->check_committed_row && !item.transaction_id.load().absent) {
      reason = kDuplicatePrimaryKeyAbortReason;
      return false;
    }
  } else {
    auto &index = std::get<IndexUpdate>(update_);
    index.primary_keys = std::atomic_load(&item.primary_keys_);
    for (const auto &change : index.changes) {
      if (change.op == wal::SecondaryIndexOp::kDelete) {
        index.primary_keys =
            PrimaryKeyList::Delete(index.primary_keys, change.primary_key);
      } else {
        if (index.constraint == IndexConstraint::kUnique &&
            !PrimaryKeyList::View(index.primary_keys).empty()) {
          reason = std::string(kDuplicateSecondaryKeyAbortPrefix) +
                   "exists_after_lock";
          return false;
        }
        index.primary_keys =
            PrimaryKeyList::Insert(index.primary_keys, change.primary_key);
      }
    }
  }
  return true;
}

bool WriteSet::Entry::AllocateSlot(DataItem &item, std::string &reason) const {
  const auto *row = std::get_if<RowUpdate>(&update_);
  if (row && row->op != RowOp::kDelete && !item.AllocateSlot()) {
    reason = "pax_slots_exhausted";
    return false;
  }
  return true;
}

void WriteSet::Entry::Apply(DataItem &item) const {
  if (const auto *row = std::get_if<RowUpdate>(&update_)) {
    if (row->op == RowOp::kDelete)
      item.Delete();
    else
      item.Write(*row->value);
  } else {
    const auto &index = std::get<IndexUpdate>(update_);
    std::atomic_store(&item.primary_keys_, index.primary_keys);
  }
}

wal::LogEntry WriteSet::Entry::BuildLog(DataItem &item,
                                        Tidword commit_tid) const {
  wal::LogEntry record(key_, nullptr, 0, &item, table_name_, index_name_,
                       PublishedTid(commit_tid, item));
  if (IsRow()) {
    record.value = item.CopyValue();
  } else {
    const auto &index = std::get<IndexUpdate>(update_);
    record.index_type = index.constraint;
    // Preserve all changes in order, including repeated primary keys.
    record.secondary_index_deltas = index.changes;
  }
  return record;
}

void WriteSet::Entry::Publish(DataItem &item, Tidword commit_tid,
                              index::Reaper &reaper) {
  const Tidword tid = PublishedTid(commit_tid, item);
  item.transaction_id.store(tid);
  owns_lock_ = false;
  if (tid.absent) reaper.Enqueue(index_, key_, item, tid);
}

bool WriteSet::Entry::IsRow() const {
  return std::holds_alternative<RowUpdate>(update_);
}

}  // namespace helios::storage::silo
