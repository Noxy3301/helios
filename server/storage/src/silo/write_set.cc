/**
 * @file server/storage/src/silo/write_set.cc
 * Registration, validation and installation of pending record updates.
 */

#include "silo/write_set.h"

#include <xmmintrin.h>
#include <algorithm>
#include <utility>

#include "index/masstree_index.h"
#include "index/reaper.h"
#include "pax/version_store.h"
#include "silo/commit.h"
#include "util/debug_sync.h"

namespace helios::storage::silo {

namespace {
Tidword PublishedTid(Tidword commit_tid, const DataItem &item) {
  commit_tid.absent = !item.IsLive();
  return commit_tid;
}
}  // namespace

bool WriteSet::AddRow(index::MasstreeIndex &index, const Write &write,
                      std::string &reason) {
  DataItem *item = index.GetOrInsert(write.key);
  auto [entry, inserted] = entries_.try_emplace(
      item,
      Entry{std::string(write.table_name),
            {},
            std::string(write.key),
            &index,
            false,
            RowUpdate{&write.value, write.op, write.op == RowOp::kInsert}});
  auto &row = std::get<RowUpdate>(entry->second.update);
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
                                   change.secondary_key, &index, false,
                                   IndexUpdate{constraint, {}, {}}})
          .first;
  auto &update = std::get<IndexUpdate>(entry->second.update);
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

bool WriteSet::Lock(Tidword &max_tid, std::string &reason) {
  for (auto &[item, entry] : entries_) {
    for (;;) {
      Tidword current = item->transaction_id.load();
      if (current.lock) {
        _mm_pause();
        continue;
      }
      Tidword locked = current;
      locked.lock = true;
      if (item->transaction_id.compare_exchange_weak(current, locked)) {
        entry.owns_lock = true;
        max_tid = std::max(max_tid, current);
        // A purge may detach the record while we wait for its lock.
        if (entry.index->Get(entry.key) != item) {
          reason = "write_target_detached";
          return false;
        }
        break;
      }
    }
  }
  return true;
}

void WriteSet::Unlock() {
  for (auto &[item, entry] : entries_) {
    if (!entry.owns_lock) continue;
    Tidword tid = item->transaction_id.load();
    tid.lock = false;
    item->transaction_id.store(tid);
    entry.owns_lock = false;
  }
}

bool WriteSet::OwnsLock(DataItem *item) const {
  const auto entry = entries_.find(item);
  return entry != entries_.end() && entry->second.owns_lock;
}

bool WriteSet::Validate(std::string &reason) {
  for (auto &[item, entry] : entries_) {
    if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
      if (row->check_committed_row && !item->transaction_id.load().absent) {
        reason = kDuplicatePrimaryKeyAbortReason;
        return false;
      }
    } else {
      auto &index = std::get<IndexUpdate>(entry.update);
      index.primary_keys = std::atomic_load(&item->primary_keys_);
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
  }
  return true;
}

bool WriteSet::AllocateSlots(std::string &reason) {
  for (const auto &[item, entry] : entries_) {
    const auto *row = std::get_if<RowUpdate>(&entry.update);
    if (row && row->op != RowOp::kDelete && !item->AllocateSlot()) {
      reason = "pax_slots_exhausted";
      return false;
    }
  }
  return true;
}

void WriteSet::Apply(EpochNumber commit_epoch) {
  // Capture the row's before-image once, before its final value is installed.
  pax::ScopedCommitEpoch scope(commit_epoch);
  bool row_applied = false;
  for (const auto &[item, entry] : entries_) {
    if (const auto *row = std::get_if<RowUpdate>(&entry.update)) {
      if (row_applied) HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
      if (row->op == RowOp::kDelete)
        item->Delete();
      else
        item->Write(*row->value);
      row_applied = true;
    } else {
      const auto &index = std::get<IndexUpdate>(entry.update);
      std::atomic_store(&item->primary_keys_, index.primary_keys);
    }
  }
}

wal::WriteSet WriteSet::BuildLog(Tidword commit_tid) const {
  wal::WriteSet log;
  log.reserve(entries_.size());
  for (const auto &[item, entry] : entries_) {
    wal::LogEntry record(entry.key, nullptr, 0, item, entry.table_name,
                         entry.index_name, PublishedTid(commit_tid, *item));
    if (std::holds_alternative<RowUpdate>(entry.update)) {
      record.value = item->CopyValue();
    } else {
      const auto &index = std::get<IndexUpdate>(entry.update);
      record.index_type = index.constraint;
      // Preserve all changes in order, including repeated primary keys.
      record.secondary_index_deltas = index.changes;
    }
    log.emplace_back(std::move(record));
  }
  return log;
}

void WriteSet::Publish(Tidword commit_tid, index::Reaper &reaper) {
  for (auto &[item, entry] : entries_) {
    const Tidword tid = PublishedTid(commit_tid, *item);
    item->transaction_id.store(tid);
    entry.owns_lock = false;
    if (tid.absent) reaper.Enqueue(*entry.index, entry.key, *item, tid);
  }
}

}  // namespace helios::storage::silo
