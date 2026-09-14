/**
 * @file server/storage/src/silo/commit.cc
 * Resolve targets, lock, validate, apply writes, and publish the commit.
 */

#include "silo/commit.h"

#include <xmmintrin.h>

#include <algorithm>
#include <cassert>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "index/data_item.h"
#include "index/masstree_index.h"
#include "index/reaper.h"
#include "index/secondary_index.h"
#include "pax/table.h"
#include "pax/version_store.h"
#include "silo/packed_transaction_id.h"
#include "silo/stable_read.h"
#include "table/table.h"
#include "table/table_dictionary.h"
#include "util/debug_sync.h"
#include "util/epoch_framework.h"
#include "wal/logger.h"

namespace helios::storage {
namespace silo {

namespace {

// A point read and its observed version, saved for validation.
struct ReadEntry {
  Table *table = nullptr;
  std::string table_name;
  std::string key;
  TransactionId captured_tid;
  bool found = false;
};

// A row write resolved to the primary-index entry it locks.
struct WriteEntry {
  std::string table_name;
  std::string key;
  // Borrows the decoded row from the request for this commit attempt.
  const pax::Row *value = nullptr;
  bool is_delete = false;
  DataItem *item = nullptr;
  index::MasstreeIndex *index = nullptr;
  // Check existing data when INSERT is the first operation on this key.
  bool check_committed_row = false;
};

// A secondary-index add or remove resolved to the entry it locks.
struct SecondaryIndexEntry {
  std::string table_name;
  std::string index_name;
  std::string secondary_key;
  std::string primary_key;
  bool is_delete = false;
  DataItem *item = nullptr;
  index::MasstreeIndex *index = nullptr;
  IndexConstraint index_type;
};

// The key and tree that must still point to the locked item.
struct LockTarget {
  DataItem *item = nullptr;
  index::MasstreeIndex *index = nullptr;
  std::string key;
};

struct LockedTid {
  DataItem *item = nullptr;
  TransactionId before_lock;
  TransactionId locked;
};

// State shared by the steps of one commit attempt.
struct CommitCtx {
  TableDictionary &tables;
  epoch::Framework &epoch;
  const CommitPayload &payload;
  std::string &abort_reason;

  std::vector<ReadEntry> reads;
  std::vector<WriteEntry> writes;
  std::vector<SecondaryIndexEntry> si_ops;
  std::vector<DataItem *> items;  // lock set: address-sorted and unique
  std::vector<LockTarget> targets;
  std::vector<LockedTid> locked;
  std::unordered_map<DataItem *, bool> si_empty;
  EpochNumber commit_epoch = 0;
  bool has_insert = false;

  // Abort before taking any row locks.
  bool Abort(const std::string &reason) {
    abort_reason = reason;
    epoch.Leave();
    return false;
  }

  // Release all locks acquired so far, then abort.
  bool AbortLocked(const std::string &reason) {
    abort_reason = reason;
    for (auto &entry : locked) {
      TransactionId current = entry.item->transaction_id.load();
      if (current.tid & kLockBit) {
        current.tid--;
        entry.item->transaction_id.store(current);
      }
    }
    epoch.Leave();
    return false;
  }

  bool IsOwnLocked(DataItem *item) const {
    for (const auto &entry : locked) {
      if (entry.item == item) return true;
    }
    return false;
  }

  // Abort on another transaction's lock instead of waiting while holding ours.
  bool LockedByOther(DataItem *item) const {
    TransactionId tid = item->transaction_id.load();
    if (!(tid.tid & kLockBit)) return false;
    return !IsOwnLocked(item);
  }
};

// Save read evidence and find the DataItems to lock for writes.
bool Resolve(CommitCtx &ctx, std::shared_mutex &schema_mutex) {
  std::unordered_set<std::string> unique_si_adds;
  // Track earlier writes to each key when the request includes an INSERT.
  std::unordered_map<std::string, bool> live_in_request;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);

  // Save each read's table, key, and observed version for validation.
  ctx.reads.reserve(ctx.payload.reads.size());
  for (const auto &read : ctx.payload.reads) {
    auto table = ctx.tables.GetTable(read.table_name);
    if (table == nullptr) {
      if (read.found || read.tid != 0) {
        return ctx.Abort("read_table_missing");
      }
      continue;
    }

    ctx.reads.push_back({table, read.table_name, read.key,
                         UnpackTransactionId(read.tid), read.found});
  }

  // Find each row's lock target, creating an absent DataItem for a new key.
  ctx.writes.reserve(ctx.payload.writes.size());
  for (const auto &write : ctx.payload.writes) {
    auto table = ctx.tables.GetTable(write.table_name);
    if (table == nullptr) {
      return ctx.Abort("write_table_missing");
    }

    if (table->GetPaxTable() == nullptr) return ctx.Abort("pax_schema_missing");

    DataItem *item = table->GetPrimaryIndex().GetOrInsert(write.key);

    // Reject INSERT if an earlier write in this request already made the key live.
    bool check_committed_row = false;
    if (ctx.has_insert) {
      const std::string request_key =
          std::string(write.table_name) + '\0' + std::string(write.key);
      auto live_it = live_in_request.find(request_key);
      if (write.op == RowOp::kInsert) {
        if (live_it == live_in_request.end()) {
          check_committed_row = true;
        } else if (live_it->second) {
          return ctx.Abort(kDuplicatePrimaryKeyAbortReason);
        }
      }
      live_in_request[request_key] = write.op != RowOp::kDelete;
    }

    auto *primary_index = &table->GetPrimaryIndex();
    ctx.writes.push_back({std::string(write.table_name), std::string(write.key),
                          &write.value, write.op == RowOp::kDelete, item,
                          primary_index, check_committed_row});
    ctx.items.push_back(item);
    ctx.targets.push_back({item, primary_index, std::string(write.key)});
  }

  // Find the secondary-index entries to lock.
  ctx.si_ops.reserve(ctx.payload.secondary_index_ops.size());
  for (const auto &op : ctx.payload.secondary_index_ops) {
    auto table = ctx.tables.GetTable(op.table_name);
    if (table == nullptr) {
      return ctx.Abort("si_table_missing");
    }

    if (table->GetPaxTable() == nullptr) return ctx.Abort("pax_schema_missing");

    index::SecondaryIndex *index = table->GetSecondaryIndex(op.index_name);
    if (index == nullptr) {
      return ctx.Abort("si_index_missing");
    }

    // Reject duplicate additions of a UNIQUE key within this request.
    if (!op.is_delete && index->constraint == IndexConstraint::kUnique) {
      const std::string unique_key =
          op.table_name + '\0' + op.index_name + '\0' + op.secondary_key;
      if (!unique_si_adds.insert(unique_key).second) {
        return ctx.Abort(std::string(kDuplicateSecondaryKeyAbortPrefix) +
                         "duplicate_in_request");
      }
    }

    // Create an absent entry if this secondary key has no slot to lock.
    DataItem *item = index->tree.GetOrInsert(op.secondary_key);

    ctx.si_ops.push_back({op.table_name, op.index_name, op.secondary_key,
                          op.primary_key, op.is_delete, item, &index->tree,
                          index->constraint});
    ctx.items.push_back(item);
    ctx.targets.push_back({item, &index->tree, op.secondary_key});
  }
  return true;
}

// Lock write targets in address order to avoid write-write deadlocks.
// Keep their previous TIDs for read validation.
bool Lock(CommitCtx &ctx) {
  std::sort(ctx.items.begin(), ctx.items.end());
  ctx.items.erase(std::unique(ctx.items.begin(), ctx.items.end()),
                  ctx.items.end());
  ctx.locked.reserve(ctx.items.size());

  // Recheck that each key still points to the item we locked.
  auto attached = [&](DataItem *item) {
    for (const auto &target : ctx.targets) {
      if (target.item != item) continue;
      DataItem *current = target.index->Get(target.key);
      if (current != item) return false;
    }
    return true;
  };

  // Wait for each target, then set its TID lock bit with CAS.
  for (auto *item : ctx.items) {
    for (;;) {
      TransactionId current = item->transaction_id.load();
      if (current.tid & kLockBit) {
        _mm_pause();
        continue;
      }
      TransactionId locked = current;
      locked.tid |= kLockBit;
      if (item->transaction_id.compare_exchange_weak(current, locked)) {
        ctx.locked.push_back({item, current, locked});
        if (!attached(item)) {
          return ctx.AbortLocked("write_target_detached");
        }
        break;
      }
    }
  }
  return true;
}

std::string KeyHex(const std::string &key) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(key.size() * 2);
  for (unsigned char byte : key) {
    out.push_back(kHex[byte >> 4]);
    out.push_back(kHex[byte & 0x0F]);
  }
  return out;
}

std::string ReadReason(const char *reason, const ReadEntry &read) {
  std::string out(reason);
  out += ':';
  out += read.table_name;
  out += ":key=";
  out += KeyHex(read.key);
  return out;
}

// Check that point reads still match their recorded presence and version.
bool ValidateReads(CommitCtx &ctx) {
  for (const auto &read : ctx.reads) {
    DataItem *item = read.table->GetPrimaryIndex().Get(read.key);
    if (item == nullptr) {
      // A row that was present must not disappear before validation.
      if (read.found) {
        return ctx.AbortLocked(ReadReason("exact_read_disappeared", read));
      }
      continue;
    }

    if (!read.found) {
      if (!item->HasRow()) {
        continue;
      }
      return ctx.AbortLocked(ReadReason("exact_read_appeared", read));
    }

    TransactionId expected = read.captured_tid;
    for (const auto &locked : ctx.locked) {
      if (locked.item == item) {
        if (locked.before_lock.epoch != read.captured_tid.epoch ||
            locked.before_lock.tid != read.captured_tid.tid) {
          return ctx.AbortLocked(ReadReason("exact_read_tid_moved", read));
        }
        expected = locked.locked;
        break;
      }
    }

    if (item->transaction_id.load() != expected) {
      return ctx.AbortLocked(ReadReason("exact_read_tid_moved", read));
    }
    if (!item->HasRow()) {
      return ctx.AbortLocked(ReadReason("exact_read_deleted", read));
    }
  }
  return true;
}

// Rescan a primary range; stop at the first difference in live keys.
bool ReplayRange(CommitCtx &ctx, const ExternalRangeReadEntry &range) {
  auto table = ctx.tables.GetTable(range.table_name);
  if (table == nullptr) return false;

  size_t result_pos = 0;
  bool aborted = false;
  bool matches = true;
  auto collect_key = [&](std::string_view key, DataItem &item) {
    if (ctx.LockedByOther(&item)) {
      aborted = true;
      return true;
    }
    if (item.HasRow()) {
      if (result_pos >= range.result_keys.size() ||
          std::string_view(range.result_keys[result_pos]) != key) {
        matches = false;
        return true;
      }
      ++result_pos;
    }
    return range.row_limit > 0 && result_pos >= range.row_limit;
  };

  if (range.reverse_scan) {
    table->GetPrimaryIndex().ScanReverse(range.start_key, range.end_key,
                                         collect_key);
  } else {
    table->GetPrimaryIndex().Scan(range.start_key, range.end_key, collect_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.result_keys.size();
}

// Rescan a secondary range and compare secondary/primary key pairs in order.
bool ReplayIndexRange(CommitCtx &ctx, const ExternalRangeReadEntry &range) {
  auto table = ctx.tables.GetTable(range.table_name);
  if (table == nullptr) return false;
  auto *index = table->GetSecondaryIndex(range.index_name);
  if (index == nullptr) return false;

  size_t result_pos = 0;
  bool aborted = false;
  bool matches = true;
  auto collect_base_row = [&](const std::string &secondary_key,
                              std::string_view primary_key) {
    DataItem *item = table->GetPrimaryIndex().Get(primary_key);
    if (item == nullptr) return false;
    if (ctx.LockedByOther(item)) {
      aborted = true;
      return true;
    }
    if (item->HasRow()) {
      if (result_pos >= range.result_keys.size() ||
          result_pos >= range.result_primary_keys.size() ||
          std::string_view(range.result_keys[result_pos]) !=
              std::string_view(secondary_key) ||
          std::string_view(range.result_primary_keys[result_pos]) !=
              primary_key) {
        matches = false;
        return true;
      }
      ++result_pos;
    }
    return range.row_limit > 0 && result_pos >= range.row_limit;
  };

  // Validation re-resolves the current entry even if the scan saw an older one.
  auto collect_secondary_key = [&](std::string_view key, DataItem &) {
    const std::string secondary_key(key);
    DataItem *item = index->tree.Get(key);
    if (item == nullptr) return false;
    // Pin the key list; allow our own lock, but abort on another lock
    // or a TID change during the load.
    const TransactionId observed = item->transaction_id.load();
    if ((observed.tid & kLockBit) && !ctx.IsOwnLocked(item)) {
      aborted = true;
      return true;
    }
    auto primary_keys = std::atomic_load(&item->primary_keys_);
    const bool secondary_live = primary_keys && primary_keys->count != 0;
    if (item->transaction_id.load() != observed) {
      aborted = true;
      return true;
    }
    if (!secondary_live) {
      return false;
    }
    for (std::string_view primary_key : PrimaryKeyList::View(primary_keys)) {
      if (collect_base_row(secondary_key, primary_key)) return true;
    }
    return false;
  };

  if (range.reverse_scan) {
    index->tree.ScanReverse(range.start_key, range.end_key,
                               collect_secondary_key);
  } else {
    index->tree.Scan(range.start_key, range.end_key, collect_secondary_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.result_keys.size() &&
         result_pos == range.result_primary_keys.size();
}

// Replay ranges and compare keys, order, and count.
// ValidateReads checks the point-read evidence submitted by the caller.
bool ValidateRanges(CommitCtx &ctx) {
  for (const auto &range : ctx.payload.range_reads) {
    const bool ok = range.index_name.empty() ? ReplayRange(ctx, range)
                                             : ReplayIndexRange(ctx, range);
    if (!ok) {
      return ctx.AbortLocked(range.index_name.empty()
                                 ? "primary_range_result_changed"
                                 : "secondary_range_result_changed");
    }
  }
  return true;
}

// Under the row lock, reject INSERT if the key already has a live row.
bool ValidateInserts(CommitCtx &ctx) {
  for (const auto &write : ctx.writes) {
    if (!write.check_committed_row) continue;
    if (write.item->HasRow()) {
      return ctx.AbortLocked(kDuplicatePrimaryKeyAbortReason);
    }
  }
  return true;
}

// Recheck UNIQUE keys under lock, applying this request's changes in order.
bool ValidateUnique(CommitCtx &ctx) {
  std::unordered_map<DataItem *, PrimaryKeyList::Ptr> si_primary_keys;
  for (const auto &op : ctx.si_ops) {
    if (op.index_type != IndexConstraint::kUnique) continue;

    auto [state_it, inserted] =
        si_primary_keys.emplace(op.item, PrimaryKeyList::Ptr{});
    if (inserted) {
      state_it->second = std::atomic_load(&op.item->primary_keys_);
    }
    auto &primary_keys = state_it->second;
    const PrimaryKeyList::View keys(primary_keys);

    auto key_it = keys.lower_bound(op.primary_key);
    const bool key_exists =
        key_it != keys.end() && *key_it == std::string_view(op.primary_key);

    if (op.is_delete) {
      if (key_exists) {
        primary_keys = PrimaryKeyList::Delete(primary_keys, op.primary_key);
      }
      continue;
    }

    if (!keys.empty()) {
      return ctx.AbortLocked(std::string(kDuplicateSecondaryKeyAbortPrefix) +
                             "exists_after_lock");
    }
    primary_keys = PrimaryKeyList::Insert(primary_keys, op.primary_key);
  }
  return true;
}

// Apply writes under lock; leave deleted entries for the reaper.
void ApplyWrites(CommitCtx &ctx) {
  {
    // Label captured before-images with this transaction's commit epoch.
    pax::ScopedCommitEpoch commit_epoch_scope(ctx.epoch.ThreadEpoch());
    size_t applied = 0;
    for (auto &write : ctx.writes) {
      if (applied > 0) {
        HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
      }
      if (write.is_delete) {
        write.item->Delete();
      } else {
        write.item->Write(*write.value);
      }
      ++applied;
    }
  }

  // Apply secondary-index changes; defer removal of empty entries.
  for (auto &op : ctx.si_ops) {
    const auto *primary_key =
        reinterpret_cast<const std::byte *>(op.primary_key.data());
    if (op.is_delete) {
      op.item->DeletePrimaryKey(primary_key, op.primary_key.size());
    } else {
      op.item->InsertPrimaryKey(primary_key, op.primary_key.size());
    }
  }

  // Record which secondary entries are empty after all changes, before unlock.
  // Later writers may replace these lists, so Publish uses this saved state.
  for (const auto &op : ctx.si_ops) {
    if (!op.is_delete) continue;
    const auto primary_keys = std::atomic_load(&op.item->primary_keys_);
    ctx.si_empty[op.item] = PrimaryKeyList::View(primary_keys).empty();
  }
}

// Copy final values into the log while row locks still protect them.
wal::WriteSet BuildLog(CommitCtx &ctx) {
  wal::WriteSet log_set;

  log_set.reserve(ctx.writes.size() + ctx.si_ops.size());
  for (const auto &write : ctx.writes) {
    wal::LogEntry entry(write.key, nullptr, 0, write.item, write.table_name,
                        "");
    entry.value = write.item->CopyValue();
    entry.tid = write.item->transaction_id.load();
    log_set.emplace_back(std::move(entry));
  }
  for (const auto &op : ctx.si_ops) {
    wal::LogEntry entry(op.secondary_key, nullptr, 0, op.item, op.table_name,
                        op.index_name, {}, op.index_type);
    entry.tid = op.item->transaction_id.load();
    entry.RecordSecondaryDelta(op.primary_key,
                               op.is_delete ? wal::SecondaryIndexOp::kDelete
                                            : wal::SecondaryIndexOp::kInsert);
    log_set.emplace_back(std::move(entry));
  }
  return log_set;
}

// Publish unlocked TIDs, update the log, and queue deletions for the reaper.
void Publish(CommitCtx &ctx, index::Reaper &reaper, wal::WriteSet &log_set) {
  // Unlock each item with a new TID in this transaction's commit epoch.
  ctx.commit_epoch = ctx.epoch.ThreadEpoch();
  std::unordered_map<DataItem *, TransactionId> published;
  published.reserve(ctx.items.size());
  for (auto *item : ctx.items) {
    // Start a new epoch at 2; odd sequence numbers have the lock bit set.
    constexpr uint32_t kFirstTidOfEpoch = 2;
    TransactionId current = item->transaction_id.load();
    const TransactionId unlocked =
        current.epoch == ctx.commit_epoch
            ? TransactionId{ctx.commit_epoch, current.tid + 1}
            : TransactionId{ctx.commit_epoch, kFirstTidOfEpoch};
    item->transaction_id.store(unlocked);
    published.emplace(item, unlocked);
  }

  // Replace locked TIDs in the log so recovery never restores a held lock.
  for (auto &entry : log_set) {
    const auto tid_it = published.find(entry.item);
    if (tid_it == published.end()) continue;
    entry.tid = tid_it->second;
  }

  // Queue row deletions with their published TIDs; the reaper checks them again.
  for (const auto &write : ctx.writes) {
    if (!write.is_delete) continue;
    auto tid_it = published.find(write.item);
    if (tid_it == published.end()) continue;
    reaper.Enqueue(*write.index, write.key, *write.item, tid_it->second);
  }

  // Queue each emptied secondary entry once, using its state before unlock.
  std::unordered_set<DataItem *> registered_si_purges;
  for (const auto &op : ctx.si_ops) {
    if (!op.is_delete) continue;
    auto empty_it = ctx.si_empty.find(op.item);
    if (empty_it == ctx.si_empty.end() || !empty_it->second) {
      continue;
    }
    if (!registered_si_purges.insert(op.item).second) continue;
    auto tid_it = published.find(op.item);
    if (tid_it == published.end()) continue;
    reaper.Enqueue(*op.index, op.secondary_key, *op.item, tid_it->second);
  }
}

// Queue the log and report whether this commit must wait for durability.
bool EnqueueLogSet(wal::Logger &logger, wal::WriteSet &log_set,
                   EpochNumber commit_epoch, CommitDurability durability) {
  if (log_set.empty()) return false;
  return logger.Enqueue(log_set, commit_epoch) &&
         durability == CommitDurability::kSync;
}

}  // namespace

bool Commit(TableDictionary &tables, std::shared_mutex &schema_mutex,
            epoch::Framework &epoch_framework, index::Reaper &reaper,
            wal::Logger &logger, const CommitPayload &payload,
            CommitDurability durability, std::string &abort_reason) {
  CommitCtx ctx{tables, epoch_framework, payload, abort_reason};

  // Enter the storage epoch before resolving and locking targets.
  epoch_framework.Join();

  ctx.has_insert = std::any_of(
      payload.writes.begin(), payload.writes.end(),
      [](const Write &entry) { return entry.op == RowOp::kInsert; });

  // Reject range evidence that omits the required exclusive end key.
  for (const auto &range : payload.range_reads) {
    if (range.end_key.empty()) {
      return ctx.Abort("range_end_key_missing");
    }
  }

  if (!Resolve(ctx, schema_mutex)) return false;

  // Test hook outside the schema lock so concurrent DDL can proceed.
  if (ctx.has_insert) {
    HELIOS_DEBUG_SYNC("silo_commit.after_index_claim");
  }

  if (!Lock(ctx)) return false;

  // Refresh the commit epoch after acquiring all write locks.
  // ThreadEpoch() keeps the epoch chosen by Join(), so leave and rejoin.
  // The separate Masstree RCU epoch stays active.
  epoch_framework.Leave();
  epoch_framework.Join();

  if (!ValidateReads(ctx)) return false;
  if (!ValidateRanges(ctx)) return false;
  if (!ValidateInserts(ctx)) return false;
  if (!ValidateUnique(ctx)) return false;

  // Reserve all destinations before changing the first stored value.
  for (const auto &write : ctx.writes) {
    if (!write.is_delete && !write.item->AllocateSlot()) {
      return ctx.AbortLocked("pax_slots_exhausted");
    }
  }

  ApplyWrites(ctx);
  wal::WriteSet log_set = BuildLog(ctx);
  Publish(ctx, reaper, log_set);
  const bool awaits_durability =
      EnqueueLogSet(logger, log_set, ctx.commit_epoch, durability);

  HELIOS_DEBUG_SYNC("silo_commit.before_offline");
  epoch_framework.Leave();

  logger.AwaitCommitDurability(ctx.commit_epoch, awaits_durability);
  return true;
}

}  // namespace silo
}  // namespace helios::storage
