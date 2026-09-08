/**
 * @file server/storage/src/silo/commit.cc
 * The commit protocol step by step: resolve, lock, validate, install.
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
#include "index/primary_index.h"
#include "index/reaper.h"
#include "index/secondary_index.h"
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

// A point read to re-validate, with the version the caller observed.
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
  // Into the payload the caller submitted, which outlives this attempt.
  std::string_view value;
  bool is_delete = false;
  DataItem *item = nullptr;
  index::PrimaryIndex *index = nullptr;
  // Insert entry that is the first entry for its key in this request, so
  // the committed row is what decides whether the key is free.
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
  index::SecondaryIndex *index = nullptr;
  IndexConstraint index_type;
};

// The index entry a locked item must still be reachable through.
struct LockTarget {
  DataItem *item = nullptr;
  index::PrimaryIndex *primary_index = nullptr;
  index::SecondaryIndex *secondary_index = nullptr;
  std::string key;
};

struct LockedTid {
  DataItem *item = nullptr;
  TransactionId before_lock;
  TransactionId locked;
};

/**
 * @brief One commit attempt's working state, passed between the phases.
 *
 * @details Resolve fills the entry vectors and the lock set; Phase 1 fills
 * `locked`; Phase 3 fills the commit epoch.
 */
struct CommitCtx {
  TableDictionary &tables;
  epoch::Framework &epoch;
  const CommitPayload &payload;
  std::string *abort_reason;

  std::vector<ReadEntry> reads;
  std::vector<WriteEntry> writes;
  std::vector<SecondaryIndexEntry> si_ops;
  std::vector<DataItem *> items;  // lock set: address-sorted and unique
  std::vector<LockTarget> targets;
  std::vector<LockedTid> locked;
  std::unordered_map<DataItem *, bool> si_empty;
  EpochNumber commit_epoch = 0;
  bool has_insert = false;

  // Abort before the lock loop: nothing to release.
  bool Abort(const std::string &reason) {
    if (abort_reason != nullptr) *abort_reason = reason;
    epoch.Leave();
    return false;
  }

  // Abort after the lock loop: release every lock this attempt took.
  bool AbortLocked(const std::string &reason) {
    if (abort_reason != nullptr) *abort_reason = reason;
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

  // Silo Phase 2 read validation is wait-free: a record locked by another
  // transaction is treated as dirty and forces abort. Spinning here would
  // break the paper's deadlock-freedom invariant, since sorted write-lock
  // acquisition protects only write-to-write edges, not the read-to-write
  // edges validators introduce.
  bool LockedByOther(DataItem *item) const {
    TransactionId tid = item->transaction_id.load();
    if (!(tid.tid & kLockBit)) return false;
    return !IsOwnLocked(item);
  }
};

// Resolve (R1-R3): map reads, writes, and SI ops to their DataItems.
bool Resolve(CommitCtx &ctx, std::shared_mutex &schema_mutex) {
  std::unordered_set<std::string> unique_si_adds;
  // Whether a key is live after the entries this request has already
  // resolved; absent means the request has not touched it. Only an insert
  // consults it, so a request without one does not pay for it.
  std::unordered_map<std::string, bool> live_in_request;

  std::shared_lock<std::shared_mutex> lk(schema_mutex);

  // Resolve point reads to the DataItem and version the caller observed
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

  // Resolve row writes and deletes to the primary-index entries to lock.
  // R2: a key with no DataItem yet cannot be locked, so GetOrInsert
  // materializes a blank slot (uninitialized, TID 0); this mutates the tree
  // but takes no row lock (Silo's native insert stages an "absent" record the
  // same way).
  ctx.writes.reserve(ctx.payload.writes.size());
  for (const auto &write : ctx.payload.writes) {
    auto table = ctx.tables.GetTable(write.table_name);
    if (table == nullptr) {
      return ctx.Abort("write_table_missing");
    }

    DataItem *item = table->GetPrimaryIndex().GetOrInsert(write.key);
    assert(item != nullptr);  // GetOrInsert materializes a blank slot

    // An insert onto a key an earlier entry of this request already made
    // live is a duplicate the committed state cannot excuse.
    bool check_committed_row = false;
    if (ctx.has_insert) {
      const std::string request_key = write.table_name + '\0' + write.key;
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
    ctx.writes.push_back({write.table_name, write.key, write.value,
                          write.op == RowOp::kDelete, item, primary_index,
                          check_committed_row});
    ctx.items.push_back(item);
    ctx.targets.push_back({item, primary_index, nullptr, write.key});
  }

  // Resolve secondary-index updates to the secondary-index entries to lock
  ctx.si_ops.reserve(ctx.payload.secondary_index_ops.size());
  for (const auto &op : ctx.payload.secondary_index_ops) {
    auto table = ctx.tables.GetTable(op.table_name);
    if (table == nullptr) {
      return ctx.Abort("si_table_missing");
    }

    index::SecondaryIndex *index = table->GetSecondaryIndex(op.index_name);
    if (index == nullptr) {
      return ctx.Abort("si_index_missing");
    }

    // R3: reject the same UNIQUE SI key appearing twice in this request.
    if (!op.is_delete && index->IsUnique()) {
      const std::string unique_key =
          op.table_name + '\0' + op.index_name + '\0' + op.secondary_key;
      if (!unique_si_adds.insert(unique_key).second) {
        return ctx.Abort(std::string(kDuplicateSecondaryKeyAbortPrefix) +
                         "duplicate_in_request");
      }
    }

    // Locking needs a slot, so an entry whose secondary key has none seeds a
    // blank one here.
    DataItem *item = index->GetOrInsert(op.secondary_key);

    ctx.si_ops.push_back({op.table_name, op.index_name, op.secondary_key,
                          op.primary_key, op.is_delete, item, index,
                          index->GetIndexType()});
    ctx.items.push_back(item);
    ctx.targets.push_back({item, nullptr, index, op.secondary_key});
  }
  return true;
}

// Phase 1.1: address-sort and CAS-lock every primary and secondary write
// target. One global lock
// order keeps concurrent committers free of write-write deadlock; the
// pre-lock TID is kept because validation must compare reads against it, not
// against the TID this transaction has just dirtied.
bool Lock(CommitCtx &ctx) {
  std::sort(ctx.items.begin(), ctx.items.end());
  ctx.items.erase(std::unique(ctx.items.begin(), ctx.items.end()),
                  ctx.items.end());
  ctx.locked.reserve(ctx.items.size());

  // True while every index key this attempt locked the item under still
  // resolves to it.
  auto attached = [&](DataItem *item) {
    for (const auto &target : ctx.targets) {
      if (target.item != item) continue;
      DataItem *current = nullptr;
      if (target.primary_index != nullptr) {
        current = target.primary_index->Get(target.key);
      } else if (target.secondary_index != nullptr) {
        current = target.secondary_index->Get(target.key);
      } else {
        continue;
      }
      if (current != item) return false;
    }
    return true;
  };

  // Lock loop: spin until the LSB CAS lands.
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

// Phase 2.1: re-read exact-key TIDs and confirm they have not moved; a moved
// TID means a concurrent commit overwrote the row after the caller read it.
bool ValidateReads(CommitCtx &ctx) {
  for (const auto &read : ctx.reads) {
    DataItem *item = read.table->GetPrimaryIndex().Get(read.key);
    if (item == nullptr) {
      // A read observed as present must still resolve at validation
      // time. An unresolvable key here means a committed delete purged
      // the slot after the read, which is a serializability conflict.
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

// Replay one primary range scan and compare the key list positionally.
// Stopping at the first divergence bounds the replay at one live row past
// the evidence even when the range is unlimited.
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

// Replay one secondary range scan and compare the secondary keys and their
// primary keys, in scan order.
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

  auto collect_secondary_key = [&](std::string_view key) {
    const std::string secondary_key(key);
    DataItem *item = index->Get(key);
    if (item == nullptr) return false;
    // Pin the immutable primary-key list under a stable read, as
    // silo::ScanIndex does. A committer publishes a new list under its lock;
    // the TID stays constant while own-locked, so re-check after loading.
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
    index->ScanReverse(range.start_key, range.end_key, collect_secondary_key);
  } else {
    index->Scan(range.start_key, range.end_key, collect_secondary_key);
  }
  if (aborted) return false;
  return matches && result_pos == range.result_keys.size() &&
         result_pos == range.result_primary_keys.size();
}

// Phase 2.2: replay each range scan and compare the key lists. This is the
// phantom check Silo performs with Masstree node versions (physical
// validation), done by value because the caller cannot hold node pointers
// across the RPC boundary. The replay compares the keys in scan order and
// by count; row TIDs are validated at 2.1.
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

// Phase 2.3: an insert must find its key free. Checked under the write lock
// that installs the rows, so a competing inserter of the same key is
// serialized behind it and sees the row this transaction is about to write.
bool ValidateInserts(CommitCtx &ctx) {
  for (const auto &write : ctx.writes) {
    if (!write.check_committed_row) continue;
    if (write.item->HasRow()) {
      return ctx.AbortLocked(kDuplicatePrimaryKeyAbortReason);
    }
  }
  return true;
}

// Phase 2.4: post-lock UNIQUE recheck. A competing add may have installed the
// same secondary key during the wait on the write lock, so the
// resolve-time dedup (R3) is not enough on its own.
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

// Phase 3.1: install row writes/deletes and SI add/remove. Deletes leave
// tombstones in the tree; physical removal is deferred until a later epoch so
// same-key reinserts reuse the slot and advance its TID chain.
void Install(CommitCtx &ctx) {
  {
    // Tags the install region with the commit epoch so the PAX
    // before-image capture can label its entries.
    pax::ScopedCommitEpoch commit_epoch_scope(ctx.epoch.ThreadEpoch());
    size_t installed = 0;
    for (auto &write : ctx.writes) {
      if (installed > 0) {
        HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
      }
      if (write.is_delete) {
        write.item->Reset(nullptr, 0);
      } else {
        write.item->Reset(
            reinterpret_cast<const std::byte *>(write.value.data()),
            write.value.size());
      }
      ++installed;
    }
  }

  // Install secondary-index add/remove. Empty SI slots are tombstones too;
  // their physical removal is deferred with primary rows.
  for (auto &op : ctx.si_ops) {
    const auto *primary_key =
        reinterpret_cast<const std::byte *>(op.primary_key.data());
    if (op.is_delete) {
      op.item->DeletePrimaryKey(primary_key, op.primary_key.size());
    } else {
      op.item->InsertPrimaryKey(primary_key, op.primary_key.size());
    }
  }

  // Capture SI tombstone state while the slots are still locked. The live
  // primary-key list pointer must not be read after unlock: a concurrent
  // committer can publish a replacement under its own lock.
  // Computed after the whole install loop so a delete-then-add sequence on
  // the same slot within this transaction reads the final state.
  for (const auto &op : ctx.si_ops) {
    if (!op.is_delete) continue;
    const auto primary_keys = std::atomic_load(&op.item->primary_keys_);
    ctx.si_empty[op.item] = PrimaryKeyList::View(primary_keys).empty();
  }
}

// Phase 3.2: build the log snapshot before unlock so a later transaction
// cannot overwrite the values just logged.
WriteSetType BuildLog(CommitCtx &ctx) {
  WriteSetType log_set;

  log_set.reserve(ctx.writes.size() + ctx.si_ops.size());
  for (const auto &write : ctx.writes) {
    Snapshot snapshot(write.key, nullptr, 0, write.item, write.table_name, "");
    snapshot.data_item_copy = *write.item;
    log_set.emplace_back(std::move(snapshot));
  }
  for (const auto &op : ctx.si_ops) {
    Snapshot snapshot(op.secondary_key, nullptr, 0, op.item, op.table_name,
                      op.index_name, {}, op.index_type);
    snapshot.data_item_copy = *op.item;
    snapshot.RecordSecondaryDelta(
        op.primary_key,
        op.is_delete ? SecondaryIndexOp::kRemove : SecondaryIndexOp::kAdd);
    log_set.emplace_back(std::move(snapshot));
  }
  return log_set;
}

// Phase 3.3-3.4: publish the new TIDs, stamp them into the log snapshot, and
// hand slots this transaction left empty to the reaper.
void Publish(CommitCtx &ctx, index::Reaper &reaper, WriteSetType &log_set) {
  // Unlock by writing the new TID. Carry the epoch forward when the captured
  // TID is from an earlier epoch.
  ctx.commit_epoch = ctx.epoch.ThreadEpoch();
  std::unordered_map<DataItem *, TransactionId> published;
  published.reserve(ctx.items.size());
  for (auto *item : ctx.items) {
    // The first version of an epoch takes the lowest even sequence, since
    // an odd one is a held lock.
    constexpr uint32_t kFirstTidOfEpoch = 2;
    TransactionId current = item->transaction_id.load();
    const TransactionId unlocked =
        current.epoch == ctx.commit_epoch
            ? TransactionId{ctx.commit_epoch, current.tid + 1}
            : TransactionId{ctx.commit_epoch, kFirstTidOfEpoch};
    item->transaction_id.store(unlocked);
    published.emplace(item, unlocked);
  }

  // The log snapshot was captured under the lock and still carries the
  // locked TID; recovery would install it verbatim, and every later access
  // to the key would spin on a lock nobody owns. Publish the unlocked TID
  // into the snapshot.
  for (auto &snapshot : log_set) {
    const auto tid_it = published.find(snapshot.item);
    if (tid_it == published.end()) continue;
    snapshot.data_item_copy.transaction_id.store(tid_it->second);
  }

  // Register slots left empty by this transaction for deferred physical
  // purge, keyed by the published unlocked TID; immediate removal could free
  // memory still visible to concurrent readers.
  for (const auto &write : ctx.writes) {
    if (!write.is_delete) continue;
    auto tid_it = published.find(write.item);
    if (tid_it == published.end()) continue;
    reaper.Enqueue(write.index, nullptr, write.key, write.item, tid_it->second);
  }

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
    reaper.Enqueue(nullptr, op.index, op.secondary_key, op.item,
                   tid_it->second);
  }
}

/**
 * @brief Phase 3.5: enqueue the log set.
 *
 * @return true when the caller must wait for the device.
 */
bool EnqueueLogSet(wal::Logger &logger, WriteSetType &log_set,
                   EpochNumber commit_epoch, CommitDurability durability) {
  if (log_set.empty()) return false;
  return logger.Enqueue(log_set, commit_epoch) &&
         durability == CommitDurability::kSync;
}

}  // namespace

bool Commit(TableDictionary &tables, std::shared_mutex &schema_mutex,
            epoch::Framework &epoch_framework, index::Reaper &reaper,
            wal::Logger &logger, const CommitPayload &payload,
            CommitDurability durability, std::string *abort_reason) {
  CommitCtx ctx{tables, epoch_framework, payload, abort_reason};

  // Epoch join.
  epoch_framework.Join();
  if (abort_reason != nullptr) abort_reason->clear();

  ctx.has_insert = std::any_of(payload.writes.begin(), payload.writes.end(),
                               [](const ExternalWriteEntry &entry) {
                                 return entry.op == RowOp::kInsert;
                               });

  // A range entry without its exclusive end bound cannot be replayed;
  // abort instead of skipping the validation.
  for (const auto &range : payload.range_reads) {
    if (range.end_key.empty()) {
      return ctx.Abort("range_end_key_missing");
    }
  }

  if (!Resolve(ctx, schema_mutex)) return false;

  // Outside the schema lock: holding it here would block a concurrent
  // connection's DDL, not only its insert.
  if (ctx.has_insert) {
    HELIOS_DEBUG_SYNC("silo_commit.after_index_claim");
  }

  if (!Lock(ctx)) return false;

  // Phase 1.2: re-read the global epoch with all locks held. This is the
  // serialization point: epoch-grouped commit and recovery follow the serial
  // order only if the commit epoch is taken here. The thread-local epoch is
  // fixed at join time, so leaving and re-joining is the only way to re-read
  // it.
  epoch_framework.Leave();
  epoch_framework.Join();

  if (!ValidateReads(ctx)) return false;
  if (!ValidateRanges(ctx)) return false;
  if (!ValidateInserts(ctx)) return false;
  if (!ValidateUnique(ctx)) return false;

  Install(ctx);
  WriteSetType log_set = BuildLog(ctx);
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
