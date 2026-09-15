/**
 * @file server/storage/src/silo/commit.cc
 * Resolve targets, lock, validate, apply writes, and publish the commit.
 */

#include "silo/commit.h"

#include <algorithm>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "index/data_item.h"
#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "pax/version_store.h"
#include "silo/stable_read.h"
#include "silo/write_set.h"
#include "table/table.h"
#include "table/table_dictionary.h"
#include "util/debug_sync.h"
#include "util/epoch_framework.h"
#include "wal/logger.h"

namespace helios::storage {
namespace silo {

namespace {

// State shared by the steps of one commit attempt.
struct CommitCtx {
  TableDictionary &tables;
  epoch::Framework &epoch;
  const CommitPayload &payload;
  std::string &abort_reason;

  // Use the caller's observations directly, including repeated reads.
  const std::vector<ExternalReadEntry> &read_set = payload.reads;
  WriteSet write_set;
  EpochNumber commit_epoch = 0;
  Tidword commit_tid{};
  // The largest word this attempt read, and the largest it found under a lock.
  Tidword max_read_tid{};
  Tidword max_write_tid{};
  bool has_insert = false;

  // Values are still unpublished; release any locks this attempt acquired.
  bool Abort(const std::string &reason) {
    abort_reason = reason;
    for (auto &[item, entry] : write_set) entry.Unlock(*item);
    epoch.Leave();
    return false;
  }
};

// Resolve the indexes; WriteSet manages records and duplicate updates.
bool ResolveWrites(CommitCtx &ctx, std::shared_mutex &schema_mutex) {
  std::shared_lock<std::shared_mutex> lk(schema_mutex);
  for (const auto &write : ctx.payload.writes) {
    auto *table = ctx.tables.GetTable(write.table_name);
    if (!table) return ctx.Abort("write_table_missing");
    if (!table->GetPaxTable()) return ctx.Abort("pax_schema_missing");
    if (!ctx.write_set.AddRow(table->GetPrimaryIndex(), write, ctx.abort_reason))
      return ctx.Abort(ctx.abort_reason);
  }
  for (const auto &change : ctx.payload.secondary_index_ops) {
    auto *table = ctx.tables.GetTable(change.table_name);
    if (!table) return ctx.Abort("si_table_missing");
    if (!table->GetPaxTable()) return ctx.Abort("pax_schema_missing");
    auto *index = table->GetSecondaryIndex(change.index_name);
    if (!index) return ctx.Abort("si_index_missing");
    if (!ctx.write_set.AddIndex(index->tree, index->constraint, change,
                               ctx.abort_reason))
      return ctx.Abort(ctx.abort_reason);
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

std::string FormatReadAbortReason(const char *reason,
                                 const ExternalReadEntry &read) {
  std::string out(reason);
  out += ':';
  out += read.table_name;
  out += ":key=";
  out += KeyHex(read.key);
  return out;
}

// Check that every record a point read observed still shows the same word.
bool ValidateReads(CommitCtx &ctx) {
  for (const auto &read : ctx.read_set) {
    auto *table = ctx.tables.GetTable(read.table_name);
    if (table == nullptr) {
      if (read.tid != 0) return ctx.Abort("read_table_missing");
      continue;
    }
    const Tidword observed(read.tid);
    DataItem *item = table->GetPrimaryIndex().Get(read.key);
    // A key with no record reads as the absent word.
    const Tidword current =
        item ? item->transaction_id.load() : Tidword::Absent();
    // Only our own lock may differ from the word the read observed.
    Tidword expected = observed;
    if (item != nullptr && ctx.write_set.OwnsLock(item)) expected.lock = true;
    if (current != expected) {
      return ctx.Abort(
          FormatReadAbortReason("exact_read_tid_moved", read));
    }
    ctx.max_read_tid = std::max(ctx.max_read_tid, observed);
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
    const Tidword tid = item.transaction_id.load();
    if (tid.lock && !ctx.write_set.OwnsLock(&item)) {
      aborted = true;
      return true;
    }
    // Include tombstones too: their delete TIDs determine the state we saw.
    ctx.max_read_tid = std::max(ctx.max_read_tid, tid);
    if (!tid.absent) {
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
    const Tidword tid = item->transaction_id.load();
    if (tid.lock && !ctx.write_set.OwnsLock(item)) {
      aborted = true;
      return true;
    }
    ctx.max_read_tid = std::max(ctx.max_read_tid, tid);
    if (!tid.absent) {
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

  auto collect_secondary_key = [&](std::string_view key, DataItem &) {
    // Find the current entry; the item supplied by the scan may be stale.
    const std::string secondary_key(key);
    DataItem *item = index->tree.Get(key);
    if (item == nullptr) return false;

    const Tidword tid = item->transaction_id.load();
    if (tid.lock && !ctx.write_set.OwnsLock(item)) {
      aborted = true;
      return true;
    }

    // The key list is a separate load; abort if the word moved around it.
    auto primary_keys = std::atomic_load(&item->primary_keys_);
    if (item->transaction_id.load() != tid) {
      aborted = true;
      return true;
    }
    ctx.max_read_tid = std::max(ctx.max_read_tid, tid);

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
      return ctx.Abort(range.index_name.empty()
                                 ? "primary_range_result_changed"
                                 : "secondary_range_result_changed");
    }
  }
  return true;
}

// Pick the TID this transaction publishes on every record it touches.
bool GenerateCommitTid(CommitCtx &ctx, const Tidword &last_commit_tid) {
  // Silo §4.2: exceed the read/write TIDs and the worker's previous TID,
  // using the epoch read after locking.
  const Tidword max_tid =
      std::max({ctx.max_read_tid, ctx.max_write_tid, last_commit_tid});
  // Recovery must not retain our commit while dropping the later state it read.
  if (max_tid.epoch > ctx.commit_epoch) {
    return ctx.Abort("commit_epoch_stale");
  }
  // A newer epoch already orders after the observed TIDs; its tid starts at 0.
  Tidword tid;
  tid.epoch = ctx.commit_epoch;
  if (max_tid.epoch == ctx.commit_epoch) {
    if (max_tid.tid >= kMaxTid) {
      return ctx.Abort("commit_tid_exhausted");
    }
    tid.tid = max_tid.tid + 1;
  }
  tid.latest = true;
  ctx.commit_tid = tid;
  return true;
}

// Queue the log and report whether this commit must wait for durability.
bool EnqueueLogEntries(wal::Logger &logger, wal::LogEntries &log_entries,
                      EpochNumber commit_epoch, CommitDurability durability) {
  if (log_entries.empty()) return false;
  return logger.Enqueue(log_entries, commit_epoch) &&
         durability == CommitDurability::kSync;
}

}  // namespace

bool Commit(TableDictionary &tables, std::shared_mutex &schema_mutex,
            epoch::Framework &epoch_framework, index::Reaper &reaper,
            wal::Logger &logger, const CommitPayload &payload,
            Tidword &last_commit_tid, CommitDurability durability,
            std::string &abort_reason) {
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

  if (!ResolveWrites(ctx, schema_mutex)) return false;

  // Test hook outside the schema lock so concurrent DDL can proceed.
  if (ctx.has_insert) {
    HELIOS_DEBUG_SYNC("silo_commit.after_index_claim");
  }

  // Phase 1: lock the write set, then read the global epoch.
  for (auto &[item, entry] : ctx.write_set) {
    Tidword observed_tid;
    if (!entry.Lock(*item, observed_tid, abort_reason))
      return ctx.Abort(abort_reason);
    ctx.max_write_tid = std::max(ctx.max_write_tid, observed_tid);
  }

  // Sample the epoch after lock waits so this commit cannot get an older epoch
  // than the writers it waited for. Masstree's RCU protection stays active.
  epoch_framework.Leave();
  ctx.commit_epoch = epoch_framework.Join();

  // Phase 2: validate read observations and assign the commit TID.
  if (!ValidateReads(ctx)) return false;
  if (!ValidateRanges(ctx)) return false;
  if (!GenerateCommitTid(ctx, last_commit_tid)) return false;

  // Storage preparation: enforce INSERT/UNIQUE and build final SI lists.
  // These checks run under write locks, before any stored value is changed.
  for (auto &[item, entry] : ctx.write_set) {
    if (!entry.PrepareUpdate(*item, abort_reason)) return ctx.Abort(abort_reason);
  }

  // Reserve all destinations before changing the first stored value.
  for (auto &[item, entry] : ctx.write_set) {
    if (!entry.AllocateSlot(*item, abort_reason)) return ctx.Abort(abort_reason);
  }

  // Phase 3: install each record, copy its log, then publish and unlock it.
  wal::LogEntries log_entries;
  log_entries.reserve(ctx.write_set.size());
  last_commit_tid = ctx.commit_tid;
  bool row_applied = false;
  for (auto &[item, entry] : ctx.write_set) {
    // Tag this install's before-image and restore the tag on leaving the loop body.
    pax::ScopedCommitEpoch scope(ctx.commit_epoch);
    if (entry.IsRow() && row_applied)
      HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
    entry.Apply(*item);
    // Once unlocked, another writer may replace this record immediately.
    log_entries.emplace_back(entry.BuildLog(*item, ctx.commit_tid));
    entry.Publish(*item, ctx.commit_tid, reaper);
    row_applied = row_applied || entry.IsRow();
  }
  const bool awaits_durability =
      EnqueueLogEntries(logger, log_entries, ctx.commit_epoch, durability);

  HELIOS_DEBUG_SYNC("silo_commit.before_offline");
  epoch_framework.Leave();

  logger.AwaitCommitDurability(ctx.commit_epoch, awaits_durability);
  return true;
}

}  // namespace silo
}  // namespace helios::storage
