/**
 * @file server/storage/src/silo/commit.cc
 * Run the commit protocol on prepared read and write sets.
 */

#include "silo/commit.h"

#include <algorithm>
#include <string>

#include "pax/version_store.h"
#include "silo/read_set.h"
#include "silo/write_set.h"
#include "util/debug_sync.h"
#include "util/epoch_framework.h"
#include "wal/logger.h"

namespace helios::storage {
namespace silo {

namespace {

// State shared by the steps of one commit attempt.
struct CommitCtx {
  epoch::Framework &epoch;
  const ReadSet &read_set;
  WriteSet &write_set;
  std::string &abort_reason;
  EpochNumber commit_epoch = 0;
  Tidword commit_tid{};
  // The largest word this attempt read, and the largest it found under a lock.
  Tidword max_read_tid{};
  Tidword max_write_tid{};

  // Values are still unpublished; release any locks this attempt acquired.
  bool Abort(const std::string &reason) {
    abort_reason = reason;
    for (auto &[item, entry] : write_set) entry.Unlock(*item);
    epoch.Leave();
    return false;
  }
};

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

CommitExecutor::CommitExecutor(TableDictionary &tables,
                               epoch::Framework &epoch_framework,
                               index::Reaper &reaper, wal::Logger &logger)
    : tables_(tables),
      epoch_framework_(epoch_framework),
      reaper_(reaper),
      logger_(logger) {}

bool CommitExecutor::Commit(const ReadSet &read_set, WriteSet &write_set,
                            Tidword &last_commit_tid,
                            CommitDurability durability,
                            std::string &abort_reason) const {
  CommitCtx ctx{epoch_framework_, read_set, write_set, abort_reason};

  // Phase 1: lock the write set, then read the global epoch.
  for (auto &[item, entry] : ctx.write_set) {
    Tidword observed_tid;
    if (!entry.Lock(*item, observed_tid, abort_reason))
      return ctx.Abort(abort_reason);
    ctx.max_write_tid = std::max(ctx.max_write_tid, observed_tid);
  }

  // Sample the epoch after lock waits so this commit cannot get an older epoch
  // than the writers it waited for. Masstree's RCU protection stays active.
  epoch_framework_.Leave();
  ctx.commit_epoch = epoch_framework_.Join();

  // Phase 2: validate read observations and assign the commit TID.
  const auto validation_result =
      ctx.read_set.Validate(tables_, ctx.write_set, ctx.max_read_tid);
  switch (validation_result) {
    case ReadSet::Status::kValid:
      break;
    case ReadSet::Status::kTableMissing:
      return ctx.Abort("read_table_missing");
    case ReadSet::Status::kPointChanged:
      return ctx.Abort("exact_read_tid_moved");
    case ReadSet::Status::kPrimaryRangeChanged:
      return ctx.Abort("primary_range_result_changed");
    case ReadSet::Status::kSecondaryRangeChanged:
      return ctx.Abort("secondary_range_result_changed");
  }
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
    entry.Publish(*item, ctx.commit_tid, reaper_);
    row_applied = row_applied || entry.IsRow();
  }
  const bool awaits_durability =
      EnqueueLogEntries(logger_, log_entries, ctx.commit_epoch, durability);

  HELIOS_DEBUG_SYNC("silo_commit.before_offline");
  epoch_framework_.Leave();

  logger_.AwaitCommitDurability(ctx.commit_epoch, awaits_durability);
  return true;
}

}  // namespace silo
}  // namespace helios::storage
