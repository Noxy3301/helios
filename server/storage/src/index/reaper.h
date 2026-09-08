/**
 * @file server/storage/src/index/reaper.h
 * Physical removal of the slots a commit left empty, deferred until no
 * reader can still hold a pointer to them.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_REAPER_H
#define HELIOS_STORAGE_SRC_INDEX_REAPER_H

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "index/data_item.h"
#include "silo/transaction_id.h"
#include "util/epoch.h"

namespace helios::storage {
namespace index {

class PrimaryIndex;
class SecondaryIndex;

/**
 * @brief Physically erases logically deleted index slots after a grace
 * period.
 *
 * A committed delete leaves its DataItem in the index as a tombstone so the
 * slot keeps its TID continuity for validation; the physical Purge runs
 * here, at least one full epoch after the delete committed. Committers
 * Enqueue candidates while still holding their commit lock, and the epoch
 * hook drives Reap: enqueue now, execute on epoch advance. The shape
 * mirrors the garbage collection subsystem of the Silo reference
 * implementation (its reaper queue and last-reaped-epoch bookkeeping).
 */
class Reaper {
 public:
  /**
   * @brief Registers one logically deleted slot for a later physical purge.
   *
   * The committer passes the already resolved owning index, exactly one of
   * primary_index / secondary_index.
   */
  void Enqueue(PrimaryIndex *primary_index, SecondaryIndex *secondary_index,
               std::string_view key, DataItem *item,
               TransactionId delete_commit_tid);

  /**
   * @brief Purges every tombstone whose delete epoch lies more than one full
   * epoch behind `published_epoch`.
   *
   * Candidates whose slot is locked are requeued; candidates whose slot was
   * reused or republished are dropped. Runs on the epoch-framework thread.
   */
  void Reap(EpochNumber published_epoch);

 private:
  /**
   * @brief One logically deleted slot awaiting its physical purge.
   *
   * Exactly one of primary_index / secondary_index is set, and which one
   * says where the slot lives. `item` is the slot pointer observed at
   * enqueue time and serves as an identity check at reap time.
   * `delete_commit_tid` is the TID the deleting commit published on the
   * slot; it acts both as the grace-period clock and as evidence that the
   * slot still holds the deleted version.
   */
  struct Tombstone {
    PrimaryIndex *primary_index = nullptr;
    SecondaryIndex *secondary_index = nullptr;
    std::string key;
    DataItem *item = nullptr;
    TransactionId delete_commit_tid;
  };

  /**
   * @brief Re-resolves the tombstone's key in its owning index.
   *
   * Returns the DataItem currently installed under the key, or nullptr.
   * Reap compares the result with `tombstone.item`: a mismatch means the
   * slot was already purged and re-created, so the tombstone is stale.
   */
  DataItem *Get(const Tombstone &tombstone);

  /**
   * @brief Physically erases the slot through the owning index's Purge.
   *
   * `retired_tid` (delete TID + 2, lock bit clear) is stamped on the erased
   * slot so an in-place reuse continues the slot's TID sequence instead of
   * restarting below the delete TID.
   */
  bool Purge(const Tombstone &tombstone, TransactionId retired_tid);

  // Guards the queue: Enqueue runs on committers, Reap on the epoch thread.
  std::mutex mutex_;
  std::vector<Tombstone> tombstones_;
};

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_REAPER_H
