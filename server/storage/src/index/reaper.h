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

class MasstreeIndex;

/**
 * @brief Physically erases logically deleted index slots after a grace
 * period.
 *
 * A committed delete leaves its DataItem in the index as a tombstone so the
 * slot keeps its TID continuity for validation; the physical Purge runs
 * here. Committers Enqueue a tombstone after publishing the unlocked delete
 * tid. Reap runs on the epoch-framework thread and purges a tombstone only
 * once published_epoch is more than one epoch past that tid's epoch.
 */
class Reaper {
 public:
  /**
   * @brief Registers one logically deleted slot for a later physical purge.
   *
   * The committer passes the tree owning the already resolved slot.
   */
  void Enqueue(MasstreeIndex &index, std::string_view key, DataItem &item,
               TransactionId delete_commit_tid);

  /**
   * @brief Purges every tombstone whose delete epoch lies more than one full
   * epoch behind `published_epoch`.
   *
   * Tombstones whose slot is locked are requeued. A tombstone is dropped when
   * the key resolves to a different item, the tid no longer matches the
   * delete, the slot is live again, or Purge fails. Runs on the
   * epoch-framework thread.
   */
  void Reap(EpochNumber published_epoch);

 private:
  /**
   * @brief One logically deleted slot awaiting its physical purge.
   *
   * index is the tree owning the slot. item is the slot pointer observed at
   * enqueue time and serves as an identity check at reap time.
   * `delete_commit_tid` is the TID the deleting commit published on the
   * slot; it acts both as the grace-period clock and as evidence that the
   * slot still holds the deleted version.
   */
  struct Tombstone {
    MasstreeIndex *index = nullptr;
    std::string key;
    DataItem *item = nullptr;
    TransactionId delete_commit_tid;
  };

  // Guards the queue: Enqueue runs on committers, Reap on the epoch thread.
  std::mutex mutex_;
  std::vector<Tombstone> tombstones_;
};

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_REAPER_H
