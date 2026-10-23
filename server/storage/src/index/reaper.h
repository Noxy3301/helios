/**
 * @file server/storage/src/index/reaper.h
 * Physical removal of deleted records after an epoch grace period.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_REAPER_H
#define HELIOS_STORAGE_SRC_INDEX_REAPER_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "index/index_record.h"
#include "silo/tidword.h"
#include "util/epoch.h"

namespace helios::storage {
namespace index {

class MasstreeIndex;

/**
 * @brief Holds deleted records by epoch until they can be physically removed.
 * Writers may reuse a queued record, so purging must still check its delete TID.
 */
class Reaper {
 public:
  /**
   * @brief Registers one logically deleted record for a later physical purge.
   *
   * The committer passes the tree owning the already resolved record.
   */
  void Enqueue(MasstreeIndex &index, std::string_view key, IndexRecord &item,
               Tidword delete_commit_tid);

  /**
   * @brief Purges candidates deleted at or before reclamation_epoch.
   * @details reclamation_epoch is the latest delete epoch whose grace has passed.
   * Unlinking is separate from freeing memory, which remains deferred by RCU.
   * Locked records remain queued for the next call; newer epochs are untouched.
   * Called by the epoch thread, never concurrently with another Purge.
   */
  void Purge(EpochNumber reclamation_epoch);

  /**
   * @brief Drops every queued record of `index`, which is about to be freed.
   *
   * @details Waits for a Purge in progress, which may hold such records. The
   * caller guarantees that no commit enqueues into `index` any more.
   */
  void Forget(const MasstreeIndex &index);

 private:
  /**
   * @brief One logically deleted record awaiting its physical purge.
   *
   * index is the tree owning the record. item is the record the key resolved
   * to at enqueue time and serves as an identity check at purge time.
   * `delete_commit_tid` identifies the deletion; a newer version cancels
   * this candidate.
   */
  struct Tombstone {
    MasstreeIndex *index = nullptr;
    std::string key;
    IndexRecord *item = nullptr;
    Tidword delete_commit_tid;
  };

  // Guards the queue shared by committers and the epoch thread.
  std::mutex mutex_;
  // Held through each Purge, so Forget never meets a batch Purge detached.
  std::mutex purge_mutex_;
  std::map<EpochNumber, std::vector<Tombstone>> tombstones_;
};

}  // namespace index
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_REAPER_H
