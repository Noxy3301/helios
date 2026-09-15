/**
 * @file server/storage/src/index/reaper.cc
 * The deferred purge queue: candidates wait for an epoch no reader can be
 * in before their slots are erased.
 */

#include "index/reaper.h"

#include <utility>

#include "index/masstree_index.h"

namespace helios::storage {
namespace index {

void Reaper::Enqueue(MasstreeIndex &index, std::string_view key, DataItem &item,
                     Tidword delete_commit_tid) {
  Tombstone tombstone;
  tombstone.index = &index;
  tombstone.key = std::string(key);
  tombstone.item = &item;
  tombstone.delete_commit_tid = delete_commit_tid;

  std::lock_guard<std::mutex> lk(mutex_);
  tombstones_[delete_commit_tid.epoch].emplace_back(std::move(tombstone));
}

void Reaper::Purge(EpochNumber reclamation_epoch) {
  std::vector<std::vector<Tombstone>> ready;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    const auto end = tombstones_.upper_bound(reclamation_epoch);
    // Detach only eligible epochs; committers can enqueue while we purge.
    for (auto it = tombstones_.begin(); it != end; ++it) {
      ready.emplace_back(std::move(it->second));
    }
    tombstones_.erase(tombstones_.begin(), end);
  }

  // Nothing is due this epoch.
  if (ready.empty()) return;

  // A writer may abort and leave the deletion intact, so retry locked records.
  std::vector<Tombstone> locked_tombstones;

  for (auto &entries : ready) {
    for (auto &tombstone : entries) {
      DataItem *item = tombstone.index->Get(tombstone.key);
      // A different item under the key means the tombstone was already replaced.
      if (item != tombstone.item) continue;

      // Check the queued deletion and exclude writers in one atomic step.
      // A newer TID means a writer has already reused this record.
      Tidword expected = tombstone.delete_commit_tid;
      Tidword locked = expected;
      locked.lock = true;
      if (!item->transaction_id.compare_exchange_strong(expected, locked)) {
        if (expected.lock) locked_tombstones.emplace_back(std::move(tombstone));
        continue;
      }

      // Mark removal so readers holding the old pointer see the change.
      Tidword retired = tombstone.delete_commit_tid;
      retired.latest = false;
      if (!tombstone.index->Purge(tombstone.key, *item, retired)) {
        // A failed purge still leaves this item locked; release it.
        item->transaction_id.store(tombstone.delete_commit_tid);
      }
    }
  }

  {
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto &tombstone : locked_tombstones) {
      tombstones_[tombstone.delete_commit_tid.epoch].emplace_back(
          std::move(tombstone));
    }
  }

  // Get and Purge enrolled this thread in masstree's RCU epoch; release it
  // before returning to the epoch hook.
  MasstreeReleaseThreadEpoch();
}

}  // namespace index
}  // namespace helios::storage
