/**
 * @file server/storage/src/index/reaper.cc
 * The deferred purge queue: candidates wait for an epoch no reader can be
 * in before their slots are erased.
 */

#include "index/reaper.h"

#include <iterator>
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
  tombstones_.emplace_back(std::move(tombstone));
}

void Reaper::Reap(EpochNumber published_epoch) {
  // Keep deleted slots until transactions in the delete epoch have left.
  std::vector<Tombstone> ready;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    std::vector<Tombstone> pending;
    pending.reserve(tombstones_.size());
    for (auto &tombstone : tombstones_) {
      const EpochNumber delete_epoch = tombstone.delete_commit_tid.epoch;
      const bool one_full_epoch_elapsed =
          published_epoch > delete_epoch && published_epoch - delete_epoch > 1;
      if (one_full_epoch_elapsed) {
        ready.emplace_back(std::move(tombstone));
      } else {
        pending.emplace_back(std::move(tombstone));
      }
    }
    tombstones_.swap(pending);
  }

  // Nothing is due this epoch.
  if (ready.empty()) return;

  // Lock, re-verify and purge each tombstone that is due.
  std::vector<Tombstone> requeue;
  requeue.reserve(ready.size());

  for (auto &tombstone : ready) {
    DataItem *item = tombstone.index->Get(tombstone.key);
    // A different item under the key means the tombstone was already replaced.
    if (item != tombstone.item) continue;

    // Lock only if the delete TID is still current.
    Tidword expected = tombstone.delete_commit_tid;
    Tidword locked = expected;
    locked.lock = true;
    if (!item->transaction_id.compare_exchange_strong(expected, locked)) {
      if (expected.lock) requeue.emplace_back(std::move(tombstone));
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

  {
    std::lock_guard<std::mutex> lk(mutex_);
    tombstones_.insert(tombstones_.end(),
                       std::make_move_iterator(requeue.begin()),
                       std::make_move_iterator(requeue.end()));
  }

  // Get and Purge enrolled this thread in masstree's RCU epoch; release it
  // before returning to the epoch hook.
  MasstreeReleaseThreadEpoch();
}

}  // namespace index
}  // namespace helios::storage
