/**
 * @file server/storage/src/index/reaper.cc
 * The deferred purge queue: candidates wait for an epoch no reader can be
 * in before their slots are erased.
 */

#include "index/reaper.h"

#include <iterator>
#include <utility>

#include "index/masstree_index.h"
#include "index/primary_index.h"
#include "index/secondary_index.h"
#include "silo/stable_read.h"

namespace helios::storage {
namespace index {

void Reaper::Enqueue(PrimaryIndex *primary_index,
                     SecondaryIndex *secondary_index, std::string_view key,
                     DataItem *item, TransactionId delete_commit_tid) {
  Tombstone tombstone;
  tombstone.primary_index = primary_index;
  tombstone.secondary_index = secondary_index;
  tombstone.key = std::string(key);
  tombstone.item = item;
  tombstone.delete_commit_tid = delete_commit_tid;

  std::lock_guard<std::mutex> lk(mutex_);
  tombstones_.emplace_back(std::move(tombstone));
}

DataItem *Reaper::Get(const Tombstone &tombstone) {
  if (tombstone.primary_index != nullptr) {
    return tombstone.primary_index->Get(tombstone.key);
  }
  return tombstone.secondary_index->Get(tombstone.key);
}

bool Reaper::Purge(const Tombstone &tombstone, TransactionId retired_tid) {
  if (tombstone.primary_index != nullptr) {
    return tombstone.primary_index->Purge(tombstone.key, tombstone.item,
                                          retired_tid);
  }
  return tombstone.secondary_index->Purge(tombstone.key, tombstone.item,
                                          retired_tid);
}

void Reaper::Reap(EpochNumber published_epoch) {
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

  if (ready.empty()) return;

  std::vector<Tombstone> requeue;
  requeue.reserve(ready.size());

  for (auto &tombstone : ready) {
    DataItem *item = Get(tombstone);
    if (item != tombstone.item) continue;

    TransactionId observed = item->transaction_id.load();
    if (observed.tid & silo::kLockBit) {
      requeue.emplace_back(std::move(tombstone));
      continue;
    }
    if (observed != tombstone.delete_commit_tid) continue;

    TransactionId locked = observed;
    locked.tid |= silo::kLockBit;
    if (!item->transaction_id.compare_exchange_strong(observed, locked)) {
      if (observed.tid & silo::kLockBit)
        requeue.emplace_back(std::move(tombstone));
      continue;
    }

    auto unlock = [&]() {
      item->transaction_id.store(tombstone.delete_commit_tid);
    };

    const bool live =
        tombstone.primary_index != nullptr ? item->HasRow() : item->IsLive();
    if (live) {
      unlock();
      continue;
    }
    if (Get(tombstone) != item) {
      unlock();
      continue;
    }
    if (item->transaction_id.load() != locked) {
      unlock();
      continue;
    }

    TransactionId retired = tombstone.delete_commit_tid;
    retired.tid = (retired.tid + 2u) & ~silo::kLockBit;
    if (!Purge(tombstone, retired)) unlock();
  }

  {
    std::lock_guard<std::mutex> lk(mutex_);
    tombstones_.insert(tombstones_.end(),
                       std::make_move_iterator(requeue.begin()),
                       std::make_move_iterator(requeue.end()));
  }

  MasstreeReleaseThreadEpoch();
}

}  // namespace index
}  // namespace helios::storage
