/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

// Modified for Helios.

/**
 * @file server/storage/src/util/epoch_framework.h
 * The epoch counter and the thread registry behind it: what advances an
 * epoch, and what a thread does to enter and leave one.
 */

#ifndef HELIOS_STORAGE_SRC_UTIL_EPOCH_FRAMEWORK_H
#define HELIOS_STORAGE_SRC_UTIL_EPOCH_FRAMEWORK_H

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "util/epoch.h"
#include "util/thread_key_storage.h"

namespace helios::storage {
namespace epoch {

/**
 * @brief The monotonically increasing number every thread shares, and the
 *        registry of the threads that participate in it.
 *
 * @details An object stamped with an epoch at or above #GetGlobalEpoch may
 * still be reachable by another thread, so it cannot be freed. Reclamation
 * waits until the epoch has passed.
 * @see [Silo]: https://dl.acm.org/doi/10.1145/2517349.2522713
 * @see [FASTER]:
 * https://www.microsoft.com/en-us/research/uploads/prod/2018/03/faster-sigmod18.pdf
 */
class Framework {
 public:
  // Sentinel for a thread that participates in no epoch; not a reachable
  // epoch value.
  static constexpr EpochNumber kThreadOffline = UINT32_MAX;

  Framework(size_t epoch_duration_ms = 40)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        epoch_thread_(&Framework::EpochThreadJob, this, epoch_duration_ms) {}
  /**
   * @brief Starts the epoch thread, parked until Start().
   *
   * @param epoch_duration_ms Epoch thread poll period.
   * @param hook Invoked on the epoch thread after each successful advance,
   * with the new global epoch. May be empty.
   */
  Framework(size_t epoch_duration_ms, std::function<void(EpochNumber)> &&hook)
      : start_(false),
        stop_(false),
        global_epoch_(1),
        epoch_hook_(std::move(hook)),
        epoch_thread_(&Framework::EpochThreadJob, this, epoch_duration_ms) {}

  ~Framework() { Stop(); }

  void SetGlobalEpoch(const EpochNumber epoch) { global_epoch_.store(epoch); }

  EpochNumber GetGlobalEpoch() const { return global_epoch_.load(); }

  /**
   * @brief Returns the epoch this thread participates in, or #kThreadOffline
   *        for none.
   *
   * @details The slot stays private so that every access to it shares one
   * sequentially consistent order with #GetSmallestEpoch's scan.
   */
  EpochNumber ThreadEpoch() {
    std::atomic<EpochNumber> *my_epoch = ThreadSlot();
    return my_epoch->load(std::memory_order_seq_cst);
  }

  /**
   * @brief Overwrites this thread's already-online slot.
   *
   * @details Valid only before #Start(), where the epoch thread has not begun
   * scanning slots.
   */
  void SetThreadEpoch(const EpochNumber epoch) {
    assert(!start_.load(std::memory_order_seq_cst));
    assert(epoch != kThreadOffline);
    std::atomic<EpochNumber> *my_epoch = ThreadSlot();
    assert(my_epoch->load(std::memory_order_seq_cst) != kThreadOffline);
    my_epoch->store(epoch, std::memory_order_seq_cst);
  }

  /**
   * @brief Joins the current epoch and returns it.
   *
   * @details The caller must not enqueue log records, and must not take its
   * commit epoch, before this returns.
   *
   * Once this returns worker epoch `e_w`, global epoch `E` cannot reach
   * `e_w + 2` while the slot still reads `e_w`: only one epoch thread scan that
   * missed the publication can be outstanding, and the next one reads `e_w`.
   * This assumes `e_w` stays clear of wraparound: the epoch thread stops at
   * #kEpochHighWater rather than wrapping, and a counter seeded at or past the
   * mark before #Start() fail-stops on the epoch thread's first eligible
   * advance. A thread still inside the loop carries no such guarantee, which is
   * why the contract above exists.
   */
  EpochNumber Join() {
    std::atomic<EpochNumber> *my_epoch = ThreadSlot();
    assert(my_epoch->load(std::memory_order_seq_cst) == kThreadOffline);

    EpochNumber published = global_epoch_.load(std::memory_order_seq_cst);
    for (;;) {
      my_epoch->store(published, std::memory_order_seq_cst);
      const EpochNumber reloaded =
          global_epoch_.load(std::memory_order_seq_cst);
      if (reloaded == published) return published;
      // A single store does not suffice: two consecutive epoch thread scans can
      // read the slot before the store lands, and the advances they gate
      // leave this thread online two epochs behind. Republish until the
      // reload agrees.
      published = reloaded;
    }
  }

  void Leave() {
    std::atomic<EpochNumber> *my_epoch = ThreadSlot();
    assert(my_epoch->load(std::memory_order_seq_cst) != kThreadOffline);
    my_epoch->store(kThreadOffline, std::memory_order_seq_cst);
  }

  /**
   * @brief Waits for the global epoch to advance twice, or for Stop().
   *
   * @details Two advances is what covers every thread: one for the threads
   * that were in the epoch this call observed, and one for the threads that
   * joined the next.
   */
  EpochNumber Sync() {
    assert(ThreadEpoch() == kThreadOffline);
    size_t reload_count = 0;
    for (;;) {
      auto current_epoch = global_epoch_.load();
      {
        std::unique_lock<std::mutex> lk(epoch_mutex_);
        epoch_cv_.wait(lk, [&] {
          return stop_.load() || (global_epoch_.load() != current_epoch);
        });
      }
      auto reload_epoch = global_epoch_.load();
      if (stop_.load()) return reload_epoch;
      reload_count++;

      if (reload_count == 2) return reload_epoch;
    }
  }

  // Margin below the uint32 wrap point. Forced advances and read view fences
  // refuse beyond it, and a read view's epoch lifetime is bounded well under
  // the margin; every read-view epoch comparison therefore stays inside one
  // wrap-free window where plain unsigned ordering is exact. The timer-driven
  // advance stops the process at the mark rather than wrapping: past the wrap,
  // the epoch no longer orders against the durable epoch, so a commit
  // could be reported durable against a comparison that has lost its
  // meaning.
  static constexpr EpochNumber kEpochHighWater = UINT32_MAX - (1u << 20);

  // Asks the epoch thread to run its advance check at once instead of at the
  // next tick. The advance condition itself is unchanged. No-op at or
  // above the high-water mark.
  void RequestEpochAdvance() {
    if (global_epoch_.load() >= kEpochHighWater) return;
    {
      std::lock_guard<std::mutex> lk(epoch_mutex_);
      advance_requested_.store(true);
    }
    epoch_thread_cv_.notify_one();
  }

  // Blocks the kThreadOffline caller until it observes `global_epoch >= target`.
  // Already-reached targets succeed even after Stop() or above the
  // high-water mark; otherwise false on timeout, Stop(), or a target
  // beyond the mark.
  bool WaitEpoch(EpochNumber target, std::chrono::milliseconds timeout) {
    return WaitEpochUntil(target, std::chrono::steady_clock::now() + timeout);
  }

  // As above, against a deadline the caller already holds, so a wait that is
  // one step of a longer bounded operation cannot restart the clock.
  bool WaitEpochUntil(EpochNumber target,
                      std::chrono::steady_clock::time_point deadline) {
    assert(ThreadEpoch() == kThreadOffline);
    if (global_epoch_.load() >= target) return true;
    if (target > kEpochHighWater) return false;
    std::unique_lock<std::mutex> lk(epoch_mutex_);
    for (;;) {
      if (global_epoch_.load() >= target) return true;
      if (stop_.load()) return false;
      advance_requested_.store(true);
      epoch_thread_cv_.notify_one();
      if (epoch_cv_.wait_until(lk, deadline) == std::cv_status::timeout) {
        return global_epoch_.load() >= target;
      }
    }
  }

  void Start() {
    {
      std::lock_guard<std::mutex> lk(epoch_mutex_);
      start_.store(true);
    }
    epoch_cv_.notify_all();
  }
  void Stop() {
    {
      std::lock_guard<std::mutex> lk(epoch_mutex_);
      stop_.store(true);
    }
    epoch_cv_.notify_all();
    epoch_thread_cv_.notify_all();
    if (epoch_thread_.joinable()) epoch_thread_.join();
  }

 private:
  /**
   * @brief Returns this thread's epoch slot, created offline on first use.
   */
  std::atomic<EpochNumber> *ThreadSlot() {
    return thread_epochs_.Get<EpochNumber>([]() { return kThreadOffline; });
  }

  /**
   * @brief Returns the lowest epoch any online thread holds, or
   *        kThreadOffline.
   */
  EpochNumber GetSmallestEpoch() {
    EpochNumber min_epoch = kThreadOffline;
    thread_epochs_.ForEach([&](const std::atomic<EpochNumber> *local_epoch) {
      const EpochNumber worker_epoch =
          local_epoch->load(std::memory_order_seq_cst);
      if (0 < worker_epoch && worker_epoch < min_epoch) {
        min_epoch = worker_epoch;
      }
    });

    return min_epoch;
  }

  void EpochThreadJob(size_t epoch_duration_ms) {
    const auto epoch_duration =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::milliseconds(epoch_duration_ms));
    // Block until Start(), or until a framework that was never started is
    // destroyed.
    {
      std::unique_lock<std::mutex> lk(epoch_mutex_);
      epoch_cv_.wait(lk, [&] { return start_.load() || stop_.load(); });
      if (!start_.load()) return;
    }

    for (;;) {
      bool forced_wake = false;
      if (stop_.load()) {
        // Post-stop the predicate below stays true; plain sleep keeps the
        // cadence while draining still-online threads
        std::this_thread::sleep_for(epoch_duration);
      } else {
        // Forced requests wake the epoch thread early; the advance condition
        // below still gates
        std::unique_lock<std::mutex> lk(epoch_mutex_);
        forced_wake = epoch_thread_cv_.wait_for(lk, epoch_duration, [&] {
          return advance_requested_.load() || stop_.load();
        });
        advance_requested_.store(false);
      }
      // Sample the smallest online epoch and the current global one.
      EpochNumber min_epoch = GetSmallestEpoch();
      EpochNumber old_epoch = global_epoch_;
      if (forced_wake && !stop_.load() && old_epoch >= kEpochHighWater) {
        // A racing forced request must not advance past the high-water
        // margin; timer cadence continues
        continue;
      }
      if (min_epoch == kThreadOffline || min_epoch == old_epoch) {
        if (old_epoch >= kEpochHighWater) {
          // Stopping here is the conservative end, including during the
          // post-Stop() drain: an epoch that wraps stops ordering against
          // the durable epoch, and refusing to advance instead would
          // stall every commit that waits for its epoch to close.
          fprintf(stderr,
                  "Helios: the global epoch reached the high-water mark "
                  "%u; stopping before the counter can run toward the wrap\n",
                  kEpochHighWater);
          std::abort();
        }
        {
          // fetch_add is atomic, but epoch_mutex_ is held here so that
          // Sync()'s cv.wait cannot miss the notify_all that follows.
          std::lock_guard<std::mutex> lk(epoch_mutex_);
          global_epoch_.fetch_add(1);
        }
        const EpochNumber global_epoch = global_epoch_.load();
        epoch_cv_.notify_all();
        if (epoch_hook_) epoch_hook_(global_epoch);
      }
      if (stop_.load() && min_epoch == kThreadOffline) break;
    }
  }

  std::atomic<bool> start_;
  std::atomic<bool> stop_;
  std::atomic<bool> advance_requested_{false};
  // `E`: the global epoch shared by all participants.
  std::atomic<EpochNumber> global_epoch_;
  std::mutex epoch_mutex_;
  std::condition_variable epoch_cv_;
  std::condition_variable epoch_thread_cv_;
  const std::function<void(EpochNumber)> epoch_hook_;
  // Started by the constructor but parked on epoch_cv_ until Start(), so it
  // cannot reach thread_epochs_ before that member is constructed.
  std::thread epoch_thread_;
  // `e_w`: the epoch published by each participating worker.
  ThreadKeyStorage<std::atomic<EpochNumber>> thread_epochs_;
};

}  // namespace epoch
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_UTIL_EPOCH_FRAMEWORK_H
