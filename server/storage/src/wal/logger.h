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
 * @file server/storage/src/wal/logger.h
 * Producer log buffers, their background writer and the durable epoch.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_LOGGER_H
#define HELIOS_STORAGE_SRC_WAL_LOGGER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include "lineairdb/config.h"

#include "util/epoch.h"
#include "util/thread_key_storage.h"
#include "wal/log_entry.h"
#include "wal/log_record.h"
#include "wal/wal.h"

namespace helios::storage {
namespace wal {

/**
 * @brief Owns the WAL, per-producer buffers and the worker that persists them.
 *
 * @details Each producer appends to its own buffer. The worker collects the
 * records and writes closed epochs, then publishes durable epoch `D` to
 * waiting commits. An epoch with no records advances without a file write.
 * The destructor joins the worker before destroying the WAL and buffers.
 */
class Logger {
 public:
  using Deadline = std::chrono::steady_clock::time_point;

  enum class WaitResult {
    kDurable,   // the durable epoch reached the requested one
    kTimedOut,  // the deadline passed first
    kStopped,   // the logger shut down before reaching it
    kFailed,    // the log could not be written
  };

  enum class RecoveryStatus { kOk, kFailed };

  struct RecoveryResult {
    RecoveryStatus status{RecoveryStatus::kOk};
    // Durable through the log's last frame and the checkpoint's end epoch.
    EpochNumber durable_epoch{0};
    LogEntries recovery_entries;
  };

  explicit Logger(const Config &config, WalIo io = WalIo::Posix());
  ~Logger();

  /**
   * @brief Buffers one committed transaction's log entries.
   * @return Whether anything was buffered: entries that produce no logged
   * writes have nothing to make durable, and the commit path must not wait.
   */
  bool Enqueue(const LogEntries &log_entries, EpochNumber epoch);

  /**
   * @brief Scans the log and zeroes the tail at its first invalid frame.
   * @details On success it sets the durable epoch from the log and any
   * published checkpoint, and returns it together with the folded log entries,
   * the checkpoint first when one was loaded, then the log. On kFailed,
   * durable_epoch is 0 and recovery_entries is empty.
   * @note Runs before the worker starts and before the database accepts
   * work.
   */
  RecoveryResult Recover();

  /**
   * @brief Starts the logger worker after recovery, before epoch notifications.
   */
  void Start();

  /**
   * @brief Requests WAL persistence without waiting for I/O.
   * @param max_epoch Highest epoch to persist, inclusive.
   * @note The caller must ensure no producer can add records at or below
   * max_epoch. Durable epoch `D` advances only after those records are synced.
   */
  void RequestFlush(EpochNumber max_epoch);

  EpochNumber GetDurableEpoch() const {
    return durable_epoch_.load(std::memory_order_seq_cst);
  }

  /**
   * @brief Blocks until the durable epoch reaches `commit_epoch`.
   * @details Pass Deadline::max() to wait without a timeout; shutdown and an
   * I/O failure still end the wait, as Stopped and Failed respectively. An
   * epoch that is already durable is reported as such even after a terminal
   * state.
   * @note The caller must have left its epoch first: waiting while online
   * would hold the epoch that has to close before the wait can end.
   */
  WaitResult WaitUntilDurable(EpochNumber commit_epoch, Deadline deadline);

  /**
   * @brief Returns once the transaction that committed in `commit_epoch` may
   * be acknowledged: at once when `awaits_durability` is false, and after
   * `commit_epoch` is durable when it is true.
   * @details The decision is the caller's, not this method's, so that a
   * durability switch cannot land between the caller's callback placement and
   * this wait and make the two disagree. A commit whose record cannot be made
   * durable stops the process: it has passed its serialization point, so an
   * abort would be a lie and an acknowledgement would be the lie the contract
   * exists to prevent.
   * @note The caller must have left its epoch, as WaitUntilDurable requires.
   */
  void AwaitCommitDurability(EpochNumber commit_epoch, bool awaits_durability);

  /**
   * @brief Drains closed epochs, joins the worker and wakes remaining waiters.
   * @details After a write failure, no further records are written. Calling
   * Stop again, or before Start, is safe.
   */
  void Stop();

  /**
   * @brief Makes a later log I/O failure terminate the process.
   * @details Without it a write failure only wakes waiters as Failed, and
   * under the async commit ack nobody waits: the process would keep
   * acknowledging commits that exist only in memory.
   * @note Left unarmed when a test constructs a Logger directly, so Failed
   * can be observed.
   */
  void SetFailStop();

 private:
  struct LogBuffer {
    std::mutex mutex;
    LogRecords records;
  };

  const std::string work_dir_;
  const bool loads_checkpoint_;
  Wal wal_;

  // One buffer per producer, also retained after that producer exits.
  ThreadKeyStorage<LogBuffer> buffers_;
  // Owned by the worker; records beyond the requested flush epoch stay here.
  std::map<EpochNumber, LogRecords> pending_records_;

  // Protects the flush request and the request to stop the worker.
  std::mutex work_mutex_;
  std::condition_variable work_cv_;
  EpochNumber flush_epoch_{0};
  bool stop_requested_{false};

  // Protects the result reported to commit and checkpoint waiters.
  enum class State { kRunning, kStopped, kFailed };
  // `D`: the highest epoch known to be durable.
  std::atomic<EpochNumber> durable_epoch_{0};
  std::mutex durability_mutex_;
  std::condition_variable durability_cv_;
  State state_{State::kRunning};
  bool process_fail_stop_{false};

  std::thread worker_;

  /**
   * @brief Collects producer buffers and persists records through target.
   * @note Called only by the worker, without holding work_mutex_.
   */
  WalAppendResult FlushThrough(EpochNumber target);

  /**
   * @brief Writes closed epochs until stopped or a batch fails.
   */
  void Worker();

  RecoveryResult FailRecovery(const WalScanResult &wal);
  void PublishDurable(EpochNumber durable_epoch);
  void PublishFailure(int error_number);
  void PublishStopped();
};

}  // namespace wal
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_WAL_LOGGER_H
