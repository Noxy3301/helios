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

/**
 * @file server/storage/src/wal/logger.h
 * The write-ahead log and the durability frontier a synchronous commit
 * waits on.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_LOGGER_H
#define HELIOS_STORAGE_SRC_WAL_LOGGER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "index/data_buffer.h"
#include "storage/config.h"
#include "util/epoch.h"
#include "wal/log_entry.h"
#include "wal/log_record.h"
#include "wal/wal.h"

namespace helios::storage {
namespace wal {

class ThreadLocalLogger;

/**
 * @brief Owns the write-ahead log and the durability frontier.
 *
 * @details The frontier is the highest epoch whose records are on the
 * device. It only ever advances, and only after the fdatasync that made
 * those records durable returned successfully (an epoch with no records
 * advances it without one); a commit that waits on its own epoch therefore
 * learns the truth rather than an intention.
 */
class Logger {
 public:
  using Deadline = std::chrono::steady_clock::time_point;

  enum class WaitResult {
    kDurable,   // the frontier reached the requested epoch
    kTimedOut,  // the deadline passed first
    kStopped,   // the logger shut down before reaching it
    kFailed,    // the log could not be written
  };

  enum class RecoveryStatus { kOk, kFailed };

  struct RecoveryResult {
    RecoveryStatus status{RecoveryStatus::kOk};
    // The epoch of the last intact frame, which after a successful scan is
    // also the durable epoch.
    EpochNumber frontier{0};
    WriteSet recovery_set;
  };

  explicit Logger(const Config &config, WalIo io = WalIo::Posix());
  ~Logger();

  /**
   * @brief Buffers one committed transaction's write set.
   * @return Whether anything was buffered: a transaction whose write set
   * produces no write has nothing to make durable, and the commit
   * path must not wait for it.
   */
  bool Enqueue(const WriteSet &ws, EpochNumber epoch);

  /**
   * @brief Scans the log and, when the scan allows it, repairs a torn tail.
   * @details On success it sets the durable epoch from the last intact frame
   * and returns that epoch together with the folded write set, the
   * checkpoint first when one was loaded, then the log. On kFailed,
   * frontier is 0 and recovery_set is empty.
   * @note Runs before the flusher starts and before the database accepts
   * work.
   */
  RecoveryResult Recover();

  /**
   * @brief Starts the flusher. Must follow Recover() and precede the first
   * tick.
   */
  void StartFlusher();

  /**
   * @brief Publishes a new closed epoch.
   * @note Called from the epoch writer thread; only records at or below
   * `closed` may be written, because a later epoch can still gain
   * participants.
   */
  void ScheduleFlush(EpochNumber closed);

  EpochNumber GetDurableEpoch() const {
    return durable_epoch_.load(std::memory_order_seq_cst);
  }

  /**
   * @brief The epoch of the last frame actually written to the log. Safe to
   * call from a thread other than the flusher's; see Wal::Frontier for what
   * makes that safe and why it is not the same question as GetDurableEpoch.
   */
  EpochNumber GetWalFrontier() const;

  /**
   * @brief Blocks until the frontier reaches `commit_epoch`.
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
   * @brief True while every closed epoch handed to the flusher is durable,
   * and unconditionally after a write failure: nothing further will be
   * written. Records buffered for epochs that have not closed yet do not
   * count.
   */
  bool IsQuiescent();

  /**
   * @brief Flushes everything already closed, then stops and joins the
   * flusher. After a write failure nothing more is flushed and the join is
   * immediate.
   */
  void StopFlusher();

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
  // Reports a scan that failed and answers Recover with kFailed.
  RecoveryResult FailRecovery(const WalScanResult &scan);

  void PublishDurable(EpochNumber frontier);
  void PublishFailure(int error_number);
  void PublishStopped();

  const std::string work_dir_;
  // Whether a published checkpoint is loaded at all. The log itself is always
  // scanned and its interrupted tail truncated.
  const bool loads_checkpoint_;
  std::atomic<EpochNumber> durable_epoch_{0};

  enum class State { kRunning, kStopped, kFailed };
  std::mutex durability_mutex_;
  std::condition_variable durability_cv_;
  State state_{State::kRunning};
  bool process_fail_stop_{false};

  // Declared last: the backend's flusher publishes through the members above,
  // so it must be destroyed before them.
  std::unique_ptr<ThreadLocalLogger> thread_local_logger_;
};

}  // namespace wal
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_WAL_LOGGER_H
