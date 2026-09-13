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
 * @file server/storage/src/wal/thread_local_logger.h
 * Per-thread record buffers and the one flusher thread that writes them.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_THREAD_LOCAL_LOGGER_H
#define HELIOS_STORAGE_SRC_WAL_THREAD_LOCAL_LOGGER_H

#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

#include "storage/config.h"
#include "util/epoch.h"
#include "util/thread_key_storage.h"
#include "wal/log_entry.h"
#include "wal/log_record.h"
#include "wal/wal.h"

namespace helios::storage {
namespace wal {

/**
 * @brief Buffers log records per producing thread and writes them from one
 * dedicated flusher thread.
 *
 * @details Producers never touch the file: a committing thread appends to
 * its own thread-local vector under a short lock and returns. The flusher
 * swaps those vectors, buckets the records by epoch, and writes one group
 * per fdatasync.
 *
 * @note The flusher owns a thread of its own so that the fdatasync it blocks
 * on does not block a thread that serves requests.
 */
class ThreadLocalLogger final {
 public:
  using PublishDurable = std::function<void(EpochNumber)>;
  using PublishFailure = std::function<void(int)>;
  using ReadDurable = std::function<EpochNumber()>;

  /**
   * @brief Opens the log and retains the callbacks. Does not start the
   *        flusher.
   *
   * @details publish_durable and publish_failure both run on the flusher
   * thread, the first with the closed epoch just made durable, including a
   * range that held no record, the second with the errno of a failed group
   * write. read_durable returns the last epoch already published as durable.
   * None of the three may be empty, and all three must stay valid until
   * StopFlusher joins.
   */
  ThreadLocalLogger(const Config &config, PublishDurable publish_durable,
                    PublishFailure publish_failure, ReadDurable read_durable,
                    WalIo io = WalIo::Posix());
  ~ThreadLocalLogger();

  /**
   * @brief Buffers one committed transaction's write set.
   * @return Whether anything was buffered: a write set producing no
   * key-value pair has nothing to make durable.
   */
  bool Enqueue(const WriteSet &ws, EpochNumber epoch);

  /**
   * @brief Scans the log and zeroes a torn tail. Completes before the
   * flusher starts.
   */
  WalScanResult Scan(EpochNumber min_epoch);

  /**
   * @brief Starts the flusher. Called once, after the log has been scanned.
   */
  void StartFlusher();

  /**
   * @brief Publishes a new closed epoch. Only records at or below `closed`
   * may be written, because a later epoch can still gain participants.
   */
  void ScheduleFlush(EpochNumber closed);

  /**
   * @brief Flushes everything already closed, then stops and joins the
   * flusher. After a write failure nothing more is flushed and the join is
   * immediate.
   */
  void StopFlusher();

  /**
   * @brief True while every closed epoch handed over is durable, and
   * unconditionally after a write failure.
   */
  bool IsQuiescent();

 private:
  struct ThreadLocalStorageNode {
    std::mutex log_records_mutex;
    LogRecords log_records;
  };

  void FlusherLoop();

  // Whether a closed epoch is still waiting for the device. Called with
  // state_mutex_ held.
  bool ClosedAheadOfDurable() const { return closed_ > read_durable_(); }

  /**
   * @brief Swaps every node's buffer, buckets by epoch, writes buckets at or
   *        below `target`.
   */
  WalAppendResult FlushThrough(EpochNumber target);

  ThreadKeyStorage<ThreadLocalStorageNode> nodes_;

  // Owned by the flusher thread alone, between StartFlusher and the join.
  std::map<EpochNumber, LogRecords> buckets_;
  Wal wal_;

  PublishDurable publish_durable_;
  PublishFailure publish_failure_;
  ReadDurable read_durable_;

  std::mutex state_mutex_;
  std::condition_variable work_cv_;
  EpochNumber closed_{0};
  bool stop_requested_{false};
  bool failed_{false};
  std::thread flusher_;
};

}  // namespace wal
}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_WAL_THREAD_LOCAL_LOGGER_H
