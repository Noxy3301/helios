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
 * @file server/storage/src/wal/thread_local_logger.cc
 * Per-thread record buffers and the one flusher thread that writes them.
 */

#include "wal/thread_local_logger.h"

#include <errno.h>

#include <cassert>
#include <cstdlib>
#include <exception>
#include <iterator>
#include <utility>

#include "util/epoch.h"
#include "util/spdlog.h"
#include "wal/flush_trace.h"

namespace helios::storage {
namespace wal {

ThreadLocalLogger::ThreadLocalLogger(const Config &config,
                                     PublishDurable publish_durable,
                                     PublishFailure publish_failure,
                                     ReadDurable read_durable, WalIo io)
    : wal_(config.work_dir, std::move(io), config.wal_initial_capacity_bytes),
      publish_durable_(std::move(publish_durable)),
      publish_failure_(std::move(publish_failure)),
      read_durable_(std::move(read_durable)) {
  helios::storage::util::InitDebugLog();
}

ThreadLocalLogger::~ThreadLocalLogger() { StopFlusher(); }

bool ThreadLocalLogger::Enqueue(const WriteSet &ws, EpochNumber epoch) {
  LogRecord log_record;
  log_record.epoch = epoch;

  for (auto &entry : ws) {
    if (entry.index_name.empty()) {
      LogRecord::Write write;
      write.key = entry.key;
      write.buffer = entry.data_item_copy.buffer.toString();
      write.transaction_id = entry.data_item_copy.transaction_id.load();
      write.table_name = entry.table_name;
      write.index_name = entry.index_name;
      write.index_type = static_cast<uint32_t>(entry.index_type);
      write.primary_keys = entry.data_item_copy.primary_keys_vector();
      write.secondary_op = SecondaryIndexOp::kNone;
      log_record.writes.emplace_back(std::move(write));
      continue;
    }

    if (entry.secondary_index_deltas.empty()) continue;
    for (const auto &delta : entry.secondary_index_deltas) {
      LogRecord::Write write;
      write.key = entry.key;
      write.buffer = entry.data_item_copy.buffer.toString();
      write.transaction_id = entry.data_item_copy.transaction_id.load();
      write.table_name = entry.table_name;
      write.index_name = entry.index_name;
      write.index_type = static_cast<uint32_t>(entry.index_type);
      write.secondary_op = delta.op;
      write.secondary_primary_key = delta.primary_key;
      log_record.writes.emplace_back(std::move(write));
    }
  }

  // Decided after building the record, not from the input write set: a write
  // set of secondary entries that carry no delta produces nothing to persist,
  // and the commit path must not wait for a record that was never buffered.
  if (log_record.writes.empty()) return false;

  auto *node = nodes_.Get();
  std::lock_guard<std::mutex> lock(node->log_records_mutex);
  node->log_records.emplace_back(std::move(log_record));
  return true;
}

WalScanResult ThreadLocalLogger::Scan(EpochNumber min_epoch) {
  return wal_.Scan(min_epoch);
}

EpochNumber ThreadLocalLogger::GetWalLastEpoch() const {
  return wal_.last_epoch();
}

void ThreadLocalLogger::StartFlusher() {
  assert(!flusher_.joinable());
  flusher_ = std::thread([this]() { FlusherLoop(); });
}

void ThreadLocalLogger::ScheduleFlush(EpochNumber closed) {
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  const int64_t close_enter = traced ? FlushTrace::Now() : 0;
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (stop_requested_ || failed_) return;
    if (closed > closed_) closed_ = closed;
  }
  // The hand-over is timed before the flusher is woken and recorded after, so
  // the census never sits between the state change and the notification.
  const int64_t close_exit = traced ? FlushTrace::Now() : 0;
  work_cv_.notify_one();
  if (traced) trace.EpochClosed(closed, close_enter, close_exit);
}

bool ThreadLocalLogger::IsQuiescent() {
  std::lock_guard<std::mutex> lock(state_mutex_);
  return failed_ || !ClosedAheadOfDurable();
}

void ThreadLocalLogger::StopFlusher() {
  {
    std::lock_guard<std::mutex> lock(state_mutex_);
    stop_requested_ = true;
  }
  work_cv_.notify_all();
  if (flusher_.joinable()) flusher_.join();
}

void ThreadLocalLogger::FlusherLoop() {
  for (;;) {
    // Wait for a closed epoch past durable, or for shutdown or failure.
    EpochNumber target = 0;
    {
      std::unique_lock<std::mutex> lock(state_mutex_);
      work_cv_.wait(lock, [this] {
        return stop_requested_ || failed_ || ClosedAheadOfDurable();
      });
      if (failed_) return;
      target = closed_;
      const bool nothing_to_do = !ClosedAheadOfDurable();
      // Stop only once everything already closed is on the device, so a
      // clean shutdown does not drop records already handed over as closed.
      if (stop_requested_ && nothing_to_do) return;
      if (nothing_to_do) continue;
    }

    // Packing and file I/O run without state_mutex_; a committing thread
    // contends only for its own node's short buffer lock, never behind
    // packing or fdatasync.
    // Write the group without holding state_mutex_.
    WalAppendResult result;
    try {
      result = FlushThrough(target);
    } catch (const std::exception &e) {
      SPDLOG_CRITICAL("Durability Error: the flusher threw: {0}", e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the flusher threw");
      result = {false, EIO};
    }

    // Freeze the flusher and publish the I/O error.
    if (!result.ok) {
      {
        std::lock_guard<std::mutex> lock(state_mutex_);
        failed_ = true;
      }
      publish_failure_(result.error_number);
      return;
    }
    // Publish the group as durable. An empty eligible range publishes
    // without an append: the skipped epochs carry no record, and an epoch
    // with nothing to persist is durable by definition.
    auto &trace = FlushTrace::Instance();
    const bool traced = trace.Enabled();
    const int64_t publish_enter = traced ? FlushTrace::Now() : 0;
    publish_durable_(target);
    if (traced) trace.GroupPublish(target, publish_enter, FlushTrace::Now());
  }
}

WalAppendResult ThreadLocalLogger::FlushThrough(EpochNumber target) {
  const EpochNumber durable_before = read_durable_();
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  if (traced) trace.GroupCollectBegin(durable_before);

  nodes_.ForEach([&](ThreadLocalStorageNode *node) {
    LogRecords swapped;
    {
      std::lock_guard<std::mutex> lock(node->log_records_mutex);
      swapped.swap(node->log_records);
    }
    for (auto &record : swapped) {
      if (record.epoch <= durable_before) {
        // A producer leaves its epoch only after Enqueue returns, so an
        // epoch the writer has already closed cannot gain a record
        // afterwards.
        // Reaching here means the closure the durability contract rests on is
        // broken, and continuing would acknowledge a record that is not on the
        // device.
        SPDLOG_CRITICAL(
            "Durability Error: a record for epoch {0} arrived after {1} was "
            "reported durable",
            record.epoch, durable_before);
        std::abort();
      }
      buckets_[record.epoch].emplace_back(std::move(record));
    }
  });

  if (traced) trace.GroupCollectEnd();

  const auto result = wal_.AppendGroup(buckets_, target);
  if (!result.ok) return result;
  // Buckets above the target stay for the next group; the ones just written are
  // the only ones dropped.
  buckets_.erase(buckets_.begin(), buckets_.upper_bound(target));
  return result;
}

}  // namespace wal
}  // namespace helios::storage
