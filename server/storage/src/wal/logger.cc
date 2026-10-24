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
 * @file server/storage/src/wal/logger.cc
 * The write-ahead log and the durable epoch a synchronous commit waits on.
 */

#include "wal/logger.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <utility>

#include "helios/config.h"

#include "util/epoch.h"
#include "util/epoch_framework.h"
#include "util/spdlog.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/flush_trace.h"

namespace helios::storage {
namespace wal {

Logger::Logger(const Config &config, WalIo io)
    : work_dir_(config.work_dir),
      loads_checkpoint_records_(config.enable_recovery),
      wal_(config.work_dir, std::move(io), config.wal_initial_capacity_bytes) {
  helios::storage::util::set_log_pattern();
}

Logger::~Logger() { Stop(); }

void Logger::Enqueue(PackedLogRecord record) {
  // Append to this thread's buffer; the logger thread collects it later.
  auto *buffer = buffers_.Get();
  std::lock_guard<std::mutex> lock(buffer->mutex);
  buffer->records.emplace_back(std::move(record));
}

Logger::RecoveryResult Logger::FailRecovery(const WalScanResult &wal) {
  SPDLOG_CRITICAL("Durability Error: {0}, errno {1}",
                  wal.detail, wal.error_number);
  PublishFailure(wal.error_number != 0 ? wal.error_number : EIO);
  RecoveryResult result;
  result.status = RecoveryStatus::kFailed;
  return result;
}

Logger::RecoveryResult Logger::Recover(const std::vector<Replay> &passes) {
  // Every startup reads the checkpoint: its publication waited for the end
  // epoch to become durable, so that epoch binds even when nothing is
  // replayed. A startup that replays nothing takes the epoch bounds alone.
  auto checkpoint = EpochScanCheckpoint::Load(work_dir_, loads_checkpoint_records_);
  if (checkpoint.status == EpochScanCheckpoint::LoadResult::Status::kUnusable) {
    // A checkpoint that cannot be trusted is not a reason to refuse to start:
    // the log alone still holds everything the checkpoint would have
    // supplied.
    SPDLOG_WARN("Ignoring the checkpoint: {0}", checkpoint.detail);
    checkpoint.records.clear();
    // The start epoch is cleared with it so the scan skips nothing; the
    // status keeps the end epoch out of the durable epoch below.
    checkpoint.start_epoch = 0;
  }
  if (!loads_checkpoint_records_) {
    // Without a replay every frame is read, so nothing is skipped.
    checkpoint.start_epoch = 0;
  }

  // Each pass replays the checkpoint, then the log. The first scan repairs
  // the tail, so a later one reads the same frames. The checkpoint is
  // released before the last scan.
  WalScanResult wal;
  for (size_t i = 0; i < passes.size(); ++i) {
    if (passes[i]) passes[i](checkpoint.records);
    if (i + 1 == passes.size()) LogRecords().swap(checkpoint.records);
    wal = wal_.Scan(checkpoint.start_epoch, passes[i]);
    if (wal.status != WalScanResult::Status::kOk) return FailRecovery(wal);
  }
  RecoveryResult result;

  if (loads_checkpoint_records_ &&
      checkpoint.status == EpochScanCheckpoint::LoadResult::Status::kOk) {
    SPDLOG_INFO(
        "Recovering from the checkpoint of epoch {0}: {1} frames of "
        "{2} bytes are covered by it and are not replayed",
        checkpoint.start_epoch, wal.frames_skipped, wal.bytes_skipped);
  }

  // A quiet log can end before the checkpoint's end epoch. Publication
  // waited for that epoch, so recovery must also resume above it.
  result.durable_epoch = wal.last_epoch;
  if (checkpoint.status == EpochScanCheckpoint::LoadResult::Status::kOk) {
    result.durable_epoch = std::max(wal.last_epoch, checkpoint.end_epoch);
  }
  durable_epoch_.store(result.durable_epoch, std::memory_order_seq_cst);
  return result;
}

void Logger::Start(epoch::Framework *epoch) {
  assert(!logger_thread_.joinable());
  epoch_ = epoch;
  logger_thread_ = std::thread(&Logger::LoggerThread, this);
}

void Logger::RequestFlush(EpochNumber max_epoch) {
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  const int64_t close_enter = traced ? FlushTrace::Now() : 0;

  // Raise the requested flush limit before waking the logger thread.
  {
    std::lock_guard<std::mutex> lock(work_mutex_);
    if (stop_requested_) return;
    if (max_epoch > flush_epoch_) flush_epoch_ = max_epoch;
  }
  const int64_t close_exit = traced ? FlushTrace::Now() : 0;
  work_cv_.notify_one();
  if (traced) trace.EpochClosed(max_epoch, close_enter, close_exit);
}

void Logger::Stop() {
  // Let the logger thread finish closed epochs before releasing its WAL and
  // buffers.
  {
    std::lock_guard<std::mutex> lock(work_mutex_);
    stop_requested_ = true;
  }
  work_cv_.notify_all();
  if (logger_thread_.joinable()) logger_thread_.join();
  PublishStopped();
}

void Logger::PublishDurable(EpochNumber durable_epoch) {
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    const EpochNumber previous = durable_epoch_.load(std::memory_order_seq_cst);
    if (durable_epoch < previous) {
      // The durable epoch is the promise the commit path hands to callers;
      // moving it backwards would retract a commit already reported.
      SPDLOG_CRITICAL(
          "Durability Error: the durable epoch moved backwards, {0} to {1}",
          previous, durable_epoch);
      std::abort();
    }
    if (state_ != State::kRunning) return;
    durable_epoch_.store(durable_epoch, std::memory_order_seq_cst);
  }
  durability_cv_.notify_all();
}

void Logger::PublishFailure(int error_number) {
  bool abort_process = false;
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    abort_process = process_fail_stop_;
    const bool first_failure = state_ != State::kFailed;
    // A repeated failure has nothing new to publish, but arming still turns
    // it into an abort: a failure that predates the arming must not exempt
    // the process afterwards.
    if (!first_failure && !abort_process) return;
    if (first_failure) {
      state_ = State::kFailed;
      SPDLOG_CRITICAL(
          "Durability Error: the log cannot be written (errno {0}); no "
          "further commit is reported durable",
          error_number);
    }
  }
  durability_cv_.notify_all();
  if (abort_process) std::abort();
}

void Logger::SetFailStop() {
  std::lock_guard<std::mutex> lock(durability_mutex_);
  process_fail_stop_ = true;
}

void Logger::PublishStopped() {
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    if (state_ == State::kRunning) state_ = State::kStopped;
  }
  durability_cv_.notify_all();
}

Logger::WaitResult Logger::WaitUntilDurable(EpochNumber commit_epoch,
                                            Deadline deadline) {
  if (durable_epoch_.load(std::memory_order_seq_cst) >= commit_epoch) {
    return WaitResult::kDurable;
  }

  // The epoch hook requests this epoch's flush once `E = commit_epoch + 2`.
  if (epoch_ != nullptr) epoch_->RequestEpochAdvance(commit_epoch + 2);

  std::unique_lock<std::mutex> lock(durability_mutex_);
  const auto reached = [&] {
    return durable_epoch_.load(std::memory_order_seq_cst) >= commit_epoch ||
           state_ != State::kRunning;
  };
  if (deadline == Deadline::max()) {
    durability_cv_.wait(lock, reached);
  } else if (!durability_cv_.wait_until(lock, deadline, reached)) {
    return WaitResult::kTimedOut;
  }

  // A durable epoch that already covers this one outranks a terminal state: the
  // records are on the device regardless of what happened afterwards.
  if (durable_epoch_.load(std::memory_order_seq_cst) >= commit_epoch) {
    return WaitResult::kDurable;
  }
  return state_ == State::kStopped ? WaitResult::kStopped : WaitResult::kFailed;
}

bool Logger::within_max_lag(epoch::Framework &epoch) const {
  // D before E: D only grows, and a pass then held when E was read.
  const EpochNumber durable_epoch = GetDurableEpoch();
  const EpochNumber global_epoch = epoch.GetGlobalEpoch();
  return global_epoch <= kMaxLagEpochs ||
         durable_epoch >= global_epoch - kMaxLagEpochs;
}

void Logger::WaitMaxLag(epoch::Framework &epoch) {
  assert(epoch.ThreadEpoch() == epoch::Framework::kThreadOffline);
  if (within_max_lag(epoch)) return;
  std::unique_lock<std::mutex> lock(durability_mutex_);
  durability_cv_.wait(
      lock, [&] { return state_ != State::kRunning || within_max_lag(epoch); });
}

void Logger::AwaitCommitDurability(EpochNumber commit_epoch,
                                   bool awaits_durability) {
  if (!awaits_durability) return;

  // The sample is drawn before the wait, so a commit whose epoch is already
  // durable is represented alongside one that waits. The durable epoch reading
  // is what the commit saw on arrival; publication can land before the wait
  // makes its own check, which is why the recorded field says only that.
  auto &trace = FlushTrace::Instance();
  const bool sampled = trace.SampleThisCommit();
  const int64_t wait_enter = sampled ? FlushTrace::Now() : 0;
  const bool not_durable_at_enter =
      sampled && durable_epoch_.load(std::memory_order_seq_cst) < commit_epoch;

  // Deadline::max() cannot time out, so any result other than Durable is a
  // stop or an I/O failure and must not be reported.
  const auto result = WaitUntilDurable(commit_epoch, Deadline::max());
  if (sampled) {
    trace.RecordCommit(commit_epoch, wait_enter, FlushTrace::Now(),
                       not_durable_at_enter);
  }
  if (result == WaitResult::kDurable) return;

  SPDLOG_CRITICAL(
      "Durability Error: the log for epoch {0} did not become durable, and the "
      "transaction that committed in it cannot be reported",
      commit_epoch);
  std::abort();
}

WalAppendResult Logger::FlushThrough(EpochNumber target) {
  const EpochNumber durable_before = GetDurableEpoch();
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  if (traced) trace.GroupCollectBegin(durable_before);

  // Take each producer's records without holding its lock during file I/O.
  buffers_.ForEach([&](LogBuffer *buffer) {
    PackedLogRecords swapped;
    {
      std::lock_guard<std::mutex> lock(buffer->mutex);
      swapped.swap(buffer->records);
    }
    for (auto &record : swapped) {
      if (record.epoch <= durable_before) {
        // Enqueue precedes epoch departure, so a durable epoch cannot gain
        // another record.
        SPDLOG_CRITICAL(
            "Durability Error: a record for epoch {0} arrived after {1} was "
            "reported durable",
            record.epoch, durable_before);
        std::abort();
      }
      pending_records_[record.epoch].emplace_back(std::move(record));
    }
  });

  if (traced) trace.GroupCollectEnd();

  // Write closed epochs; future epochs remain pending for the next batch.
  const auto result = wal_.AppendGroup(pending_records_, target);
  if (!result.ok) return result;
  pending_records_.erase(pending_records_.begin(),
                         pending_records_.upper_bound(target));
  return result;
}

void Logger::LoggerThread() {
  for (;;) {
    // Wait for a flush request, or finish draining on shutdown.
    EpochNumber target = 0;
    {
      std::unique_lock<std::mutex> lock(work_mutex_);
      work_cv_.wait(lock, [this] {
        return stop_requested_ || flush_epoch_ > GetDurableEpoch();
      });
      target = flush_epoch_;
      if (target <= GetDurableEpoch()) {
        if (stop_requested_) return;
        continue;
      }
    }

    // Collect and write without holding the notification mutex.
    WalAppendResult result;
    try {
      result = FlushThrough(target);
    } catch (const std::exception &e) {
      SPDLOG_CRITICAL("Durability Error: the logger thread threw: {0}",
                      e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the logger thread threw");
      result = {false, EIO};
    }

    // Stop accepting epoch notifications and report the failed write.
    if (!result.ok) {
      {
        std::lock_guard<std::mutex> lock(work_mutex_);
        stop_requested_ = true;
      }
      PublishFailure(result.error_number);
      return;
    }

    // Publish the closed range after syncing its records, if any.
    auto &trace = FlushTrace::Instance();
    const bool traced = trace.Enabled();
    const int64_t publish_enter = traced ? FlushTrace::Now() : 0;
    PublishDurable(target);
    if (traced) trace.GroupPublish(target, publish_enter, FlushTrace::Now());
  }
}

}  // namespace wal
}  // namespace helios::storage
