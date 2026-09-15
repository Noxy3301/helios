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
 * Folds the checkpoint and the log tail into the entries recovery replays.
 */

#include "wal/logger.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <exception>
#include <functional>
#include <unordered_map>
#include <utility>

#include "lineairdb/config.h"

#include "util/epoch.h"
#include "util/spdlog.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/flush_trace.h"

namespace helios::storage {
namespace wal {

namespace {

/**
 * @brief Folds one more field's hash into a seed.
 * @note The constant is `2^32` divided by the golden ratio, boost's
 * hash_combine mixer; with the shifts it spreads each field's bits before
 * the fold.
 */
size_t HashCombine(size_t seed, size_t value) {
  constexpr size_t kGoldenRatioMix = 0x9e3779b9u;
  return seed ^ (value + kGoldenRatioMix + (seed << 6) + (seed >> 2));
}

using Write = LogRecord::Write;

// One secondary-index delta: which primary key the entry gained or lost.
struct SecondaryOpKey {
  std::string table_name;
  std::string index_name;
  uint32_t index_type;
  std::string secondary_key;
  std::string primary_key;
  bool operator==(const SecondaryOpKey &rhs) const {
    return table_name == rhs.table_name && index_name == rhs.index_name &&
           index_type == rhs.index_type && secondary_key == rhs.secondary_key &&
           primary_key == rhs.primary_key;
  }
};
struct SecondaryOpKeyHash {
  size_t operator()(const SecondaryOpKey &key) const {
    const std::hash<std::string> hasher;
    size_t seed = hasher(key.table_name);
    seed = HashCombine(seed, hasher(key.index_name));
    seed = HashCombine(seed, std::hash<uint32_t>{}(key.index_type));
    seed = HashCombine(seed, hasher(key.secondary_key));
    seed = HashCombine(seed, hasher(key.primary_key));
    return seed;
  }
};
struct SecondaryOpState {
  Tidword tid;
  SecondaryIndexOp op;
};

// One secondary-index entry the deltas above are regrouped into.
struct SecondaryGroupKey {
  std::string table_name;
  std::string index_name;
  uint32_t index_type;
  std::string secondary_key;
  bool operator==(const SecondaryGroupKey &rhs) const {
    return table_name == rhs.table_name && index_name == rhs.index_name &&
           index_type == rhs.index_type && secondary_key == rhs.secondary_key;
  }
};
struct SecondaryGroupKeyHash {
  size_t operator()(const SecondaryGroupKey &key) const {
    const std::hash<std::string> hasher;
    size_t seed = hasher(key.table_name);
    seed = HashCombine(seed, hasher(key.index_name));
    seed = HashCombine(seed, std::hash<uint32_t>{}(key.index_type));
    seed = HashCombine(seed, hasher(key.secondary_key));
    return seed;
  }
};
struct SecondaryGroupValue {
  Tidword max_tid{};
  std::vector<std::string> primary_keys;
};

struct PrimaryKeyHash {
  size_t operator()(const std::pair<std::string, std::string> &key) const {
    const std::hash<std::string> hasher;
    return HashCombine(hasher(key.first), hasher(key.second));
  }
};

using SecondaryOps =
    std::unordered_map<SecondaryOpKey, SecondaryOpState, SecondaryOpKeyHash>;
// (table_name, key) -> position in the recovery entries. Only the primary path
// uses it; a primary write always carries an empty index_name.
using PrimaryPos = std::unordered_map<std::pair<std::string, std::string>,
                                      size_t, PrimaryKeyHash>;

// The index name alone decides: a primary write carries an empty one, and
// the other fields are written on both paths.
bool IsSecondary(const Write &write) { return !write.index_name.empty(); }

// Keep the newest delta per (secondary key, primary key). A full entry
// arrives as one add per primary key it holds.
void FoldSecondary(const Write &write, SecondaryOps &ops) {
  const auto op = write.secondary_op;
  if (op == SecondaryIndexOp::kFull) {
    for (const auto &pk : write.primary_keys) {
      SecondaryOpKey op_key{write.table_name, write.index_name,
                            write.index_type, write.key, pk};
      auto it = ops.find(op_key);
      if (it == ops.end() || it->second.tid < write.transaction_id) {
        ops[op_key] = {write.transaction_id, SecondaryIndexOp::kInsert};
      }
    }
  } else if (!write.secondary_primary_key.empty()) {
    SecondaryOpKey op_key{write.table_name, write.index_name, write.index_type,
                          write.key, write.secondary_primary_key};
    auto it = ops.find(op_key);
    if (it == ops.end() || !(write.transaction_id < it->second.tid)) {
      ops[op_key] = {write.transaction_id, op};
    }
  }
}

// Keep the newest version per row. Folded through a position map rather than
// a rescan of the entries: the fold runs once per logged write, and a linear
// rescan makes recovery quadratic in the log size.
void FoldPrimary(const Write &write, LogEntries &recovery_entries,
                 PrimaryPos &positions) {
  const auto it = positions.find({write.table_name, write.key});
  if (it != positions.end()) {
    auto &item = recovery_entries[it->second];
    if (item.tid < write.transaction_id) {
      item.value = write.buffer;
      item.tid = write.transaction_id;
      item.table_name = write.table_name;
      item.index_name = write.index_name;
      item.index_type = static_cast<IndexConstraint>(write.index_type);
    }
    return;
  }

  positions.emplace(std::make_pair(write.table_name, write.key),
                    recovery_entries.size());
  LogEntry entry;
  entry.key = write.key;
  entry.value = write.buffer;
  entry.tid = write.transaction_id;
  entry.table_name = write.table_name;
  entry.index_name = write.index_name;
  entry.index_type = static_cast<IndexConstraint>(write.index_type);
  recovery_entries.emplace_back(std::move(entry));
}

// Regroup the surviving adds into one entry per secondary key, so a key
// deleted after being added does not come back.
void GroupSecondary(const SecondaryOps &ops, LogEntries &recovery_entries) {
  std::unordered_map<SecondaryGroupKey, SecondaryGroupValue,
                     SecondaryGroupKeyHash>
      grouped;
  for (const auto &[op_key, state] : ops) {
    if (state.op != SecondaryIndexOp::kInsert) continue;
    SecondaryGroupKey group_key{op_key.table_name, op_key.index_name,
                                op_key.index_type, op_key.secondary_key};
    auto &entry = grouped[group_key];
    entry.primary_keys.emplace_back(op_key.primary_key);
    if (entry.max_tid < state.tid) entry.max_tid = state.tid;
  }

  for (auto &[group_key, entry] : grouped) {
    if (entry.primary_keys.empty()) continue;
    std::sort(entry.primary_keys.begin(), entry.primary_keys.end());
    entry.primary_keys.erase(
        std::unique(entry.primary_keys.begin(), entry.primary_keys.end()),
        entry.primary_keys.end());
    LogEntry log_entry;
    log_entry.key = group_key.secondary_key;
    log_entry.tid = entry.max_tid;
    log_entry.primary_keys = std::move(entry.primary_keys);
    log_entry.table_name = group_key.table_name;
    log_entry.index_name = group_key.index_name;
    log_entry.index_type = static_cast<IndexConstraint>(group_key.index_type);
    recovery_entries.emplace_back(std::move(log_entry));
  }
}

/**
 * @brief Folds log records into the entries the database replays.
 *
 * A key may appear in several epochs; the newest transaction id wins. Secondary
 * index entries arrive as per-primary-key deltas and are regrouped into one
 * entry per secondary key.
 *
 * The checkpoint is folded in ahead of the log's tail as ordinary
 * records, under the same rule that resolves two epochs of the log.
 */
LogEntries BuildRecoveryEntries(const LogRecords &checkpoint,
                                const LogRecords &tail) {
  SecondaryOps secondary_latest;
  PrimaryPos primary_position;
  LogEntries recovery_entries;

  const LogRecords *sources[] = {&checkpoint, &tail};
  for (const auto *source : sources) {
    for (const auto &log_record : *source) {
      for (const auto &write : log_record.writes) {
        if (IsSecondary(write)) {
          FoldSecondary(write, secondary_latest);
        } else {
          FoldPrimary(write, recovery_entries, primary_position);
        }
      }
    }
  }

  GroupSecondary(secondary_latest, recovery_entries);
  return recovery_entries;
}

}  // namespace

Logger::Logger(const Config &config, WalIo io)
    : work_dir_(config.work_dir),
      loads_checkpoint_(config.enable_recovery),
      wal_(config.work_dir, std::move(io), config.wal_initial_capacity_bytes) {
  helios::storage::util::InitDebugLog();
}

Logger::~Logger() { Stop(); }

void Logger::Enqueue(LogRecord record) {
  // Append to this thread's buffer; the worker collects it later.
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

Logger::RecoveryResult Logger::Recover() {
  // Only a replay reads the checkpoint. A startup that scans the log without
  // replaying it does so to find the end of the log, which the checkpoint says
  // nothing about.
  EpochScanCheckpoint::LoadResult checkpoint;
  if (loads_checkpoint_) {
    checkpoint = EpochScanCheckpoint::Load(work_dir_);
    if (checkpoint.status ==
        EpochScanCheckpoint::LoadResult::Status::kUnusable) {
      // A checkpoint that cannot be trusted is not a reason to refuse to start:
      // the log alone still holds everything the checkpoint would have
      // supplied.
      SPDLOG_WARN("Ignoring the checkpoint: {0}", checkpoint.detail);
      checkpoint.records.clear();
      // The start epoch is cleared with it so the scan skips nothing.
      checkpoint.start_epoch = 0;
    }
  }

  const auto wal = wal_.Scan(checkpoint.start_epoch);
  RecoveryResult result;
  if (wal.status != WalScanResult::Status::kOk) return FailRecovery(wal);

  if (checkpoint.status == EpochScanCheckpoint::LoadResult::Status::kOk) {
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
  result.recovery_entries = BuildRecoveryEntries(checkpoint.records, wal.records);
  return result;
}

void Logger::Start() {
  assert(!worker_.joinable());
  worker_ = std::thread(&Logger::Worker, this);
}

void Logger::RequestFlush(EpochNumber max_epoch) {
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  const int64_t close_enter = traced ? FlushTrace::Now() : 0;

  // Raise the requested flush limit before waking the worker.
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
  // Let the worker finish closed epochs before releasing its WAL and buffers.
  {
    std::lock_guard<std::mutex> lock(work_mutex_);
    stop_requested_ = true;
  }
  work_cv_.notify_all();
  if (worker_.joinable()) worker_.join();
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
    LogRecords swapped;
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

void Logger::Worker() {
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
      SPDLOG_CRITICAL("Durability Error: the logger worker threw: {0}",
                      e.what());
      result = {false, EIO};
    } catch (...) {
      SPDLOG_CRITICAL("Durability Error: the logger worker threw");
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
