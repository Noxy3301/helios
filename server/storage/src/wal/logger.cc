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
 * @file server/storage/src/wal/logger.cc
 * The write-ahead log and the durability frontier a synchronous commit waits
 * on. Folds the checkpoint and the log tail into the write set recovery
 * replays.
 */

#include "wal/logger.h"

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <unordered_map>
#include <utility>

#include "storage/config.h"
#include "util/epoch.h"
#include "util/spdlog.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/flush_trace.h"
#include "wal/thread_local_logger.h"

namespace helios::storage {
namespace wal {

namespace {

/**
 * @brief Folds one more field's hash into a seed.
 * @note The constant is 2^32 divided by the golden ratio, boost's
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
  TransactionId tid;
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
  TransactionId max_tid{};
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
// (table_name, key) -> position in the recovery set. Only the primary path
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
        ops[op_key] = {write.transaction_id, SecondaryIndexOp::kAdd};
      }
    }
  } else if (!write.secondary_primary_key.empty()) {
    SecondaryOpKey op_key{write.table_name, write.index_name, write.index_type,
                          write.key, write.secondary_primary_key};
    auto it = ops.find(op_key);
    if (it == ops.end() || it->second.tid < write.transaction_id) {
      ops[op_key] = {write.transaction_id, op};
    }
  }
}

// Keep the newest version per row. Folded through a position map rather than
// a rescan of the set: the fold runs once per logged write, and a linear
// rescan makes recovery quadratic in the log size.
void FoldPrimary(const Write &write, WriteSetType &recovery_set,
                 PrimaryPos &positions) {
  const std::byte *value_ptr =
      write.buffer.empty()
          ? nullptr
          : reinterpret_cast<const std::byte *>(write.buffer.data());
  const auto it = positions.find({write.table_name, write.key});
  if (it != positions.end()) {
    auto &item = recovery_set[it->second];
    if (item.data_item_copy.transaction_id.load() < write.transaction_id) {
      item.data_item_copy.Reset(value_ptr, write.buffer.size(),
                                write.transaction_id);
      item.table_name = write.table_name;
      item.index_name = write.index_name;
      item.index_type = static_cast<IndexConstraint>(write.index_type);
    }
    return;
  }

  positions.emplace(std::make_pair(write.table_name, write.key),
                    recovery_set.size());
  Snapshot snapshot = {
      write.key,
      reinterpret_cast<const std::byte *>(write.buffer.data()),
      write.buffer.size(),
      nullptr,
      write.table_name,
      write.index_name,
      write.transaction_id,
      static_cast<IndexConstraint>(write.index_type),
  };
  recovery_set.emplace_back(std::move(snapshot));
}

// Regroup the surviving adds into one entry per secondary key, so a key
// deleted after being added does not come back.
void GroupSecondary(const SecondaryOps &ops, WriteSetType &recovery_set) {
  std::unordered_map<SecondaryGroupKey, SecondaryGroupValue,
                     SecondaryGroupKeyHash>
      grouped;
  for (const auto &[op_key, state] : ops) {
    if (state.op != SecondaryIndexOp::kAdd) continue;
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
    Snapshot snapshot = {group_key.secondary_key,
                         nullptr,
                         0,
                         nullptr,
                         group_key.table_name,
                         group_key.index_name,
                         entry.max_tid,
                         static_cast<IndexConstraint>(group_key.index_type)};
    snapshot.data_item_copy.SetPrimaryKeys(std::move(entry.primary_keys));
    snapshot.data_item_copy.Reset(nullptr, 0, entry.max_tid);
    recovery_set.emplace_back(std::move(snapshot));
  }
}

/**
 * @brief Folds the records read from the log into the write set the
 *        database replays.
 *
 * A key may appear in several epochs; the newest transaction id wins. Secondary
 * index entries arrive as per-primary-key deltas and are regrouped into one
 * entry per secondary key.
 *
 * The checkpoint image is folded in ahead of the log's tail as ordinary
 * records, under the same rule that resolves two epochs of the log.
 */
WriteSetType BuildRecoverySet(const LogRecords &image, const LogRecords &tail) {
  SecondaryOps secondary_latest;
  PrimaryPos primary_position;
  WriteSetType recovery_set;

  const LogRecords *sources[] = {&image, &tail};
  for (const auto *source : sources) {
    for (const auto &log_record : *source) {
      for (const auto &write : log_record.writes) {
        if (IsSecondary(write)) {
          FoldSecondary(write, secondary_latest);
        } else {
          FoldPrimary(write, recovery_set, primary_position);
        }
      }
    }
  }

  GroupSecondary(secondary_latest, recovery_set);
  return recovery_set;
}

}  // namespace

Logger::Logger(const Config &config, WalIo io)
    : work_dir_(config.work_dir), loads_checkpoint_(config.enable_recovery) {
  helios::storage::util::InitDebugLog();
  thread_local_logger_ = std::make_unique<ThreadLocalLogger>(
      config, [this](EpochNumber frontier) { PublishDurable(frontier); },
      [this](int error_number) { PublishFailure(error_number); },
      [this]() { return GetDurableEpoch(); }, std::move(io));
}

Logger::~Logger() {
  StopFlusher();
  thread_local_logger_.reset();
}

bool Logger::Enqueue(const WriteSetType &ws, EpochNumber epoch) {
  return thread_local_logger_->Enqueue(ws, epoch);
}

Logger::RecoveryResult Logger::FailRecovery(const WalScanResult &scan) {
  SPDLOG_CRITICAL(
      "Durability Error: {0} ({1}), errno {2}", scan.detail,
      scan.status == WalScanResult::Status::kCorrupt ? "corrupt" : "I/O error",
      scan.error_number);
  PublishFailure(scan.error_number != 0 ? scan.error_number : EIO);
  RecoveryResult result;
  result.status = RecoveryStatus::kFailed;
  return result;
}

Logger::RecoveryResult Logger::Recover() {
  // Only a replay reads the image. A startup that scans the log without
  // replaying it does so to find the end of the log, which the image says
  // nothing about.
  // FIXME: an image is bound to a log by sharing a directory with it, and
  // neither file names the database it came from
  EpochScanCheckpoint::Image image;
  if (loads_checkpoint_) {
    image = EpochScanCheckpoint::Load(work_dir_);
    if (image.status == EpochScanCheckpoint::Image::Status::kUnusable) {
      // An image that cannot be trusted is not a reason to refuse to start:
      // the log alone still holds everything the image would have supplied.
      SPDLOG_WARN("Ignoring the checkpoint image: {0}", image.detail);
      image.records.clear();
      // The cut is cleared with it so the scan hops nothing.
      image.cut_epoch = 0;
    }
  }

  auto scan = thread_local_logger_->ScanAndRepair(image.cut_epoch);
  RecoveryResult result;
  if (scan.status != WalScanResult::Status::kOk) return FailRecovery(scan);

  if (image.status == EpochScanCheckpoint::Image::Status::kOk) {
    // The image is honoured only when this log's last written epoch is at
    // least the one recorded at publish; a shorter log cannot be the file
    // that image named (see the FIXME above).
    if (scan.frontier < image.wal_frontier_at_publish) {
      SPDLOG_WARN(
          "Ignoring the checkpoint image: it was published when the log was "
          "durable through epoch {0}, past the last epoch {1} this log "
          "holds",
          image.wal_frontier_at_publish, scan.frontier);
      image.records.clear();
      // The frames the first scan hopped have to be read now that the image
      // is gone.
      scan = thread_local_logger_->ScanAndRepair(0);
      if (scan.status != WalScanResult::Status::kOk) return FailRecovery(scan);
    } else {
      SPDLOG_INFO(
          "Recovering from the checkpoint image of epoch {0}: {1} frames of "
          "{2} bytes are covered by it and are not replayed",
          image.cut_epoch, scan.frames_skipped, scan.bytes_skipped);
    }
  }

  durable_epoch_.store(scan.frontier, std::memory_order_seq_cst);
  result.frontier = scan.frontier;
  result.recovery_set = BuildRecoverySet(image.records, scan.records);
  return result;
}

void Logger::StartFlusher() { thread_local_logger_->StartFlusher(); }

void Logger::ScheduleFlush(EpochNumber closed) {
  thread_local_logger_->ScheduleFlush(closed);
}

EpochNumber Logger::GetWalFrontier() const {
  return thread_local_logger_->GetWalFrontier();
}

bool Logger::IsQuiescent() { return thread_local_logger_->IsQuiescent(); }

void Logger::StopFlusher() {
  if (thread_local_logger_) thread_local_logger_->StopFlusher();
  PublishStopped();
}

void Logger::PublishDurable(EpochNumber frontier) {
  {
    std::lock_guard<std::mutex> lock(durability_mutex_);
    const EpochNumber previous = durable_epoch_.load(std::memory_order_seq_cst);
    if (frontier < previous) {
      // The frontier is the promise the commit path hands to callers; moving it
      // backwards would retract an acknowledgement.
      SPDLOG_CRITICAL(
          "Durability Error: the durable epoch moved backwards, {0} to {1}",
          previous, frontier);
      std::abort();
    }
    if (state_ != State::kRunning) return;
    durable_epoch_.store(frontier, std::memory_order_seq_cst);
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
          "further commit is acknowledged as durable",
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

  // A frontier that already covers this epoch outranks a terminal state: the
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
  // durable is represented alongside one that waits. The frontier reading is
  // what the commit saw on arrival; publication can land before the wait makes
  // its own check, which is why the recorded field says only that.
  auto &trace = FlushTrace::Instance();
  const bool sampled = trace.SampleThisCommit();
  const int64_t wait_enter = sampled ? FlushTrace::Now() : 0;
  const bool not_durable_at_enter =
      sampled && durable_epoch_.load(std::memory_order_seq_cst) < commit_epoch;

  // Deadline::max() cannot time out, so any result other than Durable is a
  // stop or an I/O failure and must not be acknowledged.
  const auto result = WaitUntilDurable(commit_epoch, Deadline::max());
  if (sampled) {
    trace.RecordCommit(commit_epoch, wait_enter, FlushTrace::Now(),
                       not_durable_at_enter);
  }
  if (result == WaitResult::kDurable) return;

  SPDLOG_CRITICAL(
      "Durability Error: the log for epoch {0} did not become durable, and the "
      "transaction that committed in it cannot be acknowledged",
      commit_epoch);
  std::abort();
}

}  // namespace wal
}  // namespace helios::storage
