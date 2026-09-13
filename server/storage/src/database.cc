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
 * @file server/storage/src/database.cc
 * Database operations and the startup and shutdown order of its epoch
 * framework, WAL, checkpoint worker and reaper.
 */

#include "storage/database.h"

#include <algorithm>
#include <shared_mutex>

#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "pax/catalog.h"
#include "silo/commit.h"
#include "silo/read.h"
#include "storage/config.h"
#include "util/spdlog.h"
#include "wal/flush_trace.h"
#include "wal/log_entry.h"
namespace helios::storage {

Database::Database() : Database(Config()) {}

void Database::ReleaseThreadEpoch() { index::MasstreeReleaseThreadEpoch(); }

bool Database::HasTable(const std::string_view table_name) {
  return GetTable(table_name) != nullptr;
}

EpochNumber Database::ResumeEpochAbove(EpochNumber durable_epoch) {
  if (durable_epoch >= epoch::Framework::kEpochHighWater - 1) {
    SPDLOG_CRITICAL(
        "Startup failed: resuming above the recovered epoch {0} would reach "
        "the epoch high-water mark {1}",
        durable_epoch, epoch::Framework::kEpochHighWater);
    exit(EXIT_FAILURE);
  }
  return durable_epoch + 1;
}

Database::Database(const Config &config)
    : config_(config),
      logger_(config_),
      epoch_framework_(config_.epoch_duration_ms, MakeEpochHook()),
      scan_checkpoint_(config_, table_dictionary_, epoch_framework_, logger_) {
  SPDLOG_INFO("Storage instance has been constructed.");

  // Restore column definitions before any recovered value is installed.
  auto catalog = pax::LoadCatalog(config_.work_dir);
  if (catalog.status == pax::Catalog::Status::kUnusable) {
    SPDLOG_CRITICAL("Cannot recover the PAX schema catalog: {}",
                    catalog.detail);
    exit(EXIT_FAILURE);
  }
  for (auto &[name, schema] : catalog.entries) {
    CreateTable(name);
    if (!GetTable(name)->InstallPaxSchema(std::move(schema))) {
      SPDLOG_CRITICAL("Cannot restore PAX schema for table {}", name);
      exit(EXIT_FAILURE);
    }
  }
  // Always scan the log, even without recovery: an interrupted tail has to be
  // removed before the first append lands behind it, and recovery only
  // controls whether the records the scan read are replayed.
  if (config_.enable_recovery) {
    Recover();
  } else {
    const auto scanned = logger_.Recover();
    if (scanned.status != wal::Logger::RecoveryStatus::kOk) {
      SPDLOG_CRITICAL(
          "Startup failed: the write-ahead log could not be read; refusing to "
          "start with an unknown durable state");
      exit(EXIT_FAILURE);
    }
    if (scanned.durable_epoch != 0) {
      // Records exist that this instance will not replay; resuming at or
      // below their epoch would let a Sync commit inherit their durable epoch.
      epoch_framework_.SetGlobalEpoch(ResumeEpochAbove(scanned.durable_epoch));
    }
  }

  // Abort the process if the running logger cannot persist its records.
  logger_.SetFailStop();

  // Initialise optional WAL timing before the logger can record events.
  wal::FlushTrace::Instance();

  // Checkpointing needs both WAL persistence and epoch advancement running.
  logger_.Start();
  epoch_framework_.Start();
  scan_checkpoint_.Start();
}

Database::~Database() noexcept {
  // Wait for two epoch advances before shutting down background work.
  epoch_framework_.Sync();

  // Finish checkpoint work while its epoch and WAL waits can still complete.
  scan_checkpoint_.Stop();

  // Stop epoch notifications, then drain closed epochs and join the logger.
  epoch_framework_.Stop();
  logger_.Stop();

  // Report final epochs and write the optional WAL timing output.
  SPDLOG_DEBUG(
      "Epoch number and Durable epoch number are ended at {0}, and {1}, "
      "respectively.",
      epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
  wal::FlushTrace::Instance().Dump();

  SPDLOG_INFO("Storage instance has been destructed.");
}

const Config &Database::GetConfig() const noexcept { return config_; }

std::function<void(EpochNumber)> Database::MakeEpochHook() {
  // The epoch writer calls this with the global epoch it just published.
  return [this](const EpochNumber global_epoch) {
    // An online worker in `e_w` keeps global epoch `E` below `e_w + 2`.
    // With `E = global_epoch`, the logger may persist records through `E - 2`.
    if (global_epoch >= 3) {
      logger_.RequestFlush(global_epoch - 2);
    }

    // Physically purge the tombstones whose grace epoch has passed.
    reaper_.Reap(global_epoch);

    // Tick masstree's globalepoch so RCU can free retired leaves and
    // DataItem limbo once min_active_epoch() catches up. Workers release
    // their epoch at RPC boundaries through ReleaseThreadEpoch; this call
    // only moves active_epoch.
    index::MasstreeAdvanceEpoch();
  };
}

bool Database::CreateTable(const std::string_view table_name) {
  return table_dictionary_.CreateTable(table_name);
}

bool Database::CreateSecondaryIndex(const std::string_view table_name,
                                    const std::string_view index_name,
                                    IndexConstraint index_type) {
  // A value the wire carried that is neither of the declared ones names a
  // promise this storage does not know how to keep.
  if (index_type != IndexConstraint::kNone &&
      index_type != IndexConstraint::kUnique) {
    return false;
  }
  // Exclusive: every reader of the definition holds this lock shared, so a
  // shared one here would let a request resolve half of a schema change.
  std::unique_lock<std::shared_mutex> lk(schema_mutex_);
  Table *table = GetTable(table_name);
  if (table == nullptr) return false;
  return table->CreateSecondaryIndex(index_name, index_type);
}

ReadResult Database::Read(const std::string_view table_name,
                          const std::string_view key,
                          const std::vector<uint32_t> *selected_columns) {
  return silo::Read(table_dictionary_, schema_mutex_, table_name, key,
                    selected_columns);
}

std::vector<ReadResult> Database::BatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return silo::BatchRead(table_dictionary_, schema_mutex_, keys);
}

ScanResult Database::Scan(const std::string_view table_name,
                          const std::string_view start_key,
                          const std::string_view end_key, uint64_t row_limit,
                          bool reverse_scan,
                          const std::vector<uint32_t> *selected_columns) {
  return silo::Scan(table_dictionary_, schema_mutex_, table_name, start_key,
                    end_key, row_limit, reverse_scan, selected_columns);
}

ScanIndexResult Database::ScanIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return silo::ScanIndex(table_dictionary_, schema_mutex_, table_name,
                         index_name, start_key, end_key, row_limit,
                         reverse_scan, selected_columns);
}

ScanPaxResult Database::ScanPax(const std::string_view table_name,
                                const std::string_view start_key,
                                const std::string_view end_key,
                                uint64_t row_limit, bool reverse_scan) {
  return silo::ScanPax(table_dictionary_, schema_mutex_, table_name, start_key,
                       end_key, row_limit, reverse_scan);
}

bool Database::Commit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    CommitDurability durability, std::string &abort_reason) {
  abort_reason.clear();

  // Convert input bytes before the commit protocol acquires any row locks.
  std::vector<silo::Write> write_set;
  write_set.reserve(writes.size());
  // GetPaxTable synchronizes lookup; its schema remains valid and unchanged
  // until shutdown, so decoding needs no schema lock.
  for (const auto &write : writes) {
    auto *table = GetTable(write.table_name);
    if (table == nullptr) {
      abort_reason = "write_table_missing";
      return false;
    }
    const auto *store = table->GetPaxTable();
    if (store == nullptr) {
      abort_reason = "pax_schema_missing";
      return false;
    }
    silo::Write entry{write.table_name, write.key, {}, write.op};
    if (write.op != RowOp::kDelete &&
        !pax::DecodeRow(
            store->schema(),
            reinterpret_cast<const std::byte *>(write.value.data()),
            write.value.size(), entry.value)) {
      abort_reason = "pax_row_decode_failed";
      return false;
    }
    write_set.emplace_back(std::move(entry));
  }
  const silo::CommitPayload payload{reads, write_set, secondary_index_ops,
                                    range_reads};
  return silo::Commit(table_dictionary_, schema_mutex_, epoch_framework_,
                      reaper_, logger_, payload, durability, abort_reason);
}

Table *Database::GetTable(const std::string_view table_name) const {
  return table_dictionary_.GetTable(table_name);
}

bool Database::WriteCheckpoint(uint64_t *out_version_retries) {
  wal::EpochScanCheckpoint::Stats stats;
  const bool published = scan_checkpoint_.RunOnce(&stats);
  if (out_version_retries != nullptr)
    *out_version_retries = stats.version_retries;
  return published;
}

void Database::Recover() {
  SPDLOG_INFO("Start recovery process");
  auto recovered = logger_.Recover();
  if (recovered.status != wal::Logger::RecoveryStatus::kOk) {
    SPDLOG_CRITICAL(
        "Recovery failed: the write-ahead log could not be read; refusing to "
        "start with an unknown durable state");
    exit(EXIT_FAILURE);
  }

  const EpochNumber durable_epoch = recovered.durable_epoch;
  // Refuse before stamping the durable epoch on this thread: the scanner
  // accepts a last epoch of UINT32_MAX, which is the registry's offline
  // sentinel. The epochs the records carry are checked again at the end.
  ResumeEpochAbove(durable_epoch);
  EpochNumber highest_epoch = std::max<EpochNumber>(1, durable_epoch);
  SPDLOG_DEBUG("  Durable epoch is resumed from {0}", durable_epoch);

  epoch_framework_.Join();
  epoch_framework_.SetThreadEpoch(durable_epoch);

  for (auto &entry : recovered.recovery_set) {
    // A tombstone carries an empty row and must not be re-inserted.
    const bool live = entry.index_name.empty() ? !entry.value.empty()
                                               : !entry.primary_keys.empty();
    if (!live) continue;
    auto table = GetTable(entry.table_name);
    if (table == nullptr || table->GetPaxTable() == nullptr) {
      SPDLOG_CRITICAL("Recovery failed: PAX schema for table {0} is missing.",
                      entry.table_name);
      exit(EXIT_FAILURE);
    }

    highest_epoch = std::max(highest_epoch, entry.tid.epoch);

    if (entry.index_name.empty()) {
      DataItem item(*table->GetPaxTable());
      const auto *bytes =
          reinterpret_cast<const std::byte *>(entry.value.data());
      pax::Row row;
      if (!pax::DecodeRow(table->GetPaxTable()->schema(), bytes,
                          entry.value.size(), row)) {
        SPDLOG_CRITICAL("Recovery failed: value of {} in {} does not fit PAX",
                        entry.key, entry.table_name);
        exit(EXIT_FAILURE);
      }
      if (!item.AllocateSlot()) {
        SPDLOG_CRITICAL("Recovery failed: no PAX slot for {} in {}",
                        entry.key, entry.table_name);
        exit(EXIT_FAILURE);
      }
      item.Write(row);
      item.transaction_id.store(entry.tid);
      table->GetPrimaryIndex().Put(entry.key, std::move(item));
    } else {
      // Secondary Index recovery
      index::SecondaryIndex *idx =
          table->GetOrCreateSecondaryIndex(entry.index_name, entry.index_type);
      if (idx != nullptr) {
        SPDLOG_DEBUG(
            "  Recovery: Secondary index '{0}' restoring key '{1}' with {2} "
            "primary keys",
            entry.index_name, entry.key, entry.primary_keys.size());
        DataItem item;
        item.transaction_id.store(entry.tid);
        item.SetPrimaryKeys(entry.primary_keys);
        idx->Put(entry.key, std::move(item));
      } else {
        SPDLOG_CRITICAL(
            "Recovery failed: secondary index {0} of table {1} is declared "
            "with a different constraint than the record restores",
            entry.index_name, entry.table_name);
        exit(EXIT_FAILURE);
      }
    }
  }
  epoch_framework_.Leave();

  const EpochNumber resumed_epoch = ResumeEpochAbove(highest_epoch);
  SPDLOG_DEBUG("  Global epoch is resumed from {0}", resumed_epoch);
  epoch_framework_.SetGlobalEpoch(resumed_epoch);
  SPDLOG_INFO("Finish recovery process");
}

}  // namespace helios::storage
