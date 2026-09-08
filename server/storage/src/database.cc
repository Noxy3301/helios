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
 * Database forwarding to its implementation, and the startup and shutdown
 * order of the epoch framework, the log and the reaper.
 */

#include "storage/database.h"

#include <algorithm>
#include <cassert>
#include <memory>

#include "database_impl.h"
#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "silo/commit.h"
#include "silo/read.h"
#include "silo/snapshot.h"
#include "storage/config.h"
#include "util/spdlog.h"
#include "wal/flush_trace.h"
namespace helios::storage {

Database::Database() : db_pimpl_(std::make_unique<Impl>()) {
  helios::storage::util::InitDebugLog();
}
Database::Database(const Config &config)
    : db_pimpl_(std::make_unique<Impl>(config)) {
  helios::storage::util::InitDebugLog();
}

Database::~Database() noexcept = default;

const Config &Database::GetConfig() const noexcept {
  return db_pimpl_->GetConfig();
}

void Database::ReleaseThreadEpoch() { index::MasstreeReleaseThreadEpoch(); }
bool Database::CreateTable(const std::string_view table_name) {
  return db_pimpl_->CreateTable(table_name);
}

bool Database::InstallPaxSchema(const std::string_view table_name,
                                const std::vector<uint32_t> &field_max_bytes,
                                const std::vector<uint8_t> &field_kind,
                                const std::vector<int8_t> &field_scale) {
  return db_pimpl_->InstallPaxSchema(table_name, field_max_bytes, field_kind,
                                     field_scale);
}

pax::PaxTable *Database::GetPaxTable(const std::string_view table_name) {
  return db_pimpl_->GetPaxTable(table_name);
}

Database::PaxReadView Database::AcquirePaxView(uint32_t fence_timeout_ms) {
  return db_pimpl_->AcquirePaxView(fence_timeout_ms);
}

void Database::ReleasePaxView(const PaxReadView &view) {
  db_pimpl_->ReleasePaxView(view);
}

bool Database::PaxViewValid(const PaxReadView &view) const {
  return db_pimpl_->PaxViewValid(view);
}

bool Database::CreateSecondaryIndex(const std::string_view table_name,
                                    const std::string_view index_name,
                                    IndexConstraint index_type) {
  return db_pimpl_->CreateSecondaryIndex(table_name, index_name, index_type);
}

bool Database::HasTable(const std::string_view table_name) {
  return db_pimpl_->GetTable(table_name) != nullptr;
}

ReadResult Database::Read(const std::string_view table_name,
                          const std::string_view key,
                          const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->Read(table_name, key, selected_columns);
}

std::vector<ReadResult> Database::BatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return db_pimpl_->BatchRead(keys);
}

ScanResult Database::Scan(const std::string_view table_name,
                          const std::string_view start_key,
                          const std::string_view end_key, uint64_t row_limit,
                          bool reverse_scan,
                          const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->Scan(table_name, start_key, end_key, row_limit,
                         reverse_scan, selected_columns);
}

ScanIndexResult Database::ScanIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return db_pimpl_->ScanIndex(table_name, index_name, start_key, end_key,
                              row_limit, reverse_scan, selected_columns);
}

ScanPaxResult Database::ScanPax(const std::string_view table_name,
                                const std::string_view start_key,
                                const std::string_view end_key,
                                uint64_t row_limit, bool reverse_scan) {
  return db_pimpl_->ScanPax(table_name, start_key, end_key, row_limit,
                            reverse_scan);
}

bool Database::IndexNdv(const std::string_view table_name,
                        const std::string_view index_name, uint32_t num_parts,
                        const KeyPartEnds &parts,
                        std::vector<uint64_t> &out_ndv) {
  return db_pimpl_->IndexNdv(table_name, index_name, num_parts, parts, out_ndv);
}

bool Database::IndexHistogram(const std::string_view table_name,
                              const std::string_view index_name,
                              uint32_t buckets, const KeyPartEnds &parts,
                              std::vector<std::string> &out_bounds,
                              std::vector<uint64_t> &out_cum) {
  return db_pimpl_->IndexHistogram(table_name, index_name, buckets, parts,
                                   out_bounds, out_cum);
}

bool Database::Commit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    CommitDurability durability, std::string *abort_reason) {
  return db_pimpl_->Commit(reads, writes, secondary_index_ops, range_reads,
                           durability, abort_reason);
}

bool Database::WriteCheckpointImage(uint64_t *out_version_retries) {
  return db_pimpl_->WriteCheckpointImage(out_version_retries);
}

EpochNumber Database::Impl::ResumeEpochAbove(EpochNumber frontier) {
  if (frontier >= epoch::Framework::kEpochHighWater - 1) {
    SPDLOG_CRITICAL(
        "Startup failed: resuming above the recovered epoch {0} would reach "
        "the epoch high-water mark {1}",
        frontier, epoch::Framework::kEpochHighWater);
    exit(EXIT_FAILURE);
  }
  return frontier + 1;
}

Database::Impl::Impl(const Config &config)
    : config_(config),
      logger_(config_),
      epoch_framework_(config_.epoch_duration_ms, MakeEpochHook()),
      scan_checkpoint_(config_, table_dictionary_, epoch_framework_, logger_) {
  if (Database::Impl::instance_ == nullptr) {
    Database::Impl::instance_ = this;
    SPDLOG_INFO("Storage instance has been constructed.");
  } else {
    SPDLOG_ERROR(
        "It is prohibited to allocate two helios::storage::Database instance "
        "at "
        "the same time.");
    exit(EXIT_FAILURE);
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
    if (scanned.frontier != 0) {
      // Records exist that this instance will not replay; resuming at or
      // below their epoch would let a Sync commit inherit their frontier.
      epoch_framework_.SetGlobalEpoch(ResumeEpochAbove(scanned.frontier));
    }
  }
  // Armed after recovery, which reports its own failures by refusing to
  // start, and before the flusher that can raise one at run time.
  logger_.SetFailStop();
  // Built before any thread records, so its storage and its dump signal are
  // in place rather than raised by whichever path happens to reach it first.
  wal::FlushTrace::Instance();
  logger_.StartFlusher();
  epoch_framework_.Start();
  // Last: its scan waits on the epoch it starts.
  scan_checkpoint_.Start();
}

Database::Impl::~Impl() {
  epoch_framework_.Sync();
  // Before the epoch writer stops, since a capture in progress waits for the
  // epoch to advance.
  scan_checkpoint_.Stop();
  epoch_framework_.Stop();
  // After the epoch writer has joined no further closed epoch arrives, so the
  // flusher can drain what it already owns and be joined before the log is
  // closed.
  logger_.StopFlusher();
  SPDLOG_DEBUG(
      "Epoch number and Durable epoch number are ended at {0}, and {1}, "
      "respectively.",
      epoch_framework_.GetGlobalEpoch(), logger_.GetDurableEpoch());
  // Written once every thread that records has joined, so the census reaches
  // the filesystem off the commit and flush paths.
  wal::FlushTrace::Instance().Dump();
  SPDLOG_INFO("Storage instance has been destructed.");
  assert(Database::Impl::instance_ == this);
  Database::Impl::instance_ = nullptr;
}

const Config &Database::Impl::GetConfig() const { return config_; }

std::function<void(EpochNumber)> Database::Impl::MakeEpochHook() {
  return [&](EpochNumber updated_epoch) {
    // Logging. The global epoch advances from U-1 to U while threads may
    // still be online in U-1, so U-2 is the newest epoch that is certainly
    // closed and safe to write. Handing the target to the logger's own
    // flusher keeps the durability fdatasync off this pool.
    // updated_epoch - 2 is the newest epoch no thread can still be in: a
    // joiner that read E keeps the global epoch below E + 2 (Framework::Join).
    if (updated_epoch >= 3) {
      logger_.ScheduleFlush(updated_epoch - 2);
    }

    // Physically purge the tombstones whose grace epoch has passed.
    reaper_.Reap(updated_epoch);

    // Tick masstree's globalepoch so RCU can free retired leaves and
    // DataItem limbo once min_active_epoch() catches up. Workers release
    // their epoch at RPC boundaries through ReleaseThreadEpoch; this call
    // only moves active_epoch.
    index::MasstreeAdvanceEpoch();
  };
}

bool Database::Impl::CreateTable(const std::string_view table_name) {
  return table_dictionary_.CreateTable(table_name);
}

bool Database::Impl::CreateSecondaryIndex(const std::string_view table_name,
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

ReadResult Database::Impl::Read(const std::string_view table_name,
                                const std::string_view key,
                                const std::vector<uint32_t> *selected_columns) {
  return silo::Read(table_dictionary_, schema_mutex_, table_name, key,
                    selected_columns);
}

std::vector<ReadResult> Database::Impl::BatchRead(
    const std::vector<std::pair<std::string, std::string>> &keys) {
  return silo::BatchRead(table_dictionary_, schema_mutex_, keys);
}

ScanResult Database::Impl::Scan(const std::string_view table_name,
                                const std::string_view start_key,
                                const std::string_view end_key,
                                uint64_t row_limit, bool reverse_scan,
                                const std::vector<uint32_t> *selected_columns) {
  return silo::Scan(table_dictionary_, schema_mutex_, table_name, start_key,
                    end_key, row_limit, reverse_scan, selected_columns);
}

ScanIndexResult Database::Impl::ScanIndex(
    const std::string_view table_name, const std::string_view index_name,
    const std::string_view start_key, const std::string_view end_key,
    uint64_t row_limit, bool reverse_scan,
    const std::vector<uint32_t> *selected_columns) {
  return silo::ScanIndex(table_dictionary_, schema_mutex_, table_name,
                         index_name, start_key, end_key, row_limit,
                         reverse_scan, selected_columns);
}

ScanPaxResult Database::Impl::ScanPax(const std::string_view table_name,
                                      const std::string_view start_key,
                                      const std::string_view end_key,
                                      uint64_t row_limit, bool reverse_scan) {
  return silo::ScanPax(table_dictionary_, schema_mutex_, table_name, start_key,
                       end_key, row_limit, reverse_scan);
}

bool Database::Impl::Commit(
    const std::vector<ExternalReadEntry> &reads,
    const std::vector<ExternalWriteEntry> &writes,
    const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
    const std::vector<ExternalRangeReadEntry> &range_reads,
    CommitDurability durability, std::string *abort_reason) {
  const silo::CommitPayload payload{reads, writes, secondary_index_ops,
                                    range_reads};
  return silo::Commit(table_dictionary_, schema_mutex_, epoch_framework_,
                      reaper_, logger_, payload, durability, abort_reason);
}

Table *Database::Impl::GetTable(const std::string_view table_name) const {
  return table_dictionary_.GetTable(table_name);
}

bool Database::Impl::WriteCheckpointImage(uint64_t *out_version_retries) {
  wal::EpochScanCheckpoint::Stats stats;
  const bool published = scan_checkpoint_.RunOnce(&stats);
  if (out_version_retries != nullptr)
    *out_version_retries = stats.version_retries;
  return published;
}

void Database::Impl::Recover() {
  SPDLOG_INFO("Start recovery process");
  auto recovered = logger_.Recover();
  if (recovered.status != wal::Logger::RecoveryStatus::kOk) {
    SPDLOG_CRITICAL(
        "Recovery failed: the write-ahead log could not be read; refusing to "
        "start with an unknown durable state");
    exit(EXIT_FAILURE);
  }

  const EpochNumber durable_epoch = recovered.frontier;
  // Refuse before stamping the frontier on this thread: the scanner accepts a
  // frontier of UINT32_MAX, which is the registry's offline sentinel. The
  // epochs the records carry are checked again at the end.
  ResumeEpochAbove(durable_epoch);
  EpochNumber highest_epoch = std::max<EpochNumber>(1, durable_epoch);
  SPDLOG_DEBUG("  Durable epoch is resumed from {0}", durable_epoch);

  epoch_framework_.Join();
  epoch_framework_.SetThreadEpoch(durable_epoch);

  for (auto &entry : recovered.recovery_set) {
    // A tombstone carries an empty row and must not be re-inserted.
    const bool live = entry.index_name.empty() ? entry.data_item_copy.HasRow()
                                               : entry.data_item_copy.IsLive();
    if (!live) continue;
    CreateTable(entry.table_name);
    auto table = GetTable(entry.table_name);
    if (table == nullptr) {
      SPDLOG_CRITICAL(
          "Recovery failed: Table {0} could not be found or created.",
          entry.table_name);
      exit(EXIT_FAILURE);
    }

    highest_epoch = std::max(highest_epoch,
                             entry.data_item_copy.transaction_id.load().epoch);

    if (entry.index_name.empty()) {
      table->GetPrimaryIndex().Put(entry.key, std::move(entry.data_item_copy));
    } else {
      // Secondary Index recovery
      index::SecondaryIndex *idx =
          table->GetOrCreateSecondaryIndex(entry.index_name, entry.index_type);
      if (idx != nullptr) {
        SPDLOG_DEBUG(
            "  Recovery: Secondary index '{0}' restoring key '{1}' with {2} "
            "primary keys",
            entry.index_name, entry.key,
            entry.data_item_copy.primary_keys_view().size());
        idx->Put(entry.key, std::move(entry.data_item_copy));
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
