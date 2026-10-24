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
 * @file server/storage/src/database.cc
 * Database operations and the startup and shutdown order of its epoch
 * framework, WAL, checkpoint worker and reaper.
 */

#include "helios/database.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>

#include "helios/config.h"

#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "pax/catalog.h"
#include "util/spdlog.h"
#include "wal/flush_trace.h"
#include "wal/log_entry.h"
namespace helios::storage {

Database::Database() : Database(Config()) {}

void Database::ReleaseThreadEpoch() { index::release_thread_epoch(); }

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

  // Restore column and index definitions before any recovered value is
  // installed.
  auto catalog = pax::LoadCatalog(config_.work_dir);
  if (catalog.status == pax::Catalog::Status::kUnusable) {
    SPDLOG_CRITICAL("Cannot recover the PAX schema catalog: {}",
                    catalog.detail);
    exit(EXIT_FAILURE);
  }
  next_index_id_ = catalog.next_index_id;
  for (auto &[name, definition] : catalog.entries) {
    CreateTable(name);
    Table *table = GetTable(name);
    if (!table->InstallPaxSchema(std::move(definition.schema))) {
      SPDLOG_CRITICAL("Cannot restore PAX schema for table {}", name);
      exit(EXIT_FAILURE);
    }
    // Declare the indexes before replay: one with no surviving record has to
    // carry its constraint too.
    for (const auto &index : definition.indexes) {
      if (!table->CreateSecondaryIndex(index.name, index.constraint, index.id,
                                       index.pk_slices)) {
        SPDLOG_CRITICAL("Cannot restore secondary index {} of table {}",
                        index.name, name);
        exit(EXIT_FAILURE);
      }
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

  // The replay enrolled this thread in the Masstree epoch; leaving it lets
  // the nodes the replay retired be reclaimed.
  index::release_thread_epoch();

  // Abort the process if the running logger cannot persist its records.
  logger_.SetFailStop();

  // Initialise optional WAL timing before the logger can record events.
  wal::FlushTrace::Instance();

  // Checkpointing needs both WAL persistence and epoch advancement running.
  logger_.Start(&epoch_framework_);
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
  // The epoch thread calls this with the global epoch it just published.
  const auto interval = std::chrono::milliseconds(config_.epoch_duration_ms);
  return [this, interval, last_tick = std::chrono::steady_clock::time_point()](
             const EpochNumber global_epoch) mutable {
    // Workers lag E by at most one epoch, so none remain in epochs <= E - 2.
    // Flushing and reclamation have different roles but currently share this
    // bound. Commit epochs are positive, so E - 2 must be at least 1.
    if (global_epoch >= 3) {
      const EpochNumber closed_epoch = global_epoch - 2;
      logger_.RequestFlush(closed_epoch);
      reaper_.Purge(closed_epoch);
    }

    // Tick masstree's epoch, which paces only RCU frees of retired leaves,
    // index records and UNIQUE key blocks, at most once per epoch duration
    // however often requests advance E; workers leave via ReleaseThreadEpoch.
    const auto now = std::chrono::steady_clock::now();
    if (now - last_tick >= interval) {
      last_tick = now;
      index::advance_epoch();
    }
  };
}

bool Database::CreateTable(const std::string_view table_name) {
  return table_dictionary_.CreateTable(table_name);
}

bool Database::CreateSecondaryIndex(const std::string_view table_name,
                                    const std::string_view index_name,
                                    IndexConstraint index_type,
                                    std::vector<uint32_t> pk_slices) {
  // A value the wire carried that is neither of the declared ones names a
  // promise this storage does not know how to keep.
  if (index_type != IndexConstraint::kNone &&
      index_type != IndexConstraint::kUnique) {
    return false;
  }
  // Only a non-unique record's key holds slices, each an offset and a length.
  if (!pk_slices.empty() &&
      (index_type == IndexConstraint::kUnique || pk_slices.size() % 2 != 0)) {
    return false;
  }
  // An empty index name marks a primary record in the WAL
  if (index_name.empty()) return false;
  // Serialized with the other definition changes.
  std::lock_guard<std::mutex> lk(ddl_mutex_);
  Table *table = GetTable(table_name);
  if (table == nullptr) return false;
  // A query node that declares an index the storage already holds gets the
  // answer the declaration deserves: the same constraint, or a refusal.
  if (auto *existing = table->GetSecondaryIndex(index_name)) {
    return existing->constraint == index_type &&
           existing->pk_slices == pk_slices;
  }
  // The catalog holds the index before it takes a write, since recovery
  // replays only the writes of indexes the catalog holds.
  const uint32_t id = next_index_id_;
  auto entries = CatalogSnapshot();
  if (auto it = entries.find(table->Name()); it != entries.end()) {
    it->second.indexes.push_back(
        {std::string(index_name), index_type, id, pk_slices});
  }
  if (!pax::StoreCatalog(config_.work_dir, entries, id + 1)) return false;
  next_index_id_ = id + 1;
  return table->CreateSecondaryIndex(index_name, index_type, id,
                                     std::move(pk_slices));
}

bool Database::DropSecondaryIndexes(
    const std::string_view table_name,
    const std::vector<std::string> &index_names) {
  std::vector<std::unique_ptr<Table::IndexNode>> dropped;
  {
    std::lock_guard<std::mutex> lk(ddl_mutex_);
    Table *table = GetTable(table_name);
    if (table == nullptr) return false;
    // The catalog loses every named index before lookups stop finding them.
    auto entries = CatalogSnapshot();
    if (auto it = entries.find(table->Name()); it != entries.end()) {
      auto &indexes = it->second.indexes;
      indexes.erase(std::remove_if(indexes.begin(), indexes.end(),
                                   [&index_names](const auto &index) {
                                     return std::count(index_names.begin(),
                                                       index_names.end(),
                                                       index.name) != 0;
                                   }),
                    indexes.end());
    }
    if (!pax::StoreCatalog(config_.work_dir, entries, next_index_id_))
      return false;
    for (const auto &name : index_names) {
      if (auto node = table->UnlinkSecondaryIndex(name))
        dropped.push_back(std::move(node));
    }
  }
  if (dropped.empty()) return true;

  // A thread finds an index only inside its Masstree epoch, and a commit
  // queues its tombstones before leaving it.
  index::synchronize_rcu();
  for (auto &node : dropped) {
    reaper_.Forget(node->index.tree);
    node->index.tree.Destroy();
  }
  return true;
}

Table *Database::GetTable(const std::string_view table_name) const {
  return table_dictionary_.GetTable(table_name);
}

pax::CatalogEntries Database::CatalogSnapshot() {
  pax::CatalogEntries entries;
  table_dictionary_.ForEachTable([&entries](Table &table) {
    const auto *store = table.GetPaxTable();
    if (store == nullptr) return;
    auto &definition = entries[table.Name()];
    definition.schema = store->schema();
    definition.indexes = table.IndexDefinitions();
  });
  return entries;
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

  for (auto &entry : recovered.recovery_entries) {
    // A delete is logged with absent set and must not be re-inserted; a
    // secondary entry survives recovery only with inserts.
    if (entry.tid.absent) continue;
    auto table = GetTable(entry.table_name);
    if (table == nullptr || table->GetPaxTable() == nullptr) {
      SPDLOG_CRITICAL("Recovery failed: PAX schema for table {0} is missing.",
                      entry.table_name);
      exit(EXIT_FAILURE);
    }

    highest_epoch = std::max<EpochNumber>(highest_epoch, entry.tid.epoch);

    if (entry.index_name.empty()) {
      const auto *bytes =
          reinterpret_cast<const std::byte *>(entry.value.data());
      pax::Row row;
      if (!pax::unpack_row(table->GetPaxTable()->schema(), bytes,
                          entry.value.size(), row)) {
        SPDLOG_CRITICAL("Recovery failed: value of {} in {} does not fit PAX",
                        entry.key, entry.table_name);
        exit(EXIT_FAILURE);
      }
      auto *item =
          static_cast<RowRecord *>(table->GetPrimaryIndex().Put(entry.key));
      if (!item->AllocateSlot(*table->GetPaxTable())) {
        SPDLOG_CRITICAL("Recovery failed: no PAX slot for {} in {}", entry.key,
                        entry.table_name);
        exit(EXIT_FAILURE);
      }
      item->InstallRow(row, entry.tid.epoch);
      item->transaction_id.store(entry.tid);
    } else {
      // An index dropped since, or an earlier one of the same name, has an id
      // the catalog no longer holds; its writes are skipped.
      index::SecondaryIndex *idx = table->GetSecondaryIndex(entry.index_name);
      if (idx != nullptr && idx->id == entry.index_id) {
        SPDLOG_DEBUG(
            "  Recovery: Secondary index '{0}' restoring key '{1}' with {2} "
            "primary keys",
            entry.index_name, entry.key, entry.primary_keys.size());
        if (idx->constraint == IndexConstraint::kUnique) {
          // A commit never leaves a UNIQUE key with two primary keys.
          if (entry.primary_keys.size() != 1) {
            SPDLOG_CRITICAL(
                "Recovery failed: UNIQUE key {0} of index {1} restores {2} "
                "primary keys",
                entry.key, entry.index_name, entry.primary_keys.size());
            exit(EXIT_FAILURE);
          }
          auto *item = static_cast<UniqueRecord *>(idx->tree.Put(entry.key));
          item->transaction_id.store(entry.tid);
          item->set_primary_key(entry.primary_keys.front());
        } else {
          // A non-unique index keeps one record per
          // (secondary key, primary key), under join_entry of the two, with
          // the length its key adds past the secondary key.
          for (const auto &primary_key : entry.primary_keys) {
            const std::string key =
                index::join_entry(*idx, entry.key, primary_key);
            auto *record = static_cast<NonUniqueRecord *>(idx->tree.Put(key));
            record->transaction_id.store(entry.tid);
            record->set_pk_len(key.size() - entry.key.size());
          }
        }
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
