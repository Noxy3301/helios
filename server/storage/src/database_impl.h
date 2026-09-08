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
 * @file server/storage/src/database_impl.h
 * The implementation behind Database: it owns the table dictionary, the
 * epoch framework, the log and the PAX version store.
 */

#ifndef HELIOS_STORAGE_SRC_DATABASE_IMPL_H
#define HELIOS_STORAGE_SRC_DATABASE_IMPL_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "index/reaper.h"
#include "storage/config.h"
#include "storage/database.h"
#include "table/table.h"
#include "table/table_dictionary.h"
#include "util/epoch_framework.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/logger.h"

namespace helios::storage {

/**
 * @brief Everything a Database owns, behind its public face.
 *
 * @details The definitions are split across database.cc for the lifecycle,
 * the tables, the reads and the commit, pax/view.cc for the columnar view,
 * and index/stats.cc for the statistics.
 */
class Database::Impl {
 private:
  // One database per process: the constructor claims this and the destructor
  // clears it. The check is not synchronized.
  inline static Database::Impl *instance_;

  /**
   * @brief The first epoch it is safe to resume at, given a recovered
   * durability frontier.
   * @details Strictly above the frontier: a transaction joining the
   * frontier's own epoch could return a Sync acknowledgement before its
   * record was written. Terminates the process when even the resumed epoch
   * would reach the high-water mark; past it the epoch no longer orders
   * against the frontier, and resuming exactly at the mark would only abort
   * on the writer's first tick instead of failing diagnosably here.
   * @note Compared before the addition: the scanner accepts a frontier of
   * UINT32_MAX by design, and `frontier + 1` would wrap to zero, the value
   * MinEpoch skips.
   */
  static EpochNumber ResumeEpochAbove(EpochNumber frontier);

 public:
  Impl(const Config &config = Config());
  ~Impl();

  const Config &GetConfig() const;

  bool CreateTable(const std::string_view table_name);

  pax::PaxTable *GetPaxTable(const std::string_view table_name);
  Database::PaxReadView AcquirePaxView(uint32_t fence_timeout_ms);
  void ReleasePaxView(const Database::PaxReadView &view);

  bool PaxViewValid(const Database::PaxReadView &view) const;

  bool InstallPaxSchema(const std::string_view table_name,
                        const std::vector<uint32_t> &field_max_bytes,
                        const std::vector<uint8_t> &field_kind = {},
                        const std::vector<int8_t> &field_scale = {});

  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            IndexConstraint index_type);

  ReadResult Read(const std::string_view table_name, const std::string_view key,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  std::vector<ReadResult> BatchRead(
      const std::vector<std::pair<std::string, std::string>> &keys);

  ScanResult Scan(const std::string_view table_name,
                  const std::string_view start_key,
                  const std::string_view end_key, uint64_t row_limit,
                  bool reverse_scan,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  ScanIndexResult ScanIndex(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan,
      const std::vector<uint32_t> *selected_columns = nullptr);

  ScanPaxResult ScanPax(const std::string_view table_name,
                        const std::string_view start_key,
                        const std::string_view end_key, uint64_t row_limit,
                        bool reverse_scan);

  /**
   * @brief Computes exact NDV for each integer key-part prefix of one index.
   *
   * @details The query layer uses this to set MySQL `rec_per_key`. The scan
   * counts live index entries only; storage/database.h states when it
   * answers false.
   */
  bool IndexNdv(const std::string_view table_name,
                const std::string_view index_name, uint32_t num_parts,
                const KeyPartEnds &parts, std::vector<uint64_t> &out_ndv);

  /**
   * @brief Builds an equi-depth histogram for one index's leading key part.
   *
   * @details The query layer uses the returned boundaries to estimate
   * one-column range cardinality locally. The scan is independent of
   * NDV/rec_per_key: pass 1 counts row weight, and pass 2 records the
   * leading-key prefix at each bucket boundary. Secondary-index entries are
   * weighted by their primary-key list size so bucket depth tracks rows, not
   * distinct secondary keys. storage/database.h states when it answers false.
   */
  bool IndexHistogram(const std::string_view table_name,
                      const std::string_view index_name, uint32_t buckets,
                      const KeyPartEnds &parts,
                      std::vector<std::string> &out_bounds,
                      std::vector<uint64_t> &out_cum);

  bool Commit(
      const std::vector<ExternalReadEntry> &reads,
      const std::vector<ExternalWriteEntry> &writes,
      const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
      const std::vector<ExternalRangeReadEntry> &range_reads,
      CommitDurability durability, std::string *abort_reason = nullptr);

  Table *GetTable(const std::string_view table_name) const;

  bool WriteCheckpointImage(uint64_t *out_version_retries);

 private:
  // Read view expiry, half the high-water margin. Enforces the wrap-free
  // window behind plain epoch comparisons: readers gate every attempt on
  // PaxViewValid, and the cut-to-global distance grows monotonically over
  // any practical read view lifetime, keeping accepted results inside the
  // bound.
  static constexpr EpochNumber kPaxReadViewEpochLifetime =
      (std::numeric_limits<EpochNumber>::max() -
       epoch::Framework::kEpochHighWater) /
      2;

  // Builds the callback the epoch writer thread runs after each advance.
  std::function<void(EpochNumber)> MakeEpochHook();

  /**
   * @brief Replays the log into the indexes and resumes the global epoch
   *        above the recovered frontier.
   *
   * @details Terminates the process when a table cannot be created or a
   * secondary index was declared with another constraint.
   */
  void Recover();

  Config config_;
  wal::Logger logger_;
  epoch::Framework epoch_framework_;
  TableDictionary table_dictionary_;
  wal::EpochScanCheckpoint scan_checkpoint_;
  // Held shared for the whole of every read, commit and statistics call,
  // exclusive for a definition change.
  std::shared_mutex schema_mutex_;
  index::Reaper reaper_;
};

}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_DATABASE_IMPL_H
