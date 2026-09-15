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
 * @file server/storage/include/lineairdb/database.h
 * The public face of the store: table and index definition, the read ops,
 * the commit attempt, and the epoch handshake every calling thread performs.
 */

#ifndef HELIOS_STORAGE_INCLUDE_LINEAIRDB_DATABASE_H
#define HELIOS_STORAGE_INCLUDE_LINEAIRDB_DATABASE_H

#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lineairdb/transaction.h"
#include "lineairdb/config.h"
#include "lineairdb/index.h"
#include "lineairdb/read.h"

#include "index/reaper.h"
#include "silo/tidword.h"
#include "table/table_dictionary.h"
#include "util/epoch_framework.h"
#include "util/thread_key_storage.h"
#include "wal/epoch_scan_checkpoint.h"
#include "wal/logger.h"

namespace helios::storage {

/**
 * @brief One storage instance: its tables, its epoch framework and its log.
 *
 * @details The server creates one instance and shares it between requests.
 * @note Multiple concurrent instances are unsupported: index reclamation
 * and PAX read-view state are shared within the process.
 */
class Database {
 public:
  /**
   * @brief Constructs a database under a default-constructed Config.
   *
   * @throws std::system_error as the Config constructor does.
   */
  Database();

  /**
   * @brief Constructs a database under `config`.
   *
   * @param config See Config for more details of configuration.
   * @throws std::system_error when the working directory cannot be opened,
   * which includes another process already holding the log's exclusive lock.
   * The caller decides what to do about it; a noexcept constructor would
   * terminate instead.
   */
  Database(const Config &config);

  ~Database() noexcept;
  Database(const Database &) = delete;
  Database &operator=(const Database &) = delete;
  Database(Database &&) = delete;
  Database &operator=(Database &&) = delete;

  const Config &GetConfig() const noexcept;

  /**
   * @brief Ends the calling thread's index reclamation section and drops it
   *        from the participant set the index reclaims against.
   *
   * @details This is the index's reclamation epoch, not the commit epoch and
   * not a read view's snapshot_epoch. No row reference obtained inside the
   * section may be used past this call: ending a section is a release, after
   * which another thread's delete can free those rows. The next store op on the
   * thread opens a new section; there is no separate begin call.
   */
  void ReleaseThreadEpoch();

  /**
   * @brief Declares a secondary index on a table.
   *
   * @return false when the table does not exist, an index of that name
   * already exists, the name is empty, or `index_type` is not one of the
   * declared values.
   */
  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            IndexConstraint index_type);

  bool HasTable(const std::string_view table_name);

  /**
   * @brief Creates a new table.
   * @param[in] table_name The name of the table to create.
   * @details InstallPaxSchema must succeed before values can be written.
   * @return true when a new table is created (the name was not previously
   * used).
   * @return false when no table is created because the table name already
   * exists.
   */
  bool CreateTable(const std::string_view table_name);

  /**
   * @brief Saves a PAX schema and enables it before the first write.
   *
   * @details Installs the per-field maximum cell widths: index 0 is the row
   * format's null-flags field, followed by one entry per column in field order.
   * Values are stored in per-column strips. A value that does not fit or
   * cannot be represented exactly is rejected before any write is installed.
   * Call after CreateTable and before loading rows. The schema is restored
   * at startup, even when value replay is disabled. Repeating the same
   * definition succeeds; changing an existing definition is refused.
   *
   * @param[in] table_name The table that should use PAX storage.
   * @param[in] field_max_bytes Maximum packed bytes for each row field.
   * @param[in] field_type Per-field storage type (see pax::FieldType). Empty
   * leaves every field untyped, as does a length that does not match
   * `field_max_bytes`, or a width that is not the one that type stores.
   * @param[in] field_scale Per-field DECIMAL scale, used by kDecimal64. Empty,
   * or a length that does not match, means a scale of zero.
   * @return true when the schema is installed for the table.
   * @return false if the table is missing, the definition is empty or
   * conflicts, or the catalog cannot be saved.
   */
  bool InstallPaxSchema(const std::string_view table_name,
                        const std::vector<uint32_t> &field_max_bytes,
                        const std::vector<pax::FieldType> &field_type = {},
                        const std::vector<int8_t> &field_scale = {});

  /**
   * @brief Returns the PAX table installed for `table_name`.
   *
   * @param table_name Target table.
   * @return A pointer this database owns, valid as long as it is, or nullptr
   * when the table is missing or has no PAX schema.
   */
  pax::PaxTable *GetPaxTable(const std::string_view table_name);

  /**
   * @brief Handle for one columnar read view.
   *
   * @details snapshot_epoch `se` is the read view's serialization point:
   * commits with `epoch <= se` are visible; later ones resolve to before-images.
   * token must be passed back to ReleasePaxView exactly once.
   */
  struct PaxReadView {
    bool valid = false;
    uint32_t snapshot_epoch = 0;
    uint64_t token = 0;
    std::string error;  // rejection reason when !valid
  };

  /**
   * @brief Arms capture and waits for global epoch `E >= se + 2`.
   *
   * @details On return every commit through snapshot_epoch `se` has finished
   * installing, and every later commit captures the rows it overwrites or
   * invalidates the read view. The calling thread must not hold an epoch (it
   * must be outside any transaction). Fails instead of falling back on
   * fence timeout, near the epoch high-water mark, or when the capture fails
   * during acquisition.
   *
   * @param fence_timeout_ms Upper bound on the fence wait.
   * @return A valid handle, or an invalid one carrying the reason.
   */
  PaxReadView AcquirePaxView(uint32_t fence_timeout_ms);

  /**
   * @brief Releases a read view; the last active release clears the undo
   * maps. Safe to call with an invalid handle (no-op).
   */
  void ReleasePaxView(const PaxReadView &view);

  /**
   * @brief Returns whether this read view's results may be used.
   *
   * @details False for an invalid view, for one whose generation's capture
   * failed, and for one that outlived its epoch-lifetime bound. Callers
   * gate every result on this before accepting it.
   */
  bool PaxViewValid(const PaxReadView &view) const;

  /**
   * @brief Reads one row without opening a server-side transaction.
   *
   * Looks the key up in the primary index of `table_name` and returns the
   * current value together with the TID word observed at read time. The
   * caller should later pass the same TID back to silo::Transaction::Read so
   * that the commit can confirm the row was not modified concurrently.
   *
   * @param table_name Target table.
   * @param key Primary key to look up.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident rows. Null selects the whole row; an empty list selects
   * no data columns. Unselected PAX fields become empty markers; null flags
   * remain in every result.
   * @return Result with `found` set when the key exists and was non-empty.
   *         When the table does not exist, `found` is false and `tid` is 0.
   */
  ReadResult Read(const std::string_view table_name, const std::string_view key,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  /**
   * @brief Reads several rows in one call.
   *
   * Each `keys[i] = {table_name, key}` is resolved with the same protocol as
   * Read. Reads do not share state, so this is purely a transport
   * optimization on top of repeated Read calls.
   *
   * @param keys (table_name, key) pairs to look up.
   * @return One ReadResult per input, in the same order.
   */
  std::vector<ReadResult> BatchRead(
      const std::vector<std::pair<std::string, std::string>> &keys);

  /**
   * @brief Range-scans the primary index and returns the rows observed in
   *        the range.
   *
   * Each returned row carries its own TID. Revalidating the range at commit
   * is a silo::Transaction::RangeRead call with this call's arguments and the
   * returned keys, plus one silo::Transaction::Read call per consumed row.
   *
   * @param table_name Target table.
   * @param start_key Inclusive start of the range.
   * @param end_key   Exclusive end of the range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate from `end_key` toward `start_key`.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident rows. Null selects the whole row; an empty list selects
   * no data columns. Unselected PAX fields become empty markers; null flags
   * remain in every result.
   * @return Result with `ok == false` if the table is missing or `end_key`
   *         is empty. Callers should treat `!ok` as an abort signal.
   */
  ScanResult Scan(const std::string_view table_name,
                  const std::string_view start_key,
                  const std::string_view end_key, uint64_t row_limit,
                  bool reverse_scan,
                  const std::vector<uint32_t> *selected_columns = nullptr);

  /**
   * @brief Range-scans a secondary index and resolves each hit to its base
   *        row.
   *
   * For every secondary key in `[start_key, end_key)`, this resolves each of
   * its primary keys, reads the base row, and reports
   * `{secondary_key, primary_key, value, tid}` per result. Revalidating the
   * range at commit is a silo::Transaction::RangeRead call with this call's
   * arguments and both returned key lists; each base row carries its TID for
   * revalidation as a point read.
   *
   * @param table_name Base table.
   * @param index_name Secondary index name.
   * @param start_key Inclusive start of the secondary range.
   * @param end_key Exclusive end of the secondary range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate in reverse secondary-key order.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident base rows, with the same null/empty rules as Read.
   * Unselected PAX columns are returned as empty fields.
   * @return Result with `ok == false` if the table or the index is missing,
   *         or `end_key` is empty.
   */
  ScanIndexResult ScanIndex(
      const std::string_view table_name, const std::string_view index_name,
      const std::string_view start_key, const std::string_view end_key,
      uint64_t row_limit, bool reverse_scan,
      const std::vector<uint32_t> *selected_columns = nullptr);

  /**
   * @brief Range-scans the primary index and returns PAX cell references.
   *
   * @details The returned rows are not materialized.
   *
   * @param table_name Target table.
   * @param start_key Inclusive start of the range.
   * @param end_key Exclusive end of the range. Must be non-empty.
   * @param row_limit Maximum live rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate in reverse key order.
   * @return Result with `ok == false` when the table is missing, its PAX
   * schema is not installed, or `end_key` is empty.
   */
  ScanPaxResult ScanPax(const std::string_view table_name,
                        const std::string_view start_key,
                        const std::string_view end_key, uint64_t row_limit,
                        bool reverse_scan);

  /**
   * @brief Reports where the leading key parts of `key` end.
   *
   * @details The statistics count prefixes and compare bounds as bytes, so
   * the key layout stays with the caller: it fills `ends[p]` with the offset
   * just past key part `p`, for `p` in `[0, num_parts)`.
   *
   * @return false to leave this key out of the statistics, which is how a
   * caller refuses a part whose bytes do not order like its values.
   */
  using KeyPartEnds = std::function<bool(std::string_view key,
                                         uint32_t num_parts, size_t *ends)>;

  /**
   * @brief Computes per-key-part-prefix NDV for one index.
   *
   * `out_ndv[d]` is the number of distinct prefixes covering key parts
   * `0..d` among live entries. `index_name == ""` selects the primary index.
   * Returns false when the table/index is missing or `parts` refused a
   * scanned key, leaving the caller to use its existing heuristic.
   */
  bool IndexNdv(const std::string_view table_name,
                const std::string_view index_name, uint32_t num_parts,
                const KeyPartEnds &parts, std::vector<uint64_t> &out_ndv);

  /**
   * @brief Builds an equi-height histogram for one index's leading key part.
   *
   * @details `out_bounds[i]` is the raw packed leading-key prefix for a
   * bucket boundary, in ascending order. `out_cum[i]` is the cumulative row
   * count up to that boundary and is monotone; the last value is the total
   * counted rows. `index_name == ""` selects the primary index. Returns false
   * when the table/index is missing, the index is empty, or `parts` refused
   * a scanned key. Only the leading part is asked for.
   */
  bool IndexHistogram(const std::string_view table_name,
                      const std::string_view index_name, uint32_t buckets,
                      const KeyPartEnds &parts,
                      std::vector<std::string> &out_bounds,
                      std::vector<uint64_t> &out_cum);

  /**
   * @brief Writes one checkpoint of the live rows, on the calling thread.
   *
   * Does what the configured checkpoint interval does, at a moment the caller
   * chooses. The caller must not be inside a transaction: the scan waits for
   * the epoch to advance past every transaction in flight, and its own would
   * hold that open.
   *
   * @param out_version_retries Optional out parameter, set to how many times a
   *        row had to be read again because a writer held it or changed it
   *        during the copy.
   * @return true when a checkpoint was published.
   * @return false when no checkpoint was published, or when the final
   *         directory sync failed after the atomic rename, in which case the
   *         new checkpoint is in place but its publication is not yet durable.
   */
  bool WriteCheckpoint(uint64_t *out_version_retries = nullptr);

 private:
  // Reads the tables, epoch framework, reaper, logger and last-TID slots.
  friend class silo::Transaction;

  // Declared in dependency order
  Config config_;
  wal::Logger logger_;
  epoch::Framework epoch_framework_;
  TableDictionary table_dictionary_;
  wal::EpochScanCheckpoint scan_checkpoint_;
  // Serializes definition changes: index creation and the catalog rewrite
  std::mutex ddl_mutex_;
  index::Reaper reaper_;
  // Each worker remembers the last TID it chose for this database.
  ThreadKeyStorage<Tidword> last_commit_tids_;

  // Bound the lifetime of a read view so `E - se` stays in the wrap-free window.
  static constexpr EpochNumber kPaxReadViewEpochLifetime =
      (std::numeric_limits<EpochNumber>::max() -
       epoch::Framework::kEpochHighWater) /
      2;

  Table *GetTable(std::string_view table_name) const;

  /**
   * @brief Chooses an epoch strictly above the recovered durable epoch.
   * @note Refuses startup if that epoch would reach the high-water mark.
   * Compares before adding, since the recovered epoch may be UINT32_MAX.
   */
  static EpochNumber ResumeEpochAbove(EpochNumber durable_epoch);

  // Builds the callback the epoch writer runs after each advance.
  std::function<void(EpochNumber)> MakeEpochHook();

  /**
   * @brief Replays WAL records into the indexes and resumes the global epoch.
   * @note Refuses startup if a table or secondary index cannot be restored.
   */
  void Recover();
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_LINEAIRDB_DATABASE_H
