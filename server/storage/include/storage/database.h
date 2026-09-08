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
 * @file server/storage/include/storage/database.h
 * The public face of the store: table and index definition, the read ops,
 * the commit, and the epoch handshake every calling thread performs.
 */

#ifndef HELIOS_STORAGE_INCLUDE_STORAGE_DATABASE_H
#define HELIOS_STORAGE_INCLUDE_STORAGE_DATABASE_H

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "storage/commit.h"
#include "storage/config.h"
#include "storage/index.h"
#include "storage/read.h"

namespace helios::storage {

namespace pax {
class PaxTable;
enum class FieldType : uint8_t;
}  // namespace pax

/**
 * @brief One storage instance: its tables, its epoch framework and its log.
 *
 * @details One per process. Constructing a second one ends the process, as
 * does a log the constructor cannot read or replay; the working directory is
 * the one failure the caller is given a chance to handle. Every method below
 * is safe to call from several threads at once.
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
   * not a read view's cut_epoch. No row reference obtained inside the section
   * may be used past this call: ending a section is a release, after which
   * another thread's delete can free those rows. The next store op on the
   * thread opens a new section; there is no separate begin call.
   */
  void ReleaseThreadEpoch();

  /**
   * @brief Declares a secondary index on a table.
   *
   * @return false when the table does not exist, an index of that name
   * already exists, or `index_type` is not one of the declared values.
   */
  bool CreateSecondaryIndex(const std::string_view table_name,
                            const std::string_view index_name,
                            IndexConstraint index_type);

  bool HasTable(const std::string_view table_name);

  /**
   * @brief Creates a new table.
   * @param[in] table_name The name of the table to create.
   * @return true when a new table is created (the name was not previously
   * used).
   * @return false when no table is created because the table name already
   * exists.
   */
  bool CreateTable(const std::string_view table_name);

  /**
   * @brief Enables PAX storage for rows created after the call.
   *
   * @details Installs the per-field maximum cell widths: index 0 is the row
   * format's null-flags field, followed by one entry per column in field order.
   * Rows written after installation are stored in per-column strips when they
   * fit the configured cell widths; oversize rows overflow to the heap.
   * Call once after CreateTable and before loading rows.
   *
   * @param[in] table_name The table that should use PAX storage.
   * @param[in] field_max_bytes Maximum packed bytes for each row field.
   * @param[in] field_type Per-field storage type (see pax::FieldType). Empty
   * leaves every field untyped, as does a length that does not match
   * `field_max_bytes`, or a width that is not the one that type stores.
   * @param[in] field_scale Per-field DECIMAL scale, used by kDecimal64. Empty,
   * or a length that does not match, means a scale of zero.
   * @return true when the schema is installed for the table.
   * @return false when the schema is empty, the table is missing, or a
   * schema is already installed.
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
   * @details `cut_epoch` is the read view's serialization point: commits
   * with epoch <= cut are visible, later ones resolve to before-images.
   * `token` must be passed back to ReleasePaxView exactly once.
   */
  struct PaxReadView {
    bool valid = false;
    uint32_t cut_epoch = 0;
    uint64_t token = 0;
    std::string error;  // rejection reason when !valid
  };

  /**
   * @brief Arms before-image capture and fences the epoch so `cut_epoch`
   * is a sound serialization point.
   *
   * @details On return every commit with epoch <= cut_epoch has finished
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

  // ----------------------------------------------------------------------
  // Reads, scans and the commit.
  //
  // The methods below hold no state between calls. Each returns the snapshot
  // the caller needs (value, packed TID) so that the caller can keep its own
  // read set across independent RPCs. The collected snapshot is replayed
  // through Commit when the logical transaction is ready to
  // commit.
  // See @ref read.h for the supporting types.
  // ----------------------------------------------------------------------

  /**
   * @brief Reads one row without opening a server-side transaction.
   *
   * Looks the key up in the primary index of `table_name` and returns the
   * current value together with the packed TID observed at read time. The
   * caller should later pass the same TID back inside an ExternalReadEntry
   * so that Commit can confirm the row was not modified
   * concurrently.
   *
   * @param table_name Target table.
   * @param key Primary key to look up.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident rows. Unselected PAX columns are returned as empty fields.
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
   * takes an ExternalRangeReadEntry built from this call's arguments and the
   * returned keys, plus one ExternalReadEntry per consumed row.
   *
   * @param table_name Target table.
   * @param start_key Inclusive start of the range.
   * @param end_key   Exclusive end of the range. Must be non-empty.
   * @param row_limit Maximum rows to return. 0 means no cap.
   * @param reverse_scan When true, iterate from `end_key` toward `start_key`.
   * @param selected_columns Optional zero-based MySQL columns to materialize
   * for PAX-resident rows. Unselected PAX columns are returned as empty fields.
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
   * range at commit takes an ExternalRangeReadEntry built from this call's
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
   * for PAX-resident base rows. Unselected PAX columns are returned as empty
   * fields.
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
   * @return Result with `ok == false` when the table is missing, has no PAX
   * table, holds rows that overflowed to the heap, or `end_key` is empty. The
   * caller then uses Scan.
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
   * @brief Validates caller-supplied read and write sets and installs the
   *        writes atomically.
   *
   * Runs the Silo commit protocol against external inputs: resolve each
   * key to its record, lock the write set, validate the reads (point
   * TIDs, range replays, UNIQUE rechecks), then install the writes,
   * append the log set, and unlock with a new TID. The full contract
   * lives with silo::Commit.
   *
   * Aborts return false. The optional `abort_reason` is set to a short
   * machine-readable label such as `exact_read_tid_moved`,
   * `primary_range_result_changed`, or `unique_si_exists_after_lock`.
   *
   * @param reads Point reads to revalidate before commit.
   * @param writes Row writes; `op` says whether the entry updates, inserts
   *                or deletes the row.
   * @param secondary_index_ops Secondary-index adds/removes to install.
   * @param range_reads Range reads assembled by the caller from earlier
   *                    scans.
   * @param durability When this commit is acknowledged, relative to its record
   *                    reaching the device.
   * @param abort_reason Optional out parameter. Set only when the function
   *                    returns false.
   * @return true on commit; false on a validation failure, or when a table
   *         or index the entries name is missing.
   */
  bool Commit(
      const std::vector<ExternalReadEntry> &reads,
      const std::vector<ExternalWriteEntry> &writes,
      const std::vector<ExternalSecondaryIndexEntry> &secondary_index_ops,
      const std::vector<ExternalRangeReadEntry> &range_reads,
      CommitDurability durability, std::string *abort_reason = nullptr);

  /**
   * @brief Writes one image of the live rows, on the calling thread.
   *
   * Does what the configured checkpoint interval does, at a moment the caller
   * chooses. The caller must not be inside a transaction: the scan waits for
   * the epoch to advance past every transaction in flight, and its own would
   * hold that open.
   *
   * @param out_version_retries Optional out parameter, set to how many times a
   *        row had to be read again because a writer held it or changed it
   *        during the copy.
   * @return true when an image was published.
   * @return false when no image was published, or when the final directory
   *         sync failed after the atomic rename, in which case the new image
   *         is in place but its publication is not yet durable.
   */
  bool WriteCheckpointImage(uint64_t *out_version_retries = nullptr);

  class Impl;

 private:
  const std::unique_ptr<Impl> db_pimpl_;
};
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_STORAGE_DATABASE_H
