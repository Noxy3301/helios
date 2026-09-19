/* Copyright (c) 2004, 2021, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file ha_helios.hh

    @brief
  The Helios storage engine handler declaration.

    @note
  The handler implementation is split across the storage/helios translation
  units. The Helios storage engine implements all methods that are *required*
  to be implemented. For a full list of all methods that you can implement, see
  handler.h.

   @see
  /sql/handler.h
*/

#ifndef HA_HELIOS_H
#define HA_HELIOS_H

#include <string.h>
#include <sys/types.h>

#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "helios_field_types.h"
#include "helios_field.hh"
#include "transaction.hh"
#include "helios_proxy.hh"
#include "index_search.hh"
#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "sql/handler.h" /* handler */
#include "sql_string.h"
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */

/**
 * @brief State every open handler of one table shares: the lock, the hidden
 * key range, the row-count shards and the cached index statistics.
 */
class Helios_share : public Handler_share
{
public:
  THR_LOCK lock;
  Helios_share();
  ~Helios_share() override { thr_lock_delete(&lock); }
  // Hidden primary keys reserved from the storage server, handed out to every
  // connection of this mysqld that writes to the table. next == end means the
  // range is spent and the next row has to reserve another one.
  struct HiddenKeyRange {
    std::mutex mutex;
    uint64_t next = 0;
    uint64_t end = 0;
    // Run of the storage server that granted it; a range from an earlier run
    // may overlap one the restarted server has since handed to someone else.
    uint64_t boot_token = 0;
  };
  HiddenKeyRange hidden_keys;

  // Row-count estimate for handler::info() (sum of committed deltas in shards).
  static constexpr size_t kRowCountShards = 64; // must be power-of-two
  struct alignas(64) RowCountShard
  {
    std::atomic<int64_t> delta{0};
  };
  std::array<RowCountShard, kRowCountShards> rowcount_shards{};

  // Baseline for committed row count (currently unused; defaults to 0).
  std::atomic<uint64_t> stats_base_records{0};

  // Per-index NDV, indexed by MySQL index name. Empty name means primary key.
  // values[i] is the NDV for the first i+1 key parts.
  std::mutex index_ndv_mu_;
  std::unordered_map<std::string, std::vector<uint64_t>> index_ndv_;
  std::atomic<bool> index_ndv_loaded_{false};

  // Row count observed when index_ndv_ was fetched.
  std::atomic<uint64_t> index_ndv_records_{0};

  // ANALYZE TABLE sets this so the next NDV fetch recomputes server stats.
  std::atomic<bool> index_ndv_force_refresh_{false};

  // Per-index range histogram on the leading key part. Empty index name means
  // primary key. Guarded by index_ndv_mu_ and refreshed with NDV stats.
  struct RangeHist {
    std::vector<std::string> bounds;
    std::vector<uint64_t> cum;
  };
  std::unordered_map<std::string, RangeHist> index_hist_;
};

// LIMIT and direction that a handler range scan may push to Helios.
// row_limit=0 means keep the scan unbounded and let MySQL apply LIMIT.
struct RangeScanLimit {
  ha_rows row_limit{0};
  bool reverse_scan{false};
};

// Returns LIMIT and scan direction to push, or row_limit=0 to keep it local.
// The storage scan carries no predicate, so the LIMIT is pushed only when
// MySQL keeps no WHERE for this table above the handler.
RangeScanLimit range_scan_limit_for_order(THD *thd, const KEY *key,
                                          uint matched_prefix,
                                          bool has_mysql_only_filter);

// Enables automatic index-statistics refresh before SELECT when the row-count
// drift exceeds 20% of the larger count; the synchronous refresh can scan every
// requested index. Initial loading and ANALYZE TABLE are independent of it.
extern bool srv_stats_drift_refresh;

// Where a statement's reads come from: kReadPathRow sends one request per
// handler read, kReadPathPlan caches what it can through tx_execute_read_plan
// and sends the rest as they happen.
enum ReadPath { kReadPathRow = 0, kReadPathPlan = 1 };
extern ulong srv_read_path;

// Whether the RPC trace is written, and where; empty names a file under /tmp
// by pid.
extern bool srv_rpc_trace;
extern char *srv_rpc_trace_path;


namespace helios {

/**
 * @brief Return the THD-scoped RPC proxy shared by Helios engines.
 *
 * The primary handler owns the connection context. Secondary handlers use this
 * entry point so both engines talk to the same Helios server for a session.
 */
std::shared_ptr<HeliosProxy> acquire_shared_proxy(THD *thd);

}  // namespace helios

/** @brief
  Class definition for the storage engine
*/
class ha_helios : public handler
{
  THR_LOCK_DATA lock;           ///< MySQL lock
  Helios_share *share;       ///< Shared lock info
  Helios_share *get_share(); ///< Get the share
  HeliosProxy *get_proxy();

private:
  std::string db_table_name;
  std::string current_index_name;

  KEY *key_info;
  size_t num_keys;

  KEY_PART_INFO *key_part;
  size_t num_key_parts;
  KEY_PART_INFO indexed_key_part;

  THD *userThread;
  uint current_position_in_index_;
  std::vector<std::string> scanned_keys_;
  std::vector<std::vector<std::byte>> scanned_values_;
  // Scan buffer so the rnd_pos() re-reads after an ORDER BY sort cost no RPC.
  // Populated during rnd_next()/fill_scan_buffer(), cleared on next rnd_init().
  std::unordered_map<std::string, size_t> scan_buffer_;  // primary key -> index in scanned_values_
  std::vector<std::string> secondary_index_results_;
  std::vector<std::string> secondary_index_payloads_;

  // Entries the index cursor fetches per RPC for index_first/index_last: the
  // handler is not told whether index_next/index_prev will follow. The size
  // trades memory for RPC frequency and is not required for correctness.
  static constexpr uint64_t INDEX_CURSOR_BATCH_SIZE = 1024;
  bool index_cursor_active_{false};
  bool index_cursor_reverse_{false};
  bool index_cursor_secondary_{false};
  bool index_cursor_at_eof_{false};
  std::string index_cursor_start_key_;
  std::string index_cursor_end_key_;

  // Set when the cached index scan holds only the first rows of its range;
  // index_next/index_next_same fetch the rest from storage up to this key.
  bool index_scan_is_partial_{false};
  std::string index_scan_end_key_;

  // Per-statement memo for set_fields_from_helios, refreshed on query_id
  // change.
  uint64_t serve_memo_query_id_{0};

  // True when this statement can skip Field::store outside read_set.
  bool serve_memo_can_skip_unread_fields_{false};

  std::string last_fetched_primary_key_;

  // Duplicate-key contract for the running statement, from extra(). REPLACE
  // may overwrite the row it finds; IGNORE and ON DUPLICATE KEY UPDATE need
  // the duplicate reported at the row, so write_row reads the key first.
  bool insert_can_replace_{false};
  bool insert_peeks_duplicates_{false};
  // Key MySQL asks about through info(HA_STATUS_ERRKEY) after a duplicate.
  uint duplicate_key_index_{MAX_KEY};
  // Row estimate for the running bulk insert and the hidden keys taken against
  // it, so one reservation can cover the whole statement. 0 means no estimate.
  ha_rows bulk_insert_rows_{0};
  ha_rows bulk_insert_generated_{0};
  // Inserted keys not yet probed; MySQL's bulk bracket defers the probe to
  // end_bulk_insert, and a statement without the bracket probes at the row.
  bool bulk_insert_active_{false};
  std::vector<std::string> insert_probe_keys_;
  // Bound on one probe request, so a LOAD DATA or INSERT ... SELECT does not
  // grow one message per row of the statement.
  static constexpr size_t kInsertProbeBatch = 1024;
  // Resolve the queued keys against the storage. Returns the error for a key
  // that already holds a row, 0 when every one of them is free.
  int flush_insert_probe(HeliosTransaction *tx);
  // The error for a duplicate the storage reported: the client's to fix only
  // when every read this transaction took still holds, else a lost race.
  int duplicate_or_conflict(HeliosTransaction *tx, uint index);
  // True while the session wants UNIQUE keys resolved by the statement.
  bool checks_unique_keys();
  // Refuse a UNIQUE secondary key another row already holds: this
  // transaction's own write settles it with no request, the storage with one
  // probe. 0 when the key is free for primary_key.
  int check_unique_secondary_key(HeliosTransaction *tx, uint index,
                                 const KEY &key_info,
                                 const std::string &secondary_key,
                                 const std::string &primary_key);
  std::string write_buffer_;
  HeliosField field_pack_;
  MEM_ROOT blobroot;

  // State for buffer fetching
  size_t buffer_position_{0};
  bool scan_exhausted_{false};

  // Search plan
  IndexSearchPlan current_plan_;

  void store_primary_key_in_ref(const std::string &primary_key);
  std::string extract_primary_key_from_ref(const uchar *pos) const;
  int generate_hidden_primary_key(HeliosTransaction *tx, std::string *key);
  std::string pack_hidden_primary_key(uint64_t row_id) const;
  bool fill_scan_buffer();

  // Refill the rows buffered for index_next()/index_prev().
  bool refill_index_cursor(HeliosTransaction *tx);
  // Fetch the rest of a range whose partial scan result ran out. False when the
  // range really ended (or the transaction is gone).
  bool refill_index_scan(HeliosTransaction *tx);

  void reset_index_search_buffers();

public:
  ha_helios(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_helios() override = default;

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override { return "Helios"; }

  /**
    Replace key algorithm with one supported by SE, return the default key
    algorithm for SE if explicit key algorithm was not provided.

    @sa handler::adjust_index_algorithm().
  */
  enum ha_key_alg get_default_index_algorithm() const override
  {
    return HA_KEY_ALG_BTREE;
  }
  bool is_index_algorithm_supported(enum ha_key_alg key_alg) const override
  {
    return key_alg == HA_KEY_ALG_BTREE;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  // HA_BLOCK_CONST_TABLE keeps equality lookups out of JOIN::optimize, where
  // no AccessPath exists for the read-plan compiler, and demotes them to
  // JT_EQ_REF.
  ulonglong table_flags() const override {
    return HA_BINLOG_ROW_CAPABLE | HA_BLOCK_CONST_TABLE;
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx [[maybe_unused]], uint part [[maybe_unused]],
                    bool all_parts [[maybe_unused]]) const override
  {
    return HA_READ_RANGE | HA_READ_NEXT | HA_READ_ORDER | HA_READ_PREV;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override
  {
    return HA_MAX_REC_LENGTH;
  }

  uint max_supported_keys() const override { return 4096; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override
  {
    [[maybe_unused]] std::string s;
    return s.max_size();
  }

  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  double scan_time() override
  {
    // Fetching all matching rows costs one RPC; NDB charges records * 1000
    // (storage/ndb/plugin/ha_ndbcluster.cc:7197).
    return (double)stats.records * 10.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  double read_time(uint index, uint ranges, ha_rows rows) override
  {
    // Cost is one RPC per index lookup plus the rows it fetches;
    // batch_fetch_secondary_payloads gives PK and secondary the same shape.
    return (double)ranges * 1.0 + (double)rows * 0.5;
  }

  /**
   * @brief Helios cost model for the MySQL 8.0 Cost_estimate API.
   *
   * @details The join planner uses these methods for scan-vs-ref and
   * join-order decisions. Helios does not pay page-I/O cost like InnoDB;
   * the dominant costs are RPC round trips, transferred bytes, remote row
   * fetches, and batched remote probes.
   *
   *   scan : io = C_rpc + bytes*C_byte               ; cpu = rows*(C_row + C_remote)
   *   ref  : io = ranges*(C_rpc/batch) + bytes*C_byte ; cpu = rows*(C_probe + C_row)
   *
   * Ref probes are batched, so a nested-loop chain is charged by effective
   * batches instead of one RPC per outer row.
   */
  // The coefficients are steering values in optimizer units, not measured
  // times. One client/server round trip: 50 is about 500 row evaluations on
  // MySQL's ROW_EVALUATE_COST=0.1 scale.
  static constexpr double kCostRpc = 50.0;
  // One transferred byte: 1 KiB costs about 0.82, so wide rows weigh on the
  // choice without overwhelming the round trip.
  static constexpr double kCostByte = 0.0008;
  // One row unpacked on the MySQL side, MySQL's ROW_EVALUATE_COST baseline.
  static constexpr double kCostRow = 0.10;
  // One ref/index probe key, lighter than unpacking a full row.
  static constexpr double kCostProbe = 0.05;
  // One row scanned on the storage server, lighter than unpacking it here.
  static constexpr double kCostRemote = 0.05;
  // Effective probe batch the round trip is amortized over; a steering
  // estimate, not a protocol limit.
  static constexpr double kCostBatch = 1024.0;
  // One primary-key lookup after a non-covering secondary scan. Inflated so
  // that a large row-by-row lookup loses to a bulk full scan even when the
  // cardinality is underestimated.
  static constexpr double kCostLookup = 8.0;

  /**
   * @brief Return whether read_cost() should charge the remote row fetch.
   *
   * The default is true. It returns false for clustered primary-key access,
   * where rows are not fetched one-by-one by a secondary-key-to-PK lookup.
   */
  bool should_charge_remote_row_cost(uint index, double rows) const;

  double helios_row_bytes() const {
    // stats.mean_rec_length is set in info() from table->s->reclength (>=100).
    // Must NOT deref table->s here: TABLE is incomplete in this header.
    double b = (double)stats.mean_rec_length;
    return b > 0 ? b : 64.0;                            // floor for unknown
  }
  Cost_estimate helios_ref_cost(double ranges, double rows) const {
    Cost_estimate c;
    const double bytes = rows * helios_row_bytes();
    // per-lookup RPC amortized over the batch size.
    const double rpc = (ranges > 0 ? ranges : 1.0) * (kCostRpc / kCostBatch);
    c.add_io(rpc + bytes * kCostByte);
    c.add_cpu(rows * (kCostProbe + kCostRow));
    return c;
  }

  Cost_estimate table_scan_cost() override
  {
    Cost_estimate c;
    const double rows = (double)stats.records;
    const double bytes = rows * helios_row_bytes();
    c.add_io(kCostRpc + bytes * kCostByte);      // 1 scan RPC + transfer wait
    c.add_cpu(rows * (kCostRow + kCostRemote));  // row fetch + remote scan CPU
    return c;
  }

  Cost_estimate read_cost(uint index, double ranges, double rows) override
  {
    Cost_estimate c = helios_ref_cost(ranges, rows);

    // read_cost() is the non-covering path: the index narrows keys, then each
    // matching base row is fetched by PK. Covering scans use index_scan_cost().
    if (should_charge_remote_row_cost(index, rows)) {
      c.add_io(std::ceil(rows / kCostBatch) * kCostRpc);
      c.add_cpu(rows * kCostLookup);
    }
    return c;
  }

  Cost_estimate index_scan_cost(uint index [[maybe_unused]], double ranges,
                                double rows) override
  {
    return helios_ref_cost(ranges, rows);
  }

  /*
    Handler method declarations. Implementations are split by behavior across
    the storage/helios translation units.

    Most of these methods are not obligatory; if an override is omitted, MySQL
    treats the capability as not implemented.
  */
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def) override; // required

  int close(void) override; // required

  int change_active_index(uint keynr);

  // Optional index and DML entry points.
  int index_init(uint idx, bool sorted [[maybe_unused]]) override;
  int index_end() override;
  int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag) override;
  int index_read_last(uchar *buf, const uchar *key, uint key_len) override;
  int write_row(uchar *buf) override;
  int update_row(const uchar *old_data, uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;
  int index_next(uchar *buf) override;

  int index_next_same(uchar *buf, const uchar *key, uint key_len) override;
  int index_prev(uchar *buf) override;
  int index_first(uchar *buf) override;
  int index_last(uchar *buf) override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int rnd_init(bool scan) override; // required
  int rnd_end() override;
  int rnd_next(uchar *buf) override;            ///< required
  int rnd_pos(uchar *buf, uchar *pos) override; ///< required
  void position(const uchar *record) override;  ///< required
  /**
   * @brief Refresh table and index cardinality estimates for the optimizer.
   */
  int info(uint flag) override;                 ///< required
  /**
   * @brief Force a row-count, NDV, and histogram refresh for ANALYZE TABLE.
   */
  int analyze(THD *thd, HA_CHECK_OPT *check_opt) override;
  int extra(enum ha_extra_function operation) override;
  int reset() override;
  /**
   * @brief Send the rows of an INSERT statement and report a duplicate key.
   *
   * @details Row writes are buffered and normally reach the storage server at
   * commit. An insert that a statement must answer for is flushed here, which
   * is where MySQL still reports the failure against that statement.
   *
   * @return 0, or the handler error the statement should print.
   */
  int end_bulk_insert() override;

  /**
   * @brief Takes MySQL's row estimate for the statement about to run.
   *
   * @details Only a sizing hint: it lets one hidden-key reservation cover the
   * whole statement. 0 (LOAD DATA) means no estimate.
   */
  void start_bulk_insert(ha_rows rows) override;
  int external_lock(THD *thd, int lock_type) override; ///< required
  int start_stmt(THD *thd, thr_lock_type lock_type) override;
  int delete_all_rows(void) override;

  /**
   * @brief Estimate rows in an index range using rec_per_key and histograms.
   */
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;
  /**
   * @brief Handle MySQL's DROP TABLE cleanup hook.
   */
  int delete_table(const char *from, const dd::Table *table_def) override;
  /**
   * @brief Report that table rename is not supported.
   */
  int rename_table(const char *from, const char *to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override;
  /**
   * @brief Create the table and its secondary indexes in Helios storage.
   */
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override; ///< required

  /**
   * @brief Advertise which ADD/DROP INDEX operations this handler can perform.
   */
  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;

  /**
   * @brief Execute an accepted in-place ALTER TABLE operation.
   */
  bool inplace_alter_table(TABLE *altered_table,
                           Alter_inplace_info *ha_alter_info,
                           const dd::Table *old_table_def,
                           dd::Table *new_table_def) override;

  THR_LOCK_DATA **store_lock(
      THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type) override; ///< required

  /**
   * @brief Advertise custom batched MRR for primary-key point lookups.
   *
   * On the row read path, these methods clear HA_MRR_USE_DEFAULT_IMPL for
   * primary-key lookup ranges so multi_range_read_init() can batch all keys
   * into one Helios RPC. The plan path keeps MySQL's default DS-MRR path,
   * where the index scan cache serves the rows.
   */
  ha_rows multi_range_read_info_const(
      uint keyno, RANGE_SEQ_IF *seq, void *seq_init_param, uint n_ranges,
      uint *bufsz, uint *flags, bool *force_default_mrr,
      Cost_estimate *cost) override;
  ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                uint *bufsz, uint *flags,
                                Cost_estimate *cost) override;

  /**
   * @brief Batch full primary-key point ranges or delegate to MySQL DS-MRR.
   *
   * Only exact full-key ranges can use HeliosTransaction::batch_read().
   * Partial-key and inequality ranges fall back to the default MRR
   * implementation.
   */
  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode,
                            HANDLER_BUFFER *buf) override;

  int multi_range_read_next(char **range_info) override;
  int read_range_first(const key_range *start_key, const key_range *end_key,
                       bool eq_range_arg, bool sorted) override;
  int read_range_next() override;

private:
  // MySQL DS-MRR session object.
  DsMrr_impl m_ds_mrr;

  // Custom MRR batch state.
  struct MrrBufferedRow {
    std::string key;
    std::string value;
    char *range_info;
  };
  std::vector<MrrBufferedRow> mrr_buffer_;
  size_t mrr_buffer_pos_ = 0;
  bool mrr_use_batch_ = false;
  // True when this statement's reads come from a read plan, so
  // the handler's own batched MRR is not the path the rows arrive on.
  static bool statement_uses_read_plan(THD *thd);
  static std::string server_connection_host();
  static int server_connection_port();
  HeliosTransaction *new_transaction(THD *thd);
  HeliosTransaction *active_transaction(THD *thd) const;
  /**
   * @brief The THD's transaction, created on first handler access.
   *
   * @details The optimizer can call handler methods (index_read_map under
   * semijoin or subquery materialization) before external_lock; the
   * transaction starts at the first of those calls, as InnoDB's does.
   */
  HeliosTransaction *&
  get_transaction(THD *thd);
  /**
   * @brief Map an aborted transaction to a handler errno: cache miss to
   * non-retryable HA_ERR_UNSUPPORTED, refused duplicate key to
   * HA_ERR_FOUND_DUPP_KEY, anything else to retryable HA_ERR_LOCK_DEADLOCK.
   * @param duplicate_is_conflict Report a duplicate as a lost race instead,
   *   for a statement that already gave its row-time duplicate answer.
   */
  int abort_errno(HeliosTransaction *tx,
                  bool duplicate_is_conflict = false);

  // Key conversion helpers
  static std::string build_prefix_range_end(const std::string &prefix);
  static uint count_used_key_parts(const KEY *key_info, key_part_map keypart_map);

  /**
   * @brief Returns in buf the row of the secondary result at
   * current_position_in_index_ and advances the position.
   *
   * @return 0 with the row in buf, HA_ERR_KEY_NOT_FOUND when the position is
   *   past the result, else the handler error of the row fetch.
   */
  int fetch_and_set_current_result(uchar *buf, HeliosTransaction *tx);

  /**
   * @brief Turns one index lookup (key, keypart_map, find_flag) into
   * current_plan_: the IndexSearchOp and the packed start and end keys.
   *
   * @details The plan covers the index_read_map call and the index_next,
   * index_prev and index_next_same calls that continue it.
   */
  void build_search_plan(const uchar *key, key_part_map keypart_map,
                         enum ha_rkey_function find_flag, KEY *key_info);

  /**
   * @brief Runs current_plan_ and returns its first row in buf.
   *
   * @return 0 with the row in buf, HA_ERR_KEY_NOT_FOUND or HA_ERR_END_OF_FILE
   *   when the plan matches no row, else the handler error of the RPC.
   *   The execute_* methods below are the per-IndexSearchOp bodies with the
   *   same contract.
   */
  int execute_plan(uchar *buf, HeliosTransaction *tx);
  int execute_index_first(uchar *buf, HeliosTransaction *tx);
  int execute_unique_point(uchar *buf, HeliosTransaction *tx);
  int execute_same_key(uchar *buf, HeliosTransaction *tx);
  int execute_prefix_first(uchar *buf, HeliosTransaction *tx);
  int execute_range(uchar *buf, HeliosTransaction *tx);
  int execute_prev_key(uchar *buf, HeliosTransaction *tx);
  int execute_prefix_last(uchar *buf, HeliosTransaction *tx);
  // Fetches the primary row of every key in secondary_index_results_ with one
  // batch read into secondary_index_payloads_.
  void batch_fetch_secondary_payloads(HeliosTransaction *tx);

  std::string pack_key(const uchar *key, key_part_map keypart_map);
  /**
   * @brief Packs the field's current value in the order-preserving key
   * packing (null marker, type tag, payload) shared by every key path.
   */
  std::string pack_key_from_field(Field *field);
  std::string build_secondary_key_from_row(const uchar *row_buffer, const KEY &key_info);
  /**
   * @brief Key of the row in `buf`, reserving a hidden one from the storage
   * server when the table declares no primary key.
   *
   * @return 0 when *key holds the key, else the handler error to report.
   *   Retryable when the server could not be reached, non-retryable when it
   *   refused. `tx` is marked aborted on failure and *key is left untouched.
   */
  int extract_key(const uchar *buf, HeliosTransaction *tx, std::string *key);
  std::string extract_key_from_mysql(const uchar *row_buffer);

  /**
   * @brief Commit one CREATE INDEX backfill chunk in its own transaction.
   *
   * @details Backfill may touch every row in the table. Chunking keeps each OCC
   * transaction bounded while preserving the normal secondary-index write path.
   * `ops` is cleared before return.
   */
  bool backfill_commit_chunk(std::vector<HeliosProxy::WriteOp> &ops);

  /**
   * @brief Backfill the indexes in `specs` in one unpack pass, committed on
   * per-key-hash workers so no two of them mutate the same index entry.
   *
   * @return false when any worker commit fails; the caller fails the ALTER.
   */
  bool backfill_indexes_parallel(
      std::vector<std::pair<std::string, std::string>> &rows,
      const std::vector<std::pair<std::string, const KEY *>> &specs);

  /**
   * @brief Backfill one unique secondary index serially via the write buffer.
   *
   * @details Scans the table and commits in bounded chunks, keeping the
   * buffered commit path so the server's in-write duplicate check runs. Returns false on
   * any failure; the caller fails the ALTER.
   */
  bool backfill_unique_serial(const std::string &index_name,
                              const KEY &runtime_key);

  void set_write_buffer(uchar *buf);
  bool is_primary_key_exists();
  void set_key_and_key_part_info(const TABLE *const table);

  bool store_blob_to_field(Field **field);
  int set_fields_from_helios(uchar *buf, const std::byte *const read_buf,
                                const size_t read_buf_size);

  // rec_per_key helpers
  bool seed_row_count_from_cache(HeliosProxy *proxy);
  void load_index_stats_from_cache(HeliosProxy *proxy);
  void mark_stale_index_ndv_for_select();
  void seed_optimizer_stats();
  void set_generic_rec_per_key(KEY *key, uint key_parts, bool is_primary);

  // records_in_range helpers
  uint calculate_key_parts_from_length(KEY *key, uint key_length);
};

#endif /* HA_HELIOS_H */
