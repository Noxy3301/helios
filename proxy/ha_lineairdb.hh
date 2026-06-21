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

/** @file ha_lineairdb.h

    @brief
  The ha_lineairdb engine is a stubbed storage engine for lineairdb purposes
  only; it does nothing at this point. Its purpose is to provide a source code
  illustration of how to begin writing new storage engines; see also
  /storage/lineairdb/ha_lineairdb.cc.

    @note
  Please read ha_lineairdb.cc before reading this file.
  Reminder: The lineairdb storage engine implements all methods that are
  *required* to be implemented. For a full list of all methods that you can
  implement, see handler.h.

   @see
  /sql/handler.h and /storage/lineairdb/ha_lineairdb.cc
*/

#ifndef HA_LINEAIRDB_H
#define HA_LINEAIRDB_H

#include <string.h>
#include <sys/types.h>

#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "lineairdb_field_types.h"
#include "lineairdb_field.hh"
#include "lineairdb_transaction.hh"
#include "lineairdb_proxy.hh"
#include "lineairdb_index_search.hh"
#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "sql/handler.h" /* handler */
#include "sql_string.h"
#include "thr_lock.h" /* THR_LOCK, THR_LOCK_DATA */

/** @brief
  LineairDB_share is a class that will be shared among all open handlers.
  This lineairdb implements the minimum of what you will probably need.
*/
class LineairDB_share : public Handler_share
{
public:
  THR_LOCK lock;
  LineairDB_share();
  ~LineairDB_share() override { thr_lock_delete(&lock); }
  std::atomic<uint64_t> next_hidden_pk{0};

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
};

// LIMIT and direction that a handler range scan may push to LineairDB.
// row_limit=0 means keep the scan unbounded and let MySQL apply LIMIT.
struct RangeScanLimit {
  ha_rows row_limit{0};
  bool reverse_scan{false};
};

// Returns LIMIT and scan direction to push, or row_limit=0 to keep it local.
// Safe only when the server scan sees every WHERE predicate that can affect
// which rows belong before the LIMIT.
RangeScanLimit range_scan_limit_for_order(THD *thd, const KEY *key,
                                          uint matched_prefix,
                                          bool has_mysql_only_filter);

/** @brief
  Class definition for the storage engine
*/
class ha_lineairdb : public handler
{
  THR_LOCK_DATA lock;           ///< MySQL lock
  LineairDB_share *share;       ///< Shared lock info
  LineairDB_share *get_share(); ///< Get the share
  LineairDBProxy *get_proxy();

private:
  std::string db_table_name;
  std::string current_index_name;

  KEY *key_info;
  size_t num_keys;
  ha_base_keytype primary_key_type;

  KEY_PART_INFO *key_part;
  size_t num_key_parts;
  KEY_PART_INFO indexed_key_part;

  THD *userThread;
  uint current_position_in_index_;
  std::vector<std::string> scanned_keys_;
  std::vector<std::vector<std::byte>> scanned_values_;
  // Local row cache, serving the same role as InnoDB's Buffer Pool for re-reads.
  // When MySQL sorts results (ORDER BY), it scans all rows via rnd_next(), then
  // re-reads them in sorted order via rnd_pos(). In InnoDB, the second read hits
  // the Buffer Pool (in-memory page cache) so it's nearly free. Since we access
  // LineairDB via RPC, there is no such cache — without this, every rnd_pos()
  // would trigger a network round-trip.
  // Populated during rnd_next()/fetch_next_batch(), cleared on next rnd_init().
  std::unordered_map<std::string, size_t> scan_cache_;  // primary key -> index in scanned_values_
  std::vector<std::string> secondary_index_results_;
  std::vector<std::string> secondary_index_payloads_;
  // Set by get_matching_keys_and_values_in_range() when these materialized
  // rows are only a LIMIT-staged window. index_next()/index_next_same() consume
  // it and abort on over-read instead of returning a false EOF.
  bool materialized_scan_truncated_{false};
  // Per-statement memo for set_fields_from_lineairdb.
  // Refreshed when query_id or projection registration epoch changes.
  uint64_t serve_memo_query_id_{0};
  uint64_t serve_memo_proj_epoch_{0};

  // Projection layout for the table currently being decoded; nullptr means
  // rows are full-width.
  const std::vector<uint32_t> *serve_memo_projection_{nullptr};

  // True when this statement can skip Field::store outside read_set.
  bool serve_memo_can_skip_unread_fields_{false};

  std::string last_fetched_primary_key_;
  std::string end_range_exclusive_key_; // For HA_READ_BEFORE_KEY: exclude this key from results
  my_off_t
      current_position_; /* Current position in the file during a file scan */
  std::string write_buffer_;
  LineairDBField ldbField;
  MEM_ROOT blobroot;

  // State for buffer fetching
  static constexpr size_t SCAN_BATCH_SIZE = 100;
  size_t buffer_position_{0};
  std::string last_batch_key_;
  bool scan_exhausted_{false};

  // Search plan
  IndexSearchPlan current_plan_;

  void store_primary_key_in_ref(const std::string &primary_key);
  std::string extract_primary_key_from_ref(const uchar *pos) const;
  bool uses_hidden_primary_key() const;
  std::string generate_hidden_primary_key();
  std::string serialize_hidden_primary_key(uint64_t row_id) const;
  bool fetch_next_batch();
  void reset_index_search_buffers();

public:
  ha_lineairdb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_lineairdb() override = default;

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override { return "LineairDB"; }

  /**
   * @brief Advertise the MySQL engine-pushdown hook.
   *
   * The hook still checks the query shape before installing the aggregate
   * executor override; unsupported queries keep the normal iterator path.
   */
  const handlerton *hton_supporting_engine_pushdown() override;

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
  // HA_BLOCK_CONST_TABLE keeps the optimizer from const-folding equality
  // lookups on this engine. A const-table read happens during JOIN::optimize
  // (read_const), before the QEP root_access_path exists, so the statement-
  // scoped autogen prefetch hook cannot see it and would issue a per-row RPC
  // or abort. Blocking const-table demotes such lookups to JT_EQ_REF, moving
  // the read into the execution phase where autogen batches it into one RPC.
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
    // Unlike InnoDB which advances a cursor one row at a time,
    // ha_lineairdb materializes all matching rows in a single Scan RPC.
    // High cost relative to read_time discourages full scan over index access.
    // NOTE: NDB uses records * 1000 (see storage/ndb/plugin/ha_ndbcluster.cc:7197).
    return (double)stats.records * 10.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  double read_time(uint index, uint ranges, ha_rows rows) override
  {
    // ranges: number of index lookups, each requires at least 1 RPC.
    // rows: estimated row count to materialize and transfer.
    // Same formula for PK and secondary indexes because
    // batch_fetch_secondary_payloads batches all primary row lookups
    // into a single RPC, making per-row cost similar.
    return (double)ranges * 1.0 + (double)rows * 0.5;
  }

  /**
   * @brief Helios cost model for the MySQL 8.0 Cost_estimate API.
   *
   * @details The join planner uses these methods for scan-vs-ref and
   * join-order decisions. LineairDB does not pay page-I/O cost like InnoDB;
   * the dominant costs are RPC round trips, transferred bytes, row
   * materialization, and batched remote probes.
   *
   *   scan : io = C_rpc + bytes*C_byte               ; cpu = rows*(C_row + C_remote)
   *   ref  : io = ranges*(C_rpc/batch) + bytes*C_byte ; cpu = rows*(C_probe + C_row)
   *
   * Ref probes are batched, so a nested-loop chain is charged by effective
   * batches instead of one RPC per outer row.
   *
   * @note Defaults are optimizer units, not wall-clock time. Later calibration
   * can fit them with NNLS (non-negative least squares):
   *
   *   measured_time ~= C_rpc*rpc_count + C_byte*bytes + C_row*rows + ...
   *
   * NNLS keeps every coefficient >= 0, so more RPCs/bytes/rows cannot make the
   * fitted cost cheaper.
   */
  static double helios_cost_param(const char *name, double def) {
    // Optional calibration knob; the Helios cost model itself is always on.
    const char *e = std::getenv(name);
    if (!e || !e[0]) return def;
    char *end = nullptr;
    double v = strtod(e, &end);
    return (end && end != e) ? v : def;
  }
  // C_rpc: one client/server round trip. 50.0 means one RPC costs about
  // 500 row evaluations on MySQL's ROW_EVALUATE_COST=0.1 scale.
  static double kC_rpc() {
    static const double v = helios_cost_param("HELIOS_C_RPC", 50.0);
    return v;
  }

  // C_byte: one transferred byte. 0.0008 makes 1 KiB cost about 0.82, so
  // wide rows affect plan choice without overwhelming the fixed RPC cost.
  static double kC_byte() {
    static const double v = helios_cost_param("HELIOS_C_BYTE", 0.0008);
    return v;
  }

  // C_row: one row materialized and decoded on the MySQL/proxy side. 0.10
  // matches MySQL's ROW_EVALUATE_COST baseline for one row.
  static double kC_row() {
    static const double v = helios_cost_param("HELIOS_C_ROW", 0.10);
    return v;
  }

  // C_probe: one ref/index probe key before row materialization. 0.05 treats
  // probe handling as lighter than decoding a full row.
  static double kC_probe() {
    static const double v = helios_cost_param("HELIOS_C_PROBE", 0.05);
    return v;
  }

  // C_remote: one row scanned on the LineairDB server side. 0.05 models remote
  // scan work as lighter than decoding a full row on the MySQL side.
  static double kC_remote() {
    static const double v = helios_cost_param("HELIOS_C_REMOTE", 0.05);
    return v;
  }

  // B_eff: effective probe batch size used to amortize RPC cost. 1024 is a
  // steering estimate, not a protocol limit.
  static double kEffBatch() {
    static const double v = helios_cost_param("HELIOS_BATCH", 1024.0);
    return v;
  }

  // C_materialise: one remote PK row fetch after a non-covering secondary scan.
  // 8.0 is an inflated steering value: it makes large row-by-row materialization
  // lose to bulk full-scan+prefetch even when cardinality is underestimated.
  static double kC_materialise() {
    static const double v =
        helios_cost_param("HELIOS_C_MATERIALISE", 8.0);
    return v;
  }

  /**
   * @brief Return whether read_cost() should charge remote row materialization.
   *
   * The default is true. It returns false for clustered primary-key access and
   * for bulk-prefetched grouped single-table SELECTs, where rows are not fetched
   * one-by-one by a secondary-key-to-PK lookup.
   */
  bool helios_charge_materialise(uint index) const;

  double helios_row_bytes() const {
    // stats.mean_rec_length is set in info() from table->s->reclength (>=100).
    // Must NOT deref table->s here: TABLE is incomplete in this header.
    double b = (double)stats.mean_rec_length;
    return b > 0 ? b : 64.0;                            // floor for unknown
  }
  Cost_estimate helios_ref_cost(double ranges, double rows) const {
    Cost_estimate c;
    const double bytes = rows * helios_row_bytes();
    // per-lookup RPC amortized over the prefetch batch size.
    const double rpc = (ranges > 0 ? ranges : 1.0) * (kC_rpc() / kEffBatch());
    c.add_io(rpc + bytes * kC_byte());
    c.add_cpu(rows * (kC_probe() + kC_row()));
    return c;
  }

  Cost_estimate table_scan_cost() override
  {
    Cost_estimate c;
    const double rows = (double)stats.records;
    const double bytes = rows * helios_row_bytes();
    c.add_io(kC_rpc() + bytes * kC_byte());      // 1 scan RPC + transfer wait
    c.add_cpu(rows * (kC_row() + kC_remote()));  // materialize + remote scan CPU
    return c;
  }

  Cost_estimate read_cost(uint index [[maybe_unused]], double ranges,
                          double rows) override
  {
    Cost_estimate c = helios_ref_cost(ranges, rows);

    // read_cost() is the non-covering path: the index narrows keys, then each
    // matching base row is fetched by PK. Covering scans use index_scan_cost().
    if (helios_charge_materialise(index)) {
      c.add_io(std::ceil(rows / kEffBatch()) * kC_rpc());
      c.add_cpu(rows * kC_materialise());
    }
    return c;
  }

  Cost_estimate index_scan_cost(uint index [[maybe_unused]], double ranges,
                                double rows) override
  {
    return helios_ref_cost(ranges, rows);
  }

  /*
    Everything below are methods that we implement in ha_lineairdb.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_lineairdb.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def) override; // required

  /** @brief
    We implement this in ha_lineairdb.cc; it's a required method.
  */
  int close(void) override; // required

  int change_active_index(uint keynr);

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_init(uint idx, bool sorted [[maybe_unused]]) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_end() override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read(uchar *buf, const uchar *key, uint key_len, enum ha_rkey_function find_flag) override;
  int index_read_last(uchar *buf, const uchar *key, uint key_len) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf) override;

  int index_next_same(uchar *buf, const uchar *key, uint key_len) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf) override;

  /** @brief
    We implement this in ha_lineairdb.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
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
  /**
   * @brief Return the next cached aggregate group row as raw VALUE bytes.
   */
  bool agg_next_raw(std::string_view *out_value);

  /**
   * @brief Attach a serialized AggregateSpec to this handler's transaction.
   *
   * @return false when the statement WHERE exists but cannot be serialized for
   * server-side aggregation.
   */
  bool tx_set_pushed_aggregate(const std::string &s);

  /**
   * @brief Clear this transaction's pushed AggregateSpec.
   */
  void tx_clear_pushed_aggregate();

  /**
   * @brief Return whether server aggregation may use read-only no-validation.
   */
  bool tx_ro_novalidate();

  /**
   * @brief Return true if aggregate staging aborted.
   */
  bool tx_is_aborted();

  int rnd_pos(uchar *buf, uchar *pos) override; ///< required
  void position(const uchar *record) override;  ///< required
  int info(uint flag) override;                 ///< required
  int analyze(THD *thd, HA_CHECK_OPT *check_opt) override;
  int extra(enum ha_extra_function operation) override;
  int external_lock(THD *thd, int lock_type) override; ///< required
  int start_stmt(THD *thd, thr_lock_type lock_type) override;
  int delete_all_rows(void) override;

  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;
  int delete_table(const char *from, const dd::Table *table_def) override;
  int rename_table(const char *from, const char *to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override; ///< required

  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;

  bool inplace_alter_table(TABLE *altered_table,
                           Alter_inplace_info *ha_alter_info,
                           const dd::Table *old_table_def,
                           dd::Table *new_table_def) override;

  THR_LOCK_DATA **store_lock(
      THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type) override; ///< required

  ha_rows multi_range_read_info_const(uint keyno, RANGE_SEQ_IF *seq,
                                      void *seq_init_param, uint n_ranges,
                                      uint *bufsz, uint *flags, bool *force_default_mrr,
                                      Cost_estimate *cost) override;
  ha_rows multi_range_read_info(uint keyno, uint n_ranges, uint keys,
                                uint *bufsz, uint *flags,
                                Cost_estimate *cost) override;
  int multi_range_read_init(RANGE_SEQ_IF *seq, void *seq_init_param,
                            uint n_ranges, uint mode,
                            HANDLER_BUFFER *buf) override;

  int multi_range_read_next(char **range_info) override;
  int read_range_first(const key_range *start_key, const key_range *end_key,
                       bool eq_range_arg, bool sorted) override;
  int read_range_next() override;

  /** Predicate pushdown: serialize WHERE conditions for server-side filtering */
  const Item *cond_push(const Item *cond) override;

  /** Table-local filter from cond_push(), for autogen scan-step pushdown. */
  const std::string &pushed_filter_for_autogen() const {
    return pushed_filter_serialized_;
  }

private:
  // Serialized PushedPredicate protobuf from cond_push()
  std::string pushed_filter_serialized_;
  // True when cond_push() cannot build a server-side filter
  bool has_unpushed_filter_{false};
  /** The multi range read session object */
  DsMrr_impl m_ds_mrr;

  // MRR batch state
  struct MrrBufferedRow {
    std::string value;
    char *range_info;
  };
  std::vector<MrrBufferedRow> mrr_buffer_;
  size_t mrr_buffer_pos_ = 0;
  bool mrr_use_batch_ = false;
  LineairDBTransaction *&
  get_transaction(THD *thd);
  /**
   * @brief Map an aborted transaction to a handler errno: HA_ERR_UNSUPPORTED
   * (non-retryable) for a prefetch cache miss, or HA_ERR_LOCK_DEADLOCK (MySQL's
   * conventional retry signal) for a genuine OCC abort.
   */
  int abort_errno(LineairDBTransaction *tx);

  // Key conversion helpers
  static std::string encode_int_key(const uchar *data, size_t len);
  static std::string encode_datetime_key(const uchar *data, size_t len,
                                          enum_field_types mysql_type);
  static std::string encode_string_key(const uchar *data, size_t len);

  static unsigned char key_part_type_tag(LineairDBFieldType type);
  static void append_key_part_encoding(std::string &out, bool is_null,
                                       LineairDBFieldType type,
                                       const std::string &payload);
  static std::string build_prefix_range_end(const std::string &prefix);
  static uint count_used_key_parts(const KEY *key_info, key_part_map keypart_map);
  int fetch_and_set_current_result(uchar *buf, LineairDBTransaction *tx);

  // Phase 2: Building the search plan
  void build_search_plan(const uchar *key, key_part_map keypart_map,
                         enum ha_rkey_function find_flag, KEY *key_info);

  // Phase 3: Executing the search plan
  int execute_plan(uchar *buf, LineairDBTransaction *tx);
  int execute_index_first(uchar *buf, LineairDBTransaction *tx);
  int execute_unique_point(uchar *buf, LineairDBTransaction *tx);
  int execute_same_key_materialize(uchar *buf, LineairDBTransaction *tx);
  int execute_prefix_first(uchar *buf, LineairDBTransaction *tx);
  int execute_range_materialize(uchar *buf, LineairDBTransaction *tx);
  int execute_prev_key(uchar *buf, LineairDBTransaction *tx);
  int execute_prefix_last(uchar *buf, LineairDBTransaction *tx);
  void batch_fetch_secondary_payloads(LineairDBTransaction *tx);

  std::string convert_key_to_ldbformat(const uchar *key, key_part_map keypart_map);
  std::string serialize_key_from_field(Field *field);
  std::string build_secondary_key_from_row(const uchar *row_buffer, const KEY &key_info);
  std::string extract_key(const uchar *buf);
  std::string autogenerate_key();
  std::string extract_key_from_mysql(const uchar *row_buffer);

  void set_write_buffer(uchar *buf);
  bool is_primary_key_exists();
  bool is_primary_key_type_int();
  void set_key_and_key_part_info(const TABLE *const table);

  bool store_blob_to_field(Field **field);
  int set_fields_from_lineairdb(uchar *buf, const std::byte *const read_buf,
                                const size_t read_buf_size);

  // rec_per_key helpers
  bool seed_row_count_from_cache(LineairDBProxy *proxy);
  void load_index_ndv_from_cache(LineairDBProxy *proxy);
  void mark_stale_index_ndv_for_select();
  void seed_optimizer_stats();
  void set_generic_rec_per_key(KEY *key, uint key_parts, bool is_primary);

  // records_in_range helpers
  uint calculate_key_parts_from_length(KEY *key, uint key_length);
};

#endif /* HA_LINEAIRDB_H */
