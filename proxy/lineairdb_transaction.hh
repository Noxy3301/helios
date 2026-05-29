#ifndef LINEAIRDB_TRANSACTION_HH
#define LINEAIRDB_TRANSACTION_HH

#include <cstdlib>
#include <ctime>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "sql/handler.h" /* handler */
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "lineairdb_proxy.hh"
#include "rpc_trace.hh"

class LineairDB_share;

/**
 * @brief 
 * Wrapper of LineairDB::Transaction
 * Takes care of registering a transaction to MySQL core
 * 
 * Lifetime of this class equals the lifetime of the transaction.
 * The instance of this class is deleted in end_transaction.
 * Set the pointer to this class to nullptr after end_transaction
 * to indicate that LineairDBTransaction is terminated.
 */
class LineairDBTransaction
{
public:
  std::string get_selected_table_name();
  void choose_table(std::string db_table_name);
  bool table_is_not_chosen();

  const std::pair<const std::byte *const, const size_t> read(std::string key);
  std::vector<std::pair<bool, std::string>> batch_read(const std::vector<std::string>& keys);
  bool batch_write(const std::string& table_name,
                   const std::vector<LineairDBProxy::BatchOp>& ops);
  std::vector<std::string> get_all_keys();
  std::vector<std::string> get_matching_keys(std::string key);
  std::vector<std::string> get_matching_keys_in_range(std::string start_key, std::string end_key);
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_in_range(
      std::string start_key, std::string end_key, uint64_t row_limit = 0,
      bool reverse_scan = false);
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_from_prefix(
      std::string prefix);

  // Phase-7 Step3 (borrowed-span serve): the helios analog of InnoDB's fetch
  // cache + compact-record pointer access. A full-table primary scan is served
  // DIRECTLY from its prefetch range entry (no slice copy, no per-row
  // scanned_keys_/scanned_values_/scan_cache_ materialization). OCC obligations
  // for the whole range are recorded once at borrow time.
  struct BorrowedScan {
    bool ok = false;
    size_t entry_idx = 0;  // index into local_range_scans_ (append-only: stable)
    size_t count = 0;      // number of rows in the borrowed entry
  };
  // Borrow the full-table ("" .. max-sentinel) primary scan for direct serving,
  // recording its OCC obligations once. Returns ok=false (caller falls back to
  // the copy path) unless ALL preconditions hold: oneshot mode, a TRUE
  // full-cover unfiltered unlimited forward entry with row_tids 1:1, and NO
  // pending writes for the table (own-write merge can't reflect into a borrow).
  BorrowedScan borrow_fullcover_pk_scan();
  // Accessors into a borrowed entry's row. Valid until the transaction ends;
  // the no-pending-write gate forbids cache mutation during the scan, and
  // local_range_scans_ is append-only so entry_idx stays valid. Return nullptr
  // on out-of-range / not-found.
  const std::string* borrowed_value(const BorrowedScan& h, size_t pos) const;
  const std::string* borrowed_key(const BorrowedScan& h, size_t pos) const;
  // rnd_pos: binary-search the borrowed entry's sorted rows for an exact PK.
  const std::string* borrowed_value_for_key(const BorrowedScan& h,
                                            const std::string& pk) const;

  bool write(std::string key, const std::string value);
  bool write_secondary_index(std::string index_name, std::string secondary_key, const std::string primary_key);
  std::vector<std::string> read_secondary_index(std::string index_name, std::string secondary_key);
  std::vector<std::string> get_matching_primary_keys_in_range(
      std::string index_name, std::string start_key, std::string end_key);
  std::vector<std::string> get_matching_primary_keys_from_prefix(
      std::string index_name, std::string prefix);
  std::optional<std::string> fetch_last_key_in_range(
      const std::string &start_key, const std::string &end_key);
  std::optional<std::string> fetch_last_primary_key_in_secondary_range(
      const std::string &index_name, const std::string &start_key,
      const std::string &end_key);
  std::optional<SecondaryIndexEntry> fetch_last_secondary_entry_in_range(
      const std::string &index_name, const std::string &start_key,
      const std::string &end_key);
  std::optional<std::string> fetch_first_key_with_prefix(
      const std::string &prefix, const std::string &prefix_end);
  std::optional<std::string> fetch_next_key_with_prefix(
      const std::string &last_key, const std::string &prefix_end);
  bool update_secondary_index(
      std::string index_name,
      std::string old_secondary_key,
      std::string new_secondary_key,
      const std::string primary_key);
  bool delete_value(std::string key);
  bool delete_secondary_index(std::string index_name, std::string secondary_key, const std::string primary_key);

  // Write buffering for batch operations
  void buffer_write(const std::string& table_name,
                    const std::string& key, const std::string& value);
  void buffer_write_secondary_index(const std::string& table_name,
                                     const std::string& index_name,
                                     const std::string& secondary_key,
                                     const std::string& primary_key);
  void buffer_delete(const std::string& table_name,
                     const std::string& key);
  void buffer_delete_secondary_index(const std::string& table_name,
                                     const std::string& index_name,
                                     const std::string& secondary_key,
                                     const std::string& primary_key);
  // Flush buffered row/index ops before reads can observe the same table.
  // Must be called before read/scan RPCs to ensure read-your-own-writes.
  bool flush_write_buffer();
  bool flush_write_buffer_for_table(const std::string& table_name);

  void begin_transaction();
  void set_status_to_abort();
  bool end_transaction();
  void fence() const;
  void set_oneshot_mode(bool enabled) { oneshot_mode_ = enabled; }
  bool is_oneshot_mode() const { return oneshot_mode_; }
  // Phase-3e: the QEP-based plan can only be derived after the optimizer runs
  // (i.e. at the first rnd_init/index_init, not at external_lock/begin). This
  // flag makes that lazy auto-generation fire exactly once per statement.
  bool oneshot_plan_resolved() const { return oneshot_plan_resolved_; }
  void set_oneshot_plan_resolved(bool v) { oneshot_plan_resolved_ = v; }
  void prefetch_stateless_reads(
      const std::vector<LineairDBProxy::StatelessReadKey>& reads);
  void execute_read_plan(const std::vector<LineairDBProxy::ReadPlanStep>& steps);

  // Deferred-execution path (Phase-3b): stage parsed steps so the plan runs
  // at the FIRST scan/read call inside this tx — by which time the per-table
  // pushed_filter_ from cond_push has been propagated to the tx. Required for
  // TPC-H style queries where the WHERE predicate must reach the server-side
  // S: scan; without it the plan over-fetches and is catastrophically slow.
  void stage_oneshot_plan(std::vector<LineairDBProxy::ReadPlanStep> steps);
  void execute_pending_oneshot_plan();

  inline bool is_not_started() const {
    if (oneshot_mode_) return !oneshot_registered_;
    if (tx_id == -1) return true;
    return false;
  }

  inline int64_t get_tx_id() const {
    return tx_id;
  }

  inline bool is_aborted() const { 
    return is_aborted_;
  }

  inline void set_aborted(bool aborted) {
    // Once aborted, stay aborted (matches LineairDB's irreversible Abort semantics).
    // Prevents subsequent RPC responses from accidentally clearing the flag.
    if (aborted) is_aborted_ = true;
  }

  inline bool is_a_single_statement() const { return !isTransaction; }

  // Predicate pushdown: serialized PushedPredicate protobuf
  void set_pushed_filter(const std::string& s) { pushed_filter_ = s; }
  const std::string& get_pushed_filter() const { return pushed_filter_; }
  void clear_pushed_filter() { pushed_filter_.clear(); }

  void add_rowcount_delta(LineairDB_share *share, const std::string &table_name, int64_t delta);
  int64_t peek_rowcount_delta(const LineairDB_share *share) const;

  // RPC trace statement boundary; TxRpcTrace dedupes repeated SQL strings.
  void on_stmt_boundary(const std::string& sql) { rpc_trace_.on_stmt(sql); }
  bool fallback_to_normal_transaction(const char* reason);

  // helios Phase-6 range-hash OCC. Set true by ha_lineairdb for a read-only
  // SELECT when HELIOS_RANGEHASH_OCC is on. When eligible, cache-served rows
  // from a FULL-COVER entry (start_key=="") skip per-row read-TID recording —
  // the server validates that range via a retained footprint digest instead.
  // At commit the proxy sets use_range_hash so the server re-derives + compares.
  void set_rangehash_eligible(bool v) { rangehash_eligible_ = v; }
  bool rangehash_eligible() const { return rangehash_eligible_; }

  LineairDBTransaction(THD* thd, 
                       LineairDBProxy* lineairdb_proxy, 
                       handlerton* lineairdb_hton,
                       bool isFence);
  ~LineairDBTransaction() = default;

private:
  int64_t tx_id;  // transaction id (instead of tx pointer), -1 means tx is not started
  LineairDBProxy* lineairdb_proxy;
  std::string db_table_key;
  THD* thread;
  bool isTransaction;
  handlerton* hton;
  bool isFence;
  bool oneshot_mode_{false};
  bool oneshot_plan_resolved_{false};
  bool oneshot_registered_{false};

  // helios 2-RPC PHYSICAL OCC (Step 5/6): when the server-side runs the
  // prefetch in physical mode it returns a non-zero tx_occ_key. The proxy
  // does NOT cache result_keys / filtered (they live server-side in
  // TxOccStore); at commit we just echo this key and the server merges the
  // retained ranges/indexes into its ValidateAndCommit input. Zero means
  // legacy logical mode and the existing range_validation_set_ path is used.
  uint64_t tx_occ_key_{0};

  // helios Phase-6 range-hash OCC eligibility (read-only SELECT + gate on).
  bool rangehash_eligible_{false};

  // HELIOS_TIMEPROF: per-phase wall time accumulators (ns). Printed at commit.
  // proxy-side only — the server publishes its own [MEMPROF-server] / [PLANSZ]
  // counters separately. Goal: per-query attribution of where the 6×–47×
  // helios/InnoDB latency gap goes.
  uint64_t tp_rpc_execute_ns_{0};       // TX_EXECUTE_READ_PLAN RPC wall time
  uint32_t tp_rpc_execute_count_{0};
  uint64_t tp_ingest_ns_{0};            // ReadPlanResult → local_*_scans cache
  uint32_t tp_ingest_count_{0};
  uint64_t tp_activate_rv_ns_{0};       // activate_range_validation cumulative
  uint32_t tp_activate_rv_count_{0};
  uint64_t tp_commit_rpc_ns_{0};        // TX_VALIDATE_AND_COMMIT RPC wall time
  uint32_t tp_commit_rpc_count_{0};

 public:
  // Phase-5 handler-entry timers. Env-gated via HELIOS_HANDLER_TIMEPROF=1.
  // Each handler entry-point in ha_lineairdb.cc records its wall-clock
  // cumulative time and call count into these counters; the existing
  // [TIMEPROF] commit line gets a sibling [HTIMEPROF] dump so the residual
  // (lat - rpc_exec - ingest - commit) can be attributed to MySQL handler
  // iteration vs row decode vs ldb cache lookups.
  struct HandlerTimeProf {
    uint64_t rnd_init_ns{0};        uint32_t rnd_init_n{0};
    uint64_t rnd_next_ns{0};        uint32_t rnd_next_n{0};
    uint64_t rnd_pos_ns{0};         uint32_t rnd_pos_n{0};
    uint64_t index_init_ns{0};      uint32_t index_init_n{0};
    uint64_t index_read_map_ns{0};  uint32_t index_read_map_n{0};
    uint64_t index_next_ns{0};      uint32_t index_next_n{0};
    uint64_t index_next_same_ns{0}; uint32_t index_next_same_n{0};
    uint64_t index_first_ns{0};     uint32_t index_first_n{0};
    uint64_t index_last_ns{0};      uint32_t index_last_n{0};
    uint64_t index_prev_ns{0};      uint32_t index_prev_n{0};
    uint64_t external_lock_ns{0};   uint32_t external_lock_n{0};
    uint64_t start_stmt_ns{0};      uint32_t start_stmt_n{0};
    uint64_t store_lock_ns{0};      uint32_t store_lock_n{0};
    uint64_t set_fields_ns{0};      uint32_t set_fields_n{0};
  };
  HandlerTimeProf htp_{};
  bool htp_enabled_{false};
  bool htp_inited_{false};
  inline bool htp_enabled() {
    if (!htp_inited_) {
      htp_enabled_ = (std::getenv("HELIOS_HANDLER_TIMEPROF") != nullptr);
      htp_inited_ = true;
    }
    return htp_enabled_;
  }
  // RAII timer: records elapsed ns into *ns_out and increments *n_out.
  // Cheap when HELIOS_HANDLER_TIMEPROF is unset (single env lookup + branch).
  class HtimeprofScope {
   public:
    HtimeprofScope(LineairDBTransaction* tx, uint64_t* ns_out, uint32_t* n_out)
        : ns_out_(nullptr), n_out_(nullptr) {
      if (tx == nullptr) return;
      if (!tx->htp_enabled()) return;
      ns_out_ = ns_out;
      n_out_ = n_out;
      timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
      t0_ = static_cast<uint64_t>(t.tv_sec) * 1000000000ull + t.tv_nsec;
    }
    ~HtimeprofScope() {
      if (ns_out_ == nullptr) return;
      timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
      const uint64_t t1 =
          static_cast<uint64_t>(t.tv_sec) * 1000000000ull + t.tv_nsec;
      *ns_out_ += (t1 - t0_);
      if (n_out_) ++(*n_out_);
    }
   private:
    uint64_t* ns_out_;
    uint32_t* n_out_;
    uint64_t t0_{0};
  };

 private:

  // stores the last RPC read result to maintain data pointer validity
  std::string last_read_value_;

  // transaction abort status (updated by RPC responses)
  bool is_aborted_;

  struct RowCountDelta {
    LineairDB_share *share;
    std::string table_name;
    int64_t delta;
  };
  std::vector<RowCountDelta> rowcount_deltas_;

  struct LocalRowEntry {
    std::string table_name;
    std::string key;
    bool found;
    std::string value;
    uint64_t tid = 0;
    bool validate_on_use = false;
  };
  // These sets live only inside one LineairDBTransaction.
  // commit/abort deletes the object, so prefetched rows never cross txs.

  // Proxy-side read set for exact primary-key reads.
  // Keyed by (table_name + '\0' + key) so dedup + lookup are O(1).
  std::unordered_map<std::string, LocalRowEntry> local_read_set_;
  static std::string make_local_read_key(const std::string& table,
                                         const std::string& key) {
    std::string k;
    k.reserve(table.size() + 1 + key.size());
    k.append(table);
    k.push_back('\0');
    k.append(key);
    return k;
  }
  // Proxy-side write set for exact primary-key writes/deletes
  std::vector<LocalRowEntry> local_write_set_;

  struct StatelessReadEntry {
    std::string table_name;
    std::string key;
    uint64_t tid;
    bool found;
  };
  std::vector<StatelessReadEntry> stateless_read_set_;
  // Phase-3b: O(1) dedup index over stateless_read_set_. Maps the composite
  // key (table_name + '\\0' + key) to the entry index. Previously the dedup
  // loop in record_stateless_read was O(N); for a TPC-H scan over 60k rows
  // that turns into O(N^2) = ~1.8B string compares (~8s wall time on the
  // commit hot path), which made oneshot ~30x slower than the NLJ baseline.
  std::unordered_map<std::string, size_t> stateless_read_index_;
  std::vector<LineairDBProxy::RangeValidationEntry> range_validation_set_;
  std::vector<LineairDBProxy::IndexValidationEntry> index_validation_set_;
  // O(1)-amortized dedup buckets for the two validation sets. activate_range_
  // validation is called once per cache scan/probe; at scale (TPC-H SF1 Q3 does
  // ~160k range scans) the previous O(N) linear dedup loop became O(N^2) (~13e9
  // compares, ~115s wall on the local join path). Each bucket maps a hash of
  // ALL compared fields (incl. the result_keys / result_primary_keys contents,
  // which the server's LOGICAL range validation actually checks) to the indices
  // of matching set entries; on a hash hit we still run the exact field-by-field
  // comparison, so dedup semantics are identical to the old linear scan, but
  // O(1) amortized. (Codex: must compare result_keys contents, not just sizes;
  // node pointers are RCU-freed and unused by logical validation.)
  std::unordered_map<uint64_t, std::vector<size_t>> range_validation_buckets_;
  std::unordered_map<uint64_t, std::vector<size_t>> index_validation_buckets_;

  // Probe-level OCC dedup: the prefetch cache is immutable within a tx, so
  // serving the SAME (table,index,start,end,limit,reverse) probe twice records
  // identical OCC obligations (the per-row record_stateless_read + the
  // activate_range_validation entry). A NLJ replays the same inner probe once
  // per outer row (e.g. ~24M times for Q9 at SF=1), so re-recording on every
  // serve re-hashes the cached range's result_keys each time — pure CPU waste.
  // This set lets a serve skip the OCC recording when the probe was already
  // recorded; the row DATA is still returned every time. Collision-free
  // length-prefixed identity key (binary keys embed NUL). General, not
  // query-specific. See probe_occ_already_recorded.
  std::unordered_set<std::string> recorded_probe_keys_;

  // Projection pushdown: for a table whose base-row VALUES were prefetched
  // TRIMMED to a subset of columns, maps table name -> kept field indices
  // (0-based TABLE::field[], ascending). Absent => that table's cached values
  // are FULL rows (legacy decode). Set by execute_read_plan when it ships a
  // projection for a step; read by the handler's value decoder. Uniform per
  // table (we only project single-reference tables in v1), so one entry per
  // table is sufficient — no per-value metadata. Fresh per tx.
  std::unordered_map<std::string, std::vector<uint32_t>> projection_by_table_;

 public:
  void set_table_projection(const std::string& table,
                            std::vector<uint32_t> kept) {
    projection_by_table_[table] = std::move(kept);
  }
  // Returns the kept field-index list if `table`'s cached values are projected,
  // else nullptr (full rows).
  const std::vector<uint32_t>* table_projection(const std::string& table) const {
    auto it = projection_by_table_.find(table);
    return it == projection_by_table_.end() ? nullptr : &it->second;
  }

  // Public so the free-function scan-cache helpers (range_entry_matches /
  // slice_range_entry and the secondary equivalents) in lineairdb_transaction.cc
  // can name these types. They hold no invariants beyond plain data.
  struct LocalRangeScanEntry {
    std::string table_name;
    std::string start_key;
    std::string end_key;
    bool reverse_scan;
    uint64_t row_limit = 0;
    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint64_t> row_tids;
    std::vector<LineairDBProxy::RangeValidationEntry> range_versions;
    std::vector<LineairDBProxy::IndexValidationEntry> index_reads;
    // The server-side pushed predicate that produced these rows (empty = none).
    // A filtered entry holds only predicate-matching rows, so it must NOT serve
    // a covering/sub-range probe that expects the unfiltered row set (Codex P1).
    std::string filter_serialized;
  };
  struct LocalSecondaryScanEntry {
    std::string table_name;
    std::string index_name;
    std::string start_key;
    std::string end_key;
    bool reverse_scan;
    uint64_t row_limit = 0;
    std::vector<std::string> secondary_keys;
    std::vector<std::string> primary_keys;
    std::vector<LineairDBProxy::RangeValidationEntry> range_versions;
    std::vector<LineairDBProxy::IndexValidationEntry> index_reads;
  };

 private:
  std::vector<LocalRangeScanEntry> local_range_scans_;
  std::vector<LocalSecondaryScanEntry> local_secondary_scans_;

  // O(1) exact-start-key index over the scan caches. FER/FES join prefetch
  // produces one cache entry per join probe (thousands of entries); the
  // original reverse linear scan in lookup_local_range_scan /
  // lookup_local_secondary_scan would then be O(N) per probe = O(N^2) total.
  // These maps give an exact-start-key hit in O(1). Keyed by start_key for the
  // primary cache and by index_name + '\0' + start_key for the secondary cache;
  // value is a list of indices into the vectors above (latest pushed last).
  std::unordered_map<std::string, std::vector<size_t>> range_scan_index_;
  std::unordered_map<std::string, std::vector<size_t>> secondary_scan_index_;
  // Above this many entries, lookups that miss the O(1) index do NOT fall back
  // to the full reverse linear scan (it would be O(N^2)); they return nullopt
  // so the caller does a stateless RPC instead. Small caches (full-scan-slice
  // plans like Q14's single S:lineitem) stay below this and keep linear slicing.
  static constexpr size_t kScanCacheLinearScanCap = 64;
  void push_local_range_scan(LocalRangeScanEntry entry);
  void push_local_secondary_scan(LocalSecondaryScanEntry entry);

  // Predicate pushdown: serialized PushedPredicate for scan filtering
  std::string pushed_filter_;

  // Phase-3b: oneshot plan staged from ha_lineairdb.cc but not yet executed.
  // Executed lazily on the first read/scan via execute_pending_oneshot_plan().
  std::vector<LineairDBProxy::ReadPlanStep> pending_oneshot_plan_steps_;

  // Max number of buffered write/delete ops before an automatic flush
  static constexpr size_t WRITE_BATCH_SIZE = 100;
  // Pending RPC flush queue for row and secondary-index ops in MySQL order
  std::vector<LineairDBProxy::BatchOp> write_buffer_ops_;

  TxRpcTrace rpc_trace_;

  std::optional<LocalRowEntry> lookup_local_write_set(
      const std::string& table_name, const std::string& key) const;
  std::optional<LocalRowEntry> lookup_local_read_set(
      const std::string& table_name, const std::string& key) const;
  void drop_local_read(const std::string& table_name,
                       const std::string& key);
  bool key_is_in_range(const std::string& key,
                       const std::string& start_key,
                       const std::string& end_key) const;
  bool key_starts_with(const std::string& key,
                       const std::string& prefix) const;
  void remove_scan_row(std::vector<std::pair<std::string, std::string>>& rows,
                       const std::string& key) const;
  void insert_scan_row_in_order(
      std::vector<std::pair<std::string, std::string>>& rows,
      const std::string& key, const std::string& value,
      bool reverse_scan) const;
  void merge_pending_rows_into_range_scan(
      std::vector<std::pair<std::string, std::string>>& rows,
      const std::string& start_key, const std::string& end_key,
      bool reverse_scan) const;
  void merge_pending_rows_into_prefix_scan(
      std::vector<std::pair<std::string, std::string>>& rows,
      const std::string& prefix) const;
  bool has_pending_ops_for_table(const std::string& table_name) const;
  // True if this oneshot transaction has accumulated any local state (prefetch
  // rows, reads, writes, validations). When false the tx is "clean" and can be
  // switched to a normal scan-capable transaction; when true an uncovered scan
  // must use a stateless RPC instead (cannot safely re-begin). (d)
  bool has_oneshot_local_state() const;
  // Ensure a server transaction exists before a non-oneshot RPC. oneshot defers
  // tx_begin; if oneshot was disabled mid-statement (no plan) the tx may still
  // be unstarted (tx_id == -1), so a normal RPC would run tx-less and abort at
  // commit. No-op for genuine normal statements (already started) and while
  // still in oneshot mode. (d/P1-a)
  void ensure_started_for_normal_rpc() {
    if (!oneshot_mode_ && is_not_started()) begin_transaction();
  }
  bool has_pending_secondary_ops_for_index(
      const std::string& table_name,
      const std::string& index_name) const;
  void drop_local_secondary_scans(const std::string& table_name,
                                  const std::string& index_name);
  void record_local_write(const std::string& table_name,
                          const std::string& key, bool found,
                          const std::string& value);
  void record_local_read(const std::string& table_name,
                         const std::string& key, bool found,
                         const std::string& value, uint64_t tid = 0,
                         bool validate_on_use = false);
  void record_stateless_read(const std::string& table_name,
                             const std::string& key, bool found,
                             uint64_t tid);
  void activate_local_read(const LocalRowEntry& entry);
  void activate_range_validation(
      const std::vector<LineairDBProxy::RangeValidationEntry>& ranges,
      const std::vector<LineairDBProxy::IndexValidationEntry>& indexes);
  // Returns true if this exact probe's OCC obligations were ALREADY recorded
  // in this tx (so the caller may skip re-recording); false on first sight (and
  // remembers it). Identity = (table,index,start,end,limit,reverse),
  // length-prefixed to be collision-free with NUL-bearing binary keys.
  bool probe_occ_already_recorded(const std::string& table_name,
                                  const std::string& index_name,
                                  const std::string& start_key,
                                  const std::string& end_key, uint64_t row_limit,
                                  bool reverse_scan);
  // Diagnostic: an oneshot prefetch cache miss (the plan didn't prefetch this
  // probe, so we'd fall back to a per-probe stateless RPC). Logs the miss when
  // HELIOS_TRACE_MISS is set; returns true (and marks the tx aborted) when
  // HELIOS_ABORT_ON_MISS is set, so callers stop instead of the noisy per-row
  // RPC fallback. Returns false otherwise (keep the existing fallback).
  bool note_oneshot_miss(const char* what, const std::string& table,
                         const std::string& key);
  std::optional<LocalRangeScanEntry> lookup_local_range_scan(
      const std::string& table_name, const std::string& start_key,
      const std::string& end_key, bool reverse_scan, uint64_t row_limit) const;
  // Negative caching: returns a completed, unlimited PK range scan entry that
  // PROVES `key` absent (key lies in the range but not in its pre-filter
  // logical key set), or nullptr. Caller activates the entry's range
  // validation so a concurrent INSERT of `key` still aborts at commit.
  const LocalRangeScanEntry* find_negative_covering_range_scan(
      const std::string& table_name, const std::string& key) const;
  // Positive covering: an exact-key PK point read whose row was prefetched by a
  // FER/FES sub-scan lives ONLY in a range entry (push_local_range_scan), never
  // in local_read_set_, so read()/batch_read() would false-miss it and abort.
  // Returns the range entry + row index whose `rows` holds `key` exactly (the
  // row PROVABLY present), or nullopt. Caller serves the value and records the
  // row's per-row TID as a commit obligation (value-update detection). Presence
  // only — absence stays with find_negative_covering_range_scan.
  struct PositiveRangeHit {
    const LocalRangeScanEntry* entry;
    size_t row_idx;
  };
  std::optional<PositiveRangeHit> lookup_positive_covering_range_row(
      const std::string& table_name, const std::string& key) const;
  std::optional<LocalSecondaryScanEntry> lookup_local_secondary_scan(
      const std::string& table_name, const std::string& index_name,
      const std::string& start_key, const std::string& end_key,
      bool reverse_scan, uint64_t row_limit) const;
  bool oneshot_validate_and_commit();
  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};

#endif /* LINEAIRDB_TRANSACTION_HH */
