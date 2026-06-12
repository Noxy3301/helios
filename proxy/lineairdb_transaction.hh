#ifndef LINEAIRDB_TRANSACTION_HH
#define LINEAIRDB_TRANSACTION_HH

#include <atomic>
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
  // served_truncated (optional out): set true when an unbounded request was
  // served from a limit-staged prefetch entry, i.e. the returned rows are the
  // first N of the range and the real range may hold more. Passing a non-null
  // pointer is the caller's opt-in to handle that (abort on over-read).
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_in_range(
      std::string start_key, std::string end_key, uint64_t row_limit = 0,
      bool reverse_scan = false, bool *served_truncated = nullptr);
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_from_prefix(
      std::string prefix);
  bool write(std::string key, const std::string value);
  bool write_secondary_index(std::string index_name, std::string secondary_key, const std::string primary_key);
  std::vector<std::string> read_secondary_index(std::string index_name, std::string secondary_key);
  std::vector<std::string> get_matching_primary_keys_in_range(
      std::string index_name, std::string start_key, std::string end_key,
      uint64_t row_limit = 0, bool reverse_scan = false);
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
  void set_prefetch_mode(bool enabled) { prefetch_mode_ = enabled; }
  bool is_prefetch_mode() const { return prefetch_mode_; }
  void prefetch_stateless_reads(
      const std::vector<LineairDBProxy::StatelessReadKey>& reads);
  void execute_read_plan(const std::vector<LineairDBProxy::ReadPlanStep>& steps);

  // Set when an injected tx-scoped plan (@_tx_plan / DSL) ran at begin, so the
  // statement-scoped autogen path stays out of the way (the two are mutually
  // exclusive per transaction).
  void set_tx_plan_used(bool used) { tx_plan_used_ = used; }
  bool tx_plan_used() const { return tx_plan_used_; }

  // Statement-scoped autogen staging is gated per MySQL statement, keyed by
  // thd->query_id: the plan is auto-generated and executed once per statement,
  // and the reads accumulate into this transaction's validation set.
  uint64_t autogen_query_id() const { return autogen_query_id_; }
  void reset_autogen_for_statement(uint64_t query_id) {
    autogen_query_id_ = query_id;
    autogen_stmt_resolved_ = false;
    autogen_stmt_handler_deferred_ = false;
    autogen_staged_roots_.clear();
  }
  bool autogen_stmt_resolved() const { return autogen_stmt_resolved_; }
  void mark_autogen_stmt_resolved() { autogen_stmt_resolved_ = true; }
  // Autocommit read-only SELECT with validation elided (sysvar gate); also
  // gates filter pushdown into staged scan steps.
  bool ro_novalidate() const { return ro_novalidate_; }
  // Optimize-time subquery staging: one staging per plan root within the
  // statement (the statement-level root is not built yet at that point).
  bool autogen_root_staged(const void *root) const {
    return autogen_staged_roots_.count(root) != 0;
  }
  void mark_autogen_root_staged(const void *root) {
    autogen_staged_roots_.insert(root);
  }
  bool is_autogen_stmt_handler_deferred() const {
    return autogen_stmt_handler_deferred_;
  }
  void mark_autogen_stmt_handler_deferred() {
    autogen_stmt_handler_deferred_ = true;
  }

  inline bool is_not_started() const {
    if (prefetch_mode_) return !prefetch_registered_;
    if (tx_id == -1) return true;
    return false;
  }

  inline int64_t get_tx_id() const {
    return tx_id;
  }

  inline bool is_aborted() const { 
    return is_aborted_;
  }

  bool aborted_by_cache_miss() const { return aborted_by_cache_miss_; }

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

  // Aggregation pushdown: serialized AggregateSpec stamped onto the matching
  // primary scan step at send time (see execute_read_plan). The table key is
  // remembered as the consumption gate: range-cache entries holding GROUP
  // rows are served only while the aggregate's own scan is consuming them
  // (see lookup_range_scan_cache), never to other readers of the table.
  void set_pushed_aggregate(const std::string& s) {
    pushed_aggregate_ = s;
    pushed_aggregate_table_ = db_table_key;
  }
  void clear_pushed_aggregate() {
    pushed_aggregate_.clear();
    pushed_aggregate_table_.clear();
  }
  bool has_pushed_aggregate() const { return !pushed_aggregate_.empty(); }

  // Inner-unit aggregation (Phase-16): an offloaded MATERIALIZED uncorrelated
  // subquery registers its full Phase-B spec + the exact WHERE filter it
  // requires at optimize time, keyed by physical table ("./db/tbl"). The
  // autogen stamp pass attaches the spec to the matching staged scan step
  // ONLY when the step's attached filter equals the registered one, and
  // records that decision so the override executor knows whether the staged
  // step returns GROUP rows (Phase B consume) or raw rows (Phase A fallback).
  struct InnerAggregate {
    std::string spec;    // serialized AggregateSpec
    std::string filter;  // serialized PushedPredicate the spec requires ("" = none)
    // Leaf TABLE*s of the registered units (identity only, never dereferenced)
    // — the stamp pass requires EVERY alias folded into a step to be one of
    // these, so a raw same-table consumer can never be merged into an
    // aggregate step (Codex P1-2).
    std::unordered_set<const void*> leaves;
    // Two same-table inner units with DIFFERENT specs/filters cannot share a
    // table-keyed registration: poison it so nothing stamps and both fall
    // back to Phase A over raw rows (Codex P1-1).
    bool poisoned = false;
  };
  const InnerAggregate* inner_aggregate(const std::string& table) const {
    auto it = inner_agg_specs_.find(table);
    if (it == inner_agg_specs_.end() || it->second.poisoned) return nullptr;
    return &it->second;
  }
  // Stamp record is LEAF-scoped: the executor asks "was MY unit's leaf
  // stamped", so a same-table unit whose own step stayed raw (poisoned or
  // later-registered) cleanly falls back to Phase A instead of tripping the
  // stamped-but-unconsumable error.
  void mark_inner_agg_stamped(const std::string& table) {
    auto it = inner_agg_specs_.find(table);
    if (it == inner_agg_specs_.end()) return;
    auto& dst = inner_agg_stamped_leaves_[table];
    for (const void* l : it->second.leaves) dst.insert(l);
  }
  // _current variants operate on the chosen table (db_table_key).
  void register_inner_aggregate_current(std::string spec, std::string filter,
                                        const void* leaf) {
    auto it = inner_agg_specs_.find(db_table_key);
    if (it == inner_agg_specs_.end()) {
      InnerAggregate ia;
      ia.spec = std::move(spec);
      ia.filter = std::move(filter);
      ia.leaves.insert(leaf);
      inner_agg_specs_.emplace(db_table_key, std::move(ia));
      return;
    }
    if (it->second.poisoned) return;
    if (it->second.spec != spec || it->second.filter != filter) {
      it->second.poisoned = true;
      return;
    }
    it->second.leaves.insert(leaf);  // identical clone (q15's two view refs)
  }
  bool inner_agg_stamped_current(const void* leaf) const {
    auto it = inner_agg_stamped_leaves_.find(db_table_key);
    return it != inner_agg_stamped_leaves_.end() &&
           it->second.count(leaf) != 0;
  }
  // Begin consuming the CURRENT table's staged group rows: opens the lookup
  // gate (and protects the pushed filter from rnd_init clearing). The caller
  // passes the spec it intends to parse with; identity with the registered
  // (= stamped) spec is required (Codex P1-1). Paired with
  // clear_pushed_aggregate() after the consuming scan ends.
  bool begin_inner_agg_consume(const std::string& expect_spec) {
    auto it = inner_agg_specs_.find(db_table_key);
    if (it == inner_agg_specs_.end() || it->second.poisoned) return false;
    if (it->second.spec != expect_spec) return false;
    pushed_aggregate_ = it->second.spec;
    pushed_aggregate_table_ = db_table_key;
    return true;
  }
  void clear_inner_aggregates() {
    inner_agg_specs_.clear();
    inner_agg_stamped_leaves_.clear();
  }

  // Projection pushdown (ro_novalidate SELECT only): per physical table, the
  // kept 0-based field ordinals its staged VALUES were trimmed to. The row
  // decoder maps the k-th parsed column to kept[k]; nullptr = full rows.
  // Registration bumps a process-wide epoch so the handler's per-statement
  // serve memo (possibly stamped BEFORE staging registered the layouts —
  // optimize-time unit serves precede statement-root staging) refreshes.
  static uint64_t projection_global_epoch() {
    return projection_epoch_.load(std::memory_order_relaxed);
  }
  void set_table_projection(const std::string& table_name,
                            std::vector<uint32_t> kept) {
    table_projection_[table_name] = std::move(kept);
    projection_epoch_.fetch_add(1, std::memory_order_relaxed);
  }
  const std::vector<uint32_t>* table_projection(
      const std::string& table_name) const {
    auto it = table_projection_.find(table_name);
    return it == table_projection_.end() ? nullptr : &it->second;
  }

  void add_rowcount_delta(LineairDB_share *share, const std::string &table_name, int64_t delta);
  int64_t peek_rowcount_delta(const LineairDB_share *share) const;

  // RPC trace statement boundary; TxRpcTrace dedupes repeated SQL strings.
  void on_stmt_boundary(const std::string& sql) { rpc_trace_.on_stmt(sql); }
  // Section timing hook for code outside this class (autogen compile etc.).
  TxRpcTrace* trace() { return &rpc_trace_; }
  bool fallback_to_normal_transaction(const char* reason);

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
  bool prefetch_mode_{false};
  bool prefetch_registered_{false};
  // Autocommit read-only SELECT with lineairdb_prefetch_ro_novalidate=ON:
  // read sets are not accumulated and commit skips the validation RPC.
  bool ro_novalidate_{false};
  bool tx_plan_used_{false};
  uint64_t autogen_query_id_{0};
  bool autogen_stmt_resolved_{false};
  std::unordered_set<const void*> autogen_staged_roots_;
  // Set when this statement's plan is built from the handler index access
  // (deferred legacy-DML path) instead of the QEP; a second handler access
  // then means an index merge the single staged range cannot serve.
  bool autogen_stmt_handler_deferred_{false};

  // stores the last RPC read result to maintain data pointer validity
  std::string last_read_value_;

  // transaction abort status (updated by RPC responses)
  bool is_aborted_;
  // Set when the abort came from a prefetch cache miss (an unstaged read
  // surface), so the handler returns a non-retryable error, not a deadlock.
  bool aborted_by_cache_miss_{false};

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

  // Proxy-side value cache for exact primary-key reads: serves a row to the
  // statement without an RPC. NOT a validation set -- the TID it carries feeds
  // base_row_read_set_ only when a cached row is actually consumed (the
  // cache-hit paths in read()/batch_read() append it). Overwritten when a
  // statement re-stages the key. Keyed by (table_name + '\0' + key) for O(1) lookup.
  std::unordered_map<std::string, LocalRowEntry> row_cache_;
  static std::string make_row_cache_key(const std::string& table,
                                         const std::string& key) {
    std::string k;
    k.reserve(table.size() + 1 + key.size());
    k.append(table);
    k.push_back('\0');
    k.append(key);
    return k;
  }
  // Proxy-side write set for exact primary-key writes/deletes
  std::vector<LocalRowEntry> own_writes_;

  // OCC commit-time read validation, two append-only sets (Silo read_set
  // style); the server re-checks each at commit and aborts on a mismatch:
  //   base_row_read_set_  per-key TID of base rows -> value changes
  //   range_read_set_     observed range membership (result key-list),
  //                       re-validated by logical replay -> phantoms
  struct StatelessReadEntry {
    std::string table_name;
    std::string key;
    uint64_t tid;
    bool found;
  };
  std::vector<StatelessReadEntry> base_row_read_set_;
  std::vector<LineairDBProxy::RangeReadEntry> range_read_set_;

  struct LocalRangeScanEntry {
    std::string table_name;
    std::string start_key;
    std::string end_key;
    bool reverse_scan;
    uint64_t row_limit = 0;
    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint64_t> row_tids;
    // Set only on copies returned by lookup_range_scan_cache: the entry was
    // staged with a row limit and filled to it, so rows past the last one may
    // exist server-side (over-reads must abort, not report EOF).
    bool truncated = false;
    // The staged step carried an AggregateSpec: `rows` are synthetic GROUP
    // rows (empty keys), not base rows. Served ONLY to the aggregate's own
    // consuming scan (pushed_aggregate gate); every other lookup skips the
    // entry — sub-range serving or point coverage from group rows would
    // corrupt any other reader of the same table.
    bool aggregate_rows = false;
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
  };
  std::vector<LocalRangeScanEntry> range_scan_cache_;
  std::vector<LocalSecondaryScanEntry> secondary_scan_cache_;
  // Exact-start lookup index over the scan caches: FER/FES staging creates one
  // bounded entry per join probe (tens of thousands), and every runtime probe
  // would otherwise scan the whole vector. Keyed table\x01index\x01start_key.
  std::unordered_map<std::string, std::vector<size_t>> range_scan_start_index_;
  std::unordered_map<std::string, std::vector<size_t>> secondary_scan_start_index_;
  void push_range_scan_cache(LocalRangeScanEntry entry);
  void push_secondary_scan_cache(LocalSecondaryScanEntry entry);

  // Predicate pushdown: serialized PushedPredicate for scan filtering
  std::string pushed_filter_;
  // Aggregation pushdown: serialized AggregateSpec for the primary scan
  std::string pushed_aggregate_;
  // Physical table whose staged GROUP rows the pushed aggregate may consume
  std::string pushed_aggregate_table_;
  // Inner-unit aggregation registrations + which leaves actually got stamped
  std::unordered_map<std::string, InnerAggregate> inner_agg_specs_;
  std::unordered_map<std::string, std::unordered_set<const void*>>
      inner_agg_stamped_leaves_;

  // Projection pushdown: physical table name -> kept field ordinals.
  std::unordered_map<std::string, std::vector<uint32_t>> table_projection_;
  // Process-wide registration epoch for the handler serve memo (see above).
  static inline std::atomic<uint64_t> projection_epoch_{0};

  // Max number of buffered write/delete ops before an automatic flush
  static constexpr size_t WRITE_BATCH_SIZE = 100;
  // Pending RPC flush queue for row and secondary-index ops in MySQL order
  std::vector<LineairDBProxy::BatchOp> write_buffer_ops_;

  // mutable: const cache lookups time themselves into the trace.
  mutable TxRpcTrace rpc_trace_;

  std::optional<LocalRowEntry> lookup_write_set(
      const std::string& table_name, const std::string& key) const;
  std::optional<LocalRowEntry> lookup_row_cache(
      const std::string& table_name, const std::string& key) const;
  void drop_row_cache(const std::string& table_name,
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
  bool has_pending_row_ops_in_range(const std::string& table_name,
                                    const std::string& start_key,
                                    const std::string& end_key) const;
  bool has_pending_secondary_ops_for_index(
      const std::string& table_name,
      const std::string& index_name) const;
  void drop_secondary_scan_cache(const std::string& table_name,
                                  const std::string& index_name);
  void record_write(const std::string& table_name,
                          const std::string& key, bool found,
                          const std::string& value);
  void record_row_cache(const std::string& table_name,
                               const std::string& key, bool found,
                               const std::string& value, uint64_t tid = 0,
                               bool validate_on_use = false);
  void append_base_row_read(const std::string& table_name,
                                    const std::string& key, bool found,
                                    uint64_t tid);
  void append_range_read(const LocalRangeScanEntry& cached);
  void append_secondary_range_read(const LocalSecondaryScanEntry& cached);
  void abort_prefetch_cache_miss(const std::string& reason);
  std::optional<LocalRangeScanEntry> lookup_range_scan_cache(
      const std::string& table_name, const std::string& start_key,
      const std::string& end_key, bool reverse_scan, uint64_t row_limit,
      bool allow_truncated = false) const;
  std::optional<LocalSecondaryScanEntry> lookup_secondary_scan_cache(
      const std::string& table_name, const std::string& index_name,
      const std::string& start_key, const std::string& end_key,
      bool reverse_scan, uint64_t row_limit) const;
  bool prefetch_validate_and_commit();
  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};

#endif /* LINEAIRDB_TRANSACTION_HH */
