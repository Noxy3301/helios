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
  // Optional out: set when this call is served from a LIMIT-staged cache
  // entry. The handler must not treat exhausting the returned rows as EOF.
  // TODO: replace this out-param with a small result struct when scan-cache
  // serving is split out of LineairDBTransaction.
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
  // True for the read-only no-validation path; staged filters use the same gate.
  bool ro_novalidate() const { return ro_novalidate_; }
  // Subqueries may be staged before the statement root exists. Remember each
  // root so the same subquery plan is not prefetched twice in one statement.
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

  // Aggregation pushdown: serialized AggregateSpec for the primary scan.
  void set_pushed_aggregate(const std::string& s) { pushed_aggregate_ = s; }
  void clear_pushed_aggregate() { pushed_aggregate_.clear(); }
  bool has_pushed_aggregate() const { return !pushed_aggregate_.empty(); }

  // Projection pushdown stores kept field ordinals per physical table.
  static uint64_t load_projection_global_epoch() {
    return projection_epoch_.load(std::memory_order_relaxed);
  }
  void set_table_projection(const std::string& table_name,
                            std::vector<uint32_t> kept) {
    table_projection_[table_name] = std::move(kept);
    projection_epoch_.fetch_add(1, std::memory_order_relaxed);
  }
  const std::vector<uint32_t>* load_table_projection(
      const std::string& table_name) const {
    auto it = table_projection_.find(table_name);
    return it == table_projection_.end() ? nullptr : &it->second;
  }

  void add_rowcount_delta(LineairDB_share *share, const std::string &table_name, int64_t delta);
  int64_t peek_rowcount_delta(const LineairDB_share *share) const;

  // RPC trace statement boundary; TxRpcTrace dedupes repeated SQL strings.
  void on_stmt_boundary(const std::string& sql) { rpc_trace_.on_stmt(sql); }
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
  // Autocommit single-statement SELECT with lineairdb_prefetch_ro_novalidate=ON:
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
  // Dedup/lookup index into own_writes_, keyed like row_cache_. own_writes_ only
  // ever grows by push_back, so a stored index never moves and stays valid.
  std::unordered_map<std::string, size_t> own_writes_index_;

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
    // Set only on lookup return copies: this entry came from a LIMIT-staged
    // window, so the handler must abort if MySQL asks past these rows.
    bool truncated = false;
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
  // Exact-start indexes for grouped range scans. Without these, each runtime
  // probe would scan the whole cache vector. Keyed table\x01index\x01start_key.
  std::unordered_map<std::string, std::vector<size_t>> range_scan_start_index_;
  std::unordered_map<std::string, std::vector<size_t>> secondary_scan_start_index_;
  void push_range_scan_cache(LocalRangeScanEntry entry);
  void push_secondary_scan_cache(LocalSecondaryScanEntry entry);

  // Predicate pushdown: serialized PushedPredicate for scan filtering
  std::string pushed_filter_;
  // Aggregation pushdown: serialized AggregateSpec for the primary scan
  std::string pushed_aggregate_;

  // Physical table name -> kept field ordinals for projected staged rows.
  std::unordered_map<std::string, std::vector<uint32_t>> table_projection_;
  // Process-wide registration epoch for handler decode memo refresh.
  static inline std::atomic<uint64_t> projection_epoch_{0};

  // Max number of buffered write/delete ops before an automatic flush
  static constexpr size_t WRITE_BATCH_SIZE = 1024;
  // Pending RPC flush queue for row and secondary-index ops in MySQL order
  std::vector<LineairDBProxy::BatchOp> write_buffer_ops_;

  TxRpcTrace rpc_trace_;

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
