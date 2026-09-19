#ifndef HELIOS_TRANSACTION_HH
#define HELIOS_TRANSACTION_HH

#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "sql/handler.h" /* handler */
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "helios_proxy.hh"
#include "rpc_trace.hh"

class Helios_share;

/**
 * @brief The transaction, kept by the query layer.
 *
 * It holds what it read (rows with their TIDs, ranges with their key lists),
 * what it wrote, and the row-count deltas, and installs all of it with one
 * tx_commit. Reads the cached read-plan results do not cover go to the
 * storage server as they happen.
 *
 * Lifetime of this class equals the lifetime of the transaction.
 * The instance of this class is deleted in end_transaction.
 * Set the pointer to this class to nullptr after end_transaction
 * to indicate that HeliosTransaction is terminated.
 */
class HeliosTransaction
{
public:
  void choose_table(std::string db_table_name);
  bool table_is_not_chosen();

  const std::pair<const std::byte *const, const size_t> read(std::string key);
  std::vector<std::pair<bool, std::string>> batch_read(const std::vector<std::string>& keys);
  // Buffer ops built elsewhere (the DDL backfill); the commit installs them.
  void buffer_writes(const std::string& table_name,
                     const std::vector<HeliosProxy::WriteOp>& ops);
  // served_truncated, when given, lets a cached range that holds only the
  // first rows serve an unlimited request; it is set when the result is a
  // partial scan result and the caller has to fetch the rest.
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_in_range(
      std::string start_key, std::string end_key, uint64_t row_limit = 0,
      bool reverse_scan = false, bool *served_truncated = nullptr);
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_from_prefix(
      std::string prefix);
  // Primary keys reached through index_name for the exact secondary key.
  // keys_only skips the base rows, for a probe that only needs the keys.
  std::vector<std::string> read_secondary_index(std::string index_name,
                                                std::string secondary_key,
                                                bool keys_only = false);
  std::vector<std::string> get_matching_primary_keys_in_range(
      std::string index_name, std::string start_key, std::string end_key,
      uint64_t row_limit = 0, bool reverse_scan = false);
  // One secondary key of the index and the primary keys under it, in key
  // order.
  struct SecondaryEntry {
    std::string secondary_key;
    std::vector<std::string> primary_keys;
  };
  // One fetch of the reverse index cursor: the highest secondary keys of the
  // range, in descending order, each with its primary keys. A batch holds
  // only whole groups, so a limited scan that cut the lowest one in the
  // middle drops it.
  struct SecondaryBatch {
    std::vector<SecondaryEntry> groups;
    // The range holds entries below the lowest group returned.
    bool more_below = false;
  };
  std::optional<SecondaryBatch> fetch_secondary_batch_below(
      const std::string &index_name, const std::string &start_key,
      const std::string &end_key, uint64_t batch_entries);
  void update_secondary_index(
      std::string index_name,
      std::string old_secondary_key,
      std::string new_secondary_key,
      const std::string primary_key);

  // Buffered writes, installed by the commit in the order they were issued.
  void buffer_write(const std::string& table_name,
                    const std::string& key, const std::string& value,
                    bool is_insert = false);
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

  // What this transaction's own writes say about a key an INSERT wants free.
  // Unknown means only the storage can answer.
  enum class KeyState { Free, Taken, Unknown };
  KeyState insert_key_state(const std::string& table_name,
                            const std::string& key) const;
  // True when this transaction's own index writes already put another row
  // under secondary_key.
  bool index_key_taken_by_own_write(const std::string& table_name,
                                    const std::string& index_name,
                                    const std::string& secondary_key,
                                    const std::string& primary_key) const;
  // True when every row and range this transaction read still reads the same
  // way. A duplicate found after one of them moved is a lost race, not a
  // constraint the client can fix. Sends one batch read and one keys-only
  // revalidation per recorded range; records nothing.
  bool reads_still_valid();
  // Probe the storage for keys an INSERT statement inserts, in one RPC. Every
  // answer enters the read set, so the commit revalidates an absence. Call it
  // only for keys insert_key_state reported Unknown: it does not consult the
  // write set, which by then holds the statement's own rows.
  bool probe_insert_keys(const std::string& table_name,
                         const std::vector<std::string>& keys);

  void begin_transaction();
  void set_status_to_abort();
  bool end_transaction(bool *transport_error = nullptr,
                       bool *duplicate_key = nullptr);
  void execute_read_plan(const std::vector<HeliosProxy::ReadPlanStep>& steps);

  // Set when an injected tx-scoped plan (@_tx_plan / DSL) ran at begin, so the
  // statement-scoped prefetch stays out of the way (the two are mutually
  // exclusive per transaction).
  void set_tx_plan_used(bool used) { tx_plan_used_ = used; }
  bool tx_plan_used() const { return tx_plan_used_; }

  // The statement-scoped prefetch is gated per MySQL statement, keyed by
  // thd->query_id: the plan is compiled and executed once per statement,
  // and the reads accumulate into this transaction's validation set.
  uint64_t stmt_plan_query_id() const { return stmt_plan_query_id_; }
  void reset_stmt_plan(uint64_t query_id) {
    stmt_plan_query_id_ = query_id;
    stmt_prefetch_done_ = false;
    single_table_dml_deferred_ = false;
    stmt_plan_roots_.clear();
    index_tail_tables_.clear();
  }
  bool stmt_prefetch_done() const { return stmt_prefetch_done_; }
  void mark_stmt_prefetch_done() { stmt_prefetch_done_ = true; }
  // Subqueries may run before the statement root exists. Remember each root
  // so the same subquery plan is not run twice in one statement.
  bool has_stmt_plan_root(const void *root) const {
    return stmt_plan_roots_.count(root) != 0;
  }
  void note_stmt_plan_root(const void *root) {
    stmt_plan_roots_.insert(root);
  }
  // Key-less index tail fetches made this statement, keyed by table.
  bool has_index_tail_fetch(const std::string &table_key) const {
    return index_tail_tables_.count(table_key) != 0;
  }
  void note_index_tail_fetch(const std::string &table_key) {
    index_tail_tables_.insert(table_key);
  }
  bool single_table_dml_deferred() const {
    return single_table_dml_deferred_;
  }
  void mark_single_table_dml_deferred() {
    single_table_dml_deferred_ = true;
  }

  inline bool is_not_started() const { return !registered_; }

  inline bool is_aborted() const {
    return is_aborted_;
  }

  bool has_transport_error() const { return transport_error_; }
  // Set when the server refused an insert because the key already held a row.
  bool duplicate_key_abort() const { return duplicate_key_abort_; }

  inline void mark_transport_error() {
    transport_error_ = true;
    is_aborted_ = true;
  }

  inline bool is_a_single_statement() const { return !isTransaction; }

  void add_rowcount_delta(Helios_share *share, const std::string &table_name, int64_t delta);
  int64_t peek_rowcount_delta(const Helios_share *share) const;

  // RPC trace statement boundary; TxRpcTrace dedupes repeated SQL strings.
  void on_stmt_boundary(const std::string& sql) { rpc_trace_.on_stmt(sql); }

  HeliosTransaction(THD* thd,
                       HeliosProxy* helios_proxy,
                       handlerton* helios_hton);
  ~HeliosTransaction() = default;

private:
  HeliosProxy* helios_proxy;
  std::string db_table_key;
  THD* thread;
  bool isTransaction;
  handlerton* hton;
  bool registered_{false};
  bool tx_plan_used_{false};
  uint64_t stmt_plan_query_id_{0};
  bool stmt_prefetch_done_{false};
  std::unordered_set<const void*> stmt_plan_roots_;
  std::unordered_set<std::string> index_tail_tables_;
  // Set when this statement's plan is built from the handler index access
  // instead of the QEP; a second handler access then means an index merge the
  // single cached range cannot serve.
  bool single_table_dml_deferred_{false};

  // stores the last RPC read result to maintain data pointer validity
  std::string last_read_value_;

  // transaction abort status (updated by RPC responses)
  bool is_aborted_;
  // A lost RPC connection is not OCC contention and must never be surfaced as
  // a retryable deadlock (or as an empty scan result).
  bool transport_error_{false};
  // A duplicate key is a permanent rejection, not contention.
  bool duplicate_key_abort_{false};

  struct RowCountDelta {
    Helios_share *share;
    std::string table_name;
    int64_t delta;
  };
  std::vector<RowCountDelta> rowcount_deltas_;

  struct RowEntry {
    std::string table_name;
    std::string key;
    bool found;
    std::string value;
    uint64_t tid = 0;
  };
  // These sets live only inside one HeliosTransaction.
  // commit/abort deletes the object, so cached rows never cross txs.

  // Served-row cache for point reads, keyed by (table_name + '\0' + key).
  // Not a validation set: an entry's TID joins base_row_read_set_ only when a
  // cached row is actually consumed. Caching a key again overwrites it.
  std::unordered_map<std::string, RowEntry> row_cache_;
  static std::string make_row_cache_key(const std::string& table,
                                         const std::string& key) {
    std::string k;
    k.reserve(table.size() + 1 + key.size());
    k.append(table);
    k.push_back('\0');
    k.append(key);
    return k;
  }
  // Write set for exact primary-key writes/deletes
  std::vector<RowEntry> own_writes_;
  // Dedup/lookup index into own_writes_, keyed like row_cache_. own_writes_ only
  // ever grows by push_back, so a stored index never moves and stays valid.
  std::unordered_map<std::string, size_t> own_writes_index_;

  // Append-only read sets the commit re-validates: per-key TIDs of base rows,
  // and per-range key lists revalidated to catch phantoms.
  std::vector<HeliosProxy::ReadEntry> base_row_read_set_;
  std::vector<HeliosProxy::RangeReadEntry> range_read_set_;

  struct RangeScanCacheEntry {
    std::string table_name;
    std::string start_key;
    std::string end_key;
    bool reverse_scan;
    uint64_t row_limit = 0;
    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint64_t> row_tids;
    // Set only on a lookup return copy: a partial scan result holds the first
    // rows of the range, not all of them.
    bool truncated = false;
  };
  struct SecondaryScanCacheEntry {
    std::string table_name;
    std::string index_name;
    std::string start_key;
    std::string end_key;
    bool reverse_scan;
    uint64_t row_limit = 0;
    std::vector<std::string> secondary_keys;
    std::vector<std::string> primary_keys;
  };
  // Ranges a read plan cached for this statement. A request the cache covers
  // is served from it; anything else goes to the storage server.
  std::vector<RangeScanCacheEntry> range_scan_cache_;
  std::vector<SecondaryScanCacheEntry> secondary_scan_cache_;
  // Exact-start indexes for grouped range scans. Without these, each runtime
  // probe would scan the whole cache vector. Keyed table\x01index\x01start_key.
  std::unordered_map<std::string, std::vector<size_t>> range_scan_start_index_;
  std::unordered_map<std::string, std::vector<size_t>> secondary_scan_start_index_;
  void push_range_scan_cache(RangeScanCacheEntry entry);
  void push_secondary_scan_cache(SecondaryScanCacheEntry entry);
  // Keep the rows (pairs) the request's bounds cover. The parallel arrays are
  // appended together at every push site, so they are the same length.
  static void trim_range_entry(RangeScanCacheEntry& entry,
                               const std::string& start_key,
                               const std::string& end_key);
  static void trim_secondary_entry(SecondaryScanCacheEntry& entry,
                                   const std::string& start_key,
                                   const std::string& end_key);

  // Row and secondary-index ops in the order MySQL issued them
  std::vector<HeliosProxy::WriteOp> write_buffer_ops_;
  // The index entries those ops leave behind: table\x01index -> secondary key
  // -> primary key -> still there. Ordered by secondary key so a probe or a
  // scan asks for one key or one range of it, instead of the walk of the whole
  // write buffer per row that made a bulk INSERT quadratic.
  std::unordered_map<
      std::string,
      std::map<std::string, std::unordered_map<std::string, bool>>>
      pending_index_entries_;
  void record_index_op(const HeliosProxy::WriteOp& op);

  TxRpcTrace rpc_trace_;

  std::optional<RowEntry> lookup_write_set(const std::string& table_name,
                                           const std::string& key) const;
  std::optional<RowEntry> lookup_row_cache(const std::string& table_name,
                                           const std::string& key) const;
  void drop_row_cache(const std::string& table_name,
                       const std::string& key);
  bool key_is_in_range(const std::string& key,
                       const std::string& start_key,
                       const std::string& end_key) const;
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
  bool has_pending_row_ops_in_range(const std::string& table_name,
                                    const std::string& start_key,
                                    const std::string& end_key) const;
  bool has_pending_secondary_ops_in_range(const std::string& table_name,
                                          const std::string& index_name,
                                          const std::string& start_key,
                                          const std::string& end_key) const;
  void merge_pending_index_ops(
      const std::string& index_name, const std::string& start_key,
      const std::string& end_key,
      std::map<std::string, std::vector<std::string>>& groups) const;
  void record_write(const std::string& table_name,
                          const std::string& key, bool found,
                          const std::string& value);
  void record_row_cache(const std::string& table_name,
                               const std::string& key, bool found,
                               const std::string& value, uint64_t tid = 0);
  void append_base_row_read(const std::string& table_name,
                                    const std::string& key, uint64_t tid);
  void append_range_read(const RangeScanCacheEntry& scanned);
  void append_secondary_range_read(const SecondaryScanCacheEntry& scanned);
  // The storage server refused the request (a missing table or index). Not
  // contention, but the statement cannot go on either.
  void abort_server_refused(const char* what);
  std::optional<RangeScanCacheEntry> lookup_range_scan_cache(
      const std::string& table_name, const std::string& start_key,
      const std::string& end_key, bool reverse_scan, uint64_t row_limit,
      bool allow_truncated) const;
  std::optional<SecondaryScanCacheEntry> lookup_secondary_scan_cache(
      const std::string& table_name, const std::string& index_name,
      const std::string& start_key, const std::string& end_key,
      bool reverse_scan, uint64_t row_limit) const;
  // Scan the storage server and record what it returned.
  std::vector<std::pair<std::string, std::string>> scan_range(
      const std::string& start_key, const std::string& end_key,
      uint64_t row_limit, bool reverse_scan);
  struct SecondaryScan {
    bool ok = false;
    std::vector<std::string> secondary_keys;
    std::vector<std::string> primary_keys;
  };
  SecondaryScan scan_index_range(const std::string& index_name,
                                 const std::string& start_key,
                                 const std::string& end_key,
                                 uint64_t row_limit, bool reverse_scan,
                                 bool keys_only);
  // Traversal order of one secondary scan: merge this transaction's own index
  // ops into the groups, drop what is left empty, and apply the caller's
  // limit.
  SecondaryScan merge_index_scan(
      const std::string& index_name, const std::string& start_key,
      const std::string& end_key, uint64_t row_limit, bool reverse_scan,
      std::map<std::string, std::vector<std::string>>& groups) const;
  // Moves row j of an unpacked plan step into the row cache; an empty value
  // is a not-found answer.
  bool take_plan_row(const HeliosProxy::ReadPlanStep& step,
                     HeliosProxy::ReadPlanStepResult& result, size_t j,
                     std::string& key, std::string& value, uint64_t& tid);
  // Appends one secondary key once per primary key grouped under it.
  static void append_index_group(
      SecondaryScan& out, const std::string& secondary_key,
      const std::vector<std::string>& primary_keys);
  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
};

#endif /* HELIOS_TRANSACTION_HH */
