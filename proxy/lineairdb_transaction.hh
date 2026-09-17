#ifndef LINEAIRDB_TRANSACTION_HH
#define LINEAIRDB_TRANSACTION_HH

#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "sql/handler.h" /* handler */
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "lineairdb_proxy.hh"
#include "rpc_trace.hh"

class LineairDB_share;

/**
 * @brief The transaction, kept by the query layer.
 *
 * It holds what it read (rows with their TIDs, ranges with their key lists),
 * what it wrote, and the row-count deltas, and installs all of it with one
 * TX_COMMIT. Reads a staged plan did not cover go to the storage server as
 * they happen.
 *
 * Lifetime of this class equals the lifetime of the transaction.
 * The instance of this class is deleted in end_transaction.
 * Set the pointer to this class to nullptr after end_transaction
 * to indicate that LineairDBTransaction is terminated.
 */
class LineairDBTransaction
{
public:
  void choose_table(std::string db_table_name);
  bool table_is_not_chosen();

  const std::pair<const std::byte *const, const size_t> read(std::string key);
  std::vector<std::pair<bool, std::string>> batch_read(const std::vector<std::string>& keys);
  // Buffer ops built elsewhere (the DDL backfill); the commit installs them.
  void buffer_writes(const std::string& table_name,
                     const std::vector<LineairDBProxy::WriteOp>& ops);
  // served_truncated, when given, lets a staged window that holds only the
  // first rows of the range serve an unlimited request; it is set when the
  // result stops at the window and the caller has to fetch the rest.
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
  // The highest secondary key in the range and its primary keys, for the
  // reverse index cursor.
  struct SecondaryEntry {
    std::string secondary_key;
    std::vector<std::string> primary_keys;
  };
  std::optional<SecondaryEntry> fetch_last_secondary_entry_in_range(
      const std::string &index_name, const std::string &start_key,
      const std::string &end_key);
  bool update_secondary_index(
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
  // replay per recorded range; records nothing.
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
    autogen_tail_staged_.clear();
  }
  bool autogen_stmt_resolved() const { return autogen_stmt_resolved_; }
  void mark_autogen_stmt_resolved() { autogen_stmt_resolved_ = true; }
  // Subqueries may be staged before the statement root exists. Remember each
  // root so the same subquery plan is not staged twice in one statement.
  bool autogen_root_staged(const void *root) const {
    return autogen_staged_roots_.count(root) != 0;
  }
  void mark_autogen_root_staged(const void *root) {
    autogen_staged_roots_.insert(root);
  }
  // Key-less index_last tail windows staged this statement, keyed by table.
  bool autogen_tail_staged(const std::string &table_key) const {
    return autogen_tail_staged_.count(table_key) != 0;
  }
  void mark_autogen_tail_staged(const std::string &table_key) {
    autogen_tail_staged_.insert(table_key);
  }
  bool is_autogen_stmt_handler_deferred() const {
    return autogen_stmt_handler_deferred_;
  }
  void mark_autogen_stmt_handler_deferred() {
    autogen_stmt_handler_deferred_ = true;
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

  void add_rowcount_delta(LineairDB_share *share, const std::string &table_name, int64_t delta);
  int64_t peek_rowcount_delta(const LineairDB_share *share) const;

  // RPC trace statement boundary; TxRpcTrace dedupes repeated SQL strings.
  void on_stmt_boundary(const std::string& sql) { rpc_trace_.on_stmt(sql); }

  LineairDBTransaction(THD* thd,
                       LineairDBProxy* lineairdb_proxy,
                       handlerton* lineairdb_hton);
  ~LineairDBTransaction() = default;

private:
  LineairDBProxy* lineairdb_proxy;
  std::string db_table_key;
  THD* thread;
  bool isTransaction;
  handlerton* hton;
  bool registered_{false};
  bool tx_plan_used_{false};
  uint64_t autogen_query_id_{0};
  bool autogen_stmt_resolved_{false};
  std::unordered_set<const void*> autogen_staged_roots_;
  std::unordered_set<std::string> autogen_tail_staged_;
  // Set when this statement's plan is built from the handler index access
  // (deferred legacy-DML path) instead of the QEP; a second handler access
  // then means an index merge the single staged range cannot serve.
  bool autogen_stmt_handler_deferred_{false};

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
  };
  // These sets live only inside one LineairDBTransaction.
  // commit/abort deletes the object, so cached rows never cross txs.

  // Value cache for exact primary-key reads: serves a row to the statement
  // without an RPC. NOT a validation set -- the TID it carries feeds
  // base_row_read_set_ only when a cached row is actually consumed (the
  // cache-hit paths in read()/batch_read() append it). Every entry comes from
  // the storage, so every consume of one is an observation to validate.
  // Overwritten when a statement re-stages the key. Keyed by
  // (table_name + '\0' + key).
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
  // Write set for exact primary-key writes/deletes
  std::vector<LocalRowEntry> own_writes_;
  // Dedup/lookup index into own_writes_, keyed like row_cache_. own_writes_ only
  // ever grows by push_back, so a stored index never moves and stays valid.
  std::unordered_map<std::string, size_t> own_writes_index_;

  // OCC commit-time read validation, two append-only sets (Silo read_set
  // style); the server re-checks each at commit and aborts on a mismatch:
  //   base_row_read_set_  per-key TID of base rows -> value changes
  //   range_read_set_     observed range membership (result key-list),
  //                       re-validated by logical replay -> phantoms
  std::vector<LineairDBProxy::ReadEntry> base_row_read_set_;
  std::vector<LineairDBProxy::RangeReadEntry> range_read_set_;

  struct LocalRangeScanEntry {
    std::string table_name;
    std::string start_key;
    std::string end_key;
    bool reverse_scan;
    uint64_t row_limit = 0;
    std::vector<std::pair<std::string, std::string>> rows;
    std::vector<uint64_t> row_tids;
    // Set only on a lookup return copy: this window holds the first rows of
    // the range, not all of them.
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
  // Windows a read plan staged for this statement. A request a window covers
  // is served from it; anything else goes to the storage server.
  std::vector<LocalRangeScanEntry> range_scan_cache_;
  std::vector<LocalSecondaryScanEntry> secondary_scan_cache_;
  // Exact-start indexes for grouped range scans. Without these, each runtime
  // probe would scan the whole cache vector. Keyed table\x01index\x01start_key.
  std::unordered_map<std::string, std::vector<size_t>> range_scan_start_index_;
  std::unordered_map<std::string, std::vector<size_t>> secondary_scan_start_index_;
  void push_range_scan_cache(LocalRangeScanEntry entry);
  void push_secondary_scan_cache(LocalSecondaryScanEntry entry);
  // Keep the rows (pairs) the request's bounds cover. The parallel arrays are
  // appended together at every push site, so they are the same length.
  static void trim_range_entry(LocalRangeScanEntry& entry,
                               const std::string& start_key,
                               const std::string& end_key);
  static void trim_secondary_entry(LocalSecondaryScanEntry& entry,
                                   const std::string& start_key,
                                   const std::string& end_key);

  // Row and secondary-index ops in the order MySQL issued them
  std::vector<LineairDBProxy::WriteOp> write_buffer_ops_;
  // The index entries those ops leave behind: table\x01index -> secondary key
  // -> primary key -> still there. Ordered by secondary key so a probe or a
  // scan asks for one key or one range of it, instead of the walk of the whole
  // write buffer per row that made a bulk INSERT quadratic.
  std::unordered_map<
      std::string,
      std::map<std::string, std::unordered_map<std::string, bool>>>
      pending_index_entries_;
  void record_index_op(const LineairDBProxy::WriteOp& op);

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
  void append_range_read(const LocalRangeScanEntry& scanned);
  void append_secondary_range_read(const LocalSecondaryScanEntry& scanned);
  // The storage server refused the request (a missing table or index). Not
  // contention, but the statement cannot go on either.
  void abort_server_refused(const char* what);
  std::optional<LocalRangeScanEntry> lookup_range_scan_cache(
      const std::string& table_name, const std::string& start_key,
      const std::string& end_key, bool reverse_scan, uint64_t row_limit,
      bool allow_truncated) const;
  std::optional<LocalSecondaryScanEntry> lookup_secondary_scan_cache(
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
  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};

#endif /* LINEAIRDB_TRANSACTION_HH */
