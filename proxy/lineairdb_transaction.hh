#ifndef LINEAIRDB_TRANSACTION_HH
#define LINEAIRDB_TRANSACTION_HH

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
                   const std::vector<LineairDBProxy::BatchWriteOp>& writes,
                   const std::vector<LineairDBProxy::BatchSecondaryIndexOp>& si_writes);
  std::vector<std::string> get_all_keys();
  std::vector<std::string> get_matching_keys(std::string key);
  std::vector<std::string> get_matching_keys_in_range(std::string start_key, std::string end_key);
  // Server-side LIMIT pushdown. 0 = unlimited. When N > 0, server stops
  // after N matches — caller must guarantee scan order matches the SQL
  // result order, else leave 0 and let MySQL truncate.
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_in_range(
      std::string start_key, std::string end_key, uint32_t limit = 0);
  std::vector<std::pair<std::string, std::string>> get_matching_keys_and_values_from_prefix(
      std::string prefix);
  bool write(std::string key, const std::string value);
  bool write_secondary_index(std::string index_name, std::string secondary_key, const std::string primary_key);
  std::vector<std::string> read_secondary_index(std::string index_name, std::string secondary_key);
  std::vector<std::string> get_matching_primary_keys_in_range(
      std::string index_name, std::string start_key, std::string end_key);
  std::vector<std::string> get_matching_primary_keys_from_prefix(
      std::string index_name, std::string prefix);
  // Combined SI scan + value fetch: returns (primary_key, value) pairs in
  // one round trip instead of the legacy SI-scan -> batch_read pattern.
  // The PK cache is also populated, so subsequent read() calls on these
  // PKs are served without an RPC.
  std::vector<std::pair<std::string, std::string>>
  get_matching_keys_and_values_in_index_range(
      std::string index_name, std::string start_key, std::string end_key);
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
  // Flush all buffered writes to LineairDB. Used at commit or before operations
  // that require a globally materialized transaction state.
  bool flush_write_buffer();

  void begin_transaction();
  void set_status_to_abort();
  // out_transport_error (when non-null) reports whether a return value of
  // false was caused by a transport-layer failure rather than a clean OCC
  // abort from the server. Lets the handler distinguish retryable OCC aborts
  // from ambiguous network failures.
  bool end_transaction(bool* out_transport_error = nullptr);
  void fence() const;
  

  inline bool is_not_started() const {
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

  // Transport error ledger. Latches on first sighting so a mid-tx network
  // failure (auto-flush, direct SI RPC, etc.) cannot be hidden by a later
  // successful RPC. Also poisons the tx as aborted: a transport-failed RPC
  // leaves the tx in an unknown state (request may or may not have reached
  // the server), so subsequent buffered writes/reads must short-circuit
  // instead of silently appearing to succeed. end_transaction folds this
  // into the commit-time transport classification.
  inline void mark_transport_error() {
    transport_error_seen_ = true;
    is_aborted_ = true;
  }
  inline bool transport_error_seen() const { return transport_error_seen_; }

  inline bool is_a_single_statement() const { return !isTransaction; }

  // Predicate pushdown: serialized PushedPredicate protobuf
  void set_pushed_filter(const std::string& s) { pushed_filter_ = s; }
  const std::string& get_pushed_filter() const { return pushed_filter_; }
  void clear_pushed_filter() { pushed_filter_.clear(); }

  void add_rowcount_delta(LineairDB_share *share, const std::string &table_name, int64_t delta);
  int64_t peek_rowcount_delta(const LineairDB_share *share) const;

  // RPC trace hooks. Push a statement-boundary marker into the trace at
  // each external_lock call; the underlying TxRpcTrace dedupes by SQL
  // string so multi-table statements only push one boundary.
  void on_stmt_boundary(const std::string& sql) { rpc_trace_.on_stmt(sql); }
  TxRpcTrace& rpc_trace() { return rpc_trace_; }


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

  // stores the last RPC read result to maintain data pointer validity
  std::string last_read_value_;

  // transaction abort status (updated by RPC responses)
  bool is_aborted_;

  // Latched once any per-tx RPC failed at transport level. Never reset
  // within a single tx lifetime; the only way to clear it is to start a
  // fresh LineairDBTransaction.
  bool transport_error_seen_ = false;

  struct RowCountDelta {
    LineairDB_share *share;
    std::string table_name;
    int64_t delta;
  };
  std::vector<RowCountDelta> rowcount_deltas_;

  // Predicate pushdown: serialized PushedPredicate for scan filtering
  std::string pushed_filter_;

  // Write buffers for batch write operations. Buffers are table-scoped so a
  // read/scan on one table does not force unrelated writes on another table
  // to be sent early.
  static constexpr size_t WRITE_BATCH_SIZE = 100;
  struct PendingWriteBuffer {
    std::vector<LineairDBProxy::BatchWriteOp> writes;
    std::vector<LineairDBProxy::BatchSecondaryIndexOp> secondary_index_writes;
  };
  std::unordered_map<std::string, PendingWriteBuffer> write_buffers_;

  // Per-tx RPC trace. Inert when ENABLE_RPC_TRACE is unset (record() bails
  // early on !active()). Activated in begin_transaction; finalized in
  // end_transaction / set_status_to_abort.
  TxRpcTrace rpc_trace_;

  // Per-tx PK row cache. Per-tx scope is mandatory — sharing across tx
  // would break 1SR.
  std::unordered_map<std::string, std::string> read_cache_;
  // Negative cache: keys this tx already confirmed not-found.
  std::unordered_set<std::string> read_cache_misses_;

  // Builds the cache key for (table, key). Encoded as table_len|table|key_len|key
  // so arbitrary bytes in either component cannot collide via a delimiter.
  static std::string make_pk_cache_key(const std::string& table, const std::string& key);
  // Drops one (table, key) entry from both positive and negative caches.
  // Called by every write / delete path before the RPC.
  void invalidate_pk_cache_entry(const std::string& table, const std::string& key);

  // Flush only table_name so unrelated table buffers can stay lazy
  bool flush_write_buffer_for_table(const std::string& table_name);
  // True when table_name has any pending base-row or secondary-index write
  bool table_has_pending_writes(const std::string& table_name) const;
  // True when a pending base-row write targets exactly this primary key
  bool table_has_pending_write_for_key(const std::string& table_name,
                                       const std::string& key) const;
  // True when a pending base-row write overlaps this primary-key scan range
  bool table_has_pending_write_in_range(const std::string& table_name,
                                        const std::string& start_key,
                                        const std::string& end_key) const;
  // True when a pending base-row write overlaps this primary-key prefix scan
  bool table_has_pending_write_with_prefix(const std::string& table_name,
                                           const std::string& prefix) const;

  // Empties both caches. Called on abort, since aborted writes are rolled back
  // server-side and any cached reads from this tx may now be stale.
  void clear_read_cache();

  bool thd_is_transaction() const;
  void register_transaction_to_mysql();
  void register_single_statement_to_mysql();
};

#endif /* LINEAIRDB_TRANSACTION_HH */
