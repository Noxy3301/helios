#ifndef LINEAIRDB_TRANSACTION_HH
#define LINEAIRDB_TRANSACTION_HH

#include <optional>
#include <unordered_map>

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
  void prefetch_stateless_reads(
      const std::vector<LineairDBProxy::StatelessReadKey>& reads);
  void execute_read_plan(const std::vector<LineairDBProxy::ReadPlanStep>& steps);

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
  bool oneshot_registered_{false};

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
  std::vector<LineairDBProxy::RangeValidationEntry> range_validation_set_;
  std::vector<LineairDBProxy::IndexValidationEntry> index_validation_set_;

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
  std::vector<LocalRangeScanEntry> local_range_scans_;
  std::vector<LocalSecondaryScanEntry> local_secondary_scans_;

  // Predicate pushdown: serialized PushedPredicate for scan filtering
  std::string pushed_filter_;

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
  std::optional<LocalRangeScanEntry> lookup_local_range_scan(
      const std::string& table_name, const std::string& start_key,
      const std::string& end_key, bool reverse_scan, uint64_t row_limit) const;
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
