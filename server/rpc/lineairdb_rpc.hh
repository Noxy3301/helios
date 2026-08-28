#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../protocol/message.hh"
#include "../storage/database_manager.hh"
#include "../storage/transaction_manager.hh"

// Server-wide table row count tracker, shared across all connections.
struct TableRowCounts {
    std::shared_mutex s_mutex;
    std::unordered_map<std::string, int64_t> counts;

    template <typename T>
    void apply_deltas(const T& deltas) {
        std::unique_lock<std::shared_mutex> lock(s_mutex);
        for (const auto& row_delta : deltas) {
            auto& count = counts[row_delta.table_name()];
            count += row_delta.delta();
            if (count < 0) count = 0;
        }
    }

    std::unordered_map<std::string, int64_t> snapshot() {
        std::shared_lock<std::shared_mutex> lock(s_mutex);
        return counts;
    }
};

// Identifies this run of the storage server: a hidden-key range is valid only
// against the run that granted it. Derived from startup time so it moves
// forward across restarts unless the clock steps back. Never zero.
uint64_t storage_boot_token();

/**
 * @brief Server-wide source of the hidden primary keys handed to tables that
 * declare none, shared across all connections.
 *
 * A range is safe to use only while the boot token it was granted under is
 * still current; the persisted watermark alone does not make a restart safe,
 * because a reservation is not durable when it is handed out.
 */
class HiddenKeyAllocator {
public:
    /**
     * @brief Reserves [*first_id, *first_id + count) for the caller.
     *
     * @param[out] error   Why the reservation failed; untouched on success.
     * @param[out] permanent  True when retrying cannot help: a bad count, a
     *   missing key table, an exhausted space. Thread-safe.
     * @return false when nothing was reserved.
     */
    bool Allocate(LineairDB::Database& database, const std::string& table_name,
                  uint32_t count, uint64_t* first_id, std::string* error,
                  bool* permanent);

    // Forgets a table's counter and any standing refusal, so a re-created table
    // is reconsidered from storage.
    void ForgetTable(const std::string& table_name);

    // Largest range one request may reserve
    static constexpr uint32_t kMaxCount = 65536;

private:
    std::mutex mutex_;
    bool watermark_table_ready_ = false;

    // Refusals that are a property of the table rather than of the moment, so
    // a rejected INSERT does not re-ask behind every row.
    std::unordered_map<std::string, std::string> refused_;

    // Tables whose resume point has been logged, so the line appears once per
    // table instead of once per reservation.
    std::unordered_set<std::string> announced_;
};

class LineairDBRpc {
public:
    LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                 std::shared_ptr<TransactionManager> tx_manager,
                 std::shared_ptr<TableRowCounts> row_counts,
                 std::shared_ptr<HiddenKeyAllocator> hidden_keys);
    ~LineairDBRpc() = default;

    void handle_rpc(uint64_t sender_id, MessageType message_type,
                   const std::string& message, std::string& result);

private:
    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TransactionManager> tx_manager_;
    std::shared_ptr<TableRowCounts> row_counts_;
    std::shared_ptr<HiddenKeyAllocator> hidden_keys_;

    // NDV cache shared by all RPC connections. Key is table\0index\0parts.
    static std::mutex ndv_cache_mu_;
    static std::unordered_map<std::string,
                              std::pair<bool, std::vector<uint64_t>>> ndv_cache_;

    // Range histogram cache shared with the NDV stats fetch path.
    struct HistEntry {
        bool available = false;
        std::vector<std::string> bounds;
        std::vector<uint64_t> cum;
    };
    static std::unordered_map<std::string, HistEntry> hist_cache_;

    // Transaction control
    void handleTxBeginTransaction(const std::string& message, std::string& result);
    void handleTxAbort(const std::string& message, std::string& result);
    void handleDbEndTransaction(const std::string& message, std::string& result);
    void handleDbSetTable(const std::string& message, std::string& result);

    // Primary key operations
    void handleTxRead(const std::string& message, std::string& result);
    void handleTxBatchRead(const std::string& message, std::string& result);
    void handleTxBatchWrite(const std::string& message, std::string& result);
    void handleTxWrite(const std::string& message, std::string& result);
    void handleTxDelete(const std::string& message, std::string& result);

    // Stateless operations
    void handleTxStatelessRead(const std::string& message, std::string& result);
    void handleTxStatelessBatchRead(const std::string& message, std::string& result);
    void handleTxValidateAndCommit(const std::string& message, std::string& result);

    // Read-plan execution
    void handleTxExecuteReadPlan(const std::string& message, std::string& result);

    // DuckDB bridge (resolved-statement request over live PAX storage).
    void handleTxExecuteDuckdbQuery(const std::string& message,
                                    std::string& result);

    // Secondary index operations
    void handleTxReadSecondaryIndex(const std::string& message, std::string& result);
    void handleTxWriteSecondaryIndex(const std::string& message, std::string& result);
    void handleTxDeleteSecondaryIndex(const std::string& message, std::string& result);
    void handleTxUpdateSecondaryIndex(const std::string& message, std::string& result);

    // Primary key scan operations
    void handleTxGetMatchingKeysInRange(const std::string& message, std::string& result);
    void handleTxGetMatchingKeysAndValuesInRange(const std::string& message, std::string& result);
    void handleTxGetMatchingKeysAndValuesFromPrefix(const std::string& message, std::string& result);
    void handleTxFetchLastKeyInRange(const std::string& message, std::string& result);
    void handleTxFetchFirstKeyWithPrefix(const std::string& message, std::string& result);
    void handleTxFetchNextKeyWithPrefix(const std::string& message, std::string& result);

    // Secondary index scan operations
    void handleTxGetMatchingPrimaryKeysInRange(const std::string& message, std::string& result);
    void handleTxGetMatchingPrimaryKeysFromPrefix(const std::string& message, std::string& result);
    void handleTxFetchLastPrimaryKeyInSecondaryRange(const std::string& message, std::string& result);
    void handleTxFetchLastSecondaryEntryInRange(const std::string& message, std::string& result);

    // Table statistics
    void handleTxGetTableStats(const std::string& message, std::string& result);

    // Database operations
    void handleDbFence(const std::string& message, std::string& result);
    void handleDbCreateTable(const std::string& message, std::string& result);
    void handleDbCreateSecondaryIndex(const std::string& message, std::string& result);
    void handleDbAllocateHiddenKeys(const std::string& message, std::string& result);
    void handleDbSetCommitDurability(const std::string& message,
                                     std::string& result);

    // utility
    bool key_prefix_is_matching(const std::string& key_prefix, const std::string& key);
};
