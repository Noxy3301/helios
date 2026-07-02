#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
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

class LineairDBRpc {
public:
    LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                 std::shared_ptr<TransactionManager> tx_manager,
                 std::shared_ptr<TableRowCounts> row_counts);
    ~LineairDBRpc() = default;

    void handle_rpc(uint64_t sender_id, MessageType message_type,
                   const std::string& message, std::string& result);

private:
    std::shared_ptr<DatabaseManager> db_manager_;
    std::shared_ptr<TransactionManager> tx_manager_;
    std::shared_ptr<TableRowCounts> row_counts_;

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

    // Transaction lifecycle
    void handleTxBeginTransaction(const std::string& message, std::string& result);
    void handleTxGetTableStats(const std::string& message, std::string& result);
    void handleTxAbort(const std::string& message, std::string& result);

    // Primary key operations
    void handleTxRead(const std::string& message, std::string& result);
    void handleTxBatchRead(const std::string& message, std::string& result);
    void handleTxBatchWrite(const std::string& message, std::string& result);
    void handleTxStatelessRead(const std::string& message, std::string& result);
    void handleTxStatelessBatchRead(const std::string& message, std::string& result);
    void handleTxExecuteReadPlan(const std::string& message, std::string& result);
    void handleTxExecuteQueryBlock(const std::string& message, std::string& result);
    void handleTxValidateAndCommit(const std::string& message, std::string& result);
    void handleTxWrite(const std::string& message, std::string& result);
    void handleTxDelete(const std::string& message, std::string& result);

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

    // Database operations
    void handleDbFence(const std::string& message, std::string& result);
    void handleDbEndTransaction(const std::string& message, std::string& result);
    void handleDbCreateTable(const std::string& message, std::string& result);
    void handleDbSetTable(const std::string& message, std::string& result);
    void handleDbCreateSecondaryIndex(const std::string& message, std::string& result);

    // utility
    bool key_prefix_is_matching(const std::string& key_prefix, const std::string& key);
};
