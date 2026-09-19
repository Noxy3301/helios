// The RPC surface of the storage server: the handler for every arm of the
// request envelope, plus the server-wide row counts, boot token and
// hidden-key allocator they use.
#pragma once

#include <chrono>
#include <string>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "helios.pb.h"

#include "../database_manager.hh"

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
    // Identifies this run of the storage server: a range is valid only against
    // the run that granted it. Restarts are milliseconds apart, so the clock
    // moves it forward.
    const uint64_t boot_token = static_cast<uint64_t>(
        std::chrono::system_clock::now().time_since_epoch().count());

    /**
     * @brief Reserves [*first_id, *first_id + count) for the caller.
     *
     * @param[out] error   Why the reservation failed; untouched on success.
     * @param[out] permanent  True when retrying cannot help: a bad count, a
     *   missing key table, an exhausted space. Thread-safe.
     * @return false when nothing was reserved.
     */
    bool Allocate(helios::storage::Database& database, const std::string& table_name,
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

class HeliosRpc {
public:
    HeliosRpc(std::shared_ptr<DatabaseManager> db_manager,
                 std::shared_ptr<TableRowCounts> row_counts,
                 std::shared_ptr<HiddenKeyAllocator> hidden_keys);
    ~HeliosRpc() = default;

    // Parses one request envelope, routes the arm it sets, and serializes the
    // response envelope into result. payload takes the bytes that ride raw
    // after the envelope, which only the read-plan reply carries. Returns
    // false when the response cannot be serialized, which leaves the request
    // unanswered.
    bool handle_rpc(const std::string& message, std::string& result,
                    std::string& payload);

private:
    std::shared_ptr<DatabaseManager> db_manager_;
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

    // Reads, scans and the commit of a transaction
    void handleTxRead(
        const Helios::Protocol::TxRead::Request& request,
        Helios::Protocol::TxRead::Response* response);
    void handleTxBatchRead(
        const Helios::Protocol::TxBatchRead::Request& request,
        Helios::Protocol::TxBatchRead::Response* response);
    void handleTxScan(
        const Helios::Protocol::TxScan::Request& request,
        Helios::Protocol::TxScan::Response* response);
    void handleTxScanIndex(
        const Helios::Protocol::TxScanIndex::Request& request,
        Helios::Protocol::TxScanIndex::Response* response);
    void handleTxCommit(
        const Helios::Protocol::TxCommit::Request& request,
        Helios::Protocol::TxCommit::Response* response);

    // Read-plan execution. The reply is the flat HELIOSRP payload, which
    // rides raw after the envelope.
    void handleTxExecuteReadPlan(
        const Helios::Protocol::TxExecuteReadPlan::Request& request,
        std::string* result);

    // DuckDB bridge (resolved-statement request over live PAX storage).
    void handleTxExecuteDuckdbQuery(
        const Helios::Protocol::TxExecuteDuckdbQuery::Request& request,
        Helios::Protocol::TxExecuteDuckdbQuery::Response* response);

    // Table statistics
    void handleTxGetTableStats(
        const Helios::Protocol::GetTableStats::Request& request,
        Helios::Protocol::GetTableStats::Response* response);

    // Database operations
    void handleDbCreateTable(
        const Helios::Protocol::DbCreateTable::Request& request,
        Helios::Protocol::DbCreateTable::Response* response);
    void handleDbCreateSecondaryIndex(
        const Helios::Protocol::DbCreateSecondaryIndex::Request& request,
        Helios::Protocol::DbCreateSecondaryIndex::Response* response);
    void handleDbAllocateHiddenKeys(
        const Helios::Protocol::DbAllocateHiddenKeys::Request& request,
        Helios::Protocol::DbAllocateHiddenKeys::Response* response);
    void handleDbSetCommitDurability(
        const Helios::Protocol::DbSetCommitDurability::Request& request,
        Helios::Protocol::DbSetCommitDurability::Response* response);
};
