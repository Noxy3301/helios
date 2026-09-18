#ifndef HELIOS_PROXY_H
#define HELIOS_PROXY_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>

#include "lineairdb.pb.h"

class TxRpcTrace;

// Frame header of one request or response (the server's message.hh), both
// fields in network order.
struct MessageHeader {
    uint32_t message_type;   // OpCode from protobuf
    uint32_t payload_size;   // size of the protobuf payload
};

// MessageType enum (corresponds to protobuf OpCode)
enum class MessageType : uint32_t {
    UNKNOWN = 0,

    // Reads
    TX_READ = 1,
    TX_BATCH_READ = 2,
    TX_SCAN = 3,
    TX_SCAN_INDEX = 4,
    TX_EXECUTE_READ_PLAN = 5,

    // The one commit of a transaction
    TX_COMMIT = 6,

    TX_GET_TABLE_STATS = 7,
    TX_EXECUTE_DUCKDB_QUERY = 8,

    // Definitions and server state
    DB_CREATE_TABLE = 9,
    DB_CREATE_SECONDARY_INDEX = 10,
    DB_ALLOCATE_HIDDEN_KEYS = 11,
    DB_SET_COMMIT_DURABILITY = 12
};

/**
 * RPC client for the storage server. Every read answers from the storage's
 * current state and leaves nothing behind there; the query layer keeps the
 * transaction and installs it with one TX_COMMIT.
 *
 * Each THD holds a HeliosProxy with its own TCP connection, managed via
 * HeliosThdCtx.
 */
class HeliosProxy {
public:
    HeliosProxy(const std::string& host, int port);
    ~HeliosProxy();

    // connection management
    bool connect(const std::string& host, int port);
    void disconnect();
    bool is_connected() const;

    struct IndexNdvResult {
        bool available = false;
        // values[i] is the NDV for the first i+1 key parts.
        std::vector<uint64_t> values;
    };
    struct IndexHistResult {
        bool available = false;
        std::vector<std::string> bounds;
        std::vector<uint64_t> cum;
    };
    // Refresh row counts, and optionally per-index NDV for one table.
    bool fetch_table_stats(
        const std::string& ndv_table = std::string(),
        const std::vector<std::pair<std::string, uint32_t>>& ndv_indexes = {},
        bool force_ndv = false);
    const std::unordered_map<std::string, IndexNdvResult>& last_index_ndv() const {
        return last_index_ndv_;
    }
    const std::unordered_map<std::string, IndexHistResult>& last_index_hist() const {
        return last_index_hist_;
    }

    // One row by primary key. ok is false when the request did not reach the
    // server; found says whether the key holds a row, tid is what the commit
    // validates.
    struct ReadResult {
        bool ok = false;
        bool found = false;
        std::string value;
        uint64_t tid = 0;
    };
    struct ReadKey {
        std::string table_name;
        std::string key;
    };
    ReadResult tx_read(const std::string& table_name, const std::string& key);
    std::vector<ReadResult> tx_batch_read(const std::vector<ReadKey>& keys);

    // Rows of [start_key, end_key) in key order, reversed when reverse_scan.
    // row_limit 0 means every row; keys_only leaves value empty. ok is false
    // when the table (or index) is missing or end_key is empty.
    struct ScanRow {
        std::string key;
        std::string value;
        uint64_t tid = 0;
    };
    struct ScanResult {
        bool ok = false;
        // The exchange itself failed, which is not contention and never a
        // refusal by the server.
        bool transport_error = false;
        std::vector<ScanRow> rows;
    };
    ScanResult tx_scan(const std::string& table_name,
                       const std::string& start_key,
                       const std::string& end_key, uint64_t row_limit,
                       bool reverse_scan, bool keys_only);

    struct ScanIndexRow {
        std::string secondary_key;
        std::string primary_key;
        std::string value;
        uint64_t tid = 0;  // the base row's TID
    };
    struct ScanIndexResult {
        bool ok = false;
        bool transport_error = false;
        std::vector<ScanIndexRow> rows;
    };
    ScanIndexResult tx_scan_index(const std::string& table_name,
                                  const std::string& index_name,
                                  const std::string& start_key,
                                  const std::string& end_key,
                                  uint64_t row_limit, bool reverse_scan,
                                  bool keys_only);

    // One row the transaction read, validated by its TID at commit.
    struct ReadEntry {
        std::string table_name;
        std::string key;
        uint64_t tid = 0;
    };
    // One range the transaction scanned. The commit re-runs the scan these
    // bounds describe and requires the same key list.
    struct RangeReadEntry {
        std::string table_name;
        std::string index_name;
        std::string start_key;
        std::string end_key;
        uint64_t row_limit = 0;
        bool reverse_scan = false;
        std::vector<std::string> result_keys;
        std::vector<std::string> result_primary_keys;
    };
    struct ReadPlanKeyBinding {
        uint32_t source_step = 0;
        uint32_t source_row = 0;
        uint32_t source_offset = 0;
        uint32_t source_length = 0;
        bool use_midpoint = false;
        bool from_key = false;
        int32_t source_column = 0;
        bool column_as_int_key = false;
        int64_t int_delta = 0;
    };
    struct ReadPlanStep {
        std::string table_name;
        std::string key_prefix;
        std::string end_key_prefix;
        bool is_scan = false;
        uint64_t scan_limit = 0;
        std::string index_name;
        std::vector<ReadPlanKeyBinding> bindings;
        std::vector<ReadPlanKeyBinding> end_bindings;
        bool for_each = false;
        bool reverse_scan = false;
    };
    struct ReadPlanStepResult {
        bool found = false;
        std::string value;
        uint64_t tid = 0;
        std::string actual_key;
        std::vector<std::string> scan_keys;
        std::vector<std::string> scan_values;
        std::vector<uint64_t> scan_tids;
        std::vector<std::string> secondary_keys;
        std::string actual_start_key;
        std::string actual_end_key;
        // Per-probe groups for for_each range scans; sizes split scan arrays.
        std::vector<uint32_t> group_sizes;
        std::vector<std::string> group_start_keys;
        std::vector<std::string> group_end_keys;
    };
    struct ReadPlanResult {
        bool ok = false;
        // No valid response came back at all, as opposed to a plan the server
        // refused: a lost connection is not contention.
        bool transport_error = false;
        std::vector<ReadPlanStepResult> steps;
    };
    ReadPlanResult tx_execute_read_plan(
        const std::vector<ReadPlanStep>& steps);
    bool tx_execute_duckdb_query(
        const LineairDB::Protocol::TxExecuteDuckdbQuery::Request& request,
        LineairDB::Protocol::TxExecuteDuckdbQuery::Response* response);

    // One row or secondary-index change the transaction installs at commit,
    // kept in the order the query layer issued it.
    struct WriteOp {
        enum class Type {
            Write,
            Delete,
            SecondaryIndexWrite,
            SecondaryIndexDelete
        };
        Type type;
        std::string key;
        std::string value;
        std::string index_name;
        std::string secondary_key;
        std::string primary_key;
        std::string table_name;
        // Row write that must find the key free; the server refuses it
        // otherwise.
        bool is_insert = false;
    };
    // Validate every read and range, then install the writes. duplicate_key is
    // set when the server refused a duplicate primary or unique secondary key.
    bool tx_commit(
        const std::vector<ReadEntry>& reads,
        const std::vector<RangeReadEntry>& range_reads,
        const std::vector<WriteOp>& ops,
        const std::vector<std::pair<std::string, int64_t>>& row_deltas,
        std::string* abort_detail = nullptr,
        bool* duplicate_key = nullptr,
        bool* transport_error = nullptr);

    // table/index management (non-transactional)
    // PAX cell widths in bytes (entry 0 = null flags, then TABLE::field
    // order); kind/scale carry typed cells, one entry per width.
    bool db_create_table(const std::string& table_name,
                         const std::vector<uint32_t>& pax_field_max_bytes,
                         const std::vector<uint32_t>& pax_field_kind,
                         const std::vector<int32_t>& pax_field_scale);
    bool db_create_secondary_index(const std::string& table_name,
                                   const std::string& index_name,
                                   uint32_t index_type);
    struct HiddenKeyReservation {
        bool ok = false;
        // The requested count starting here belongs to this query layer
        uint64_t first_id = 0;
        // The run of the storage server that granted it
        uint64_t boot_token = 0;
        // The server rejected the request and will reject it again
        bool permanent = false;
        bool transport_error = false;
        std::string error;
    };
    // Reserve hidden primary keys for a table that declares none. The server
    // owns the counter, so the range is unique across every query layer
    // sharing this storage.
    HiddenKeyReservation db_allocate_hidden_keys(const std::string& table_name,
                                                 uint32_t count);

    // statistics: cached table row counts, refreshed by every commit
    const std::unordered_map<std::string, int64_t>& cached_table_stats() const {
        return table_stats_cache_;
    }

    // Route per-RPC measurements to the active transaction trace.
    void set_current_trace(TxRpcTrace* trace) { current_trace_ = trace; }

    // Run of the storage server this connection has heard from; 0 until heard
    // and after any transport failure, which forces re-reservation.
    uint64_t storage_boot_token() const { return storage_boot_token_; }

private:
    std::unordered_map<std::string, int64_t> table_stats_cache_;
    std::unordered_map<std::string, IndexNdvResult> last_index_ndv_;
    std::unordered_map<std::string, IndexHistResult> last_index_hist_;
    template<typename RequestType, typename ResponseType>
    bool send_protobuf_message(const RequestType& request, ResponseType& response,
                               MessageType message_type, const std::string& meta = "");
    // Send protobuf request, receive raw binary response
    template<typename RequestType>
    bool send_protobuf_recv_binary(const RequestType& request, std::string& raw_response,
                                   MessageType message_type, const std::string& meta = "");
    bool send_message_with_header(const std::string& serialized_request,
                                  std::string& serialized_response,
                                  MessageType message_type,
                                  const std::string& meta = "");
    // The exchange itself; send_message_with_header wraps it so that every
    // transport failure invalidates the boot token.
    bool exchange_message(const std::string& serialized_request,
                          std::string& serialized_response,
                          MessageType message_type, const std::string& meta);

    // Connect on demand so a channel closed by a transport error is reopened
    // by the next RPC.
    bool ensure_connected();

    uint64_t storage_boot_token_ = 0;
    int socket_fd_;
    bool connected_;
    std::string host_;
    int port_;
    TxRpcTrace* current_trace_ = nullptr;
};

#endif // HELIOS_PROXY_H
