#ifndef LINEAIRDB_PROXY_H
#define LINEAIRDB_PROXY_H

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>

#include "lineairdb.pb.h"

class LineairDBTransaction;
class TxRpcTrace;

struct KeyValue {
    std::string key;
    std::string value;
};

struct SecondaryIndexEntry {
    std::string secondary_key;
    std::vector<std::string> primary_keys;
};

// Message header for RPC communication. MUST match server/protocol/message.hh
// byte-for-byte (both peers memcpy this struct over the wire).
struct MessageHeader {
    uint64_t sender_id;      // sender ID
    uint32_t message_type;   // OpCode from protobuf
    uint32_t _pad;           // explicit padding so payload_size is 8-aligned
    uint64_t payload_size;   // payload size (uint64: prefetch responses >4GB)
};

// MessageType enum (corresponds to protobuf OpCode)
enum class MessageType : uint32_t {
    UNKNOWN = 0,

    // Transaction lifecycle
    TX_BEGIN_TRANSACTION = 1,
    TX_ABORT = 2,

    // Primary key operations
    TX_READ = 3,
    TX_WRITE = 4,
    TX_DELETE = 5,

    // Secondary index operations
    TX_READ_SECONDARY_INDEX = 6,
    TX_WRITE_SECONDARY_INDEX = 7,
    TX_DELETE_SECONDARY_INDEX = 8,
    TX_UPDATE_SECONDARY_INDEX = 9,

    // Primary key scan operations
    TX_GET_MATCHING_KEYS_IN_RANGE = 10,
    TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE = 11,
    TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX = 12,
    TX_FETCH_LAST_KEY_IN_RANGE = 13,
    TX_FETCH_FIRST_KEY_WITH_PREFIX = 14,
    TX_FETCH_NEXT_KEY_WITH_PREFIX = 15,

    // Secondary index scan operations
    TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE = 16,
    TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX = 17,
    TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE = 18,
    TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE = 19,

    // Database operations
    DB_FENCE = 20,
    DB_END_TRANSACTION = 21,
    DB_CREATE_TABLE = 22,
    DB_SET_TABLE = 23,
    DB_CREATE_SECONDARY_INDEX = 24,

    // Batch operations
    TX_BATCH_READ = 25,
    TX_BATCH_WRITE = 26,

    // Experimental one-shot operations
    TX_STATELESS_READ = 27,
    TX_STATELESS_BATCH_READ = 28,
    TX_VALIDATE_AND_COMMIT = 29,
    TX_STATELESS_RANGE_SCAN = 30,
    TX_STATELESS_SECONDARY_RANGE_SCAN = 31,
    TX_EXECUTE_READ_PLAN = 32
};

/**
 * RPC client that provides the same transactional API as LineairDB,
 * but internally forwards all operations to a remote server via RPC over TCP.
 *
 * In this disaggregated architecture, MySQL instances do not embed LineairDB
 * directly; instead, each THD holds a LineairDBProxy that maintains a
 * TCP connection to the remote LineairDB server. Managed via LineairDBThdCtx.
 */
class LineairDBProxy {
public:
    LineairDBProxy(const std::string& host, int port);
    ~LineairDBProxy();

    // connection management
    bool connect(const std::string& host, int port);
    void disconnect();
    bool is_connected() const;

    // transaction management
    int64_t tx_begin_transaction();
    void tx_abort(int64_t tx_id);

    // primary key operations
    std::string tx_read(LineairDBTransaction* tx, const std::string& key);
    bool tx_write(LineairDBTransaction* tx, const std::string& key, const std::string& value);
    bool tx_delete(LineairDBTransaction* tx, const std::string& key);

    // batch operations
    struct BatchReadResult {
        bool found;
        std::string value;
    };
    struct StatelessReadResult {
        bool ok = false;
        bool found = false;
        std::string value;
        uint64_t tid = 0;
    };
    struct StatelessReadKey {
        std::string table_name;
        std::string key;
    };
    struct RangeValidationEntry {
        std::string table_name;
        std::string index_name;
        uint64_t owner_ptr;
        uint64_t node_ptr;
        uint64_t version;
        std::string start_key;
        std::string end_key;
        uint64_t row_limit = 0;
        bool reverse_scan = false;
        std::vector<std::string> result_keys;
        std::vector<std::string> result_primary_keys;
    };
    struct IndexValidationEntry {
        std::string table_name;
        std::string index_name;
        std::string key;
        uint64_t tid;
        bool found;
    };
    struct StatelessRangeScanRow {
        std::string key;
        std::string value;
        bool found;
        uint64_t tid;
    };
    struct StatelessRangeScanResult {
        bool ok = false;
        std::vector<StatelessRangeScanRow> rows;
        std::vector<RangeValidationEntry> range_versions;
        std::vector<IndexValidationEntry> index_reads;
    };
    struct StatelessSecondaryRangeScanRow {
        std::string secondary_key;
        std::string primary_key;
        std::string value;
        bool found;
        uint64_t tid;
    };
    struct StatelessSecondaryRangeScanResult {
        bool ok = false;
        std::vector<StatelessSecondaryRangeScanRow> rows;
        std::vector<RangeValidationEntry> range_versions;
        std::vector<IndexValidationEntry> index_reads;
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
        // Serialized PushedPredicate (proto) applied on the server side when
        // the step is a primary-PK scan (is_scan && !for_each && index_name
        // empty). Populated by LineairDBTransaction::execute_read_plan from
        // tx->pushed_filter_ at send time. Empty = no filter.
        std::string filter_serialized;
        // Projection pushdown: kept base-row field indices (0-based, ascending,
        // unique). EMPTY = no projection => server returns the full row (legacy).
        // projection_num_columns = table->s->fields when projecting.
        std::vector<uint32_t> projection;
        uint32_t projection_num_columns = 0;
    };
    // One per-source-row sub-scan from a for_each && is_scan step (FER/FES).
    struct ReadPlanSubScan {
        std::string start_key;
        std::string end_key;
        std::vector<std::string> scan_keys;       // primary keys
        std::vector<std::string> scan_values;
        std::vector<uint64_t> scan_tids;
        std::vector<std::string> secondary_keys;  // secondary scans only
        std::vector<RangeValidationEntry> range_versions;
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
        std::vector<RangeValidationEntry> range_versions;
        std::vector<IndexValidationEntry> index_reads;
        std::vector<ReadPlanSubScan> subscans;
        // (c) filter-rejected rows in the range + their TIDs (validating reads)
        std::vector<std::string> filtered_keys;
        std::vector<uint64_t> filtered_tids;
    };
    struct ReadPlanResult {
        bool ok = false;
        std::vector<ReadPlanStepResult> steps;
        std::vector<RangeValidationEntry> range_versions;
        std::vector<IndexValidationEntry> index_reads;
        // Non-zero in PHYSICAL OCC mode: opaque token the proxy echoes back in
        // TX_VALIDATE_AND_COMMIT.Request.tx_occ_key. The server retains the
        // full NodeVersionEntries + filtered/tombstone (key,tid) under this
        // key; the proxy holds NO OCC bookkeeping for the prefetched ranges.
        uint64_t tx_occ_key = 0;
    };
    std::vector<BatchReadResult> tx_batch_read(LineairDBTransaction* tx,
                                                const std::vector<std::string>& keys);
    struct BatchOp {
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
    };
    bool tx_batch_write(LineairDBTransaction* tx,
                        const std::string& table_name,
                        const std::vector<BatchOp>& ops);
    StatelessReadResult tx_stateless_read(const std::string& table_name,
                                          const std::string& key);
    std::vector<StatelessReadResult> tx_stateless_batch_read(
        const std::vector<StatelessReadKey>& keys);
    StatelessRangeScanResult tx_stateless_range_scan(
        const std::string& table_name,
        const std::string& start_key,
        const std::string& end_key,
        uint64_t row_limit,
        bool reverse_scan);
    StatelessSecondaryRangeScanResult tx_stateless_secondary_range_scan(
        const std::string& table_name,
        const std::string& index_name,
        const std::string& start_key,
        const std::string& end_key,
        uint64_t row_limit,
        bool reverse_scan);
    ReadPlanResult tx_execute_read_plan(
        const std::vector<ReadPlanStep>& steps);
    bool tx_validate_and_commit(
        const std::vector<StatelessReadKey>& reads,
        const std::vector<uint64_t>& read_tids,
        const std::vector<bool>& read_found,
        const std::vector<RangeValidationEntry>& range_reads,
        const std::vector<IndexValidationEntry>& index_reads,
        const std::vector<BatchOp>& ops,
        const std::vector<std::pair<std::string, int64_t>>& row_deltas,
        bool isFence,
        std::string* abort_reason = nullptr,
        // helios 2-RPC OCC (Step 6): when non-zero, echo this key in the
        // request so the server merges its retained TxOccState entries
        // (range_versions + filtered + tombstones) into the validate input.
        uint64_t tx_occ_key = 0,
        // helios Phase-6 range-hash OCC: read-only only. Tells the server to
        // revalidate full-cover ranges via retained footprint digests (the
        // proxy skipped their per-row reads).
        bool use_range_hash = false);

    // secondary index operations
    std::vector<std::string> tx_read_secondary_index(LineairDBTransaction* tx,
                                                     const std::string& index_name,
                                                     const std::string& secondary_key);
    bool tx_write_secondary_index(LineairDBTransaction* tx,
                                  const std::string& index_name,
                                  const std::string& secondary_key,
                                  const std::string& primary_key);
    bool tx_delete_secondary_index(LineairDBTransaction* tx,
                                   const std::string& index_name,
                                   const std::string& secondary_key,
                                   const std::string& primary_key);
    bool tx_update_secondary_index(LineairDBTransaction* tx,
                                   const std::string& index_name,
                                   const std::string& old_secondary_key,
                                   const std::string& new_secondary_key,
                                   const std::string& primary_key);

    // primary key scan operations
    std::vector<std::string> tx_get_matching_keys_in_range(LineairDBTransaction* tx,
                                                           const std::string& start_key,
                                                           const std::string& end_key);
    std::vector<KeyValue> tx_get_matching_keys_and_values_in_range(LineairDBTransaction* tx,
                                                                    const std::string& start_key,
                                                                    const std::string& end_key,
                                                                    uint64_t row_limit = 0,
                                                                    bool reverse_scan = false);
    std::vector<KeyValue> tx_get_matching_keys_and_values_from_prefix(LineairDBTransaction* tx,
                                                                       const std::string& prefix);
    // Zero-copy variant: parse binary response directly into caller-provided buffers.
    // Returns number of entries parsed, or -1 on error.
    int tx_scan_into_buffers(LineairDBTransaction* tx,
                             const std::string& prefix,
                             std::vector<std::string>& out_keys,
                             std::vector<std::vector<std::byte>>& out_values,
                             std::unordered_map<std::string, size_t>& out_cache);
    std::optional<std::string> tx_fetch_last_key_in_range(LineairDBTransaction* tx,
                                                           const std::string& start_key,
                                                           const std::string& end_key);
    std::optional<std::string> tx_fetch_first_key_with_prefix(LineairDBTransaction* tx,
                                                               const std::string& prefix,
                                                               const std::string& prefix_end);
    std::optional<std::string> tx_fetch_next_key_with_prefix(LineairDBTransaction* tx,
                                                              const std::string& last_key,
                                                              const std::string& prefix_end);

    // secondary index scan operations
    std::vector<std::string> tx_get_matching_primary_keys_in_range(LineairDBTransaction* tx,
                                                                    const std::string& index_name,
                                                                    const std::string& start_key,
                                                                    const std::string& end_key);
    std::vector<std::string> tx_get_matching_primary_keys_from_prefix(LineairDBTransaction* tx,
                                                                       const std::string& index_name,
                                                                       const std::string& prefix);
    std::optional<std::string> tx_fetch_last_primary_key_in_secondary_range(LineairDBTransaction* tx,
                                                                             const std::string& index_name,
                                                                             const std::string& start_key,
                                                                             const std::string& end_key);
    std::optional<SecondaryIndexEntry> tx_fetch_last_secondary_entry_in_range(LineairDBTransaction* tx,
                                                                               const std::string& index_name,
                                                                               const std::string& start_key,
                                                                               const std::string& end_key);

    // table/index management (non-transactional)
    bool db_create_table(const std::string& table_name);
    bool db_set_table(int64_t tx_id, const std::string& table_name);
    bool db_create_secondary_index(const std::string& table_name,
                                   const std::string& index_name,
                                   uint32_t index_type);

    // database operations
    bool db_end_transaction(int64_t tx_id, bool isFence,
                            const std::vector<std::pair<std::string, int64_t>>& row_deltas = {});
    void db_fence();

    // statistics: cached table row counts, refreshed on BEGIN/END
    const std::unordered_map<std::string, int64_t>& cached_table_stats() const {
        return table_stats_cache_;
    }

    // Route per-RPC measurements to the active transaction trace.
    void set_current_trace(TxRpcTrace* trace) { current_trace_ = trace; }

private:
    std::unordered_map<std::string, int64_t> table_stats_cache_;
    template<typename RequestType, typename ResponseType>
    bool send_protobuf_message(const RequestType& request, ResponseType& response,
                               MessageType message_type, const std::string& meta = "");
    // Send protobuf request, receive raw binary response
    template<typename RequestType>
    bool send_protobuf_recv_binary(const RequestType& request, std::string& raw_response,
                                   MessageType message_type, const std::string& meta = "");
    // Parse flat binary scan response: [is_aborted:1B] [entries...] [sentinel: key_len=0]
    static std::vector<KeyValue> parse_binary_kv_response(const std::string& raw, bool& is_aborted);
    bool send_message(const std::string& serialized_request, std::string& serialized_response);
    bool send_message_with_header(const std::string& serialized_request,
                                  std::string& serialized_response,
                                  MessageType message_type,
                                  const std::string& meta = "");

    int socket_fd_;
    bool connected_;
    std::string host_;
    int port_;
    TxRpcTrace* current_trace_ = nullptr;
};

#endif // LINEAIRDB_PROXY_H
