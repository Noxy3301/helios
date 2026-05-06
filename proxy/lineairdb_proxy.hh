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

// Message header for RPC communication (matching server implementation)
struct MessageHeader {
    uint64_t sender_id;      // sender ID
    uint32_t message_type;   // OpCode from protobuf
    uint32_t payload_size;   // size of the protobuf payload
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
    TX_BATCH_WRITE = 26
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
    std::vector<BatchReadResult> tx_batch_read(LineairDBTransaction* tx,
                                                const std::vector<std::string>& keys);
    struct BatchWriteOp {
        std::string key;
        std::string value;
    };
    struct BatchSecondaryIndexOp {
        std::string index_name;
        std::string secondary_key;
        std::string primary_key;
    };
    bool tx_batch_write(LineairDBTransaction* tx,
                        const std::string& table_name,
                        const std::vector<BatchWriteOp>& writes,
                        const std::vector<BatchSecondaryIndexOp>& si_writes);

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
                                                                    const std::string& end_key);
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
