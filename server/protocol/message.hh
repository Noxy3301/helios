#pragma once

#include <cstdint>

// Message header for RPC communication
struct MessageHeader {
    uint64_t sender_id;      // sender ID (not used in LineairDB but keeping for consistency)
    uint32_t message_type;   // OpCode from protobuf
    uint32_t payload_size;   // size of the protobuf payload
};

// MessageType enum (the names and values of OpCode in lineairdb.proto)
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
