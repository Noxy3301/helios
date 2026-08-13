#ifndef HELIOS_PROTOCOL_MESSAGE_HH
#define HELIOS_PROTOCOL_MESSAGE_HH

#include <cstdint>

// Message header for RPC communication
struct MessageHeader {
  uint64_t sender_id;     // sender ID (not used in LineairDB but keeping for consistency)
  uint32_t message_type;  // OpCode from protobuf
  uint32_t payload_size;  // size of the protobuf payload
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

  // Experimental prefetch operations
  TX_STATELESS_READ = 27,
  TX_STATELESS_BATCH_READ = 28,
  TX_VALIDATE_AND_COMMIT = 29,
  TX_EXECUTE_READ_PLAN = 30,
  TX_GET_TABLE_STATS = 31,

  // DuckDB SQL bridge (raw SQL text). See lineairdb.proto.
  TX_EXECUTE_SQL_DUCKDB = 36
};

#endif  // HELIOS_PROTOCOL_MESSAGE_HH
