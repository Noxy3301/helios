#include "lineairdb_rpc.hh"

#include <cstdint>
#include <string>

#include "../../common/log.h"
#include "rpc_timing.hh"

LineairDBRpc::LineairDBRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TransactionManager> tx_manager,
                           std::shared_ptr<TableRowCounts> row_counts)
    : db_manager_(db_manager), tx_manager_(tx_manager), row_counts_(row_counts) {
}

void LineairDBRpc::handle_rpc(uint64_t sender_id, MessageType message_type,
                             const std::string& message, std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));

    // Close this thread's masstree RCU critical section after each
    // self-contained handler (stateless RPCs and DB_END_TRANSACTION) so
    // RCU can reclaim retired leaves; the next masstree op re-opens it.
    auto release_masstree_thread_epoch = [this]() {
        if (db_manager_) {
            auto db = db_manager_->get_database();
            if (db) db->ReleaseMasstreeThreadEpoch();
        }
    };
    // Per-opcode timing (HELIOS_RPC_TIMING=1) wraps only the handler call,
    // excluding release_masstree_thread_epoch() and the recv/send around
    // handle_rpc in the caller.
    switch(message_type) {
        // Transaction control
        case MessageType::TX_BEGIN_TRANSACTION:
            rpc_timing::time_call(message_type, [&] { handleTxBeginTransaction(message, result); });
            return;
        case MessageType::TX_ABORT:
            rpc_timing::time_call(message_type, [&] { handleTxAbort(message, result); });
            return;

        // Primary key operations
        case MessageType::TX_READ:
            rpc_timing::time_call(message_type, [&] { handleTxRead(message, result); });
            return;
        case MessageType::TX_BATCH_READ:
            rpc_timing::time_call(message_type, [&] { handleTxBatchRead(message, result); });
            return;
        case MessageType::TX_BATCH_WRITE:
            rpc_timing::time_call(message_type, [&] { handleTxBatchWrite(message, result); });
            return;
        case MessageType::TX_WRITE:
            rpc_timing::time_call(message_type, [&] { handleTxWrite(message, result); });
            return;
        case MessageType::TX_DELETE:
            rpc_timing::time_call(message_type, [&] { handleTxDelete(message, result); });
            return;

        // Stateless operations
        case MessageType::TX_STATELESS_READ:
            rpc_timing::time_call(message_type, [&] { handleTxStatelessRead(message, result); });
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_STATELESS_BATCH_READ:
            rpc_timing::time_call(message_type, [&] { handleTxStatelessBatchRead(message, result); });
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_VALIDATE_AND_COMMIT:
            // Variant-tagged overload (fence/entries/frame-size/response-size/
            // stats-size), see rpc_timing.hh.
            rpc_timing::time_call(message_type, message, result,
                                  [&] { handleTxValidateAndCommit(message, result); });
            release_masstree_thread_epoch();
            return;

        // Read-plan execution
        case MessageType::TX_EXECUTE_READ_PLAN:
            // Variant-tagged overload (steps/scan_limit/inspected-rows/
            // response-size), see rpc_timing.hh.
            rpc_timing::time_call(message_type, message, result,
                                  [&] { handleTxExecuteReadPlan(message, result); });
            release_masstree_thread_epoch();
            return;
        case MessageType::TX_EXECUTE_SQL_DUCKDB:
            rpc_timing::time_call(message_type, [&] { handleTxExecuteSqlDuckdb(message, result); });
            release_masstree_thread_epoch();
            return;

        // Table statistics
        case MessageType::TX_GET_TABLE_STATS:
            rpc_timing::time_call(message_type, [&] { handleTxGetTableStats(message, result); });
            release_masstree_thread_epoch();
            return;

        // Secondary index operations
        case MessageType::TX_READ_SECONDARY_INDEX:
            rpc_timing::time_call(message_type, [&] { handleTxReadSecondaryIndex(message, result); });
            return;
        case MessageType::TX_WRITE_SECONDARY_INDEX:
            rpc_timing::time_call(message_type, [&] { handleTxWriteSecondaryIndex(message, result); });
            return;
        case MessageType::TX_DELETE_SECONDARY_INDEX:
            rpc_timing::time_call(message_type, [&] { handleTxDeleteSecondaryIndex(message, result); });
            return;
        case MessageType::TX_UPDATE_SECONDARY_INDEX:
            rpc_timing::time_call(message_type, [&] { handleTxUpdateSecondaryIndex(message, result); });
            return;

        // Primary key scan operations
        case MessageType::TX_GET_MATCHING_KEYS_IN_RANGE:
            rpc_timing::time_call(message_type, [&] { handleTxGetMatchingKeysInRange(message, result); });
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE:
            rpc_timing::time_call(message_type, [&] { handleTxGetMatchingKeysAndValuesInRange(message, result); });
            return;
        case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX:
            rpc_timing::time_call(message_type, [&] { handleTxGetMatchingKeysAndValuesFromPrefix(message, result); });
            return;
        case MessageType::TX_FETCH_LAST_KEY_IN_RANGE:
            rpc_timing::time_call(message_type, [&] { handleTxFetchLastKeyInRange(message, result); });
            return;
        case MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX:
            rpc_timing::time_call(message_type, [&] { handleTxFetchFirstKeyWithPrefix(message, result); });
            return;
        case MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX:
            rpc_timing::time_call(message_type, [&] { handleTxFetchNextKeyWithPrefix(message, result); });
            return;

        // Secondary index scan operations
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE:
            rpc_timing::time_call(message_type, [&] { handleTxGetMatchingPrimaryKeysInRange(message, result); });
            return;
        case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX:
            rpc_timing::time_call(message_type, [&] { handleTxGetMatchingPrimaryKeysFromPrefix(message, result); });
            return;
        case MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE:
            rpc_timing::time_call(message_type, [&] { handleTxFetchLastPrimaryKeyInSecondaryRange(message, result); });
            return;
        case MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE:
            rpc_timing::time_call(message_type, [&] { handleTxFetchLastSecondaryEntryInRange(message, result); });
            return;

        // Database operations
        case MessageType::DB_FENCE:
            rpc_timing::time_call(message_type, [&] { handleDbFence(message, result); });
            return;
        case MessageType::DB_END_TRANSACTION:
            rpc_timing::time_call(message_type, [&] { handleDbEndTransaction(message, result); });
            release_masstree_thread_epoch();
            return;
        case MessageType::DB_CREATE_TABLE:
            rpc_timing::time_call(message_type, [&] { handleDbCreateTable(message, result); });
            return;
        case MessageType::DB_SET_TABLE:
            rpc_timing::time_call(message_type, [&] { handleDbSetTable(message, result); });
            return;
        case MessageType::DB_CREATE_SECONDARY_INDEX:
            rpc_timing::time_call(message_type, [&] { handleDbCreateSecondaryIndex(message, result); });
            return;

        default:
            LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
            return;
    }
}
