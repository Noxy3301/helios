// Dispatch of one request: decodes nothing itself, routes the OpCode to its
// handler and releases the thread's epoch once the reply is built.

#include "helios_rpc.hh"

#include <cstdint>
#include <string>

#include "../../common/log.h"

HeliosRpc::HeliosRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TableRowCounts> row_counts,
                           std::shared_ptr<HiddenKeyAllocator> hidden_keys)
    : db_manager_(db_manager), row_counts_(row_counts),
      hidden_keys_(hidden_keys) {
}

void HeliosRpc::handle_rpc(MessageType message_type,
                              const std::string& message,
                              std::string& result) {
    LOG_DEBUG("Handling RPC: message_type=%u", static_cast<uint32_t>(message_type));

    switch(message_type) {
        // Reads
        case MessageType::TX_READ:
            handleTxRead(message, result);
            break;
        case MessageType::TX_BATCH_READ:
            handleTxBatchRead(message, result);
            break;
        case MessageType::TX_SCAN:
            handleTxScan(message, result);
            break;
        case MessageType::TX_SCAN_INDEX:
            handleTxScanIndex(message, result);
            break;
        case MessageType::TX_EXECUTE_READ_PLAN:
            handleTxExecuteReadPlan(message, result);
            break;

        // The one commit of a transaction
        case MessageType::TX_COMMIT:
            handleTxCommit(message, result);
            break;

        case MessageType::TX_GET_TABLE_STATS:
            handleTxGetTableStats(message, result);
            break;
        case MessageType::TX_EXECUTE_DUCKDB_QUERY:
            handleTxExecuteDuckdbQuery(message, result);
            break;

        // Definitions and server state
        case MessageType::DB_CREATE_TABLE:
            handleDbCreateTable(message, result);
            break;
        case MessageType::DB_CREATE_SECONDARY_INDEX:
            handleDbCreateSecondaryIndex(message, result);
            break;
        case MessageType::DB_ALLOCATE_HIDDEN_KEYS:
            handleDbAllocateHiddenKeys(message, result);
            break;
        case MessageType::DB_SET_COMMIT_DURABILITY:
            handleDbSetCommitDurability(message, result);
            break;

        default:
            LOG_ERROR("Unknown message type: %u", static_cast<uint32_t>(message_type));
            break;
    }

    // A request holds nothing in the index past its reply.
    db_manager_->get_database()->ReleaseThreadEpoch();
}
