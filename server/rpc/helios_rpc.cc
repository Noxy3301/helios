// Dispatch of one request: routes the arm the envelope sets to its handler
// and releases the thread's epoch once the reply is built.

#include "helios_rpc.hh"

#include <string>

#include <spdlog/spdlog.h>

using Helios::Protocol::Request;

HeliosRpc::HeliosRpc(std::shared_ptr<DatabaseManager> db_manager,
                           std::shared_ptr<TableRowCounts> row_counts,
                           std::shared_ptr<HiddenKeyAllocator> hidden_keys)
    : db_manager_(db_manager), row_counts_(row_counts),
      hidden_keys_(hidden_keys) {
}

bool HeliosRpc::handle_rpc(const std::string& message, std::string& result,
                           std::string& payload) {
    Request request;
    Helios::Protocol::Response response;

    if (!request.ParseFromString(message)) {
        SPDLOG_ERROR("Malformed request envelope ({} bytes)", message.size());
        response.set_error("malformed request");
    } else {
        // No default: -Wswitch reports an arm that has no handler.
        switch (request.body_case()) {
            // Reads
            case Request::kTxRead:
                handleTxRead(request.tx_read(), response.mutable_tx_read());
                break;
            case Request::kTxBatchRead:
                handleTxBatchRead(request.tx_batch_read(),
                                  response.mutable_tx_batch_read());
                break;
            case Request::kTxScan:
                handleTxScan(request.tx_scan(), response.mutable_tx_scan());
                break;
            case Request::kTxScanIndex:
                handleTxScanIndex(request.tx_scan_index(),
                                  response.mutable_tx_scan_index());
                break;
            case Request::kTxExecuteReadPlan:
                // The arm is a marker: the flat result rides raw after the
                // envelope instead of being copied into protobuf.
                response.mutable_tx_execute_read_plan();
                handleTxExecuteReadPlan(request.tx_execute_read_plan(),
                                        &payload);
                break;

            // The one commit of a transaction
            case Request::kTxCommit:
                handleTxCommit(request.tx_commit(), response.mutable_tx_commit());
                break;

            case Request::kGetTableStats:
                handleTxGetTableStats(request.get_table_stats(),
                                      response.mutable_get_table_stats());
                break;
            case Request::kTxExecuteDuckdbQuery:
                handleTxExecuteDuckdbQuery(
                    request.tx_execute_duckdb_query(),
                    response.mutable_tx_execute_duckdb_query());
                break;

            // Definitions and server state
            case Request::kDbCreateTable:
                handleDbCreateTable(request.db_create_table(),
                                    response.mutable_db_create_table());
                break;
            case Request::kDbCreateSecondaryIndex:
                handleDbCreateSecondaryIndex(
                    request.db_create_secondary_index(),
                    response.mutable_db_create_secondary_index());
                break;
            case Request::kDbAllocateHiddenKeys:
                handleDbAllocateHiddenKeys(
                    request.db_allocate_hidden_keys(),
                    response.mutable_db_allocate_hidden_keys());
                break;
            case Request::kDbSetCommitDurability:
                handleDbSetCommitDurability(
                    request.db_set_commit_durability(),
                    response.mutable_db_set_commit_durability());
                break;

            case Request::BODY_NOT_SET:
                SPDLOG_ERROR("Request envelope carries no body");
                response.set_error("request carries no body");
                break;
        }
    }

    // A request holds nothing in the index past its reply.
    db_manager_->get_database()->ReleaseThreadEpoch();

    if (!response.SerializeToString(&result)) {
        SPDLOG_ERROR("Failed to serialize the response envelope");
        return false;
    }
    return true;
}
