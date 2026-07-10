#include "lineairdb_rpc.hh"

#include <memory>
#include <string>

#include "lineairdb.pb.h"
#include "query_block_executor.hh"

// Query-block dispatch: parse TX_EXECUTE_QUERY_BLOCK requests and hand them to
// the server-side executor. Operator execution stays in query_block_executor.cc.

void LineairDBRpc::handleTxExecuteQueryBlock(const std::string& message,
                                             std::string& result) {
    LineairDB::Protocol::TxExecuteQueryBlock::Request request;
    LineairDB::Protocol::TxExecuteQueryBlock::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_error("failed to parse query-block request");
        result = response.SerializeAsString();
        return;
    }

    std::shared_ptr<LineairDB::Database> db =
        db_manager_ ? db_manager_->get_database() : nullptr;
    if (!db) {
        response.set_ok(false);
        response.set_error("database is unavailable");
        result = response.SerializeAsString();
        return;
    }

    query_block::ExecuteQueryBlock(db.get(), request, &response);
    result = response.SerializeAsString();
}
