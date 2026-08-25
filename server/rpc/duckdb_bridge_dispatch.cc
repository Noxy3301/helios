#include "lineairdb_rpc.hh"

#include <memory>
#include <string>

#include "duckdb_bridge_executor.hh"
#include "lineairdb.pb.h"

/**
 * @brief Parses a TX_EXECUTE_DUCKDB_QUERY request and hands it to
 * duckdb_bridge::ExecuteDuckdbQuery.
 */
void LineairDBRpc::handleTxExecuteDuckdbQuery(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::TxExecuteDuckdbQuery::Request request;
    LineairDB::Protocol::TxExecuteDuckdbQuery::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_error("failed to parse duckdb-query request");
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

    duckdb_bridge::ExecuteDuckdbQuery(db.get(), request, &response);
    result = response.SerializeAsString();
}
