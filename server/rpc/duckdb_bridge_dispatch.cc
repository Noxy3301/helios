#include "lineairdb_rpc.hh"

#include <memory>
#include <string>

#include "duckdb_bridge_executor.hh"
#include "lineairdb.pb.h"

/**
 * @brief Parses a TX_EXECUTE_SQL_DUCKDB request and hands it to
 * duckdb_bridge::ExecuteSql.
 *
 * @details Follows query_block_dispatch.cc's handleTxExecuteQueryBlock: a
 * request that fails to parse, and a request arriving while the database is
 * unavailable, are answered with ok=false without reaching the executor.
 *
 * @param message Serialized TxExecuteSqlDuckdb::Request.
 * @param result Receives the serialized TxExecuteSqlDuckdb::Response.
 */
void LineairDBRpc::handleTxExecuteSqlDuckdb(const std::string& message,
                                            std::string& result) {
    LineairDB::Protocol::TxExecuteSqlDuckdb::Request request;
    LineairDB::Protocol::TxExecuteSqlDuckdb::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_error("failed to parse duckdb-bridge request");
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

    duckdb_bridge::ExecuteSql(db.get(), request, &response);
    result = response.SerializeAsString();
}
