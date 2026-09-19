#include "helios_rpc.hh"

#include <memory>
#include <string>

#include "duckdb_bridge_executor.hh"
#include "helios.pb.h"

/**
 * @brief Hands a resolved-statement request to olap::execute_duckdb_query.
 */
void HeliosRpc::handleTxExecuteDuckdbQuery(
    const Helios::Protocol::TxExecuteDuckdbQuery::Request& request,
    Helios::Protocol::TxExecuteDuckdbQuery::Response* response) {
    std::shared_ptr<helios::storage::Database> db =
        db_manager_ ? db_manager_->get_database() : nullptr;
    if (!db) {
        response->set_ok(false);
        response->set_error("database is unavailable");
        return;
    }

    olap::execute_duckdb_query(db.get(), request, response);
}
