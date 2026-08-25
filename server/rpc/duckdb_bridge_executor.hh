#pragma once

#include "lineairdb.pb.h"

namespace LineairDB {
class Database;
}

namespace duckdb_bridge {

/**
 * @brief Executes a TX_EXECUTE_DUCKDB_QUERY request.
 *
 * @details Constructs DuckDB's parsed AST from the proxy's serialization of
 * the resolved statement and executes it; no SQL text is parsed. Runs under
 * the same epoch-fenced columnar read view, poison gate, and bulk-group
 * audit as the text path, but keeps no per-request catalog state: the one
 * process-lifetime helios_pax_scan(POINTER) function reads request-owned
 * table views passed as pointer constants inside the AST.
 */
void ExecuteDuckdbQuery(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteDuckdbQuery::Request& request,
    LineairDB::Protocol::TxExecuteDuckdbQuery::Response* response);

}  // namespace duckdb_bridge
