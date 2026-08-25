#pragma once

#include "lineairdb.pb.h"

namespace LineairDB {
class Database;
}

namespace duckdb_bridge {

/**
 * @brief Executes a TX_EXECUTE_SQL_DUCKDB request.
 *
 * @details Runs the request's verbatim SQL text on the server's embedded
 * DuckDB executor. Table functions registered per request read the live
 * LineairDB::Database PAX storage in place; the request carries every base
 * table's PAX cell metadata (kind/width/scale), and the server keeps no
 * schema registry for this path. The scan runs under an epoch-fenced
 * columnar read view: rows written after the view's cut resolve to their
 * saved before-images, and a result is accepted only through the poison and
 * audit gates.
 *
 * @param db Live server database; source of the PaxStore instances.
 * @param request Parsed TX_EXECUTE_SQL_DUCKDB request.
 * @param response Filled with ok/error and result rows in the proxy row
 * format.
 */
void ExecuteSql(LineairDB::Database* db,
                const LineairDB::Protocol::TxExecuteSqlDuckdb::Request& request,
                LineairDB::Protocol::TxExecuteSqlDuckdb::Response* response);

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
