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

}  // namespace duckdb_bridge
