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
 * schema registry for this path. Every referenced table's write state is
 * captured before execution and re-checked after (OCC quiescence): a
 * concurrent write discards the result. Coverage is not limited to the query
 * shapes the query-block executor recognizes.
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
