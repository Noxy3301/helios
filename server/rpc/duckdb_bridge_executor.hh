#pragma once

#include "helios.pb.h"

namespace helios::storage {
class Database;
}

namespace duckdb_bridge {

/**
 * @brief Executes a TX_EXECUTE_DUCKDB_QUERY request.
 *
 * @details Constructs DuckDB's parsed AST from the proxy's serialization of
 * the resolved statement and executes it; no SQL text is parsed. Runs under
 * the same epoch-fenced columnar read view, validity gate, and bulk-group
 * audit as the text path, but keeps no per-request catalog state: the one
 * process-lifetime helios_pax_scan(POINTER) function reads request-owned
 * table views passed as pointer constants inside the AST.
 */
void ExecuteDuckdbQuery(
    helios::storage::Database* db,
    const Helios::Protocol::TxExecuteDuckdbQuery::Request& request,
    Helios::Protocol::TxExecuteDuckdbQuery::Response* response);

/**
 * @brief Reads bridge_threads and bridge_mem_limit_bytes into the bounds the
 * DuckDB runtime starts under.
 *
 * @details Call once on the start path: the runtime is built at the first
 * analytical request and takes the bounds as they stand then.
 */
void ConfigureLimits();

}  // namespace duckdb_bridge
