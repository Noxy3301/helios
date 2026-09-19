#pragma once

#include "helios.pb.h"

namespace helios::storage {
class Database;
}

namespace olap {

/**
 * @brief Executes a tx_execute_duckdb_query request.
 *
 * @details Constructs DuckDB's parsed AST from the request the plugin builds
 * from the resolved statement and executes it; no SQL text is parsed. Runs
 * under the read view fence, validity gate and chunk validation, and keeps no
 * per-request catalog state: the one process-lifetime helios_pax_scan(POINTER)
 * function reads request-owned table views passed as pointer constants inside
 * the AST.
 */
void execute_duckdb_query(
    helios::storage::Database* db,
    const Helios::Protocol::TxExecuteDuckdbQuery::Request& request,
    Helios::Protocol::TxExecuteDuckdbQuery::Response* response);

/**
 * @brief Reads olap_threads and olap_mem_limit_bytes into the bounds the
 * DuckDB runtime starts under.
 *
 * @details Call once on the start path: the runtime is built at the first
 * analytical request and takes the bounds as they stand then.
 */
void configure_limits();

}  // namespace olap
