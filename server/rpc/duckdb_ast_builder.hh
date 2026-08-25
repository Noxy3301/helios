#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "lineairdb.pb.h"

namespace duckdb {
class SelectStatement;
}

namespace duckdb_bridge {

/**
 * @brief Constructs a DuckDB parsed AST from a resolved-query request.
 *
 * @details The request is the proxy's serialization of MySQL's resolved
 * statement; no SQL text is involved and DuckDB's parser never runs. Every
 * node kind is translated by an explicit rule; a request holding anything
 * without a rule is refused, never approximated. Base tables are referenced
 * through the process-lifetime helios_pax_scan(POINTER) table function; the
 * caller owns the scan handles and passes one pointer per Request.tables
 * entry.
 */
struct AstBuildResult {
    std::unique_ptr<duckdb::SelectStatement> statement;
    std::string error;  // non-empty means the request was refused
};

AstBuildResult BuildSelectStatement(
    const LineairDB::Protocol::TxExecuteDuckdbQuery::Request& request,
    const std::vector<uintptr_t>& table_handles);

}  // namespace duckdb_bridge
