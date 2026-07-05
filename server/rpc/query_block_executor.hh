#pragma once

#include "lineairdb.pb.h"

namespace LineairDB {
class Database;
}

namespace query_block {

/**
 * @brief Execute a LineairDB query-block request on the server.
 *
 * The request is the protobuf operator tree built by the proxy from supported
 * SELECT shapes. It is not SQL text and it is not MySQL's AccessPath or Query
 * Execution Plan object. The executor runs accepted operators over LineairDB's
 * PAX storage and returns final rows in the proxy row format.
 */
void ExecuteQueryBlock(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteQueryBlock::Request& request,
    LineairDB::Protocol::TxExecuteQueryBlock::Response* response);

}  // namespace query_block
