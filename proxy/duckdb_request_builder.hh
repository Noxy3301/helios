#pragma once

#include <string>

#include "lineairdb.pb.h"

class THD;
struct LEX;

namespace lineairdb_columnar {

/**
 * @brief Builds the duckdb bridge request from MySQL's resolved statement.
 *
 * @details Runs after resolution and before optimization, so the Item trees
 * are the permanent resolved ones, not yet edited in place by the
 * optimizer. Every Item and Table_ref kind is translated by an explicit
 * rule; anything without a rule refuses with *why set and nothing emitted.
 * The request the server executes is therefore exactly the statement this
 * walk examined.
 *
 * @param thd Connection running the statement.
 * @param lex The statement to walk (thd->lex at the capture point).
 * @param request Receives the wire request; cleared on entry.
 * @param why Receives the refusal reason when the walk returns false.
 * @return True when the whole statement translated into *request.
 */
bool BuildDuckdbQueryRequest(
    THD* thd, LEX* lex,
    LineairDB::Protocol::TxExecuteDuckdbQuery::Request* request,
    std::string* why);

}  // namespace lineairdb_columnar
