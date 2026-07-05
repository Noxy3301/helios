#pragma once

// Declares the LineairDB engine-pushdown hook installed during plugin
// initialization.

#include "lineairdb.pb.h"

class Item;
class THD;
class JOIN;
struct AccessPath;

namespace lineairdb {

/**
 * @brief Serialize an aggregate argument expression for LineairDB.
 *
 * Supports field references, integer constants, and simple arithmetic over
 * those terms. The server evaluates the serialized tree for decimal SUM/AVG
 * aggregation.
 */
bool serialize_aggregate_expression(
    const Item *item, LineairDB::Protocol::FilterExpr *out);

}  // namespace lineairdb

/**
 * @brief Install the aggregate executor override for supported query shapes.
 *
 * Unsupported queries leave `override_executor_func` unset and continue through
 * MySQL's normal iterator executor.
 */
int lineairdb_push_to_engine(THD *thd, AccessPath *root_path, JOIN *join);
