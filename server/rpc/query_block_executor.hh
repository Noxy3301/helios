#ifndef QUERY_BLOCK_EXECUTOR_HH
#define QUERY_BLOCK_EXECUTOR_HH

/**
 * Secondary-engine computation pushdown: execute a whole query block
 * (scans -> joins -> aggregation -> order by/limit) server-side over PAX
 * column strips.
 *
 * Execution model (Phase B0/B1): late materialization. A tuple is a set of
 * per-table PAX row references (ref = group_idx * kRows + slot); operators
 * carry columns of refs, and expressions read cells straight from the
 * strips on demand. Scans and joins run morsel-parallel (morsel = PAX
 * group / row chunk) on a shared pool of workers.
 *
 * Consistency: reads are unvalidated (the proxy only offloads read-only
 * autocommit SELECTs). Every touched group's write counter is snapshotted
 * before the first scan and re-checked after execution; any concurrent
 * modification fails the request and the proxy re-runs the statement on
 * the primary engine.
 */

#include <string>

#include "lineairdb.pb.h"

namespace LineairDB {
class Database;
}

namespace qb {

// Executes `request` and fills `response` (ok/error/rows). Never throws.
void ExecuteQueryBlock(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteQueryBlock::Request& request,
    LineairDB::Protocol::TxExecuteQueryBlock::Response* response);

}  // namespace qb

#endif  // QUERY_BLOCK_EXECUTOR_HH
