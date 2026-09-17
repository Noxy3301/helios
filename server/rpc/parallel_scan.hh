#pragma once

#include <string>

#include "lineairdb.pb.h"
#include "lineairdb/database.h"

/**
 * @brief Scans a primary range of a PAX table with several workers and emits
 * the rows as PAX row references.
 *
 * @return true when the rows were emitted; false when the table, the key
 *   shape or the row count does not fit, and the caller materializes the
 *   range serially.
 */
bool parallel_primary_pax_row_ref_scan(
    helios::storage::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result);
