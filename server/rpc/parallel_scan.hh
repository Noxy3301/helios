#pragma once

#include <string>

#include "lineairdb.pb.h"
#include "lineairdb/lineairdb.h"

// Parallel scan helpers for integer primary-key ranges. These functions split
// a range into ordered key slices and fall back to the serial path when the
// table, key shape, or row count is not suitable for parallel execution.

/**
 * @brief Scan integer primary-key slices in parallel and aggregate locally.
 *
 * @return true when group rows were emitted; false lets the caller use the
 * serial scan path.
 */
bool parallel_primary_aggregate_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result);

/**
 * @brief Scan integer primary-key slices in parallel for filtered base rows.
 *
 * @return true when rows were emitted; false lets the caller use the serial
 * scan path.
 */
bool parallel_primary_filter_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result);
