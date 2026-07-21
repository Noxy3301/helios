#pragma once

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "lineairdb.pb.h"
#include "lineairdb/lineairdb.h"

// Parallel scan helpers for primary ranges and PAX strip-direct scans. These
// functions fall back to the serial path when the table, key shape, or row
// count is not suitable for the specialized path.

/**
 * @brief Membership set for one hoisted semijoin reduction.
 */
struct SemijoinReduction {
    std::unordered_set<std::string> keys;  // accepted join-key values
    uint32_t probe_column = 0;             // probed column in the current row
};

/**
 * @brief Scan primary rows through PAX row references and gather survivors.
 *
 * @return true when rows were emitted; false lets the caller use the
 * materialized-row path.
 */
bool parallel_primary_pax_row_ref_scan(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result,
    bool& projection_failed,
    const std::vector<SemijoinReduction>& semijoin_reductions);

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
