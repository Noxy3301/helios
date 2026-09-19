#pragma once

#include <string>

#include "helios.pb.h"
#include "helios/database.h"

/**
 * @brief Scans a primary range of a PAX table with several workers and emits
 * the rows as PAX row references.
 *
 * @return true when the rows were emitted; false when the table, the key
 *   shape or the row count does not fit, and the caller scans the range
 *   serially.
 */
bool parallel_primary_pax_row_ref_scan(
    helios::storage::Database* db,
    const Helios::Protocol::TxExecuteReadPlan::PlanStep& step,
    const std::string& start_key, const std::string& end_key,
    Helios::Protocol::TxExecuteReadPlan::StepResult* step_result);
