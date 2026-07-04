#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "lineairdb.pb.h"
#include "lineairdb/stateless.h"

#include "decimal_arithmetic.hh"

// Server-side aggregate execution helpers. These functions build per-group
// partials from scan rows and serialize the partials back into synthetic rows
// consumed by the proxy.

/**
 * @brief Accumulators for one GROUP BY key.
 */
struct AggGroupState {
    std::vector<std::string> key_cols;   // captured group-by column values
    std::vector<uint64_t> count;         // per agg: COUNT / non-null counter
    std::vector<DecimalValue> sum;       // per agg: SUM/AVG accumulator
};

/**
 * @brief Accumulate rows in [begin, end) into a caller-owned group map.
 */
void aggregate_rows_range(
    const LineairDB::Protocol::AggregateSpec& spec,
    const std::vector<LineairDB::StatelessScanRow>& rows,
    size_t begin, size_t end,
    std::unordered_map<std::string, AggGroupState>& groups);

/**
 * @brief Merge one worker-local aggregate map into another.
 */
void merge_agg_groups(std::unordered_map<std::string, AggGroupState>& dst,
                      std::unordered_map<std::string, AggGroupState>& src,
                      int n_agg);

/**
 * @brief Serialize aggregate groups as synthetic scan rows for the proxy.
 */
void emit_agg_groups(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::unordered_map<std::string, AggGroupState>& groups,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result);

/**
 * @brief Aggregate scan rows and emit one synthetic group row per group.
 *
 * Each output row is [null_flags][group columns][value,count per aggregate].
 */
bool server_aggregate_scan(
    const LineairDB::Protocol::AggregateSpec& spec,
    std::vector<LineairDB::StatelessScanRow>& rows,
    LineairDB::Protocol::TxExecuteReadPlan::StepResult* step_result);
