#pragma once

#include <string>
#include <string_view>

#include "lineairdb.pb.h"

// Arithmetic helpers for decimal fixed-point values used by server-side
// aggregate partials. DecimalValue stores a value as mantissa * 10^-scale,
// plus a null marker.

/**
 * @brief Exact fixed-point decimal used for server-side SUM/AVG partials.
 */
struct DecimalValue {
    __int128 mantissa = 0;
    int scale = 0;
    bool is_null = false;
};

/**
 * @brief Add value into total after aligning their scales.
 */
void add_decimal_value(DecimalValue& total, const DecimalValue& value);

/**
 * @brief Evaluate an aggregate argument expression against one base row.
 */
DecimalValue evaluate_decimal_expression(
    const LineairDB::Protocol::FilterExpr& expression,
    const std::string& row);

/**
 * @brief Format a decimal value back to its string representation.
 */
std::string format_decimal_value(const DecimalValue& value);
