#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "lineairdb.pb.h"

struct PaxRowRef;

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
 * @brief Parse an ASCII decimal string into a DecimalValue.
 */
DecimalValue parse_decimal_value(std::string_view value);

/**
 * @brief Decode a numeric PAX cell to a DecimalValue by its declared kind.
 *
 * @details FK_INT32/FK_INT64/FK_DATE decode the native binary to an integer
 * (scale 0); FK_DEC64 yields a scaled int with `scale`; FK_UNTYPED falls back
 * to parse_decimal_value on the ASCII bytes. An empty cell is SQL NULL.
 */
DecimalValue decode_typed_decimal(std::string_view value, uint8_t kind,
                                  int scale);

/**
 * @brief Compare two decimal values after aligning their scales.
 *
 * Returns -1, 0, or 1 for less-than, equal, or greater-than.
 */
int compare_decimal_values(const DecimalValue& lhs, const DecimalValue& rhs);

/**
 * @brief Evaluate a decimal-valued aggregate argument tree against one
 * materialized row.
 *
 * Aggregate arguments reuse FilterExpr as a scalar-expression tree, for example
 * the argument of `SUM(l_extendedprice * (1 - l_discount))`.
 */
DecimalValue evaluate_decimal_expression(
    const LineairDB::Protocol::FilterExpr& expression,
    const std::string& row);

/**
 * @brief Evaluate a decimal-valued aggregate argument tree against one PAX row
 * slot.
 *
 * Column references read directly from PAX strip cells instead of a
 * materialized row.
 */
DecimalValue evaluate_decimal_expression(
    const LineairDB::Protocol::FilterExpr& expression,
    const PaxRowRef& row);

/**
 * @brief Format a decimal value back to its string representation.
 */
std::string format_decimal_value(const DecimalValue& value);
