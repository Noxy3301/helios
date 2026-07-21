#pragma once

#include <string>

// Exact fixed-point decimal formatting for typed PAX cells. DecimalValue
// stores a value as mantissa * 10^-scale, plus a null marker.

/**
 * @brief Exact fixed-point decimal value.
 */
struct DecimalValue {
    __int128 mantissa = 0;
    int scale = 0;
    bool is_null = false;
};

/**
 * @brief Format a decimal value back to its string representation.
 */
std::string format_decimal_value(const DecimalValue& value);
