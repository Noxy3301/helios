#pragma once

#include <string>
#include <string_view>

#include "lineairdb.pb.h"

// Arithmetic helpers for decimal fixed-point values used by server-side
// aggregate partials. Dec stores a value as m * 10^-s, plus a null marker.

/**
 * @brief Exact fixed-point decimal used for server-side SUM/AVG partials.
 */
struct Dec {
    __int128 m = 0;
    int s = 0;
    bool null = false;
};

/**
 * @brief Add or subtract two Dec values after aligning their scales.
 */
void dec_addsub(Dec& a, const Dec& b, bool sub);

/**
 * @brief Evaluate an aggregate argument expression against one base row.
 */
Dec dec_eval(const LineairDB::Protocol::FilterExpr& e,
             const std::string& row);

/**
 * @brief Format Dec back to its decimal string representation.
 */
std::string dec_format(const Dec& d);
