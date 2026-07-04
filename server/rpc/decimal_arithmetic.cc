#include "decimal_arithmetic.hh"

#include <algorithm>

#include "row_codec.hh"

namespace {

/**
 * @brief Return 10^n as an exact integer.
 */
__int128 decimal_power_of_ten(int n) {
    __int128 r = 1;
    while (n-- > 0) r *= 10;
    return r;
}

/**
 * @brief Parse an ASCII decimal string into DecimalValue.
 */
DecimalValue parse_decimal(std::string_view v) {
    DecimalValue d;
    if (v.empty()) {
        d.is_null = true;
        return d;
    }
    size_t i = 0;
    bool neg = false;
    if (v[0] == '-') {
        neg = true;
        i = 1;
    } else if (v[0] == '+') {
        i = 1;
    }
    __int128 mantissa = 0;
    int scale = 0;
    bool dot = false;
    for (; i < v.size(); ++i) {
        char c = v[i];
        if (c == '.') {
            dot = true;
            continue;
        }
        if (c < '0' || c > '9') {
            d.is_null = true;
            return d;
        }
        mantissa = mantissa * 10 + (c - '0');
        if (dot) ++scale;
    }
    d.mantissa = neg ? -mantissa : mantissa;
    d.scale = scale;
    return d;
}

enum class DecimalOperation {
    kAdd,
    kSubtract,
};

void combine_decimal_values(DecimalValue& a, const DecimalValue& b,
                            DecimalOperation operation) {
    const int scale = a.scale > b.scale ? a.scale : b.scale;
    __int128 a_mantissa =
        a.mantissa * decimal_power_of_ten(scale - a.scale);
    __int128 b_mantissa =
        b.mantissa * decimal_power_of_ten(scale - b.scale);
    a.mantissa = operation == DecimalOperation::kSubtract
                     ? a_mantissa - b_mantissa
                     : a_mantissa + b_mantissa;
    a.scale = scale;
    a.is_null = a.is_null || b.is_null;
}

}  // namespace

void add_decimal_value(DecimalValue& total, const DecimalValue& value) {
    combine_decimal_values(total, value, DecimalOperation::kAdd);
}

DecimalValue evaluate_decimal_expression(
    const LineairDB::Protocol::FilterExpr& expression,
    const std::string& row) {
    using FE = LineairDB::Protocol::FilterExpr;
    switch (expression.op()) {
        case FE::COLUMN_REF:
            return parse_decimal(
                extract_value_column(row, expression.column_index()));
        case FE::CONST_INT: {
            DecimalValue d;
            d.mantissa = expression.int_val();
            d.scale = 0;
            return d;
        }
        case FE::CONST_UINT: {
            DecimalValue d;
            d.mantissa = static_cast<__int128>(expression.uint_val());
            d.scale = 0;
            return d;
        }
        case FE::OP_ADD: {
            if (expression.children_size() != 2) {
                DecimalValue d;
                d.is_null = true;
                return d;
            }
            DecimalValue a = evaluate_decimal_expression(
                expression.children(0), row);
            combine_decimal_values(
                a, evaluate_decimal_expression(expression.children(1), row),
                DecimalOperation::kAdd);
            return a;
        }
        case FE::OP_SUB: {
            if (expression.children_size() != 2) {
                DecimalValue d;
                d.is_null = true;
                return d;
            }
            DecimalValue a = evaluate_decimal_expression(
                expression.children(0), row);
            combine_decimal_values(
                a, evaluate_decimal_expression(expression.children(1), row),
                DecimalOperation::kSubtract);
            return a;
        }
        case FE::OP_MUL: {
            if (expression.children_size() != 2) {
                DecimalValue d;
                d.is_null = true;
                return d;
            }
            DecimalValue a = evaluate_decimal_expression(
                expression.children(0), row);
            DecimalValue b = evaluate_decimal_expression(
                expression.children(1), row);
            DecimalValue r;
            r.mantissa = a.mantissa * b.mantissa;
            r.scale = a.scale + b.scale;
            r.is_null = a.is_null || b.is_null;
            return r;
        }
        case FE::OP_NEG: {
            if (expression.children_size() != 1) {
                DecimalValue d;
                d.is_null = true;
                return d;
            }
            DecimalValue a = evaluate_decimal_expression(
                expression.children(0), row);
            a.mantissa = -a.mantissa;
            return a;
        }
        default: {
            DecimalValue d;
            d.is_null = true;
            return d;
        }
    }
}

std::string format_decimal_value(const DecimalValue& value) {
    __int128 mantissa = value.mantissa;
    bool neg = mantissa < 0;
    if (neg) mantissa = -mantissa;
    std::string digits;
    if (mantissa == 0) {
        digits = "0";
    } else {
        while (mantissa) {
            digits.push_back(
                static_cast<char>('0' + static_cast<int>(mantissa % 10)));
            mantissa /= 10;
        }
        std::reverse(digits.begin(), digits.end());
    }
    while (static_cast<int>(digits.size()) <= value.scale) {
        digits.insert(digits.begin(), '0');
    }
    std::string out;
    if (neg) out.push_back('-');
    if (value.scale == 0) {
        out += digits;
        return out;
    }
    const size_t ip = digits.size() - value.scale;
    out.append(digits, 0, ip);
    out.push_back('.');
    out.append(digits, ip, std::string::npos);
    return out;
}
