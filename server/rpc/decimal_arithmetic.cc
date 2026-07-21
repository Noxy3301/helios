#include "decimal_arithmetic.hh"

#include <algorithm>

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
