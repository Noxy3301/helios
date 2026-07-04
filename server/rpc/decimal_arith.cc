#include "decimal_arith.hh"

#include <algorithm>

#include "row_codec.hh"

/**
 * @brief Return 10^n as an exact integer.
 */
static __int128 dec_pow10(int n) {
    __int128 r = 1;
    while (n-- > 0) r *= 10;
    return r;
}

/**
 * @brief Parse an ASCII decimal string into Dec.
 */
static Dec dec_parse(std::string_view v) {
    Dec d;
    if (v.empty()) { d.null = true; return d; }
    size_t i = 0; bool neg = false;
    if (v[0] == '-') { neg = true; i = 1; } else if (v[0] == '+') i = 1;
    __int128 m = 0; int scale = 0; bool dot = false;
    for (; i < v.size(); ++i) {
        char c = v[i];
        if (c == '.') { dot = true; continue; }
        if (c < '0' || c > '9') { d.null = true; return d; }
        m = m * 10 + (c - '0');
        if (dot) ++scale;
    }
    d.m = neg ? -m : m; d.s = scale;
    return d;
}

void dec_addsub(Dec& a, const Dec& b, bool sub) {
    const int s = a.s > b.s ? a.s : b.s;
    __int128 am = a.m * dec_pow10(s - a.s);
    __int128 bm = b.m * dec_pow10(s - b.s);
    a.m = sub ? am - bm : am + bm;
    a.s = s;
    a.null = a.null || b.null;
}

Dec dec_eval(const LineairDB::Protocol::FilterExpr& e,
             const std::string& row) {
    using FE = LineairDB::Protocol::FilterExpr;
    switch (e.op()) {
        case FE::COLUMN_REF:
            return dec_parse(extract_value_column(row, e.column_index()));
        case FE::CONST_INT:  { Dec d; d.m = e.int_val();  d.s = 0; return d; }
        case FE::CONST_UINT: { Dec d; d.m = (__int128)e.uint_val(); d.s = 0; return d; }
        case FE::OP_ADD: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            dec_addsub(a, dec_eval(e.children(1), row), false);
            return a;
        }
        case FE::OP_SUB: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            dec_addsub(a, dec_eval(e.children(1), row), true);
            return a;
        }
        case FE::OP_MUL: {
            if (e.children_size() != 2) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row);
            Dec b = dec_eval(e.children(1), row);
            Dec r; r.m = a.m * b.m; r.s = a.s + b.s; r.null = a.null || b.null;
            return r;
        }
        case FE::OP_NEG: {
            if (e.children_size() != 1) { Dec d; d.null = true; return d; }
            Dec a = dec_eval(e.children(0), row); a.m = -a.m; return a;
        }
        default: { Dec d; d.null = true; return d; }
    }
}

std::string dec_format(const Dec& d) {
    __int128 m = d.m; bool neg = m < 0; if (neg) m = -m;
    std::string digits;
    if (m == 0) digits = "0";
    else { while (m) { digits.push_back(char('0' + int(m % 10))); m /= 10; }
           std::reverse(digits.begin(), digits.end()); }
    while (static_cast<int>(digits.size()) <= d.s) digits.insert(digits.begin(), '0');
    std::string out;
    if (neg) out.push_back('-');
    if (d.s == 0) { out += digits; return out; }
    const size_t ip = digits.size() - d.s;
    out.append(digits, 0, ip); out.push_back('.'); out.append(digits, ip, std::string::npos);
    return out;
}
