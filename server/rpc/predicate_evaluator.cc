#include "predicate_evaluator.hh"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using FilterExpr = LineairDB::Protocol::FilterExpr;

namespace {

bool parse_int_value(std::string_view value, int64_t* out) {
  if (out == nullptr || value.empty()) return false;
  const std::string text(value);
  errno = 0;
  char* end = nullptr;
  const long long parsed = std::strtoll(text.c_str(), &end, 10);
  if (errno != 0 || end != text.c_str() + text.size()) return false;
  *out = static_cast<int64_t>(parsed);
  return true;
}

bool parse_uint_value(std::string_view value, uint64_t* out) {
  if (out == nullptr || value.empty() || value.front() == '-') return false;
  const std::string text(value);
  errno = 0;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
  if (errno != 0 || end != text.c_str() + text.size()) return false;
  *out = static_cast<uint64_t>(parsed);
  return true;
}

bool parse_double_value(std::string_view value, double* out) {
  if (out == nullptr || value.empty()) return false;
  const std::string text(value);
  errno = 0;
  char* end = nullptr;
  const double parsed = std::strtod(text.c_str(), &end);
  if (errno != 0 || end != text.c_str() + text.size()) return false;
  *out = parsed;
  return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// Row parsing
// ---------------------------------------------------------------------------

bool PredicateEvaluator::parse_row(const char* data, size_t length,
                                   uint32_t num_columns) {
  columns_.clear();
  null_flags_.clear();
  schema_ = nullptr;  // materialized rows carry ASCII val_str bytes

  // Row format produced by ha_lineairdb (via LineairDBField):
  //   [null_flags_field] [col_0] [col_1] ... [col_N-1]
  // Each field:
  //   [byteSize: 1B] [valueLength: byteSize B] [value: valueLength B]
  //   byteSize == 0xFF → null / empty field
  //
  // The first field stores MySQL null flags; subsequent fields are user columns.

  size_t offset = 0;
  uint32_t field_index = 0;           // 0 = null flags, 1..N = columns
  uint32_t total_fields = num_columns + 1;  // +1 for null flags

  while (offset < length && field_index < total_fields) {
    if (offset >= length) return false;

    auto byte_size = static_cast<uint8_t>(data[offset]);
    offset += 1;

    if (byte_size == 0xFF) {
      // Null / empty field
      if (field_index == 0) {
        null_flags_.clear();
      } else {
        columns_.emplace_back();  // empty string_view → null column
      }
      field_index++;
      continue;
    }

    if (offset + byte_size > length) return false;

    // Decode valueLength (little-endian, byte_size bytes)
    size_t value_length = 0;
    for (uint8_t i = 0; i < byte_size; i++) {
      value_length |= static_cast<size_t>(static_cast<uint8_t>(data[offset + i]))
                      << (CHAR_BIT * i);
    }
    offset += byte_size;

    if (offset + value_length > length) return false;

    if (field_index == 0) {
      null_flags_.assign(data + offset, value_length);
    } else {
      columns_.emplace_back(data + offset, value_length);
    }
    offset += value_length;
    field_index++;
  }

  // If the row ended before all filter columns were present, keep it in the
  // response so MySQL can evaluate the condition.
  return field_index == total_fields;
}

bool PredicateEvaluator::set_row_from_pax(
    const LineairDB::Pax::PaxGroup& group, uint32_t slot,
    uint32_t num_columns) {
  columns_.clear();
  null_flags_.clear();

  if (group.schema().field_count() < static_cast<size_t>(num_columns) + 1) {
    return false;
  }

  const std::string_view null_flags = group.cell(0, slot);
  null_flags_.assign(null_flags.data(), null_flags.size());

  columns_.reserve(num_columns);
  for (uint32_t i = 0; i < num_columns; ++i) {
    columns_.push_back(group.cell(i + 1, slot));
  }
  schema_ = &group.schema();  // typed-cell decode
  return true;
}

bool PredicateEvaluator::set_row_from_pax_cols(
    const LineairDB::Pax::PaxGroup& group, uint32_t slot, uint32_t num_columns,
    const std::vector<uint32_t>& cols) {
  if (group.schema().field_count() < static_cast<size_t>(num_columns) + 1) {
    return false;
  }

  const std::string_view null_flags = group.cell(0, slot);
  null_flags_.assign(null_flags.data(), null_flags.size());

  columns_.assign(num_columns, std::string_view());
  for (uint32_t column_idx : cols) {
    if (column_idx < num_columns) {
      columns_[column_idx] = group.cell(column_idx + 1, slot);
    }
  }
  schema_ = &group.schema();  // typed-cell decode
  return true;
}

void PredicateEvaluator::collect_columns(const FilterExpr& expr,
                                         std::vector<uint32_t>* out) {
  if (expr.op() == FilterExpr::COLUMN_REF) out->push_back(expr.column_index());
  for (const FilterExpr& child : expr.children()) collect_columns(child, out);
  // Callers pass the root once; normalize on the way out.
  std::sort(out->begin(), out->end());
  out->erase(std::unique(out->begin(), out->end()), out->end());
}

void PredicateEvaluator::set_row_from_views(
    const std::vector<std::string_view>& cells,
    const std::vector<bool>& nulls) {
  schema_ = nullptr;  // synthesized rows are already canonical ASCII
  columns_.assign(cells.begin(), cells.end());
  null_flags_.assign((columns_.size() + 7) / 8, 0);
  for (size_t idx = 0; idx < nulls.size() && idx < columns_.size(); ++idx) {
    if (nulls[idx]) {
      null_flags_[idx / 8] =
          static_cast<char>(static_cast<unsigned char>(null_flags_[idx / 8]) |
                            (1u << (idx % 8)));
    }
  }
}

// ---------------------------------------------------------------------------
// Value extraction
// ---------------------------------------------------------------------------

PredicateEvaluator::Val PredicateEvaluator::extract_value(
    const FilterExpr& expr) const {
  Val v;
  switch (expr.op()) {
    case FilterExpr::CONST_INT:
      v.type = ValType::INT;
      v.i = expr.int_val();
      break;
    case FilterExpr::CONST_UINT:
      v.type = ValType::UINT;
      v.u = expr.uint_val();
      break;
    case FilterExpr::CONST_DOUBLE:
      v.type = ValType::DOUBLE;
      v.d = expr.double_val();
      break;
    case FilterExpr::CONST_STRING:
      v.type = ValType::STRING;
      v.s = std::string_view(
          reinterpret_cast<const char*>(expr.string_val().data()),
          expr.string_val().size());
      break;
    case FilterExpr::CONST_NULL:
      v.type = ValType::NONE;
      break;
    case FilterExpr::COLUMN_REF: {
      uint32_t idx = expr.column_index();
      if (idx >= columns_.size()) {
        v.type = ValType::NONE;
        break;
      }
      auto col = columns_[idx];
      if (col.empty()) {
        // Check MySQL null bitmap: column is null if its bit is set.
        // Null flags byte layout: bit 0 of byte 0 = column 0, etc.
        uint32_t byte_pos = idx / 8;
        uint32_t bit_pos = idx % 8;
        if (byte_pos < null_flags_.size() &&
            (static_cast<uint8_t>(null_flags_[byte_pos]) & (1u << bit_pos))) {
          v.type = ValType::NONE;  // NULL
          break;
        }
        // Empty but not null → treat as empty string
        v.type = ValType::STRING;
        v.s = col;
        break;
      }
      // Typed cell: decode the fixed-width binary directly. `col` here is the
      // raw binary (non-empty => present). DATE keeps a YYYYMMDD int so compare()
      // pairs it with a 'YYYY-MM-DD' literal without a per-row format; INT
      // decodes to a native integer (exact -- no re-parse). The ASCII branch
      // below is untouched for UNTYPED columns.
      if (schema_ != nullptr) {
        const uint8_t kind =
            schema_->kind_of(static_cast<size_t>(idx) + 1);
        if (kind == LineairDB::Pax::FK_DATE) {
          int32_t x;
          std::memcpy(&x, col.data(), 4);
          v.type = ValType::DATE;
          v.i = x;
          break;
        }
        if (kind == LineairDB::Pax::FK_INT32 ||
            kind == LineairDB::Pax::FK_INT64) {
          int64_t x;
          if (kind == LineairDB::Pax::FK_INT32) {
            int32_t t;
            std::memcpy(&t, col.data(), 4);
            x = t;
          } else {
            std::memcpy(&x, col.data(), 8);
          }
          switch (expr.compare_type()) {
            case 1:  // UNSIGNED_INT -- stay UINT so a mixed compare with a
                     // CONST_UINT literal > INT64_MAX is not cast to a negative
                     // int64. A typed unsigned column's value fits a positive
                     // int64 (LONG UNSIGNED <= 4.29e9; BIGINT UNSIGNED stays
                     // UNTYPED), so this reinterpret is exact.
              v.type = ValType::UINT;
              v.u = static_cast<uint64_t>(x);
              break;
            case 2:  // DOUBLE
              v.type = ValType::DOUBLE;
              v.d = static_cast<double>(x);
              break;
            case 3: {  // STRING (compare against a string constant)
              std::string& b = next_fmtbuf();
              b = std::to_string(x);
              v.type = ValType::STRING;
              v.s = b;
              break;
            }
            default:  // SIGNED_INT
              v.type = ValType::INT;
              v.i = x;
              break;
          }
          break;
        }
        if (kind == LineairDB::Pax::FK_DEC64) {
          // Reproduce the byte path's DECIMAL compare, which folds the cell to a
          // double via strtod(val_str). For a scaled int64 m with scale s,
          // (double)m / 10^s is bit-identical to strtod of the same value: the
          // proxy's precision<=15 cap keeps m < 2^53 and 10^s exact, so the
          // single IEEE division is correctly rounded. A plain DOUBLE Val then
          // keeps the existing DOUBLE compare byte-exact with no operator-aware
          // integer boundary (q6's 0.07-excluding BETWEEN bound is preserved).
          int64_t m;
          std::memcpy(&m, col.data(), 8);
          const int s = schema_->scale_of(static_cast<size_t>(idx) + 1);
          double p = 1.0;
          for (int k = 0; k < s; ++k) p *= 10.0;
          v.type = ValType::DOUBLE;
          v.d = static_cast<double>(m) / p;
          break;
        }
      }
      // Convert column string to typed value based on compare_type hint.
      switch (expr.compare_type()) {
        case 0: {  // SIGNED_INT
          v.type = ValType::INT;
          if (!parse_int_value(col, &v.i)) {
            // Conversion failed → fall back to string comparison
            v.type = ValType::STRING;
            v.s = col;
          }
          break;
        }
        case 1: {  // UNSIGNED_INT
          v.type = ValType::UINT;
          if (!parse_uint_value(col, &v.u)) {
            v.type = ValType::STRING;
            v.s = col;
          }
          break;
        }
        case 2: {  // DOUBLE
          v.type = ValType::DOUBLE;
          if (!parse_double_value(col, &v.d)) {
            v.type = ValType::STRING;
            v.s = col;
          }
          break;
        }
        default:  // STRING
          v.type = ValType::STRING;
          v.s = col;
          break;
      }
      break;
    }
    default:
      v.type = ValType::NONE;
      break;
  }
  return v;
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

// A DATE operand -> YYYYMMDD int. A 'YYYY-MM-DD' string literal parses to the
// same domain (string order == int order), so a typed DATE column and a date
// literal compare as integers. Returns false for a non-date string.
bool PredicateEvaluator::date_operand_to_int(const Val& v, int64_t* out) {
  if (v.type == ValType::DATE || v.type == ValType::INT) {
    *out = v.i;
    return true;
  }
  if (v.type == ValType::UINT) {
    *out = static_cast<int64_t>(v.u);
    return true;
  }
  if (v.type == ValType::STRING) {
    const std::string_view s = v.s;
    if (s.size() == 10 && s[4] == '-' && s[7] == '-') {
      int64_t y = 0, m = 0, d = 0;
      auto dig = [](const char* p, int n, int64_t* o) {
        for (int i = 0; i < n; i++) {
          if (p[i] < '0' || p[i] > '9') return false;
          *o = *o * 10 + (p[i] - '0');
        }
        return true;
      };
      if (dig(s.data(), 4, &y) && dig(s.data() + 5, 2, &m) &&
          dig(s.data() + 8, 2, &d)) {
        *out = y * 10000 + m * 100 + d;
        return true;
      }
    }
    return false;
  }
  return false;
}

// Canonical "YYYY-MM-DD" of a DATE Val into buf (for the rare fallback where a
// DATE is compared to a non-date string); a STRING Val returns its own view.
std::string_view PredicateEvaluator::date_operand_to_sv(const Val& v, char* buf,
                                                        size_t n) {
  if (v.type == ValType::DATE) {
    const int32_t x = static_cast<int32_t>(v.i);
    const int len = std::snprintf(buf, n, "%04d-%02d-%02d", x / 10000,
                                  (x / 100) % 100, x % 100);
    return std::string_view(buf, len > 0 ? static_cast<size_t>(len) : 0);
  }
  return v.s;
}

int PredicateEvaluator::compare(const Val& lhs, const Val& rhs) {
  if (lhs.type == ValType::NONE || rhs.type == ValType::NONE) return -2;

  // DATE pairing (typed DATE column vs date literal / another DATE): compare
  // YYYYMMDD ints. Falls back to string comparison of the canonical form only
  // when the other operand is not a date literal (rare; preserves the untyped
  // string-compare semantics exactly).
  if (lhs.type == ValType::DATE || rhs.type == ValType::DATE) {
    int64_t li, ri;
    if (date_operand_to_int(lhs, &li) && date_operand_to_int(rhs, &ri))
      return (li < ri) ? -1 : (li > ri) ? 1 : 0;
    char lb[16], rb[16];
    const std::string_view ls = date_operand_to_sv(lhs, lb, sizeof(lb));
    const std::string_view rs = date_operand_to_sv(rhs, rb, sizeof(rb));
    const int c = ls.compare(rs);
    return (c < 0) ? -1 : (c > 0) ? 1 : 0;
  }

  // Promote to common type for comparison
  // INT vs UINT → both to INT (safe for typical DB values)
  // INT/UINT vs DOUBLE → both to DOUBLE
  // Anything vs STRING → string comparison

  if (lhs.type == ValType::STRING || rhs.type == ValType::STRING) {
    // String comparison
    std::string_view ls = lhs.s, rs = rhs.s;
    // For non-string types compared with string, use the string_view if available
    if (lhs.type == ValType::STRING && rhs.type == ValType::STRING) {
      int r = ls.compare(rs);
      return (r < 0) ? -1 : (r > 0) ? 1 : 0;
    }
    // Mixed: fall through to string comparison of the string side
    // This case shouldn't normally happen with correct compare_type
    return ls.compare(rs) < 0 ? -1 : ls.compare(rs) > 0 ? 1 : 0;
  }

  // Numeric comparison
  double dl, dr;
  if (lhs.type == ValType::DOUBLE || rhs.type == ValType::DOUBLE) {
    dl = (lhs.type == ValType::DOUBLE) ? lhs.d
         : (lhs.type == ValType::INT)  ? static_cast<double>(lhs.i)
                                       : static_cast<double>(lhs.u);
    dr = (rhs.type == ValType::DOUBLE) ? rhs.d
         : (rhs.type == ValType::INT)  ? static_cast<double>(rhs.i)
                                       : static_cast<double>(rhs.u);
    return (dl < dr) ? -1 : (dl > dr) ? 1 : 0;
  }

  // Both are integer types
  if (lhs.type == ValType::INT && rhs.type == ValType::INT) {
    return (lhs.i < rhs.i) ? -1 : (lhs.i > rhs.i) ? 1 : 0;
  }
  if (lhs.type == ValType::UINT && rhs.type == ValType::UINT) {
    return (lhs.u < rhs.u) ? -1 : (lhs.u > rhs.u) ? 1 : 0;
  }
  // INT vs UINT: promote both to int64 (works for values < INT64_MAX)
  int64_t li = (lhs.type == ValType::INT) ? lhs.i : static_cast<int64_t>(lhs.u);
  int64_t ri = (rhs.type == ValType::INT) ? rhs.i : static_cast<int64_t>(rhs.u);
  return (li < ri) ? -1 : (li > ri) ? 1 : 0;
}

// ---------------------------------------------------------------------------
// LIKE matching
// ---------------------------------------------------------------------------

bool PredicateEvaluator::like_match(std::string_view text,
                                    std::string_view pattern) {
  size_t ti = 0, pi = 0;
  size_t star_pi = std::string_view::npos, star_ti = 0;

  while (ti < text.size()) {
    if (pi < pattern.size() &&
        (pattern[pi] == '_' || pattern[pi] == text[ti])) {
      pi++;
      ti++;
    } else if (pi < pattern.size() && pattern[pi] == '%') {
      star_pi = pi;
      star_ti = ti;
      pi++;
    } else if (star_pi != std::string_view::npos) {
      pi = star_pi + 1;
      star_ti++;
      ti = star_ti;
    } else {
      return false;
    }
  }
  while (pi < pattern.size() && pattern[pi] == '%') pi++;
  return pi == pattern.size();
}

// ---------------------------------------------------------------------------
// Expression evaluation
// ---------------------------------------------------------------------------

bool PredicateEvaluator::evaluate(const FilterExpr& expr) const {
  switch (expr.op()) {
    // --- Comparison operators ---
    case FilterExpr::OP_EQ:
    case FilterExpr::OP_NE:
    case FilterExpr::OP_LT:
    case FilterExpr::OP_LE:
    case FilterExpr::OP_GT:
    case FilterExpr::OP_GE: {
      if (expr.children_size() < 2) return true;  // malformed → include row
      Val lhs = extract_value(expr.children(0));
      Val rhs = extract_value(expr.children(1));
      int cmp = compare(lhs, rhs);
      if (cmp == -2) return false;  // NULL → condition is unknown → exclude
      switch (expr.op()) {
        case FilterExpr::OP_EQ: return cmp == 0;
        case FilterExpr::OP_NE: return cmp != 0;
        case FilterExpr::OP_LT: return cmp < 0;
        case FilterExpr::OP_LE: return cmp <= 0;
        case FilterExpr::OP_GT: return cmp > 0;
        case FilterExpr::OP_GE: return cmp >= 0;
        default: return true;
      }
    }

    // --- Logical operators ---
    case FilterExpr::OP_AND: {
      for (int i = 0; i < expr.children_size(); i++) {
        if (!evaluate(expr.children(i))) return false;  // short-circuit
      }
      return true;
    }
    case FilterExpr::OP_OR: {
      if (expr.children_size() == 0) return true;
      for (int i = 0; i < expr.children_size(); i++) {
        if (evaluate(expr.children(i))) return true;  // short-circuit
      }
      return false;
    }
    case FilterExpr::OP_NOT: {
      if (expr.children_size() < 1) return true;
      return !evaluate(expr.children(0));
    }

    // --- BETWEEN: children[0] BETWEEN children[1] AND children[2] ---
    case FilterExpr::OP_BETWEEN: {
      if (expr.children_size() < 3) return true;
      Val val = extract_value(expr.children(0));
      Val lo = extract_value(expr.children(1));
      Val hi = extract_value(expr.children(2));
      int cmp_lo = compare(val, lo);
      int cmp_hi = compare(val, hi);
      if (cmp_lo == -2 || cmp_hi == -2) return false;
      bool result = (cmp_lo >= 0 && cmp_hi <= 0);
      return expr.negated() ? !result : result;
    }

    // --- IN: children[0] IN (children[1], children[2], ...) ---
    case FilterExpr::OP_IN: {
      if (expr.children_size() < 2) return true;
      Val val = extract_value(expr.children(0));
      if (val.type == ValType::NONE) return false;
      for (int i = 1; i < expr.children_size(); i++) {
        Val item = extract_value(expr.children(i));
        if (compare(val, item) == 0) {
          return !expr.negated();
        }
      }
      return expr.negated();
    }

    // --- LIKE: children[0] LIKE children[1] ---
    case FilterExpr::OP_LIKE: {
      if (expr.children_size() < 2) return true;
      Val val = extract_value(expr.children(0));
      Val pat = extract_value(expr.children(1));
      if (val.type == ValType::NONE || pat.type == ValType::NONE) return false;
      std::string_view pattern = pat.s;
      // A typed DATE column LIKE-matches on its canonical "YYYY-MM-DD" form
      // (untyped the cell was that ASCII string).
      char db[16];
      std::string_view text;
      if (val.type == ValType::DATE) {
        text = date_operand_to_sv(val, db, sizeof(db));
      } else if (val.type == ValType::STRING) {
        text = val.s;
      } else {
        // LIKE on a numeric column is unusual; include row (safe fallback)
        return true;
      }
      return like_match(text, pattern);
    }

    // --- IS NULL / IS NOT NULL ---
    case FilterExpr::OP_IS_NULL: {
      if (expr.children_size() < 1) return true;
      Val val = extract_value(expr.children(0));
      return val.type == ValType::NONE;
    }
    case FilterExpr::OP_IS_NOT_NULL: {
      if (expr.children_size() < 1) return true;
      Val val = extract_value(expr.children(0));
      return val.type != ValType::NONE;
    }

    default:
      // Unknown op → include row (safe fallback)
      return true;
  }
}
