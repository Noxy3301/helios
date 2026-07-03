#ifndef PREDICATE_EVALUATOR_HH
#define PREDICATE_EVALUATOR_HH

#include "lineairdb.pb.h"

#include <lineairdb/pax_store.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// Evaluates a pushed FilterExpr against a single LineairDB row.
// Used in Scan callbacks to skip non-matching rows server-side.
//
// Usage:
//   PredicateEvaluator eval;
//   if (eval.parse_row(data, len, num_cols)) {
//       if (!eval.evaluate(filter_expr)) return false;  // skip row
//   }
//   // parse failure → include row (safe fallback)
class PredicateEvaluator {
 public:
  // Parse a LineairDB row: [null_flags][col_0][col_1]...[col_N-1]
  // Each field: [byteSize:1B][valueLength:byteSize B][value:valueLength B]
  // byteSize == 0xFF means null/empty.
  // Returns false if the row is malformed.
  bool parse_row(const char* data, size_t length, uint32_t num_columns);

  // PAX strip-direct variant of parse_row: point columns_ straight at the
  // group's column cells (no row materialization). The views stay valid as
  // long as the group's write counter is unchanged — the caller's
  // quiescence re-check covers evaluation.
  bool set_row_from_pax(const LineairDB::Pax::PaxGroup& group, uint32_t slot,
                        uint32_t num_columns);

  // Like set_row_from_pax, but fetch only the given column ordinals — the
  // ones the filter expression actually references (collect_columns).
  // Untouched columns stay empty and must not be read by the expression.
  bool set_row_from_pax_cols(const LineairDB::Pax::PaxGroup& group,
                             uint32_t slot, uint32_t num_columns,
                             const std::vector<uint32_t>& cols);

  // Gather the column ordinals referenced by an expression tree
  // (sorted, deduplicated).
  static void collect_columns(const LineairDB::Protocol::FilterExpr& expr,
                              std::vector<uint32_t>* out);

  // Synthesized-row variant for joined tuples: columns_[i] = cells[i];
  // nulls[i] marks SQL NULL (e.g. a LEFT-join miss).
  void set_row_from_views(const std::vector<std::string_view>& cells,
                          const std::vector<bool>& nulls);

  // Recursively evaluate a FilterExpr tree against the parsed row.
  // Returns true if the row satisfies the predicate.
  bool evaluate(const LineairDB::Protocol::FilterExpr& expr) const;

 private:
  // Parsed column values (string_view into the original row buffer).
  // Index 0 = first user column (null flags are consumed separately).
  std::vector<std::string_view> columns_;
  // Null bitmap: bit set = column is null.
  std::string null_flags_;

  // Typed cells (M2): schema of the PAX group whose cells columns_ points into
  // (nullptr for the ASCII synthesized-row path). When a column is typed,
  // extract_value decodes the fixed-width binary directly (INT -> int, DATE ->
  // "YYYY-MM-DD" string so string order == date order); UNTYPED keeps the
  // existing strtoll/strtod ASCII path. Field index for column c is c+1.
  const LineairDB::Pax::TableSchema* schema_ = nullptr;
  // Rotating scratch backing the formatted-string Vals a single comparison may
  // hold at once (BETWEEN = 3 operands; two DATE columns in one predicate,
  // e.g. l_commitdate < l_receiptdate, = 2). 4 is safely above both.
  mutable std::string fmtbuf_[4];
  mutable int fmtidx_ = 0;
  std::string& next_fmtbuf() const {
    std::string& b = fmtbuf_[fmtidx_];
    fmtidx_ = (fmtidx_ + 1) & 3;
    return b;
  }

  // Typed value for comparison. DATE carries a YYYYMMDD int (i) so a typed DATE
  // column compares as an integer (string order == int order for YYYY-MM-DD),
  // avoiding a per-row format; compare() pairs it with a 'YYYY-MM-DD' literal.
  enum class ValType { NONE, INT, UINT, DOUBLE, STRING, DATE };
  struct Val {
    ValType type = ValType::NONE;
    int64_t i = 0;
    uint64_t u = 0;
    double d = 0.0;
    std::string_view s;
  };

  // Extract a typed value from a FilterExpr node (constant or column ref).
  Val extract_value(const LineairDB::Protocol::FilterExpr& expr) const;

  // Compare two values. Returns -1, 0, 1 for <, ==, >.
  // Returns -2 if either value is NONE (NULL semantics: NULL cmp X → unknown).
  static int compare(const Val& lhs, const Val& rhs);

  // DATE helpers (see compare): operand -> YYYYMMDD int (false for a non-date
  // string), and DATE -> canonical "YYYY-MM-DD" into buf for the rare fallback.
  static bool date_operand_to_int(const Val& v, int64_t* out);
  static std::string_view date_operand_to_sv(const Val& v, char* buf, size_t n);

  // Simple LIKE pattern matching with '%' and '_' wildcards.
  static bool like_match(std::string_view text, std::string_view pattern);
};

#endif  // PREDICATE_EVALUATOR_HH
