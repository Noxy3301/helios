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

  /**
   * @brief Reads predicate inputs directly from a PAX row-group slot.
   *
   * The null-flags field is stored at PAX field 0, and MySQL column N is
   * stored at PAX field N + 1.
   *
   * @return false when the PAX schema cannot contain the requested columns.
   */
  bool set_row_from_pax(const LineairDB::Pax::PaxGroup& group, uint32_t slot,
                        uint32_t num_columns);

  /**
   * @brief Read predicate inputs from already decoded column views.
   *
   * Joined tuple filters build a compact row consisting only of the columns
   * referenced by the predicate. `nulls[i]` marks SQL NULL for `cells[i]`.
   */
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

  // Typed value for comparison.
  enum class ValType { NONE, INT, UINT, DOUBLE, STRING };
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

  // Simple LIKE pattern matching with '%' and '_' wildcards.
  static bool like_match(std::string_view text, std::string_view pattern);
};

#endif  // PREDICATE_EVALUATOR_HH
