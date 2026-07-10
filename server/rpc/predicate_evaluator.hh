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
   * @brief Like set_row_from_pax, but read only the requested column ordinals.
   *
   * `cols` lists the columns the filter expression references (as gathered by
   * collect_columns). Every other column is left as an empty view and must not
   * be read during evaluation.
   *
   * @return false when the PAX schema cannot contain the requested columns.
   */
  bool set_row_from_pax_cols(const LineairDB::Pax::PaxGroup& group,
                             uint32_t slot, uint32_t num_columns,
                             const std::vector<uint32_t>& cols);

  /**
   * @brief Collect the column ordinals referenced by an expression tree.
   *
   * The gathered ordinals are sorted and deduplicated in ascending order.
   */
  static void collect_columns(const LineairDB::Protocol::FilterExpr& expr,
                              std::vector<uint32_t>* out);

  /**
   * @brief Read predicate inputs from already decoded column views.
   *
   * Joined tuple filters build a compact row consisting only of the columns
   * referenced by the predicate. `nulls[i]` marks SQL NULL for `cells[i]`.
   */
  void set_row_from_views(const std::vector<std::string_view>& cells,
                          const std::vector<bool>& nulls);

  // Typed synthesized-row variant: cells hold RAW storage bytes and
  // kinds/scales give the per-column storage kind (FK_*), so extract_value
  // decodes typed binary cells directly -- the same decode as the PAX schema_
  // path -- instead of the caller formatting to ASCII and this evaluator
  // re-parsing it per row (joined-tuple filter and join residual hot paths).
  // kinds[i] == FK_UNTYPED marks a canonical-ASCII cell (virtual tables).
  void set_row_from_views_typed(const std::vector<std::string_view>& cells,
                                const std::vector<bool>& nulls,
                                const std::vector<uint8_t>& kinds,
                                const std::vector<int>& scales);

  // Recursively evaluate a FilterExpr tree against the parsed row.
  // Returns true if the row satisfies the predicate.
  bool evaluate(const LineairDB::Protocol::FilterExpr& expr) const;

 private:
  // Parsed column values (string_view into the original row buffer).
  // Index 0 = first user column (null flags are consumed separately).
  std::vector<std::string_view> columns_;
  // Null bitmap: bit set = column is null.
  std::string null_flags_;

  // Typed cells: schema of the PAX group whose cells columns_ points into
  // (nullptr for the ASCII synthesized-row and materialized-row paths). When a
  // column is typed, extract_value decodes the fixed-width binary directly
  // (INT -> int, DATE -> YYYYMMDD int); UNTYPED keeps the strtoll/strtod ASCII
  // path. Field index for column c is c+1.
  const LineairDB::Pax::TableSchema* schema_ = nullptr;
  // Per-column kinds/scales for the typed views path (empty = all UNTYPED;
  // ignored while schema_ is set -- the two sources are mutually exclusive).
  std::vector<uint8_t> vkinds_;
  std::vector<int> vscales_;
  // Rotating scratch backing the formatted-string Vals a single comparison may
  // hold at once (BETWEEN = 3 operands; two DATE columns in one predicate = 2).
  // 4 is safely above both.
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
