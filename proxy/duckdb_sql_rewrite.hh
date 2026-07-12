#ifndef DUCKDB_SQL_REWRITE_HH
#define DUCKDB_SQL_REWRITE_HH

#include <string>

/**
 * @brief Rewrites a MySQL-printed SQL fragment into DuckDB-parseable form.
 *
 * @details Two rewrites in one lexical pass:
 * - quoting: backtick identifiers become double-quoted (`x` -> "x");
 * - qualification: table references lose every qualifier
 *   (`db`.`table` -> "table"); the bridge's DuckDB catalog registers bare
 *   table names only (see RegisterPaxView in duckdb_bridge_executor.cc).
 *
 * The input is always MySQL printer output (Table_ref::view_body_utf8,
 * used for view->CTE expansion), never client SQL: the printer's
 * conventions are fixed, which is what makes the position rules below
 * sound.
 *
 * Table/column classification, by syntax alone (no catalog access):
 * - chain directly after "from"/"join": table reference, keep the last
 *   part (the printer emits these keywords lowercase for every FROM-clause
 *   list, including comma lists: `from (`t1` `a` join `t2` `b`)`);
 * - 3-part chain elsewhere (`db`.`table`.`column`): drop the leading db;
 * - 2-part chain elsewhere (`alias`.`column`): untouched -- collapsing it
 *   would turn a correlated predicate `t2`.`x` = `t1`.`x` into x = x.
 *
 * Guards:
 * - EXTRACT/TRIM/SUBSTRING/SUBSTR use FROM as an argument separator; a
 *   paren stack keyed on the innermost open call excludes them from the
 *   table-position rule. NTH_VALUE's FROM {FIRST|LAST} never appears in
 *   printed output and is not covered.
 * - Reprinting the live view tree with QT_NO_DB is not a substitute for
 *   this rewrite: the post-optimization tree prints Item_cache artifacts
 *   ("<cache>(expr)") and ON-less semi joins, which DuckDB cannot parse;
 *   view_body_utf8 is a pre-optimization snapshot free of both.
 *
 * @param sql MySQL-printed SQL fragment.
 * @return The fragment in DuckDB identifier syntax.
 */
std::string ConvertBacktickIdentifiers(const std::string &sql);

#endif  // DUCKDB_SQL_REWRITE_HH
