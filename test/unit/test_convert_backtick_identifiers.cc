// Regression suite for ConvertBacktickIdentifiers
// (proxy/duckdb_sql_rewrite.cc), the backtick-to-doublequote /
// qualifier-stripping rewrite the DuckDB bridge applies to MySQL-printed view
// bodies. See the function's header for the classification rules.
//
// Failure modes pinned by this suite:
// - collapsing an alias-qualified 2-part column ref (`t2`.`x`) the way a
//   table ref is collapsed turns a correlated predicate into the tautology
//   x = x;
// - EXTRACT/TRIM/SUBSTRING's FROM-as-argument-separator syntax must not read
//   as a table position;
// - only the innermost open call suppresses FROM: a subquery nested inside
//   EXTRACT(... FROM (SELECT ...)) contains ordinary table positions again;
// - MySQL's printer wraps multi-table FROM lists in parens directly after
//   from/join (`from (`t1` `a` join `t2` `b` ...)`) with no intervening
//   word, and the first table in the list must still be stripped;
// - identifier lexing includes '_' and digits: my_join / date_from /
//   straight_join must not truncate to the keywords join/from.
//
// Build and run (from the repository root):
//   g++ -std=c++17 -Wall -Wextra -I proxy
//       test/unit/test_convert_backtick_identifiers.cc
//       proxy/duckdb_sql_rewrite.cc -o /tmp/test_convert_backtick
//   /tmp/test_convert_backtick   (one command; wrapped here for width)
//
// Exit code is the number of failed cases (0 = all green).

#include <cstdio>
#include <string>

#include "duckdb_sql_rewrite.hh"

namespace {

int g_failures = 0;
int g_total = 0;

void Check(const std::string &name, const std::string &input,
           const std::string &expected) {
  g_total++;
  const std::string actual = ConvertBacktickIdentifiers(input);
  if (actual == expected) {
    std::printf("[PASS] %s\n", name.c_str());
    return;
  }
  g_failures++;
  std::printf("[FAIL] %s\n", name.c_str());
  std::printf("  input:    %s\n", input.c_str());
  std::printf("  expected: %s\n", expected.c_str());
  std::printf("  actual:   %s\n", actual.c_str());
}

}  // namespace

int main() {
  // --- Baseline: plain table refs -----------------------------------
  Check("plain table ref, db-qualified",
        "select * from `benchbase`.`lineitem`",
        "select * from \"lineitem\"");

  Check("plain table ref, bare (1-part)",
        "select * from `t1`",
        "select * from \"t1\"");

  Check("plain table ref after JOIN keyword",
        "select * from `benchbase`.`t1` join `benchbase`.`t2`",
        "select * from \"t1\" join \"t2\"");

  // --- 2-part and 3-part column refs ---------------------------------
  Check("3-part column ref: drop only leading db",
        "select `benchbase`.`lineitem`.`l_orderkey` from `benchbase`.`lineitem`",
        "select \"lineitem\".\"l_orderkey\" from \"lineitem\"");

  Check("2-part column ref (table-alias qualified) left untouched",
        "select `l`.`l_orderkey` from `benchbase`.`lineitem` `l`",
        "select \"l\".\"l_orderkey\" from \"lineitem\" \"l\"");

  // --- Correlated subquery ---------------------------------------------
  Check("correlated EXISTS: alias-qualified columns not collapsed to x=x",
        "select * from `benchbase`.`t1` where exists (select 1 from "
        "`benchbase`.`t2` where `t2`.`x` = `t1`.`x`)",
        "select * from \"t1\" where exists (select 1 from \"t2\" where "
        "\"t2\".\"x\" = \"t1\".\"x\")");

  // --- JOIN ... ON -----------------------------------------------------
  Check("JOIN ... ON: table refs stripped, ON columns (2-part) untouched",
        "select * from `benchbase`.`t1` join `benchbase`.`t2` on "
        "`t1`.`id` = `t2`.`id`",
        "select * from \"t1\" join \"t2\" on \"t1\".\"id\" = \"t2\".\"id\"");

  // --- Parenthesized join list: MySQL's printer wraps multi-table FROM --
  // --- lists in parens right after from/join with no intervening word, --
  // --- e.g. `from (`t1` `a` join `t2` `b` on (...))`. The first table  --
  // --- right after "from (" must still be detected as a table position. -
  Check("parenthesized join list: first table after 'from (' still "
        "stripped to bare name",
        "select `s`.`s_suppkey`,`n`.`n_name` from (`benchbase`.`supplier` "
        "`s` join `benchbase`.`nation` `n` on(`s`.`s_nationkey` = "
        "`n`.`n_nationkey`))",
        "select \"s\".\"s_suppkey\",\"n\".\"n_name\" from (\"supplier\" "
        "\"s\" join \"nation\" \"n\" on(\"s\".\"s_nationkey\" = "
        "\"n\".\"n_nationkey\"))");

  Check("parenthesized join list: three-way join, parens nested two deep, "
        "every table in the list stripped",
        "select 1 from ((`benchbase`.`t1` join `benchbase`.`t2` on(`t1`."
        "`id` = `t2`.`id`)) join `benchbase`.`t3` on(`t2`.`id` = `t3`."
        "`id`))",
        "select 1 from ((\"t1\" join \"t2\" on(\"t1\".\"id\" = \"t2\"."
        "\"id\")) join \"t3\" on(\"t2\".\"id\" = \"t3\".\"id\"))");

  // --- FROM as a function-argument separator ---------------------------
  Check("EXTRACT: FROM is an argument separator, not a table position",
        "select extract(year from `benchbase`.`lineitem`.`l_shipdate`) "
        "from `benchbase`.`lineitem`",
        "select extract(year from \"lineitem\".\"l_shipdate\") from "
        "\"lineitem\"");

  Check("TRIM: FROM is an argument separator",
        "select trim(both ' ' from `benchbase`.`t1`.`name`) from "
        "`benchbase`.`t1`",
        "select trim(both ' ' from \"t1\".\"name\") from \"t1\"");

  Check("SUBSTRING FROM/FOR: guarded even though MySQL prints it as "
        "substr(str,pos)",
        "select substring(`benchbase`.`t1`.`s` from 1 for 5) from "
        "`benchbase`.`t1`",
        "select substring(\"t1\".\"s\" from 1 for 5) from \"t1\"");

  // --- Innermost-call semantics of the paren stack ----------------------
  Check("EXTRACT(... FROM (subquery)): subquery's own FROM is a table "
        "position again",
        "select extract(year from (select max(`benchbase`.`lineitem`."
        "`l_shipdate`) from `benchbase`.`lineitem` where `benchbase`."
        "`lineitem`.`l_orderkey` = 1)) from `benchbase`.`t1`",
        "select extract(year from (select max(\"lineitem\".\"l_shipdate\") "
        "from \"lineitem\" where \"lineitem\".\"l_orderkey\" = 1)) from "
        "\"t1\"");

  Check("nested EXTRACT inside subquery inside outer EXTRACT: inner "
        "EXTRACT's own FROM suppressed, subquery's FROM not",
        "select extract(year from (select extract(month from "
        "`benchbase`.`t2`.`x`) from `benchbase`.`t2`)) from `benchbase`.`t1`",
        "select extract(year from (select extract(month from "
        "\"t2\".\"x\") from \"t2\")) from \"t1\"");

  Check("TRIM with nested subquery in its FROM argument",
        "select trim(from (select `benchbase`.`t2`.`s` from `benchbase`."
        "`t2` limit 1)) from `benchbase`.`t1`",
        "select trim(from (select \"t2\".\"s\" from \"t2\" limit 1)) from "
        "\"t1\"");

  Check("three levels of EXTRACT/subquery nesting",
        "select extract(year from (select extract(month from (select "
        "extract(day from `benchbase`.`t3`.`d`) from `benchbase`.`t3`)) "
        "from `benchbase`.`t2`)) from `benchbase`.`t1`",
        "select extract(year from (select extract(month from (select "
        "extract(day from \"t3\".\"d\") from \"t3\")) from \"t2\")) from "
        "\"t1\"");

  Check("JOIN...ON with EXTRACT in the ON clause, followed by another real "
        "JOIN: stack returns to empty between them",
        "select * from `benchbase`.`t1` join `benchbase`.`t2` on "
        "extract(year from `t1`.`d`) = extract(year from `t2`.`d`) join "
        "`benchbase`.`t3` on `t2`.`id` = `t3`.`id`",
        "select * from \"t1\" join \"t2\" on extract(year from \"t1\"."
        "\"d\") = extract(year from \"t2\".\"d\") join \"t3\" on \"t2\"."
        "\"id\" = \"t3\".\"id\"");

  Check("two sibling EXTRACT(...FROM subquery...) calls: push/pop does not "
        "leak state between them",
        "select extract(year from (select 1 from `benchbase`.`a`)), "
        "extract(month from (select 1 from `benchbase`.`b`)) from "
        "`benchbase`.`t1`",
        "select extract(year from (select 1 from \"a\")), extract(month "
        "from (select 1 from \"b\")) from \"t1\"");

  Check("plain function call (not EXTRACT/TRIM/SUBSTRING) opens a "
        "non-keyword-fn paren: a later FROM inside it is not suppressed",
        "select max(`benchbase`.`t1`.`x`) from `benchbase`.`t1` where "
        "`benchbase`.`t1`.`id` in (select `benchbase`.`t2`.`id` from "
        "`benchbase`.`t2`)",
        "select max(\"t1\".\"x\") from \"t1\" where \"t1\".\"id\" in "
        "(select \"t2\".\"id\" from \"t2\")");

  Check("mixed-case FROM/JOIN/EXTRACT keywords recognized (word-building "
        "lowercases)",
        "select EXTRACT(YEAR From `benchbase`.`t1`.`d`) FROM "
        "`benchbase`.`t1` JOIN `benchbase`.`t2`",
        "select EXTRACT(YEAR From \"t1\".\"d\") FROM \"t1\" JOIN \"t2\"");

  Check("escaped backtick inside identifier survives",
        "select * from `benchbase`.`weird``name`",
        "select * from \"weird`name\"");

  Check("string literal containing FROM/EXTRACT/parens does not perturb "
        "word-tracking for the backtick that follows",
        "select * from `benchbase`.`t1` where `benchbase`.`t1`.`s` = "
        "'from (extract) join'",
        "select * from \"t1\" where \"t1\".\"s\" = 'from (extract) join'");

  Check("embedded double-quote in identifier is escaped as ''",
        "select * from `benchbase`.`weird\"name`",
        "select * from \"weird\"\"name\"");

  Check("EXTRACT followed at top level by a real FROM after its closing "
        "paren: stack is empty again",
        "select extract(year from `benchbase`.`t1`.`d`) from "
        "`benchbase`.`t1` join `benchbase`.`t2`",
        "select extract(year from \"t1\".\"d\") from \"t1\" join \"t2\"");

  Check("backtick chain right after '(' of a non-keyword fn (preceding "
        "word is \"max\", not from/join) is not a table position",
        "select max(`benchbase`.`t1`.`x`)",
        "select max(\"t1\".\"x\")");

  // --- Identifier lexing: '_' and digits continue the token -------------
  Check("UDF my_join(...): word is the full identifier, not truncated to "
        "\"join\"; the argument column ref is not a table position",
        "select my_join(`benchbase`.`t1`.`x`) from `benchbase`.`t1`",
        "select my_join(\"t1\".\"x\") from \"t1\"");

  Check("UDF date_from(...): word is \"date_from\", not truncated to "
        "\"from\"",
        "select date_from(`benchbase`.`t1`.`d`) from `benchbase`.`t1`",
        "select date_from(\"t1\".\"d\") from \"t1\"");

  Check("STRAIGHT_JOIN SELECT-option keyword: \"straight_join\" does not "
        "truncate to \"join\" and strip the next column ref",
        "select straight_join `t1`.`x`, `t2`.`y` from `benchbase`.`t1` "
        "join `benchbase`.`t2` on `t1`.`id` = `t2`.`id`",
        "select straight_join \"t1\".\"x\", \"t2\".\"y\" from \"t1\" join "
        "\"t2\" on \"t1\".\"id\" = \"t2\".\"id\"");

  // --- from/join as a substring of ordinary identifiers -----------------
  Check("\"join\" as suffix in a bare identifier (SELECT-list alias) is "
        "inert",
        "select `benchbase`.`t1`.`x` as outer_join_flag from "
        "`benchbase`.`t1`",
        "select \"t1\".\"x\" as outer_join_flag from \"t1\"");

  Check("\"join\" as prefix of a function name: joins_table(...) is inert",
        "select joins_table(`benchbase`.`t1`.`x`) from `benchbase`.`t1`",
        "select joins_table(\"t1\".\"x\") from \"t1\"");

  Check("\"from\" as prefix of a function name: from_date(...) is inert",
        "select from_date(`benchbase`.`t1`.`d`) from `benchbase`.`t1`",
        "select from_date(\"t1\".\"d\") from \"t1\"");

  Check("\"join\" as a middle segment of a function name: "
        "x_outer_join_flag_y(...) is inert",
        "select x_outer_join_flag_y(`benchbase`.`t1`.`x`) from "
        "`benchbase`.`t1`",
        "select x_outer_join_flag_y(\"t1\".\"x\") from \"t1\"");

  Check("genuine from clause followed by lookalike function date_from "
        "later in the same query: both resolve independently",
        "select date_from(`benchbase`.`t2`.`d`) from `benchbase`.`t1` join "
        "`benchbase`.`t2` on `t1`.`id` = `t2`.`id`",
        "select date_from(\"t2\".\"d\") from \"t1\" join \"t2\" on \"t1\"."
        "\"id\" = \"t2\".\"id\"");

  Check("digits mixed into an identifier: md5_from_col does not truncate "
        "to \"from\"",
        "select md5_from_col(`benchbase`.`t1`.`x`) from `benchbase`.`t1`",
        "select md5_from_col(\"t1\".\"x\") from \"t1\"");

  std::printf("\n%d / %d passed\n", g_total - g_failures, g_total);
  return g_failures;
}
