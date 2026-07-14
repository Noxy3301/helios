#include "duckdb_sql_rewrite.hh"

#include <cctype>
#include <vector>

std::string ConvertBacktickIdentifiers(const std::string &sql) {
  std::string out;
  out.reserve(sql.size() + 8);
  bool in_string = false;
  std::string word;      // most recently completed identifier/keyword token
  bool in_word = false;  // currently inside such a token
  // Stack of currently-open '(': true if it opened a call to EXTRACT/TRIM/
  // SUBSTRING/SUBSTR (see the header for why these four). Only the TOP
  // (innermost, not-yet-closed) entry matters: it tells what kind of paren
  // the current position is directly inside. A subquery nested inside
  // EXTRACT(... FROM (SELECT ... FROM t ...)) opens its own '(' (pushed as
  // false: the word before it is "from", not one of the four names), and
  // inside that subquery its own "from"/"join" keywords introduce ordinary
  // table references again, regardless of the outer EXTRACT further down
  // the stack. A second EXTRACT inside that subquery pushes a fresh true on
  // top and correctly re-suppresses for its own FROM argument.
  std::vector<bool> paren_is_keyword_fn;
  auto inside_keyword_fn = [&]() {
    return !paren_is_keyword_fn.empty() && paren_is_keyword_fn.back();
  };
  size_t i = 0;
  while (i < sql.size()) {
    const char c = sql[i];
    if (in_string) {
      out.push_back(c);
      if (c == '\\' && i + 1 < sql.size()) {
        out.push_back(sql[i + 1]);
        i += 2;
        continue;
      }
      if (c == '\'') {
        if (i + 1 < sql.size() && sql[i + 1] == '\'') {
          out.push_back(sql[i + 1]);
          i += 2;
          continue;
        }
        in_string = false;
      }
      i++;
      continue;
    }
    if (c == '\'') {
      in_string = true;
      out.push_back(c);
      i++;
      word.clear();
      in_word = false;
      continue;
    }
    if (c == '`') {
      const bool is_table_position =
          !inside_keyword_fn() && (word == "from" || word == "join");
      word.clear();
      in_word = false;

      std::vector<std::string> parts;
      while (i < sql.size() && sql[i] == '`') {
        i++;  // opening backtick
        std::string identifier;
        while (i < sql.size()) {
          if (sql[i] == '`') {
            if (i + 1 < sql.size() && sql[i + 1] == '`') {
              identifier.push_back('`');  // `` escape -> literal backtick
              i += 2;
              continue;
            }
            i++;  // closing backtick
            break;
          }
          identifier.push_back(sql[i]);
          i++;
        }
        parts.push_back(std::move(identifier));
        if (i < sql.size() && sql[i] == '.' && i + 1 < sql.size() &&
            sql[i + 1] == '`') {
          i++;  // consume '.'; loop continues into the next qualified part
        } else {
          break;
        }
      }

      size_t keep_from = 0;  // index of first part to emit
      if (is_table_position) {
        keep_from = parts.size() - 1;  // table ref: keep only the bare name
      } else if (parts.size() >= 3) {
        keep_from = 1;  // db.table.column: drop only the leading db
      }
      for (size_t part_index = keep_from; part_index < parts.size();
           part_index++) {
        if (part_index != keep_from) out.push_back('.');
        out.push_back('"');
        for (char part_char : parts[part_index]) {
          if (part_char == '"') out.push_back('"');  // escape embedded " as ""
          out.push_back(part_char);
        }
        out.push_back('"');
      }
      continue;
    }
    if (c == '(') {
      paren_is_keyword_fn.push_back(word == "extract" || word == "trim" ||
                                    word == "substring" || word == "substr");
      // `word` is deliberately NOT cleared here (only `in_word`, so the next
      // alpha run starts fresh rather than appending onto this one). MySQL
      // prints a multi-table FROM list as `from (`t1` `a` join `t2` `b` on
      // (...))` -- the '(' immediately after "from"/"join" has no
      // intervening word, and clearing `word` would make the backtick
      // handler see word=="" instead of "from"/"join" and leave the FIRST
      // table's db qualifier in place, which DuckDB's single-schema catalog
      // rejects. Leaving `word` intact is safe: a fresh alpha run (the
      // normal case, e.g. `extract(year ...`) clears and rebuilds it via
      // the `if (!in_word) word.clear()` branch below.
      in_word = false;
      out.push_back(c);
      i++;
      continue;
    }
    if (c == ')') {
      if (!paren_is_keyword_fn.empty()) paren_is_keyword_fn.pop_back();
      out.push_back(c);
      i++;
      continue;
    }
    // SQL identifiers/keywords are [A-Za-z_][A-Za-z0-9_]*: underscore and
    // digits continue the token. Breaking the token on '_' would truncate an
    // identifier ending in "_join"/"_from" (a UDF name like `my_join` or
    // `date_from`, or the STRAIGHT_JOIN keyword) down to its last segment,
    // which then spuriously equals the literal keyword "join"/"from" and
    // makes the NEXT backtick chain read as a table position. With real
    // identifier lexing, `word` holds the complete token, and equality
    // against "from"/"join"/"extract"/"trim"/"substring"/"substr" succeeds
    // only when the token IS that keyword.
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
      if (!in_word) word.clear();
      word.push_back(
          static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
      in_word = true;
    } else {
      in_word = false;  // word (if any) persists as "most recently completed"
    }
    out.push_back(c);
    i++;
  }
  return out;
}
