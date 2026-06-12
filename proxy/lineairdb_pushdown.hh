#ifndef LINEAIRDB_PUSHDOWN_HH
#define LINEAIRDB_PUSHDOWN_HH

#include <string>

#include "lineairdb.pb.h"

class Item;
class THD;
struct TABLE;
class LineairDBTransaction;

bool serialize_item(const Item *item,
                    LineairDB::Protocol::FilterExpr *expr);

bool prepare_select_filter_for_tx(THD *thd, TABLE *table,
                                  LineairDBTransaction *tx,
                                  std::string *serialized_filter);

// Derive a SOUND table-local predicate for `table` from its owning query
// block's WHERE: top-level AND conjuncts with used_tables()==table, plus the
// table-local NECESSARY condition of OR conjuncts (every disjunct must
// constrain the table, else nothing is pushed). MySQL re-evaluates the full
// WHERE above the handler, so a dropped/relaxed predicate only costs
// over-fetch, never correctness. Returns false when nothing is pushable.
//
// `out_exact` (optional): set true ONLY when the serialized predicate is
// EQUIVALENT to the whole WHERE — every top-level conjunct is a table-local
// atom that serialized verbatim (the OR necessary-condition path is weaker
// and never exact). Aggregation pushdown folds rows the filter passes into
// group rows MySQL cannot re-check, so it must require exactness; plain scan
// filtering is ship-and-recheck and does not care.
bool build_single_table_filter(THD *thd, TABLE *table,
                               std::string *out_serialized,
                               bool *out_exact = nullptr);

#endif // LINEAIRDB_PUSHDOWN_HH
