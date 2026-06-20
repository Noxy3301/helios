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

/**
 * @brief Build a predicate that is safe to run on one table's staged scan.
 *
 * @details Top-level AND predicates can be pushed when they reference only
 * `table`. For OR, every branch must contribute a table-local predicate;
 * otherwise the OR is left for MySQL. MySQL still evaluates the full WHERE, so
 * under-pushing is safe.
 *
 * @param thd Current MySQL session.
 * @param table Table whose staged scan would receive the predicate.
 * @param out_serialized Serialized PushedPredicate output.
 * @return true when a non-empty predicate was serialized.
 */
bool build_single_table_filter(THD *thd, TABLE *table,
                               std::string *out_serialized);

#endif // LINEAIRDB_PUSHDOWN_HH
