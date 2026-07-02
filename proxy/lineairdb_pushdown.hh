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

// Optional column-reference encoder for serialize_item. When set (non-null),
// every Item_field is encoded via this callback (return the column_index to
// emit, or -1 to reject) instead of the default single-table field_index().
// Used by the secondary engine to serialize predicates over joined tuples.
#include <functional>
class Field;
using SerializeColumnEncoder = std::function<int(const Field *)>;
void set_serialize_column_encoder(const SerializeColumnEncoder *encoder);

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
