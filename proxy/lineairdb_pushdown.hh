#ifndef LINEAIRDB_PUSHDOWN_HH
#define LINEAIRDB_PUSHDOWN_HH

#include <functional>
#include <string>

#include "lineairdb.pb.h"
#include "my_table_map.h"  // table_map

class Item;
class Field;
class THD;
struct TABLE;
class LineairDBTransaction;

bool serialize_item(const Item *item,
                    LineairDB::Protocol::FilterExpr *expr);

/**
 * @brief Serialize the table-local NECESSARY condition of a cross-table OR.
 *
 * @details Builds the OR over each branch's `me`-local conjuncts. This is
 * implied by the original OR (never stricter), so it can be pushed as an
 * extra scan pre-filter while the exact predicate still runs downstream.
 * Every branch must constrain `me`; otherwise the derived condition would be
 * stricter than the query and the call fails.
 *
 * @param or_item Candidate cross-table OR predicate.
 * @param me Table whose scan would receive the derived condition.
 * @param out Serialized necessary condition; left unusable when the call fails.
 * @return false when any branch has no `me`-local conjunct or a conjunct fails
 * to serialize.
 */
bool serialize_or_necessary_condition(Item *or_item, table_map me,
                                      LineairDB::Protocol::FilterExpr *out);

using SerializeColumnEncoder = std::function<int(const Field *)>;

/**
 * @brief Install a statement-local column encoder for predicate serialization.
 *
 * @details When unset, `serialize_item()` encodes `Item_field` references as
 * the field index within one table. Joined tuple predicates can set this hook
 * while serializing so each field becomes an ordinal in a tuple-wide column
 * registry.
 */
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
