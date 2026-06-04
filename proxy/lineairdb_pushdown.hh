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

#endif // LINEAIRDB_PUSHDOWN_HH
