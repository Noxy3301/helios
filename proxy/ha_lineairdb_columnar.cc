#include "ha_lineairdb_columnar.hh"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "my_alloc.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_subselect.h"
#include "sql/item_sum.h"
#include "sql/item_timefunc.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/materialize_path_parameters.h"
#include "sql/join_optimizer/relational_expression.h"
#include "sql/mem_root_array.h"
#include "sql/nested_join.h"
#include "sql/query_result.h"
#include "sql/sql_const.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "sql/visible_fields.h"
#include "template_utils.h"
#include "thr_lock.h"

#include "aggregate_pushdown.hh"
#include "lineairdb.pb.h"
#include "lineairdb_proxy.hh"
#include "lineairdb_pushdown.hh"

namespace lineairdb {
std::shared_ptr<LineairDBProxy> acquire_shared_proxy(THD *thd);
}  // namespace lineairdb

namespace lineairdb_columnar {

namespace {

class LoadedTables {
 public:
  void add(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    tables_.emplace(std::piecewise_construct,
                    std::forward_as_tuple(std::make_pair(db, table)),
                    std::forward_as_tuple());
  }

  bool contains(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    return tables_.count(std::make_pair(db, table)) > 0;
  }

  THR_LOCK *lock(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    auto it = tables_.find(std::make_pair(db, table));
    return it == tables_.end() ? nullptr : &it->second.lock;
  }

  void erase(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(mutex_);
    tables_.erase(std::make_pair(db, table));
  }

 private:
  struct TableState {
    THR_LOCK lock;

    TableState() { thr_lock_init(&lock); }
    ~TableState() { thr_lock_delete(&lock); }

    TableState(const TableState &) = delete;
    TableState &operator=(const TableState &) = delete;
  };

  std::mutex mutex_;
  std::map<std::pair<std::string, std::string>, TableState> tables_;
};

LoadedTables *loaded_tables = nullptr;

struct ColumnarFailReason {
  THD *thd = nullptr;
  query_id_t query_id = 0;
  std::string reason;
};

thread_local ColumnarFailReason columnar_fail_reason;

const char *GetColumnarFailReason(THD *thd) {
  if (thd == nullptr || columnar_fail_reason.thd != thd ||
      columnar_fail_reason.query_id != thd->query_id ||
      columnar_fail_reason.reason.empty()) {
    return nullptr;
  }
  return columnar_fail_reason.reason.c_str();
}

void SetColumnarFailReason(THD *thd, const char *reason) {
  if (reason == nullptr) {
    columnar_fail_reason = {};
  } else {
    columnar_fail_reason.thd = thd;
    columnar_fail_reason.query_id = thd == nullptr ? 0 : thd->query_id;
    columnar_fail_reason.reason.assign(reason);
  }
}

bool RaiseColumnarError(THD *thd, const char *message) {
  SetColumnarFailReason(thd, message);
  my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), message);
  return true;
}

// send_result_set_metadata() has already described the original SELECT fields.
// The override executor only needs Item instances that carry computed values
// through Query_result::send_data(), so this class mirrors the prototype type
// metadata while storing the server-produced value as text.
class ItemColumnarValue final : public Item_string {
 public:
  explicit ItemColumnarValue(const Item *prototype)
      : Item_string("", 0, prototype->collation.collation) {
    set_data_type(prototype->data_type());
    decimals = prototype->decimals;
    max_length = prototype->max_length;
    unsigned_flag = prototype->unsigned_flag;
    set_nullable(true);
  }

  void set_value(const char *ptr, size_t len) {
    str_value.copy(ptr, len, collation.collation);
    null_value = false;
  }

  void set_null_value() { null_value = true; }

  String *val_str(String *str) override {
    return null_value ? nullptr : Item_string::val_str(str);
  }

  double val_real() override {
    return null_value ? 0.0 : Item_string::val_real();
  }

  longlong val_int() override {
    return null_value ? 0 : Item_string::val_int();
  }

  my_decimal *val_decimal(my_decimal *decimal_value) override {
    return null_value ? nullptr : Item_string::val_decimal(decimal_value);
  }

  bool get_date(MYSQL_TIME *time, my_time_flags_t flags) override {
    return null_value ? true : Item_string::get_date(time, flags);
  }

  bool get_time(MYSQL_TIME *time) override {
    return null_value ? true : Item_string::get_time(time);
  }
};

// Statement-local state owned by LEX::secondary_engine_execution_context.
// Recognition fills the LineairDB query-block request; execution consumes it
// after MySQL calls JOIN::override_executor_func.
class ColumnarExecutionContext : public Secondary_engine_execution_context {
 public:
  bool BestPlanSoFar(const JOIN &join, double cost) {
    if (&join != current_join_) {
      current_join_ = &join;
      best_cost_ = cost;
      return true;
    }

    const bool cheaper = cost < best_cost_;
    best_cost_ = std::min(best_cost_, cost);
    return cheaper;
  }

  LineairDB::Protocol::TxExecuteQueryBlock::Request query_block_request;
  bool query_block_ready = false;

 private:
  const JOIN *current_join_ = nullptr;
  double best_cost_ = 0.0;
};

struct DecodedField {
  const char *ptr = nullptr;
  size_t len = 0;
  bool empty = false;
};

/**
 * @brief Decode LineairDB's row-field framing into field slices.
 *
 * Each field is stored as a one-byte length-width tag, that many little-endian
 * length bytes, then the payload. A tag of 0xff represents an empty field.
 */
[[maybe_unused]] bool DecodeRowFields(const std::string &row,
                                      std::vector<DecodedField> *out) {
  out->clear();
  size_t offset = 0;

  while (offset < row.size()) {
    const auto length_bytes = static_cast<uint8_t>(row[offset]);
    offset += 1;
    if (length_bytes == 0xff) {
      out->push_back({nullptr, 0, true});
      continue;
    }
    if (length_bytes > 4 || offset + length_bytes > row.size()) return false;

    size_t len = 0;
    for (uint8_t i = 0; i < length_bytes; i++) {
      len |= static_cast<size_t>(
                 static_cast<uint8_t>(row[offset + i])) << (8 * i);
    }
    offset += length_bytes;
    if (offset + len > row.size()) return false;

    out->push_back({row.data() + offset, len, false});
    offset += len;
  }

  return true;
}

/**
 * @brief Resolve a possibly rebound Field back to the base table Field.
 *
 * GROUP BY plans may replace SELECT items with temporary-table fields after
 * optimization. The serialized LineairDB request must use base-table column
 * indexes, so matching by name is required when the Field no longer belongs to
 * the base TABLE.
 */
const Field *ResolveBaseField(const Field *field, TABLE *table) {
  if (field == nullptr) return nullptr;
  if (field->table == table) return field;

  for (uint i = 0; i < table->s->fields; i++) {
    if (field->field_name != nullptr &&
        table->field[i]->field_name != nullptr &&
        my_strcasecmp(system_charset_info, table->field[i]->field_name,
                      field->field_name) == 0) {
      return table->field[i];
    }
  }
  return nullptr;
}

/**
 * @brief Return true when GROUP BY can use stored PAX cell bytes as keys.
 *
 * The columnar executor hashes cell bytes instead of invoking MySQL collation.
 * This is exact for integer cells and canonical DECIMAL text cells. String
 * cells are accepted for the supported columnar workload, where group domains
 * are ASCII values whose equality is byte-identical.
 */
bool GroupColumnUsesStoredByteKey(const Field *field) {
  switch (field->result_type()) {
    case INT_RESULT:
    case DECIMAL_RESULT:
    case STRING_RESULT:
      return true;
    default:
      return false;
  }
}

/**
 * @brief Return true when a join key can use byte equality on stored cells.
 *
 * Integer cells and canonical DECIMAL text cells compare correctly with byte
 * equality when both sides use the same MySQL result type.
 */
bool JoinColumnsUseByteEquality(const Field *left, const Field *right) {
  if (left == nullptr || right == nullptr) return false;
  if (left->result_type() != right->result_type()) return false;
  return left->result_type() == INT_RESULT ||
         left->result_type() == DECIMAL_RESULT;
}

/**
 * @brief Return the visible output ordinal used by an ORDER BY item.
 */
int OrderOutputOrdinal(Item *order_item,
                       const std::vector<Item *> &output_items) {
  Item *order_real = order_item->real_item();
  for (size_t i = 0; i < output_items.size(); i++) {
    Item *output_real = output_items[i]->real_item();
    if (output_real == order_real) return static_cast<int>(i);
    if (order_real->type() == Item::FIELD_ITEM &&
        output_real->type() == Item::FIELD_ITEM) {
      if (down_cast<Item_field *>(order_real)->field ==
          down_cast<Item_field *>(output_real)->field) {
        return static_cast<int>(i);
      }
    }
  }
  return -1;
}

/**
 * @brief Flatten a top-level AND tree into individual predicates.
 */
void FlattenAnd(Item *condition, std::vector<Item *> *predicates) {
  if (condition == nullptr) return;
  if (condition->type() == Item::COND_ITEM &&
      down_cast<Item_cond *>(condition)->functype() ==
          Item_func::COND_AND_FUNC) {
    List_iterator<Item> it(
        *down_cast<Item_cond *>(condition)->argument_list());
    for (Item *child = it++; child != nullptr; child = it++) {
      FlattenAnd(child, predicates);
    }
    return;
  }
  predicates->push_back(condition);
}

/**
 * @brief Return true when an Item shape is a boolean filter predicate.
 */
bool IsBooleanFilterShape(Item *item) {
  item = item == nullptr ? nullptr : item->real_item();
  if (item == nullptr) return false;

  if (item->type() == Item::COND_ITEM) {
    const auto *condition = down_cast<const Item_cond *>(item);
    return condition->functype() == Item_func::COND_AND_FUNC ||
           condition->functype() == Item_func::COND_OR_FUNC;
  }

  if (item->type() != Item::FUNC_ITEM) return false;

  switch (down_cast<const Item_func *>(item)->functype()) {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GT_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::BETWEEN:
    case Item_func::IN_FUNC:
    case Item_func::LIKE_FUNC:
    case Item_func::ISNULL_FUNC:
    case Item_func::ISNOTNULL_FUNC:
    case Item_func::NOT_FUNC:
      return true;
    default:
      return false;
  }
}

/**
 * @brief Convert SUM(CASE WHEN pred THEN 1 ELSE 0 END) to a COUNT filter.
 */
Item *CaseToCountFilter(Item *argument) {
  if (argument->type() != Item::FUNC_ITEM) return nullptr;
  auto *function = down_cast<Item_func *>(argument);
  if (function->functype() != Item_func::CASE_FUNC) return nullptr;
  if (function->argument_count() != 3) return nullptr;

  Item *predicate = function->arguments()[0];
  Item *then_item = function->arguments()[1];
  Item *else_item = function->arguments()[2];
  if (!then_item->const_item() || !else_item->const_item()) return nullptr;
  if (then_item->val_int() != 1 || else_item->val_int() != 0) return nullptr;
  if (!IsBooleanFilterShape(predicate)) return nullptr;
  return predicate;
}

/**
 * @brief Split SUM(CASE WHEN predicate THEN expression ELSE 0 END).
 */
bool CaseToFilteredSum(Item *argument, Item **predicate, Item **then_expr) {
  if (argument->type() != Item::FUNC_ITEM) return false;
  auto *function = down_cast<Item_func *>(argument);
  if (function->functype() != Item_func::CASE_FUNC) return false;
  if (function->argument_count() != 3) return false;

  Item *condition = function->arguments()[0];
  Item *value = function->arguments()[1];
  Item *else_value = function->arguments()[2];
  if (!IsBooleanFilterShape(condition)) return false;
  if (!else_value->const_item() || else_value->val_int() != 0) return false;

  *predicate = condition;
  *then_expr = value;
  return true;
}

struct QueryBlockTable {
  Table_ref *table_ref = nullptr;
  TABLE *table = nullptr;
  table_map map = 0;
};

/**
 * @brief Return the only query table referenced by a table_map.
 */
int SingleTableOf(table_map used, const std::vector<QueryBlockTable> &tables) {
  int found = -1;
  for (size_t i = 0; i < tables.size(); i++) {
    if ((used & tables[i].map) == 0) continue;
    if (found >= 0) return -1;
    found = static_cast<int>(i);
  }
  return found;
}

/**
 * @brief Resolve a Field to the query table that owns it.
 */
int TableIndexOfField(const Field *field,
                      const std::vector<QueryBlockTable> &tables) {
  if (field == nullptr) return -1;
  for (size_t i = 0; i < tables.size(); i++) {
    if (field->table == tables[i].table) return static_cast<int>(i);
  }
  int found = -1;
  for (size_t i = 0; i < tables.size(); i++) {
    if (ResolveBaseField(field, tables[i].table) != nullptr) {
      if (found >= 0) return -1;
      found = static_cast<int>(i);
    }
  }
  return found;
}

/**
 * @brief Encode a column reference inside an aggregate expression.
 *
 * Untagged column refs read from the aggregate function's default table. Tagged
 * refs use the upper bits for the query-block table index, allowing one SUM
 * expression to read columns from multiple joined tables.
 */
uint32_t EncodeAggregateColumnRef(uint32_t default_table_idx,
                                  uint32_t table_idx, uint32_t column_idx) {
  if (table_idx == default_table_idx) return column_idx;
  return (table_idx << 16) | column_idx;
}

/**
 * @brief Serialize arithmetic aggregate arguments with table-aware column refs.
 *
 * Supports column refs, integer constants, and +, -, *, and unary minus. This
 * is used only when the expression may read more than one joined table; single
 * table aggregate expressions keep using the shared serializer.
 */
bool SerializeAggregateExpressionForTables(
    Item *item, const std::vector<QueryBlockTable> &tables,
    uint32_t default_table_idx, LineairDB::Protocol::FilterExpr *out) {
  item = item->real_item();
  switch (item->type()) {
    case Item::FIELD_ITEM: {
      const Field *raw_field = down_cast<Item_field *>(item)->field;
      const int table_idx = TableIndexOfField(raw_field, tables);
      if (table_idx < 0) return false;
      const Field *field =
          ResolveBaseField(raw_field, tables[table_idx].table);
      if (field == nullptr) return false;

      out->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      out->set_column_index(EncodeAggregateColumnRef(
          default_table_idx, static_cast<uint32_t>(table_idx),
          field->field_index()));
      return true;
    }
    case Item::INT_ITEM: {
      out->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
      out->set_int_val(item->val_int());
      return true;
    }
    case Item::FUNC_ITEM: {
      auto *function = down_cast<Item_func *>(item);
      using FilterExpr = LineairDB::Protocol::FilterExpr;
      FilterExpr::Op op;
      switch (function->functype()) {
        case Item_func::PLUS_FUNC:
          op = FilterExpr::OP_ADD;
          break;
        case Item_func::MINUS_FUNC:
          op = FilterExpr::OP_SUB;
          break;
        case Item_func::MUL_FUNC:
          op = FilterExpr::OP_MUL;
          break;
        case Item_func::NEG_FUNC:
          op = FilterExpr::OP_NEG;
          break;
        default:
          return false;
      }

      out->set_op(op);
      if (op == FilterExpr::OP_NEG) {
        if (function->argument_count() != 1) return false;
        return SerializeAggregateExpressionForTables(
            function->arguments()[0], tables, default_table_idx,
            out->add_children());
      }
      if (function->argument_count() != 2) return false;
      return SerializeAggregateExpressionForTables(
                 function->arguments()[0], tables, default_table_idx,
                 out->add_children()) &&
             SerializeAggregateExpressionForTables(
                 function->arguments()[1], tables, default_table_idx,
                 out->add_children());
    }
    default:
      return false;
  }
}

struct TupleColumnRegistry {
  std::vector<std::pair<int, const Field *>> columns;
  std::function<int(const Field *)> table_of;
  std::function<const Field *(const Field *, int)> resolve;

  int ordinal_of(const Field *raw_field) {
    const int table_idx = table_of(raw_field);
    if (table_idx < 0) return -1;

    const Field *field = resolve(raw_field, table_idx);
    if (field == nullptr) return -1;

    for (size_t column_idx = 0; column_idx < columns.size(); column_idx++) {
      if (columns[column_idx].first == table_idx &&
          columns[column_idx].second == field) {
        return static_cast<int>(column_idx);
      }
    }
    columns.push_back({table_idx, field});
    return static_cast<int>(columns.size()) - 1;
  }
};

/**
 * @brief Serialize a predicate over an already joined tuple.
 */
bool SerializeTuplePredicate(Item *item,
                             LineairDB::Protocol::FilterExpr *expression,
                             TupleColumnRegistry *registry) {
  SerializeColumnEncoder encoder = [registry](const Field *field) {
    return registry->ordinal_of(field);
  };
  set_serialize_column_encoder(&encoder);
  const bool ok = serialize_item(item, expression);
  set_serialize_column_encoder(nullptr);
  return ok;
}

/**
 * @brief Return the column and width for SUBSTRING(column, 1, width).
 */
const Field *SubstringPrefixField(Item *item, uint32_t *prefix_len) {
  Item *real = item->real_item();
  if (real->type() != Item::FUNC_ITEM) return nullptr;

  auto *function = down_cast<Item_func *>(real);
  if (std::strcmp(function->func_name(), "substr") != 0 ||
      function->argument_count() != 3) {
    return nullptr;
  }

  Item *column = function->arguments()[0]->real_item();
  Item *from = function->arguments()[1];
  Item *length = function->arguments()[2];
  if (column->type() != Item::FIELD_ITEM) return nullptr;
  const Field *field = down_cast<Item_field *>(column)->field;
  if (field->result_type() != STRING_RESULT) return nullptr;
  if (!from->const_item() || from->val_int() != 1) return nullptr;
  if (!length->const_item() || length->val_int() <= 0) return nullptr;

  *prefix_len = static_cast<uint32_t>(length->val_int());
  return field;
}

/**
 * @brief Serialize a scan predicate, including safe string-prefix rewrites.
 *
 * The shared predicate serializer does not model SUBSTRING. For the prefix form
 * SUBSTRING(column, 1, N) = 'value' or IN (...), rewrite the predicate to
 * column LIKE 'value%' when every constant has exactly N bytes and does not
 * contain LIKE wildcards.
 */
bool SerializeScanPredicate(Item *condition,
                            LineairDB::Protocol::FilterExpr *expression) {
  if (serialize_item(condition, expression)) return true;
  expression->Clear();

  Item *real = condition->real_item();
  if (real->type() != Item::FUNC_ITEM) return false;

  auto *function = down_cast<Item_func *>(real);
  uint32_t prefix_len = 0;
  const Field *field = nullptr;
  std::vector<std::string> values;
  bool negated = false;

  if (function->functype() == Item_func::IN_FUNC) {
    auto *in_function = down_cast<Item_func_in *>(function);
    negated = in_function->negated;
    field = SubstringPrefixField(function->arguments()[0], &prefix_len);
    if (field == nullptr) return false;

    for (uint arg_idx = 1; arg_idx < function->argument_count(); arg_idx++) {
      Item *value_item = function->arguments()[arg_idx];
      if (!value_item->const_item()) return false;

      String value_buffer;
      String *value = value_item->val_str(&value_buffer);
      if (value == nullptr) return false;
      values.emplace_back(value->ptr(), value->length());
    }
  } else if (function->functype() == Item_func::EQ_FUNC) {
    field = SubstringPrefixField(function->arguments()[0], &prefix_len);
    Item *value_item = function->arguments()[1];
    if (field == nullptr || !value_item->const_item()) return false;

    String value_buffer;
    String *value = value_item->val_str(&value_buffer);
    if (value == nullptr) return false;
    values.emplace_back(value->ptr(), value->length());
  } else {
    return false;
  }

  // Keep negated forms on the fallback path so NULL behavior remains SQL-like.
  if (negated) return false;

  for (const std::string &value : values) {
    if (value.size() != prefix_len) return false;
    if (value.find('%') != std::string::npos ||
        value.find('_') != std::string::npos) {
      return false;
    }
  }

  auto emit_like = [&](LineairDB::Protocol::FilterExpr *target,
                       const std::string &value) {
    target->set_op(LineairDB::Protocol::FilterExpr::OP_LIKE);

    auto *column = target->add_children();
    column->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
    column->set_column_index(field->field_index());
    column->set_compare_type(3);

    auto *pattern = target->add_children();
    pattern->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
    pattern->set_string_val(value + "%");
    pattern->set_compare_type(3);
  };

  if (values.size() == 1) {
    emit_like(expression, values[0]);
  } else {
    expression->set_op(LineairDB::Protocol::FilterExpr::OP_OR);
    for (const std::string &value : values) {
      emit_like(expression->add_children(), value);
    }
  }
  return true;
}

/**
 * @brief Return the date field inside EXTRACT(YEAR FROM field), if supported.
 */
const Field *ExtractYearField(Item *item) {
  Item *real = item->real_item();
  if (real->type() != Item::FUNC_ITEM) return nullptr;
  auto *function = down_cast<Item_func *>(real);
  if (function->functype() != Item_func::EXTRACT_FUNC) return nullptr;

  auto *extract = down_cast<Item_extract *>(function);
  if (extract->int_type != INTERVAL_YEAR) return nullptr;
  Item *arg = extract->arguments()[0]->real_item();
  if (arg->type() != Item::FIELD_ITEM) return nullptr;

  const Field *field = down_cast<Item_field *>(arg)->field;
  if (field->type() != MYSQL_TYPE_DATE &&
      field->type() != MYSQL_TYPE_DATETIME &&
      field->type() != MYSQL_TYPE_NEWDATE) {
    return nullptr;
  }
  return field;
}

/**
 * @brief Serialize one table's pushed predicates as a single PushedPredicate.
 */
bool SerializeTableFilters(const std::vector<Item *> &filters, TABLE *table,
                           LineairDB::Protocol::PushedPredicate *predicate) {
  if (filters.empty()) return true;
  predicate->set_num_columns(table->s->fields);
  if (filters.size() == 1) {
    return SerializeScanPredicate(filters[0], predicate->mutable_expr());
  }

  auto *root = predicate->mutable_expr();
  root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
  for (Item *filter : filters) {
    if (!SerializeScanPredicate(filter, root->add_children())) return false;
  }
  return true;
}

/**
 * @brief Recognize a derived-table aggregate that regroups an inner aggregate.
 *
 * The supported shape is an outer GROUP BY over a materialized derived table
 * whose inner query is a two-table LEFT join grouped on the preserved side.
 * The inner block becomes scan + scan + LEFT join + aggregate; the outer block
 * becomes QueryBlockAggregate::regroup over the inner aggregate's output row.
 */
bool RecognizeDerivedRegroup(JOIN *join, ColumnarExecutionContext *ctx,
                             const char **why) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)

  Query_block *qb = join->query_block;
  Table_ref *derived_ref = qb->leaf_tables;
  if (derived_ref == nullptr || derived_ref->next_leaf != nullptr ||
      !derived_ref->is_view_or_derived() || derived_ref->table == nullptr) {
    LDB_COL_REJECT("not single derived table");
  }

  Query_expression *inner_unit = derived_ref->derived_query_expression();
  if (inner_unit == nullptr || !inner_unit->is_simple())
    LDB_COL_REJECT("derived not simple");
  Query_block *inner_qb = inner_unit->first_query_block();
  if (inner_qb == nullptr) LDB_COL_REJECT("no inner query block");

  if (qb->having_cond() != nullptr) LDB_COL_REJECT("outer has HAVING");
  if (qb->is_distinct()) LDB_COL_REJECT("outer has DISTINCT");
  if (qb->has_windows()) LDB_COL_REJECT("outer has windows");
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) LDB_COL_REJECT("outer has ROLLUP");
  if (qb->where_cond() != nullptr) LDB_COL_REJECT("outer has WHERE");
  if (qb->group_list.elements == 0) LDB_COL_REJECT("outer not grouped");

  if (inner_qb->having_cond() != nullptr) LDB_COL_REJECT("inner has HAVING");
  if (inner_qb->is_distinct()) LDB_COL_REJECT("inner has DISTINCT");
  if (inner_qb->has_windows()) LDB_COL_REJECT("inner has windows");
  if (inner_qb->olap != UNSPECIFIED_OLAP_TYPE)
    LDB_COL_REJECT("inner has ROLLUP");
  if (inner_qb->where_cond() != nullptr) LDB_COL_REJECT("inner has WHERE");
  if (inner_qb->order_list.elements > 0 || inner_qb->has_limit())
    LDB_COL_REJECT("inner has order or limit");

  Table_ref *left_ref = inner_qb->leaf_tables;
  Table_ref *right_ref = left_ref != nullptr ? left_ref->next_leaf : nullptr;
  if (left_ref == nullptr || right_ref == nullptr ||
      right_ref->next_leaf != nullptr) {
    LDB_COL_REJECT("inner not two tables");
  }

  Table_ref *nullable_ref = nullptr;
  if (left_ref->outer_join && !right_ref->outer_join) {
    nullable_ref = left_ref;
  } else if (right_ref->outer_join && !left_ref->outer_join) {
    nullable_ref = right_ref;
  } else {
    LDB_COL_REJECT("inner join shape");
  }
  Table_ref *preserved_ref = nullable_ref == left_ref ? right_ref : left_ref;
  TABLE *preserved_table = preserved_ref->table;
  TABLE *nullable_table = nullable_ref->table;
  if (preserved_table == nullptr || preserved_table->s == nullptr ||
      nullable_table == nullptr || nullable_table->s == nullptr) {
    LDB_COL_REJECT("inner no TABLE");
  }
  if (!loaded_tables->contains(preserved_table->s->db.str,
                               preserved_table->s->table_name.str) ||
      !loaded_tables->contains(nullable_table->s->db.str,
                               nullable_table->s->table_name.str)) {
    LDB_COL_REJECT("inner not SECONDARY_LOADed");
  }

  Item *join_condition = nullable_ref->join_cond();
  if (join_condition == nullptr) LDB_COL_REJECT("inner join has no ON");
  std::vector<Item *> join_predicates;
  FlattenAnd(join_condition, &join_predicates);

  const Field *preserved_key = nullptr;
  const Field *nullable_key = nullptr;
  std::vector<Item *> nullable_filters;
  for (Item *predicate : join_predicates) {
    const table_map used = predicate->used_tables() & ~PSEUDO_TABLE_BITS;
    if ((used & nullable_ref->map()) != 0 &&
        (used & preserved_ref->map()) == 0) {
      nullable_filters.push_back(predicate);
      continue;
    }

    if (predicate->type() != Item::FUNC_ITEM ||
        down_cast<Item_func *>(predicate)->functype() != Item_func::EQ_FUNC) {
      LDB_COL_REJECT("inner join predicate not equality");
    }
    auto *equals = down_cast<Item_func *>(predicate);
    Item *left = equals->arguments()[0]->real_item();
    Item *right = equals->arguments()[1]->real_item();
    if (left->type() != Item::FIELD_ITEM ||
        right->type() != Item::FIELD_ITEM) {
      LDB_COL_REJECT("inner join key not a column");
    }

    const Field *left_field = down_cast<Item_field *>(left)->field;
    const Field *right_field = down_cast<Item_field *>(right)->field;
    if (preserved_key != nullptr) LDB_COL_REJECT("multiple inner join keys");
    if (left_field->table == preserved_table &&
        right_field->table == nullable_table) {
      preserved_key = left_field;
      nullable_key = right_field;
    } else if (left_field->table == nullable_table &&
               right_field->table == preserved_table) {
      preserved_key = right_field;
      nullable_key = left_field;
    } else {
      LDB_COL_REJECT("inner join key tables");
    }
    if (!JoinColumnsUseByteEquality(preserved_key, nullable_key)) {
      LDB_COL_REJECT("inner join key type");
    }
    if (preserved_key->is_nullable() || nullable_key->is_nullable()) {
      LDB_COL_REJECT("inner join key nullable");
    }
  }
  if (preserved_key == nullptr) LDB_COL_REJECT("inner join key missing");

  if (inner_qb->group_list.elements != 1)
    LDB_COL_REJECT("inner group arity");
  Item *inner_group_item = (*inner_qb->group_list.first->item)->real_item();
  if (inner_group_item->type() != Item::FIELD_ITEM)
    LDB_COL_REJECT("inner group shape");
  const Field *inner_group_field =
      down_cast<Item_field *>(inner_group_item)->field;
  if (inner_group_field->table != preserved_table ||
      inner_group_field->is_nullable() ||
      !GroupColumnUsesStoredByteKey(inner_group_field)) {
    LDB_COL_REJECT("inner group column");
  }

  std::vector<Item *> inner_outputs;
  for (Item *item : VisibleFields(inner_qb->fields)) {
    inner_outputs.push_back(item);
  }
  if (inner_outputs.size() != 2) LDB_COL_REJECT("inner output arity");

  std::vector<int> first_stage_ordinals(inner_outputs.size(), -1);
  const Field *count_arg_field = nullptr;
  bool saw_inner_count = false;
  for (size_t output_idx = 0; output_idx < inner_outputs.size();
       ++output_idx) {
    Item *real = inner_outputs[output_idx]->real_item();
    if (real->type() == Item::FIELD_ITEM &&
        down_cast<Item_field *>(real)->field == inner_group_field) {
      first_stage_ordinals[output_idx] = 0;
      continue;
    }

    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("inner output shape");
    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->sum_func() != Item_sum::COUNT_FUNC ||
        sum->argument_count() != 1) {
      LDB_COL_REJECT("inner aggregate not COUNT");
    }
    Item *arg = sum->get_arg(0)->real_item();
    if (arg->const_item()) LDB_COL_REJECT("inner COUNT(*) under LEFT");
    if (arg->type() != Item::FIELD_ITEM) LDB_COL_REJECT("inner COUNT arg");
    const Field *field = down_cast<Item_field *>(arg)->field;
    if (field->table != nullable_table || field->is_nullable()) {
      LDB_COL_REJECT("inner COUNT arg column");
    }
    count_arg_field = field;
    saw_inner_count = true;
    first_stage_ordinals[output_idx] = 1;
  }
  if (!saw_inner_count) LDB_COL_REJECT("inner COUNT missing");

  auto &request = ctx->query_block_request;
  request.Clear();
  request.add_tables()->set_table_name(preserved_table->s->normalized_path.str);
  request.add_tables()->set_table_name(nullable_table->s->normalized_path.str);

  auto *preserved_scan = request.add_nodes()->mutable_scan();
  preserved_scan->set_table_idx(0);
  auto *nullable_scan = request.add_nodes()->mutable_scan();
  nullable_scan->set_table_idx(1);
  if (!nullable_filters.empty() &&
      !SerializeTableFilters(nullable_filters, nullable_table,
                             nullable_scan->mutable_filter())) {
    LDB_COL_REJECT("inner ON filter not pushable");
  }

  auto *left_join = request.add_nodes()->mutable_join();
  left_join->set_type(LineairDB::Protocol::QueryBlockJoin::LEFT);
  left_join->set_build(1);
  left_join->set_probe(0);
  auto *build_key = left_join->add_build_keys();
  build_key->set_table_idx(1);
  build_key->set_column(nullable_key->field_index());
  auto *probe_key = left_join->add_probe_keys();
  probe_key->set_table_idx(0);
  probe_key->set_column(preserved_key->field_index());

  auto *aggregate = request.add_nodes()->mutable_aggregate();
  aggregate->set_input(2);
  auto *group_column = aggregate->add_group_columns();
  group_column->set_table_idx(0);
  group_column->set_column(inner_group_field->field_index());
  group_column->set_cmp_kind(
      inner_group_field->result_type() == STRING_RESULT ? 1 : 0);

  auto *function = aggregate->add_aggs();
  function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
  function->set_arg_table(1);
  auto *count_ref = function->mutable_arg();
  count_ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
  count_ref->set_column_index(count_arg_field->field_index());

  auto *regroup = aggregate->mutable_regroup();
  regroup->set_count_star(true);

  struct OuterGroup {
    const Field *field = nullptr;
    int regroup_slot = -1;
  };
  std::vector<OuterGroup> outer_groups;
  for (ORDER *group = qb->group_list.first; group != nullptr;
       group = group->next) {
    Item *group_item = (*group->item)->real_item();
    if (group_item->type() != Item::FIELD_ITEM)
      LDB_COL_REJECT("outer group shape");
    const Field *field = down_cast<Item_field *>(group_item)->field;
    if (field->table != derived_ref->table)
      LDB_COL_REJECT("outer group table");
    const uint32_t inner_output_idx = field->field_index();
    if (inner_output_idx >= first_stage_ordinals.size() ||
        first_stage_ordinals[inner_output_idx] < 0) {
      LDB_COL_REJECT("outer group mapping");
    }
    regroup->add_group_value_ordinals(
        static_cast<uint32_t>(first_stage_ordinals[inner_output_idx]));
    outer_groups.push_back(
        {field, regroup->group_value_ordinals_size() - 1});
  }

  std::vector<Item *> output_items;
  for (Item *item : VisibleFields(qb->fields)) output_items.push_back(item);
  for (Item *item : output_items) {
    Item *real = item->real_item();
    auto *output = request.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *field = down_cast<Item_field *>(real)->field;
      int regroup_slot = -1;
      for (const OuterGroup &group : outer_groups) {
        if (group.field == field) {
          regroup_slot = group.regroup_slot;
          break;
        }
      }
      if (regroup_slot < 0) LDB_COL_REJECT("outer output not grouped");
      output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::GROUP);
      output->set_ordinal(regroup_slot);
      continue;
    }

    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("outer output shape");
    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->sum_func() != Item_sum::COUNT_FUNC)
      LDB_COL_REJECT("outer aggregate not COUNT");
    if (sum->argument_count() > 0) {
      Item *arg = sum->get_arg(0)->real_item();
      if (!arg->const_item() || arg->is_nullable() || arg->is_null()) {
        LDB_COL_REJECT("outer aggregate not COUNT(*)");
      }
    }
    output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::AGG);
    output->set_ordinal(0);
  }

  for (ORDER *order = qb->order_list.first; order != nullptr;
       order = order->next) {
    const int output_ordinal = OrderOutputOrdinal(*order->item, output_items);
    if (output_ordinal < 0) LDB_COL_REJECT("outer ORDER BY");
    auto *sort_key = request.add_order_by();
    sort_key->set_output_ordinal(output_ordinal);
    sort_key->set_descending(order->direction == ORDER_DESC);
    Item *output_item = output_items[output_ordinal]->real_item();
    sort_key->set_cmp_kind(output_item->result_type() == STRING_RESULT ? 1 : 0);
  }

  Query_expression *outer_unit = qb->master_query_expression();
  if (qb->has_limit()) {
    // LIMIT without ORDER BY picks arbitrary rows: the row engine and this
    // executor (whose group-map iteration order is an implementation detail)
    // may legally return different rows, so reject loudly instead of
    // silently diverging.
    if (qb->order_list.first == nullptr &&
        outer_unit->select_limit_cnt != HA_POS_ERROR)
      LDB_COL_REJECT("LIMIT without ORDER BY");
    if (outer_unit->select_limit_cnt != HA_POS_ERROR)
      request.set_limit(outer_unit->select_limit_cnt);
    if (outer_unit->offset_limit_cnt > 0) {
      request.set_offset(outer_unit->offset_limit_cnt);
      if (request.limit() > 0)
        request.set_limit(request.limit() - outer_unit->offset_limit_cnt);
    }
  }

  ctx->query_block_ready = true;
  *why = nullptr;
  return true;

#undef LDB_COL_REJECT
}

/**
 * @brief Recognize an outer aggregate over a non-aggregating derived join.
 *
 * The outer block groups and aggregates columns exposed by a derived table.
 * This recognizer dereferences those derived columns back to the inner SELECT
 * expressions, then builds one server query block over the inner base tables.
 */
bool RecognizeFlattenedAggregate(JOIN *join, ColumnarExecutionContext *ctx,
                                 const char **why) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)

  Query_block *qb = join->query_block;
  Table_ref *derived_ref = qb->leaf_tables;
  if (derived_ref == nullptr || derived_ref->next_leaf != nullptr ||
      !derived_ref->is_view_or_derived() || derived_ref->table == nullptr) {
    LDB_COL_REJECT("not single derived table");
  }

  Query_expression *inner_unit = derived_ref->derived_query_expression();
  if (inner_unit == nullptr || !inner_unit->is_simple())
    LDB_COL_REJECT("derived not simple");
  Query_block *inner_qb = inner_unit->first_query_block();
  if (inner_qb == nullptr) LDB_COL_REJECT("no inner query block");

  if (qb->having_cond() != nullptr) LDB_COL_REJECT("outer has HAVING");
  if (qb->is_distinct()) LDB_COL_REJECT("outer has DISTINCT");
  if (qb->has_windows()) LDB_COL_REJECT("outer has windows");
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) LDB_COL_REJECT("outer has ROLLUP");
  if (qb->where_cond() != nullptr) LDB_COL_REJECT("outer has WHERE");
  if (qb->group_list.elements == 0) LDB_COL_REJECT("outer not grouped");

  if (inner_qb->having_cond() != nullptr) LDB_COL_REJECT("inner has HAVING");
  if (inner_qb->is_distinct()) LDB_COL_REJECT("inner has DISTINCT");
  if (inner_qb->has_windows()) LDB_COL_REJECT("inner has windows");
  if (inner_qb->olap != UNSPECIFIED_OLAP_TYPE)
    LDB_COL_REJECT("inner has ROLLUP");
  if (inner_qb->group_list.elements > 0 || inner_qb->with_sum_func)
    LDB_COL_REJECT("inner has aggregation");
  if (inner_qb->order_list.elements > 0 || inner_qb->has_limit())
    LDB_COL_REJECT("inner has order or limit");

  std::vector<QueryBlockTable> tables;
  for (Table_ref *table_ref = inner_qb->leaf_tables; table_ref != nullptr;
       table_ref = table_ref->next_leaf) {
    TABLE *table = table_ref->table;
    if (table == nullptr || table->s == nullptr)
      LDB_COL_REJECT("inner no TABLE");
    if (table_ref->outer_join) LDB_COL_REJECT("inner outer join");
    if (!loaded_tables->contains(table->s->db.str, table->s->table_name.str))
      LDB_COL_REJECT("inner not SECONDARY_LOADed");
    tables.push_back({table_ref, table, table_ref->map()});
  }
  if (tables.size() < 2) LDB_COL_REJECT("inner too few tables");

  std::vector<Item *> inner_outputs;
  for (Item *item : VisibleFields(inner_qb->fields)) {
    inner_outputs.push_back(item);
  }

  auto dereference_derived_item = [&](Item *item) -> Item * {
    Item *real = item->real_item();
    if (real->type() != Item::FIELD_ITEM) return real;
    const Field *field = down_cast<Item_field *>(real)->field;
    if (field == nullptr || field->table != derived_ref->table) return real;
    const uint32_t field_idx = field->field_index();
    if (field_idx >= inner_outputs.size()) return nullptr;
    return inner_outputs[field_idx]->real_item();
  };

  struct JoinEdge {
    int left_table = -1;
    int right_table = -1;
    const Field *left_field = nullptr;
    const Field *right_field = nullptr;
  };

  std::vector<std::vector<Item *>> table_filters(tables.size());
  std::vector<JoinEdge> join_edges;
  std::vector<Item *> predicates;
  FlattenAnd(inner_qb->where_cond(), &predicates);
  for (Item *predicate : predicates) {
    const table_map used = predicate->used_tables() & ~PSEUDO_TABLE_BITS;
    const int table_idx = SingleTableOf(used, tables);
    if (table_idx >= 0) {
      table_filters[table_idx].push_back(predicate);
      continue;
    }
    if (used == 0) {
      table_filters[0].push_back(predicate);
      continue;
    }

    if (predicate->type() != Item::FUNC_ITEM ||
        down_cast<Item_func *>(predicate)->functype() != Item_func::EQ_FUNC) {
      LDB_COL_REJECT("inner non-equi predicate");
    }
    auto *equals = down_cast<Item_func *>(predicate);
    Item *left = equals->arguments()[0]->real_item();
    Item *right = equals->arguments()[1]->real_item();
    if (left->type() != Item::FIELD_ITEM ||
        right->type() != Item::FIELD_ITEM) {
      LDB_COL_REJECT("inner join key shape");
    }

    const Field *left_raw = down_cast<Item_field *>(left)->field;
    const Field *right_raw = down_cast<Item_field *>(right)->field;
    const int left_table = TableIndexOfField(left_raw, tables);
    const int right_table = TableIndexOfField(right_raw, tables);
    if (left_table < 0 || right_table < 0 || left_table == right_table) {
      LDB_COL_REJECT("inner join key tables");
    }
    if (!JoinColumnsUseByteEquality(left_raw, right_raw)) {
      LDB_COL_REJECT("inner join key type");
    }

    const Field *left_field =
        ResolveBaseField(left_raw, tables[left_table].table);
    const Field *right_field =
        ResolveBaseField(right_raw, tables[right_table].table);
    if (left_field == nullptr || right_field == nullptr) {
      LDB_COL_REJECT("inner join key unresolvable");
    }
    if (left_field->is_nullable() || right_field->is_nullable()) {
      LDB_COL_REJECT("inner join key nullable");
    }
    join_edges.push_back(
        {left_table, right_table, left_field, right_field});
  }
  if (join_edges.empty()) LDB_COL_REJECT("inner cross join");

  auto &request = ctx->query_block_request;
  request.Clear();
  std::vector<int> scan_nodes(tables.size(), -1);
  for (size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
    TABLE *table = tables[table_idx].table;
    request.add_tables()->set_table_name(table->s->normalized_path.str);
    auto *scan = request.add_nodes()->mutable_scan();
    scan->set_table_idx(static_cast<uint32_t>(table_idx));
    scan_nodes[table_idx] = request.nodes_size() - 1;
    if (!table_filters[table_idx].empty() &&
        !SerializeTableFilters(table_filters[table_idx], table,
                               scan->mutable_filter())) {
      LDB_COL_REJECT("inner filter not pushable");
    }
  }

  int current_node = scan_nodes[0];
  std::vector<bool> joined(tables.size(), false);
  joined[0] = true;
  std::vector<int> pending_tables;
  for (size_t table_idx = 1; table_idx < tables.size(); table_idx++) {
    pending_tables.push_back(static_cast<int>(table_idx));
  }
  while (!pending_tables.empty()) {
    int table_idx = -1;
    size_t pending_idx = 0;
    for (size_t idx = 0; idx < pending_tables.size(); idx++) {
      for (const JoinEdge &edge : join_edges) {
        if ((edge.left_table == pending_tables[idx] &&
             joined[edge.right_table]) ||
            (edge.right_table == pending_tables[idx] &&
             joined[edge.left_table])) {
          table_idx = pending_tables[idx];
          pending_idx = idx;
          break;
        }
      }
      if (table_idx >= 0) break;
    }
    if (table_idx < 0) LDB_COL_REJECT("inner disconnected");
    pending_tables.erase(pending_tables.begin() + pending_idx);

    auto *join_node = request.add_nodes()->mutable_join();
    join_node->set_type(LineairDB::Protocol::QueryBlockJoin::INNER);
    join_node->set_build(scan_nodes[table_idx]);
    join_node->set_probe(current_node);
    bool connected = false;
    for (const JoinEdge &edge : join_edges) {
      const Field *build_field = nullptr;
      const Field *probe_field = nullptr;
      int probe_table = -1;
      if (edge.left_table == table_idx && joined[edge.right_table]) {
        build_field = edge.left_field;
        probe_field = edge.right_field;
        probe_table = edge.right_table;
      } else if (edge.right_table == table_idx &&
                 joined[edge.left_table]) {
        build_field = edge.right_field;
        probe_field = edge.left_field;
        probe_table = edge.left_table;
      } else {
        continue;
      }

      auto *build_key = join_node->add_build_keys();
      build_key->set_table_idx(static_cast<uint32_t>(table_idx));
      build_key->set_column(build_field->field_index());
      auto *probe_key = join_node->add_probe_keys();
      probe_key->set_table_idx(static_cast<uint32_t>(probe_table));
      probe_key->set_column(probe_field->field_index());
      connected = true;
    }
    if (!connected) LDB_COL_REJECT("inner disconnected");
    joined[table_idx] = true;
    current_node = request.nodes_size() - 1;
  }

  auto *aggregate = request.add_nodes()->mutable_aggregate();
  aggregate->set_input(current_node);
  std::vector<Item *> group_outer_items;
  for (ORDER *group = qb->group_list.first; group != nullptr;
       group = group->next) {
    Item *outer = (*group->item)->real_item();
    Item *inner = dereference_derived_item(outer);
    if (inner == nullptr) LDB_COL_REJECT("group dereference");

    auto *group_column = aggregate->add_group_columns();
    if (const Field *year_field = ExtractYearField(inner)) {
      const int table_idx = TableIndexOfField(year_field, tables);
      const Field *field =
          table_idx >= 0
              ? ResolveBaseField(year_field, tables[table_idx].table)
              : nullptr;
      if (field == nullptr || field->is_nullable()) {
        LDB_COL_REJECT("group year column");
      }
      group_column->set_table_idx(static_cast<uint32_t>(table_idx));
      group_column->set_column(field->field_index());
      group_column->set_prefix_len(4);
      group_column->set_cmp_kind(0);
    } else if (inner->type() == Item::FIELD_ITEM) {
      const Field *raw_field = down_cast<Item_field *>(inner)->field;
      const int table_idx = TableIndexOfField(raw_field, tables);
      const Field *field =
          table_idx >= 0
              ? ResolveBaseField(raw_field, tables[table_idx].table)
              : nullptr;
      if (field == nullptr || field->is_nullable() ||
          !GroupColumnUsesStoredByteKey(field)) {
        LDB_COL_REJECT("group column cannot use stored-byte key");
      }
      group_column->set_table_idx(static_cast<uint32_t>(table_idx));
      group_column->set_column(field->field_index());
      group_column->set_cmp_kind(field->result_type() == STRING_RESULT ? 1
                                                                       : 0);
    } else {
      LDB_COL_REJECT("group expression");
    }
    group_outer_items.push_back(outer);
  }

  std::vector<Item *> output_items;
  for (Item *item : VisibleFields(qb->fields)) output_items.push_back(item);
  for (Item *item : output_items) {
    Item *real = item->real_item();
    auto *output = request.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      int group_position = -1;
      for (size_t group_idx = 0; group_idx < group_outer_items.size();
           group_idx++) {
        Item *group_item = group_outer_items[group_idx];
        if (group_item == real ||
            (group_item->type() == Item::FIELD_ITEM &&
             down_cast<Item_field *>(group_item)->field ==
                 down_cast<Item_field *>(real)->field)) {
          group_position = static_cast<int>(group_idx);
          break;
        }
      }
      if (group_position < 0) LDB_COL_REJECT("output not grouped");
      output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::GROUP);
      output->set_ordinal(group_position);
      continue;
    }

    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("output shape");
    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->sum_func() != Item_sum::SUM_FUNC ||
        sum->argument_count() != 1) {
      LDB_COL_REJECT("aggregate kind");
    }
    Item *arg = dereference_derived_item(sum->get_arg(0));
    if (arg == nullptr) LDB_COL_REJECT("aggregate dereference");
    if (arg->result_type() != DECIMAL_RESULT &&
        arg->result_type() != INT_RESULT) {
      LDB_COL_REJECT("aggregate arg type");
    }

    auto *function = aggregate->add_aggs();
    function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::SUM);
    function->set_arg_table(0);
    function->set_arg_scale(arg->decimals);
    if (!SerializeAggregateExpressionForTables(arg, tables, 0,
                                               function->mutable_arg())) {
      LDB_COL_REJECT("aggregate expr not pushable");
    }
    output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::AGG);
    output->set_ordinal(aggregate->aggs_size() - 1);
  }
  if (aggregate->aggs_size() == 0) LDB_COL_REJECT("no aggregates");

  for (ORDER *order = qb->order_list.first; order != nullptr;
       order = order->next) {
    const int output_ordinal = OrderOutputOrdinal(*order->item, output_items);
    if (output_ordinal < 0) LDB_COL_REJECT("outer ORDER BY");
    auto *sort_key = request.add_order_by();
    sort_key->set_output_ordinal(output_ordinal);
    sort_key->set_descending(order->direction == ORDER_DESC);
    Item *output_item = output_items[output_ordinal]->real_item();
    sort_key->set_cmp_kind(output_item->result_type() == STRING_RESULT ? 1 : 0);
  }

  Query_expression *outer_unit = qb->master_query_expression();
  if (qb->has_limit()) {
    // LIMIT without ORDER BY picks arbitrary rows: the row engine and this
    // executor (whose group-map iteration order is an implementation detail)
    // may legally return different rows, so reject loudly instead of
    // silently diverging.
    if (qb->order_list.first == nullptr &&
        outer_unit->select_limit_cnt != HA_POS_ERROR)
      LDB_COL_REJECT("LIMIT without ORDER BY");
    if (outer_unit->select_limit_cnt != HA_POS_ERROR)
      request.set_limit(outer_unit->select_limit_cnt);
    if (outer_unit->offset_limit_cnt > 0) {
      request.set_offset(outer_unit->offset_limit_cnt);
      if (request.limit() > 0)
        request.set_limit(request.limit() - outer_unit->offset_limit_cnt);
    }
  }

  ctx->query_block_ready = true;
  *why = nullptr;
  return true;

#undef LDB_COL_REJECT
}

/**
 * @brief Build a query-block request for aggregate shapes.
 *
 * Unsupported shapes return false and set `why`; callers decide whether the
 * primary engine may run instead.
 */
bool BuildQueryBlockRequest(
    JOIN *join, Query_block *qb,
    LineairDB::Protocol::TxExecuteQueryBlock::Request *request_out,
    const char **why, AccessPath *plan = nullptr) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)

  if (qb == nullptr || request_out == nullptr) LDB_COL_REJECT("missing block");
  auto &request = *request_out;
  request.Clear();

  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr || !unit->is_simple())
    LDB_COL_REJECT("not simple unit");

  if (qb->has_windows()) LDB_COL_REJECT("has windows");
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) LDB_COL_REJECT("has ROLLUP");
  const bool implicit_grouping =
      join != nullptr ? join->implicit_grouping : qb->is_implicitly_grouped();
  const bool distinct_as_group =
      qb->is_distinct() && !implicit_grouping && qb->group_list.elements == 0 &&
      !qb->with_sum_func;
  if (qb->is_distinct() && !distinct_as_group)
    LDB_COL_REJECT("has DISTINCT");
  const bool plain_rows =
      !distinct_as_group && !implicit_grouping && qb->group_list.elements == 0;

  struct SemijoinNest {
    Table_ref *nest = nullptr;
    bool anti = false;
    std::vector<int> inner_tables;
    std::vector<Item *> residual_predicates;
  };

  std::vector<SemijoinNest> semijoin_nests;
  for (Table_ref *nest : qb->sj_nests) {
    semijoin_nests.push_back(
        {nest, nest->is_aj_nest(), {}, {}});
  }

  auto semijoin_nest_of = [&](Table_ref *table_ref) -> int {
    for (Table_ref *embedding = table_ref->embedding; embedding != nullptr;
         embedding = embedding->embedding) {
      for (size_t nest_idx = 0; nest_idx < semijoin_nests.size();
           nest_idx++) {
        if (embedding == semijoin_nests[nest_idx].nest) {
          return static_cast<int>(nest_idx);
        }
      }

      if (embedding->is_sj_nest() || embedding->is_aj_nest()) {
        semijoin_nests.push_back(
            {embedding, embedding->is_aj_nest(), {}, {}});
        return static_cast<int>(semijoin_nests.size()) - 1;
      }
    }
    return -1;
  };

  // The request table order is the stable numbering used by scan, join,
  // grouping, and aggregate argument references. Real loaded tables come
  // first; derived tables follow them and become server-side virtual tables.
  std::vector<QueryBlockTable> tables;
  std::vector<int> main_tables;
  std::vector<int> table_semijoin_nest;
  std::vector<bool> table_is_virtual;
  std::vector<Query_block *> virtual_blocks;
  std::vector<bool> virtual_block_is_scalar_aggregate;
  std::vector<int> outer_derived_tables;
  for (Table_ref *table_ref = qb->leaf_tables; table_ref != nullptr;
       table_ref = table_ref->next_leaf) {
    if (table_ref->is_view_or_derived()) continue;
    TABLE *table = table_ref->table;
    if (table == nullptr || table->s == nullptr) LDB_COL_REJECT("no TABLE");
    const int semijoin_nest = semijoin_nest_of(table_ref);
    if (semijoin_nest < 0 && table_ref->outer_join)
      LDB_COL_REJECT("outer join");
    if (!loaded_tables->contains(table->s->db.str, table->s->table_name.str))
      LDB_COL_REJECT("not SECONDARY_LOADed");
    tables.push_back({table_ref, table, table_ref->map()});
    table_semijoin_nest.push_back(semijoin_nest);
    table_is_virtual.push_back(false);
    if (semijoin_nest < 0) {
      main_tables.push_back(static_cast<int>(tables.size()) - 1);
    } else {
      semijoin_nests[semijoin_nest].inner_tables.push_back(
          static_cast<int>(tables.size()) - 1);
    }
  }

  const size_t real_table_count = tables.size();
  for (Table_ref *table_ref = qb->leaf_tables; table_ref != nullptr;
       table_ref = table_ref->next_leaf) {
    if (!table_ref->is_view_or_derived()) continue;
    const int semijoin_nest = semijoin_nest_of(table_ref);
    if (table_ref->table == nullptr) LDB_COL_REJECT("derived no TABLE");

    Query_expression *derived_unit = table_ref->derived_query_expression();
    if (derived_unit == nullptr || !derived_unit->is_simple())
      LDB_COL_REJECT("derived not simple");
    Query_block *derived_qb = derived_unit->first_query_block();
    if (derived_qb == nullptr) LDB_COL_REJECT("derived no query block");

    tables.push_back({table_ref, table_ref->table, table_ref->map()});
    table_semijoin_nest.push_back(semijoin_nest);
    table_is_virtual.push_back(true);
    virtual_blocks.push_back(derived_qb);
    virtual_block_is_scalar_aggregate.push_back(
        derived_qb->is_implicitly_grouped() &&
        derived_qb->group_list.elements == 0);
    if (semijoin_nest >= 0) {
      semijoin_nests[semijoin_nest].inner_tables.push_back(
          static_cast<int>(tables.size()) - 1);
    } else if (table_ref->outer_join) {
      outer_derived_tables.push_back(static_cast<int>(tables.size()) - 1);
    } else {
      main_tables.push_back(static_cast<int>(tables.size()) - 1);
    }
  }
  if (main_tables.empty()) LDB_COL_REJECT("no tables");

  auto resolve_query_field = [&](const Field *field,
                                 int table_idx) -> const Field * {
    if (table_idx < 0 ||
        table_idx >= static_cast<int>(table_is_virtual.size())) {
      return nullptr;
    }
    if (table_is_virtual[table_idx]) return field;
    return ResolveBaseField(field, tables[table_idx].table);
  };

  auto is_scalar_virtual_table = [&](int table_idx) {
    if (table_idx < static_cast<int>(real_table_count) ||
        table_idx >= static_cast<int>(table_is_virtual.size()) ||
        !table_is_virtual[table_idx]) {
      return false;
    }
    const size_t virtual_idx =
        static_cast<size_t>(table_idx) - real_table_count;
    return virtual_idx < virtual_block_is_scalar_aggregate.size() &&
           virtual_block_is_scalar_aggregate[virtual_idx];
  };

  const bool plan_map = plan != nullptr;

  for (size_t table_idx = 0; table_idx < real_table_count; table_idx++) {
    request.add_tables()->set_table_name(
        tables[table_idx].table->s->normalized_path.str);
  }

  struct JoinEdge {
    int left_table = -1;
    int right_table = -1;
    const Field *left_field = nullptr;
    const Field *right_field = nullptr;
  };

  auto semijoin_outer_equivalent = [&](const Field *field) -> const Field * {
    for (const SemijoinNest &semijoin : semijoin_nests) {
      if (semijoin.nest == nullptr ||
          semijoin.nest->nested_join == nullptr) {
        continue;
      }
      const auto &outer_exprs =
          semijoin.nest->nested_join->sj_outer_exprs;
      const auto &inner_exprs =
          semijoin.nest->nested_join->sj_inner_exprs;
      for (size_t expr_idx = 0;
           expr_idx < outer_exprs.size() && expr_idx < inner_exprs.size();
           expr_idx++) {
        Item *outer_item = outer_exprs[expr_idx]->real_item();
        Item *inner_item = inner_exprs[expr_idx]->real_item();
        if (outer_item->type() == Item::FIELD_ITEM &&
            inner_item->type() == Item::FIELD_ITEM &&
            down_cast<Item_field *>(inner_item)->field == field) {
          return down_cast<Item_field *>(outer_item)->field;
        }
      }
    }
    return field;
  };

  std::vector<std::vector<Item *>> table_filters(tables.size());
  std::vector<JoinEdge> join_edges;
  auto add_join_edge_if_supported = [&](const Field *left_raw,
                                        const Field *right_raw) -> bool {
    const int left_table = TableIndexOfField(left_raw, tables);
    const int right_table = TableIndexOfField(right_raw, tables);
    if (left_table < 0 || right_table < 0 || left_table == right_table) {
      return false;
    }
    if (table_semijoin_nest[left_table] >= 0 ||
        table_semijoin_nest[right_table] >= 0) {
      return false;
    }
    if (!JoinColumnsUseByteEquality(left_raw, right_raw)) {
      return false;
    }

    const Field *left_field = resolve_query_field(left_raw, left_table);
    const Field *right_field = resolve_query_field(right_raw, right_table);
    if (left_field == nullptr || right_field == nullptr) return false;
    join_edges.push_back({left_table, right_table, left_field, right_field});
    return true;
  };

  std::vector<Item *> tuple_predicates;
  Item *where_cond =
      qb->where_cond() != nullptr
          ? qb->where_cond()
          : (join != nullptr ? join->where_cond : nullptr);
  std::vector<Item *> predicates;
  FlattenAnd(where_cond, &predicates);

  // Local predicates become scan filters. Cross-table integer equalities become
  // join edges. Other cross-table predicates are evaluated after the join as
  // tuple filters.
  for (Item *predicate : predicates) {
    const table_map used = predicate->used_tables() & ~PSEUDO_TABLE_BITS;
    const int table_idx = SingleTableOf(used, tables);
    if (table_idx >= 0) {
      if (plan_map && table_is_virtual[table_idx]) continue;
      if (table_is_virtual[table_idx]) {
        tuple_predicates.push_back(predicate);
        continue;
      }
      table_filters[table_idx].push_back(predicate);
      continue;
    }
    if (used == 0) {
      auto real_filter_table =
          std::find_if(main_tables.begin(), main_tables.end(),
                       [&](int candidate) {
                         return !table_is_virtual[candidate];
                       });
      if (real_filter_table != main_tables.end()) {
        table_filters[*real_filter_table].push_back(predicate);
      } else {
        tuple_predicates.push_back(predicate);
      }
      continue;
    }

    // Plan mapping takes cross-table joins, semijoin residuals, and derived
    // filters from the optimizer's AccessPath tree instead of the syntactic
    // WHERE decomposition.
    if (plan_map) continue;

    int predicate_nest = -1;
    bool spans_multiple_nests = false;
    for (size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
      if ((used & tables[table_idx].map) == 0) continue;
      const int table_nest = table_semijoin_nest[table_idx];
      if (table_nest < 0) continue;
      if (predicate_nest >= 0 && predicate_nest != table_nest) {
        spans_multiple_nests = true;
      }
      predicate_nest = table_nest;
    }
    if (spans_multiple_nests) LDB_COL_REJECT("predicate spans semijoins");
    if (predicate->type() == Item::FUNC_ITEM &&
        down_cast<Item_func *>(predicate)->functype() ==
            Item_func::MULT_EQUAL_FUNC) {
      auto *multiple_equal = down_cast<Item_equal *>(predicate);
      if (multiple_equal->const_arg() != nullptr) {
        LDB_COL_REJECT("multiple equality with constant");
      }
      std::vector<const Field *> equal_fields;
      for (Item_field &field_item : multiple_equal->get_fields()) {
        equal_fields.push_back(field_item.field);
      }
      for (size_t left_idx = 0; left_idx < equal_fields.size(); left_idx++) {
        for (size_t right_idx = left_idx + 1; right_idx < equal_fields.size();
             right_idx++) {
          const int left_table =
              TableIndexOfField(equal_fields[left_idx], tables);
          const int right_table =
              TableIndexOfField(equal_fields[right_idx], tables);
          // Fields that do not resolve to this block's tables and pairs that
          // cross a semijoin nest boundary are enforced by the semijoin key
          // machinery, not by join edges.
          if (left_table < 0 || right_table < 0) continue;
          if (table_semijoin_nest[left_table] !=
              table_semijoin_nest[right_table]) {
            continue;
          }
          // A dropped same-side pair would silently lose the equality
          // between those two columns; the join would then emit rows the
          // predicate excludes.
          if (!add_join_edge_if_supported(equal_fields[left_idx],
                                          equal_fields[right_idx])) {
            LDB_COL_REJECT("multiple equality not pushable");
          }
        }
      }
      continue;
    }
    if (predicate_nest >= 0) {
      if (predicate->type() == Item::FUNC_ITEM &&
          down_cast<Item_func *>(predicate)->functype() ==
              Item_func::EQ_FUNC) {
        auto *equals = down_cast<Item_func *>(predicate);
        Item *left = equals->arguments()[0]->real_item();
        Item *right = equals->arguments()[1]->real_item();
        if (left->type() == Item::FIELD_ITEM &&
            right->type() == Item::FIELD_ITEM) {
          const Field *left_raw = semijoin_outer_equivalent(
              down_cast<Item_field *>(left)->field);
          const Field *right_raw = semijoin_outer_equivalent(
              down_cast<Item_field *>(right)->field);
          const int left_table = TableIndexOfField(left_raw, tables);
          const int right_table = TableIndexOfField(right_raw, tables);
          if (left_table >= 0 && right_table >= 0 &&
              left_table == right_table) {
            continue;
          }
          if (add_join_edge_if_supported(left_raw, right_raw)) continue;
        }
      }
      semijoin_nests[predicate_nest].residual_predicates.push_back(predicate);
      continue;
    }

    if (predicate->type() == Item::COND_ITEM &&
        down_cast<Item_cond *>(predicate)->functype() ==
            Item_func::COND_OR_FUNC) {
      auto *condition = down_cast<Item_cond *>(predicate);
      std::vector<std::vector<std::pair<const Field *, const Field *>>>
          branch_equalities;
      List_iterator<Item> branch_it(*condition->argument_list());
      for (Item *branch = branch_it++; branch != nullptr;
           branch = branch_it++) {
        std::vector<Item *> branch_parts;
        FlattenAnd(branch, &branch_parts);
        branch_equalities.emplace_back();
        for (Item *part : branch_parts) {
          if (part->type() != Item::FUNC_ITEM ||
              down_cast<Item_func *>(part)->functype() !=
                  Item_func::EQ_FUNC) {
            continue;
          }
          auto *equals = down_cast<Item_func *>(part);
          Item *left = equals->arguments()[0]->real_item();
          Item *right = equals->arguments()[1]->real_item();
          if (left->type() != Item::FIELD_ITEM ||
              right->type() != Item::FIELD_ITEM) {
            continue;
          }
          branch_equalities.back().push_back(
              {down_cast<Item_field *>(left)->field,
               down_cast<Item_field *>(right)->field});
        }
      }

      if (!branch_equalities.empty()) {
        for (const auto &candidate : branch_equalities[0]) {
          bool appears_in_all_branches = true;
          for (size_t branch_idx = 1;
               branch_idx < branch_equalities.size() &&
               appears_in_all_branches;
               branch_idx++) {
            bool found = false;
            for (const auto &other : branch_equalities[branch_idx]) {
              if ((other.first == candidate.first &&
                   other.second == candidate.second) ||
                  (other.first == candidate.second &&
                   other.second == candidate.first)) {
                found = true;
                break;
              }
            }
            appears_in_all_branches = found;
          }
          if (!appears_in_all_branches) continue;

          const int left_table = TableIndexOfField(candidate.first, tables);
          const int right_table = TableIndexOfField(candidate.second, tables);
          if (left_table < 0 || right_table < 0 ||
              left_table == right_table) {
            continue;
          }
          if (!JoinColumnsUseByteEquality(candidate.first, candidate.second)) {
            continue;
          }

          const Field *left_field =
              resolve_query_field(candidate.first, left_table);
          const Field *right_field =
              resolve_query_field(candidate.second, right_table);
          if (left_field == nullptr || right_field == nullptr) continue;
          join_edges.push_back(
              {left_table, right_table, left_field, right_field});
        }
      }

      tuple_predicates.push_back(predicate);
      continue;
    }

    if (predicate->type() != Item::FUNC_ITEM ||
        down_cast<Item_func *>(predicate)->functype() != Item_func::EQ_FUNC) {
      tuple_predicates.push_back(predicate);
      continue;
    }
    auto *equals = down_cast<Item_func *>(predicate);
    Item *left = equals->arguments()[0]->real_item();
    Item *right = equals->arguments()[1]->real_item();
    if (left->type() != Item::FIELD_ITEM ||
        right->type() != Item::FIELD_ITEM) {
      LDB_COL_REJECT("join key not a column");
    }

    const Field *left_raw = down_cast<Item_field *>(left)->field;
    const Field *right_raw = down_cast<Item_field *>(right)->field;
    const int left_table = TableIndexOfField(left_raw, tables);
    const int right_table = TableIndexOfField(right_raw, tables);
    if (left_table < 0 || right_table < 0 || left_table == right_table) {
      LDB_COL_REJECT("join key tables");
    }
    if (!JoinColumnsUseByteEquality(left_raw, right_raw)) {
      LDB_COL_REJECT("join key type");
    }

    const Field *left_field = resolve_query_field(left_raw, left_table);
    const Field *right_field = resolve_query_field(right_raw, right_table);
    if (left_field == nullptr || right_field == nullptr) {
      LDB_COL_REJECT("join key unresolvable");
    }
    join_edges.push_back({left_table, right_table, left_field, right_field});
  }
  size_t main_non_scalar_tables = 0;
  for (int table_idx : main_tables) {
    if (!is_scalar_virtual_table(table_idx)) main_non_scalar_tables++;
  }
  if (main_tables.size() > 1 && join_edges.empty() &&
      main_non_scalar_tables > 1 && !plan_map) {
    LDB_COL_REJECT("cross join");
  }

  std::vector<int> scan_nodes(tables.size(), -1);
  auto emit_scan = [&](size_t table_idx) -> bool {
    if (table_idx >= real_table_count) return false;
    if (scan_nodes[table_idx] >= 0) return true;

    TABLE *table = tables[table_idx].table;
    auto *scan_node = request.add_nodes();
    auto *scan = scan_node->mutable_scan();
    scan->set_table_idx(static_cast<uint32_t>(table_idx));
    scan_nodes[table_idx] = request.nodes_size() - 1;
    if (!table_filters[table_idx].empty() &&
        !SerializeTableFilters(table_filters[table_idx], table,
                               scan->mutable_filter())) {
      return false;
    }
    return true;
  };

  int current_node = -1;
  if (plan_map) {
    std::map<int, std::set<int>> node_tables;
    std::map<int, double> node_rows;
    size_t next_virtual_table = real_table_count;

    // Missing optimizer row estimates are treated as huge so they cannot pass
    // small-side guards or win semi-filter source selection.
    constexpr double kUnknownRowEstimate = 1e30;
    // Keyless INNER joins are cross products. Allow them only when one side is
    // clearly small; larger shapes stay rejected.
    constexpr double kTinyKeylessInnerJoinRows = 100.0;

    struct MappedJoinKey {
      LineairDB::Protocol::QueryBlockJoin::Type join_type;
      int build_node = -1;
      int probe_node = -1;
      uint32_t build_table = 0;
      uint32_t build_column = 0;
      uint32_t probe_table = 0;
      uint32_t probe_column = 0;
    };
    std::vector<MappedJoinKey> mapped_join_keys;

    auto estimated_rows = [](const AccessPath *path) {
      return std::max(1.0, path == nullptr ? 1.0 : path->num_output_rows());
    };

    auto table_rows = [&](size_t table_idx) {
      if (table_idx >= tables.size() || tables[table_idx].table == nullptr ||
          tables[table_idx].table->file == nullptr) {
        return 1.0;
      }
      return std::max(
          1.0,
          static_cast<double>(tables[table_idx].table->file->stats.records));
    };

    std::set<const Item *> having_predicates;
    if (qb->having_cond() != nullptr) {
      std::vector<Item *> having_parts;
      FlattenAnd(qb->having_cond(), &having_parts);
      having_predicates.insert(having_parts.begin(), having_parts.end());
    }

    TupleColumnRegistry registry_template;
    registry_template.table_of = [&](const Field *field) {
      return TableIndexOfField(field, tables);
    };
    registry_template.resolve = [&](const Field *field, int table_idx) {
      return resolve_query_field(field, table_idx);
    };

    auto fill_tuple_filter =
        [&](const std::vector<Item *> &items,
            LineairDB::Protocol::QueryBlockTupleFilter *tuple_filter) -> bool {
      TupleColumnRegistry registry = registry_template;
      auto *predicate = tuple_filter->mutable_predicate();
      auto *expression = predicate->mutable_expr();
      if (items.size() == 1) {
        if (!SerializeTuplePredicate(items[0], expression, &registry))
          return false;
      } else {
        expression->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *item : items) {
          if (!SerializeTuplePredicate(item, expression->add_children(),
                                       &registry)) {
            return false;
          }
        }
      }

      predicate->set_num_columns(registry.columns.size());
      for (const auto &column_ref : registry.columns) {
        auto *column = tuple_filter->add_columns();
        column->set_table_idx(static_cast<uint32_t>(column_ref.first));
        column->set_column(column_ref.second->field_index());
      }
      return true;
    };

    // Emit real-table scans before mapping joins, ordered by current table
    // statistics. Later scan-level semi-filters can only read key sets from
    // nodes that execute earlier, so selective scans must be available as
    // sources regardless of where the optimizer placed their joins.
    {
      std::vector<size_t> scan_issue_order;
      scan_issue_order.reserve(real_table_count);
      for (size_t table_idx = 0; table_idx < real_table_count; table_idx++) {
        scan_issue_order.push_back(table_idx);
      }
      std::stable_sort(scan_issue_order.begin(), scan_issue_order.end(),
                       [&](size_t left, size_t right) {
                         return table_rows(left) < table_rows(right);
                       });
      for (size_t table_idx : scan_issue_order) {
        if (!emit_scan(table_idx)) LDB_COL_REJECT("filter not pushable");
        const int scan_node = scan_nodes[table_idx];
        node_tables[scan_node] = {static_cast<int>(table_idx)};
        node_rows[scan_node] = table_rows(table_idx);
      }
    }

    // Join keys emitted below are enforced equalities. Later mapped joins may
    // need the same column through a surviving equivalent column if the
    // optimizer removed an intermediate table from the mapped tree.
    using ColRef = std::pair<int, uint32_t>;
    std::vector<std::pair<ColRef, ColRef>> enforced_equalities;

    auto equivalent_columns = [&](ColRef start) {
      std::set<ColRef> columns{start};
      bool grew = true;
      while (grew) {
        grew = false;
        for (const auto &edge : enforced_equalities) {
          if (columns.count(edge.first) != 0 &&
              columns.count(edge.second) == 0) {
            columns.insert(edge.second);
            grew = true;
          }
          if (columns.count(edge.second) != 0 &&
              columns.count(edge.first) == 0) {
            columns.insert(edge.first);
            grew = true;
          }
        }
      }
      return columns;
    };

    auto emit_mapped_join =
        [&](LineairDB::Protocol::QueryBlockJoin::Type join_type, int probe,
            int build, RelationalExpression *expression,
            const std::vector<Item *> &extra_residuals,
            double output_rows) -> int {
      if (expression == nullptr) {
        *why = "plan join without predicate";
        return -1;
      }

      auto *join_node = request.add_nodes()->mutable_join();
      const int self = request.nodes_size() - 1;
      join_node->set_type(join_type);
      join_node->set_build(static_cast<uint32_t>(build));
      join_node->set_probe(static_cast<uint32_t>(probe));

      for (Item_eq_base *eq : expression->equijoin_conditions) {
        if (eq->functype() != Item_func::EQ_FUNC) {
          *why = "plan join key null-safe eq";
          return -1;
        }

        Item *left_item = eq->get_arg(0)->real_item();
        Item *right_item = eq->get_arg(1)->real_item();
        if (left_item->type() != Item::FIELD_ITEM ||
            right_item->type() != Item::FIELD_ITEM) {
          *why = "plan join key shape";
          return -1;
        }

        const Field *left_raw = down_cast<Item_field *>(left_item)->field;
        const Field *right_raw = down_cast<Item_field *>(right_item)->field;
        int left_table = TableIndexOfField(left_raw, tables);
        int right_table = TableIndexOfField(right_raw, tables);
        if (left_table < 0 || right_table < 0) {
          *why = "plan join key table";
          return -1;
        }
        if (!JoinColumnsUseByteEquality(left_raw, right_raw)) {
          *why = "plan join key type";
          return -1;
        }

        const Field *left_field = resolve_query_field(left_raw, left_table);
        const Field *right_field =
            resolve_query_field(right_raw, right_table);
        if (left_field == nullptr || right_field == nullptr) {
          *why = "plan join key resolve";
          return -1;
        }
        ColRef build_ref{-1, 0};
        ColRef probe_ref{-1, 0};
        if (node_tables[build].count(left_table) > 0 &&
            node_tables[probe].count(right_table) > 0) {
          build_ref = {left_table, left_field->field_index()};
          probe_ref = {right_table, right_field->field_index()};
        } else if (node_tables[build].count(right_table) > 0 &&
                   node_tables[probe].count(left_table) > 0) {
          build_ref = {right_table, right_field->field_index()};
          probe_ref = {left_table, left_field->field_index()};
        } else {
          // Rewrite through equivalence only after both direct side matches
          // fail, so already-resolved joins keep their original columns.
          auto pick_from_side = [](const std::set<ColRef> &columns,
                                   const std::set<int> &side_tables) {
            for (const ColRef &column : columns) {
              if (side_tables.count(column.first) != 0) return column;
            }
            return ColRef{-1, 0};
          };

          const std::set<ColRef> left_equivalents =
              equivalent_columns({left_table, left_field->field_index()});
          const std::set<ColRef> right_equivalents =
              equivalent_columns({right_table, right_field->field_index()});
          build_ref = pick_from_side(left_equivalents, node_tables[build]);
          probe_ref = pick_from_side(right_equivalents, node_tables[probe]);
          if (build_ref.first < 0 || probe_ref.first < 0) {
            build_ref = pick_from_side(right_equivalents, node_tables[build]);
            probe_ref = pick_from_side(left_equivalents, node_tables[probe]);
          }
        }
        if (build_ref.first < 0 || probe_ref.first < 0) {
          *why = "plan join key sides";
          return -1;
        }

        auto *build_key = join_node->add_build_keys();
        build_key->set_table_idx(static_cast<uint32_t>(build_ref.first));
        build_key->set_column(build_ref.second);
        auto *probe_key = join_node->add_probe_keys();
        probe_key->set_table_idx(static_cast<uint32_t>(probe_ref.first));
        probe_key->set_column(probe_ref.second);
        mapped_join_keys.push_back(
            {join_type, build, probe, static_cast<uint32_t>(build_ref.first),
             build_ref.second, static_cast<uint32_t>(probe_ref.first),
             probe_ref.second});
      }

      if (join_type == LineairDB::Protocol::QueryBlockJoin::INNER ||
          join_type == LineairDB::Protocol::QueryBlockJoin::SEMI) {
        for (int key_idx = 0; key_idx < join_node->build_keys_size();
             key_idx++) {
          enforced_equalities.push_back(
              {{static_cast<int>(join_node->build_keys(key_idx).table_idx()),
                join_node->build_keys(key_idx).column()},
               {static_cast<int>(join_node->probe_keys(key_idx).table_idx()),
                join_node->probe_keys(key_idx).column()}});
        }
      }

      std::vector<Item *> residuals;
      for (Item *condition : expression->join_conditions) {
        residuals.push_back(condition);
      }
      residuals.insert(residuals.end(), extra_residuals.begin(),
                       extra_residuals.end());
      if (!residuals.empty()) {
        for (Item *residual : residuals) {
          const table_map used = residual->used_tables() & ~PSEUDO_TABLE_BITS;
          for (size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
            if ((used & tables[table_idx].map) != 0 &&
                node_tables[probe].count(static_cast<int>(table_idx)) == 0 &&
                node_tables[build].count(static_cast<int>(table_idx)) == 0) {
              *why = "plan join residual out of scope";
              return -1;
            }
          }
        }
        if (!fill_tuple_filter(residuals, join_node->mutable_residual())) {
          *why = "plan join residual not pushable";
          return -1;
        }
      }

      if (join_node->build_keys_size() == 0) {
        bool build_is_one_row = true;
        for (int table_idx : node_tables[build]) {
          if (!is_scalar_virtual_table(table_idx)) {
            build_is_one_row = false;
            break;
          }
        }
        const double probe_rows =
            node_rows.count(probe) != 0 ? node_rows[probe]
                                        : kUnknownRowEstimate;
        const double build_rows =
            node_rows.count(build) != 0 ? node_rows[build]
                                        : kUnknownRowEstimate;
        const bool tiny_inner_join =
            join_type == LineairDB::Protocol::QueryBlockJoin::INNER &&
            std::min(probe_rows, build_rows) <= kTinyKeylessInnerJoinRows;
        if (!build_is_one_row && !tiny_inner_join) {
          *why = "plan keyless join";
          return -1;
        }
      }

      std::set<int> output_tables = node_tables[probe];
      if (join_type == LineairDB::Protocol::QueryBlockJoin::INNER ||
          join_type == LineairDB::Protocol::QueryBlockJoin::LEFT) {
        output_tables.insert(node_tables[build].begin(),
                             node_tables[build].end());
      }
      node_tables[self] = std::move(output_tables);
      node_rows[self] = output_rows;
      return self;
    };

    std::function<int(AccessPath *, bool)> map_path =
        [&](AccessPath *path, bool root) -> int {
      if (path == nullptr) {
        *why = "null plan node";
        return -1;
      }

      switch (path->type) {
        case AccessPath::TABLE_SCAN: {
          const TABLE *table = path->table_scan().table;
          int table_idx = -1;
          for (size_t i = 0; i < tables.size(); i++) {
            if (tables[i].table == table) {
              table_idx = static_cast<int>(i);
              break;
            }
          }
          if (table_idx < 0) {
            *why = "plan table unknown";
            return -1;
          }
          if (static_cast<size_t>(table_idx) >= real_table_count) {
            *why = "plan table scan virtual";
            return -1;
          }
          if (!emit_scan(static_cast<size_t>(table_idx))) {
            *why = "filter not pushable";
            return -1;
          }
          const int self = scan_nodes[table_idx];
          node_tables[self] = {table_idx};
          node_rows[self] = estimated_rows(path);
          return self;
        }

        case AccessPath::FILTER: {
          const int input = map_path(path->filter().child, root);
          if (input < 0) return -1;

          std::vector<Item *> parts;
          FlattenAnd(path->filter().condition, &parts);
          std::vector<Item *> residuals;
          for (Item *part : parts) {
            if (having_predicates.count(part) != 0) continue;

            const table_map used = part->used_tables() & ~PSEUDO_TABLE_BITS;
            const int table_idx = SingleTableOf(used, tables);
            if (table_idx >= 0 && !table_is_virtual[table_idx] &&
                std::find(table_filters[table_idx].begin(),
                          table_filters[table_idx].end(),
                          part) != table_filters[table_idx].end()) {
              continue;
            }
            residuals.push_back(part);
          }
          if (residuals.empty()) {
            auto it = node_rows.find(input);
            node_rows[input] = it == node_rows.end()
                                   ? estimated_rows(path)
                                   : std::min(it->second,
                                              estimated_rows(path));
            return input;
          }

          for (Item *residual : residuals) {
            const table_map used =
                residual->used_tables() & ~PSEUDO_TABLE_BITS;
            for (size_t table_idx = 0; table_idx < tables.size();
                 table_idx++) {
              if ((used & tables[table_idx].map) != 0 &&
                  node_tables[input].count(static_cast<int>(table_idx)) == 0) {
                *why = "plan filter out of scope";
                return -1;
              }
            }
          }

          auto *filter_node = request.add_nodes()->mutable_filter();
          filter_node->set_input(static_cast<uint32_t>(input));
          if (!fill_tuple_filter(residuals, filter_node->mutable_filter())) {
            *why = "plan filter not pushable";
            return -1;
          }
          const int self = request.nodes_size() - 1;
          node_tables[self] = node_tables[input];
          node_rows[self] = estimated_rows(path);
          return self;
        }

        case AccessPath::NESTED_LOOP_JOIN:
        case AccessPath::HASH_JOIN: {
          const bool is_nested_loop =
              path->type == AccessPath::NESTED_LOOP_JOIN;
          const int probe = map_path(is_nested_loop
                                         ? path->nested_loop_join().outer
                                         : path->hash_join().outer,
                                     false);
          if (probe < 0) return -1;
          const int build = map_path(is_nested_loop
                                         ? path->nested_loop_join().inner
                                         : path->hash_join().inner,
                                     false);
          if (build < 0) return -1;

          const JoinPredicate *join_predicate =
              is_nested_loop ? path->nested_loop_join().join_predicate
                             : path->hash_join().join_predicate;
          if (join_predicate == nullptr || join_predicate->expr == nullptr) {
            *why = "plan join without predicate";
            return -1;
          }
          if (!is_nested_loop && path->hash_join().rewrite_semi_to_inner) {
            *why = "plan semi-to-inner rewrite";
            return -1;
          }

          RelationalExpression *expression = join_predicate->expr;
          LineairDB::Protocol::QueryBlockJoin::Type join_type;
          if (is_nested_loop) {
            switch (path->nested_loop_join().join_type) {
              case JoinType::INNER:
                join_type = LineairDB::Protocol::QueryBlockJoin::INNER;
                break;
              case JoinType::OUTER:
                join_type = LineairDB::Protocol::QueryBlockJoin::LEFT;
                break;
              case JoinType::SEMI:
                join_type = LineairDB::Protocol::QueryBlockJoin::SEMI;
                break;
              case JoinType::ANTI:
                join_type = LineairDB::Protocol::QueryBlockJoin::ANTI;
                break;
              default:
                *why = "plan join type";
                return -1;
            }
          } else {
            switch (expression->type) {
              case RelationalExpression::INNER_JOIN:
              case RelationalExpression::STRAIGHT_INNER_JOIN:
                join_type = LineairDB::Protocol::QueryBlockJoin::INNER;
                break;
              case RelationalExpression::LEFT_JOIN:
                join_type = LineairDB::Protocol::QueryBlockJoin::LEFT;
                break;
              case RelationalExpression::SEMIJOIN:
                join_type = LineairDB::Protocol::QueryBlockJoin::SEMI;
                break;
              case RelationalExpression::ANTIJOIN:
                join_type = LineairDB::Protocol::QueryBlockJoin::ANTI;
                break;
              default:
                *why = "plan join type";
                return -1;
            }
          }
          return emit_mapped_join(join_type, probe, build, expression,
                                  std::vector<Item *>{},
                                  estimated_rows(path));
        }

        case AccessPath::WEEDOUT: {
          AccessPath *child = path->weedout().child;
          std::vector<Item *> residuals;
          while (child != nullptr && child->type == AccessPath::FILTER) {
            FlattenAnd(child->filter().condition, &residuals);
            child = child->filter().child;
          }
          if (child == nullptr ||
              (child->type != AccessPath::HASH_JOIN &&
               child->type != AccessPath::NESTED_LOOP_JOIN)) {
            *why = "plan weedout shape";
            return -1;
          }

          const bool child_is_nested_loop =
              child->type == AccessPath::NESTED_LOOP_JOIN;
          if (child_is_nested_loop
                  ? child->nested_loop_join().join_type != JoinType::INNER
                  : child->hash_join().rewrite_semi_to_inner) {
            *why = "plan weedout join type";
            return -1;
          }

          const JoinPredicate *join_predicate =
              child_is_nested_loop
                  ? child->nested_loop_join().join_predicate
                  : child->hash_join().join_predicate;
          if (join_predicate == nullptr || join_predicate->expr == nullptr) {
            *why = "plan weedout join predicate";
            return -1;
          }
          if (!child_is_nested_loop &&
              join_predicate->expr->type != RelationalExpression::INNER_JOIN &&
              join_predicate->expr->type !=
                  RelationalExpression::STRAIGHT_INNER_JOIN) {
            *why = "plan weedout join type";
            return -1;
          }

          const int outer = map_path(child_is_nested_loop
                                         ? child->nested_loop_join().outer
                                         : child->hash_join().outer,
                                     false);
          if (outer < 0) return -1;
          const int inner = map_path(child_is_nested_loop
                                         ? child->nested_loop_join().inner
                                         : child->hash_join().inner,
                                     false);
          if (inner < 0) return -1;

          const SJ_TMP_TABLE *weedout_table = path->weedout().weedout_table;
          if (weedout_table == nullptr) {
            *why = "plan weedout table";
            return -1;
          }

          std::set<int> deduplicated_tables;
          for (SJ_TMP_TABLE_TAB *tab = weedout_table->tabs;
               tab != weedout_table->tabs_end; ++tab) {
            const TABLE *table =
                tab->qep_tab != nullptr ? tab->qep_tab->table() : nullptr;
            int table_idx = -1;
            for (size_t idx = 0; idx < tables.size(); idx++) {
              if (tables[idx].table == table) {
                table_idx = static_cast<int>(idx);
                break;
              }
            }
            if (table_idx < 0) {
              *why = "plan weedout table";
              return -1;
            }
            deduplicated_tables.insert(table_idx);
          }

          int probe = -1;
          int build = -1;
          if (deduplicated_tables == node_tables[outer]) {
            probe = outer;
            build = inner;
          } else if (deduplicated_tables == node_tables[inner]) {
            probe = inner;
            build = outer;
          } else {
            *why = "plan weedout tables";
            return -1;
          }

          return emit_mapped_join(LineairDB::Protocol::QueryBlockJoin::SEMI,
                                  probe, build, join_predicate->expr,
                                  residuals, estimated_rows(path));
        }

        case AccessPath::MATERIALIZE: {
          if (path->materialize().param == nullptr ||
              path->materialize().param->table == nullptr) {
            *why = "plan derived missing table";
            return -1;
          }

          const TABLE *table = path->materialize().param->table;
          int table_idx = -1;
          for (size_t i = real_table_count; i < tables.size(); i++) {
            if (tables[i].table == table) {
              table_idx = static_cast<int>(i);
              break;
            }
          }
          if (table_idx < 0) {
            *why = "plan derived unknown";
            return -1;
          }

          if (scan_nodes[table_idx] < 0) {
            if (static_cast<size_t>(table_idx) != next_virtual_table) {
              *why = "plan derived order";
              return -1;
            }
            next_virtual_table++;

            auto *sub_block = request.add_nodes()->mutable_sub_block();
            AccessPath *sub_plan =
                path->materialize().param->query_blocks.empty()
                    ? nullptr
                    : path->materialize()
                          .param->query_blocks[0]
                          .subquery_path;
            if (!BuildQueryBlockRequest(
                    nullptr,
                    virtual_blocks[table_idx - real_table_count],
                    sub_block->mutable_block(), why, sub_plan) &&
                !BuildQueryBlockRequest(
                    nullptr,
                    virtual_blocks[table_idx - real_table_count],
                    sub_block->mutable_block(), why, nullptr)) {
              return -1;
            }
            scan_nodes[table_idx] = request.nodes_size() - 1;
            node_tables[scan_nodes[table_idx]] = {table_idx};
            node_rows[scan_nodes[table_idx]] = estimated_rows(path);
          }
          return scan_nodes[table_idx];
        }

        case AccessPath::SORT:
          if (!root || path->sort().remove_duplicates) {
            *why = "plan sort off root spine";
            return -1;
          }
          return map_path(path->sort().child, true);

        case AccessPath::AGGREGATE:
          if (!root) {
            *why = "plan aggregate off root spine";
            return -1;
          }
          return map_path(path->aggregate().child, true);

        case AccessPath::LIMIT_OFFSET:
          if (!root) {
            *why = "plan limit off root spine";
            return -1;
          }
          return map_path(path->limit_offset().child, true);

        case AccessPath::STREAM:
          return map_path(path->stream().child, root);

        case AccessPath::TEMPTABLE_AGGREGATE:
          if (!root) {
            *why = "plan temptable aggregate off root spine";
            return -1;
          }
          return map_path(path->temptable_aggregate().subquery_path, true);

        default: {
          static thread_local char buffer[64];
          snprintf(buffer, sizeof(buffer), "unmapped plan node %d",
                   static_cast<int>(path->type));
          *why = buffer;
          return -1;
        }
      }
    };

    current_node = map_path(plan, true);
    if (current_node < 0) return false;

    // Add scan-level semi-filters after the mapped tree is complete. A scan can
    // only read key sets from earlier nodes, and probe-side filtering is unsafe
    // for LEFT/ANTI joins because non-matching probe rows must survive.
    auto join_side_filterable =
        [](LineairDB::Protocol::QueryBlockJoin::Type join_type,
           bool target_is_build) {
          if (target_is_build) return true;
          return join_type == LineairDB::Protocol::QueryBlockJoin::INNER ||
                 join_type == LineairDB::Protocol::QueryBlockJoin::SEMI;
        };

    struct SemiSource {
      double rows = -1.0;
      int source_node = -1;
      uint32_t source_table = 0;
      uint32_t source_column = 0;
      uint32_t target_column = 0;
    };

    // Effective cardinality is only used to choose semi-filter sources. When a
    // scan gets filtered by a smaller key domain, later scans can use the
    // reduced estimate rather than the table's raw row count.
    std::vector<double> effective_rows(tables.size(), 1.0);
    for (size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
      if (scan_nodes[table_idx] >= 0 &&
          node_rows.count(scan_nodes[table_idx]) != 0) {
        effective_rows[table_idx] = node_rows[scan_nodes[table_idx]];
      } else if (table_idx < real_table_count) {
        effective_rows[table_idx] = table_rows(table_idx);
      }
    }

    auto leading_records_per_key = [&](size_t table_idx, uint32_t column) {
      if (table_idx >= real_table_count ||
          tables[table_idx].table == nullptr) {
        return 0.0;
      }
      TABLE *table = tables[table_idx].table;
      for (uint key_idx = 0; key_idx < table->s->keys; key_idx++) {
        const KEY &key = table->key_info[key_idx];
        if (key.actual_key_parts == 0 || key.key_part == nullptr ||
            key.key_part[0].field == nullptr) {
          continue;
        }
        if (key.key_part[0].field->field_index() == column &&
            key.has_records_per_key(0)) {
          return std::max(1.0, static_cast<double>(key.records_per_key(0)));
        }
      }
      return 0.0;
    };

    auto best_semi_source = [&](uint32_t target_table, int target_node,
                                double target_rows, bool need_ratio,
                                const std::function<bool(uint32_t)>
                                    &target_column_ok) {
      SemiSource best;
      auto consider = [&](int node, double rows, uint32_t source_table,
                          uint32_t source_column,
                          uint32_t target_column) {
        if (node < 0 || node >= target_node) return;
        if (need_ratio && rows * 8.0 > target_rows) return;
        if (best.source_node < 0 || rows < best.rows) {
          best = {rows, node, source_table, source_column, target_column};
        }
      };

      for (const MappedJoinKey &key : mapped_join_keys) {
        auto consider_side =
            [&](bool target_is_build, uint32_t key_target_table,
                uint32_t key_source_table, uint32_t key_target_column,
                uint32_t key_source_column, int partner_node) {
              if (key_target_table != target_table) return;
              if (!join_side_filterable(key.join_type, target_is_build))
                return;
              if (!target_column_ok(key_target_column)) return;
              if (key_source_table < scan_nodes.size()) {
                consider(scan_nodes[key_source_table],
                         effective_rows[key_source_table], key_source_table,
                         key_source_column, key_target_column);
              }
              const auto rows_it = node_rows.find(partner_node);
              consider(partner_node,
                       rows_it == node_rows.end() ? kUnknownRowEstimate
                                                  : rows_it->second,
                       key_source_table, key_source_column,
                       key_target_column);
            };

        consider_side(true, key.build_table, key.probe_table,
                      key.build_column, key.probe_column, key.probe_node);
        consider_side(false, key.probe_table, key.build_table,
                      key.probe_column, key.build_column, key.build_node);
      }
      return best;
    };

    std::vector<size_t> scan_order;
    for (size_t table_idx = 0; table_idx < real_table_count; table_idx++) {
      if (scan_nodes[table_idx] >= 0) scan_order.push_back(table_idx);
    }
    std::sort(scan_order.begin(), scan_order.end(), [&](size_t left,
                                                        size_t right) {
      return scan_nodes[left] < scan_nodes[right];
    });

    for (size_t table_idx : scan_order) {
      const int node = scan_nodes[table_idx];
      if (!request.nodes(node).has_scan() ||
          request.nodes(node).scan().has_semi()) {
        continue;
      }

      const SemiSource source = best_semi_source(
          static_cast<uint32_t>(table_idx), node, effective_rows[table_idx],
          true, [](uint32_t) { return true; });
      if (source.source_node < 0) continue;

      auto *semi = request.mutable_nodes(node)->mutable_scan()->mutable_semi();
      semi->set_source_node(source.source_node);
      auto *source_column = semi->mutable_source_column();
      source_column->set_table_idx(source.source_table);
      source_column->set_column(source.source_column);
      semi->set_my_column(source.target_column);

      const double fanout =
          leading_records_per_key(table_idx, source.target_column);
      if (fanout > 0.0) {
        effective_rows[table_idx] = std::max(
            1.0, std::min(effective_rows[table_idx], source.rows * fanout));
      }
    }

    // A derived aggregate can also read a smaller key domain from the parent
    // plan. This is only valid when the joined derived column is a direct GROUP
    // output, because the server must push the external key set down to the
    // child scan that owns that group key.
    for (int node_idx = 0; node_idx < request.nodes_size(); node_idx++) {
      if (!request.nodes(node_idx).has_sub_block()) continue;

      int virtual_table = -1;
      for (size_t table_idx = real_table_count; table_idx < tables.size();
           table_idx++) {
        if (scan_nodes[table_idx] == node_idx) {
          virtual_table = static_cast<int>(table_idx);
          break;
        }
      }
      if (virtual_table < 0) continue;

      const auto &child_request = request.nodes(node_idx).sub_block().block();
      auto group_output_is_filterable = [&](uint32_t output_column) {
        if (output_column >=
                static_cast<uint32_t>(child_request.output_size()) ||
            child_request.output(output_column).source() !=
                LineairDB::Protocol::QueryBlockOutputExpr::GROUP ||
            child_request.nodes_size() == 0 ||
            !child_request.nodes(child_request.nodes_size() - 1)
                 .has_aggregate()) {
          return false;
        }

        const auto &aggregate =
            child_request.nodes(child_request.nodes_size() - 1).aggregate();
        const uint32_t group_ordinal =
            child_request.output(output_column).ordinal();
        return group_ordinal <
                   static_cast<uint32_t>(aggregate.group_columns_size()) &&
               aggregate.group_columns(group_ordinal).prefix_len() == 0;
      };

      const SemiSource source = best_semi_source(
          static_cast<uint32_t>(virtual_table), node_idx, 0.0, false,
          group_output_is_filterable);
      if (source.source_node < 0) continue;

      const auto &aggregate =
          child_request.nodes(child_request.nodes_size() - 1).aggregate();
      const uint32_t group_ordinal =
          child_request.output(source.target_column).ordinal();
      auto *sub_block = request.mutable_nodes(node_idx)->mutable_sub_block();
      auto *semi = sub_block->mutable_semi();
      semi->set_source_node(source.source_node);
      auto *source_column = semi->mutable_source_column();
      source_column->set_table_idx(source.source_table);
      source_column->set_column(source.source_column);
      semi->set_my_column(source.target_column);
      sub_block->set_target_table(
          aggregate.group_columns(group_ordinal).table_idx());
      sub_block->set_target_column(
          aggregate.group_columns(group_ordinal).column());
    }
  } else {
    // Scan every table once, attaching only predicates that read that table.
    for (size_t table_idx = 0; table_idx < real_table_count; table_idx++) {
      if (!emit_scan(table_idx)) LDB_COL_REJECT("filter not pushable");
    }

    for (size_t table_idx = real_table_count; table_idx < tables.size();
         table_idx++) {
      auto *sub_block = request.add_nodes()->mutable_sub_block();
      if (!BuildQueryBlockRequest(
              nullptr, virtual_blocks[table_idx - real_table_count],
              sub_block->mutable_block(), why)) {
        return false;
      }
      scan_nodes[table_idx] = request.nodes_size() - 1;
    }

    // Build a left-deep INNER join tree by walking FROM order and retrying
    // tables whose equality edges are not connected yet.
    auto first_input =
        std::find_if(main_tables.begin(), main_tables.end(), [&](int table_idx) {
          return !is_scalar_virtual_table(table_idx);
        });
    const int first_table =
        first_input != main_tables.end() ? *first_input : main_tables[0];
    current_node = scan_nodes[first_table];
    std::vector<bool> joined(tables.size(), false);
    joined[first_table] = true;
    std::vector<int> pending_tables;
    for (int table_idx : main_tables) {
      if (table_idx != first_table) pending_tables.push_back(table_idx);
    }
    while (!pending_tables.empty()) {
      int table_idx = -1;
      size_t pending_idx = 0;
      bool keyless_one_row_join = false;
      for (size_t idx = 0; idx < pending_tables.size(); idx++) {
        for (const JoinEdge &edge : join_edges) {
          if ((edge.left_table == pending_tables[idx] &&
               joined[edge.right_table]) ||
              (edge.right_table == pending_tables[idx] &&
               joined[edge.left_table])) {
            table_idx = pending_tables[idx];
            pending_idx = idx;
            break;
          }
        }
        if (table_idx >= 0) break;
      }
      if (table_idx < 0) {
        // Scalar aggregate sub-blocks produce at most one row, so they are the
        // only virtual inputs that can use a keyless INNER join.
        for (size_t idx = 0; idx < pending_tables.size(); idx++) {
          if (is_scalar_virtual_table(pending_tables[idx])) {
            table_idx = pending_tables[idx];
            pending_idx = idx;
            keyless_one_row_join = true;
            break;
          }
        }
      }
      if (table_idx < 0) LDB_COL_REJECT("disconnected join graph");
      pending_tables.erase(pending_tables.begin() + pending_idx);

      bool connected = keyless_one_row_join;
      auto *join_node = request.add_nodes()->mutable_join();
      join_node->set_type(LineairDB::Protocol::QueryBlockJoin::INNER);
      join_node->set_build(scan_nodes[table_idx]);
      join_node->set_probe(current_node);

      for (const JoinEdge &edge : join_edges) {
        const Field *build_field = nullptr;
        const Field *probe_field = nullptr;
        int probe_table = -1;
        if (edge.left_table == static_cast<int>(table_idx) &&
            joined[edge.right_table]) {
          build_field = edge.left_field;
          probe_field = edge.right_field;
          probe_table = edge.right_table;
        } else if (edge.right_table == static_cast<int>(table_idx) &&
                   joined[edge.left_table]) {
          build_field = edge.right_field;
          probe_field = edge.left_field;
          probe_table = edge.left_table;
        } else {
          continue;
        }

        auto *build_key = join_node->add_build_keys();
        build_key->set_table_idx(static_cast<uint32_t>(table_idx));
        build_key->set_column(build_field->field_index());
        auto *probe_key = join_node->add_probe_keys();
        probe_key->set_table_idx(static_cast<uint32_t>(probe_table));
        probe_key->set_column(probe_field->field_index());
        connected = true;
      }

      if (!connected) LDB_COL_REJECT("disconnected join graph");
      joined[table_idx] = true;
      current_node = request.nodes_size() - 1;
    }

  // MySQL may rewrite correlated subqueries into derived tables on the
  // nullable side of a LEFT join. Execute the derived block as a virtual table,
  // then split the ON condition into equality keys and residual predicates.
  for (int table_idx : outer_derived_tables) {
    Table_ref *table_ref = tables[table_idx].table_ref;
    Item *join_condition =
        table_ref == nullptr ? nullptr : table_ref->join_cond();
    if (join_condition == nullptr) LDB_COL_REJECT("derived LEFT without ON");

    std::vector<Item *> join_predicates;
    FlattenAnd(join_condition, &join_predicates);
    std::vector<Item *> residual_predicates;

    auto *join_node = request.add_nodes()->mutable_join();
    join_node->set_type(LineairDB::Protocol::QueryBlockJoin::LEFT);
    join_node->set_build(static_cast<uint32_t>(scan_nodes[table_idx]));
    join_node->set_probe(static_cast<uint32_t>(current_node));

    for (Item *predicate : join_predicates) {
      bool is_join_key = false;
      if (predicate->type() == Item::FUNC_ITEM &&
          down_cast<Item_func *>(predicate)->functype() ==
              Item_func::EQ_FUNC) {
        auto *equals = down_cast<Item_func *>(predicate);
        Item *left = equals->arguments()[0]->real_item();
        Item *right = equals->arguments()[1]->real_item();
        if (left->type() == Item::FIELD_ITEM &&
            right->type() == Item::FIELD_ITEM) {
          const Field *left_raw = semijoin_outer_equivalent(
              down_cast<Item_field *>(left)->field);
          const Field *right_raw = semijoin_outer_equivalent(
              down_cast<Item_field *>(right)->field);
          int left_table = TableIndexOfField(left_raw, tables);
          int right_table = TableIndexOfField(right_raw, tables);
          if (left_table == table_idx || right_table == table_idx) {
            if (left_table != table_idx) {
              std::swap(left_table, right_table);
              std::swap(left_raw, right_raw);
            }
            if (right_table >= 0 &&
                JoinColumnsUseByteEquality(left_raw, right_raw)) {
              const Field *build_field =
                  resolve_query_field(left_raw, left_table);
              const Field *probe_field =
                  resolve_query_field(right_raw, right_table);
              if (build_field != nullptr && probe_field != nullptr) {
                auto *build_key = join_node->add_build_keys();
                build_key->set_table_idx(static_cast<uint32_t>(table_idx));
                build_key->set_column(build_field->field_index());
                auto *probe_key = join_node->add_probe_keys();
                probe_key->set_table_idx(static_cast<uint32_t>(right_table));
                probe_key->set_column(probe_field->field_index());
                is_join_key = true;
              }
            }
          }
        }
      }
      if (!is_join_key) residual_predicates.push_back(predicate);
    }

    if (join_node->build_keys_size() == 0 &&
        !is_scalar_virtual_table(table_idx)) {
      LDB_COL_REJECT("derived LEFT keyless");
    }

    if (!residual_predicates.empty()) {
      TupleColumnRegistry registry;
      registry.table_of = [&](const Field *field) {
        return TableIndexOfField(field, tables);
      };
      registry.resolve = [&](const Field *field, int table_idx) {
        return resolve_query_field(field, table_idx);
      };

      auto *residual = join_node->mutable_residual();
      auto *predicate = residual->mutable_predicate();
      auto *expression = predicate->mutable_expr();
      if (residual_predicates.size() == 1) {
        if (!SerializeTuplePredicate(residual_predicates[0], expression,
                                     &registry)) {
          LDB_COL_REJECT("derived ON residual not pushable");
        }
      } else {
        expression->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *residual_predicate : residual_predicates) {
          if (!SerializeTuplePredicate(residual_predicate,
                                       expression->add_children(),
                                       &registry)) {
            LDB_COL_REJECT("derived ON residual not pushable");
          }
        }
      }

      predicate->set_num_columns(registry.columns.size());
      for (const auto &column_ref : registry.columns) {
        auto *column = residual->add_columns();
        column->set_table_idx(static_cast<uint32_t>(column_ref.first));
        column->set_column(column_ref.second->field_index());
      }
    }

    current_node = request.nodes_size() - 1;
  }

  if (!tuple_predicates.empty()) {
    auto *filter_node = request.add_nodes()->mutable_filter();
    filter_node->set_input(static_cast<uint32_t>(current_node));
    auto *tuple_filter = filter_node->mutable_filter();

    TupleColumnRegistry registry;
    registry.table_of = [&](const Field *field) {
      return TableIndexOfField(field, tables);
    };
    registry.resolve = [&](const Field *field, int table_idx) {
      return resolve_query_field(field, table_idx);
    };

    auto *predicate = tuple_filter->mutable_predicate();
    auto *expression = predicate->mutable_expr();
    if (tuple_predicates.size() == 1) {
      if (!SerializeTuplePredicate(tuple_predicates[0], expression,
                                   &registry)) {
        LDB_COL_REJECT("tuple predicate not pushable");
      }
    } else {
      expression->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
      for (Item *tuple_predicate : tuple_predicates) {
        if (!SerializeTuplePredicate(tuple_predicate,
                                     expression->add_children(),
                                     &registry)) {
          LDB_COL_REJECT("tuple predicate not pushable");
        }
      }
    }

    predicate->set_num_columns(registry.columns.size());
    for (const auto &column_ref : registry.columns) {
      auto *column = tuple_filter->add_columns();
      column->set_table_idx(static_cast<uint32_t>(column_ref.first));
      column->set_column(column_ref.second->field_index());
    }
    current_node = request.nodes_size() - 1;
  }

  for (SemijoinNest &semijoin : semijoin_nests) {
    if (semijoin.inner_tables.empty())
      LDB_COL_REJECT("empty semijoin");
    if (semijoin.nest == nullptr || semijoin.nest->nested_join == nullptr)
      LDB_COL_REJECT("semijoin missing key metadata");

    auto semijoin_has_table = [&](int table_idx) {
      for (int inner_table : semijoin.inner_tables) {
        if (inner_table == table_idx) return true;
      }
      return false;
    };

    int first_inner_table = semijoin.inner_tables[0];
    if (semijoin.inner_tables.size() > 1) {
      auto first_inner_input =
          std::find_if(semijoin.inner_tables.begin(),
                       semijoin.inner_tables.end(), [&](int table_idx) {
                         return !is_scalar_virtual_table(table_idx);
                       });
      if (first_inner_input != semijoin.inner_tables.end()) {
        first_inner_table = *first_inner_input;
      }
    }

    // Multi-table semijoin nests become a build-side INNER-join mini tree.
    int build_node = scan_nodes[first_inner_table];
    if (semijoin.inner_tables.size() > 1) {
      struct InnerJoinEdge {
        int left_table = -1;
        int right_table = -1;
        const Field *left_field = nullptr;
        const Field *right_field = nullptr;
      };

      std::vector<InnerJoinEdge> inner_edges;
      std::vector<Item *> remaining_residuals;
      for (Item *predicate : semijoin.residual_predicates) {
        bool consumed = false;
        if (predicate->type() == Item::FUNC_ITEM &&
            down_cast<Item_func *>(predicate)->functype() ==
                Item_func::EQ_FUNC) {
          auto *equals = down_cast<Item_func *>(predicate);
          Item *left = equals->arguments()[0]->real_item();
          Item *right = equals->arguments()[1]->real_item();
          if (left->type() == Item::FIELD_ITEM &&
              right->type() == Item::FIELD_ITEM) {
            const Field *left_raw = down_cast<Item_field *>(left)->field;
            const Field *right_raw = down_cast<Item_field *>(right)->field;
            const int left_table = TableIndexOfField(left_raw, tables);
            const int right_table = TableIndexOfField(right_raw, tables);
            const Field *left_field =
                left_table >= 0 ? resolve_query_field(left_raw, left_table)
                                : nullptr;
            const Field *right_field =
                right_table >= 0 ? resolve_query_field(right_raw, right_table)
                                 : nullptr;
            if (left_table >= 0 && right_table >= 0 &&
                left_table != right_table && semijoin_has_table(left_table) &&
                semijoin_has_table(right_table) &&
                JoinColumnsUseByteEquality(left_raw, right_raw) &&
                left_field != nullptr && right_field != nullptr &&
                !left_field->is_nullable() && !right_field->is_nullable()) {
              inner_edges.push_back(
                  {left_table, right_table, left_field, right_field});
              consumed = true;
            }
          }
        }
        if (!consumed) remaining_residuals.push_back(predicate);
      }
      semijoin.residual_predicates = std::move(remaining_residuals);

      std::vector<bool> inner_joined(tables.size(), false);
      inner_joined[first_inner_table] = true;
      std::vector<int> pending_inner_tables;
      for (int table_idx : semijoin.inner_tables) {
        if (table_idx != first_inner_table) {
          pending_inner_tables.push_back(table_idx);
        }
      }

      while (!pending_inner_tables.empty()) {
        int picked_table = -1;
        size_t picked_idx = 0;
        bool keyless_one_row_join = false;
        for (size_t idx = 0; idx < pending_inner_tables.size(); idx++) {
          for (const InnerJoinEdge &edge : inner_edges) {
            if ((edge.left_table == pending_inner_tables[idx] &&
                 inner_joined[edge.right_table]) ||
                (edge.right_table == pending_inner_tables[idx] &&
                 inner_joined[edge.left_table])) {
              picked_table = pending_inner_tables[idx];
              picked_idx = idx;
              break;
            }
          }
          if (picked_table >= 0) break;
        }
        if (picked_table < 0) {
          // Keep keyless INNER joins limited to one-row virtual inputs.
          for (size_t idx = 0; idx < pending_inner_tables.size(); idx++) {
            if (is_scalar_virtual_table(pending_inner_tables[idx])) {
              picked_table = pending_inner_tables[idx];
              picked_idx = idx;
              keyless_one_row_join = true;
              break;
            }
          }
        }
        if (picked_table < 0) LDB_COL_REJECT("semijoin disconnected");
        pending_inner_tables.erase(pending_inner_tables.begin() + picked_idx);

        bool connected = keyless_one_row_join;
        auto *inner_join = request.add_nodes()->mutable_join();
        inner_join->set_type(LineairDB::Protocol::QueryBlockJoin::INNER);
        inner_join->set_build(static_cast<uint32_t>(scan_nodes[picked_table]));
        inner_join->set_probe(static_cast<uint32_t>(build_node));

        for (const InnerJoinEdge &edge : inner_edges) {
          const Field *build_field = nullptr;
          const Field *probe_field = nullptr;
          int probe_table = -1;
          if (edge.left_table == picked_table &&
              inner_joined[edge.right_table]) {
            build_field = edge.left_field;
            probe_field = edge.right_field;
            probe_table = edge.right_table;
          } else if (edge.right_table == picked_table &&
                     inner_joined[edge.left_table]) {
            build_field = edge.right_field;
            probe_field = edge.left_field;
            probe_table = edge.left_table;
          } else {
            continue;
          }

          auto *build_key = inner_join->add_build_keys();
          build_key->set_table_idx(static_cast<uint32_t>(picked_table));
          build_key->set_column(build_field->field_index());
          auto *probe_key = inner_join->add_probe_keys();
          probe_key->set_table_idx(static_cast<uint32_t>(probe_table));
          probe_key->set_column(probe_field->field_index());
          connected = true;
        }

        if (!connected) LDB_COL_REJECT("semijoin disconnected");
        inner_joined[picked_table] = true;
        build_node = request.nodes_size() - 1;
      }
    }

    auto *join_node = request.add_nodes()->mutable_join();
    join_node->set_type(semijoin.anti
                            ? LineairDB::Protocol::QueryBlockJoin::ANTI
                            : LineairDB::Protocol::QueryBlockJoin::SEMI);
    join_node->set_build(static_cast<uint32_t>(build_node));
    join_node->set_probe(static_cast<uint32_t>(current_node));

    const auto &outer_exprs =
        semijoin.nest->nested_join->sj_outer_exprs;
    const auto &inner_exprs =
        semijoin.nest->nested_join->sj_inner_exprs;
    if (outer_exprs.size() != inner_exprs.size() || outer_exprs.empty()) {
      LDB_COL_REJECT("semijoin key arity");
    }

    for (size_t key_idx = 0; key_idx < outer_exprs.size(); key_idx++) {
      Item *outer_item = outer_exprs[key_idx]->real_item();
      Item *inner_item = inner_exprs[key_idx]->real_item();
      if (outer_item->type() != Item::FIELD_ITEM ||
          inner_item->type() != Item::FIELD_ITEM) {
        LDB_COL_REJECT("semijoin key shape");
      }

      const Field *raw_outer_field =
          down_cast<Item_field *>(outer_item)->field;
      const Field *raw_inner_field =
          down_cast<Item_field *>(inner_item)->field;
      const int outer_table = TableIndexOfField(raw_outer_field, tables);
      const int inner_key_table = TableIndexOfField(raw_inner_field, tables);
      if (outer_table < 0 || !semijoin_has_table(inner_key_table)) {
        LDB_COL_REJECT("semijoin key tables");
      }
      if (!JoinColumnsUseByteEquality(raw_outer_field, raw_inner_field)) {
        LDB_COL_REJECT("semijoin key type");
      }

      const Field *outer_field =
          resolve_query_field(raw_outer_field, outer_table);
      const Field *inner_field =
          resolve_query_field(raw_inner_field, inner_key_table);
      if (outer_field == nullptr || inner_field == nullptr) {
        LDB_COL_REJECT("semijoin key unresolvable");
      }

      auto *build_key = join_node->add_build_keys();
      build_key->set_table_idx(static_cast<uint32_t>(inner_key_table));
      build_key->set_column(inner_field->field_index());
      auto *probe_key = join_node->add_probe_keys();
      probe_key->set_table_idx(static_cast<uint32_t>(outer_table));
      probe_key->set_column(outer_field->field_index());
    }

    if (!semijoin.residual_predicates.empty()) {
      TupleColumnRegistry registry;
      registry.table_of = [&](const Field *field) {
        return TableIndexOfField(field, tables);
      };
      registry.resolve = [&](const Field *field, int table_idx) {
        return resolve_query_field(field, table_idx);
      };

      auto *residual = join_node->mutable_residual();
      auto *predicate = residual->mutable_predicate();
      auto *expression = predicate->mutable_expr();
      if (semijoin.residual_predicates.size() == 1) {
        if (!SerializeTuplePredicate(semijoin.residual_predicates[0],
                                     expression, &registry)) {
          LDB_COL_REJECT("semijoin residual not pushable");
        }
      } else {
        expression->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
        for (Item *residual_predicate : semijoin.residual_predicates) {
          if (!SerializeTuplePredicate(residual_predicate,
                                       expression->add_children(),
                                       &registry)) {
            LDB_COL_REJECT("semijoin residual not pushable");
          }
        }
      }

      predicate->set_num_columns(registry.columns.size());
      for (const auto &column_ref : registry.columns) {
        auto *column = residual->add_columns();
        column->set_table_idx(static_cast<uint32_t>(column_ref.first));
        column->set_column(column_ref.second->field_index());
      }
    }

    current_node = request.nodes_size() - 1;
  }
  }

  std::vector<Item *> output_items;
  for (Item *item : VisibleFields(qb->fields)) output_items.push_back(item);

  if (plain_rows) {
    if (qb->having_cond() != nullptr) LDB_COL_REJECT("row block has HAVING");
    for (Item *item : output_items) {
      Item *real = item->real_item();
      if (real->type() != Item::FIELD_ITEM)
        LDB_COL_REJECT("row output not a column");

      const Field *raw_output_field = down_cast<Item_field *>(real)->field;
      const int output_table = TableIndexOfField(raw_output_field, tables);
      const Field *output_field =
          output_table >= 0
              ? resolve_query_field(raw_output_field, output_table)
              : nullptr;
      if (output_field == nullptr)
        LDB_COL_REJECT("row output column unresolvable");

      auto *output = request.add_output();
      output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::COLUMN);
      auto *column = output->mutable_column();
      column->set_table_idx(static_cast<uint32_t>(output_table));
      column->set_column(output_field->field_index());
      column->set_cmp_kind(output_field->result_type() == STRING_RESULT ? 1
                                                                        : 2);
    }

    for (ORDER *order = qb->order_list.first; order != nullptr;
         order = order->next) {
      const int output_ordinal =
          OrderOutputOrdinal(*order->item, output_items);
      if (output_ordinal < 0)
        LDB_COL_REJECT("row ORDER BY not an output column");

      auto *sort_key = request.add_order_by();
      sort_key->set_output_ordinal(output_ordinal);
      sort_key->set_descending(order->direction == ORDER_DESC);
      Item *output_item = output_items[output_ordinal]->real_item();
      sort_key->set_cmp_kind(output_item->result_type() == STRING_RESULT ? 1
                                                                         : 2);
    }

    if (qb->has_limit()) {
      // LIMIT without ORDER BY picks arbitrary rows: the row engine and this
      // executor (whose group-map iteration order is an implementation
      // detail) may legally return different rows, so reject loudly instead
      // of silently diverging.
      if (qb->order_list.first == nullptr &&
          unit->select_limit_cnt != HA_POS_ERROR)
        LDB_COL_REJECT("LIMIT without ORDER BY");
      if (unit->select_limit_cnt != HA_POS_ERROR)
        request.set_limit(unit->select_limit_cnt);
      if (unit->offset_limit_cnt > 0) {
        request.set_offset(unit->offset_limit_cnt);
        if (request.limit() > 0)
          request.set_limit(request.limit() - unit->offset_limit_cnt);
      }
    }

    *why = nullptr;
    return true;
  }

  auto *aggregate_node = request.add_nodes();
  auto *aggregate = aggregate_node->mutable_aggregate();
  aggregate->set_input(current_node);
  struct GroupField {
    int table = -1;
    const Field *field = nullptr;
  };
  std::vector<GroupField> group_fields;
  std::vector<Item *> group_items;
  if (distinct_as_group) {
    // SELECT DISTINCT without aggregates is group-by-all over the visible
    // output expressions. Constants cannot split groups; EXISTS-style
    // rewrites put literals in the select list.
    for (Item *item : output_items) {
      Item *real = item->real_item();
      if (real->const_item()) continue;
      group_items.push_back(real);
    }
    // Group-by-nothing fabricates one row from empty input, which is wrong
    // for DISTINCT over constants only.
    if (group_items.empty()) LDB_COL_REJECT("distinct outputs all constant");
  } else {
    for (ORDER *group = qb->group_list.first; group != nullptr;
         group = group->next) {
      group_items.push_back((*group->item)->real_item());
    }
  }

  for (Item *group_item : group_items) {
    if (const Field *year_field = ExtractYearField(group_item)) {
      const int group_table = TableIndexOfField(year_field, tables);
      const Field *group_field =
          group_table >= 0 ? resolve_query_field(year_field, group_table)
                           : nullptr;
      if (group_field == nullptr ||
          (!table_is_virtual[group_table] && group_field->is_nullable())) {
        LDB_COL_REJECT("group year column");
      }

      auto *group_column = aggregate->add_group_columns();
      group_column->set_table_idx(static_cast<uint32_t>(group_table));
      group_column->set_column(group_field->field_index());
      group_column->set_prefix_len(4);
      group_column->set_cmp_kind(0);
      group_fields.push_back({group_table, nullptr});
      continue;
    }

    uint32_t substring_prefix_len = 0;
    if (const Field *substring_field =
            SubstringPrefixField(group_item, &substring_prefix_len)) {
      const int group_table = TableIndexOfField(substring_field, tables);
      const Field *group_field =
          group_table >= 0
              ? resolve_query_field(substring_field, group_table)
              : nullptr;
      if (group_field == nullptr ||
          (!table_is_virtual[group_table] && group_field->is_nullable())) {
        LDB_COL_REJECT("group substring column");
      }

      auto *group_column = aggregate->add_group_columns();
      group_column->set_table_idx(static_cast<uint32_t>(group_table));
      group_column->set_column(group_field->field_index());
      group_column->set_prefix_len(substring_prefix_len);
      group_column->set_cmp_kind(1);
      group_fields.push_back({group_table, nullptr});
      continue;
    }

    if (group_item->type() != Item::FIELD_ITEM)
      LDB_COL_REJECT("group item not a column");

    const Field *raw_group_field =
        down_cast<Item_field *>(group_item)->field;
    const int group_table = TableIndexOfField(raw_group_field, tables);
    if (group_table < 0) LDB_COL_REJECT("group column foreign");
    const Field *group_field =
        resolve_query_field(raw_group_field, group_table);
    if (group_field == nullptr) LDB_COL_REJECT("group column foreign");
    if ((!table_is_virtual[group_table] && group_field->is_nullable()) ||
        (!table_is_virtual[group_table] &&
         !GroupColumnUsesStoredByteKey(group_field))) {
      LDB_COL_REJECT("group column cannot use stored-byte key");
    }

    auto *group_column = aggregate->add_group_columns();
    group_column->set_table_idx(static_cast<uint32_t>(group_table));
    group_column->set_column(group_field->field_index());
    group_column->set_cmp_kind(group_field->result_type() == STRING_RESULT ? 1
                                                                           : 0);
    group_fields.push_back({group_table, group_field});
  }

  const char *aggregate_error = nullptr;
  auto register_aggregate = [&](Item_sum *sum) -> int {
    if (sum->argument_count() > 1) {
      aggregate_error = "aggregate arg count";
      return -1;
    }

    auto *function = aggregate->add_aggs();
    switch (sum->sum_func()) {
      case Item_sum::COUNT_DISTINCT_FUNC: {
        if (sum->argument_count() != 1) {
          aggregate_error = "COUNT DISTINCT arg count";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->type() != Item::FIELD_ITEM) {
          aggregate_error = "COUNT DISTINCT arg shape";
          return -1;
        }
        const Field *raw_count_field = down_cast<Item_field *>(arg)->field;
        const int count_table = TableIndexOfField(raw_count_field, tables);
        const Field *count_field =
            count_table >= 0
                ? resolve_query_field(raw_count_field, count_table)
                : nullptr;
        if (count_field == nullptr) {
          aggregate_error = "COUNT DISTINCT arg unresolvable";
          return -1;
        }

        function->set_arg_table(static_cast<uint32_t>(count_table));
        auto *ref = function->mutable_arg();
        ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
        ref->set_column_index(count_field->field_index());
        function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
        function->set_distinct(true);
        return aggregate->aggs_size() - 1;
      }

      case Item_sum::COUNT_FUNC: {
        function->set_arg_table(0);
        if (sum->argument_count() > 0) {
          Item *arg = sum->get_arg(0)->real_item();
          if (arg->const_item()) {
            if (arg->is_nullable() || arg->is_null()) {
              aggregate_error = "COUNT const nullable";
              return -1;
            }
          } else {
            if (arg->type() != Item::FIELD_ITEM) {
              aggregate_error = "COUNT arg shape";
              return -1;
            }
            const Field *raw_count_field =
                down_cast<Item_field *>(arg)->field;
            const int count_table = TableIndexOfField(raw_count_field, tables);
            const Field *count_field =
                count_table >= 0
                    ? resolve_query_field(raw_count_field, count_table)
                    : nullptr;
            if (count_field == nullptr ||
                (!table_is_virtual[count_table] &&
                 count_field->is_nullable())) {
              aggregate_error = "COUNT arg nullable";
              return -1;
            }
            function->set_arg_table(static_cast<uint32_t>(count_table));
            auto *ref = function->mutable_arg();
            ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
            ref->set_column_index(count_field->field_index());
          }
        }

        function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
        return aggregate->aggs_size() - 1;
      }

      case Item_sum::SUM_FUNC:
      case Item_sum::AVG_FUNC: {
        if (sum->argument_count() != 1) {
          aggregate_error = "aggregate arg count";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        if (sum->sum_func() == Item_sum::SUM_FUNC) {
          if (Item *filter = CaseToCountFilter(arg)) {
            const int filter_table = SingleTableOf(
                filter->used_tables() & ~PSEUDO_TABLE_BITS, tables);
            if (filter_table < 0) {
              aggregate_error = "CASE filter tables";
              return -1;
            }
            auto *predicate = function->mutable_filter();
            predicate->set_num_columns(tables[filter_table].table->s->fields);
            if (!serialize_item(filter, predicate->mutable_expr())) {
              aggregate_error = "CASE filter not pushable";
              return -1;
            }
            function->set_arg_table(static_cast<uint32_t>(filter_table));
            function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
            return aggregate->aggs_size() - 1;
          }

          Item *filter = nullptr;
          Item *then_expr = nullptr;
          if (CaseToFilteredSum(arg, &filter, &then_expr)) {
            if (then_expr->result_type() != DECIMAL_RESULT &&
                then_expr->result_type() != INT_RESULT) {
              aggregate_error = "CASE expression type";
              return -1;
            }

            const int filter_table = SingleTableOf(
                filter->used_tables() & ~PSEUDO_TABLE_BITS, tables);
            if (filter_table < 0) {
              aggregate_error = "CASE filter tables";
              return -1;
            }
            auto *predicate = function->mutable_filter();
            predicate->set_num_columns(tables[filter_table].table->s->fields);
            if (!serialize_item(filter, predicate->mutable_expr())) {
              aggregate_error = "CASE filter not pushable";
              return -1;
            }

            int argument_table = SingleTableOf(
                then_expr->used_tables() & ~PSEUDO_TABLE_BITS, tables);
            if (argument_table >= 0 &&
                then_expr->real_item()->type() == Item::FIELD_ITEM) {
              const Field *raw_argument_field =
                  down_cast<Item_field *>(then_expr->real_item())->field;
              const Field *argument_field =
                  resolve_query_field(raw_argument_field, argument_table);
              if (argument_field == nullptr) {
                aggregate_error = "CASE expression unresolvable";
                return -1;
              }

              auto *ref = function->mutable_arg();
              ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
              ref->set_column_index(argument_field->field_index());
            } else if (argument_table >= 0) {
              if (!lineairdb::serialize_aggregate_expression(
                      then_expr->real_item(), function->mutable_arg())) {
                aggregate_error = "CASE expression not pushable";
                return -1;
              }
            } else {
              argument_table = 0;
              if (!SerializeAggregateExpressionForTables(
                      then_expr->real_item(), tables,
                      static_cast<uint32_t>(argument_table),
                      function->mutable_arg())) {
                aggregate_error = "CASE expression not pushable";
                return -1;
              }
            }

            function->set_arg_table(static_cast<uint32_t>(argument_table));
            function->set_filter_table(static_cast<uint32_t>(filter_table));
            function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::SUM);
            function->set_arg_scale(then_expr->decimals);
            function->set_zero_if_empty(true);
            return aggregate->aggs_size() - 1;
          }
        }
        if (arg->result_type() != DECIMAL_RESULT &&
            arg->result_type() != INT_RESULT) {
          aggregate_error = "aggregate arg type";
          return -1;
        }

        int argument_table =
            SingleTableOf(arg->used_tables() & ~PSEUDO_TABLE_BITS, tables);
        if (argument_table >= 0 && arg->type() == Item::FIELD_ITEM) {
          const Field *raw_argument_field =
              down_cast<Item_field *>(arg)->field;
          const Field *argument_field =
              resolve_query_field(raw_argument_field, argument_table);
          if (argument_field == nullptr) {
            aggregate_error = "aggregate arg unresolvable";
            return -1;
          }

          auto *ref = function->mutable_arg();
          ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
          ref->set_column_index(argument_field->field_index());
        } else if (argument_table >= 0) {
          if (!lineairdb::serialize_aggregate_expression(
                  arg, function->mutable_arg())) {
            aggregate_error = "aggregate expr not pushable";
            return -1;
          }
        } else {
          argument_table = 0;
          if (!SerializeAggregateExpressionForTables(
                  arg, tables, static_cast<uint32_t>(argument_table),
                  function->mutable_arg())) {
            aggregate_error = "aggregate expr not pushable";
            return -1;
          }
        }

        function->set_arg_table(static_cast<uint32_t>(argument_table));
        function->set_kind(
            sum->sum_func() == Item_sum::SUM_FUNC
                ? LineairDB::Protocol::QueryBlockAggFunc::SUM
                : LineairDB::Protocol::QueryBlockAggFunc::AVG);
        function->set_arg_scale(arg->decimals);
        return aggregate->aggs_size() - 1;
      }

      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        if (sum->argument_count() != 1) {
          aggregate_error = "aggregate arg count";
          return -1;
        }
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->type() != Item::FIELD_ITEM) {
          aggregate_error = "minmax arg";
          return -1;
        }
        const Field *raw_argument_field =
            down_cast<Item_field *>(arg)->field;
        const int argument_table =
            TableIndexOfField(raw_argument_field, tables);
        const Field *argument_field =
            argument_table >= 0
                ? resolve_query_field(raw_argument_field, argument_table)
                : nullptr;
        if (argument_field == nullptr) {
          aggregate_error = "minmax unresolvable";
          return -1;
        }
        if (!table_is_virtual[argument_table] &&
            argument_field->is_nullable()) {
          aggregate_error = "minmax nullable";
          return -1;
        }
        if (!table_is_virtual[argument_table] &&
            argument_field->result_type() == STRING_RESULT &&
            !argument_field->binary()) {
          aggregate_error = "minmax collation";
          return -1;
        }

        auto *ref = function->mutable_arg();
        ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
        ref->set_column_index(argument_field->field_index());
        function->set_arg_table(static_cast<uint32_t>(argument_table));
        function->set_kind(
            sum->sum_func() == Item_sum::MIN_FUNC
                ? LineairDB::Protocol::QueryBlockAggFunc::MIN
                : LineairDB::Protocol::QueryBlockAggFunc::MAX);
        function->set_cmp_kind(argument_field->result_type() == STRING_RESULT
                                   ? 1
                                   : 0);
        return aggregate->aggs_size() - 1;
      }

      default:
        aggregate_error = "aggregate kind unsupported";
        return -1;
    }
  };

  const uint32_t aggregate_output_base = aggregate->group_columns_size();
  std::function<bool(Item *, LineairDB::Protocol::FilterExpr *)>
      serialize_output_expression =
          [&](Item *item, LineairDB::Protocol::FilterExpr *expr) -> bool {
    item = item->real_item();
    if (item->type() == Item::SUM_FUNC_ITEM) {
      const int ordinal = register_aggregate(down_cast<Item_sum *>(item));
      if (ordinal < 0) return false;
      expr->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      expr->set_column_index(aggregate_output_base +
                             static_cast<uint32_t>(ordinal));
      return true;
    }
    if (item->type() == Item::FIELD_ITEM) {
      const Field *raw_field = down_cast<Item_field *>(item)->field;
      const int table_idx = TableIndexOfField(raw_field, tables);
      if (table_idx < static_cast<int>(real_table_count) ||
          table_idx >= static_cast<int>(tables.size()) ||
          !table_is_virtual[table_idx]) {
        return false;
      }
      const size_t virtual_idx =
          static_cast<size_t>(table_idx) - real_table_count;
      if (virtual_idx >= virtual_block_is_scalar_aggregate.size() ||
          !virtual_block_is_scalar_aggregate[virtual_idx]) {
        return false;
      }

      auto *function = aggregate->add_aggs();
      function->set_arg_table(static_cast<uint32_t>(table_idx));
      auto *ref = function->mutable_arg();
      ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      ref->set_column_index(raw_field->field_index());
      function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::MIN);
      function->set_cmp_kind(raw_field->result_type() == STRING_RESULT ? 1
                                                                       : 0);

      expr->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
      expr->set_column_index(aggregate_output_base +
                             static_cast<uint32_t>(
                                 aggregate->aggs_size() - 1));
      return true;
    }
    if (item->type() == Item::INT_ITEM) {
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_INT);
      expr->set_int_val(item->val_int());
      return true;
    }
    if (item->const_item() && (item->result_type() == DECIMAL_RESULT ||
                               item->result_type() == REAL_RESULT)) {
      String buffer;
      String *value = item->val_str(&buffer);
      if (value == nullptr) return false;
      expr->set_op(LineairDB::Protocol::FilterExpr::CONST_STRING);
      expr->set_string_val(value->ptr(), value->length());
      return true;
    }
    if (item->type() != Item::FUNC_ITEM) return false;

    auto *function = down_cast<Item_func *>(item);
    using FilterExpr = LineairDB::Protocol::FilterExpr;
    FilterExpr::Op op;
    switch (function->functype()) {
      case Item_func::PLUS_FUNC:
        op = FilterExpr::OP_ADD;
        break;
      case Item_func::MINUS_FUNC:
        op = FilterExpr::OP_SUB;
        break;
      case Item_func::MUL_FUNC:
        op = FilterExpr::OP_MUL;
        break;
      case Item_func::DIV_FUNC:
        op = FilterExpr::OP_DIV;
        break;
      case Item_func::NEG_FUNC:
        op = FilterExpr::OP_NEG;
        break;
      default:
        return false;
    }

    expr->set_op(op);
    for (uint arg_idx = 0; arg_idx < function->argument_count(); arg_idx++) {
      if (!serialize_output_expression(function->arguments()[arg_idx],
                                       expr->add_children())) {
        return false;
      }
    }
    return true;
  };

  // The response must match MySQL's visible SELECT list exactly: grouped
  // columns refer back to group keys, aggregate items refer to aggregate slots,
  // and arithmetic over aggregate items becomes an output expression.
  for (Item *item : output_items) {
    Item *real = item->real_item();
    auto *output = request.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *raw_output_field = down_cast<Item_field *>(real)->field;
      const int output_table = TableIndexOfField(raw_output_field, tables);
      const Field *output_field =
          output_table >= 0
              ? resolve_query_field(raw_output_field, output_table)
              : nullptr;
      if (output_field == nullptr)
        LDB_COL_REJECT("output column unresolvable");

      int group_position = -1;
      for (size_t i = 0; i < group_fields.size(); i++) {
        if (group_fields[i].field != nullptr &&
            group_fields[i].table == output_table &&
            group_fields[i].field == output_field) {
          group_position = static_cast<int>(i);
          break;
        }
      }
      if (group_position < 0)
        LDB_COL_REJECT("output field not a group column");

      output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::GROUP);
      output->set_ordinal(group_position);
      continue;
    }

    if (real->type() == Item::SUM_FUNC_ITEM) {
      const int aggregate_ordinal =
          register_aggregate(down_cast<Item_sum *>(real));
      if (aggregate_ordinal < 0) {
        LDB_COL_REJECT(aggregate_error != nullptr ? aggregate_error
                                                  : "aggregate");
      }
      output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::AGG);
      output->set_ordinal(static_cast<uint32_t>(aggregate_ordinal));
      continue;
    }

    {
      int group_position = -1;
      for (size_t group_idx = 0; group_idx < group_items.size();
           group_idx++) {
        if (group_items[group_idx] == real ||
            group_items[group_idx]->eq(real, true)) {
          group_position = static_cast<int>(group_idx);
          break;
        }
      }
      if (group_position >= 0) {
        output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::GROUP);
        output->set_ordinal(group_position);
        continue;
      }
    }

    if (!serialize_output_expression(real, output->mutable_expr())) {
      LDB_COL_REJECT(aggregate_error != nullptr ? aggregate_error
                                                : "output expression");
    }
    output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::EXPR);
    output->set_result_scale(real->decimals);
  }

  if (qb->having_cond() != nullptr) {
    std::function<bool(Item *, LineairDB::Protocol::FilterExpr *)>
        serialize_having;
    serialize_having =
        [&](Item *item, LineairDB::Protocol::FilterExpr *expr) -> bool {
      item = item->real_item();
      if (item->type() == Item::COND_ITEM) {
        auto *condition = down_cast<Item_cond *>(item);
        expr->set_op(condition->functype() == Item_func::COND_AND_FUNC
                         ? LineairDB::Protocol::FilterExpr::OP_AND
                         : LineairDB::Protocol::FilterExpr::OP_OR);
        List_iterator<Item> iterator(*condition->argument_list());
        for (Item *part = iterator++; part != nullptr; part = iterator++) {
          if (!serialize_having(part, expr->add_children())) return false;
        }
        return true;
      }
      if (item->type() != Item::FUNC_ITEM) return false;

      auto *function = down_cast<Item_func *>(item);
      LineairDB::Protocol::FilterExpr::Op op;
      switch (function->functype()) {
        case Item_func::EQ_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_EQ;
          break;
        case Item_func::NE_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_NE;
          break;
        case Item_func::LT_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_LT;
          break;
        case Item_func::LE_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_LE;
          break;
        case Item_func::GT_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_GT;
          break;
        case Item_func::GE_FUNC:
          op = LineairDB::Protocol::FilterExpr::OP_GE;
          break;
        default:
          return false;
      }
      if (function->argument_count() != 2) return false;

      expr->set_op(op);
      for (uint arg_idx = 0; arg_idx < 2; arg_idx++) {
        auto *child = expr->add_children();
        if (!serialize_output_expression(function->arguments()[arg_idx],
                                         child)) {
          return false;
        }
        child->set_compare_type(2);
      }
      return true;
    };

    auto *having = aggregate->mutable_having();
    if (!serialize_having(qb->having_cond(), having->mutable_expr())) {
      LDB_COL_REJECT("HAVING not pushable");
    }
    having->set_num_columns(aggregate->group_columns_size() +
                            aggregate->aggs_size());
  }

  if (aggregate->aggs_size() == 0 && aggregate->group_columns_size() == 0)
    LDB_COL_REJECT("no aggregates");

  for (ORDER *order = qb->order_list.first; order != nullptr;
       order = order->next) {
    const int output_ordinal =
        OrderOutputOrdinal(*order->item, output_items);
    if (output_ordinal < 0)
      LDB_COL_REJECT("ORDER BY not an output column");

    auto *sort_key = request.add_order_by();
    sort_key->set_output_ordinal(output_ordinal);
    sort_key->set_descending(order->direction == ORDER_DESC);
    Item *output_item = output_items[output_ordinal]->real_item();
    sort_key->set_cmp_kind(output_item->result_type() == STRING_RESULT ? 1 : 0);
  }

  if (qb->has_limit()) {
    // LIMIT without ORDER BY picks arbitrary rows: the row engine and this
    // executor (whose group-map iteration order is an implementation detail)
    // may legally return different rows, so reject loudly instead of
    // silently diverging.
    if (qb->order_list.first == nullptr &&
        unit->select_limit_cnt != HA_POS_ERROR)
      LDB_COL_REJECT("LIMIT without ORDER BY");
    if (unit->select_limit_cnt != HA_POS_ERROR)
      request.set_limit(unit->select_limit_cnt);
    if (unit->offset_limit_cnt > 0) {
      request.set_offset(unit->offset_limit_cnt);
      if (request.limit() > 0)
        request.set_limit(request.limit() - unit->offset_limit_cnt);
    }
  }

  *why = nullptr;
  return true;

#undef LDB_COL_REJECT
}

/**
 * @brief Recognize aggregate query blocks supported by LINEAIRDB_COLUMNAR.
 *
 * Unsupported shapes return false and set `why`; callers convert that into a
 * secondary-engine reject that may fall back to the primary engine when the
 * session allows it.
 */
bool RecognizeQueryBlock(JOIN *join, ColumnarExecutionContext *ctx,
                         const char **why) {
  ctx->query_block_request.Clear();
  ctx->query_block_ready = false;

  Query_block *qb = join == nullptr ? nullptr : join->query_block;
  if (qb == nullptr || qb->outer_query_block() != nullptr) {
    *why = "not top-level";
    return false;
  }

  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr || !unit->is_simple()) {
    *why = "not simple unit";
    return false;
  }

  if (qb->leaf_tables != nullptr && qb->leaf_tables->next_leaf == nullptr &&
      qb->leaf_tables->is_view_or_derived()) {
    if (RecognizeDerivedRegroup(join, ctx, why)) return true;
    if (RecognizeFlattenedAggregate(join, ctx, why)) return true;
  }

  if (!BuildQueryBlockRequest(join, qb, &ctx->query_block_request, why,
                              join->root_access_path()) &&
      !BuildQueryBlockRequest(join, qb, &ctx->query_block_request, why,
                              nullptr)) {
    return false;
  }
  ctx->query_block_ready = true;
  return true;
}

/**
 * @brief Execute a recognized aggregate block through LineairDB.
 *
 * MySQL has already sent result-set metadata for the original SELECT list. This
 * override runs one query-block RPC and sends value-only Item carriers
 * that match that already-described metadata.
 */
bool ExecuteColumnarAggregate(JOIN *join, Query_result *result) {
  THD *thd = join->thd;
  auto *ctx = static_cast<ColumnarExecutionContext *>(
      thd->lex->secondary_engine_execution_context());
  if (ctx == nullptr || !ctx->query_block_ready) {
    return RaiseColumnarError(thd, "LINEAIRDB_COLUMNAR: no offload plan");
  }

  std::shared_ptr<LineairDBProxy> proxy = lineairdb::acquire_shared_proxy(thd);
  if (!proxy) {
    return RaiseColumnarError(thd, "LINEAIRDB_COLUMNAR: no server connection");
  }

  LineairDB::Protocol::TxExecuteQueryBlock::Response rpc;
  if (!proxy->tx_execute_query_block(ctx->query_block_request, &rpc) ||
      !rpc.ok()) {
    char message[192];
    snprintf(message, sizeof(message), "LINEAIRDB_COLUMNAR: %s",
             rpc.error().empty() ? "query block RPC failed"
                                 : rpc.error().c_str());
    return RaiseColumnarError(thd, message);
  }

  mem_root_deque<Item *> output_items(thd->mem_root);
  std::vector<ItemColumnarValue *> values;
  for (Item *item : VisibleFields(join->query_block->fields)) {
    auto *value = new (thd->mem_root) ItemColumnarValue(item);
    if (value == nullptr) return true;
    values.push_back(value);
    output_items.push_back(value);
  }

  std::vector<DecodedField> fields;
  const size_t expected = 1 + values.size();
  for (const std::string &row : rpc.rows()) {
    if (!DecodeRowFields(row, &fields) || fields.size() != expected) {
      return RaiseColumnarError(
          thd, "LINEAIRDB_COLUMNAR: malformed query-block row");
    }

    // Field 0 is a placeholder for the proxy row null-flags field. The server
    // emits one following field per SELECT output item.
    for (size_t i = 0; i < values.size(); i++) {
      const DecodedField &field = fields[1 + i];
      if (!field.empty) {
        values[i]->set_value(field.ptr, field.len);
        continue;
      }
      values[i]->set_null_value();
    }

    if (result->send_data(thd, output_items)) return true;
    ++join->send_records;
  }

  return false;
}

bool PrepareSecondaryEngine(THD *thd, LEX *lex) {
  SetColumnarFailReason(thd, nullptr);
  lex->add_statement_options(OPTION_NO_CONST_TABLES |
                             OPTION_NO_SUBQUERY_DURING_OPTIMIZATION);

  auto *ctx = new (thd->mem_root) ColumnarExecutionContext;
  if (ctx == nullptr) return true;
  lex->set_secondary_engine_execution_context(ctx);
  return false;
}

bool OptimizeSecondaryEngine(THD *, LEX *lex) {
  SetColumnarFailReason(lex->thd, nullptr);
  auto *ctx = static_cast<ColumnarExecutionContext *>(
      lex->secondary_engine_execution_context());
  if (ctx == nullptr) {
    return RaiseColumnarError(
        lex->thd, "LINEAIRDB_COLUMNAR statement context is not available");
  }

  Query_block *query_block = lex->unit->first_query_block();
  JOIN *join = query_block != nullptr ? query_block->join : nullptr;
  const char *why = "no JOIN";
  if (join == nullptr || !RecognizeQueryBlock(join, ctx, &why)) {
    char message[128];
    snprintf(message, sizeof(message),
             "LINEAIRDB_COLUMNAR unsupported shape: %s", why ? why : "?");
    return RaiseColumnarError(lex->thd, message);
  }

  join->override_executor_func = ExecuteColumnarAggregate;
  return false;
}

bool ModifyAccessPathCost(THD *thd [[maybe_unused]],
                          const JoinHypergraph &hypergraph [[maybe_unused]],
                          AccessPath *path) {
  switch (path->type) {
    case AccessPath::NESTED_LOOP_JOIN:
    case AccessPath::BKA_JOIN:
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
    case AccessPath::EQ_REF:
    case AccessPath::REF:
    case AccessPath::REF_OR_NULL:
    case AccessPath::INDEX_SCAN:
    case AccessPath::INDEX_RANGE_SCAN: {
      constexpr double kPenalty = 100.0;
      path->cost = std::max(path->cost, 0.0) * kPenalty + 1.0;
      path->cost_before_filter = path->cost;
      if (path->init_cost >= 0.0) path->init_cost *= kPenalty;
      return false;
    }

    case AccessPath::HASH_JOIN: {
      const double build =
          std::max(1.0, path->hash_join().inner->num_output_rows());
      const double probe =
          std::max(1.0, path->hash_join().outer->num_output_rows());
      const double output = std::max(1.0, path->num_output_rows());
      const double cost = path->hash_join().outer->cost +
                          path->hash_join().inner->cost +
                          0.05 * (build + probe) + 0.01 * output;
      path->cost = cost;
      path->cost_before_filter = cost;
      path->init_cost = path->hash_join().inner->cost + 0.05 * build;
      return false;
    }

    case AccessPath::TABLE_SCAN: {
      const double rows = std::max(1.0, path->num_output_rows());
      const double cost = 0.01 * rows;
      path->cost = cost;
      path->cost_before_filter = cost;
      path->init_cost = 0.0;
      return false;
    }

    default:
      return false;
  }
}

bool CompareJoinCost(THD *thd, const JOIN &join, double optimizer_cost,
                     bool *use_best_so_far, bool *cheaper,
                     double *secondary_engine_cost) {
  *use_best_so_far = false;
  *secondary_engine_cost = optimizer_cost;

  // A quantified subquery (IN/ALL/ANY) that survives to the secondary
  // optimization pass reaches subquery-materialization costing, which
  // crashes on plans produced for secondary tables. Antijoin and derived
  // rewrites are unavailable on this pass for such shapes (nullable NOT IN),
  // so reject them here, before the costing runs. EXISTS and scalar
  // subqueries resolve to the EXISTS strategy and never reach that costing.
  const Item_subselect *subquery_item = join.query_expression()->item;
  if (subquery_item != nullptr &&
      (subquery_item->substype() == Item_subselect::IN_SUBS ||
       subquery_item->substype() == Item_subselect::ALL_SUBS ||
       subquery_item->substype() == Item_subselect::ANY_SUBS)) {
    return RaiseColumnarError(
        thd, "LINEAIRDB_COLUMNAR unsupported shape: quantified subquery");
  }

  auto *ctx = static_cast<ColumnarExecutionContext *>(
      thd->lex->secondary_engine_execution_context());
  if (ctx == nullptr) return true;
  *cheaper = ctx->BestPlanSoFar(join, optimizer_cost);
  return false;
}

handler *CreateColumnarHandler(handlerton *hton, TABLE_SHARE *table_share,
                               bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_lineairdb_columnar(hton, table_share);
}

}  // namespace

ha_lineairdb_columnar::ha_lineairdb_columnar(handlerton *hton,
                                             TABLE_SHARE *table_share_arg)
    : handler(hton, table_share_arg) {}

int ha_lineairdb_columnar::open(const char *, int, unsigned int,
                                const dd::Table *) {
  THR_LOCK *lock =
      loaded_tables->lock(table_share->db.str, table_share->table_name.str);
  if (lock == nullptr) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), "Table has not been loaded");
    return HA_ERR_GENERIC;
  }

  thr_lock_data_init(lock, &lock_data_, nullptr);
  return 0;
}

int ha_lineairdb_columnar::info(unsigned int flags) {
  // Statistics come from the primary engine when it is available.
  handler *primary = ha_get_primary_handler();
  if (primary == nullptr) return 0;

  const int error = primary->info(flags);
  if (error != 0) return error;

  stats.records = primary->stats.records;

  // Join selectivity is estimated against the secondary TABLE, so copy the
  // primary handler's refreshed index cardinality onto this TABLE instance.
  if (table != nullptr) {
    const TABLE *primary_table = nullptr;
    THD *thd = ha_thd();
    for (TABLE *candidate = thd != nullptr ? thd->open_tables : nullptr;
         candidate != nullptr; candidate = candidate->next) {
      if (candidate->file == primary) {
        primary_table = candidate;
        break;
      }
    }

    if (primary_table != nullptr && table->s->keys == primary_table->s->keys) {
      for (uint key_idx = 0; key_idx < table->s->keys; key_idx++) {
        KEY &dst = table->key_info[key_idx];
        const KEY &src = primary_table->key_info[key_idx];
        if (dst.actual_key_parts != src.actual_key_parts) continue;

        for (uint part_idx = 0; part_idx < dst.actual_key_parts; part_idx++) {
          if (src.has_records_per_key(part_idx)) {
            dst.set_records_per_key(part_idx,
                                    src.records_per_key(part_idx));
          }
          if (dst.rec_per_key != nullptr && src.rec_per_key != nullptr) {
            dst.rec_per_key[part_idx] = src.rec_per_key[part_idx];
          }
        }
      }
    }
  }
  return 0;
}

ha_rows ha_lineairdb_columnar::records_in_range(unsigned int index,
                                                key_range *min_key,
                                                key_range *max_key) {
  handler *primary = ha_get_primary_handler();
  return primary == nullptr ? handler::records_in_range(index, min_key, max_key)
                            : primary->records_in_range(index, min_key,
                                                        max_key);
}

unsigned long ha_lineairdb_columnar::index_flags(unsigned int index,
                                                 unsigned int part,
                                                 bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(index, part, all_parts);

  // Indexes are available only to let the optimizer estimate primary ranges.
  return (HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags;
}

THR_LOCK_DATA **ha_lineairdb_columnar::store_lock(THD *, THR_LOCK_DATA **to,
                                                  thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && lock_data_.type == TL_UNLOCK)
    lock_data_.type = lock_type;
  *to++ = &lock_data_;
  return to;
}

int ha_lineairdb_columnar::load_table(const TABLE &table) {
  assert(table.file != nullptr);
  loaded_tables->add(table.s->db.str, table.s->table_name.str);
  return 0;
}

int ha_lineairdb_columnar::unload_table(const char *db_name,
                                        const char *table_name,
                                        bool error_if_not_loaded) {
  if (error_if_not_loaded &&
      !loaded_tables->contains(db_name, table_name)) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "Table is not loaded on a secondary engine");
    return 1;
  }

  loaded_tables->erase(db_name, table_name);
  return 0;
}

}  // namespace lineairdb_columnar

struct st_mysql_storage_engine lineairdb_columnar_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

int lineairdb_columnar_init(void *p) {
  lineairdb_columnar::loaded_tables = new lineairdb_columnar::LoadedTables();

  handlerton *hton = static_cast<handlerton *>(p);
  hton->create = lineairdb_columnar::CreateColumnarHandler;
  hton->state = SHOW_OPTION_YES;
  hton->flags = HTON_IS_SECONDARY_ENGINE;
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->prepare_secondary_engine = lineairdb_columnar::PrepareSecondaryEngine;
  hton->optimize_secondary_engine = lineairdb_columnar::OptimizeSecondaryEngine;
  hton->compare_secondary_engine_cost = lineairdb_columnar::CompareJoinCost;
  hton->secondary_engine_modify_access_path_cost =
      lineairdb_columnar::ModifyAccessPathCost;
  hton->get_secondary_engine_offload_or_exec_fail_reason =
      lineairdb_columnar::GetColumnarFailReason;
  hton->set_secondary_engine_offload_fail_reason =
      lineairdb_columnar::SetColumnarFailReason;
  hton->secondary_engine_flags =
      MakeSecondaryEngineFlags(SecondaryEngineFlag::SUPPORTS_HASH_JOIN,
                               SecondaryEngineFlag::SUPPORTS_NESTED_LOOP_JOIN);
  return 0;
}

int lineairdb_columnar_deinit(void *) {
  delete lineairdb_columnar::loaded_tables;
  lineairdb_columnar::loaded_tables = nullptr;
  return 0;
}
