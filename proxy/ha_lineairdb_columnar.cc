#include "ha_lineairdb_columnar.hh"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
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
#include "sql/item_sum.h"
#include "sql/item_timefunc.h"
#include "sql/mem_root_array.h"
#include "sql/query_result.h"
#include "sql/sql_const.h"
#include "sql/sql_class.h"
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
 * @brief Return true when raw-byte GROUP BY keys match MySQL equality.
 *
 * DECIMAL cells are stored as canonical base-10 text in PAX, so equal values
 * have equal bytes within one schema.
 */
bool GroupColumnIsBinarySafe(const Field *field) {
  switch (field->result_type()) {
    case INT_RESULT:
    case DECIMAL_RESULT:
      return true;
    case STRING_RESULT:
      return field->binary();
    default:
      return false;
  }
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
  return predicate;
}

struct QueryBlockTable {
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
    return serialize_item(filters[0], predicate->mutable_expr());
  }

  auto *root = predicate->mutable_expr();
  root->set_op(LineairDB::Protocol::FilterExpr::OP_AND);
  for (Item *filter : filters) {
    if (!serialize_item(filter, root->add_children())) return false;
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
    if (preserved_key->result_type() != INT_RESULT ||
        nullable_key->result_type() != INT_RESULT) {
      LDB_COL_REJECT("inner join key type");
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
      !GroupColumnIsBinarySafe(inner_group_field)) {
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
 * @brief Recognize aggregate query blocks supported by LINEAIRDB_COLUMNAR.
 *
 * Unsupported shapes return false and set `why`; callers convert that into a
 * secondary-engine reject that may fall back to the primary engine when the
 * session allows it.
 */
bool RecognizeQueryBlock(JOIN *join, ColumnarExecutionContext *ctx,
                         const char **why) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)

  ctx->query_block_request.Clear();
  ctx->query_block_ready = false;

  Query_block *qb = join->query_block;
  if (qb == nullptr || qb->outer_query_block() != nullptr)
    LDB_COL_REJECT("not top-level");

  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr || !unit->is_simple())
    LDB_COL_REJECT("not simple unit");

  if (qb->leaf_tables != nullptr && qb->leaf_tables->next_leaf == nullptr &&
      qb->leaf_tables->is_view_or_derived()) {
    return RecognizeDerivedRegroup(join, ctx, why);
  }

  if (qb->having_cond() != nullptr) LDB_COL_REJECT("has HAVING");
  if (qb->is_distinct()) LDB_COL_REJECT("has DISTINCT");
  if (qb->has_windows()) LDB_COL_REJECT("has windows");
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) LDB_COL_REJECT("has ROLLUP");
  if (!join->implicit_grouping && qb->group_list.elements == 0)
    LDB_COL_REJECT("no aggregation");

  // The request table order is the stable numbering used by scan, join,
  // grouping, and aggregate argument references.
  std::vector<QueryBlockTable> tables;
  for (Table_ref *table_ref = qb->leaf_tables; table_ref != nullptr;
       table_ref = table_ref->next_leaf) {
    TABLE *table = table_ref->table;
    if (table == nullptr || table->s == nullptr) LDB_COL_REJECT("no TABLE");
    if (table_ref->outer_join) LDB_COL_REJECT("outer join");
    if (!loaded_tables->contains(table->s->db.str, table->s->table_name.str))
      LDB_COL_REJECT("not SECONDARY_LOADed");
    tables.push_back({table, table_ref->map()});
  }
  if (tables.empty()) LDB_COL_REJECT("no tables");

  struct JoinEdge {
    int left_table = -1;
    int right_table = -1;
    const Field *left_field = nullptr;
    const Field *right_field = nullptr;
  };

  std::vector<std::vector<Item *>> table_filters(tables.size());
  std::vector<JoinEdge> join_edges;
  Item *where_cond =
      qb->where_cond() != nullptr ? qb->where_cond() : join->where_cond;
  std::vector<Item *> predicates;
  FlattenAnd(where_cond, &predicates);

  // Local predicates become scan filters. Cross-table integer equalities become
  // join edges; other cross-table shapes stay on the primary engine path.
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
      LDB_COL_REJECT("non-equi join predicate");
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
    if (left_raw->result_type() != INT_RESULT ||
        right_raw->result_type() != INT_RESULT) {
      LDB_COL_REJECT("non-integer join key");
    }

    const Field *left_field =
        ResolveBaseField(left_raw, tables[left_table].table);
    const Field *right_field =
        ResolveBaseField(right_raw, tables[right_table].table);
    if (left_field == nullptr || right_field == nullptr) {
      LDB_COL_REJECT("join key unresolvable");
    }
    join_edges.push_back({left_table, right_table, left_field, right_field});
  }
  if (tables.size() > 1 && join_edges.empty()) LDB_COL_REJECT("cross join");

  // Scan every table once, attaching only predicates that read that table.
  auto &request = ctx->query_block_request;
  std::vector<int> scan_nodes(tables.size(), -1);
  for (size_t table_idx = 0; table_idx < tables.size(); table_idx++) {
    TABLE *table = tables[table_idx].table;
    request.add_tables()->set_table_name(table->s->normalized_path.str);
    auto *scan_node = request.add_nodes();
    auto *scan = scan_node->mutable_scan();
    scan->set_table_idx(static_cast<uint32_t>(table_idx));
    scan_nodes[table_idx] = request.nodes_size() - 1;
    if (!table_filters[table_idx].empty() &&
        !SerializeTableFilters(table_filters[table_idx], table,
                               scan->mutable_filter())) {
      LDB_COL_REJECT("filter not pushable");
    }
  }

  // Build a left-deep INNER join tree by walking FROM order and retrying tables
  // whose equality edges are not connected yet.
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
    if (table_idx < 0) LDB_COL_REJECT("disconnected join graph");
    pending_tables.erase(pending_tables.begin() + pending_idx);

    bool connected = false;
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

  auto *aggregate_node = request.add_nodes();
  auto *aggregate = aggregate_node->mutable_aggregate();
  aggregate->set_input(current_node);
  struct GroupField {
    int table = -1;
    const Field *field = nullptr;
  };
  std::vector<GroupField> group_fields;
  std::vector<Item *> group_items;
  for (ORDER *group = qb->group_list.first; group != nullptr;
       group = group->next) {
    Item *group_item = (*group->item)->real_item();
    group_items.push_back(group_item);
    if (const Field *year_field = ExtractYearField(group_item)) {
      const int group_table = TableIndexOfField(year_field, tables);
      const Field *group_field =
          group_table >= 0
              ? ResolveBaseField(year_field, tables[group_table].table)
              : nullptr;
      if (group_field == nullptr || group_field->is_nullable()) {
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

    if (group_item->type() != Item::FIELD_ITEM)
      LDB_COL_REJECT("group item not a column");

    const Field *raw_group_field =
        down_cast<Item_field *>(group_item)->field;
    const int group_table = TableIndexOfField(raw_group_field, tables);
    if (group_table < 0) LDB_COL_REJECT("group column foreign");
    const Field *group_field =
        ResolveBaseField(raw_group_field, tables[group_table].table);
    if (group_field == nullptr) LDB_COL_REJECT("group column foreign");
    if (group_field->is_nullable() ||
        !GroupColumnIsBinarySafe(group_field)) {
      LDB_COL_REJECT("group column not binary-safe");
    }

    auto *group_column = aggregate->add_group_columns();
    group_column->set_table_idx(static_cast<uint32_t>(group_table));
    group_column->set_column(group_field->field_index());
    group_column->set_cmp_kind(group_field->result_type() == STRING_RESULT ? 1
                                                                           : 0);
    group_fields.push_back({group_table, group_field});
  }

  std::vector<Item *> output_items;
  for (Item *item : VisibleFields(qb->fields)) output_items.push_back(item);

  // The response must match MySQL's visible SELECT list exactly: grouped
  // columns refer back to group keys, aggregate items refer to aggregate slots.
  for (Item *item : output_items) {
    Item *real = item->real_item();
    auto *output = request.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *raw_output_field = down_cast<Item_field *>(real)->field;
      const int output_table = TableIndexOfField(raw_output_field, tables);
      const Field *output_field =
          output_table >= 0
              ? ResolveBaseField(raw_output_field,
                                 tables[output_table].table)
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

    if (real->type() != Item::SUM_FUNC_ITEM) {
      int group_position = -1;
      for (size_t group_idx = 0; group_idx < group_items.size();
           group_idx++) {
        if (group_items[group_idx] == real ||
            group_items[group_idx]->eq(real, true)) {
          group_position = static_cast<int>(group_idx);
          break;
        }
      }
      if (group_position < 0) LDB_COL_REJECT("output not aggregate");
      output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::GROUP);
      output->set_ordinal(group_position);
      continue;
    }

    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->argument_count() > 1) LDB_COL_REJECT("aggregate arg count");

    auto *function = aggregate->add_aggs();
    switch (sum->sum_func()) {
      case Item_sum::COUNT_FUNC: {
        function->set_arg_table(0);
        if (sum->argument_count() > 0) {
          Item *arg = sum->get_arg(0)->real_item();
          if (arg->const_item()) {
            if (arg->is_nullable() || arg->is_null())
              LDB_COL_REJECT("COUNT const nullable");
          } else {
            if (arg->type() != Item::FIELD_ITEM)
              LDB_COL_REJECT("COUNT arg shape");
            const Field *raw_count_field =
                down_cast<Item_field *>(arg)->field;
            const int count_table = TableIndexOfField(raw_count_field, tables);
            const Field *count_field =
                count_table >= 0
                    ? ResolveBaseField(raw_count_field,
                                       tables[count_table].table)
                    : nullptr;
            if (count_field == nullptr || count_field->is_nullable())
              LDB_COL_REJECT("COUNT arg nullable");
            function->set_arg_table(static_cast<uint32_t>(count_table));
            auto *ref = function->mutable_arg();
            ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
            ref->set_column_index(count_field->field_index());
          }
        }

        function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
        break;
      }

      case Item_sum::SUM_FUNC:
      case Item_sum::AVG_FUNC: {
        if (sum->argument_count() != 1) LDB_COL_REJECT("aggregate arg count");
        Item *arg = sum->get_arg(0)->real_item();
        if (sum->sum_func() == Item_sum::SUM_FUNC) {
          if (Item *filter = CaseToCountFilter(arg)) {
            const int filter_table = SingleTableOf(
                filter->used_tables() & ~PSEUDO_TABLE_BITS, tables);
            if (filter_table < 0) LDB_COL_REJECT("CASE filter tables");
            auto *predicate = function->mutable_filter();
            predicate->set_num_columns(tables[filter_table].table->s->fields);
            if (!serialize_item(filter, predicate->mutable_expr()))
              LDB_COL_REJECT("CASE filter not pushable");
            function->set_arg_table(static_cast<uint32_t>(filter_table));
            function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
            break;
          }
        }
        if (arg->result_type() != DECIMAL_RESULT &&
            arg->result_type() != INT_RESULT) {
          LDB_COL_REJECT("aggregate arg type");
        }

        int argument_table =
            SingleTableOf(arg->used_tables() & ~PSEUDO_TABLE_BITS, tables);
        if (argument_table >= 0 && arg->type() == Item::FIELD_ITEM) {
          const Field *raw_argument_field =
              down_cast<Item_field *>(arg)->field;
          const Field *argument_field = ResolveBaseField(
              raw_argument_field, tables[argument_table].table);
          if (argument_field == nullptr)
            LDB_COL_REJECT("aggregate arg unresolvable");

          auto *ref = function->mutable_arg();
          ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
          ref->set_column_index(argument_field->field_index());
        } else if (argument_table >= 0) {
          if (!lineairdb::serialize_aggregate_expression(
                  arg, function->mutable_arg())) {
            LDB_COL_REJECT("aggregate expr not pushable");
          }
        } else {
          argument_table = 0;
          if (!SerializeAggregateExpressionForTables(
                  arg, tables, static_cast<uint32_t>(argument_table),
                  function->mutable_arg())) {
            LDB_COL_REJECT("aggregate expr not pushable");
          }
        }

        function->set_arg_table(static_cast<uint32_t>(argument_table));
        function->set_kind(
            sum->sum_func() == Item_sum::SUM_FUNC
                ? LineairDB::Protocol::QueryBlockAggFunc::SUM
                : LineairDB::Protocol::QueryBlockAggFunc::AVG);
        function->set_arg_scale(arg->decimals);
        break;
      }

      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        if (sum->argument_count() != 1) LDB_COL_REJECT("aggregate arg count");
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->type() != Item::FIELD_ITEM) LDB_COL_REJECT("minmax arg");
        const Field *raw_argument_field =
            down_cast<Item_field *>(arg)->field;
        const int argument_table =
            TableIndexOfField(raw_argument_field, tables);
        const Field *argument_field =
            argument_table >= 0
                ? ResolveBaseField(raw_argument_field,
                                   tables[argument_table].table)
                : nullptr;
        if (argument_field == nullptr) LDB_COL_REJECT("minmax unresolvable");

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
        break;
      }

      default:
        LDB_COL_REJECT("aggregate kind unsupported");
    }

    output->set_source(LineairDB::Protocol::QueryBlockOutputExpr::AGG);
    output->set_ordinal(aggregate->aggs_size() - 1);
  }

  if (aggregate->aggs_size() == 0) LDB_COL_REJECT("no aggregates");

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
    if (unit->select_limit_cnt != HA_POS_ERROR)
      request.set_limit(unit->select_limit_cnt);
    if (unit->offset_limit_cnt > 0) {
      request.set_offset(unit->offset_limit_cnt);
      if (request.limit() > 0)
        request.set_limit(request.limit() - unit->offset_limit_cnt);
    }
  }

  ctx->query_block_ready = true;

  *why = nullptr;
  return true;

#undef LDB_COL_REJECT
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

bool CompareJoinCost(THD *thd, const JOIN &join, double optimizer_cost,
                     bool *use_best_so_far, bool *cheaper,
                     double *secondary_engine_cost) {
  *use_best_so_far = false;
  *secondary_engine_cost = optimizer_cost;

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
  if (error == 0) stats.records = primary->stats.records;
  return error;
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
