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
 */
bool GroupColumnIsBinarySafe(const Field *field) {
  switch (field->result_type()) {
    case INT_RESULT:
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
                       const std::vector<Item *> &output_items, TABLE *table) {
  Item *order_real = order_item->real_item();
  for (size_t i = 0; i < output_items.size(); i++) {
    Item *output_real = output_items[i]->real_item();
    if (output_real == order_real) return static_cast<int>(i);
    if (order_real->type() == Item::FIELD_ITEM &&
        output_real->type() == Item::FIELD_ITEM) {
      const Field *order_field = ResolveBaseField(
          down_cast<Item_field *>(order_real)->field, table);
      const Field *output_field = ResolveBaseField(
          down_cast<Item_field *>(output_real)->field, table);
      if (order_field != nullptr && order_field == output_field) {
        return static_cast<int>(i);
      }
    }
  }
  return -1;
}

/**
 * @brief Recognize single-table aggregate blocks supported by LINEAIRDB_COLUMNAR.
 *
 * Unsupported shapes return false and set `why`; callers convert that into a
 * secondary-engine reject that may fall back to the primary engine when the
 * session allows it.
 */
bool RecognizeSingleTableAggregate(JOIN *join, ColumnarExecutionContext *ctx,
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

  Table_ref *table_ref = qb->leaf_tables;
  if (table_ref == nullptr || table_ref->next_leaf != nullptr)
    LDB_COL_REJECT("not single table");

  TABLE *table = table_ref->table;
  if (table == nullptr || table->s == nullptr) LDB_COL_REJECT("no TABLE");

  if (qb->having_cond() != nullptr) LDB_COL_REJECT("has HAVING");
  if (qb->is_distinct()) LDB_COL_REJECT("has DISTINCT");
  if (qb->has_windows()) LDB_COL_REJECT("has windows");
  if (qb->olap != UNSPECIFIED_OLAP_TYPE) LDB_COL_REJECT("has ROLLUP");
  if (!join->implicit_grouping && qb->group_list.elements == 0)
    LDB_COL_REJECT("no aggregation");

  if (!loaded_tables->contains(table->s->db.str, table->s->table_name.str))
    LDB_COL_REJECT("not SECONDARY_LOADed");

  auto &request = ctx->query_block_request;
  request.add_tables()->set_table_name(table->s->normalized_path.str);

  auto *scan_node = request.add_nodes();
  auto *scan = scan_node->mutable_scan();
  scan->set_table_idx(0);
  Item *where_cond =
      qb->where_cond() != nullptr ? qb->where_cond() : join->where_cond;
  if (where_cond != nullptr) {
    auto *predicate = scan->mutable_filter();
    if (!serialize_item(where_cond, predicate->mutable_expr()))
      LDB_COL_REJECT("WHERE not pushable");
    predicate->set_num_columns(table->s->fields);
  }

  auto *aggregate_node = request.add_nodes();
  auto *aggregate = aggregate_node->mutable_aggregate();
  aggregate->set_input(0);
  std::vector<const Field *> group_fields;
  for (ORDER *group = qb->group_list.first; group != nullptr;
       group = group->next) {
    Item *group_item = (*group->item)->real_item();
    if (group_item->type() != Item::FIELD_ITEM)
      LDB_COL_REJECT("group item not a column");

    const Field *group_field = ResolveBaseField(
        down_cast<Item_field *>(group_item)->field, table);
    if (group_field == nullptr) LDB_COL_REJECT("group column foreign");
    if (group_field->is_nullable() ||
        !GroupColumnIsBinarySafe(group_field)) {
      LDB_COL_REJECT("group column not binary-safe");
    }

    auto *group_column = aggregate->add_group_columns();
    group_column->set_table_idx(0);
    group_column->set_column(group_field->field_index());
    group_column->set_cmp_kind(group_field->result_type() == STRING_RESULT ? 1
                                                                           : 0);
    group_fields.push_back(group_field);
  }

  std::vector<Item *> output_items;
  for (Item *item : VisibleFields(qb->fields)) output_items.push_back(item);

  for (Item *item : output_items) {
    Item *real = item->real_item();
    auto *output = request.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *output_field = ResolveBaseField(
          down_cast<Item_field *>(real)->field, table);
      if (output_field == nullptr)
        LDB_COL_REJECT("output column unresolvable");

      int group_position = -1;
      for (size_t i = 0; i < group_fields.size(); i++) {
        if (group_fields[i] == output_field) {
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

    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("output not aggregate");

    Item_sum *sum = down_cast<Item_sum *>(real);
    if (sum->argument_count() > 1) LDB_COL_REJECT("aggregate arg count");

    auto *function = aggregate->add_aggs();
    function->set_arg_table(0);

    switch (sum->sum_func()) {
      case Item_sum::COUNT_FUNC: {
        if (sum->argument_count() > 0) {
          Item *arg = sum->get_arg(0)->real_item();
          if (arg->const_item()) {
            if (arg->is_nullable() || arg->is_null())
              LDB_COL_REJECT("COUNT const nullable");
          } else {
            if (arg->type() != Item::FIELD_ITEM)
              LDB_COL_REJECT("COUNT arg shape");
            const Field *count_field = ResolveBaseField(
                down_cast<Item_field *>(arg)->field, table);
            if (count_field == nullptr || count_field->is_nullable())
              LDB_COL_REJECT("COUNT arg nullable");
          }
        }

        function->set_kind(LineairDB::Protocol::QueryBlockAggFunc::COUNT);
        break;
      }

      case Item_sum::SUM_FUNC:
      case Item_sum::AVG_FUNC: {
        if (sum->argument_count() != 1) LDB_COL_REJECT("aggregate arg count");
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->result_type() != DECIMAL_RESULT &&
            arg->result_type() != INT_RESULT) {
          LDB_COL_REJECT("aggregate arg type");
        }

        if (arg->type() == Item::FIELD_ITEM) {
          const Field *argument_field = ResolveBaseField(
              down_cast<Item_field *>(arg)->field, table);
          if (argument_field == nullptr)
            LDB_COL_REJECT("aggregate arg unresolvable");

          auto *ref = function->mutable_arg();
          ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
          ref->set_column_index(argument_field->field_index());
        } else {
          if (!lineairdb::serialize_aggregate_expression(
                  arg, function->mutable_arg())) {
            LDB_COL_REJECT("aggregate expr not pushable");
          }
        }

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
        const Field *argument_field = ResolveBaseField(
            down_cast<Item_field *>(arg)->field, table);
        if (argument_field == nullptr) LDB_COL_REJECT("minmax unresolvable");

        auto *ref = function->mutable_arg();
        ref->set_op(LineairDB::Protocol::FilterExpr::COLUMN_REF);
        ref->set_column_index(argument_field->field_index());
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
        OrderOutputOrdinal(*order->item, output_items, table);
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
  if (join == nullptr || !RecognizeSingleTableAggregate(join, ctx, &why)) {
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
