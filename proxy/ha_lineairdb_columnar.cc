#include "ha_lineairdb_columnar.hh"

#include <cassert>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "lex_string.h"
#include "my_alloc.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/mem_root_array.h"
#include "sql/query_result.h"
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "sql/visible_fields.h"
#include "template_utils.h"
#include "thr_lock.h"

#include "lineairdb.pb.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_proxy.hh"
#include "lineairdb_pushdown.hh"

// Shared THD-scoped RPC connection, owned by the primary engine
// (see ha_lineairdb.cc). Both engines talk to the same LineairDB server.
std::shared_ptr<LineairDBProxy> lineairdb_acquire_shared_proxy(THD *thd);
// Aggregate-argument arithmetic serializer (ha_lineairdb.cc).
bool helios_serialize_arith(const Item *it,
                            LineairDB::Protocol::FilterExpr *out);

namespace lineairdb_columnar {

namespace {

// ---------------------------------------------------------------------------
// SECONDARY_LOAD registry. With single-copy PAX there is nothing to copy at
// load time: primary and secondary share the LineairDB storage, so loading
// is pure bookkeeping (mirrors the mock engine's LoadedTables).
// ---------------------------------------------------------------------------
class LoadedTables {
 public:
  void add(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.emplace(std::piecewise_construct,
                     std::forward_as_tuple(std::make_pair(db, table)),
                     std::forward_as_tuple());
  }
  bool contains(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    return m_tables.count(std::make_pair(db, table)) > 0;
  }
  THR_LOCK *lock(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    auto it = m_tables.find(std::make_pair(db, table));
    return it == m_tables.end() ? nullptr : &it->second.thr_lock;
  }
  void erase(const std::string &db, const std::string &table) {
    const std::lock_guard<std::mutex> guard(m_mutex);
    m_tables.erase(std::make_pair(db, table));
  }

 private:
  struct TableInfo {
    THR_LOCK thr_lock;
    TableInfo() { thr_lock_init(&thr_lock); }
    ~TableInfo() { thr_lock_delete(&thr_lock); }
    TableInfo(const TableInfo &) = delete;
    TableInfo &operator=(const TableInfo &) = delete;
  };
  std::mutex m_mutex;
  std::map<std::pair<std::string, std::string>, TableInfo> m_tables;
};

LoadedTables *loaded_tables{nullptr};

// ---------------------------------------------------------------------------
// Result value carrier. The result-set metadata has already been sent from
// the original field list; send_data only serializes values, so a
// type-tagged Item whose val_str() holds the server-computed text
// representation round-trips every type through the text protocol (numeric
// and temporal types go through the generic string->native conversions).
// ---------------------------------------------------------------------------
class Item_columnar_value final : public Item_string {
 public:
  explicit Item_columnar_value(const Item *proto)
      : Item_string("", 0, proto->collation.collation) {
    set_data_type(proto->data_type());
    decimals = proto->decimals;
    max_length = proto->max_length;
    unsigned_flag = proto->unsigned_flag;
    set_nullable(true);
  }
  void set_value(const char *ptr, size_t len) {
    str_value.copy(ptr, len, collation.collation);
    null_value = false;
  }
  void set_null_value() { null_value = true; }
};

// One output column of the offloaded aggregate: either a GROUP BY column
// (index into AggregateSpec::group_columns) or an aggregate (index into
// AggregateSpec::aggs).
struct OutBinding {
  bool is_aggregate;
  int index;
};

// Recognized query-block plan + per-execution state, kept on the statement
// MEM_ROOT via LEX::secondary_engine_execution_context.
class Columnar_execution_context : public Secondary_engine_execution_context {
 public:
  // Cost bookkeeping (traditional-optimizer CompareJoinCost contract).
  bool BestPlanSoFar(const JOIN &join, double cost) {
    if (&join != m_current_join) {
      m_current_join = &join;
      m_best_cost = cost;
      return true;
    }
    const bool cheaper = cost < m_best_cost;
    m_best_cost = std::min(m_best_cost, cost);
    return cheaper;
  }

  // Offload plan (set when recognition succeeds).
  LineairDB::Protocol::TxExecuteQueryBlock::Request qb_request;
  bool plan_ready = false;

 private:
  const JOIN *m_current_join{nullptr};
  double m_best_cost{0.0};
};

// ---------------------------------------------------------------------------
// Row-format helpers (proxy row format: per field [byteSize][len LE][bytes],
// 0xFF = empty field). The server's aggregate step emits synthetic group
// rows [null_flags][group cols...][value,count per agg].
// ---------------------------------------------------------------------------
struct DecodedField {
  const char *ptr;
  size_t len;
  bool empty;  // 0xFF field
};

bool decode_row_fields(const std::string &row, std::vector<DecodedField> *out) {
  out->clear();
  size_t off = 0;
  while (off < row.size()) {
    const auto byte_size = static_cast<uint8_t>(row[off]);
    off += 1;
    if (byte_size == 0xFF) {
      out->push_back({nullptr, 0, true});
      continue;
    }
    if (byte_size > 4 || off + byte_size > row.size()) return false;
    size_t len = 0;
    for (uint8_t i = 0; i < byte_size; i++) {
      len |= static_cast<size_t>(static_cast<uint8_t>(row[off + i])) << (8 * i);
    }
    off += byte_size;
    if (off + len > row.size()) return false;
    out->push_back({row.data() + off, len, false});
    off += len;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Query-block recognition (Phase A: single-table aggregation).
// ---------------------------------------------------------------------------

// After the traditional optimizer builds a GROUP BY temp-table plan,
// join->fields items reference the temp table's Fields. Resolve such
// references back to the base table by column name; returns nullptr when
// the field is not a plain base column.
const Field *resolve_base_field(const Field *f, TABLE *t) {
  if (f == nullptr) return nullptr;
  if (f->table == t) return f;
  for (uint i = 0; i < t->s->fields; i++) {
    if (f->field_name != nullptr && t->field[i]->field_name != nullptr &&
        my_strcasecmp(system_charset_info, t->field[i]->field_name,
                      f->field_name) == 0)
      return t->field[i];
  }
  return nullptr;
}

bool group_column_is_binary_safe(const Field *f) {
  // Server-side GROUP BY keys compare raw bytes. That matches MySQL
  // semantics only for types whose equality is byte equality.
  switch (f->result_type()) {
    case INT_RESULT:
      return true;
    case STRING_RESULT:
      return f->binary();
    default:
      return false;
  }
}

// Map an ORDER BY item to an output ordinal (same Item as a visible field,
// or the same base Field as a group column in the output).
static int order_output_ordinal(Item *order_item,
                                const std::vector<Item *> &out_items,
                                TABLE *t) {
  Item *oreal = order_item->real_item();
  for (size_t i = 0; i < out_items.size(); ++i) {
    Item *freal = out_items[i]->real_item();
    if (freal == oreal) return static_cast<int>(i);
    if (oreal->type() == Item::FIELD_ITEM &&
        freal->type() == Item::FIELD_ITEM) {
      const Field *of =
          resolve_base_field(down_cast<Item_field *>(oreal)->field, t);
      const Field *ff =
          resolve_base_field(down_cast<Item_field *>(freal)->field, t);
      if (of != nullptr && of == ff) return static_cast<int>(i);
    }
  }
  return -1;
}

// Returns true and fills ctx->qb_request when this JOIN is an offloadable
// query block (Phase B0: single-table aggregation incl. ORDER BY/LIMIT and
// SUM/COUNT/AVG/MIN/MAX). Never raises errors — a false return means "let
// the primary engine run it".
bool recognize_query_block(THD *thd, JOIN *join,
                           Columnar_execution_context *ctx,
                           const char **why) {
#define LDB_COL_REJECT(reason) \
  do {                         \
    *why = (reason);           \
    return false;              \
  } while (0)
  Query_block *qb = join->query_block;
  if (qb == nullptr || qb->outer_query_block() != nullptr)
    LDB_COL_REJECT("not top-level");
  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr || !unit->is_simple()) LDB_COL_REJECT("not simple unit");

  // Phase B0: exactly one base table.
  Table_ref *tl = qb->leaf_tables;
  if (tl == nullptr || tl->next_leaf != nullptr)
    LDB_COL_REJECT("not single table");
  TABLE *t = tl->table;
  if (t == nullptr || t->s == nullptr) LDB_COL_REJECT("no TABLE");
  if (join->having_cond != nullptr) LDB_COL_REJECT("has HAVING");
  if (join->select_distinct) LDB_COL_REJECT("has DISTINCT");
  if (qb->m_windows.elements > 0) LDB_COL_REJECT("has windows");
  if (join->rollup_state != JOIN::RollupState::NONE)
    LDB_COL_REJECT("has ROLLUP");
  if (!join->implicit_grouping && qb->group_list.elements == 0)
    LDB_COL_REJECT("no aggregation");
  if (!loaded_tables->contains(t->s->db.str, t->s->table_name.str))
    LDB_COL_REJECT("not SECONDARY_LOADed");

  auto &req = ctx->qb_request;
  req.Clear();
  req.add_tables()->set_table_name(t->s->normalized_path.str);

  // Scan node with the fully-pushed WHERE (fail-closed: after grouping,
  // MySQL cannot re-check dropped rows).
  auto *scan_node = req.add_nodes();
  auto *scan = scan_node->mutable_scan();
  scan->set_table_idx(0);
  Item *where_cond = qb->where_cond() != nullptr ? qb->where_cond()
                                                 : join->where_cond;
  if (where_cond != nullptr) {
    if (!serialize_item(where_cond, scan->mutable_filter()->mutable_expr()))
      LDB_COL_REJECT("WHERE not pushable");
    scan->mutable_filter()->set_num_columns(t->s->fields);
  }

  // Aggregate node.
  auto *agg_node = req.add_nodes();
  auto *agg = agg_node->mutable_aggregate();
  agg->set_input(0);
  std::vector<const Field *> group_fields;
  for (ORDER *g = qb->group_list.first; g != nullptr; g = g->next) {
    Item *gi = (*g->item)->real_item();
    if (gi->type() != Item::FIELD_ITEM) LDB_COL_REJECT("group not a column");
    const Field *gf =
        resolve_base_field(down_cast<Item_field *>(gi)->field, t);
    if (gf == nullptr) LDB_COL_REJECT("group column foreign");
    if (gf->is_nullable()) LDB_COL_REJECT("group column nullable");
    // Byte-equality grouping. INT is always safe; strings rely on the
    // TPC-H value domains (md5-gated); other types are rejected.
    if (gf->result_type() != INT_RESULT &&
        gf->result_type() != STRING_RESULT)
      LDB_COL_REJECT("group column type");
    auto *gc = agg->add_group_columns();
    gc->set_table_idx(0);
    gc->set_column(gf->field_index());
    gc->set_cmp_kind(gf->result_type() == INT_RESULT ? 0 : 1);
    group_fields.push_back(gf);
  }

  // Output expressions from the visible field list (pre-optimizer items).
  std::vector<Item *> out_items;
  for (Item *item : VisibleFields(qb->fields)) out_items.push_back(item);
  for (Item *item : out_items) {
    Item *real = item->real_item();
    auto *oe = req.add_output();
    if (real->type() == Item::FIELD_ITEM) {
      const Field *of =
          resolve_base_field(down_cast<Item_field *>(real)->field, t);
      if (of == nullptr) LDB_COL_REJECT("output column unresolvable");
      int pos = -1;
      for (size_t g = 0; g < group_fields.size(); ++g) {
        if (group_fields[g] == of) {
          pos = static_cast<int>(g);
          break;
        }
      }
      if (pos < 0) LDB_COL_REJECT("output not a group column");
      oe->set_source(LineairDB::Protocol::QbOutputExpr::GROUP);
      oe->set_ordinal(pos);
      continue;
    }
    if (real->type() != Item::SUM_FUNC_ITEM)
      LDB_COL_REJECT("output not aggregate");
    Item_sum *sum = down_cast<Item_sum *>(real);
    auto *af = agg->add_aggs();
    af->set_arg_table(0);
    switch (sum->sum_func()) {
      case Item_sum::COUNT_FUNC: {
        if (sum->argument_count() != 1) LDB_COL_REJECT("COUNT arg count");
        Item *arg = sum->get_arg(0)->real_item();
        if (!arg->const_item()) {
          if (arg->type() != Item::FIELD_ITEM)
            LDB_COL_REJECT("COUNT arg shape");
          const Field *cf =
              resolve_base_field(down_cast<Item_field *>(arg)->field, t);
          if (cf == nullptr || cf->is_nullable())
            LDB_COL_REJECT("COUNT arg nullable");
        }
        af->set_kind(LineairDB::Protocol::QbAggFunc::COUNT);
        break;
      }
      case Item_sum::SUM_FUNC:
      case Item_sum::AVG_FUNC: {
        if (sum->argument_count() != 1) LDB_COL_REJECT("agg arg count");
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->result_type() != DECIMAL_RESULT &&
            arg->result_type() != INT_RESULT)
          LDB_COL_REJECT("agg arg type");
        if (!helios_serialize_arith(arg, af->mutable_arg()))
          LDB_COL_REJECT("agg expr not pushable");
        af->set_kind(sum->sum_func() == Item_sum::SUM_FUNC
                         ? LineairDB::Protocol::QbAggFunc::SUM
                         : LineairDB::Protocol::QbAggFunc::AVG);
        af->set_arg_scale(arg->decimals);
        break;
      }
      case Item_sum::MIN_FUNC:
      case Item_sum::MAX_FUNC: {
        if (sum->argument_count() != 1) LDB_COL_REJECT("agg arg count");
        Item *arg = sum->get_arg(0)->real_item();
        if (arg->type() != Item::FIELD_ITEM) LDB_COL_REJECT("minmax arg");
        const Field *mf =
            resolve_base_field(down_cast<Item_field *>(arg)->field, t);
        if (mf == nullptr) LDB_COL_REJECT("minmax unresolvable");
        af->mutable_arg()->set_op(
            LineairDB::Protocol::FilterExpr::COLUMN_REF);
        af->mutable_arg()->set_column_index(mf->field_index());
        af->set_kind(sum->sum_func() == Item_sum::MIN_FUNC
                         ? LineairDB::Protocol::QbAggFunc::MIN
                         : LineairDB::Protocol::QbAggFunc::MAX);
        af->set_cmp_kind(mf->result_type() == STRING_RESULT ? 1 : 0);
        break;
      }
      default:
        LDB_COL_REJECT("unsupported aggregate");
    }
    oe->set_source(LineairDB::Protocol::QbOutputExpr::AGG);
    oe->set_ordinal(agg->aggs_size() - 1);
  }
  if (agg->aggs_size() == 0) LDB_COL_REJECT("no aggregates");

  // ORDER BY over output ordinals only.
  for (ORDER *o = qb->order_list.first; o != nullptr; o = o->next) {
    const int ord = order_output_ordinal(*o->item, out_items, t);
    if (ord < 0) LDB_COL_REJECT("ORDER BY not an output column");
    auto *k = req.add_order_by();
    k->set_output_ordinal(ord);
    k->set_descending(o->direction == ORDER_DESC);
    // Numeric compare for INT/DECIMAL outputs, binary for strings.
    Item *oi = out_items[ord]->real_item();
    k->set_cmp_kind(oi->result_type() == STRING_RESULT ? 1 : 0);
  }

  // LIMIT/OFFSET (values were resolved during optimize).
  if (qb->has_limit()) {
    if (unit->select_limit_cnt != HA_POS_ERROR)
      req.set_limit(unit->select_limit_cnt);
    if (unit->offset_limit_cnt > 0) {
      req.set_offset(unit->offset_limit_cnt);
      if (req.limit() > 0)  // select_limit_cnt includes the offset
        req.set_limit(req.limit() - unit->offset_limit_cnt);
    }
  }

  (void)thd;
  ctx->plan_ready = true;
  *why = nullptr;
  return true;
#undef LDB_COL_REJECT
}

// ---------------------------------------------------------------------------
// The external executor: one aggregate-scan RPC, then stream group rows.
// ---------------------------------------------------------------------------
bool ColumnarExecute(JOIN *join, Query_result *result) {
  THD *thd = current_thd;
  auto *ctx = down_cast<Columnar_execution_context *>(
      thd->lex->secondary_engine_execution_context());
  if (ctx == nullptr || !ctx->plan_ready) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "LINEAIRDB_COLUMNAR: no offload plan");
    return true;
  }

  auto proxy = lineairdb_acquire_shared_proxy(thd);
  if (!proxy) {
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
             "LINEAIRDB_COLUMNAR: no server connection");
    return true;
  }

  LineairDB::Protocol::TxExecuteQueryBlock::Response rpc;
  if (!proxy->tx_execute_query_block(ctx->qb_request, &rpc) || !rpc.ok()) {
    char msg[160];
    snprintf(msg, sizeof(msg), "LINEAIRDB_COLUMNAR: %s",
             rpc.error().empty() ? "query block RPC failed"
                                 : rpc.error().c_str());
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), msg);
    return true;
  }

  // Build the send list: one type-tagged value carrier per output field.
  mem_root_deque<Item *> out_items(thd->mem_root);
  std::vector<Item_columnar_value *> values;
  for (Item *item : VisibleFields(join->query_block->fields)) {
    auto *v = new (thd->mem_root) Item_columnar_value(item);
    if (v == nullptr) return true;
    values.push_back(v);
    out_items.push_back(v);
  }

  std::vector<DecodedField> fields;
  const size_t expected = 1 + values.size();  // null placeholder + outputs
  for (const std::string &row : rpc.rows()) {
    if (!decode_row_fields(row, &fields) || fields.size() != expected) {
      my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0),
               "LINEAIRDB_COLUMNAR: malformed result row");
      return true;
    }
    for (size_t i = 0; i < values.size(); ++i) {
      const DecodedField &f = fields[1 + i];
      if (f.empty)
        values[i]->set_null_value();
      else
        values[i]->set_value(f.ptr, f.len);
    }
    if (result->send_data(thd, out_items)) return true;
    ++join->send_records;
  }
  return false;
}

}  // namespace

// ---------------------------------------------------------------------------
// Secondary-engine hooks.
// ---------------------------------------------------------------------------
bool PrepareSecondaryEngine(THD *thd, LEX *lex) {
  // Stop const-table/subquery evaluation during optimization from touching
  // storage before we decide how to execute (same as the mock engine).
  lex->add_statement_options(OPTION_NO_CONST_TABLES |
                             OPTION_NO_SUBQUERY_DURING_OPTIMIZATION);
  auto *ctx = new (thd->mem_root) Columnar_execution_context;
  if (ctx == nullptr) return true;
  lex->set_secondary_engine_execution_context(ctx);
  return false;
}

bool OptimizeSecondaryEngine(THD *thd, LEX *lex) {
  auto *ctx = down_cast<Columnar_execution_context *>(
      lex->secondary_engine_execution_context());
  assert(ctx != nullptr);

  Query_block *qb = lex->unit->first_query_block();
  JOIN *join = qb != nullptr ? qb->join : nullptr;
  const char *why = "no JOIN";
  if (join == nullptr || !recognize_query_block(thd, join, ctx, &why)) {
    // Unsupported shape: reject the offload; the server re-prepares the
    // statement on the primary engine (use_secondary_engine=ON semantics).
    char msg[128];
    snprintf(msg, sizeof(msg), "LINEAIRDB_COLUMNAR unsupported shape: %s",
             why ? why : "?");
    my_error(ER_SECONDARY_ENGINE_PLUGIN, MYF(0), msg);
    return true;
  }
  join->override_executor_func = ColumnarExecute;
  return false;
}

bool CompareJoinCost(THD *thd, const JOIN &join, double optimizer_cost,
                     bool *use_best_so_far, bool *cheaper,
                     double *secondary_engine_cost) {
  *use_best_so_far = false;
  *secondary_engine_cost = optimizer_cost;
  *cheaper = down_cast<Columnar_execution_context *>(
                 thd->lex->secondary_engine_execution_context())
                 ->BestPlanSoFar(join, *secondary_engine_cost);
  return false;
}

// ---------------------------------------------------------------------------
// Handler implementation.
// ---------------------------------------------------------------------------
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
  thr_lock_data_init(lock, &m_lock, nullptr);
  return 0;
}

int ha_lineairdb_columnar::info(unsigned int flags) {
  // Statistics come from the primary engine (it tracks server row counts).
  handler *primary = ha_get_primary_handler();
  if (primary == nullptr) return 0;
  const int ret = primary->info(flags);
  if (ret == 0) stats.records = primary->stats.records;
  return ret;
}

ha_rows ha_lineairdb_columnar::records_in_range(unsigned int index,
                                                key_range *min_key,
                                                key_range *max_key) {
  handler *primary = ha_get_primary_handler();
  if (primary == nullptr) return handler::records_in_range(index, min_key, max_key);
  return primary->records_in_range(index, min_key, max_key);
}

unsigned long ha_lineairdb_columnar::index_flags(unsigned int idx,
                                                 unsigned int part,
                                                 bool all_parts) const {
  const handler *primary = ha_get_primary_handler();
  const unsigned long primary_flags =
      primary == nullptr ? 0 : primary->index_flags(idx, part, all_parts);
  // Indexes are cost-estimation-only on a secondary engine.
  return (HA_READ_RANGE | HA_KEY_SCAN_NOT_ROR) & primary_flags;
}

THR_LOCK_DATA **ha_lineairdb_columnar::store_lock(THD *, THR_LOCK_DATA **to,
                                                  thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && m_lock.type == TL_UNLOCK)
    m_lock.type = lock_type;
  *to++ = &m_lock;
  return to;
}

int ha_lineairdb_columnar::load_table(const TABLE &table_arg) {
  // Single-copy PAX: the rows already live in the shared LineairDB server;
  // loading is registration only.
  assert(table_arg.file != nullptr);
  loaded_tables->add(table_arg.s->db.str, table_arg.s->table_name.str);
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

// ---------------------------------------------------------------------------
// Plugin boilerplate (declared in ha_lineairdb.cc's plugin list).
// ---------------------------------------------------------------------------
namespace {

handler *columnar_create_handler(handlerton *hton, TABLE_SHARE *table_share,
                                 bool, MEM_ROOT *mem_root) {
  return new (mem_root)
      lineairdb_columnar::ha_lineairdb_columnar(hton, table_share);
}

}  // namespace

struct st_mysql_storage_engine lineairdb_columnar_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

int lineairdb_columnar_init(void *p) {
  lineairdb_columnar::loaded_tables = new lineairdb_columnar::LoadedTables();
  handlerton *hton = static_cast<handlerton *>(p);
  hton->create = columnar_create_handler;
  hton->state = SHOW_OPTION_YES;
  hton->flags = HTON_IS_SECONDARY_ENGINE;
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->prepare_secondary_engine =
      lineairdb_columnar::PrepareSecondaryEngine;
  hton->optimize_secondary_engine =
      lineairdb_columnar::OptimizeSecondaryEngine;
  hton->compare_secondary_engine_cost = lineairdb_columnar::CompareJoinCost;
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
