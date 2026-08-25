#include "ha_lineairdb_columnar.hh"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "my_alloc.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_subselect.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/query_result.h"
#include "sql/sql_const.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/table.h"
#include "sql/visible_fields.h"
#include "thr_lock.h"

#include "lineairdb.pb.h"
#include "lineairdb_field_types.h"
#include "duckdb_request_builder.hh"
#include "lineairdb_proxy.hh"

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
// external_lock records the DuckDB bridge request; ExecuteDuckdbBridge
// consumes it after MySQL calls JOIN::override_executor_func.
class ColumnarExecutionContext : public Secondary_engine_execution_context {
 public:
  bool BestPlanSoFar(const JOIN &join, double cost) {
    // A second join-order search on the same JOIN resets join.best_read to
    // DBL_MAX; reset the parallel best cost at the same boundary.
    if (&join != current_join_ || join.best_read == DBL_MAX) {
      current_join_ = &join;
      best_cost_ = cost;
      return true;
    }

    const bool cheaper = cost < best_cost_;
    best_cost_ = std::min(best_cost_, cost);
    return cheaper;
  }

  // Built by external_lock (post-resolve, pre-optimize), shipped by
  // ExecuteDuckdbBridge. A refused or never-built statement is declined by
  // OptimizeSecondaryEngine; the reason lives in `refusal`.
  LineairDB::Protocol::TxExecuteDuckdbQuery::Request duckdb_request;
  bool request_build_attempted = false;
  std::string refusal;
  bool duckdb_ready = false;

  // Semijoin flattening fuses IN/EXISTS into the outer join for MySQL's
  // own executor. Keep the subquery Items intact; DuckDB flattens on its
  // own. Cleared per statement, restored with the context.
  void DisableSemijoin(THD *thd) {
    restore_thd_ = thd;
    saved_optimizer_switch_ = thd->variables.optimizer_switch;
    // With only the EXISTS strategy left, no subquery reaches the costing
    // pass that breaks on secondary plans. The derived-table rewrite is
    // barred through the LEX flag below.
    thd->variables.optimizer_switch &=
        ~(OPTIMIZER_SWITCH_SEMIJOIN | OPTIMIZER_SWITCH_MATERIALIZATION |
          OPTIMIZER_SWITCH_SUBQUERY_TO_DERIVED);
    thd->lex->m_subquery_to_derived_is_impossible = true;
  }
  ~ColumnarExecutionContext() override {
    if (restore_thd_ != nullptr) {
      restore_thd_->variables.optimizer_switch = saved_optimizer_switch_;
      restore_thd_->lex->m_subquery_to_derived_is_impossible = false;
    }
  }

 private:
  const JOIN *current_join_ = nullptr;
  double best_cost_ = 0.0;
  THD *restore_thd_ = nullptr;
  ulonglong saved_optimizer_switch_ = 0;
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
 * @brief Rounds an ASCII decimal literal to exactly `target_scale`
 * fractional digits using string/integer arithmetic only.
 *
 * @details DuckDB evaluates DECIMAL division (including AVG) in DOUBLE by
 * design, so its text output does not follow MySQL's exact-decimal scale
 * rules. `item->decimals` carries MySQL's authoritative scale for each
 * output column; reformatting to it restores the row-format contract for
 * any decimal-division shape. Inputs already at or below `target_scale`
 * are only zero-padded.
 *
 * @return false when the input is not a plain [+-]?digits(.digits)?
 * literal; the caller keeps the original text.
 */
bool RoundDecimalText(const char *ptr, size_t len, uint32_t target_scale,
                      std::string *out) {
  if (len == 0) return false;
  size_t pos = 0;
  bool negative = false;
  if (ptr[0] == '+' || ptr[0] == '-') {
    negative = (ptr[0] == '-');
    pos = 1;
  }
  size_t dot = std::string::npos;
  for (size_t i = pos; i < len; i++) {
    if (ptr[i] == '.' && dot == std::string::npos) {
      dot = i;
    } else if (!std::isdigit(static_cast<unsigned char>(ptr[i]))) {
      return false;  // not a plain decimal literal: leave text unchanged
    }
  }

  std::string int_part(ptr + pos, dot == std::string::npos ? len - pos : dot - pos);
  std::string frac_part = dot == std::string::npos
                              ? std::string()
                              : std::string(ptr + dot + 1, len - dot - 1);
  if (int_part.empty()) int_part = "0";

  if (frac_part.size() <= target_scale) {
    frac_part.append(target_scale - frac_part.size(), '0');
    *out = (negative ? "-" : "") + int_part +
           (target_scale > 0 ? "." + frac_part : "");
    return true;
  }

  // Round-half-up (away from zero) at position target_scale, matching
  // MySQL's decimal display rounding convention.
  const bool round_up = frac_part[target_scale] >= '5';
  frac_part.resize(target_scale);
  if (round_up) {
    int i = static_cast<int>(frac_part.size()) - 1;
    for (; i >= 0; i--) {
      if (frac_part[i] == '9') {
        frac_part[i] = '0';
      } else {
        frac_part[i]++;
        break;
      }
    }
    if (i < 0) {  // carried out of the fractional part into int_part
      int j = static_cast<int>(int_part.size()) - 1;
      for (; j >= 0; j--) {
        if (int_part[j] == '9') {
          int_part[j] = '0';
        } else {
          int_part[j]++;
          break;
        }
      }
      if (j < 0) int_part.insert(int_part.begin(), '1');
    }
  }
  *out = (negative ? "-" : "") + int_part +
         (target_scale > 0 ? "." + frac_part : "");
  return true;
}

/**
 * @brief Execute the request recorded by BuildDuckdbQueryRequest.
 *
 * MySQL has already sent result-set metadata for the original SELECT list;
 * this override only ships value-only Item carriers that match it. The
 * response rows use the proxy row format (DecodeRowFields).
 */
bool ExecuteDuckdbBridge(JOIN *join, Query_result *result) {
  THD *thd = join->thd;
  auto *ctx = static_cast<ColumnarExecutionContext *>(
      thd->lex->secondary_engine_execution_context());
  if (ctx == nullptr || !ctx->duckdb_ready) {
    return RaiseColumnarError(thd,
                              "LINEAIRDB_COLUMNAR: no duckdb-bridge plan");
  }

  std::shared_ptr<LineairDBProxy> proxy = lineairdb::acquire_shared_proxy(thd);
  if (!proxy) {
    return RaiseColumnarError(thd, "LINEAIRDB_COLUMNAR: no server connection");
  }

  LineairDB::Protocol::TxExecuteDuckdbQuery::Response rpc;
  if (!proxy->tx_execute_duckdb_query(ctx->duckdb_request, &rpc) ||
      !rpc.ok()) {
    char message[192];
    snprintf(message, sizeof(message), "LINEAIRDB_COLUMNAR duckdb-bridge: %s",
             rpc.error().empty() ? "duckdb bridge RPC failed"
                                 : rpc.error().c_str());
    return RaiseColumnarError(thd, message);
  }

  mem_root_deque<Item *> output_items(thd->mem_root);
  std::vector<ItemColumnarValue *> values;
  // Parallel to `values`: MySQL's authoritative decimal scale per output
  // expression, or DECIMAL_NOT_SPECIFIED for "don't touch". DuckDB returns
  // decimal-division results as DOUBLE text; RoundDecimalText reformats
  // those columns back to this scale.
  std::vector<uint32_t> target_scale;
  for (Item *item : VisibleFields(join->query_block->fields)) {
    auto *value = new (thd->mem_root) ItemColumnarValue(item);
    if (value == nullptr) return true;
    values.push_back(value);
    output_items.push_back(value);

    const Item_result rt = item->result_type();
    const bool fixed_scale_numeric =
        (rt == DECIMAL_RESULT || rt == REAL_RESULT) &&
        item->decimals <= DECIMAL_MAX_SCALE;
    target_scale.push_back(fixed_scale_numeric
                                ? static_cast<uint32_t>(item->decimals)
                                : static_cast<uint32_t>(DECIMAL_NOT_SPECIFIED));
  }

  std::vector<DecodedField> fields;
  std::string rounded;
  const size_t expected = 1 + values.size();
  for (const std::string &row : rpc.rows()) {
    if (!DecodeRowFields(row, &fields) || fields.size() != expected) {
      return RaiseColumnarError(
          thd,
          "LINEAIRDB_COLUMNAR duckdb-bridge: malformed row (DuckDB result "
          "column count may not match the original SELECT list)");
    }

    // Field 0 is a placeholder for the proxy row null-flags field. The
    // server emits one following field per DuckDB output column.
    for (size_t i = 0; i < values.size(); i++) {
      const DecodedField &field = fields[1 + i];
      if (!field.empty) {
        if (target_scale[i] != static_cast<uint32_t>(DECIMAL_NOT_SPECIFIED) &&
            RoundDecimalText(field.ptr, field.len, target_scale[i], &rounded)) {
          values[i]->set_value(rounded.data(), rounded.size());
        } else {
          values[i]->set_value(field.ptr, field.len);
        }
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
  // Install first: set_... destroys any prior context, whose destructor
  // restores the switches this call is about to save.
  lex->set_secondary_engine_execution_context(ctx);
  ctx->DisableSemijoin(thd);
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
  if (join == nullptr) {
    return RaiseColumnarError(lex->thd,
                              "LINEAIRDB_COLUMNAR unsupported shape: no JOIN");
  }

  if (!ctx->request_build_attempted || !ctx->refusal.empty()) {
    std::string message = "LINEAIRDB_COLUMNAR duckdb-query: ";
    message.append(ctx->request_build_attempted
                       ? ctx->refusal
                       : std::string("request was not built before optimization"));
    return RaiseColumnarError(lex->thd, message.c_str());
  }
  ctx->duckdb_ready = true;
  join->override_executor_func = ExecuteDuckdbBridge;
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

  // DisableSemijoin leaves every surviving subquery on the EXISTS
  // strategy, so its JOIN reaches this hook like any other.
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

// Builds the request once per statement, at the only stock point after
// resolution and before optimization. The outcome is recorded, not raised:
// a non-zero return here reads as a lock error and aborts the statement.
int ha_lineairdb_columnar::external_lock(THD *thd, int lock_type) {
  if (lock_type == F_UNLCK) return 0;
  auto *ctx = static_cast<ColumnarExecutionContext *>(
      thd->lex->secondary_engine_execution_context());
  if (ctx == nullptr || ctx->request_build_attempted) return 0;
  ctx->request_build_attempted = true;
  if (!BuildDuckdbQueryRequest(thd, thd->lex, &ctx->duckdb_request,
                              &ctx->refusal) &&
      ctx->refusal.empty()) {
    ctx->refusal = "request build failed";
  }
  static const bool debug_resolved = [] {
    const char *value = std::getenv("ENABLE_DUCKDB_BRIDGE_DEBUG");
    return value != nullptr && value[0] != '\0' &&
           std::string_view(value) != "0";
  }();
  if (debug_resolved) {
    std::fprintf(stderr, "[duckdb-request] refusal='%s'\n%s\n",
                 ctx->refusal.c_str(),
                 ctx->duckdb_request.DebugString().c_str());
  }
  return 0;
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
