#include "storage/lineairdb/ha_lineairdb.hh"
#include "../common/log.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>
// for ::strcasecmp
#include <strings.h>

#include "lineairdb_autogen.hh"
#include "lineairdb_field_types.h"
#include "lineairdb_keyenc.hh"
#include "lineairdb_prefetch.hh"
#include "lineairdb.pb.h"
#include "my_base.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "sql/table_trigger_dispatcher.h"
#include "typelib.h"

// Enable Prefetch only for DML; DDL must keep the normal transaction path
bool thd_can_use_prefetch(THD *thd) {
  if (thd == nullptr) return false;                  // Missing session
  if (thd->lex == nullptr) return false;             // Missing SQL state

  switch (thd->lex->sql_command) {
    case SQLCOM_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
      return true;
    default:
      return false;
  }
}

// Encode one integer DSL key part into the same bytes as handler keys
static std::string encode_plan_int_key_part(int64_t value, int size = 4) {
  uint64_t raw = 0;
  uint64_t sign_mask = 0;

  switch (size) {
    case 1:
      raw = static_cast<uint8_t>(static_cast<int8_t>(value));
      sign_mask = 0x80ULL;
      break;
    case 2:
      raw = static_cast<uint16_t>(static_cast<int16_t>(value));
      sign_mask = 0x8000ULL;
      break;
    case 4:
      raw = static_cast<uint32_t>(static_cast<int32_t>(value));
      sign_mask = 0x80000000ULL;
      break;
    case 8:
      raw = static_cast<uint64_t>(value);
      sign_mask = 0x8000000000000000ULL;
      break;
    default:
      raw = static_cast<uint32_t>(static_cast<int32_t>(value));
      sign_mask = 0x80000000ULL;
      size = 4;
      break;
  }

  const uint64_t encoded = raw ^ sign_mask;
  std::string out;
  out.push_back(static_cast<char>(kKeyMarkerNotNull));
  out.push_back(static_cast<char>(kKeyTypeInt));
  out.push_back(static_cast<char>((size >> 8) & 0xFF));
  out.push_back(static_cast<char>(size & 0xFF));
  for (int i = size - 1; i >= 0; --i) {
    out.push_back(static_cast<char>((encoded >> (i * 8)) & 0xFF));
  }
  return out;
}

// Encode one string DSL key part into the same bytes as handler keys
static std::string encode_plan_string_key_part(const std::string& value) {
  std::string out;
  out.push_back(static_cast<char>(kKeyMarkerNotNull));
  out.push_back(static_cast<char>(kKeyTypeString));
  out.append(value);
  out.push_back('\0');
  const uint16_t length = static_cast<uint16_t>(value.size());
  out.push_back(static_cast<char>((length >> 8) & 0xFF));
  out.push_back(static_cast<char>(length & 0xFF));
  return out;
}

static bool try_parse_plan_int(const std::string& text, int64_t *value) {
  if (text.empty() || value == nullptr) return false;
  char *end = nullptr;
  *value = std::strtoll(text.c_str(), &end, 10);
  return end == text.c_str() + text.size();
}

bool thd_has_tx_plan(THD *thd) {
  if (thd == nullptr) return false;
  auto it = thd->user_vars.find("_tx_plan");
  if (it == thd->user_vars.end()) return false;

  auto *entry = it->second.get();
  return entry != nullptr && entry->ptr() != nullptr && entry->length() > 0;
}

// Encode one DSL segment: 42=INT, 42t=TINYINT, 42s=SMALLINT, 42l=BIGINT
static std::string encode_plan_key_segment(const std::string& segment) {
  if (segment.empty()) return {};

  int int_size = 4;
  std::string number = segment;
  const char suffix = segment.back();
  if (suffix == 't') {
    int_size = 1;
    number = segment.substr(0, segment.size() - 1);
  } else if (suffix == 's') {
    int_size = 2;
    number = segment.substr(0, segment.size() - 1);
  } else if (suffix == 'i') {
    int_size = 4;
    number = segment.substr(0, segment.size() - 1);
  } else if (suffix == 'l') {
    int_size = 8;
    number = segment.substr(0, segment.size() - 1);
  }

  int64_t int_value = 0;
  if (try_parse_plan_int(number, &int_value)) {
    return encode_plan_int_key_part(int_value, int_size);
  }
  return encode_plan_string_key_part(segment);
}

static std::vector<std::string> split_plan_text(const std::string& text,
                                                char delimiter) {
  std::vector<std::string> parts;
  std::istringstream stream(text);
  std::string part;
  while (std::getline(stream, part, delimiter)) {
    parts.push_back(part);
  }
  return parts;
}

static std::string normalize_plan_table_name(THD *thd,
                                             const std::string& table_name) {
  if (table_name.empty()) return table_name;
  if (table_name[0] == '.') return table_name;
  if (table_name.find('/') != std::string::npos) return "./" + table_name;

  std::string prefix = "./";
  if (thd != nullptr && thd->db().str != nullptr && thd->db().length > 0) {
    prefix += std::string(thd->db().str, thd->db().length) + "/";
  }
  return prefix + table_name;
}

static LineairDBProxy::ReadPlanKeyBinding parse_plan_binding(
    const std::string& spec) {
  LineairDBProxy::ReadPlanKeyBinding binding;
  if (spec.size() < 2 || spec[0] != 'B') return binding;

  size_t pos = 1;
  size_t step_end = spec.find('.', pos);
  if (step_end == std::string::npos) step_end = spec.size();
  binding.source_step = static_cast<uint32_t>(
      std::strtoul(spec.substr(pos, step_end - pos).c_str(), nullptr, 10));
  if (step_end >= spec.size()) return binding;
  pos = step_end + 1;

  size_t type_end = spec.find('.', pos);
  if (type_end == std::string::npos) type_end = spec.size();
  const std::string type = spec.substr(pos, type_end - pos);
  pos = type_end + 1;

  if (type == "K") {
    binding.from_key = true;
  } else if (type == "V") {
    binding.from_key = false;
  } else if (type == "MK") {
    binding.use_midpoint = true;
    binding.from_key = true;
  } else if (type == "M") {
    binding.use_midpoint = true;
  } else if (type.rfind("MCI", 0) == 0 || type.rfind("CI", 0) == 0) {
    const bool midpoint = type.rfind("MCI", 0) == 0;
    const size_t number_pos = midpoint ? 3 : 2;
    const std::string number = type.substr(number_pos);
    char *end = nullptr;
    const long column = std::strtol(number.c_str(), &end, 10);
    binding.use_midpoint = midpoint;
    binding.source_column = static_cast<int32_t>(column + 1);
    binding.column_as_int_key = true;
    if (end != nullptr && *end != '\0') {
      binding.int_delta = std::strtoll(end, nullptr, 10);
    }
    return binding;
  }

  if (pos < spec.size()) {
    size_t offset_end = spec.find('.', pos);
    if (offset_end == std::string::npos) offset_end = spec.size();
    binding.source_offset = static_cast<uint32_t>(
        std::strtoul(spec.substr(pos, offset_end - pos).c_str(), nullptr, 10));
    pos = offset_end + 1;
  }
  if (pos < spec.size()) {
    binding.source_length =
        static_cast<uint32_t>(std::strtoul(spec.substr(pos).c_str(), nullptr, 10));
  }
  return binding;
}

static bool token_is_plan_binding(const std::string& token) {
  if (token.size() < 4 || token[0] != 'B') return false;
  size_t pos = 1;
  while (pos < token.size() &&
         std::isdigit(static_cast<unsigned char>(token[pos]))) {
    ++pos;
  }
  return pos > 1 && pos < token.size() && token[pos] == '.';
}

static void append_plan_key_token(LineairDBProxy::ReadPlanStep *step,
                                  const std::string& token,
                                  bool end_key) {
  if (step == nullptr || token.empty()) return;
  if (token_is_plan_binding(token)) {
    auto binding = parse_plan_binding(token);
    if (end_key) {
      step->end_bindings.push_back(std::move(binding));
    } else {
      step->bindings.push_back(std::move(binding));
    }
    return;
  }

  if (end_key) {
    step->end_key_prefix += encode_plan_key_segment(token);
  } else {
    step->key_prefix += encode_plan_key_segment(token);
  }
}

// Parse read-plan DSL: R=point, S=PK range/prefix scan, SI=secondary scan
static std::vector<LineairDBProxy::ReadPlanStep> parse_plan_steps(
    THD *thd, const std::string& plan_text) {
  std::vector<LineairDBProxy::ReadPlanStep> steps;

  for (const auto& step : split_plan_text(plan_text, ';')) {
    if (step.empty()) continue;
    const auto parts = split_plan_text(step, ':');
    if (parts.size() < 2) continue;

    LineairDBProxy::ReadPlanStep parsed;
    parsed.table_name = normalize_plan_table_name(thd, parts[1]);
    bool end_key = false;
    size_t token_start = 2;
    if (parts[0] == "R") {
      parsed.is_scan = false;
    } else if (parts[0] == "S") {
      parsed.is_scan = true;
    } else if (parts[0] == "SI") {
      if (parts.size() < 3) continue;
      parsed.is_scan = true;
      parsed.index_name = parts[2];
      token_start = 3;
    } else if (parts[0] == "FE") {
      parsed.for_each = true;
    } else {
      continue;
    }

    for (size_t i = token_start; i < parts.size(); ++i) {
      const auto& token = parts[i];
      if (token == "E") {
        end_key = true;
        continue;
      }
      if (token.rfind("limit=", 0) == 0) {
        parsed.scan_limit = static_cast<uint64_t>(
            std::strtoull(token.substr(6).c_str(), nullptr, 10));
        continue;
      }
      if (token.rfind("reverse=", 0) == 0) {
        parsed.reverse_scan = token.substr(8) == "1";
        continue;
      }
      append_plan_key_token(&parsed, token, end_key);
    }
    if (!parsed.table_name.empty()) {
      steps.push_back(std::move(parsed));
    }
  }

  return steps;
}

// Read @_tx_plan once and clear it so the next statement starts clean
static std::string read_and_clear_tx_plan(THD *thd) {
  std::string plan;
  if (thd == nullptr) return plan;

  auto it = thd->user_vars.find("_tx_plan");
  if (it == thd->user_vars.end()) return plan;

  auto *entry = it->second.get();
  if (entry == nullptr || entry->ptr() == nullptr || entry->length() == 0) {
    return plan;
  }

  plan.assign(entry->ptr(), entry->length());
  entry->lock();
  entry->set_null_value(STRING_RESULT);
  entry->unlock();
  return plan;
}

void maybe_prefetch_for_transaction(THD *thd,
                                            LineairDBTransaction *tx) {
  if (tx == nullptr || !tx->is_prefetch_mode()) return;

  const std::string plan_text = read_and_clear_tx_plan(thd);
  if (plan_text.empty()) return;

  const auto steps = parse_plan_steps(thd, plan_text);
  if (steps.empty()) return;
  tx->set_tx_plan_used(true);
  tx->execute_read_plan(steps);
}

// Build the read plan from the given QEP root and run it in one prefetch RPC.
static int autogen_and_execute_prefetch(THD *thd, AccessPath *root,
                                        LineairDBTransaction *tx) {
  std::vector<LineairDBProxy::ReadPlanStep> steps;
  if (!autogen_read_plan_from_qep(thd, root, tx->ro_novalidate(), &steps)) {
    // autogen has already raised a my_error describing the unsupported shape.
    tx->set_status_to_abort();
    thd_mark_transaction_to_rollback(thd, 1);
    return HA_ERR_UNSUPPORTED;
  }
  if (steps.empty()) return 0;

  // Loads the prefetched rows into the local cache and validation sets.
  tx->execute_read_plan(steps);
  return tx->is_aborted() ? HA_ERR_LOCK_DEADLOCK : 0;
}

// Plan root of the whole statement; null while the optimizer is still
// building it (e.g. it evaluates an uncorrelated subquery for constant
// folding before the outer plan exists).
static AccessPath *statement_plan_root(THD *thd) {
  if (thd == nullptr || thd->lex == nullptr || thd->lex->unit == nullptr) {
    return nullptr;
  }
  if (AccessPath *root = thd->lex->unit->root_access_path()) return root;
  Query_block *qb = thd->lex->unit->first_query_block();
  if (qb == nullptr || qb->join == nullptr) return nullptr;
  return qb->join->root_access_path();
}

// Plan root of the query expression that owns `table` — the subquery being
// evaluated when the statement-level root is not available yet.
static AccessPath *table_unit_plan_root(TABLE *table) {
  if (table == nullptr || table->pos_in_table_list == nullptr) return nullptr;
  Query_block *qb = table->pos_in_table_list->query_block;
  if (qb == nullptr) return nullptr;
  Query_expression *unit = qb->master_query_expression();
  if (unit != nullptr) {
    if (AccessPath *root = unit->root_access_path()) return root;
  }
  if (qb->join != nullptr) {
    if (AccessPath *root = qb->join->root_access_path()) return root;
  }
  return nullptr;
}

static void sync_autogen_statement(THD *thd, LineairDBTransaction *tx) {
  const uint64_t query_id =
      (thd != nullptr) ? static_cast<uint64_t>(thd->query_id) : 0;
  if (tx->autogen_query_id() != query_id) {
    tx->reset_autogen_for_statement(query_id);
  }
}

// True for a single-table UPDATE/DELETE on the pre-iterator executor, where
// no JOIN/root AccessPath exists for QEP autogen to read.
static bool is_legacy_single_table_dml(THD *thd) {
  if (thd == nullptr || thd->lex == nullptr || thd->lex->unit == nullptr) {
    return false;
  }
  if (thd->lex->sql_command != SQLCOM_UPDATE &&
      thd->lex->sql_command != SQLCOM_DELETE) {
    return false;
  }

  Query_block *query_block = thd->lex->unit->first_query_block();
  if (query_block == nullptr || query_block->join == nullptr) return true;
  return query_block->join->root_access_path() == nullptr;
}

// Return why a legacy-DML shape (extra tables, subquery, ORDER BY/LIMIT,
// partitioning, triggers) cannot be served by one staged range; nullptr when
// the shape is safe.
static const char *legacy_dml_shape_rejection(THD *thd, TABLE *table) {
  if (thd == nullptr || thd->lex == nullptr || thd->lex->unit == nullptr ||
      table == nullptr) {
    return "missing legacy DML metadata";
  }

  Query_block *query_block = thd->lex->unit->first_query_block();
  if (query_block == nullptr) return "missing legacy DML query block";
  if (query_block->leaf_table_count != 1 ||
      query_block->derived_table_count != 0 ||
      query_block->materialized_derived_table_count != 0) {
    return "legacy DML additional-table read";
  }
  if (query_block->first_inner_query_expression() != nullptr) {
    return "legacy DML subquery";
  }
  if (query_block->is_ordered() || query_block->has_limit()) {
    return "legacy DML ORDER BY/LIMIT";
  }
  if (query_block->partitioned_table_count != 0 ||
      table->part_info != nullptr) {
    return "legacy DML partitioned table";
  }
  if (table->triggers != nullptr) {
    if (thd->lex->sql_command == SQLCOM_UPDATE &&
        table->triggers->has_update_triggers()) {
      return "legacy DML UPDATE trigger";
    }
    if (thd->lex->sql_command == SQLCOM_DELETE &&
        table->triggers->has_delete_triggers()) {
      return "legacy DML DELETE trigger";
    }
  }
  return nullptr;
}

int maybe_prefetch_for_statement(THD *thd, LineairDBTransaction *tx,
                                 TABLE *table) {
  if (tx == nullptr || !tx->is_prefetch_mode()) return 0;  // not prefetch protocol
  if (tx->tx_plan_used()) return 0;                        // tx-scoped plan covers it

  sync_autogen_statement(thd, tx);

  AccessPath *stmt_root = statement_plan_root(thd);
  if (stmt_root != nullptr) {
    if (tx->autogen_stmt_resolved()) return 0;  // already done this statement
    tx->mark_autogen_stmt_resolved();

    // A read is imminent (called from rnd_init / index_read_map / ...). A
    // command whose read side cannot be prefetched (e.g. INSERT ... SELECT)
    // would miss and abort silently in prefetch mode, so fail loudly instead.
    if (!thd_can_use_prefetch(thd)) {
      return prefetch_reject_unsupported(
          thd, tx, "read-bearing statement is not prefetch-eligible");
    }

    return autogen_and_execute_prefetch(thd, stmt_root, tx);
  }

  // No statement root yet: the optimizer is evaluating a subquery (constant
  // folding) before the outer plan is built. Stage the subquery unit's own
  // plan, once per root; the statement-level staging still happens when the
  // outer execution starts.
  AccessPath *unit_root = table_unit_plan_root(table);
  if (unit_root == nullptr) {
    if (tx->autogen_stmt_resolved()) return 0;
    tx->mark_autogen_stmt_resolved();
    return prefetch_reject_unsupported(thd, tx,
                                       "missing JOIN root_access_path");
  }
  if (tx->autogen_root_staged(unit_root)) return 0;
  tx->mark_autogen_root_staged(unit_root);

  if (!thd_can_use_prefetch(thd)) {
    return prefetch_reject_unsupported(
        thd, tx, "read-bearing statement is not prefetch-eligible");
  }

  return autogen_and_execute_prefetch(thd, unit_root, tx);
}

// Gate for the handler entry points: true when autogen must defer to the
// handler index access, marking it handler-deferred on the first call.
bool prefetch_needs_legacy_dml_handler(THD *thd,
                                      LineairDBTransaction *tx) {
  if (tx == nullptr || !tx->is_prefetch_mode() || tx->tx_plan_used()) {
    return false;
  }
  sync_autogen_statement(thd, tx);
  if (tx->autogen_stmt_resolved()) return false;
  if (tx->is_autogen_stmt_handler_deferred()) return true;
  if (!is_legacy_single_table_dml(thd)) return false;
  tx->mark_autogen_stmt_handler_deferred();
  return true;
}

// Build, stage, and serve a legacy single-table UPDATE/DELETE plan from its
// first handler index access, once per statement; reject unsupported shapes.
int maybe_prefetch_for_legacy_dml_handler(
    THD *thd, LineairDBTransaction *tx, TABLE *table, uint index,
    const IndexSearchPlan &search) {
  if (tx == nullptr || !tx->is_prefetch_mode() || tx->tx_plan_used()) return 0;

  sync_autogen_statement(thd, tx);
  if (tx->autogen_stmt_resolved()) return 0;
  tx->mark_autogen_stmt_handler_deferred();
  tx->mark_autogen_stmt_resolved();

  if (!is_legacy_single_table_dml(thd)) {
    return prefetch_reject_unsupported(
        thd, tx, "handler-derived plan requested for non-legacy DML");
  }
  if (const char *reason = legacy_dml_shape_rejection(thd, table)) {
    return prefetch_reject_unsupported(thd, tx, reason);
  }

  std::vector<LineairDBProxy::ReadPlanStep> steps;
  if (!autogen_read_plan_from_index_search(thd, table, index, search, &steps)) {
    tx->set_status_to_abort();
    thd_mark_transaction_to_rollback(thd, 1);
    return HA_ERR_UNSUPPORTED;
  }

  tx->execute_read_plan(steps);
  return tx->is_aborted() ? HA_ERR_LOCK_DEADLOCK : 0;
}

int prefetch_reject_unsupported(THD *thd, LineairDBTransaction *tx,
                                const char *reason) {
  std::string msg = "LineairDB prefetch unsupported: ";
  msg += reason != nullptr ? reason : "unsupported access shape";
  if (thd != nullptr) {
    const LEX_CSTRING query = thd->query();
    if (query.str != nullptr && query.length > 0) {
      msg += " sql=";
      msg.append(query.str, query.length);
    }
  }
  my_error(ER_NOT_SUPPORTED_YET, MYF(0), msg.c_str());
  if (tx != nullptr) tx->set_status_to_abort();
  thd_mark_transaction_to_rollback(thd, 1);
  return HA_ERR_UNSUPPORTED;
}
