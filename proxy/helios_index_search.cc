#include "storage/helios/ha_helios.hh"
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

#include "helios_field_types.h"
#include "helios_index_search.hh"
#include "helios_keyenc.hh"
#include "lineairdb.pb.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"
#include "sql/table.h"
#include "typelib.h"

// The query block a table is read in. A table inside a derived table or a
// subquery is governed by that block's own WHERE and LIMIT, not the
// statement's outermost block.
static Query_block *owning_query_block(THD *thd, const TABLE *table) {
  if (table != nullptr && table->pos_in_table_list != nullptr &&
      table->pos_in_table_list->query_block != nullptr) {
    return table->pos_in_table_list->query_block;
  }
  if (thd == nullptr || thd->lex == nullptr || thd->lex->unit == nullptr) {
    return nullptr;
  }
  return thd->lex->unit->global_parameters();
}

// A storage scan carries no predicate, so every row MySQL drops above the
// handler would have come out of a pushed LIMIT. Only a SELECT with no WHERE
// left for this table's rows may push one.
static bool select_scan_limit_is_safe(THD *thd, TABLE *table) {
  if (thd == nullptr || thd->lex == nullptr || thd->lex->unit == nullptr) {
    return false;
  }
  if (table == nullptr || table->s == nullptr) return false;

  Query_block *qb = owning_query_block(thd, table);
  if (qb == nullptr) return false;
  return qb->where_cond() == nullptr;
}

// True when one ORDER BY item is exactly the requested keypart and direction
static bool order_item_matches_key_part(ORDER *order, const KEY *key,
                                        uint key_part_idx,
                                        enum_order direction) {
  // Validate the ORDER BY node shape before comparing fields
  if (order == nullptr) return false;                // Missing ORDER node
  if (key == nullptr) return false;                  // Missing key metadata
  if (order->direction != direction) return false;   // Wrong direction

  const bool key_part_exists = (key_part_idx < key->user_defined_key_parts);
  if (!key_part_exists) return false;                // ORDER exceeds key

  const bool has_order_item =
      (order->item != nullptr && *(order->item) != nullptr);
  if (!has_order_item) return false;                 // Missing ORDER item

  // Resolve ORDER BY expression to a table field
  Item *item = (*order->item)->real_item();
  const bool is_field_item = (item->type() == Item::FIELD_ITEM);
  if (!is_field_item) return false;                  // Not a bare field

  // Compare the ORDER BY field with the expected keypart
  Item_field *field_item = down_cast<Item_field *>(item);
  const Field *order_field = field_item->field;
  const Field *key_field = key->key_part[key_part_idx].field;
  if (order_field == nullptr) return false;          // Missing ORDER field
  if (key_field == nullptr) return false;            // Missing key field

  return order_field->field_index() == key_field->field_index();
}

// True when ORDER BY is a prefix of the remaining key suffix in one direction
static bool order_by_matches_key_suffix(Query_block *qb, const KEY *key,
                                        uint matched_prefix,
                                        enum_order direction) {
  // LIMIT needs an explicit ORDER BY that matches scan order
  if (qb == nullptr) return false;                   // Missing query block
  if (key == nullptr) return false;                  // Missing key metadata
  if (qb->order_list.elements == 0) return false;    // Unordered LIMIT

  // If all keyparts are fixed, the scan can return at most one key
  const uint key_parts = key->user_defined_key_parts;
  if (matched_prefix >= key_parts) return true;      // Full key fixed

  // ORDER BY must follow the unfixed key suffix in one direction
  uint order_pos = 0;
  for (ORDER *order = qb->order_list.first; order != nullptr;
       order = order->next, ++order_pos) {
    const uint key_part_idx = matched_prefix + order_pos;
    if (!order_item_matches_key_part(order, key, key_part_idx, direction)) {
      return false;
    }
  }

  return true;
}

RangeScanLimit range_scan_limit_for_order(
    THD *thd, const KEY *key, uint matched_prefix, bool has_mysql_only_filter) {
  RangeScanLimit scan_limit;

  // Keep scans unlimited when MySQL must evaluate a filter the server lacks
  if (has_mysql_only_filter) return scan_limit;        // Server may miss rows

  // Validate this is a simple SELECT with a query expression to inspect
  if (thd == nullptr) return scan_limit;               // Missing session
  if (thd->lex == nullptr) return scan_limit;          // Missing SQL state
  if (thd->lex->unit == nullptr) return scan_limit;    // Missing query unit
  const bool is_select = (thd->lex->sql_command == SQLCOM_SELECT);
  if (!is_select) return scan_limit;                   // Not SELECT

  // LIMIT is only safe for one-table, non-aggregate reads
  const TABLE *scanned = (key != nullptr && key->key_part != nullptr &&
                          key->key_part[0].field != nullptr)
                             ? key->key_part[0].field->table
                             : nullptr;
  Query_block *qb = owning_query_block(thd, scanned);
  if (qb == nullptr) return scan_limit;                // Missing query block
  Query_expression *unit = qb->master_query_expression();
  if (unit == nullptr) return scan_limit;              // Missing query unit
  if (qb->leaf_table_count != 1) return scan_limit;    // Join needs root LIMIT
  if (qb->is_explicitly_grouped()) return scan_limit;  // GROUP BY changes rows
  if (qb->is_implicitly_grouped()) return scan_limit;  // Aggregate changes rows

  // Extract LIMIT N and reject shapes the storage scan cannot represent
  if (unit->offset_limit_cnt != 0) return scan_limit;  // OFFSET not supported
  if (unit->select_limit_cnt == 0) return scan_limit;  // Empty LIMIT
  const bool has_limit = (unit->select_limit_cnt != HA_POS_ERROR);
  if (!has_limit) return scan_limit;                   // No LIMIT clause

  // A full-key point lookup returns at most one row, so direction is irrelevant
  const uint key_parts = key->user_defined_key_parts;
  if (matched_prefix >= key_parts) {
    scan_limit.row_limit = unit->select_limit_cnt;
    return scan_limit;
  }

  // ASC uses the natural key order; DESC uses Helios ScanReverse
  if (order_by_matches_key_suffix(qb, key, matched_prefix, ORDER_ASC)) {
    scan_limit.row_limit = unit->select_limit_cnt;
    return scan_limit;
  }
  if (order_by_matches_key_suffix(qb, key, matched_prefix, ORDER_DESC)) {
    scan_limit.row_limit = unit->select_limit_cnt;
    scan_limit.reverse_scan = true;
    return scan_limit;
  }

  return scan_limit;
}

uint ha_helios::calculate_key_parts_from_length(KEY *key, uint key_length) {
  if (key == nullptr || key_length == 0)
    return 0;

  uint parts = 0;
  uint accumulated_length = 0;

  for (uint i = 0; i < key->user_defined_key_parts; i++) {
    KEY_PART_INFO *part = &key->key_part[i];

    // Add length for this key part (including null byte if nullable)
    uint part_length = part->store_length;
    accumulated_length += part_length;

    if (accumulated_length <= key_length) {
      parts++;
    } else {
      break;
    }
  }

  return parts;
}

uint ha_helios::count_used_key_parts(const KEY *key_info,
                                        key_part_map keypart_map) {
  uint count = 0;
  for (uint i = 0; i < key_info->user_defined_key_parts; i++) {
    if ((keypart_map >> i) & 1)
      count++;
    else
      break;
  }
  return count;
}

void ha_helios::build_search_plan(const uchar *key, key_part_map keypart_map,
                                     enum ha_rkey_function find_flag,
                                     KEY *key_info) {
  // 1. Reset state
  current_plan_.reset();
  reset_index_search_buffers();

  // 2. Extract basic information
  current_plan_.is_primary = (active_index == table->s->primary_key);
  current_plan_.find_flag = find_flag;

  // HA_WHOLE_KEY support
  if (keypart_map == HA_WHOLE_KEY) {
    current_plan_.used_key_parts = key_info->user_defined_key_parts;
    current_plan_.all_parts_specified = true;
  } else {
    current_plan_.used_key_parts = count_used_key_parts(key_info, keypart_map);
    current_plan_.all_parts_specified =
        (current_plan_.used_key_parts == key_info->user_defined_key_parts);
  }

  // Unique check
  current_plan_.is_unique_index = (key_info->flags & HA_NOSAME) != 0;
  current_plan_.has_nullable_parts = (key_info->flags & HA_NULL_PART_KEY) != 0;

  // 3. Decide op
  if (key == nullptr) {
    current_plan_.op = IndexSearchOp::kIndexFirst;
  } else if (find_flag == HA_READ_KEY_EXACT &&
             current_plan_.all_parts_specified &&
             current_plan_.is_unique_index &&
             !current_plan_.has_nullable_parts) {
    current_plan_.op = IndexSearchOp::kUniquePoint;
  } else if (find_flag == HA_READ_KEY_EXACT) {
    current_plan_.op = IndexSearchOp::kSameKeyMaterialize;
  } else if (find_flag == HA_READ_PREFIX) {
    current_plan_.op = IndexSearchOp::kPrefixFirst;
  } else if (find_flag == HA_READ_PREFIX_LAST ||
             find_flag == HA_READ_PREFIX_LAST_OR_PREV) {
    if (find_flag == HA_READ_PREFIX_LAST_OR_PREV &&
        current_plan_.all_parts_specified) {
      current_plan_.op = IndexSearchOp::kPrevKey;
    } else {
      current_plan_.op = IndexSearchOp::kPrefixLast;
    }
  } else if (find_flag == HA_READ_KEY_OR_PREV ||
             find_flag == HA_READ_BEFORE_KEY) {
    current_plan_.op = IndexSearchOp::kPrevKey;
  } else {
    current_plan_.op = IndexSearchOp::kRangeMaterialize;
  }

  // 4. Serialize boundaries
  if (key != nullptr) {
    current_plan_.start_key_serialized = encode_key(key, keypart_map);

    // same group boundary (prefix operations)
    if (current_plan_.op == IndexSearchOp::kSameKeyMaterialize ||
        current_plan_.op == IndexSearchOp::kPrefixFirst ||
        current_plan_.op == IndexSearchOp::kPrefixLast) {
      current_plan_.same_group_prefix_serialized =
          current_plan_.start_key_serialized;
      current_plan_.same_group_end_serialized =
          build_prefix_range_end(current_plan_.start_key_serialized);
    }
  }

  // end_range processing
  if (end_range != nullptr) {
    current_plan_.end_key_serialized =
        encode_key(end_range->key, end_range->keypart_map);

    if (end_range->flag != HA_READ_BEFORE_KEY) {
      // SQL inclusive upper bound must become Helios's exclusive upper
      // bound. This is required for both full keys (k <= 30) and partial-key
      // prefix ranges.
      current_plan_.end_key_serialized =
          build_prefix_range_end(current_plan_.end_key_serialized);
    }
  }
}

int ha_helios::execute_plan(uchar *buf, HeliosTransaction *tx) {
  switch (current_plan_.op) {
  case IndexSearchOp::kIndexFirst:
    return execute_index_first(buf, tx);
  case IndexSearchOp::kUniquePoint:
    return execute_unique_point(buf, tx);
  case IndexSearchOp::kSameKeyMaterialize:
    return execute_same_key_materialize(buf, tx);
  case IndexSearchOp::kPrefixFirst:
    return execute_prefix_first(buf, tx);
  case IndexSearchOp::kRangeMaterialize:
    return execute_range_materialize(buf, tx);
  case IndexSearchOp::kPrevKey:
    return execute_prev_key(buf, tx);
  case IndexSearchOp::kPrefixLast:
    return execute_prefix_last(buf, tx);
  default:
    return HA_ERR_WRONG_COMMAND;
  }
}

int ha_helios::execute_index_first(uchar *buf, HeliosTransaction *tx) {
  std::string start_key = "";
  std::string end_key = current_plan_.end_key_serialized.empty()
                            ? helios_keyenc::scan_end_sentinel()
                            : current_plan_.end_key_serialized;

  if (current_plan_.is_primary && !statement_uses_read_plan(ha_thd())) {
    index_cursor_active_ = true;
    index_cursor_reverse_ = false;
    index_cursor_secondary_ = false;
    index_cursor_start_key_ = start_key;
    index_cursor_end_key_ = end_key;
    (void)refill_index_cursor(tx);
  } else if (current_plan_.is_primary) {
    // Staged windows are keyed by the full-range shape, so ask for the whole
    // range instead of walking it in cursor windows.
    auto key_values =
        tx->get_matching_keys_and_values_in_range(start_key, end_key);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, start_key, end_key);
    batch_fetch_secondary_payloads(tx);
  }

  // phantom detection check
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::execute_unique_point(uchar *buf, HeliosTransaction *tx) {
  if (current_plan_.is_primary) {
    auto result = tx->read(current_plan_.start_key_serialized);

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }

    if (result.first == nullptr || result.second == 0) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    if (set_fields_from_helios(buf, result.first, result.second)) {
      tx->set_status_to_abort();
      return HA_ERR_OUT_OF_MEM;
    }

    // set state for index_next to return EOF
    secondary_index_results_.push_back(current_plan_.start_key_serialized);
    current_position_in_index_ = 1;
    last_fetched_primary_key_ = current_plan_.start_key_serialized;
    return 0;
  } else {
    // Secondary UNIQUE: read_secondary_index → read primary key
    // Proxy adaptation: read_secondary_index returns vector<string> (primary keys)
    auto primary_keys = tx->read_secondary_index(
        current_index_name, current_plan_.start_key_serialized);

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }

    for (auto &pk : primary_keys) {
      secondary_index_results_.push_back(pk);
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    batch_fetch_secondary_payloads(tx);
    return fetch_and_set_current_result(buf, tx);
  }
}

int ha_helios::execute_same_key_materialize(uchar *buf,
                                               HeliosTransaction *tx) {
  const std::string &prefix = current_plan_.same_group_prefix_serialized;
  const std::string &prefix_end = current_plan_.same_group_end_serialized;

  if (current_plan_.is_primary) {
    // Push LIMIT only when the server can also apply the SELECT WHERE.
    const KEY *key = &table->key_info[active_index];
    RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        !select_scan_limit_is_safe(ha_thd(), table));
    // Autogen stages the forward, unlimited range and MySQL applies ORDER BY,
    // LIMIT and WHERE above the handler (see execute_range_materialize). The
    // DSL stages the limit and direction it names.
    if (statement_uses_read_plan(ha_thd()) && !tx->tx_plan_used()) {
      scan_limit = RangeScanLimit{};
    }

    // Same-key scans can use ASC or DESC LIMIT when ORDER BY matches the key.
    truncated_scan_end_ = prefix_end;
    auto key_values = tx->get_matching_keys_and_values_in_range(
        prefix, prefix_end, static_cast<uint64_t>(scan_limit.row_limit),
        scan_limit.reverse_scan, &materialized_scan_truncated_);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  secondary_index_results_ = tx->get_matching_primary_keys_in_range(
      current_index_name, prefix, prefix_end);
  batch_fetch_secondary_payloads(tx);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::execute_prefix_first(uchar *buf, HeliosTransaction *tx) {
  const std::string &prefix = current_plan_.same_group_prefix_serialized;
  const std::string &prefix_end = current_plan_.same_group_end_serialized;

  if (current_plan_.is_primary) {
    // Restrict to [prefix, prefix_end) so index_next never leaks non-prefix
    // rows. A limit-staged hit continues from the storage when index_next
    // reads past it.
    truncated_scan_end_ = prefix_end;
    auto key_values = tx->get_matching_keys_and_values_in_range(
        prefix, prefix_end, 0, false, &materialized_scan_truncated_);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_KEY_NOT_FOUND;
    }

    return fetch_and_set_current_result(buf, tx);
  }

  // Restrict to [prefix, prefix_end) so index_next never leaks non-prefix
  // rows.
  secondary_index_results_ = tx->get_matching_primary_keys_in_range(
      current_index_name, prefix, prefix_end);
  batch_fetch_secondary_payloads(tx);

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  return fetch_and_set_current_result(buf, tx);
}

/**
 * @brief kRangeMaterialize: range search (AFTER_KEY, KEY_OR_NEXT, etc.)
 */
int ha_helios::execute_range_materialize(uchar *buf,
                                            HeliosTransaction *tx) {
  std::string effective_start = current_plan_.start_key_serialized;
  std::string effective_end = current_plan_.end_key_serialized;

  // adjust start key based on find_flag
  if (current_plan_.find_flag == HA_READ_AFTER_KEY) {
    effective_start.push_back('\x00'); // exclude start key
  }

  // execute scan
  if (current_plan_.is_primary) {
    // Push LIMIT only when the server can also apply the SELECT WHERE.
    const KEY *key = &table->key_info[active_index];
    RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        !select_scan_limit_is_safe(ha_thd(), table));
    // Autogen stages only the forward, unlimited range; ORDER BY, LIMIT and
    // WHERE stay above the handler. The DSL stages the limit and direction it
    // names, and its plan matches them.
    if (statement_uses_read_plan(ha_thd()) && !tx->tx_plan_used()) {
      scan_limit = RangeScanLimit{};
    }

    // Range scans can use ASC or DESC LIMIT when ORDER BY matches the key.
    truncated_scan_end_ = effective_end;
    auto key_values = tx->get_matching_keys_and_values_in_range(
        effective_start, effective_end,
        static_cast<uint64_t>(scan_limit.row_limit), scan_limit.reverse_scan,
        &materialized_scan_truncated_);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, effective_start, effective_end);
    batch_fetch_secondary_payloads(tx);
  }

  // phantom detection check
  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::execute_prev_key(uchar *buf, HeliosTransaction *tx) {
  const std::string &target_key = current_plan_.start_key_serialized;
  // HA_READ_BEFORE_KEY : SQL < target → already exclusive end.
  // HA_READ_KEY_OR_PREV: SQL <= target → convert via build_prefix_range_end so
  //                      [begin, end) scan keeps target itself.
  const bool exclude_target = (current_plan_.find_flag == HA_READ_BEFORE_KEY);
  const std::string effective_end =
      exclude_target ? target_key : build_prefix_range_end(target_key);

  if (current_plan_.is_primary) {
    auto key_values =
        tx->get_matching_keys_and_values_in_range("", effective_end);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, "", effective_end);
    batch_fetch_secondary_payloads(tx);
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  current_position_in_index_ = secondary_index_results_.size() - 1;
  return fetch_and_set_current_result(buf, tx);
}

int ha_helios::execute_prefix_last(uchar *buf, HeliosTransaction *tx) {
  if (current_plan_.find_flag == HA_READ_PREFIX_LAST_OR_PREV) {
    const std::string &prefix = current_plan_.same_group_prefix_serialized;
    const std::string &prefix_end = current_plan_.same_group_end_serialized;

    if (current_plan_.is_primary) {
      // Push LIMIT only when the server can also apply the SELECT WHERE.
      const KEY *key = &table->key_info[active_index];
      RangeScanLimit scan_limit = range_scan_limit_for_order(
          ha_thd(), key, current_plan_.used_key_parts,
          !select_scan_limit_is_safe(ha_thd(), table));
      // Autogen stages the forward, unlimited range and MySQL applies ORDER BY,
      // LIMIT and WHERE above the handler (see execute_range_materialize). The
      // DSL stages the limit and direction it names.
      if (statement_uses_read_plan(ha_thd()) && !tx->tx_plan_used()) {
        scan_limit = RangeScanLimit{};
      }
      const bool push_desc_limit =
          (scan_limit.row_limit == 1 && scan_limit.reverse_scan);

      // Prefix-last can use reverse scan for ORDER BY ... DESC LIMIT 1.
      auto key_values = tx->get_matching_keys_and_values_in_range(
          prefix, prefix_end,
          push_desc_limit ? static_cast<uint64_t>(scan_limit.row_limit) : 0,
          push_desc_limit);
      if (key_values.empty()) {
        // HA_READ_PREFIX_LAST_OR_PREV may fall back to the previous prefix.
        key_values = tx->get_matching_keys_and_values_in_range(
            "", prefix,
            push_desc_limit ? static_cast<uint64_t>(scan_limit.row_limit) : 0,
            push_desc_limit);
      }
      for (auto &kv : key_values) {
        secondary_index_results_.push_back(kv.first);
        secondary_index_payloads_.push_back(std::move(kv.second));
      }
    } else {
      secondary_index_results_ = tx->get_matching_primary_keys_in_range(
          current_index_name, prefix, prefix_end);
      if (secondary_index_results_.empty()) {
        secondary_index_results_ = tx->get_matching_primary_keys_in_range(
            current_index_name, "", prefix);
      }
      batch_fetch_secondary_payloads(tx);
    }

    if (tx->is_aborted()) {
      return abort_errno(tx);
    }

    if (secondary_index_results_.empty()) {
      return HA_ERR_END_OF_FILE;
    }

    current_position_in_index_ = secondary_index_results_.size() - 1;
    return fetch_and_set_current_result(buf, tx);
  }

  // materialize mode
  if (current_plan_.is_primary) {
    // Push LIMIT only when the server can also apply the SELECT WHERE.
    const KEY *key = &table->key_info[active_index];
    RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        !select_scan_limit_is_safe(ha_thd(), table));
    // Autogen stages the forward, unlimited range and MySQL applies ORDER BY,
    // LIMIT and WHERE above the handler (see execute_range_materialize). The
    // DSL stages the limit and direction it names.
    if (statement_uses_read_plan(ha_thd()) && !tx->tx_plan_used()) {
      scan_limit = RangeScanLimit{};
    }
    const bool push_desc_limit =
        (scan_limit.row_limit == 1 && scan_limit.reverse_scan);

    // Prefix-last materialization only uses DESC LIMIT 1.
    auto key_values = tx->get_matching_keys_and_values_in_range(
        current_plan_.same_group_prefix_serialized,
        current_plan_.same_group_end_serialized,
        push_desc_limit ? static_cast<uint64_t>(scan_limit.row_limit) : 0,
        push_desc_limit);
    for (auto &kv : key_values) {
      secondary_index_results_.push_back(kv.first);
      secondary_index_payloads_.push_back(std::move(kv.second));
    }
  } else {
    // A prefix-last probe may hit a staged reverse LIMIT 1 secondary scan.
    const KEY *key = &table->key_info[active_index];
    RangeScanLimit scan_limit = range_scan_limit_for_order(
        ha_thd(), key, current_plan_.used_key_parts,
        !select_scan_limit_is_safe(ha_thd(), table));
    if (statement_uses_read_plan(ha_thd()) && !tx->tx_plan_used()) {
      scan_limit = RangeScanLimit{};
    }
    const bool push_desc_limit =
        (scan_limit.row_limit == 1 && scan_limit.reverse_scan);

    secondary_index_results_ = tx->get_matching_primary_keys_in_range(
        current_index_name, current_plan_.same_group_prefix_serialized,
        current_plan_.same_group_end_serialized, push_desc_limit ? 1 : 0,
        push_desc_limit);
    batch_fetch_secondary_payloads(tx);
  }

  if (tx->is_aborted()) {
    return abort_errno(tx);
  }

  if (secondary_index_results_.empty()) {
    return HA_ERR_END_OF_FILE;
  }

  // get the last element
  current_position_in_index_ = secondary_index_results_.size() - 1;
  return fetch_and_set_current_result(buf, tx);
}

void ha_helios::batch_fetch_secondary_payloads(HeliosTransaction *tx) {
  if (secondary_index_results_.empty()) return;

  auto results = tx->batch_read(secondary_index_results_);

  secondary_index_payloads_.clear();
  secondary_index_payloads_.reserve(results.size());
  for (auto &r : results) {
    secondary_index_payloads_.push_back(
        r.first ? std::move(r.second) : std::string());
  }
}

int ha_helios::fetch_and_set_current_result(uchar *buf,
                                               HeliosTransaction *tx) {
  if (secondary_index_results_.empty()) {
    return HA_ERR_KEY_NOT_FOUND;
  }

  std::string primary_key =
      secondary_index_results_[current_position_in_index_];

  tx->choose_table(db_table_name);

  const bool has_inline_value =
      current_position_in_index_ < secondary_index_payloads_.size();
  const std::byte *value_ptr = nullptr;
  size_t value_size = 0;

  if (has_inline_value) {
    const std::string &inline_value =
        secondary_index_payloads_[current_position_in_index_];
    if (inline_value.empty()) {
      // The base row vanished under a concurrent commit between the index walk
      // and the row fetch; the recorded range no longer replays at commit.
      tx->set_status_to_abort();
      return abort_errno(tx);
    }
    value_ptr = reinterpret_cast<const std::byte *>(inline_value.data());
    value_size = inline_value.size();
  } else {
    auto result = tx->read(primary_key);
    if (tx->is_aborted()) {
      return abort_errno(tx);
    }
    if (result.first == nullptr || result.second == 0) {
      return HA_ERR_KEY_NOT_FOUND;
    }
    value_ptr = result.first;
    value_size = result.second;
  }

  if (set_fields_from_helios(buf, value_ptr, value_size)) {
    tx->set_status_to_abort();
    return HA_ERR_OUT_OF_MEM;
  }

  current_position_in_index_++;
  last_fetched_primary_key_ = primary_key;
  return 0;
}
