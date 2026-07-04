#include "storage/lineairdb/ha_lineairdb.hh"

#include <string>
#include <string_view>

#include "lineairdb_pushdown.hh"
#include "sql/item.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

// Handler-side bridge used by the aggregate pushdown executor.

/**
 * @brief Attach an aggregate spec and its server-side WHERE filter to the tx.
 *
 * Server aggregation folds base rows into group rows, so MySQL cannot recheck
 * the original WHERE afterward. If the WHERE cannot be serialized, return
 * false and let the caller aggregate locally.
 */
bool ha_lineairdb::tx_set_pushed_aggregate(const std::string &s) {
  auto tx = get_transaction(ha_thd());
  // agg_next_raw bypasses the usual read path that selects the table.
  tx->choose_table(db_table_name);
  // Aggregation needs only WHERE serialization; LIMIT safety is irrelevant here.
  prepare_select_filter_for_tx(ha_thd(), table, tx, nullptr);
  const Item *agg_where = nullptr;
  if (ha_thd()->lex != nullptr && ha_thd()->lex->unit != nullptr) {
    Query_block *qb = ha_thd()->lex->unit->global_parameters();
    if (qb != nullptr) agg_where = qb->where_cond();
  }
  if (agg_where != nullptr && tx->get_pushed_filter().empty()) {
    return false;  // WHERE exists but could not be fully serialized
  }
  tx->set_pushed_aggregate(s);
  return true;
}

/**
 * @brief Return true when the current transaction has aborted during staging.
 */
bool ha_lineairdb::tx_is_aborted() {
  auto tx = get_transaction(ha_thd());
  return tx == nullptr || tx->is_aborted();
}

/**
 * @brief Clear the aggregate spec from the current transaction.
 */
void ha_lineairdb::tx_clear_pushed_aggregate() {
  get_transaction(ha_thd())->clear_pushed_aggregate();
}

/**
 * @brief Return the next raw group row from the staged scan cache.
 *
 * This is the aggregate equivalent of rnd_next(): it advances the same cursor
 * but returns server-produced group-row bytes instead of unpacking a base row
 * into MySQL's record buffer.
 */
bool ha_lineairdb::agg_next_raw(std::string_view *out_value) {
  // Surface staging aborts instead of treating them as clean EOF.
  {
    auto tx = get_transaction(ha_thd());
    if (tx != nullptr && tx->is_aborted()) return false;
  }
  if (buffer_position_ >= scanned_keys_.size()) {
    if (scan_exhausted_) return false;
    if (!fetch_next_batch()) {
      scan_exhausted_ = true;
      return false;
    }
  }
  if (buffer_position_ >= scanned_values_.size()) return false;
  const auto &value = scanned_values_[buffer_position_];
  buffer_position_++;
  *out_value = std::string_view(reinterpret_cast<const char *>(value.data()),
                                value.size());
  current_position_++;
  return true;
}
