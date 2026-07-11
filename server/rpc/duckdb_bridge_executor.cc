// DuckDB bridge executor: runs a TX_EXECUTE_SQL_DUCKDB request's verbatim SQL
// text on the server's embedded DuckDB executor, whose table functions read
// the live PaxStore instances in place (no data copy).
// duckdb_bridge_dispatch.cc routes the opcode here.
//
// OCC contract, shared with query_block_executor.cc's PrepareTables /
// TablesAreStillQuiet (duplicated here; that class is private to
// query_block_executor.cc's anonymous namespace): per table, reject when any
// heap-fallback (overflow) rows exist, then capture slots_allocated(),
// overflow_count(), and every allocated group's write_counter, rejecting when
// any counter is odd (a write is in progress). After the full result set has
// been produced, the captured write state is re-checked; any difference means a
// concurrent write raced the read, and the whole response is discarded as a
// "concurrent modification" error rather than risking a torn read.

#include "duckdb_bridge_executor.hh"

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace duckdb_bridge {
namespace {

namespace pb = LineairDB::Protocol;
namespace pax = LineairDB::Pax;
using pax::PaxGroup;
using pax::PaxStore;

// ---------------------------------------------------------------------------
// OCC write-state capture and quiescence re-check (see file header).
// ---------------------------------------------------------------------------

/**
 * @brief Per-table write state captured before execution. Members mirror the
 * PaxStore accessors they were read from.
 */
struct TableWriteState {
  PaxStore* store = nullptr;  // live server storage, never owned here
  uint64_t slots_allocated = 0;
  uint64_t overflow_count = 0;
  std::vector<uint64_t> write_counters;
};

/**
 * @brief Resolves every requested table to its PaxStore and captures its
 * write state.
 *
 * @details Rejects a table that has heap-fallback (overflow) rows or an odd
 * write counter (a write is in progress); see the file header for the
 * contract.
 *
 * @param db Live server database.
 * @param request Request whose tables() list every base table.
 * @param out Receives one TableWriteState per requested table.
 * @param error Receives the rejection reason.
 * @return false when any table cannot be captured; *out is incomplete.
 */
bool CaptureTableWriteStates(LineairDB::Database* db,
                             const pb::TxExecuteSqlDuckdb::Request& request,
                             std::vector<TableWriteState>* out,
                             std::string* error) {
  out->clear();
  out->reserve(request.tables_size());
  for (const auto& table_desc : request.tables()) {
    PaxStore* store = db->GetPaxStore(table_desc.table_name());
    if (store == nullptr) {
      *error = "table has no PAX store: " + table_desc.table_name();
      return false;
    }
    const uint64_t overflow_count = store->overflow_count();
    if (overflow_count > 0) {
      *error = "table has heap fallback rows: " + table_desc.table_name();
      return false;
    }

    TableWriteState write_state;
    write_state.store = store;
    write_state.slots_allocated = store->slots_allocated();
    write_state.overflow_count = overflow_count;
    write_state.write_counters.resize(store->group_count());
    for (size_t group_index = 0;
         group_index < write_state.write_counters.size(); group_index++) {
      PaxGroup* group = store->group(group_index);
      const uint64_t write_counter =
          group == nullptr ? 0
                          : group->write_counter.load(std::memory_order_acquire);
      write_state.write_counters[group_index] = write_counter;
      if ((write_counter & 1u) != 0) {
        *error = "table has in-progress PAX writes: " + table_desc.table_name();
        return false;
      }
    }
    out->push_back(std::move(write_state));
  }
  return true;
}

}  // namespace

void ExecuteSql(LineairDB::Database* db,
                const pb::TxExecuteSqlDuckdb::Request& request,
                pb::TxExecuteSqlDuckdb::Response* response) {
  if (response == nullptr) return;
  response->Clear();

  if (db == nullptr) {
    response->set_ok(false);
    response->set_error("database is unavailable");
    return;
  }

  std::vector<TableWriteState> captured_tables;
  std::string error;
  if (!CaptureTableWriteStates(db, request, &captured_tables, &error)) {
    response->set_ok(false);
    response->set_error(error);
    return;
  }

  response->set_ok(false);
  response->set_error("duckdb bridge: execution stage is not present");
}

}  // namespace duckdb_bridge
