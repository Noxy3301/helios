// DuckDB bridge executor: runs a TX_EXECUTE_SQL_DUCKDB request's verbatim SQL
// text on the embedded DuckDB executor, whose table functions read the live
// PaxStore instances in place (no data copy). DuckDB contributes its parser,
// planner, and vectorized execution runtime; no table data ever lives inside
// DuckDB. duckdb_bridge_dispatch.cc routes the opcode here.
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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <duckdb.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

namespace duckdb_bridge {
namespace {

namespace pb = LineairDB::Protocol;
namespace pax = LineairDB::Pax;
using pax::FK_DATE;
using pax::FK_DEC64;
using pax::FK_INT32;
using pax::FK_INT64;
using pax::FK_UNTYPED;
using pax::PaxGroup;
using pax::PaxStore;

// ---------------------------------------------------------------------------
// Proxy row-format encoding.
// ---------------------------------------------------------------------------

/**
 * @brief Number of little-endian bytes needed to encode `length`.
 */
uint32_t LengthPrefixBytes(uint32_t length) {
  uint32_t prefix_bytes = 0;
  for (uint32_t value = length; value > 0; value /= 256) prefix_bytes++;
  return prefix_bytes;
}

/**
 * @brief Appends one field in the proxy row format (matches
 * proxy/ha_lineairdb_columnar.cc's DecodeRowFields).
 *
 * @details One byte length-width tag (0xFF = SQL NULL), then that many
 * little-endian length bytes, then the payload. NULL and empty string are
 * distinct and must not be conflated: `is_null` is the only signal for the
 * 0xFF sentinel. An empty-but-non-null payload (e.g. a genuine VARCHAR ''
 * result) takes the normal path with length 0 -- exactly one tag byte 0x00,
 * no length bytes, no payload -- which DecodeRowFields reads as {ptr, len=0,
 * empty=false}, distinct from the {nullptr, 0, empty=true} it produces for
 * the sentinel. Nullness is never inferred from payload.empty(); this
 * mirrors query_block_executor.cc's append_result_field(), which takes the
 * same explicit `is_null` bool.
 *
 * @param out Destination row buffer.
 * @param payload Field bytes; ignored when is_null.
 * @param is_null True encodes the SQL NULL sentinel.
 */
void AppendProxyField(std::string& out, std::string_view payload,
                      bool is_null) {
  if (is_null) {
    out.push_back(static_cast<char>(0xFF));
    return;
  }
  const uint32_t length = static_cast<uint32_t>(payload.size());
  const uint32_t prefix_bytes = LengthPrefixBytes(length);
  out.push_back(static_cast<char>(prefix_bytes));
  for (uint32_t i = 0; i < prefix_bytes; i++) {
    out.push_back(static_cast<char>((length >> (8 * i)) & 0xFF));
  }
  out.append(payload.data(), payload.size());
}

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

/**
 * @brief Re-reads every captured counter and compares against the capture.
 *
 * @details An odd counter or any changed value means a write overlapped the
 * read window; the caller must discard the result.
 *
 * @param captured_tables Write states captured before execution.
 * @return true when no referenced table changed since capture.
 */
bool WriteStateUnchanged(const std::vector<TableWriteState>& captured_tables) {
  for (const TableWriteState& state : captured_tables) {
    if (state.store->slots_allocated() != state.slots_allocated ||
        state.store->overflow_count() != state.overflow_count) {
      return false;
    }
    for (size_t group_index = 0; group_index < state.write_counters.size();
         group_index++) {
      PaxGroup* group = state.store->group(group_index);
      const uint64_t current =
          group == nullptr ? 0
                          : group->write_counter.load(std::memory_order_acquire);
      if ((current & 1u) != 0 || current != state.write_counters[group_index]) {
        return false;
      }
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Per-table view over live PAX storage.
// ---------------------------------------------------------------------------

/**
 * @brief PAX cell metadata for one column, from the request's ColumnDesc.
 */
struct ColumnSpec {
  std::string name;
  uint8_t kind = FK_UNTYPED;
  uint32_t width = 0;
  int8_t scale = 0;
};

/**
 * @brief Per-request description of one live PAX table.
 *
 * @details Column metadata comes from the request: the proxy recomputes
 * kind/width/scale from TABLE::field[] with the pure function used at CREATE
 * TABLE time (see proxy/lineairdb_field_types.h), matching what the server
 * stored while the schema is unchanged.
 */
struct PaxTableView {
  std::string sql_name;  // bare name the SQL text references, e.g. "lineitem"
  PaxStore* store = nullptr;
  std::vector<ColumnSpec> columns;
  size_t group_count = 0;  // fixed at capture time, not live store state
};

// ---------------------------------------------------------------------------
// DuckDB table function over PaxTableView: parallel scan, projection
// pushdown, bulk decode for typed columns, inline string_t for short strings.
// ---------------------------------------------------------------------------

using duckdb::ClientContext;
using duckdb::column_t;
using duckdb::Connection;
using duckdb::DataChunk;
using duckdb::Date;
using duckdb::date_t;
using duckdb::ExecutionContext;
using duckdb::FlatVector;
using duckdb::FunctionData;
using duckdb::GlobalTableFunctionState;
using duckdb::hugeint_t;
using duckdb::idx_t;
using duckdb::LocalTableFunctionState;
using duckdb::LogicalType;
using duckdb::PhysicalType;
using duckdb::string;
using duckdb::StringVector;
using duckdb::TableFunction;
using duckdb::TableFunctionBindInput;
using duckdb::TableFunctionInfo;
using duckdb::TableFunctionInitInput;
using duckdb::TableFunctionInput;
using duckdb::unique_ptr;
using duckdb::Vector;
using duckdb::vector;

/**
 * @brief Registration-time handle carrying the request's PaxTableView into
 * PaxBind.
 */
struct PaxTableInfo : public TableFunctionInfo {
  PaxTableView* table = nullptr;
};

/**
 * @brief Bound function data; refers to the request-owned PaxTableView.
 */
struct PaxBindData : public FunctionData {
  PaxTableView* table = nullptr;

  unique_ptr<FunctionData> Copy() const override {
    auto copy = duckdb::make_uniq<PaxBindData>();
    copy->table = table;
    return std::move(copy);
  }
  bool Equals(const FunctionData& other) const override {
    return this == &other;
  }
};

/**
 * @brief Shared scan state: the group claim counter and the projected column
 * ids.
 */
struct PaxGlobalState : public GlobalTableFunctionState {
  std::atomic<uint64_t> next_group{0};
  size_t group_count = 0;
  idx_t threads = 1;
  std::vector<column_t> column_ids;

  idx_t MaxThreads() const override { return threads; }
};

/**
 * @brief Per-thread scan cursor over the currently claimed group.
 */
struct PaxLocalState : public LocalTableFunctionState {
  uint32_t current_group = UINT32_MAX;
  uint32_t current_slot = 0;
  PaxGroup* group_ptr = nullptr;
};

/**
 * @brief Maps a PAX FieldKind to the DuckDB column type.
 */
LogicalType FieldKindToLogicalType(uint8_t kind, int8_t scale) {
  switch (kind) {
    case FK_INT32:
      return LogicalType::INTEGER;
    case FK_INT64:
      return LogicalType::BIGINT;
    case FK_DATE:
      return LogicalType::DATE;
    case FK_DEC64:
      // Width 15 matches the DEC64 typing rule (precision <= 15); see
      // LineairDB::Pax::FieldKind.
      return LogicalType::DECIMAL(15, static_cast<uint8_t>(scale));
    default:
      return LogicalType::VARCHAR;
  }
}

/**
 * @brief Declares the table's column names and types from its ColumnSpec
 * list.
 */
unique_ptr<FunctionData> PaxBind(ClientContext&, TableFunctionBindInput& input,
                                 vector<LogicalType>& return_types,
                                 vector<string>& names) {
  auto& info = input.info->Cast<PaxTableInfo>();
  auto bind_data = duckdb::make_uniq<PaxBindData>();
  bind_data->table = info.table;
  for (const auto& column : info.table->columns) {
    return_types.push_back(FieldKindToLogicalType(column.kind, column.scale));
    names.push_back(column.name);
  }
  return std::move(bind_data);
}

/**
 * @brief Builds the shared scan state: the projected column ids and a thread
 * count of min(hardware threads, group count).
 */
unique_ptr<GlobalTableFunctionState> PaxInitGlobal(
    ClientContext&, TableFunctionInitInput& input) {
  const auto& bind_data = input.bind_data->Cast<PaxBindData>();
  PaxTableView& table_view = *bind_data.table;
  auto global_state = duckdb::make_uniq<PaxGlobalState>();
  global_state->group_count = table_view.group_count;
  global_state->next_group = 0;

  const size_t column_count = table_view.columns.size();
  if (!input.column_ids.empty()) {
    for (auto column_id : input.column_ids) {
      if (column_id < column_count) {
        global_state->column_ids.push_back(static_cast<column_t>(column_id));
      }
    }
    if (global_state->column_ids.empty()) {
      for (column_t column = 0; column < column_count; column++) {
        global_state->column_ids.push_back(column);
      }
    }
  } else {
    for (column_t column = 0; column < column_count; column++) {
      global_state->column_ids.push_back(column);
    }
  }

  const unsigned hardware_threads =
      std::max(1u, std::thread::hardware_concurrency());
  global_state->threads = static_cast<idx_t>(std::max<size_t>(
      1, std::min<size_t>(hardware_threads,
                          std::max<size_t>(1, table_view.group_count))));
  return std::move(global_state);
}

/**
 * @brief Creates the per-thread scan cursor.
 */
unique_ptr<LocalTableFunctionState> PaxInitLocal(ExecutionContext&,
                                                 TableFunctionInitInput&,
                                                 GlobalTableFunctionState*) {
  return duckdb::make_uniq<PaxLocalState>();
}

/**
 * @brief Writes a scaled DEC64 mantissa through the vector's physical type.
 *
 * @details DuckDB stores DECIMAL(p, s) in the narrowest integer type that
 * fits p; the mantissa is written as that type.
 */
inline void WriteDecimalPhysical(Vector& output_vector, idx_t row,
                                 int64_t mantissa, PhysicalType physical_type) {
  switch (physical_type) {
    case PhysicalType::INT16:
      FlatVector::GetData<int16_t>(output_vector)[row] =
          static_cast<int16_t>(mantissa);
      break;
    case PhysicalType::INT32:
      FlatVector::GetData<int32_t>(output_vector)[row] =
          static_cast<int32_t>(mantissa);
      break;
    case PhysicalType::INT64:
      FlatVector::GetData<int64_t>(output_vector)[row] = mantissa;
      break;
    default:
      FlatVector::GetData<hugeint_t>(output_vector)[row] =
          hugeint_t(mantissa < 0 ? -1 : 0, static_cast<uint64_t>(mantissa));
      break;
  }
}

/**
 * @brief Bulk-decodes one typed (non-UNTYPED) column across a contiguous run
 * of visible slots.
 *
 * @details A typed cell is [u16 len][fixed-width LE payload]; a length equal
 * to the column width means the payload is present, any other length (an
 * empty cell) is SQL NULL.
 */
void BulkDecodeTyped(uint8_t kind, const PaxGroup& group, size_t field,
                     uint32_t width, uint32_t slot_start, uint32_t count,
                     Vector& output_vector, idx_t out_base,
                     PhysicalType decimal_physical_type) {
  const std::byte* strip_base = group.strip(field);
  const uint32_t stride = group.stride(field);
  const std::byte* src = strip_base + static_cast<size_t>(stride) * slot_start;
  constexpr uint32_t kCellLenBytes = PaxGroup::kCellLenBytes;

  switch (kind) {
    case FK_INT32: {
      int32_t* dst = FlatVector::GetData<int32_t>(output_vector) + out_base;
      for (uint32_t i = 0; i < count; i++, src += stride) {
        uint16_t cell_length;
        std::memcpy(&cell_length, src, sizeof(cell_length));
        if (cell_length == width) {
          std::memcpy(&dst[i], src + kCellLenBytes, sizeof(int32_t));
        } else {
          dst[i] = 0;
          FlatVector::SetNull(output_vector, out_base + i, true);
        }
      }
      break;
    }
    case FK_INT64: {
      int64_t* dst = FlatVector::GetData<int64_t>(output_vector) + out_base;
      for (uint32_t i = 0; i < count; i++, src += stride) {
        uint16_t cell_length;
        std::memcpy(&cell_length, src, sizeof(cell_length));
        if (cell_length == width) {
          std::memcpy(&dst[i], src + kCellLenBytes, sizeof(int64_t));
        } else {
          dst[i] = 0;
          FlatVector::SetNull(output_vector, out_base + i, true);
        }
      }
      break;
    }
    case FK_DATE: {
      date_t* dst = FlatVector::GetData<date_t>(output_vector) + out_base;
      for (uint32_t i = 0; i < count; i++, src += stride) {
        uint16_t cell_length;
        std::memcpy(&cell_length, src, sizeof(cell_length));
        if (cell_length == width) {
          int32_t ymd;
          std::memcpy(&ymd, src + kCellLenBytes, sizeof(ymd));
          dst[i] = Date::FromDate(ymd / 10000, (ymd / 100) % 100, ymd % 100);
        } else {
          FlatVector::SetNull(output_vector, out_base + i, true);
        }
      }
      break;
    }
    case FK_DEC64: {
      for (uint32_t i = 0; i < count; i++, src += stride) {
        uint16_t cell_length;
        std::memcpy(&cell_length, src, sizeof(cell_length));
        if (cell_length == width) {
          int64_t mantissa;
          std::memcpy(&mantissa, src + kCellLenBytes, sizeof(mantissa));
          WriteDecimalPhysical(output_vector, out_base + i, mantissa,
                               decimal_physical_type);
        } else {
          FlatVector::SetNull(output_vector, out_base + i, true);
        }
      }
      break;
    }
    default:
      break;  // FK_UNTYPED never reaches here
  }
}

/**
 * @brief Decodes one untyped cell into a VARCHAR vector slot.
 *
 * @details An empty untyped cell is SQL NULL; short payloads inline into
 * string_t, longer ones copy into the vector's string heap.
 */
inline void DecodeUntypedCell(const PaxGroup& group, size_t field,
                              uint32_t slot, Vector& output_vector,
                              idx_t out_row) {
  const std::string_view cell_value = group.cell(field, slot);
  if (cell_value.empty()) {
    FlatVector::SetNull(output_vector, out_row, true);
  } else if (cell_value.size() <= duckdb::string_t::INLINE_LENGTH) {
    FlatVector::GetData<duckdb::string_t>(output_vector)[out_row] =
        duckdb::string_t(cell_value.data(),
                         static_cast<uint32_t>(cell_value.size()));
  } else {
    FlatVector::GetData<duckdb::string_t>(output_vector)[out_row] =
        StringVector::AddString(output_vector, cell_value.data(),
                                cell_value.size());
  }
}

/**
 * @brief Scan worker: fills one output chunk from claimed PAX groups.
 *
 * @details Threads claim whole groups from the shared next_group counter and
 * decode contiguous visible-slot runs column-at-a-time.
 */
void PaxScan(ClientContext&, TableFunctionInput& data, DataChunk& output) {
  const auto& bind_data = data.bind_data->Cast<PaxBindData>();
  auto& global_state = data.global_state->Cast<PaxGlobalState>();
  auto& local_state = data.local_state->Cast<PaxLocalState>();

  PaxTableView& table_view = *bind_data.table;
  PaxStore* store = table_view.store;
  const idx_t max_rows = output.GetCapacity();
  idx_t rows_emitted = 0;

  struct ColumnContext {
    uint8_t kind;
    size_t field;
    uint32_t width;
    PhysicalType decimal_physical_type;
  };
  std::vector<ColumnContext> scan_columns(global_state.column_ids.size());
  for (idx_t i = 0; i < global_state.column_ids.size(); i++) {
    const column_t column = global_state.column_ids[i];
    const ColumnSpec& spec = table_view.columns[column];
    scan_columns[i].kind = spec.kind;
    scan_columns[i].field = static_cast<size_t>(column) + 1;  // field 0 is null flags
    scan_columns[i].width = spec.width;
    scan_columns[i].decimal_physical_type =
        (spec.kind == FK_DEC64) ? output.data[i].GetType().InternalType()
                                : PhysicalType::INVALID;
  }

  while (rows_emitted < max_rows) {
    if (local_state.group_ptr == nullptr ||
        local_state.current_slot >= PaxGroup::kRows) {
      uint32_t claimed = UINT32_MAX;
      const uint64_t next =
          global_state.next_group.fetch_add(1, std::memory_order_relaxed);
      if (next < global_state.group_count) {
        claimed = static_cast<uint32_t>(next);
      }
      if (claimed == UINT32_MAX) break;
      local_state.group_ptr = store->group(claimed);
      local_state.current_group = claimed;
      local_state.current_slot = 0;
      if (local_state.group_ptr == nullptr) continue;
    }

    PaxGroup* group = local_state.group_ptr;
    const uint32_t slot = local_state.current_slot;
    if (slot >= PaxGroup::kRows) {
      local_state.group_ptr = nullptr;
      continue;
    }
    if (!group->IsVisible(slot)) {
      local_state.current_slot = slot + 1;
      continue;
    }

    const uint32_t max_run_length = std::min<uint32_t>(
        PaxGroup::kRows - slot, static_cast<uint32_t>(max_rows - rows_emitted));
    uint32_t run_length = 1;
    while (run_length < max_run_length && group->IsVisible(slot + run_length)) {
      run_length++;
    }

    for (idx_t i = 0; i < scan_columns.size(); i++) {
      const ColumnContext& column = scan_columns[i];
      if (column.kind == FK_UNTYPED) {
        for (uint32_t row = 0; row < run_length; row++) {
          DecodeUntypedCell(*group, column.field, slot + row, output.data[i],
                            rows_emitted + row);
        }
      } else {
        BulkDecodeTyped(column.kind, *group, column.field, column.width, slot,
                        run_length, output.data[i], rows_emitted,
                        column.decimal_physical_type);
      }
    }

    rows_emitted += run_length;
    local_state.current_slot = slot + run_length;
  }

  output.SetCardinality(rows_emitted);
}

// ---------------------------------------------------------------------------
// Per-request catalog registration. Serialized by GlobalCatalogMutex().
// ---------------------------------------------------------------------------

/**
 * @brief Registers the scan table function for one table.
 *
 * @details `nonce` disambiguates the function name across requests sharing
 * the runtime's system catalog: each request's PaxTableView objects live
 * only for that request's stack frame, and a stale function entry from a
 * prior request must never be reachable by name again.
 *
 * @return The registered function name.
 */
std::string RegisterPaxFunction(Connection& connection,
                                PaxTableView& table_view, size_t table_index,
                                uint64_t nonce) {
  auto info = duckdb::make_shared_ptr<PaxTableInfo>();
  info->table = &table_view;

  const std::string function_name =
      "pax_scan_" + std::to_string(nonce) + "_" + std::to_string(table_index);
  TableFunction function(function_name, {}, PaxScan, PaxBind, PaxInitGlobal,
                         PaxInitLocal);
  function.projection_pushdown = true;
  function.filter_pushdown = false;
  function.function_info = info;

  connection.context->RunFunctionInTransaction([&]() {
    auto& catalog = duckdb::Catalog::GetSystemCatalog(*connection.context);
    duckdb::CreateTableFunctionInfo create_info(function);
    catalog.CreateTableFunction(*connection.context, create_info);
  });
  // RunFunctionInTransaction starts a transaction when the connection is in
  // auto-commit mode but does not commit it. Committing explicitly here,
  // immediately after the mutation that opened it, keeps every subsequent
  // statement in this request looking at a clean, fully-committed catalog
  // state instead of an open transaction carried across statements.
  if (connection.HasActiveTransaction()) connection.Commit();
  return function_name;
}

/**
 * @brief Creates the bare view for one table's already-registered function.
 *
 * @details The view name is NOT nonce-disambiguated: it must equal the table
 * name the client SQL references, hence CREATE OR REPLACE (idempotent across
 * requests, and GlobalCatalogMutex() already serializes this section).
 *
 * Only the bare, unqualified name may be registered here. A schema-qualified
 * sibling view ("db"."table") must not be added: with two or more different
 * custom table-function-backed tables in one query, a qualified view makes
 * DuckDB's (v1.5.4) binder intermittently fail to resolve the second table
 * ("Catalog Error: Table ... does not exist"). Db-qualified table references
 * are instead stripped from the SQL text on the proxy side.
 */
void RegisterPaxView(Connection& connection, PaxTableView& table_view,
                     const std::string& function_name) {
  // sql_name is a MySQL identifier straight from TABLE_SHARE, not
  // attacker-controlled SQL text; double-quoting it is a safe-identifier
  // quoting step, not string-built SQL from client input.
  const std::string view_sql = "CREATE OR REPLACE VIEW \"" +
                               table_view.sql_name + "\" AS SELECT * FROM " +
                               function_name + "();";
  auto view_result = connection.Query(view_sql);
  if (view_result->HasError()) {
    throw std::runtime_error("CREATE VIEW " + table_view.sql_name +
                             " failed: " + view_result->GetError());
  }
}

// ---------------------------------------------------------------------------
// Result -> proxy row format.
// ---------------------------------------------------------------------------

/**
 * @brief Encodes one result row into the proxy row format.
 *
 * @details Uses DuckDB's own Value::ToString(): an exact fixed-point
 * representation for DECIMAL (no precision loss) and ISO "YYYY-MM-DD" for
 * DATE, matching this codebase's ASCII row-value convention.
 *
 * DECIMAL division / AVG: DuckDB resolves AVG() of a DECIMAL column (and any
 * DECIMAL/DECIMAL division) to DOUBLE by design
 * (https://duckdb.org/docs/stable/sql/data_types/numeric). Value::ToString()
 * on that DOUBLE uses shortest-round-trip formatting; the proxy reformats it
 * to MySQL's decimals convention with exact string/integer rounding once the
 * row crosses the wire.
 *
 * KNOWN LIMITATION: that proxy-side reformatting corrects display scale
 * only. The value itself went through DOUBLE division, and at large enough
 * magnitudes double's ~15-17 significant decimal digits could place the true
 * value on the wrong side of a rounding boundary relative to MySQL's exact
 * fixed-point computation. An exact fix would compute DECIMAL division in
 * integer arithmetic (DuckDB extension or server-side) instead of trusting
 * the DOUBLE result.
 */
void EncodeRow(duckdb::MaterializedQueryResult& result, idx_t row_index,
               std::string* out) {
  out->clear();
  // Field 0 mirrors the row null-flags field of the proxy row format; the
  // proxy does not read it for bridge results.
  AppendProxyField(*out, "", /*is_null=*/true);
  for (idx_t column_index = 0; column_index < result.ColumnCount();
       column_index++) {
    const duckdb::Value value = result.GetValue(column_index, row_index);
    // Check is_null BEFORE looking at the text: a genuine empty-string result
    // (value.ToString() == "") is a valid non-null value, not a signal for
    // the NULL sentinel. The text is only computed in the non-null case.
    if (value.IsNull()) {
      AppendProxyField(*out, "", /*is_null=*/true);
    } else {
      const std::string text = value.ToString();
      AppendProxyField(*out, text, /*is_null=*/false);
    }
  }
}

// ---------------------------------------------------------------------------
// Process-lifetime state.
// ---------------------------------------------------------------------------

/**
 * @brief Process-lifetime DuckDB runtime.
 *
 * @details The bridge borrows DuckDB's parser, planner, and vectorized
 * executor; the in-memory duckdb::DuckDB instance holds no table data, and
 * its system catalog only ever contains this bridge's table functions and
 * views. The function-local static gives thread-safe, exactly-once
 * construction: the first request pays the construction cost, every later
 * request on any thread reuses the instance. This follows DuckDB's
 * documented concurrency model
 * (https://duckdb.org/docs/stable/connect/concurrency): one shared instance,
 * one fresh Connection per request/thread.
 */
duckdb::DuckDB& GlobalRuntime() {
  static duckdb::DuckDB runtime(nullptr);  // nullptr: in-memory, no db file
  return runtime;
}

/**
 * @brief Serializes each request's register-views/run-query/encode-results
 * section against the shared system catalog.
 *
 * @details View names must equal the table names the client SQL references
 * (e.g. "lineitem"), and the runtime has one shared system catalog; two
 * concurrent requests touching the same table would race on CREATE OR
 * REPLACE VIEW. There is no per-request schema isolation; single-statement
 * callers do not contend, and the whole response is bounded by the OCC
 * re-check regardless.
 */
std::mutex& GlobalCatalogMutex() {
  static std::mutex mutex;
  return mutex;
}

/**
 * @brief Disambiguates table-function names (not view names, see
 * RegisterPaxView) across requests sharing the runtime's system catalog.
 */
std::atomic<uint64_t> g_request_nonce{0};

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

  // Stage timing, off by default: ENABLE_DUCKDB_BRIDGE_TRACE logs a
  // one-line stderr breakdown per request (write-state capture / view build /
  // catalog-mutex wait + registration / query / row encoding).
  // Enabled when set to anything but empty or "0", matching ENABLE_RPC_TRACE
  static const bool trace = [] {
    const char* value = std::getenv("ENABLE_DUCKDB_BRIDGE_TRACE");
    return value != nullptr && value[0] != '\0' &&
           std::string_view(value) != "0";
  }();
  using Clock = std::chrono::steady_clock;
  Clock::time_point trace_start, after_capture, after_view_build,
      after_register, after_query, after_encode;

  try {
    if (trace) trace_start = Clock::now();
    std::vector<TableWriteState> captured_tables;
    std::string error;
    if (!CaptureTableWriteStates(db, request, &captured_tables, &error)) {
      response->set_ok(false);
      response->set_error(error);
      return;
    }
    if (trace) after_capture = Clock::now();

    std::vector<PaxTableView> table_views(
        static_cast<size_t>(request.tables_size()));
    for (int i = 0; i < request.tables_size(); i++) {
      const pb::TxExecuteSqlDuckdb::TableDesc& table_desc = request.tables(i);
      PaxTableView& table_view = table_views[static_cast<size_t>(i)];
      table_view.sql_name = table_desc.sql_name();
      table_view.store = captured_tables[static_cast<size_t>(i)].store;
      table_view.group_count =
          captured_tables[static_cast<size_t>(i)].write_counters.size();
      table_view.columns.reserve(static_cast<size_t>(table_desc.columns_size()));
      for (const auto& column : table_desc.columns()) {
        ColumnSpec spec;
        spec.name = column.name();
        spec.kind = static_cast<uint8_t>(column.pax_kind());
        spec.width = column.pax_width();
        spec.scale = static_cast<int8_t>(column.pax_scale());
        table_view.columns.push_back(std::move(spec));
      }
    }

    if (trace) after_view_build = Clock::now();
    const uint64_t nonce =
        g_request_nonce.fetch_add(1, std::memory_order_relaxed);

    std::unique_ptr<duckdb::MaterializedQueryResult> result;
    {
      // This section touches the runtime's shared system catalog (table
      // functions and fixed-name views); see GlobalCatalogMutex.
      std::lock_guard<std::mutex> catalog_lock(GlobalCatalogMutex());
      Connection connection(GlobalRuntime());
      for (size_t i = 0; i < table_views.size(); i++) {
        const std::string function_name =
            RegisterPaxFunction(connection, table_views[i], i, nonce);
        RegisterPaxView(connection, table_views[i], function_name);
      }
      if (trace) after_register = Clock::now();

      result = connection.Query(request.sql());
      if (trace) after_query = Clock::now();
      if (result->HasError()) {
        response->set_ok(false);
        response->set_error(result->GetError());
        return;
      }

      std::string row;
      for (idx_t row_index = 0; row_index < result->RowCount(); row_index++) {
        EncodeRow(*result, row_index, &row);
        response->add_rows(row);
      }
    }
    if (trace) after_encode = Clock::now();

    if (!WriteStateUnchanged(captured_tables)) {
      response->Clear();
      response->set_ok(false);
      response->set_error("concurrent modification");
      return;
    }

    if (trace) {
      auto milliseconds = [](Clock::time_point from, Clock::time_point to) {
        return std::chrono::duration<double, std::milli>(to - from).count();
      };
      std::fprintf(
          stderr,
          "[duckdb_bridge] capture=%.1fms view_build=%.1fms "
          "lock_wait+register=%.1fms query=%.1fms encode=%.1fms rows=%llu\n",
          milliseconds(trace_start, after_capture),
          milliseconds(after_capture, after_view_build),
          milliseconds(after_view_build, after_register),
          milliseconds(after_register, after_query),
          milliseconds(after_query, after_encode),
          static_cast<unsigned long long>(result->RowCount()));
    }

    response->set_ok(true);
  } catch (const std::exception& exception) {
    response->Clear();
    response->set_ok(false);
    response->set_error(exception.what());
  } catch (...) {
    response->Clear();
    response->set_ok(false);
    response->set_error("duckdb bridge execution failed");
  }
}

}  // namespace duckdb_bridge
