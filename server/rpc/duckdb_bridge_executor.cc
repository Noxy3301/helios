// DuckDB bridge executor: runs a TX_EXECUTE_SQL_DUCKDB request's verbatim SQL
// text on the embedded DuckDB executor, whose table functions read the live
// PaxStore instances -- in place for groups without captured entries,
// through captured before-images otherwise. DuckDB contributes its parser,
// planner, and vectorized execution runtime; no table data ever lives inside
// DuckDB. duckdb_bridge_dispatch.cc routes the opcode here.
//
// Consistency: the request runs against a columnar read view with cut epoch
// E (Database::AcquirePaxReadView). Groups without captured entries are
// bulk-decoded in place and audited against their capture counters after
// the result set is produced; a dirty audit retries the whole query under
// the same cut. Groups with captured entries resolve per slot: the oldest
// entry with epoch > E supplies the value the slot held at E, and
// entry-less slots validate their in-place read against the capture
// counter. Writers never wait for a reader to finish.

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
#include <unordered_map>
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
 * the sentinel. Nullness is never inferred from payload.empty().
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
// Read view tuning knobs.
// ---------------------------------------------------------------------------

/**
 * @brief Upper bound on the read view's epoch-fence wait.
 */
uint32_t FenceTimeoutMs() {
  static const uint32_t timeout_ms = [] {
    const char* value = std::getenv("HELIOS_READ_VIEW_FENCE_TIMEOUT_MS");
    if (value == nullptr) return 5000u;
    const long parsed = std::strtol(value, nullptr, 10);
    return parsed > 0 ? static_cast<uint32_t>(parsed) : 5000u;
  }();
  return timeout_ms;
}

/**
 * @brief Number of whole-query attempts before giving up with "concurrent
 * modification". Retries reuse the same read view cut.
 */
int MaxReadViewAttempts() {
  static const int attempts = [] {
    const char* value = std::getenv("HELIOS_READ_VIEW_MAX_ATTEMPTS");
    if (value == nullptr) return 3;
    const long parsed = std::strtol(value, nullptr, 10);
    return parsed > 0 ? static_cast<int>(parsed) : 3;
  }();
  return attempts;
}

// ---------------------------------------------------------------------------
// Proxy row-format decoding, for undo before-images. A captured old_row is a
// proxy row payload whose typed fields carry val_str ASCII; the parsers
// mirror the scatter-side codec, and a decode failure is a broken invariant
// and throws.
// ---------------------------------------------------------------------------

/**
 * @brief Splits one proxy row payload into per-field {pointer, length} refs.
 *
 * @details Field 0 is the null-flags field; MySQL column i is field i + 1. A
 * 0xFF width tag (no value) and a zero-length payload both yield length 0,
 * matching the strip-cell convention "empty == SQL NULL".
 */
void SplitProxyRow(const std::string& row,
                   std::vector<std::pair<const char*, uint32_t>>* fields) {
  fields->clear();
  const char* data = row.data();
  const size_t size = row.size();
  size_t offset = 0;
  while (offset < size) {
    const uint8_t byte_size = static_cast<uint8_t>(data[offset]);
    offset += 1;
    uint32_t length = 0;
    if (byte_size != 0xFF) {
      if (byte_size > 4 || offset + byte_size > size) {
        throw std::runtime_error("undo before-image row is malformed");
      }
      for (uint32_t i = 0; i < byte_size; i++) {
        length |= static_cast<uint32_t>(static_cast<uint8_t>(data[offset + i]))
                  << (8 * i);
      }
      offset += byte_size;
      if (offset + length > size) {
        throw std::runtime_error("undo before-image row is malformed");
      }
    }
    fields->emplace_back(data + offset, length);
    offset += length;
  }
}

/**
 * @brief Parses a val_str ASCII integer (optional sign, digits only).
 */
bool ParseAsciiInt64(const char* s, uint32_t len, int64_t* out) {
  if (len == 0) return false;
  uint32_t i = 0;
  bool negative = false;
  if (s[0] == '-') {
    negative = true;
    i = 1;
  } else if (s[0] == '+') {
    i = 1;
  }
  if (i == len) return false;
  __int128 value = 0;
  for (; i < len; i++) {
    const char c = s[i];
    if (c < '0' || c > '9') return false;
    value = value * 10 + (c - '0');
    if (value > static_cast<__int128>(INT64_MAX) + 1) return false;
  }
  if (negative) value = -value;
  if (value > INT64_MAX || value < INT64_MIN) return false;
  *out = static_cast<int64_t>(value);
  return true;
}

/**
 * @brief Parses a val_str "YYYY-MM-DD" date into its year/month/day parts.
 */
bool ParseAsciiDate(const char* s, uint32_t len, int32_t* year, int32_t* month,
                    int32_t* day) {
  if (len != 10 || s[4] != '-' || s[7] != '-') return false;
  int32_t parts[3] = {0, 0, 0};
  const uint32_t spans[3][2] = {{0, 4}, {5, 7}, {8, 10}};
  for (int p = 0; p < 3; p++) {
    for (uint32_t i = spans[p][0]; i < spans[p][1]; i++) {
      const char c = s[i];
      if (c < '0' || c > '9') return false;
      parts[p] = parts[p] * 10 + (c - '0');
    }
  }
  *year = parts[0];
  *month = parts[1];
  *day = parts[2];
  return true;
}

/**
 * @brief Parses a val_str fixed-point decimal into a mantissa scaled by
 * `scale` (mirrors the scatter-side DEC64 rule, including padding when fewer
 * fractional digits are present).
 */
bool ParseAsciiDecimalScaled(const char* s, uint32_t len, int scale,
                             int64_t* out) {
  if (len == 0) return false;
  uint32_t i = 0;
  bool negative = false;
  if (s[0] == '-') {
    negative = true;
    i = 1;
  } else if (s[0] == '+') {
    i = 1;
  }
  __int128 mantissa = 0;
  int fractional_digits = 0;
  bool seen_dot = false, any_digit = false;
  for (; i < len; i++) {
    const char c = s[i];
    if (c == '.') {
      if (seen_dot) return false;
      seen_dot = true;
      continue;
    }
    if (c < '0' || c > '9') return false;
    mantissa = mantissa * 10 + (c - '0');
    if (seen_dot) fractional_digits++;
    any_digit = true;
    if (mantissa > static_cast<__int128>(INT64_MAX) + 1) return false;
  }
  if (!any_digit || fractional_digits > scale) return false;
  while (fractional_digits < scale) {
    mantissa *= 10;
    fractional_digits++;
    if (mantissa > static_cast<__int128>(INT64_MAX) + 1) return false;
  }
  if (negative) mantissa = -mantissa;
  if (mantissa > INT64_MAX || mantissa < INT64_MIN) return false;
  *out = static_cast<int64_t>(mantissa);
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
  size_t group_count = 0;   // fixed after the read view fence, not live state
  uint32_t cut_epoch = 0;   // read view visibility cut E

  // Set after a dirty bulk audit: the retry escalates every group to the
  // per-slot path, which needs no audit and therefore terminates.
  bool force_per_slot = false;

  // Groups read via the bulk in-place path this attempt; the result is
  // accepted only if every listed group's capture counter is still zero.
  // Scan workers append under the mutex.
  std::mutex bulk_mutex;
  std::vector<uint32_t> bulk_groups;
};

/**
 * @brief Returns whether every bulk-read group is still capture-free.
 *
 * @details Runs after the attempt's result set is fully produced. A
 * non-zero counter means bulk-read cells may be torn; the attempt is
 * discarded and the retry resolves the group through before-images.
 */
bool BulkGroupsUnchanged(const std::vector<PaxTableView>& table_views) {
  for (const PaxTableView& table_view : table_views) {
    for (const uint32_t group_index : table_view.bulk_groups) {
      PaxGroup* group = table_view.store->group(group_index);
      if (group != nullptr && pax::UndoGroupCaptureCount(group) != 0) {
        return false;
      }
    }
  }
  return true;
}

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
 *
 * @details `resolve_per_slot` is chosen at claim time: a group with captured
 * entries resolves slot-by-slot against `undo` (a copy of the group's undo
 * map sampled after `count_at_claim`; a capture landing between the two
 * samples trips the counter revalidation); a capture-free group takes the
 * bulk in-place path and is audited at attempt end instead.
 */
struct PaxLocalState : public LocalTableFunctionState {
  uint32_t current_group = UINT32_MAX;
  uint32_t current_slot = 0;
  PaxGroup* group_ptr = nullptr;
  bool resolve_per_slot = false;
  uint64_t count_at_claim = 0;
  std::unordered_map<uint32_t, std::vector<pax::UndoEntry>> undo;
  // Scratch for decoding one before-image row into output vectors.
  std::vector<std::pair<const char*, uint32_t>> field_refs;
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
 * @brief Row-count estimate for the join-order optimizer.
 *
 * @details slots_allocated() bounds live rows from above, so the estimate
 * is an upper bound rather than an exact count.
 */
unique_ptr<duckdb::NodeStatistics> PaxCardinality(
    ClientContext&, const FunctionData* bind_data) {
  const auto& data = bind_data->Cast<PaxBindData>();
  const uint64_t rows = data.table->store->slots_allocated();
  return duckdb::make_uniq<duckdb::NodeStatistics>(rows, rows);
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
 * @brief Projection context for one scanned column.
 */
struct ColumnContext {
  uint8_t kind;
  size_t field;  // strip field index; field 0 is the null-flags field
  uint32_t width;
  int8_t scale;
  PhysicalType decimal_physical_type;
};

/**
 * @brief Decodes one slot's projected columns from the strip cells into
 * chunk row `out_row`.
 *
 * @details Clears each column's validity first: per-slot resolution may
 * overwrite a row index whose previous occupant was dropped or replaced, so
 * a stale NULL bit must not survive into the new row.
 */
void EmitInPlaceRow(const PaxGroup& group,
                    const std::vector<ColumnContext>& scan_columns,
                    uint32_t slot, DataChunk& output, idx_t out_row) {
  for (idx_t i = 0; i < scan_columns.size(); i++) {
    const ColumnContext& column = scan_columns[i];
    FlatVector::SetNull(output.data[i], out_row, false);
    if (column.kind == FK_UNTYPED) {
      DecodeUntypedCell(group, column.field, slot, output.data[i], out_row);
    } else {
      BulkDecodeTyped(column.kind, group, column.field, column.width, slot, 1,
                      output.data[i], out_row, column.decimal_physical_type);
    }
  }
}

/**
 * @brief Decodes one captured before-image into chunk row `out_row`.
 *
 * @details The before-image is a proxy row payload whose typed fields carry
 * val_str ASCII (the gather round-trip contract); parse failures throw
 * because a captured image that fails to parse is a broken invariant, and
 * the request must fail rather than emit a wrong row.
 */
void EmitBeforeImageRow(const std::string& old_row,
                        const std::vector<ColumnContext>& scan_columns,
                        std::vector<std::pair<const char*, uint32_t>>& refs,
                        DataChunk& output, idx_t out_row) {
  SplitProxyRow(old_row, &refs);
  for (idx_t i = 0; i < scan_columns.size(); i++) {
    const ColumnContext& column = scan_columns[i];
    Vector& output_vector = output.data[i];
    if (column.field >= refs.size()) {
      throw std::runtime_error(
          "undo before-image row is missing a projected field");
    }
    const char* payload = refs[column.field].first;
    const uint32_t length = refs[column.field].second;
    if (length == 0) {  // empty == SQL NULL, as in the strip cells
      FlatVector::SetNull(output_vector, out_row, true);
      continue;
    }
    FlatVector::SetNull(output_vector, out_row, false);
    switch (column.kind) {
      case FK_INT32: {
        int64_t value;
        if (!ParseAsciiInt64(payload, length, &value) || value < INT32_MIN ||
            value > INT32_MAX) {
          throw std::runtime_error(
              "undo before-image INT32 field does not parse");
        }
        FlatVector::GetData<int32_t>(output_vector)[out_row] =
            static_cast<int32_t>(value);
        break;
      }
      case FK_INT64: {
        int64_t value;
        if (!ParseAsciiInt64(payload, length, &value)) {
          throw std::runtime_error(
              "undo before-image INT64 field does not parse");
        }
        FlatVector::GetData<int64_t>(output_vector)[out_row] = value;
        break;
      }
      case FK_DATE: {
        int32_t year, month, day;
        if (!ParseAsciiDate(payload, length, &year, &month, &day)) {
          throw std::runtime_error(
              "undo before-image DATE field does not parse");
        }
        FlatVector::GetData<date_t>(output_vector)[out_row] =
            Date::FromDate(year, month, day);
        break;
      }
      case FK_DEC64: {
        int64_t mantissa;
        if (!ParseAsciiDecimalScaled(payload, length, column.scale,
                                     &mantissa)) {
          throw std::runtime_error(
              "undo before-image DECIMAL field does not parse");
        }
        WriteDecimalPhysical(output_vector, out_row, mantissa,
                             column.decimal_physical_type);
        break;
      }
      default: {  // FK_UNTYPED: verbatim bytes
        if (length <= duckdb::string_t::INLINE_LENGTH) {
          FlatVector::GetData<duckdb::string_t>(output_vector)[out_row] =
              duckdb::string_t(payload, length);
        } else {
          FlatVector::GetData<duckdb::string_t>(output_vector)[out_row] =
              StringVector::AddString(output_vector, payload, length);
        }
        break;
      }
    }
  }
}

/**
 * @brief Returns the oldest entry whose writer epoch is after the cut, or
 * nullptr.
 *
 * @details Entries are published in install order, which is
 * epoch-non-decreasing per slot, so the first match is the oldest one and
 * its before-image is the value the slot held at the cut.
 */
const pax::UndoEntry* OldestEntryAfterCut(
    const std::vector<pax::UndoEntry>& entries, uint32_t cut_epoch) {
  for (const pax::UndoEntry& entry : entries) {
    if (pax::EpochAfterCut(entry.writer_epoch, cut_epoch)) {
      return &entry;
    }
  }
  return nullptr;
}

/**
 * @brief Scan worker: fills one output chunk from claimed PAX groups.
 *
 * @details Threads claim whole groups from the shared next_group counter.
 * Capture-free groups decode contiguous visible-slot runs column-at-a-time
 * (bulk path, audited at attempt end); groups with captured entries resolve
 * per slot against the read view cut (see the file header).
 */
void PaxScan(ClientContext&, TableFunctionInput& data, DataChunk& output) {
  const auto& bind_data = data.bind_data->Cast<PaxBindData>();
  auto& global_state = data.global_state->Cast<PaxGlobalState>();
  auto& local_state = data.local_state->Cast<PaxLocalState>();

  PaxTableView& table_view = *bind_data.table;
  PaxStore* store = table_view.store;
  const idx_t max_rows = output.GetCapacity();
  idx_t rows_emitted = 0;

  std::vector<ColumnContext> scan_columns(global_state.column_ids.size());
  for (idx_t i = 0; i < global_state.column_ids.size(); i++) {
    const column_t column = global_state.column_ids[i];
    const ColumnSpec& spec = table_view.columns[column];
    scan_columns[i].kind = spec.kind;
    scan_columns[i].field = static_cast<size_t>(column) + 1;  // field 0 is null flags
    scan_columns[i].width = spec.width;
    scan_columns[i].scale = spec.scale;
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
      // A group with captured entries resolves per slot; an untouched one
      // takes the bulk path and is recorded for the attempt-end audit.
      local_state.count_at_claim =
          pax::UndoGroupCaptureCount(local_state.group_ptr);
      local_state.resolve_per_slot =
          table_view.force_per_slot || local_state.count_at_claim != 0;
      if (local_state.resolve_per_slot) {
        local_state.undo = pax::UndoGroupEntries(local_state.group_ptr);
      } else {
        std::lock_guard<std::mutex> lk(table_view.bulk_mutex);
        table_view.bulk_groups.push_back(claimed);
      }
    }

    PaxGroup* group = local_state.group_ptr;
    const uint32_t slot = local_state.current_slot;
    if (slot >= PaxGroup::kRows) {
      local_state.group_ptr = nullptr;
      continue;
    }

    if (!local_state.resolve_per_slot) {
      // Bulk path: no captured entries when claimed, so the visibility
      // bitmap and cells are the values at the cut unless a capture lands
      // mid-attempt -- which the attempt-end audit catches.
      if (!group->IsVisible(slot)) {
        local_state.current_slot = slot + 1;
        continue;
      }
      const uint32_t max_run_length = std::min<uint32_t>(
          PaxGroup::kRows - slot,
          static_cast<uint32_t>(max_rows - rows_emitted));
      uint32_t run_length = 1;
      while (run_length < max_run_length &&
             group->IsVisible(slot + run_length)) {
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
          BulkDecodeTyped(column.kind, *group, column.field, column.width,
                          slot, run_length, output.data[i], rows_emitted,
                          column.decimal_physical_type);
        }
      }

      rows_emitted += run_length;
      local_state.current_slot = slot + run_length;
      continue;
    }

    // Per-slot resolution path.
    local_state.current_slot = slot + 1;
    const pax::UndoEntry* cut_entry = nullptr;
    const auto undo_it = local_state.undo.find(slot);
    if (undo_it != local_state.undo.end()) {
      cut_entry = OldestEntryAfterCut(undo_it->second, table_view.cut_epoch);
    }
    if (cut_entry != nullptr) {
      // The slot was installed into after the cut; its before-image is the
      // value at the cut (or the slot held no row then).
      if (!cut_entry->was_visible) continue;
      EmitBeforeImageRow(cut_entry->old_row, scan_columns,
                         local_state.field_refs, output, rows_emitted);
      rows_emitted++;
      continue;
    }
    // No post-cut entry in the copy: the in-place bit and cells carry the
    // state at the cut. Read them, then revalidate the capture counter (a
    // writer bumps it before its first mutation, so a torn read cannot
    // pass). Invisible slots revalidate too: a post-cut delete landing
    // after the map copy cleared the bit, and its before-image is the row
    // this read view owes.
    const bool visible_now = group->IsVisible(slot);
    if (visible_now) {
      EmitInPlaceRow(*group, scan_columns, slot, output, rows_emitted);
    }
    const uint64_t count_now = pax::UndoGroupCaptureCount(group);
    if (count_now != local_state.count_at_claim) {
      // A capture landed after the map copy; re-resolve this slot from a
      // fresh lookup.
      const auto fresh = pax::UndoSlotEntries(group, slot);
      const pax::UndoEntry* late =
          OldestEntryAfterCut(fresh, table_view.cut_epoch);
      local_state.count_at_claim = count_now;
      local_state.undo = pax::UndoGroupEntries(group);
      if (late != nullptr) {
        // Entry resolution is final: emit the before-image, or drop the
        // slot (a row index already written in place is reused by the
        // next row).
        if (!late->was_visible) continue;
        EmitBeforeImageRow(late->old_row, scan_columns, local_state.field_refs,
                           output, rows_emitted);
        rows_emitted++;
        continue;
      }
    }
    if (visible_now) rows_emitted++;
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
  function.cardinality = PaxCardinality;
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
 * REPLACE VIEW. Concurrent bridge requests therefore serialize on this
 * mutex for their whole execution, and every result is still accepted
 * through the read view poison and audit gate.
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
  // one-line stderr breakdown per request (read view fence / table view build /
  // catalog-mutex wait + registration / query / row encoding).
  // Enabled when set to anything but empty or "0", matching ENABLE_RPC_TRACE
  static const bool trace = [] {
    const char* value = std::getenv("ENABLE_DUCKDB_BRIDGE_TRACE");
    return value != nullptr && value[0] != '\0' &&
           std::string_view(value) != "0";
  }();
  using Clock = std::chrono::steady_clock;
  Clock::time_point trace_start, after_fence, after_view_build,
      after_register, after_query, after_encode;

  try {
    if (trace) trace_start = Clock::now();
    // The cut stays fixed for the whole request, retries included; the
    // undo entries accumulated under it are what make retries converge.
    const LineairDB::Database::PaxReadView read_view =
        db->AcquirePaxReadView(FenceTimeoutMs());
    if (!read_view.valid) {
      response->set_ok(false);
      response->set_error(read_view.error);
      return;
    }
    // Every exit releases the read view; the last active release clears the
    // undo maps.
    struct ReadViewRelease {
      LineairDB::Database* database;
      const LineairDB::Database::PaxReadView& handle;
      ~ReadViewRelease() { database->ReleasePaxReadView(handle); }
    } read_view_release{db, read_view};
    if (trace) after_fence = Clock::now();

    std::vector<PaxTableView> table_views(
        static_cast<size_t>(request.tables_size()));
    for (int i = 0; i < request.tables_size(); i++) {
      const pb::TxExecuteSqlDuckdb::TableDesc& table_desc = request.tables(i);
      PaxTableView& table_view = table_views[static_cast<size_t>(i)];
      PaxStore* store = db->GetPaxStore(table_desc.table_name());
      if (store == nullptr) {
        response->set_ok(false);
        response->set_error("table has no PAX store: " +
                            table_desc.table_name());
        return;
      }
      // Rows that ever took heap fallback are invisible to the strip scan;
      // reject the table up front. Post-fence, the check covers every
      // commit at or below the cut; later overflow is post-cut anyway.
      if (store->overflow_count() > 0) {
        response->set_ok(false);
        response->set_error("table has heap fallback rows: " +
                            table_desc.table_name());
        return;
      }
      table_view.sql_name = table_desc.sql_name();
      table_view.store = store;
      // Post-fence group count bounds the scan; groups allocated later can
      // only hold post-cut rows.
      table_view.group_count = store->group_count();
      table_view.cut_epoch = read_view.cut_epoch;
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
    int attempts_used = 0;
    bool accepted = false;
    {
      // Touches the runtime's shared system catalog; see
      // GlobalCatalogMutex. The lock spans registration and every attempt:
      // the registered function is nonce-unique and binds this request's
      // stack-owned views.
      std::lock_guard<std::mutex> catalog_lock(GlobalCatalogMutex());
      Connection connection(GlobalRuntime());
      for (size_t i = 0; i < table_views.size(); i++) {
        const std::string function_name =
            RegisterPaxFunction(connection, table_views[i], i, nonce);
        RegisterPaxView(connection, table_views[i], function_name);
      }
      if (trace) after_register = Clock::now();

      const int max_attempts = MaxReadViewAttempts();
      for (int attempt = 1; attempt <= max_attempts && !accepted; attempt++) {
        attempts_used = attempt;
        for (PaxTableView& table_view : table_views) {
          table_view.bulk_groups.clear();
        }

        result = connection.Query(request.sql());
        if (trace) after_query = Clock::now();

        // Every attempt exit runs the same gate, error or not: poison
        // first (the mutation that poisoned may have no entry and no
        // counter increment, and the audit alone would pass vacuously),
        // then the bulk-group audit.
        if (db->PaxReadViewPoisoned(read_view)) {
          response->set_ok(false);
          response->set_error(
              "columnar read view was poisoned during execution");
          return;
        }
        const bool audit_clean = BulkGroupsUnchanged(table_views);
        if (!audit_clean) {
          // Escalate the retry to per-slot resolution everywhere; the
          // escalated attempt has no bulk groups to audit and terminates.
          for (PaxTableView& table_view : table_views) {
            table_view.force_per_slot = true;
          }
        }

        if (result->HasError()) {
          // A concurrent capture can tear an in-flight bulk decode hard
          // enough to throw; the error is final only when the audit is
          // clean, otherwise retry.
          if (audit_clean) {
            response->set_ok(false);
            response->set_error(result->GetError());
            return;
          }
          continue;
        }
        if (!audit_clean) continue;  // retry, same cut

        // Stage attempt-locally; only an accepted attempt reaches the
        // response.
        std::vector<std::string> staged_rows;
        staged_rows.reserve(static_cast<size_t>(result->RowCount()));
        std::string row;
        for (idx_t row_index = 0; row_index < result->RowCount();
             row_index++) {
          EncodeRow(*result, row_index, &row);
          staged_rows.push_back(std::move(row));
        }
        if (trace) after_encode = Clock::now();

        for (std::string& staged : staged_rows) {
          response->add_rows(std::move(staged));
        }
        accepted = true;
      }
    }
    if (!accepted) {
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
          "[duckdb_bridge] fence=%.1fms view_build=%.1fms "
          "lock_wait+register=%.1fms query=%.1fms encode=%.1fms rows=%llu "
          "attempts=%d\n",
          milliseconds(trace_start, after_fence),
          milliseconds(after_fence, after_view_build),
          milliseconds(after_view_build, after_register),
          milliseconds(after_register, after_query),
          milliseconds(after_query, after_encode),
          static_cast<unsigned long long>(result->RowCount()), attempts_used);
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
