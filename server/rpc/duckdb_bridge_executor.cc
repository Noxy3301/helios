// DuckDB bridge executor: runs a TX_EXECUTE_DUCKDB_QUERY request by building
// DuckDB's parsed AST from the wire IR and executing it on the embedded
// runtime, whose scan function reads the live PaxTable instances, in place for
// groups without epoch images and through those images otherwise. DuckDB
// contributes its binder, planner, and vectorized runtime; no table data ever
// lives inside DuckDB. duckdb_bridge_dispatch.cc routes the opcode here.
//
// Consistency: the request runs against a columnar read view with snapshot
// epoch se (Database::OpenPaxView). A slot that holds an epoch image resolves
// through it (the oldest image with epoch > se is the value the slot held at
// se); every other slot is bulk-decoded in place. Before each output chunk is
// released, the preserve counter of every group that contributed in-place rows
// to it is re-read; a counter that moved rewinds that group's rows of this
// chunk and re-reads its slots one at a time from a fresh image copy. The
// statement runs once. Writers never wait for a reader to finish.

#include "duckdb_bridge_executor.hh"

#include "duckdb_ast_builder.hh"

#include <duckdb/common/vector_operations/ternary_executor.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/parser/parsed_data/create_collation_info.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "../mysql_charset_runtime.hh"
#include "m_ctype.h"

#include "lineairdb/database.h"
#include "lineairdb/pax.h"

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
namespace pax = helios::storage::pax;
using pax::FieldType;
using pax::PaxGroup;
using pax::PaxTable;

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
 * @brief Whether ENABLE_DUCKDB_BRIDGE_DEBUG asks for the bridge's trace lines.
 */
bool BridgeDebugEnabled() {
  static const bool enabled = [] {
    const char* value = std::getenv("ENABLE_DUCKDB_BRIDGE_DEBUG");
    return value != nullptr && value[0] != '\0' &&
           std::string_view(value) != "0";
  }();
  return enabled;
}

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

// ---------------------------------------------------------------------------
// Proxy row-format decoding, for epoch images. A preserved old_row is a proxy
// row payload whose typed fields carry val_str ASCII; the parsers mirror the
// scatter-side codec, and a decode failure is a broken invariant and throws.
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
        throw std::runtime_error("epoch image row is malformed");
      }
      for (uint32_t i = 0; i < byte_size; i++) {
        length |= static_cast<uint32_t>(static_cast<uint8_t>(data[offset + i]))
                  << (8 * i);
      }
      offset += byte_size;
      if (offset + length > size) {
        throw std::runtime_error("epoch image row is malformed");
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
  FieldType type = FieldType::kUntyped;
  uint32_t width = 0;
  int8_t scale = 0;
};

/**
 * @brief Per-request description of one live PAX table.
 *
 * @details Column metadata comes from the request: the proxy recomputes
 * type/width/scale from TABLE::field[] with the pure function used at CREATE
 * TABLE time (see proxy/lineairdb_field_types.h), matching what the server
 * stored while the schema is unchanged.
 */
struct PaxTableView {
  PaxTable* table = nullptr;
  std::vector<ColumnSpec> columns;
  size_t group_count = 0;  // fixed after the read view fence, not live state
  uint32_t snapshot_epoch = 0;  // read view serialization point se

  // Scan tallies for one request, reported under ENABLE_DUCKDB_BRIDGE_DEBUG.
  std::atomic<uint64_t> groups_scanned{0};
  std::atomic<uint64_t> groups_with_images{0};
  std::atomic<uint64_t> chunk_audits_redone{0};
  std::atomic<uint64_t> slots_from_images{0};
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
 * @details Claim time samples `count_at_claim` and then copies the group's
 * image map and its imaged-slot bits; a preserve landing between the two
 * bumps a value the chunk audit compares against. `image_state` is the
 * group's image state as of the claim, null while the group has none.
 */
struct PaxLocalState : public LocalTableFunctionState {
  uint32_t current_slot = 0;
  PaxGroup* group_ptr = nullptr;
  pax::GroupImageState* image_state = nullptr;
  uint64_t count_at_claim = 0;
  bool has_images = false;
  uint64_t imaged[PaxGroup::kRows / PaxGroup::kVisibilityWordBits] = {};
  std::unordered_map<uint32_t, std::vector<pax::EpochImage>> images;
  // Scratch for decoding one epoch image row into output vectors.
  std::vector<std::pair<const char*, uint32_t>> field_refs;
};

/**
 * @brief Returns the claimed group's image state, reloading it once when the
 * group had none when it was claimed.
 */
inline pax::GroupImageState* GroupState(PaxLocalState& state,
                                        const PaxGroup* group) {
  if (state.image_state == nullptr) state.image_state = pax::ImageState(group);
  return state.image_state;
}

/**
 * @brief Returns the oldest image whose writer epoch is after the snapshot
 * epoch, or nullptr.
 *
 * @details Images are published in install order, which is
 * epoch-non-decreasing per slot, so the first match is the oldest one and it
 * holds the value the slot had at se.
 */
const pax::EpochImage* OldestImageAfterSnapshot(
    const std::vector<pax::EpochImage>& images, uint32_t snapshot_epoch) {
  for (const pax::EpochImage& image : images) {
    if (pax::EpochAfterSnapshot(image.writer_epoch, snapshot_epoch)) {
      return &image;
    }
  }
  return nullptr;
}

/**
 * @brief Copies the group's images and keeps in the imaged-slot bitset only
 * the slots holding an image this snapshot epoch resolves through.
 *
 * @details A slot whose images all lie at or before the snapshot holds its
 * value at se in its cells, so it stays out of the bitset and joins the bulk
 * visible runs.
 */
inline void CopyGroupImages(PaxLocalState& state, uint32_t snapshot_epoch) {
  constexpr uint32_t kWordBits = PaxGroup::kVisibilityWordBits;
  state.images = pax::GroupImages(state.image_state, state.imaged);
  state.has_images = false;
  for (const auto& [slot, images] : state.images) {
    if (slot >= PaxGroup::kRows) continue;
    if (OldestImageAfterSnapshot(images, snapshot_epoch) != nullptr) {
      state.has_images = true;
    } else {
      state.imaged[slot / kWordBits] &= ~(uint64_t{1} << (slot % kWordBits));
    }
  }
}

/**
 * @brief Samples the group's preserve counter, then copies its images and
 * imaged-slot bits. The counter first, so a preserve in between is caught.
 */
inline void ClaimGroupImages(PaxLocalState& state, const PaxGroup* group,
                             uint32_t snapshot_epoch) {
  state.image_state = pax::ImageState(group);
  state.count_at_claim = pax::PreserveCount(state.image_state);
  CopyGroupImages(state, snapshot_epoch);
}

/**
 * @brief Returns whether the claimed copy resolves `slot` through an image.
 */
inline bool SlotImaged(const PaxLocalState& state, uint32_t slot) {
  constexpr uint32_t kWordBits = PaxGroup::kVisibilityWordBits;
  return state.has_images &&
         ((state.imaged[slot / kWordBits] >> (slot % kWordBits)) & 1u) != 0;
}

/**
 * @brief Maps a PAX FieldType to the DuckDB column type.
 */
LogicalType FieldTypeToLogicalType(FieldType type, int8_t scale) {
  switch (type) {
    case FieldType::kInt32:
      return LogicalType::INTEGER;
    case FieldType::kInt64:
      return LogicalType::BIGINT;
    case FieldType::kDate:
      return LogicalType::DATE;
    case FieldType::kDecimal64:
      // Width 15 matches the kDecimal64 typing rule (precision <= 15); see
      // helios::storage::pax::FieldType.
      return LogicalType::DECIMAL(15, static_cast<uint8_t>(scale));
    default:
      return LogicalType::VARCHAR;
  }
}

/**
 * @brief Bind for the duckdb-query path: the table arrives as a POINTER
 * argument owned by the request, not through the catalog. Columns are
 * exposed as _c0.._cN in TABLE::field order, matching the wire ColumnRef
 * ordinals, so no MySQL identifier ever participates in DuckDB binding.
 */
unique_ptr<FunctionData> PaxPointerBind(ClientContext&,
                                        TableFunctionBindInput& input,
                                        vector<LogicalType>& return_types,
                                        vector<string>& names) {
  auto bind_data = duckdb::make_uniq<PaxBindData>();
  bind_data->table = reinterpret_cast<PaxTableView*>(
      input.inputs[0].GetPointer());
  size_t ordinal = 0;
  for (const auto& column : bind_data->table->columns) {
    return_types.push_back(FieldTypeToLogicalType(column.type, column.scale));
    names.push_back("_c" + std::to_string(ordinal++));
  }
  return std::move(bind_data);
}

/**
 * @brief Row-count estimate for the join-order optimizer.
 *
 * @details SlotsAllocated() bounds live rows from above, so the estimate
 * is an upper bound rather than an exact count.
 */
unique_ptr<duckdb::NodeStatistics> PaxCardinality(
    ClientContext&, const FunctionData* bind_data) {
  const auto& data = bind_data->Cast<PaxBindData>();
  const uint64_t rows = pax::SlotsAllocated(data.table->table);
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
void BulkDecodeTyped(FieldType type, const PaxGroup& group, size_t field,
                     uint32_t width, uint32_t slot_start, uint32_t count,
                     Vector& output_vector, idx_t out_base,
                     PhysicalType decimal_physical_type) {
  const std::byte* strip_base = group.strip(field);
  const uint32_t stride = group.stride(field);
  const std::byte* src = strip_base + static_cast<size_t>(stride) * slot_start;
  constexpr uint32_t kCellLenBytes = PaxGroup::kCellLenBytes;

  switch (type) {
    case FieldType::kInt32: {
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
    case FieldType::kInt64: {
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
    case FieldType::kDate: {
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
    case FieldType::kDecimal64: {
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
      break;  // kUntyped never reaches here
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
  FieldType type;
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
    if (column.type == FieldType::kUntyped) {
      DecodeUntypedCell(group, column.field, slot, output.data[i], out_row);
    } else {
      BulkDecodeTyped(column.type, group, column.field, column.width, slot, 1,
                      output.data[i], out_row, column.decimal_physical_type);
    }
  }
}

/**
 * @brief Decodes one epoch image into chunk row `out_row`.
 *
 * @details The image is a proxy row payload whose typed fields carry val_str
 * ASCII (the gather round-trip contract); parse failures throw because an
 * image that fails to parse is a broken invariant, and the request must fail
 * rather than emit a wrong row.
 */
void EmitImageRow(const std::string& old_row,
                  const std::vector<ColumnContext>& scan_columns,
                  std::vector<std::pair<const char*, uint32_t>>& refs,
                  DataChunk& output, idx_t out_row) {
  SplitProxyRow(old_row, &refs);
  for (idx_t i = 0; i < scan_columns.size(); i++) {
    const ColumnContext& column = scan_columns[i];
    Vector& output_vector = output.data[i];
    if (column.field >= refs.size()) {
      throw std::runtime_error("epoch image row is missing a projected field");
    }
    const char* payload = refs[column.field].first;
    const uint32_t length = refs[column.field].second;
    if (length == 0) {  // empty == SQL NULL, as in the strip cells
      FlatVector::SetNull(output_vector, out_row, true);
      continue;
    }
    FlatVector::SetNull(output_vector, out_row, false);
    switch (column.type) {
      case FieldType::kInt32: {
        int64_t value;
        if (!ParseAsciiInt64(payload, length, &value) || value < INT32_MIN ||
            value > INT32_MAX) {
          throw std::runtime_error("epoch image INT32 field does not parse");
        }
        FlatVector::GetData<int32_t>(output_vector)[out_row] =
            static_cast<int32_t>(value);
        break;
      }
      case FieldType::kInt64: {
        int64_t value;
        if (!ParseAsciiInt64(payload, length, &value)) {
          throw std::runtime_error("epoch image INT64 field does not parse");
        }
        FlatVector::GetData<int64_t>(output_vector)[out_row] = value;
        break;
      }
      case FieldType::kDate: {
        int32_t year, month, day;
        if (!ParseAsciiDate(payload, length, &year, &month, &day)) {
          throw std::runtime_error("epoch image DATE field does not parse");
        }
        FlatVector::GetData<date_t>(output_vector)[out_row] =
            Date::FromDate(year, month, day);
        break;
      }
      case FieldType::kDecimal64: {
        int64_t mantissa;
        if (!ParseAsciiDecimalScaled(payload, length, column.scale,
                                     &mantissa)) {
          throw std::runtime_error("epoch image DECIMAL field does not parse");
        }
        WriteDecimalPhysical(output_vector, out_row, mantissa,
                             column.decimal_physical_type);
        break;
      }
      default: {  // kUntyped: verbatim bytes
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
 * @brief Scan worker: fills one output chunk from claimed PAX groups.
 *
 * @details Threads claim whole groups from the shared next_group counter.
 * Slots the claimed image copy marks emit their image for the snapshot epoch;
 * the rest decode contiguous visible-slot runs column-at-a-time. The group's
 * preserve counter is re-read before the chunk is released, and a counter
 * that moved rewinds this call's rows of that group and re-reads its slots
 * one at a time (see the file header).
 */
void PaxScan(ClientContext&, TableFunctionInput& data, DataChunk& output) {
  const auto& bind_data = data.bind_data->Cast<PaxBindData>();
  auto& global_state = data.global_state->Cast<PaxGlobalState>();
  auto& local_state = data.local_state->Cast<PaxLocalState>();

  PaxTableView& table_view = *bind_data.table;
  PaxTable* table = table_view.table;
  const uint32_t snapshot_epoch = table_view.snapshot_epoch;
  // The chunk DuckDB hands in holds STANDARD_VECTOR_SIZE rows (2048 in the
  // default build); the audit below runs once per chunk, before it is returned.
  const idx_t max_rows = output.GetCapacity();
  idx_t rows_emitted = 0;

  std::vector<ColumnContext> scan_columns(global_state.column_ids.size());
  for (idx_t i = 0; i < global_state.column_ids.size(); i++) {
    const column_t column = global_state.column_ids[i];
    const ColumnSpec& spec = table_view.columns[column];
    scan_columns[i].type = spec.type;
    scan_columns[i].field = static_cast<size_t>(column) + 1;  // field 0 is null flags
    scan_columns[i].width = spec.width;
    scan_columns[i].scale = spec.scale;
    scan_columns[i].decimal_physical_type =
        (spec.type == FieldType::kDecimal64)
            ? output.data[i].GetType().InternalType()
            : PhysicalType::INVALID;
  }

  // The current group's in-place reads in this call: the slot they started at
  // and the output row the group's rows begin at, which a dirty audit rewinds
  // to. A group carried over from an earlier call resumes at its cursor.
  bool audit_pending = local_state.group_ptr != nullptr &&
                       local_state.current_slot < PaxGroup::kRows;
  uint32_t entry_slot = local_state.current_slot;
  idx_t entry_row = 0;

  auto emit_image = [&](const pax::EpochImage& image) {
    table_view.slots_from_images.fetch_add(1, std::memory_order_relaxed);
    if (!image.was_visible) return;  // the slot held no row at se
    EmitImageRow(image.old_row, scan_columns, local_state.field_refs, output,
                 rows_emitted);
    rows_emitted++;
  };

  // Re-reads [from, to) of the claimed group against a fresh image copy,
  // revalidating the counter per row, and stops when the chunk fills: the
  // group stays claimed and the next call resumes at current_slot.
  auto redo_slots = [&](uint32_t from, uint32_t to) {
    PaxGroup* group = local_state.group_ptr;
    ClaimGroupImages(local_state, group, snapshot_epoch);
    for (uint32_t slot = from; slot < to; slot++) {
      if (rows_emitted >= max_rows) {
        local_state.current_slot = slot;
        return;
      }
      const auto images_it = local_state.images.find(slot);
      if (images_it != local_state.images.end()) {
        const pax::EpochImage* image =
            OldestImageAfterSnapshot(images_it->second, snapshot_epoch);
        if (image != nullptr) {
          emit_image(*image);
          continue;
        }
      }
      const bool visible_now = group->IsVisible(slot);
      if (visible_now) {
        EmitInPlaceRow(*group, scan_columns, slot, output, rows_emitted);
      }
      // The fence orders the cell reads above before the closing sample; an
      // acquire load alone leaves them free to sink past it.
      std::atomic_thread_fence(std::memory_order_acquire);
      const uint64_t count_now =
          pax::PreserveCount(GroupState(local_state, group));
      if (count_now != local_state.count_at_claim) {
        // A preserve landed after the copy; re-resolve this slot from a fresh
        // lookup, then refresh the copy for the slots after it.
        const auto fresh = pax::SlotImages(local_state.image_state, slot);
        const pax::EpochImage* late =
            OldestImageAfterSnapshot(fresh, snapshot_epoch);
        local_state.count_at_claim = count_now;
        CopyGroupImages(local_state, snapshot_epoch);
        if (late != nullptr) {
          emit_image(*late);
          continue;
        }
      }
      if (visible_now) rows_emitted++;
    }
    local_state.current_slot = to;
  };

  // A preserve bumps the counter before the writer's first strip mutation, so
  // an unchanged counter means no in-place row of this group was read torn.
  // The audit runs before the chunk reaches DuckDB, which cannot give one back.
  auto audit = [&]() {
    if (!audit_pending) return;
    audit_pending = false;
    PaxGroup* group = local_state.group_ptr;
    const uint32_t read_through = local_state.current_slot;
    // The fence orders this call's cell reads before the closing sample; an
    // acquire load alone leaves them free to sink past it.
    std::atomic_thread_fence(std::memory_order_acquire);
    if (pax::PreserveCount(GroupState(local_state, group)) ==
        local_state.count_at_claim) {
      return;
    }
    table_view.chunk_audits_redone.fetch_add(1, std::memory_order_relaxed);
    const idx_t high_water = rows_emitted;
    rows_emitted = entry_row;
    redo_slots(entry_slot, read_through);
    // The rows the redo abandons keep the validity bits the rewound pass
    // wrote, and the bulk run that fills those rows next writes cells only,
    // so their columns are marked valid again.
    for (idx_t column = 0; column < scan_columns.size(); column++) {
      for (idx_t row = rows_emitted; row < high_water; row++) {
        FlatVector::SetNull(output.data[column], row, false);
      }
    }
    // The redo's fresh copy and counter are the baseline of the window any
    // further in-place read of this group in this call belongs to.
    audit_pending = local_state.current_slot < PaxGroup::kRows;
    entry_slot = local_state.current_slot;
    entry_row = rows_emitted;
  };

  // An empty chunk ends this thread's scan, and a redo can empty one, so a
  // call that has groups left to claim claims them rather than returning
  // cardinality 0.
  bool groups_exhausted = false;
  for (;;) {
    while (rows_emitted < max_rows) {
      if (local_state.group_ptr != nullptr &&
          local_state.current_slot >= PaxGroup::kRows) {
        audit();
        // A redo that fills the chunk leaves the group unfinished.
        if (local_state.current_slot < PaxGroup::kRows) continue;
        local_state.group_ptr = nullptr;
        // A redo can fill the chunk on the group's last slot.
        if (rows_emitted >= max_rows) break;
      }
      if (local_state.group_ptr == nullptr) {
        const uint64_t next =
            global_state.next_group.fetch_add(1, std::memory_order_relaxed);
        if (next >= global_state.group_count) {
          groups_exhausted = true;
          break;
        }
        local_state.group_ptr = pax::Group(table, static_cast<uint32_t>(next));
        local_state.current_slot = 0;
        if (local_state.group_ptr == nullptr) continue;
        table_view.groups_scanned.fetch_add(1, std::memory_order_relaxed);
        ClaimGroupImages(local_state, local_state.group_ptr, snapshot_epoch);
        if (local_state.has_images) {
          table_view.groups_with_images.fetch_add(1, std::memory_order_relaxed);
        }
        audit_pending = true;
        entry_slot = 0;
        entry_row = rows_emitted;
      }

      PaxGroup* group = local_state.group_ptr;
      const uint32_t slot = local_state.current_slot;

      if (SlotImaged(local_state, slot)) {
        local_state.current_slot = slot + 1;
        emit_image(*OldestImageAfterSnapshot(
            local_state.images.find(slot)->second, snapshot_epoch));
        continue;
      }

      if (!group->IsVisible(slot)) {
        local_state.current_slot = slot + 1;
        continue;
      }
      const uint32_t max_run_length =
          std::min<uint32_t>(PaxGroup::kRows - slot,
                             static_cast<uint32_t>(max_rows - rows_emitted));
      uint32_t run_length = 1;
      while (run_length < max_run_length &&
             !SlotImaged(local_state, slot + run_length) &&
             group->IsVisible(slot + run_length)) {
        run_length++;
      }

      for (idx_t i = 0; i < scan_columns.size(); i++) {
        const ColumnContext& column = scan_columns[i];
        if (column.type == FieldType::kUntyped) {
          for (uint32_t row = 0; row < run_length; row++) {
            DecodeUntypedCell(*group, column.field, slot + row, output.data[i],
                              rows_emitted + row);
          }
        } else {
          BulkDecodeTyped(column.type, *group, column.field, column.width, slot,
                          run_length, output.data[i], rows_emitted,
                          column.decimal_physical_type);
        }
      }

      rows_emitted += run_length;
      local_state.current_slot = slot + run_length;
    }

    audit();
    if (rows_emitted > 0 || groups_exhausted) break;
  }
  // Past this line the chunk is DuckDB's; every group that fed it is audited.
  output.SetCardinality(rows_emitted);
}

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
    } else if (value.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
      // MySQL's boolean surface is 1/0; Item_string::val_int reads both
      // "true" and "false" as 0.
      AppendProxyField(*out, value.GetValue<bool>() ? "1" : "0",
                       /*is_null=*/false);
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
 * @details The bridge borrows DuckDB's binder, planner, and vectorized
 * executor; the in-memory duckdb::DuckDB instance holds no table data, and
 * its system catalog only ever contains this bridge's scan function. The function-local static gives thread-safe, exactly-once
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

// MySQL's collation number for utf8mb4_0900_ai_ci, as the wire IR carries it.
constexpr uint32_t kUtf8mb40900AiCiCollationId = 255;
// Name the collation is registered under inside DuckDB (COLLATE targets it).
constexpr const char* kUtf8mb40900AiCiDuckdbName = "utf8mb4_0900_ai_ci";
// DuckDB scalar functions implementing MySQL LIKE / NOT LIKE under this
// collation; the AST builder emits calls to them by these names.
constexpr const char* kUtf8mb40900AiCiLikeFunction =
    "mysql_utf8mb4_0900_ai_ci_like";
constexpr const char* kUtf8mb40900AiCiNotLikeFunction =
    "mysql_utf8mb4_0900_ai_ci_not_like";

/**
 * @brief DuckDB collation scalar for MySQL utf8mb4_0900_ai_ci.
 *
 * @details DuckDB implements a collation by replacing the collated VARCHAR
 * with this scalar's byte-comparable result wherever comparison, ordering,
 * grouping, or ordinary DISTINCT needs a key. Returning BLOB keeps MySQL's
 * raw strnxfrm bytes instead of hex-encoding them to twice their size.
 */
void Utf8mb40900AiCiSortKey(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  const CHARSET_INFO* collation =
      mysql_charset_runtime::initialize(nullptr).utf8mb4_0900_ai_ci;
  if (collation == nullptr ||
      collation->number != kUtf8mb40900AiCiCollationId ||
      collation->pad_attribute != NO_PAD) {
    throw std::runtime_error(
        "utf8mb4_0900_ai_ci collation runtime is not ready or is not NO PAD");
  }

  duckdb::UnaryExecutor::Execute<duckdb::string_t, duckdb::string_t>(
      args.data[0], result, args.size(), [&](duckdb::string_t input) {
        const size_t input_size = input.GetSize();
        if (input_size > SIZE_MAX / collation->mbmaxlen) {
          throw std::runtime_error(
              "utf8mb4_0900_ai_ci sort key input is too large");
        }
        // strnxfrmlen requires a pessimistic byte count: utf8mb4's maximum
        // bytes per codepoint times the maximum possible codepoint count.
        const size_t pessimistic_bytes = input_size * collation->mbmaxlen;
        const size_t capacity =
            collation->coll->strnxfrmlen(collation, pessimistic_bytes);
        if ((capacity & 1) != 0 ||
            capacity > duckdb::string_t::MAX_STRING_SIZE) {
          throw std::runtime_error(
              "utf8mb4_0900_ai_ci sort key is too large for DuckDB");
        }
        duckdb::string_t key =
            duckdb::StringVector::EmptyString(result, capacity);
        const size_t key_size = collation->coll->strnxfrm(
            collation, reinterpret_cast<uchar*>(key.GetDataWriteable()),
            capacity, /*num_codepoints=*/0,
            reinterpret_cast<const uchar*>(input.GetData()), input_size,
            /*flags=*/0);
        if (key_size > capacity || key_size > UINT32_MAX) {
          throw std::runtime_error(
              "utf8mb4_0900_ai_ci sort key exceeded its allocation");
        }
        // strnxfrm can use less than its worst-case allocation. This both
        // records the actual length and refreshes string_t's cached prefix;
        // comparison uses the prefix while hashing reads the payload.
        key.SetSizeAndFinalize(static_cast<uint32_t>(key_size), capacity);
        return key;
      });
}

template <bool Negated>
void Utf8mb40900AiCiLike(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  const CHARSET_INFO* collation =
      mysql_charset_runtime::initialize(nullptr).utf8mb4_0900_ai_ci;
  if (collation == nullptr ||
      collation->number != kUtf8mb40900AiCiCollationId) {
    throw std::runtime_error("utf8mb4_0900_ai_ci LIKE runtime is not ready");
  }
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   int32_t, bool>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](duckdb::string_t text, duckdb::string_t pattern, int32_t escape) {
        const int compared = my_wildcmp(
            collation, text.GetData(), text.GetData() + text.GetSize(),
            pattern.GetData(), pattern.GetData() + pattern.GetSize(), escape,
            escape == '_' ? -1 : '_', escape == '%' ? -1 : '%');
        const bool matched = compared == 0;
        return Negated ? !matched : matched;
      });
}

/**
 * @brief MySQL's ASCII(): the first BYTE of the string, 0 for the empty
 * string. DuckDB's own ascii() is codepoint-valued and differs on any
 * multibyte head.
 */
void MysqlAsciiFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                        duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
      args.data[0], result, args.size(), [](duckdb::string_t input) {
        if (input.GetSize() == 0) return 0;
        return static_cast<int32_t>(
            static_cast<unsigned char>(input.GetData()[0]));
      });
}

void RegisterMySqlCollationRuntime(Connection& connection) {
  duckdb::ScalarFunction sort_key(
      "mysql_utf8mb4_0900_ai_ci_sort_key", {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BLOB, Utf8mb40900AiCiSortKey);
  duckdb::CreateCollationInfo create_info(
      kUtf8mb40900AiCiDuckdbName, std::move(sort_key),
      /*combinable=*/false,
      /*not_required_for_equality=*/false);
  create_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  connection.context->RunFunctionInTransaction([&]() {
    auto& catalog = duckdb::Catalog::GetSystemCatalog(*connection.context);
    catalog.CreateCollation(*connection.context, create_info);
  });
  if (connection.HasActiveTransaction()) connection.Commit();

  const duckdb::vector<duckdb::LogicalType> arguments = {
      duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
      duckdb::LogicalType::INTEGER};
  duckdb::ScalarFunction like(kUtf8mb40900AiCiLikeFunction, arguments,
                              duckdb::LogicalType::BOOLEAN,
                              Utf8mb40900AiCiLike<false>);
  duckdb::CreateScalarFunctionInfo like_info(std::move(like));
  like_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  connection.context->RegisterFunction(like_info);
  duckdb::ScalarFunction not_like(kUtf8mb40900AiCiNotLikeFunction, arguments,
                                  duckdb::LogicalType::BOOLEAN,
                                  Utf8mb40900AiCiLike<true>);
  duckdb::CreateScalarFunctionInfo not_like_info(std::move(not_like));
  not_like_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  connection.context->RegisterFunction(not_like_info);

  // Callable form of the sort key, for aggregate-DISTINCT deduplication
  // where DuckDB does not push a non-combinable collation into children.
  duckdb::ScalarFunction sort_key_fn(
      "mysql_utf8mb4_0900_ai_ci_sort_key", {duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BLOB, Utf8mb40900AiCiSortKey);
  duckdb::CreateScalarFunctionInfo sort_key_info(std::move(sort_key_fn));
  sort_key_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  connection.context->RegisterFunction(sort_key_info);

  duckdb::ScalarFunction mysql_ascii("mysql_ascii",
                                     {duckdb::LogicalType::VARCHAR},
                                     duckdb::LogicalType::INTEGER,
                                     MysqlAsciiFunction);
  duckdb::CreateScalarFunctionInfo ascii_info(std::move(mysql_ascii));
  ascii_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  connection.context->RegisterFunction(ascii_info);
}

/**
 * @brief Registers helios_pax_scan(POINTER) once for the process lifetime.
 *
 * @details The duckdb-query path keeps no per-request catalog state: the
 * one immutable function is registered on first use, and every request hands
 * its stack-owned PaxTableView in as a pointer constant inside the AST.
 */
void EnsureDuckdbScanRegistered() {
  static std::once_flag registered;
  std::call_once(registered, [] {
    Connection connection(GlobalRuntime());
    TableFunction function("helios_pax_scan", {duckdb::LogicalType::POINTER},
                           PaxScan, PaxPointerBind, PaxInitGlobal,
                           PaxInitLocal);
    function.projection_pushdown = true;
    function.filter_pushdown = false;
    function.cardinality = PaxCardinality;
    connection.context->RunFunctionInTransaction([&]() {
      auto& catalog = duckdb::Catalog::GetSystemCatalog(*connection.context);
      duckdb::CreateTableFunctionInfo create_info(function);
      create_info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
      catalog.CreateTableFunction(*connection.context, create_info);
    });
    RegisterMySqlCollationRuntime(connection);
  });
}

}  // namespace

void ExecuteDuckdbQuery(
    helios::storage::Database* db,
    const pb::TxExecuteDuckdbQuery::Request& request,
    pb::TxExecuteDuckdbQuery::Response* response) {
  if (response == nullptr) return;
  response->Clear();
  if (db == nullptr) {
    response->set_ok(false);
    response->set_error("database is unavailable");
    return;
  }
  try {
    const helios::storage::Database::PaxReadView read_view =
        db->OpenPaxView(FenceTimeoutMs());
    if (!read_view.valid) {
      response->set_ok(false);
      response->set_error(read_view.error);
      return;
    }
    struct ReadViewRelease {
      helios::storage::Database* database;
      const helios::storage::Database::PaxReadView& handle;
      ~ReadViewRelease() { database->ClosePaxView(handle); }
    } read_view_release{db, read_view};

    std::vector<PaxTableView> table_views(
        static_cast<size_t>(request.tables_size()));
    std::vector<uintptr_t> handles(table_views.size());
    for (int i = 0; i < request.tables_size(); i++) {
      const pb::TxExecuteDuckdbQuery::TableDesc& table_desc = request.tables(i);
      PaxTableView& table_view = table_views[static_cast<size_t>(i)];
      PaxTable* table = db->GetPaxTable(table_desc.table_name());
      if (table == nullptr) {
        response->set_ok(false);
        response->set_error("table has no PAX store: " +
                            table_desc.table_name());
        return;
      }
      // The wire descriptor drives strip access; a shape that disagrees
      // with the store's own schema would read out of bounds or decode a
      // cell under the wrong width. Field 0 is the row null-flags field.
      const auto& schema = pax::Schema(table);
      if (schema.field_count() !=
          static_cast<size_t>(table_desc.columns_size()) + 1) {
        response->set_ok(false);
        response->set_error("table descriptor does not match the store: " +
                            table_desc.table_name());
        return;
      }
      for (int c = 0; c < table_desc.columns_size(); c++) {
        const auto& column = table_desc.columns(c);
        const size_t f = static_cast<size_t>(c) + 1;
        // type_of/scale_of handle the documented empty-vector shapes
        // (an untyped store keeps field_type empty).
        if (schema.type_of(f) != static_cast<FieldType>(column.pax_kind()) ||
            schema.field_max_bytes[f] != column.pax_width() ||
            schema.scale_of(f) != static_cast<int>(column.pax_scale())) {
          response->set_ok(false);
          response->set_error(
              "table descriptor does not match the store: " +
              table_desc.table_name());
          return;
        }
      }
      table_view.table = table;
      table_view.group_count = pax::GroupCount(table);
      table_view.snapshot_epoch = read_view.snapshot_epoch;
      table_view.columns.reserve(
          static_cast<size_t>(table_desc.columns_size()));
      for (const auto& column : table_desc.columns()) {
        ColumnSpec spec;
        spec.type = static_cast<FieldType>(column.pax_kind());
        spec.width = column.pax_width();
        spec.scale = static_cast<int8_t>(column.pax_scale());
        table_view.columns.push_back(std::move(spec));
      }
      handles[static_cast<size_t>(i)] =
          reinterpret_cast<uintptr_t>(&table_view);
    }

    EnsureDuckdbScanRegistered();
    Connection connection(GlobalRuntime());
    auto built = BuildSelectStatement(request, handles);
    if (!built.statement) {
      response->set_ok(false);
      response->set_error(built.error);
      return;
    }
    duckdb::unique_ptr<duckdb::SQLStatement> statement(
        built.statement.release());
    const bool debug_resolved = BridgeDebugEnabled();
    if (debug_resolved) {
      std::fprintf(stderr, "[duckdb-ast] %s\n", statement->ToString().c_str());
    }
    auto query_result = connection.Query(std::move(statement));
    std::unique_ptr<duckdb::MaterializedQueryResult> result(
        static_cast<duckdb::MaterializedQueryResult*>(query_result.release()));

    if (!db->PaxViewValid(read_view)) {
      response->set_ok(false);
      response->set_error("columnar read view expired during execution");
      return;
    }
    if (debug_resolved) {
      for (size_t i = 0; i < table_views.size(); i++) {
        const PaxTableView& table_view = table_views[i];
        std::fprintf(stderr,
                     "[duckdb-scan] %s groups=%lu imaged_groups=%lu "
                     "chunk_redos=%lu image_slots=%lu\n",
                     request.tables(static_cast<int>(i)).table_name().c_str(),
                     table_view.groups_scanned.load(),
                     table_view.groups_with_images.load(),
                     table_view.chunk_audits_redone.load(),
                     table_view.slots_from_images.load());
      }
    }
    if (result->HasError()) {
      response->set_ok(false);
      response->set_error(result->GetError());
      return;
    }

    std::string row;
    for (idx_t row_index = 0; row_index < result->RowCount(); row_index++) {
      EncodeRow(*result, row_index, &row);
      response->add_rows(std::move(row));
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
