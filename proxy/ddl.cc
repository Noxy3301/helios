#include "storage/lineairdb/ha_lineairdb.hh"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "lineairdb_field_types.h"
#include "my_dbug.h"
#include "sql/field.h"
#include "sql/key.h"
#include "sql/sql_class.h"
#include "sql/table.h"

// Handler table lifecycle and DDL entry points. These methods open MySQL table
// metadata, create LineairDB-side tables/indexes, and backfill secondary
// indexes for online ALTER TABLE ADD INDEX.

namespace {

// LineairDB SecondaryIndexOption::Constraint wire bit for UNIQUE.
constexpr uint kUniqueSecondaryIndex = 1u;

// Backfill batching bounds each OCC write set while keeping connection reuse.
constexpr bool kFence = false;
constexpr uint64_t kBackfillWriteChunkRows = 2000;
constexpr size_t kBackfillParallelWorkers = 16;

// PAX typed-cell kinds (mirror LineairDB::Pax::FieldKind in pax_store.h; the
// proxy is PAX-oblivious so the values are duplicated by design, kept in sync).
namespace pax_kind {
constexpr uint32_t UNTYPED = 0;
constexpr uint32_t INT32 = 1;                   // 4-byte LE signed int
constexpr uint32_t INT64 = 2;                   // 8-byte LE signed int
constexpr uint32_t DATE = 3;                    // 4-byte LE YYYYMMDD
constexpr uint32_t DEC64 = 4;                   // 8-byte LE scaled int (DEC64)
}  // namespace pax_kind

/**
  @brief Computes PAX cell widths, kinds, and scales for the encoded row fields
  of `table`.

  @details LineairDB rows store each MySQL field as the string payload produced
  by Field::val_str(). The returned vector contains one maximum payload width
  per encoded row field: entry 0 is the row null-flags field, and the remaining
  entries follow TABLE::field order. For an UNTYPED field the width is a safe
  upper bound on the val_str bytes; for a typed field (INT family, DATE) the
  width is the fixed binary payload width (4/8) and the kind tells the engine to
  parse val_str once at scatter and reformat it at gather. A table with any
  field wider than the PAX cell cap returns an empty vector so CREATE TABLE
  keeps the ordinary row layout instead of reserving very wide cells for every
  row. When non-null, `kinds`/`scales` are filled in lockstep with the returned
  widths.

  @return Per-field maximum payload widths, or an empty vector when the table
  should not use PAX storage.
*/
std::vector<uint32_t> compute_pax_field_widths(
    TABLE *table, std::vector<uint32_t> *kinds = nullptr,
    std::vector<int32_t> *scales = nullptr) {
  // Keep fixed-width cells bounded for variable-width columns such as TEXT.
  constexpr uint32_t kMaxCellBytes = 2048;

  std::vector<uint32_t> widths;
  widths.reserve(table->s->fields + 1);
  if (kinds) {
    kinds->clear();
    kinds->reserve(table->s->fields + 1);
    kinds->push_back(pax_kind::UNTYPED);  // field #0: null flags, verbatim
  }
  if (scales) {
    scales->clear();
    scales->reserve(table->s->fields + 1);
    scales->push_back(0);
  }
  widths.push_back(table->s->null_bytes);

  for (uint i = 0; i < table->s->fields; i++) {
    Field *field = table->field[i];
    uint32_t width = field->field_length;
    uint32_t kind = pax_kind::UNTYPED;
    int32_t scale = 0;

    // A ZEROFILL integer left-pads val_str to its display width, so the value
    // alone cannot reproduce the exact bytes -- keep such columns UNTYPED.
    // MYSQL_TYPE_YEAR renders zero-filled to its display width ("0000" for 0)
    // REGARDLESS of the Field_num::zerofill member (which is observed false at
    // runtime), so relying on that flag mis-types YEAR and gathers "0" != "0000"
    // -- force YEAR UNTYPED explicitly.
    const bool zerofill =
        (field->type() == MYSQL_TYPE_YEAR)
            ? true
            : ((field->type() == MYSQL_TYPE_TINY ||
                field->type() == MYSQL_TYPE_SHORT ||
                field->type() == MYSQL_TYPE_INT24 ||
                field->type() == MYSQL_TYPE_LONG ||
                field->type() == MYSQL_TYPE_LONGLONG ||
                field->type() == MYSQL_TYPE_DECIMAL ||
                field->type() == MYSQL_TYPE_NEWDECIMAL)
                   ? down_cast<const Field_num *>(field)->zerofill
                   : false);

    switch (field->type()) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
      case MYSQL_TYPE_YEAR:
        // Integer display width is not a payload bound for signed 64-bit text.
        width = std::max<uint32_t>(width, 21);
        if (!zerofill) {
          const bool is_ll = field->type() == MYSQL_TYPE_LONGLONG;
          const bool is_long = field->type() == MYSQL_TYPE_LONG;
          if (is_ll && field->is_unsigned()) {
            // BIGINT UNSIGNED can exceed int64 range: keep ASCII.
          } else if (is_ll || (is_long && field->is_unsigned())) {
            kind = pax_kind::INT64;  // 8-byte range
            width = 8;
          } else {
            kind = pax_kind::INT32;  // TINY/SHORT/INT24/LONG signed
            width = 4;
          }
        }
        break;
      case MYSQL_TYPE_FLOAT:
      case MYSQL_TYPE_DOUBLE:
        width = std::max<uint32_t>(width, 40);
        break;
      case MYSQL_TYPE_DECIMAL:
        // Legacy fixed-point text; reserve sign + decimal point slack (UNTYPED
        // bound). Not a Field_new_decimal, so it never becomes FK_DEC64.
        width += 2;
        break;
      case MYSQL_TYPE_NEWDECIMAL: {
        // Sign + decimal point slack for the UNTYPED bound, kept as the fallback
        // width when the value is not encoded as a scaled int64 below.
        width += 2;
        // A fixed-scale DECIMAL(p,s) becomes an 8-byte FK_DEC64 cell holding
        // value * 10^s as a scaled int64. The precision <= 15 cap has two
        // independent exactness reasons:
        //   (i)  the scaled int64 must hold every value: 10^15 - 1 < INT64_MAX;
        //   (ii) the server FILTER path compares (double)m / 10^s, which must
        //        equal strtod(val_str) EXACTLY to preserve the byte path's
        //        double compare semantics (q6's 0.07-excluding BETWEEN bound).
        //        That holds only while m and 10^s are both exact doubles, i.e.
        //        m < 2^53 <=> p <= 15 (10^15 < 2^53). The AGG path stays exact
        //        regardless via the int64 mantissa.
        // A ZEROFILL DECIMAL left-pads val_str, so its bytes are not
        // reproducible from the value alone -- keep it UNTYPED, as is p > 15.
        const auto *fd = down_cast<const Field_new_decimal *>(field);
        if (!zerofill && fd->precision <= 15) {
          kind = pax_kind::DEC64;
          scale = static_cast<int32_t>(field->decimals());
          width = 8;
        }
        break;
      }
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
        // DATE val_str is always "YYYY-MM-DD" -> YYYYMMDD int (fits int32).
        kind = pax_kind::DATE;
        width = 4;
        break;
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIMESTAMP2:
        width = std::max<uint32_t>(width, 32);
        break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
        // field_length is the charset octet length: utf8mb4 reserves 4 bytes
        // per declared character, padding a pure-ASCII VARCHAR(44) cell to 176
        // B. Size the cell to the declared character count instead (a no-op for
        // latin1/binary where mbmaxlen == 1). A genuine multibyte row whose
        // bytes exceed char_length() falls back to the existing per-row heap
        // path, which disables strip-direct scans for that table.
        width = field->char_length();
        break;
      default:
        break;
    }

    if (width > kMaxCellBytes) return {};
    widths.push_back(width);
    if (kinds) kinds->push_back(kind);
    if (scales) scales->push_back(scale);
  }

  return widths;
}

}  // namespace

void ha_lineairdb::set_key_and_key_part_info(const TABLE *const table) {
  key_info = table->key_info;
  uint pk_index = table->s->primary_key;

  if (pk_index != MAX_KEY) {
    primary_key_type = static_cast<ha_base_keytype>(
        table->key_info[pk_index].key_part[0].type);

    key_part = table->key_info[pk_index].key_part;
    indexed_key_part = key_part[0];
    num_key_parts = table->key_info[pk_index].user_defined_key_parts;
  } else {
    primary_key_type = HA_KEYTYPE_END;
    key_part = nullptr;
    num_key_parts = 0;
  }
}

int ha_lineairdb::open(const char *table_name, int, uint, const dd::Table *) {
  DBUG_TRACE;
  if (!(share = get_share()))
    return 1;
  thr_lock_data_init(&share->lock, &lock, nullptr);

  db_table_name = std::string(table_name);

  if ((num_keys = table->s->keys))
    set_key_and_key_part_info(table);

  if (table->s->primary_key != MAX_KEY) {
    // Calculate LineairDBField-encoded PK size. Each key part is encoded as:
    //   1 (null marker) + 1 (type tag) + 2 (length field) + payload
    // For STRING types, an extra terminator byte is added (+5 total overhead).
    // MySQL's key_length only counts raw column bytes, which is smaller.
    uint pk_index = table->s->primary_key;
    KEY *pk = &table->key_info[pk_index];
    size_t encoded_pk_size = 0;
    for (uint i = 0; i < pk->user_defined_key_parts; i++) {
      KEY_PART_INFO *part = &pk->key_part[i];
      Field *field = part->field;
      LineairDBFieldType ldb_type =
          convert_mysql_type_to_lineairdb(field->type());
      if (ldb_type == LineairDBFieldType::LINEAIRDB_STRING) {
        // STRING: marker(1) + type(1) + payload + terminator(1) + length(2)
        encoded_pk_size += 5 + part->length;
      } else {
        // INT/DATETIME/OTHER: marker(1) + type(1) + length(2) + payload
        encoded_pk_size += 4 + field->pack_length();
      }
    }
    ref_length = sizeof(uint16_t) + encoded_pk_size;
  } else {
    ref_length = sizeof(uint16_t) + serialize_hidden_primary_key(0).size();
  }

  return 0;
}

int ha_lineairdb::close(void) {
  DBUG_TRACE;
  return 0;
}

int ha_lineairdb::delete_table(const char *, const dd::Table *) {
  DBUG_TRACE;
  return 0;
}

int ha_lineairdb::rename_table(const char *, const char *, const dd::Table *,
                               dd::Table *) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

int ha_lineairdb::create(const char *table_name, TABLE *table, HA_CREATE_INFO *,
                         dd::Table *) {
  DBUG_TRACE;
  db_table_name = std::string(table_name);

  // create() is called without external_lock/start_stmt, so userThread may not
  // be set yet. Use ha_thd() to ensure get_proxy() can find the THD context.
  userThread = ha_thd();

  // In a disaggregated setup, multiple MySQL nodes share the same LineairDB
  // storage. The table/index may already exist from another node's CREATE
  // TABLE; MySQL-side metadata still needs to be created.
  auto proxy = get_proxy();
  std::vector<uint32_t> pax_kinds;
  std::vector<int32_t> pax_scales;
  std::vector<uint32_t> pax_widths =
      compute_pax_field_widths(table, &pax_kinds, &pax_scales);
  if (pax_widths.empty()) {  // table skips PAX: send nothing typed
    pax_kinds.clear();
    pax_scales.clear();
  }
  proxy->db_create_table(db_table_name, pax_widths, pax_kinds, pax_scales);

  for (uint i = 0; i < table->s->keys; i++) {
    auto key_info = table->key_info[i];
    uint index_type =
        (key_info.flags & HA_NOSAME) ? kUniqueSecondaryIndex : 0;
    if (i != table->s->primary_key) {
      proxy->db_create_secondary_index(
          db_table_name, std::string(key_info.name), index_type);
    }
  }
  return 0;
}

enum_alter_inplace_result ha_lineairdb::check_if_supported_inplace_alter(
    TABLE *altered_table [[maybe_unused]], Alter_inplace_info *ha_alter_info) {
  DBUG_TRACE;

  // DROP_INDEX is a no-op placeholder: the index data remains in LineairDB. It
  // must be accepted because MySQL sends ADD_INDEX | DROP_INDEX together when
  // replacing a foreign-key auto-index with an explicit CREATE INDEX.
  Alter_inplace_info::HA_ALTER_FLAGS dominated_flags =
      Alter_inplace_info::ADD_INDEX | Alter_inplace_info::DROP_INDEX |
      Alter_inplace_info::ADD_UNIQUE_INDEX |
      Alter_inplace_info::DROP_UNIQUE_INDEX;

  // ALTER TABLE ... SECONDARY_ENGINE = x|NULL only changes DD metadata.
  if (ha_alter_info->handler_flags ==
          Alter_inplace_info::CHANGE_CREATE_OPTION &&
      ha_alter_info->create_info != nullptr &&
      (ha_alter_info->create_info->used_fields &
       HA_CREATE_USED_SECONDARY_ENGINE) != 0 &&
      (ha_alter_info->create_info->used_fields &
       ~HA_CREATE_USED_SECONDARY_ENGINE) == 0) {
    return HA_ALTER_INPLACE_INSTANT;
  }

  if (ha_alter_info->handler_flags & ~dominated_flags) {
    return HA_ALTER_INPLACE_NOT_SUPPORTED;
  }

  return HA_ALTER_INPLACE_EXCLUSIVE_LOCK;
}

bool ha_lineairdb::backfill_commit_chunk(
    std::vector<LineairDBProxy::BatchOp> &ops) {
  if (ops.empty()) return true;

  auto *chunk_tx = new_transaction(ha_thd(), kFence);
  if (chunk_tx == nullptr) return false;
  chunk_tx->set_prefetch_mode(false);
  chunk_tx->begin_transaction();
  chunk_tx->choose_table(db_table_name);

  // One checked batch write per chunk so a server-side abort is observed.
  const bool wrote = chunk_tx->batch_write(db_table_name, ops);
  ops.clear();
  if (!wrote || chunk_tx->is_aborted()) {
    chunk_tx->set_status_to_abort();
    chunk_tx->end_transaction();
    return false;
  }
  return chunk_tx->end_transaction();
}

bool ha_lineairdb::backfill_indexes_parallel(
    std::vector<std::pair<std::string, std::string>> &rows,
    const std::vector<std::pair<std::string, const KEY *>> &specs) {
  // Phase A: decode each row once, build one write per index, and bucket it by
  // secondary-key hash. Single-threaded -- decode uses the shared record buffer.
  std::vector<std::vector<LineairDBProxy::BatchOp>> partition(
      kBackfillParallelWorkers);
  // Reserve each bucket to its expected hash share so the per-row push_back
  // below does not repeatedly reallocate the per-worker write buffers.
  const size_t total_ops = rows.size() * specs.size();
  const size_t reserve_per_bucket =
      total_ops == 0 ? 0
                     : total_ops / kBackfillParallelWorkers +
                           total_ops / (kBackfillParallelWorkers * 8) + 1;
  if (reserve_per_bucket != 0) {
    for (auto &bucket : partition) bucket.reserve(reserve_per_bucket);
  }
  std::hash<std::string> hasher;
  bool decode_failed = false;
  for (auto &row : rows) {
    if (row.second.empty()) continue;
    const auto *value = reinterpret_cast<const std::byte *>(row.second.data());
    if (set_fields_from_lineairdb(table->record[0], value, row.second.size())) {
      decode_failed = true;
      break;
    }
    for (const auto &spec : specs) {
      LineairDBProxy::BatchOp op;
      op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
      op.table_name = db_table_name;
      op.index_name = spec.first;
      op.primary_key = row.first;
      op.secondary_key =
          build_secondary_key_from_row(table->record[0], *spec.second);
      partition[hasher(op.secondary_key) % kBackfillParallelWorkers].push_back(
          std::move(op));
    }
  }
  blobroot.Clear();
  if (decode_failed) return false;

  // Phase B: one worker per partition, each on its own connection. The hash
  // partition commits every op for a secondary key on one worker, so no two
  // workers mutate the same index DataItem; distinct keys are distinct
  // DataItems committed through the normal concurrent path LineairDB serves for
  // multiple query layers. Workers touch no MySQL state; a failure sets the
  // shared flag for the caller to report.
  std::atomic<bool> failed{false};
  const std::string host = server_connection_host();
  const int port = server_connection_port();
  std::vector<std::thread> workers;
  workers.reserve(kBackfillParallelWorkers);
  for (size_t w = 0; w < kBackfillParallelWorkers; ++w) {
    if (partition[w].empty()) continue;
    workers.emplace_back([&, w]() {
      LineairDBProxy conn(host, port);
      std::vector<LineairDBProxy::BatchOp> chunk;
      chunk.reserve(kBackfillWriteChunkRows);
      // Ship the buffered writes as one stateless commit (no reads to validate).
      auto commit_chunk = [&]() -> bool {
        if (chunk.empty()) return true;
        std::string reason;
        const bool ok = conn.tx_validate_and_commit({}, {}, {}, {}, chunk, {},
                                                     kFence, &reason);
        chunk.clear();
        return ok;
      };
      for (auto &op : partition[w]) {
        if (failed.load(std::memory_order_relaxed)) return;
        chunk.push_back(std::move(op));
        if (chunk.size() >= kBackfillWriteChunkRows && !commit_chunk()) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
      if (!commit_chunk()) failed.store(true, std::memory_order_relaxed);
    });
  }
  for (auto &t : workers) t.join();
  return !failed.load(std::memory_order_relaxed);
}

bool ha_lineairdb::backfill_unique_serial(const std::string &index_name,
                                          const KEY &runtime_key) {
  // A unique index scans and commits serially through the staging path, which
  // keeps the in-write duplicate check. Its cost is small (no unique index is on
  // the large fact table); the parallel scan-once path is for the non-unique set.
  auto *scan_tx = get_transaction(ha_thd());
  if (scan_tx == nullptr || scan_tx->is_aborted()) return false;
  scan_tx->choose_table(db_table_name);
  auto rows = scan_tx->get_matching_keys_and_values_from_prefix(std::string());
  if (scan_tx->is_aborted()) return false;

  std::vector<LineairDBProxy::BatchOp> write_chunk;
  write_chunk.reserve(kBackfillWriteChunkRows);
  bool failed = false;
  for (auto &row : rows) {
    if (row.second.empty()) continue;
    const auto *value = reinterpret_cast<const std::byte *>(row.second.data());
    if (set_fields_from_lineairdb(table->record[0], value, row.second.size())) {
      failed = true;
      break;
    }
    LineairDBProxy::BatchOp op;
    op.type = LineairDBProxy::BatchOp::Type::SecondaryIndexWrite;
    op.table_name = db_table_name;
    op.index_name = index_name;
    op.primary_key = std::move(row.first);
    op.secondary_key =
        build_secondary_key_from_row(table->record[0], runtime_key);
    write_chunk.push_back(std::move(op));
    if (write_chunk.size() >= kBackfillWriteChunkRows &&
        !backfill_commit_chunk(write_chunk)) {
      failed = true;
      break;
    }
  }
  blobroot.Clear();
  if (failed || scan_tx->is_aborted() ||
      !backfill_commit_chunk(write_chunk)) {
    return false;
  }
  return true;
}

bool ha_lineairdb::inplace_alter_table(TABLE *altered_table,
                                       Alter_inplace_info *ha_alter_info,
                                       const dd::Table *old_table_def
                                       [[maybe_unused]],
                                       dd::Table *new_table_def
                                       [[maybe_unused]]) {
  DBUG_TRACE;

  // Fill each new secondary index from existing rows. The EXCLUSIVE metadata
  // lock is node-local, so this covers single-node ADD INDEX only; cross-node
  // DDL coordination belongs to the ddl-sync work.
  userThread = ha_thd();
  auto proxy = get_proxy();

  if (altered_table == nullptr || altered_table->s == nullptr) return true;

  // Non-unique indexes are collected and backfilled together below so a single
  // scan and decode pass feeds them all. A unique index keeps the staging commit
  // path (its in-write duplicate check) and is backfilled serially.
  std::vector<std::pair<std::string, const KEY *>> nu_specs;
  for (uint i = 0; i < ha_alter_info->index_add_count; i++) {
    const uint key_idx = ha_alter_info->index_add_buffer[i];
    const KEY *key_info = &ha_alter_info->key_info_buffer[key_idx];
    const std::string index_name(key_info->name ? key_info->name : "");
    if (index_name.empty()) return true;  // fail closed: unnamed index

    const uint index_type =
        (key_info->flags & HA_NOSAME) ? kUniqueSecondaryIndex : 0;

    // key_info_buffer and TABLE::key_info use different field-number bases;
    // resolve the runtime KEY by name or the encoder reads the wrong column.
    const KEY *runtime_key = nullptr;
    for (uint k = 0; k < altered_table->s->keys; ++k) {
      const KEY *candidate = &altered_table->key_info[k];
      if (candidate->name != nullptr && index_name == candidate->name) {
        runtime_key = candidate;
        break;
      }
    }
    if (runtime_key == nullptr) return true;

    // LineairDB treats an encoded NULL key as a duplicate, but SQL allows many
    // NULLs in a UNIQUE index; reject nullable UNIQUE backfill instead.
    if (key_info->flags & HA_NOSAME) {
      for (uint p = 0; p < runtime_key->user_defined_key_parts; ++p) {
        if (runtime_key->key_part[p].null_bit != 0) return true;
      }
    }

    // Register the index; fail closed on error. On a multi-index ALTER a later
    // failure leaves earlier backfilled indexes on the server (the DD rollback
    // hides them); purging them is the ddl-sync DROP work.
    if (!proxy->db_create_secondary_index(db_table_name, index_name,
                                          index_type)) {
      return true;
    }

    if (index_type == kUniqueSecondaryIndex) {
      if (!backfill_unique_serial(index_name, *runtime_key)) return true;
    } else {
      nu_specs.emplace_back(index_name, runtime_key);
    }
  }

  // Non-unique indexes share one scan and one decode pass, then commit in
  // parallel. A multi-index ALTER on the fact table makes this the common path.
  if (!nu_specs.empty()) {
    auto *scan_tx = get_transaction(ha_thd());
    if (scan_tx == nullptr || scan_tx->is_aborted()) return true;
    scan_tx->choose_table(db_table_name);

    auto rows =
        scan_tx->get_matching_keys_and_values_from_prefix(std::string());
    if (scan_tx->is_aborted()) return true;  // aborted scan: emit no writes
    if (!backfill_indexes_parallel(rows, nu_specs)) return true;
  }

  return false;
}
