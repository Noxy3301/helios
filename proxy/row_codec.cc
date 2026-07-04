#include "storage/lineairdb/ha_lineairdb.hh"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "my_dbug.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

// MySQL record <-> LineairDB row-byte conversion helpers used by DML and scan
// paths.

void ha_lineairdb::set_write_buffer(uchar *buf) {
  ldbField.set_null_field(buf, table->s->null_bytes);
  write_buffer_ = ldbField.get_null_field();

  String attribute;
  attribute.set_charset(&my_charset_bin);

  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->read_set);
  for (Field **field = table->field; *field; field++) {
    if ((*field)->is_nullable() && (*field)->is_null()) {
      ldbField.set_lineairdb_field("", 0);
    } else {
      attribute.length(0);
      (*field)->val_str(&attribute, &attribute);
      ldbField.set_lineairdb_field(attribute.c_ptr(), attribute.length());
    }
    write_buffer_ += ldbField.get_lineairdb_field();
  }
  tmp_restore_column_map(table->read_set, org_bitmap);
}

bool ha_lineairdb::is_primary_key_exists() {
  return table->s->primary_key != MAX_KEY;
}

bool ha_lineairdb::store_blob_to_field(Field **field) {
  if ((*field)->is_flag_set(BLOB_FLAG)) {
    Field_blob *blob_field = down_cast<Field_blob *>(*field);
    size_t length = blob_field->get_length();
    if (length > 0) {
      unsigned char *new_blob = new (&blobroot) unsigned char[length];
      if (new_blob == nullptr) return true;
      memcpy(new_blob, blob_field->get_blob_data(), length);
      blob_field->set_ptr(length, new_blob);
    }
  }
  return false;
}

int ha_lineairdb::set_fields_from_lineairdb(uchar *buf,
                                            const std::byte *const read_buf,
                                            const size_t read_buf_size) {
  // Clear BLOB data from the previous row.
  blobroot.ClearForReuse();
  ldbField.make_mysql_table_row(read_buf, read_buf_size);
  // For each 8 potentially-null columns, buf holds 1 byte flag at the front.
  // MySQL starts these bytes at 0xff and clears one bit per non-null column.
  const auto nullFlags = ldbField.get_null_flags();
  for (size_t i = 0; i < nullFlags.size(); i++) {
    buf[i] = static_cast<uchar>(nullFlags[i]);
  }

  // Avoid asserts in ::store() for columns that are not going to be updated.
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  THD *const thd_for_serve = ha_thd();
  const uint64_t serve_query_id =
      thd_for_serve != nullptr
          ? static_cast<uint64_t>(thd_for_serve->query_id)
          : 0;

  // Refresh once per statement, and again if statement-root staging registers
  // projection after an earlier unit serve populated the memo.
  const uint64_t proj_epoch =
      LineairDBTransaction::load_projection_global_epoch();
  if (serve_memo_query_id_ != serve_query_id ||
      serve_memo_proj_epoch_ != proj_epoch) {
    serve_memo_query_id_ = serve_query_id;
    serve_memo_proj_epoch_ = proj_epoch;
    auto tx = get_transaction(thd_for_serve);
    serve_memo_projection_ =
        tx != nullptr ? tx->load_table_projection(db_table_name) : nullptr;

    // DML may rebuild rows and secondary keys from this buffer, so only a
    // plain SELECT can leave unread fields untouched.
    serve_memo_can_skip_unread_fields_ =
        thd_for_serve != nullptr && thd_for_serve->lex != nullptr &&
        thd_for_serve->lex->sql_command == SQLCOM_SELECT;
  }

  const bool can_skip_unread_fields = serve_memo_can_skip_unread_fields_;

  {
    // Projected rows contain kept columns only; null flags remain full-width.
    const std::vector<uint32_t> *kept = serve_memo_projection_;

    // Unit-only prefetch can still serve full rows for a projected table.
    if (kept != nullptr && ldbField.get_row_size() != kept->size()) {
      kept = nullptr;
    }

    if (kept != nullptr) {
      for (size_t projected_col = 0; projected_col < kept->size();
           ++projected_col) {
        const uint32_t field_index = (*kept)[projected_col];
        if (field_index >= table->s->fields) break;
        if (can_skip_unread_fields &&
            !bitmap_is_set(table->read_set, field_index)) {
          continue;
        }
        Field *f = table->field[field_index];
        const auto mysqlFieldValue =
            ldbField.get_column_of_row(projected_col);
        if (f->is_nullable() && f->is_null_in_record(buf)) {
          f->set_null();
        } else {
          f->store(mysqlFieldValue.data(), mysqlFieldValue.size(),
                   &my_charset_bin, CHECK_FIELD_WARN);
          Field *arr[2] = {f, nullptr};
          if (store_blob_to_field(arr)) {
            dbug_tmp_restore_column_map(table->write_set, org_bitmap);
            return HA_ERR_OUT_OF_MEM;
          }
        }
      }
      dbug_tmp_restore_column_map(table->write_set, org_bitmap);
      return 0;
    }
  }

  // Full rows map parsed column index directly to TABLE::field[].
  size_t columnIndex = 0;
  for (Field **field = table->field; *field; field++) {
    if (columnIndex >= ldbField.get_row_size()) break;
    const auto mysqlFieldValue = ldbField.get_column_of_row(columnIndex++);
    if (can_skip_unread_fields &&
        !bitmap_is_set(table->read_set, (*field)->field_index())) {
      continue;
    }
    if ((*field)->is_nullable() && (*field)->is_null_in_record(buf)) {
      (*field)->set_null();
    } else {
      (*field)->store(mysqlFieldValue.data(), mysqlFieldValue.size(),
                      &my_charset_bin, CHECK_FIELD_WARN);
      if (store_blob_to_field(field)) return HA_ERR_OUT_OF_MEM;
    }
  }
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  return 0;
}
