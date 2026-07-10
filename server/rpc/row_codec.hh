#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include <google/protobuf/repeated_field.h>
#include <lineairdb/pax_store.h>

// Byte-level encode/decode helpers shared across the RPC handlers:
// LineairDBField-format row access plus the int-keyed primary-key layout
// mirrored from the proxy.

// Bump the byte string to its lexicographic successor; returns empty on overflow.
std::string next_lexicographic_key(std::string key);

/**
 * @brief Borrowed view of one row stored in a PAX group.
 *
 * Server-side evaluators use this to read column values from PAX strips without
 * materializing a full LineairDBField-format row.
 */
struct PaxRowRef {
    const LineairDB::Pax::PaxGroup* group = nullptr;
    uint32_t slot = 0;
};

/**
 * @brief Trim a serialized row value to the projected columns.
 *
 * @details Row format is [null_flags][col_0]..[col_n], where each field is
 * [byteSize:1B][len:byteSize B][bytes] and byteSize==0xFF means NULL. The
 * output keeps the full null_flags field and emits only the kept columns.
 * Returns false on malformed input; the caller then fails the read-plan
 * response instead of shipping a mismatched row layout.
 */
bool trim_row_value(const std::string& full,
                    const google::protobuf::RepeatedField<uint32_t>& kept,
                    uint32_t num_columns, std::string& out);

/**
 * @brief Return the bytes of a column from a serialized MySQL row payload.
 */
std::string_view extract_value_column(const std::string& row,
                                      int column_index);

/**
 * @brief Return the bytes of a column from a PAX row slot.
 *
 * @details The returned view is the raw stored cell. For a typed column
 * (FieldKind != FK_UNTYPED) these are fixed-width little-endian binary, not
 * ASCII, so callers must decode by the column's declared kind
 * (format_typed_cell / decode_typed_i64 / typed_key_view) and never parse the
 * bytes directly.
 */
std::string_view extract_value_column(const PaxRowRef& row, int column_index);

// ---------------------------------------------------------------------------
// Typed PAX cell codec. A typed column's cell holds fixed-width LE binary; an
// UNTYPED column's cell holds verbatim val_str ASCII. Every render/decode is
// schema-driven by the column's declared FieldKind -- a typed int whose LE
// bytes happen to be ASCII digits (808464432 == "0000") would otherwise be
// silently mis-parsed, so the raw bytes are never sniffed.
// ---------------------------------------------------------------------------

/**
 * @brief Append the exact val_str ASCII of a cell to `*out`.
 *
 * @details Verbatim for FK_UNTYPED (and empty cells); reformatted from the
 * fixed-width binary for a typed cell. Matches the engine's gather formatting
 * byte-for-byte.
 */
void format_typed_cell(uint8_t kind, int scale, std::string_view value,
                       std::string* out);

/**
 * @brief Decode an integer/int-key candidate cell to int64.
 *
 * @details FK_INT32/FK_INT64 decode the native binary; FK_UNTYPED parses a
 * full-length ASCII integer. Returns false for SQL NULL, a non-integer UNTYPED
 * cell, and FK_DATE/FK_DEC64 -- the latter keeps DATE/DECIMAL keys on the
 * string path so hash-join build and probe decide int-ness symmetrically.
 */
bool decode_typed_i64(std::string_view value, uint8_t kind, int64_t* out);

/**
 * @brief Canonical val_str ASCII view of a cell.
 *
 * @details Returns the cell itself for FK_UNTYPED or an empty cell; otherwise
 * formats into `buf` (whose storage backs the returned view).
 */
std::string_view typed_key_view(std::string_view value, uint8_t kind, int scale,
                                std::string& buf);

/**
 * @brief Build an int-keyed primary-key part in LineairDB's byte layout.
 *
 * @details Mirrors the layout produced by ha_lineairdb::append_key_part_encoding,
 * so server-side read-plan scan boundaries match what the proxy wrote into
 * LineairDB:
 *
 * @verbatim
 * [0x00 not-null marker | 0x10 INT type tag | 2-byte big-endian length=4
 *  | 4-byte signed int with top bit flipped]
 * @endverbatim
 *
 * Flipping the top bit makes byte-wise lexicographic order match signed integer
 * order, so Masstree can sort without knowing the column type.
 *
 * TODO: factor this and ha_lineairdb's encoder into a shared encoder so the two
 * ends cannot drift.
 */
std::string encode_int_key_part(int64_t value);

/**
 * @brief Decode the leading integer key part produced by encode_int_key_part.
 */
bool decode_leading_int_key(std::string_view key, int64_t& out);

// Parse a decimal-string column and encode it as int-keyed primary key bytes.
std::string encode_column_as_int_key(std::string_view column,
                                     int64_t int_delta);
