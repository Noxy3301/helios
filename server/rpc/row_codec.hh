#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include <google/protobuf/repeated_field.h>

// Byte-level encode/decode helpers shared across the RPC handlers:
// LineairDBField-format row access plus the int-keyed primary-key layout
// mirrored from the proxy.

// Bump the byte string to its lexicographic successor; returns empty on overflow.
std::string next_lexicographic_key(std::string key);

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

// Return the bytes of column `column_index` from a serialized MySQL row payload.
std::string_view extract_value_column(const std::string& row,
                                      int column_index);

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
