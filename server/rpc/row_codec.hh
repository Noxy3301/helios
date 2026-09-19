#pragma once

#include <cstdint>
#include <string>
#include <string_view>

// Byte-level pack/unpack helpers shared across the RPC handlers:
// HeliosField-format row access plus the int-keyed primary-key layout
// mirrored from the proxy.

// Bump the byte string to its lexicographic successor; empty on overflow.
std::string next_lexicographic_key(std::string key);

// Bytes of one column of a serialized row [null_flags][col_0]..[col_n], each
// field [byteSize:1B][len:byteSize B][bytes], byteSize 0xFF meaning NULL.
std::string_view unpack_row_field(const std::string& row,
                                      int column_index);

/**
 * @brief Build an int-keyed primary-key part in Helios's byte layout.
 *
 * @details [0x00 not-null][0x10 INT tag][2-byte big-endian length 4][4-byte
 * signed int with the top bit flipped], the layout ha_helios writes, so
 * byte-wise order matches signed integer order.
 */
std::string pack_int_key_part(int64_t value);

bool unpack_leading_int_key(std::string_view key, int64_t& out);

// Parse a decimal-string column and pack it as int-keyed primary key bytes.
std::string pack_column_as_int_key(std::string_view column,
                                     int64_t int_delta);
