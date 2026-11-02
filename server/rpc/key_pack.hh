#pragma once

#include <cstdint>
#include <string>
#include <string_view>

// Byte-level pack/unpack helpers shared across the RPC handlers:
// HeliosField-format row access plus the int-keyed primary-key layout
// mirrored from the plugin.

// Bump the byte string to its lexicographic successor. A string of 0xFF bytes
// has none, so it gets a key that sorts after every stored key.
std::string next_lexicographic_key(std::string key);

// Bytes of one column of a packed row [null_flags][col_0]..[col_n], each
// field [byteSize:1B][len:byteSize B][bytes]. A byteSize of 0xFF marks an
// empty payload; whether the column is NULL is in the null flags.
std::string_view unpack_row_field(const std::string& row, int column_index);

/**
 * @brief Build a NOT NULL INT key part in Helios's byte layout.
 *
 * @details The 4-byte big-endian signed int with the top bit flipped, the
 * layout ha_helios writes, so byte-wise order matches signed integer order.
 */
std::string pack_int_key_part(int64_t value);

/**
 * @brief Read a key's first four bytes as a packed INT part.
 *
 * @details The value never decreases as the key grows, whatever the key's
 * first part is, so it can split a key range into contiguous pieces.
 *
 * @return false when the key is shorter than four bytes.
 */
bool unpack_leading_int_key(std::string_view key, int64_t& out);

// Parse a decimal-string column and pack it as int-keyed primary key bytes.
std::string pack_column_as_int_key(std::string_view column, int64_t int_delta);
