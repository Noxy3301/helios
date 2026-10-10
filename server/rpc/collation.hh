/**
 * @file server/rpc/collation.hh
 * MySQL string semantics the DuckDB executor evaluates: the utf8mb4_0900_ai_ci
 * sort key and LIKE under utf8mb4_0900_ai_ci, utf8mb4_0900_bin and binary.
 */
#ifndef HELIOS_SERVER_RPC_COLLATION_H
#define HELIOS_SERVER_RPC_COLLATION_H

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace collation {

// A code point takes at least one byte and yields at most 8 two-byte weights.
constexpr size_t kKeyBytesPerByte = 16;

/**
 * @brief Writes the utf8mb4_0900_ai_ci sort key of a UTF-8 string.
 *
 * The key is the big-endian 16-bit UCA 9.0.0 primary weights of the string's
 * code points in order (NO PAD, variable weights non-ignorable, no
 * contractions, Hangul syllables decomposed into jamo). Comparing two keys
 * bytewise, the shorter prefix first, orders the strings; equal keys mean
 * equal strings. The key covers the bytes before the first ill-formed UTF-8
 * sequence.
 *
 * @param s   UTF-8 bytes.
 * @param dst Output of at least kKeyBytesPerByte * s.size() bytes.
 * @return Bytes written to dst.
 */
size_t utf8mb4_0900_ai_ci_key(std::string_view s, uint8_t *dst);

/**
 * @brief MySQL LIKE of text against pattern under utf8mb4_0900_ai_ci.
 *
 * '%' matches any run of characters and '_' exactly one code point. The
 * escape code point makes the next pattern character literal; at the end of
 * the pattern it is literal itself. An escape equal to '%' or '_' replaces
 * that wildcard. A literal matches one text code point that is the same code
 * point, or that has the same primary weights when both code points lie in
 * blocks of 256 code points with a DUCET entry. A primary-ignorable code point
 * still takes one position.
 * Ill-formed UTF-8 at a character the match reads makes the result false.
 *
 * @param text    UTF-8 value.
 * @param pattern UTF-8 pattern.
 * @param escape  Escape code point, as the ESCAPE clause evaluates.
 * @return True when text matches pattern.
 */
bool utf8mb4_0900_ai_ci_like(std::string_view text, std::string_view pattern,
                             int escape);

/**
 * @brief MySQL LIKE of text against pattern under utf8mb4_0900_bin.
 *
 * Same wildcards as utf8mb4_0900_ai_ci_like over characters that are
 * well-formed UTF-8 sequences or else single bytes. A literal matches its
 * own bytes in the text; right after a '%' run it must match a whole text
 * character. The wildcards and the escape compare with the first byte of a
 * pattern character, and the escape skips that byte only.
 *
 * @param text    Value bytes.
 * @param pattern Pattern bytes.
 * @param escape  Escape code point, as the ESCAPE clause evaluates.
 * @return True when text matches pattern.
 */
bool utf8mb4_0900_bin_like(std::string_view text, std::string_view pattern,
                           int escape);

/**
 * @brief MySQL LIKE of text against pattern under binary.
 *
 * Same wildcards over bytes ('_' is one byte); a literal matches the same
 * byte. The wildcards and the escape compare with a pattern byte read as a
 * signed char, so 0xFF stands for the wildcard an escape of '%' or '_'
 * replaces; right after a '%' run the escape compares with the unsigned byte.
 *
 * @param text    Value bytes.
 * @param pattern Pattern bytes.
 * @param escape  Escape byte value, as the ESCAPE clause evaluates.
 * @return True when text matches pattern.
 */
bool binary_like(std::string_view text, std::string_view pattern, int escape);

}  // namespace collation

#endif  // HELIOS_SERVER_RPC_COLLATION_H
