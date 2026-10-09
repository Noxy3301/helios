/**
 * @file server/rpc/collation.cc
 * Sort key and LIKE written from UTS #10 revision 34 (UCA 9.0.0) and DUCET
 * allkeys-9.0.0.txt: https://www.unicode.org/reports/tr10/tr10-34.html
 */
#include "server/rpc/collation.hh"

#include <cstring>

#include "server/rpc/uca900_primary.hh"

namespace collation {
namespace {

// Decodes one well-formed UTF-8 sequence (Unicode Table 3-7); 0 if ill-formed.
size_t utf8_decode(const uint8_t *s, const uint8_t *end, char32_t *cp) {
  const uint8_t b = s[0];
  if (b < 0x80) {
    *cp = b;
    return 1;
  }

  size_t n;
  char32_t c;
  uint8_t lo = 0x80;
  uint8_t hi = 0xBF;
  if (b < 0xC2) {
    return 0;
  } else if (b < 0xE0) {
    n = 2;
    c = b & 0x1F;
  } else if (b < 0xF0) {
    n = 3;
    c = b & 0x0F;
    if (b == 0xE0) lo = 0xA0;
    if (b == 0xED) hi = 0x9F;
  } else if (b < 0xF5) {
    n = 4;
    c = b & 0x07;
    if (b == 0xF0) lo = 0x90;
    if (b == 0xF4) hi = 0x8F;
  } else {
    return 0;
  }
  if (static_cast<size_t>(end - s) < n || s[1] < lo || s[1] > hi) return 0;

  c = c << 6 | (s[1] & 0x3F);
  for (size_t i = 2; i < n; i++) {
    if ((s[i] & 0xC0) != 0x80) return 0;
    c = c << 6 | (s[i] & 0x3F);
  }
  *cp = c;
  return n;
}

uint16_t table_value(char32_t cp) {
  if (cp >= kUcaEnd) return kAbsent;
  return kUcaPages[(kUcaPageIndex[cp >> kUcaPageBits] << kUcaPageBits) |
                   (cp & ((1 << kUcaPageBits) - 1))];
}

// Whether the block of 256 code points holding cp has a DUCET entry; blocks
// without one map to page 0.
bool block_has_entry(char32_t cp) {
  return cp < kUcaEnd && kUcaPageIndex[cp >> kUcaPageBits] != 0;
}

// Writes the primary weights of cp to w and returns their count.
int weights(char32_t cp, uint16_t *w) {
  // Hangul syllables weigh as their jamo L V [T], one weight each; MySQL
  // decomposes the whole Hangul Syllables block, AC00..D7AF.
  if (cp >= 0xAC00 && cp <= 0xD7AF) {
    const char32_t s = cp - 0xAC00;
    w[0] = table_value(0x1100 + s / 588);
    w[1] = table_value(0x1161 + s % 588 / 28);
    if (s % 28 == 0) return 2;
    w[2] = table_value(0x11A7 + s % 28);
    return 3;
  }

  const uint16_t v = table_value(cp);
  if (v == kIgnorable) return 0;
  if (v & kExpansion) {
    const uint16_t *e = kUcaExpansions + (v & ~kExpansion);
    std::memcpy(w, e + 1, e[0] * sizeof(*w));
    return e[0];
  }
  if (v != kAbsent) {
    w[0] = v;
    return 1;
  }

  // Implicit weights, UTS #10 Table 16 with the Unicode 9.0 Tangut and
  // Unified_Ideograph ranges.
  if (cp >= 0x17000 && cp <= 0x18AFF) {
    w[0] = 0xFB00;
    w[1] = (cp - 0x17000) | 0x8000;
    return 2;
  }
  uint16_t base = 0xFBC0;
  if (cp >= 0x4E00 && cp <= 0x9FD5) {
    base = 0xFB40;
  } else if ((cp >= 0x3400 && cp <= 0x4DB5) ||
             (cp >= 0x20000 && cp <= 0x2A6D6) ||
             (cp >= 0x2A700 && cp <= 0x2B734) ||
             (cp >= 0x2B740 && cp <= 0x2B81D) ||
             (cp >= 0x2B820 && cp <= 0x2CEA1)) {
    base = 0xFB80;
  }
  w[0] = base + (cp >> 15);
  w[1] = (cp & 0x7FFF) | 0x8000;
  return 2;
}

// Length of the LIKE pattern character at p, 0 if ill-formed; c is its code
// point, which the wildcards and the escape compare with.
size_t pattern_char(const uint8_t *p, const uint8_t *end, int *c) {
  char32_t cp;
  const size_t n = utf8_decode(p, end, &cp);
  if (n == 0) return 0;
  *c = cp;
  return n;
}

}  // namespace

size_t utf8mb4_0900_ai_ci_key(std::string_view s, uint8_t *dst) {
  const auto *p = reinterpret_cast<const uint8_t *>(s.data());
  const auto *end = p + s.size();
  uint8_t *out = dst;
  while (p != end) {
    char32_t cp;
    const size_t n = utf8_decode(p, end, &cp);
    if (n == 0) break;
    p += n;

    // One weight, the common case, skips the general path.
    const uint16_t v = table_value(cp);
    if (v > kAbsent && (v & kExpansion) == 0) {
      *out++ = v >> 8;
      *out++ = v & 0xFF;
      continue;
    }
    uint16_t w[kMaxWeights];
    const int count = weights(cp, w);
    for (int i = 0; i < count; i++) {
      *out++ = w[i] >> 8;
      *out++ = w[i] & 0xFF;
    }
  }
  return out - dst;
}

// Greedy match that, on a mismatch, lets the last '%' absorb one more text
// character and retries from there; O(text * pattern) characters.
bool utf8mb4_0900_ai_ci_like(std::string_view text, std::string_view pattern,
                             int escape) {
  const int one = escape == '_' ? -1 : '_';
  const int many = escape == '%' ? -1 : '%';
  const auto *t = reinterpret_cast<const uint8_t *>(text.data());
  const auto *t_end = t + text.size();
  const auto *p = reinterpret_cast<const uint8_t *>(pattern.data());
  const auto *p_end = p + pattern.size();
  const uint8_t *star_p = nullptr;
  const uint8_t *star_t = nullptr;
  char32_t tc = 0;

  for (;;) {
    if (p != p_end) {
      int pc;
      size_t pn = pattern_char(p, p_end, &pc);
      if (pn == 0) return false;
      if (pc == many) {
        // A run of '%' and '_' takes one text character per '_' up front.
        p += pn;
        while (p != p_end) {
          pn = pattern_char(p, p_end, &pc);
          if (pn == 0) return false;
          if (pc != many && pc != one) break;
          if (pc == one) {
            if (t == t_end) return false;
            const size_t tn = utf8_decode(t, t_end, &tc);
            if (tn == 0) return false;
            t += tn;
          }
          p += pn;
        }
        if (p == p_end) return true;
        star_p = p;
        star_t = t;
        continue;
      }
      if (t != t_end) {
        const size_t tn = utf8_decode(t, t_end, &tc);
        if (tn == 0) return false;
        if (pc == one) {
          p += pn;
          t += tn;
          continue;
        }
        const bool scan = p == star_p;
        if (pc == escape && p + pn != p_end) {
          p += pn;
          pn = pattern_char(p, p_end, &pc);
          if (pn == 0) return false;
        }
        // One weight or none on both sides compares the table values; a code
        // point in a block without DUCET entries equals only itself.
        bool eq = static_cast<char32_t>(pc) == tc;
        if (!eq) {
          const uint16_t a = table_value(pc);
          const uint16_t b = table_value(tc);
          if (a != kAbsent && b != kAbsent && ((a | b) & kExpansion) == 0) {
            eq = a == b;
          } else if (block_has_entry(pc) && block_has_entry(tc)) {
            uint16_t wp[kMaxWeights];
            uint16_t wt[kMaxWeights];
            const int n = weights(pc, wp);
            eq = n == weights(tc, wt) &&
                 std::memcmp(wp, wt, n * sizeof(*wp)) == 0;
          }
        }
        if (eq) {
          p += pn;
          t += tn;
          continue;
        }
        if (scan) {
          star_t = t + tn;
          t = star_t;
          p = star_p;
          continue;
        }
      }
    } else if (t == t_end) {
      return true;
    }

    if (star_p == nullptr || star_t == t_end) return false;
    const size_t n = utf8_decode(star_t, t_end, &tc);
    if (n == 0) return false;
    star_t += n;
    t = star_t;
    p = star_p;
  }
}

}  // namespace collation
