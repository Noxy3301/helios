/**
 * @file server/storage/src/pax/table.cc
 * Slot allocation and the typed cell round trip behind the PAX strips.
 */

#include "pax/table.h"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

namespace helios::storage {
namespace pax {

namespace {

// ---------------------------------------------------------------------------
// Row bytes follow plugin/helios_field.cc: a width byte, that many
// little-endian length bytes, then the payload. Width 0xFF marks an empty
// field; the first field carries the row's SQL NULL flags.
//
// unpack_row converts typed fields from text to fixed-width binary values.
// ScatterRow copies those values into PAX; GatherRow restores the input bytes.
// Values are rejected if parsing fails, they exceed the supported range, or
// gathering would change their bytes.
// ---------------------------------------------------------------------------

// int64 from a full ASCII integer (from_chars, whole span consumed).
inline bool ParseI64(const char *s, size_t len, int64_t &out) {
  if (len == 0) return false;
  const auto res = std::from_chars(s, s + len, out);
  return res.ec == std::errc() && res.ptr == s + len;
}

// n ASCII digits, most significant first, accumulated into out.
inline bool parse_digits(const char *p, int n, int64_t &out) {
  for (int i = 0; i < n; i++) {
    if (p[i] < '0' || p[i] > '9') return false;
    out = out * 10 + (p[i] - '0');
  }
  return true;
}

// "YYYY-MM-DD" -> YYYYMMDD (fits int32; `string order == int order`).
inline bool ParseDate(const char *s, size_t len, int64_t &out) {
  if (len != 10 || s[4] != '-' || s[7] != '-') return false;
  int64_t y = 0, m = 0, d = 0;
  if (!parse_digits(s, 4, y) || !parse_digits(s + 5, 2, m) ||
      !parse_digits(s + 8, 2, d)) {
    return false;
  }
  out = y * 10000 + m * 100 + d;
  return true;
}

// Exact DECIMAL(p,s) val_str -> scaled int64 (`value * 10^scale`). The input has
// the column's declared scale of fractional digits (MySQL pads), so scaling is
// exact and no double is involved.
inline bool ParseDecScaled(const char *s, size_t len, int scale, int64_t &out) {
  if (len == 0 || scale < 0 || scale > 18) return false;
  size_t i = 0;
  bool neg = false;
  if (s[0] == '-') {
    neg = true;
    i = 1;
  } else if (s[0] == '+') {
    i = 1;
  }
  __int128 mant = 0;
  int frac_digits = 0;
  bool seen_dot = false, any = false;
  for (; i < len; i++) {
    const char c = s[i];
    if (c == '.') {
      if (seen_dot) return false;
      seen_dot = true;
      continue;
    }
    if (c < '0' || c > '9') return false;
    const __int128 limit = static_cast<__int128>(INT64_MAX) + 1;
    if (mant > (limit - (c - '0')) / 10) return false;
    mant = mant * 10 + (c - '0');
    if (seen_dot) frac_digits++;
    any = true;
  }
  if (!any) return false;
  // A different scale would change the bytes and size when gathered.
  if (frac_digits != scale) return false;
  if (neg) mant = -mant;
  if (mant > INT64_MAX || mant < INT64_MIN) return false;
  out = static_cast<int64_t>(mant);
  return true;
}

// Parse one field's ASCII into the low bytes of out. Returns false on any
// failure, so commit can reject the value before installing any write.
inline bool ParseTyped(FieldType type, int scale, const std::byte *payload,
                       uint32_t len, uint64_t &out) {
  const char *s = reinterpret_cast<const char *>(payload);
  switch (type) {
    case FieldType::kInt32: {
      int64_t v;
      if (!ParseI64(s, len, v) || v < INT32_MIN || v > INT32_MAX) return false;
      out = static_cast<uint64_t>(v);
      return true;
    }
    case FieldType::kInt64: {
      int64_t v;
      if (!ParseI64(s, len, v)) return false;
      out = static_cast<uint64_t>(v);
      return true;
    }
    case FieldType::kDate: {
      int64_t ymd;
      if (!ParseDate(s, len, ymd)) return false;
      out = static_cast<uint64_t>(ymd);
      return true;
    }
    case FieldType::kDecimal64: {
      int64_t m;
      if (!ParseDecScaled(s, len, scale, m)) return false;
      out = static_cast<uint64_t>(m);
      return true;
    }
    default:
      return false;
  }
}

inline void AppendI64(std::string &out, int64_t v) {
  char buf[24];
  const auto res = std::to_chars(buf, buf + sizeof(buf), v);
  out.append(buf, res.ptr);
}

// Format a typed cell's `width` LE bytes back into the exact val_str ASCII.
// `cell` points at the payload (at least `width` bytes are readable because
// the cell stride reserves them, so this is memory-safe even under a torn
// read).
void FormatTyped(FieldType type, int scale, const std::byte *cell,
                 uint32_t width, std::string &out) {
  (void)width;
  switch (type) {
    case FieldType::kInt32: {
      int32_t v;
      std::memcpy(&v, cell, 4);
      AppendI64(out, v);
      break;
    }
    case FieldType::kInt64: {
      int64_t v;
      std::memcpy(&v, cell, 8);
      AppendI64(out, v);
      break;
    }
    case FieldType::kDate: {
      int32_t v;
      std::memcpy(&v, cell, 4);
      const int64_t y = v / 10000, m = (v / 100) % 100, d = v % 100;
      char buf[16];
      // %04d-%02d-%02d, matching MySQL DATE val_str.
      const int n = std::snprintf(
          buf, sizeof(buf), "%04lld-%02lld-%02lld", static_cast<long long>(y),
          static_cast<long long>(m), static_cast<long long>(d));
      if (n > 0) out.append(buf, static_cast<size_t>(n));
      break;
    }
    case FieldType::kDecimal64: {
      int64_t mant;
      std::memcpy(&mant, cell, 8);
      const bool neg = mant < 0;
      __int128 rest = neg ? -static_cast<__int128>(mant) : mant;
      std::string digits;
      if (rest == 0) {
        digits = "0";
      } else {
        while (rest) {
          digits.push_back(static_cast<char>('0' + int(rest % 10)));
          rest /= 10;
        }
        std::reverse(digits.begin(), digits.end());
      }
      while (static_cast<int>(digits.size()) <= scale)
        digits.insert(digits.begin(), '0');
      if (neg) out.push_back('-');
      if (scale == 0) {
        out += digits;
      } else {
        const size_t int_len = digits.size() - static_cast<size_t>(scale);
        out.append(digits, 0, int_len);
        out.push_back('.');
        out.append(digits, int_len, std::string::npos);
      }
      break;
    }
    default:
      break;
  }
}

// splitmix64's finalizer: spreads keys that differ in one bit across the
// whole word, which the sketch's register choice depends on.
inline uint64_t mix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ull;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
  return x ^ (x >> 31);
}

// Row format marker for an empty/no-value field.
constexpr std::byte kNoValue{0xFF};

// Each field's strip starts on a cache line.
constexpr size_t kStripAlign = 64;
static_assert(PaxGroup::kRows % PaxGroup::kVisibilityWordBits == 0,
              "the visibility bitmap covers whole words");

/**
 * @brief Returns the minimum little-endian base-256 byte count for `len`.
 *
 * @details This mirrors the length-width rule of the row format, so gathered
 * rows are byte-identical to the ones the query layer writes.
 */
inline uint32_t LengthPrefixBytes(uint32_t len) {
  uint32_t n = 0;
  for (uint32_t v = len; v > 0; v /= 256) n++;
  return n;
}

// Writes one field in row format: the marker for an empty payload, otherwise
// the length-prefix width, the little-endian length and the bytes.
void AppendField(std::string &out, std::string_view payload) {
  if (payload.empty()) {
    out.push_back(static_cast<char>(kNoValue));
    return;
  }
  const uint32_t len = static_cast<uint32_t>(payload.size());
  const uint32_t prefix = LengthPrefixBytes(len);
  out.push_back(static_cast<char>(prefix));
  for (uint32_t i = 0; i < prefix; i++) {
    out.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
  }
  out.append(payload.data(), payload.size());
}

/**
 * @brief Parses row bytes into per-field payload references.
 *
 * @param row Row bytes.
 * @param size Number of bytes in `row`.
 * @param out Destination array for the field references.
 * @param max_fields Maximum number of entries available in `out`.
 * @return Number of fields read, or `SIZE_MAX` when the input is malformed
 * or contains more than `max_fields` fields.
 */
size_t split_row_fields(const std::byte *row, size_t size, Row::Field *out,
                        size_t max_fields) {
  size_t off = 0;
  size_t n = 0;
  while (off < size) {
    if (n == max_fields) return SIZE_MAX;
    const auto prefix = static_cast<uint8_t>(row[off]);
    off += 1;
    uint32_t len = 0;
    if (row[off - 1] != kNoValue) {
      if (prefix > 4 || off + prefix > size) return SIZE_MAX;
      for (uint32_t i = 0; i < prefix; i++) {
        len |= static_cast<uint32_t>(static_cast<uint8_t>(row[off + i]))
               << (8 * i);
      }
      off += prefix;
      if (len == 0 || prefix != LengthPrefixBytes(len)) return SIZE_MAX;
      if (off + len > size) return SIZE_MAX;
    }
    out[n].payload = row + off;
    out[n].len = len;
    off += len;
    n++;
  }
  return (off == size) ? n : SIZE_MAX;
}

}  // namespace

bool unpack_row(const TableSchema &schema, const std::byte *value, size_t size,
               Row &out) {
  const size_t fields = schema.field_count();
  if (fields == 0) return false;
  out.fields.resize(fields);
  if (split_row_fields(value, size, out.fields.data(), fields) != fields) {
    return false;
  }

  // Keep the conversion result so installation only copies prepared cells.
  for (size_t f = 0; f < fields; ++f) {
    auto &field = out.fields[f];
    const FieldType type = schema.type_of(f);
    if (type == FieldType::kUntyped) {
      if (field.len > schema.field_max_bytes[f] || field.len > UINT16_MAX)
        return false;
    } else if (field.len != 0) {
      if (!ParseTyped(type, schema.scale_of(f), field.payload, field.len,
                      field.typed_value))
        return false;
      std::string restored;
      FormatTyped(type, schema.scale_of(f),
                  reinterpret_cast<const std::byte *>(&field.typed_value),
                  schema.field_max_bytes[f], restored);
      if (restored !=
          std::string_view(reinterpret_cast<const char *>(field.payload),
                           field.len))
        return false;
    }
  }
  out.size = size;
  return true;
}

PaxGroup::PaxGroup(const TableSchema &schema, PaxTable &table)
    : schema_(schema), table_(table) {
  const size_t fields = schema.field_count();
  stride_.resize(fields);
  strip_offset_.resize(fields);
  size_t total = 0;
  for (size_t f = 0; f < fields; f++) {
    stride_[f] = kCellLenBytes + schema.field_max_bytes[f];
    total = (total + kStripAlign - 1) & ~size_t{kStripAlign - 1};
    strip_offset_[f] = total;
    total += static_cast<size_t>(stride_[f]) * kRows;
  }
  arena_.reset(new std::byte[total]());  // zero-init: len=0 everywhere
  // Allocate visibility flags for this group's slots.
  visible_.reset(new std::atomic<uint64_t>[kRows / kVisibilityWordBits]());
}

PaxGroup::~PaxGroup() { ForgetGroup(this); }

void PaxGroup::ScatterRow(uint32_t slot, const Row &row) {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  assert(row.fields.size() == fields);
  for (size_t f = 0; f < fields; f++) {
    const auto &field = row.fields[f];
    std::byte *cell = arena_.get() + strip_offset_[f] +
                      static_cast<size_t>(stride_[f]) * slot;
    const FieldType type = schema_.type_of(f);
    if (type != FieldType::kUntyped && field.len != 0) {
      const uint16_t len = static_cast<uint16_t>(schema_.field_max_bytes[f]);
      std::memcpy(cell, &len, sizeof(len));
      std::memcpy(cell + kCellLenBytes, &field.typed_value, len);
      table_.observe(f, static_cast<int64_t>(field.typed_value));
      continue;
    }
    // Untyped bytes and NULL markers need no numeric conversion.
    const uint16_t len = static_cast<uint16_t>(field.len);
    std::memcpy(cell, &len, sizeof(len));
    if (len > 0) std::memcpy(cell + kCellLenBytes, field.payload, len);
  }
  // Publish this slot to strip-direct readers after the cells are written.
  visible_[slot / kVisibilityWordBits].fetch_or(
      uint64_t{1} << (slot % kVisibilityWordBits), std::memory_order_release);
}

void PaxGroup::RetireSlot(uint32_t slot) {
  assert(slot < kRows);
  // Hide this slot from strip-direct readers.
  visible_[slot / kVisibilityWordBits].fetch_and(
      ~(uint64_t{1} << (slot % kVisibilityWordBits)),
      std::memory_order_release);
}

size_t PaxGroup::GatherRow(uint32_t slot, std::byte *dst,
                           size_t expected_size) const {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  std::string scratch;  // reused typed->ASCII buffer (no per-field alloc)
  size_t off = 0;
  for (size_t f = 0; f < fields; f++) {
    // Load and clamp the cell length: a torn read can produce garbage but
    // must stay memory-safe. The caller's TID re-check rejects torn rows.
    const std::byte *cell = arena_.get() + strip_offset_[f] +
                            static_cast<size_t>(stride_[f]) * slot;
    uint16_t len;
    std::memcpy(&len, cell, sizeof(len));
    if (len > schema_.field_max_bytes[f]) len = 0;
    // Emit the empty-field marker.
    if (len == 0) {
      if (off + 1 > expected_size) return off;
      dst[off++] = kNoValue;
      continue;
    }
    // Resolve the payload: typed ASCII, or the untyped bytes.
    const FieldType type = schema_.type_of(f);
    const char *src;
    uint32_t vlen;
    if (type != FieldType::kUntyped) {
      scratch.clear();
      FormatTyped(type, schema_.scale_of(f), cell + kCellLenBytes,
                  schema_.field_max_bytes[f], scratch);
      src = scratch.data();
      vlen = static_cast<uint32_t>(scratch.size());
    } else {
      src = reinterpret_cast<const char *>(cell + kCellLenBytes);
      vlen = len;
    }
    // Write the length prefix and the payload.
    const uint32_t prefix = LengthPrefixBytes(vlen);
    if (off + 1 + prefix + vlen > expected_size) return off;
    dst[off++] = static_cast<std::byte>(prefix);
    for (uint32_t i = 0; i < prefix; i++) {
      dst[off++] = static_cast<std::byte>((vlen >> (8 * i)) & 0xFF);
    }
    std::memcpy(dst + off, src, vlen);
    off += vlen;
  }
  return off;
}

void PaxGroup::AppendCellField(size_t field, uint32_t slot,
                               std::string &out) const {
  const std::string_view cv = cell(field, slot);
  const FieldType type = schema_.type_of(field);
  if (type == FieldType::kUntyped || cv.empty()) {
    AppendField(out, cv);
    return;
  }
  const std::byte *c = arena_.get() + strip_offset_[field] +
                       static_cast<size_t>(stride_[field]) * slot;
  std::string tmp;
  FormatTyped(type, schema_.scale_of(field), c + kCellLenBytes,
              schema_.field_max_bytes[field], tmp);
  AppendField(out, tmp);
}

bool PaxGroup::GatherRowProjected(uint32_t slot, const uint32_t *columns,
                                  size_t n_columns, std::string &out) const {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  AppendCellField(0, slot, out);  // null-flags field (always UNTYPED)
  for (size_t i = 0; i < n_columns; i++) {
    const size_t field = static_cast<size_t>(columns[i]) + 1;
    if (field >= fields) return false;
    AppendCellField(field, slot, out);
  }
  return true;
}

void PaxGroup::GatherRowMasked(uint32_t slot, const uint32_t *columns,
                               size_t n_columns, std::string &out) const {
  assert(slot < kRows);
  const size_t fields = schema_.field_count();
  AppendCellField(0, slot, out);  // null-flags field (always UNTYPED)

  // Merge the ascending MySQL column numbers onto fields `1..n`; mark the rest
  // empty.
  size_t column_index = 0;
  for (size_t field = 1; field < fields; ++field) {
    if (column_index < n_columns &&
        static_cast<size_t>(columns[column_index]) + 1 == field) {
      AppendCellField(field, slot, out);
      ++column_index;
      continue;
    }
    out.push_back(static_cast<char>(kNoValue));
  }
}

PaxTable::PaxTable(TableSchema schema) : schema_(std::move(schema)) {
  dir_.reset(new std::atomic<PaxGroup *>[kMaxGroups]());
  const size_t fields = schema_.field_count();
  // Start every field's range empty; Observe widens the typed ones.
  lo_.reset(new std::atomic<int64_t>[fields]);
  hi_.reset(new std::atomic<int64_t>[fields]);
  for (size_t f = 0; f < fields; f++) {
    lo_[f].store(INT64_MAX, std::memory_order_relaxed);
    hi_[f].store(INT64_MIN, std::memory_order_relaxed);
  }
  sketch_.reset(new std::atomic<uint8_t>[fields * kSketchRegisters]());
}

PaxTable::~PaxTable() {
  for (size_t i = 0; i < group_count(); ++i) delete group(i);
}

std::pair<PaxGroup *, uint32_t> PaxTable::AllocateSlot() {
  const uint64_t idx = next_slot_.fetch_add(1, std::memory_order_relaxed);
  const uint64_t group_idx = idx / PaxGroup::kRows;
  if (group_idx >= kMaxGroups) return {nullptr, 0};
  PaxGroup *grp = dir_[group_idx].load(std::memory_order_acquire);
  if (grp == nullptr) {
    std::lock_guard<std::mutex> lk(alloc_mutex_);
    grp = dir_[group_idx].load(std::memory_order_acquire);
    if (grp == nullptr) {
      grp = new PaxGroup(schema_, *this);
      dir_[group_idx].store(grp, std::memory_order_release);
    }
  }
  return {grp, static_cast<uint32_t>(idx % PaxGroup::kRows)};
}

void PaxTable::observe(size_t field, int64_t value) {
  std::atomic<int64_t> &lo = lo_[field];
  int64_t low = lo.load(std::memory_order_relaxed);
  while (value < low && !lo.compare_exchange_weak(low, value,
                                                  std::memory_order_relaxed)) {
  }
  std::atomic<int64_t> &hi = hi_[field];
  int64_t high = hi.load(std::memory_order_relaxed);
  while (value > high && !hi.compare_exchange_weak(high, value,
                                                   std::memory_order_relaxed)) {
  }

  // HyperLogLog: the top bits pick the register, the run of zeros below them
  // is the rank. Neighbouring keys must land in unrelated registers, so the
  // value passes through a finalizer first.
  const uint64_t h = mix64(static_cast<uint64_t>(value));
  const size_t reg_index = static_cast<size_t>(h >> (64 - kSketchBits));
  const uint64_t tail = h << kSketchBits;
  const uint8_t rank =
      tail == 0 ? static_cast<uint8_t>(64 - kSketchBits + 1)
                : static_cast<uint8_t>(__builtin_clzll(tail) + 1);
  std::atomic<uint8_t> &reg = sketch_[field * kSketchRegisters + reg_index];
  uint8_t seen = reg.load(std::memory_order_relaxed);
  while (rank > seen &&
         !reg.compare_exchange_weak(seen, rank, std::memory_order_relaxed)) {
  }
}

bool PaxTable::range(size_t field, int64_t *lo, int64_t *hi) const {
  if (field >= schema_.field_count()) return false;
  const int64_t low = lo_[field].load(std::memory_order_relaxed);
  const int64_t high = hi_[field].load(std::memory_order_relaxed);
  if (low > high) return false;
  *lo = low;
  *hi = high;
  return true;
}

bool PaxTable::distinct(size_t field, uint64_t *ndv) const {
  if (field >= schema_.field_count()) return false;
  const std::atomic<uint8_t> *regs = &sketch_[field * kSketchRegisters];
  double inverse = 0;
  size_t empty = 0;
  for (size_t i = 0; i < kSketchRegisters; i++) {
    const uint8_t rank = regs[i].load(std::memory_order_relaxed);
    if (rank == 0) empty++;
    inverse += std::ldexp(1.0, -rank);
  }
  if (empty == kSketchRegisters) return false;
  const double m = static_cast<double>(kSketchRegisters);
  // Flajolet's alpha_m for the raw estimate.
  const double alpha = 0.7213 / (1.0 + 1.079 / m);
  double estimate = alpha * m * m / inverse;
  // At 2.5m and below the raw estimate is biased; the small-range correction
  // reads the empty registers instead.
  if (estimate <= 2.5 * m && empty > 0) {
    estimate = m * std::log(m / static_cast<double>(empty));
  }
  if (estimate < 1.0) estimate = 1.0;
  // Every register at its highest rank puts the raw estimate past 2^64, which
  // has no uint64_t to convert to.
  *ndv = estimate >= 0x1p64 ? UINT64_MAX : static_cast<uint64_t>(estimate);
  return true;
}

const TableSchema &Schema(const PaxTable *store) { return store->schema(); }

PaxGroup *Group(const PaxTable *store, size_t idx) { return store->group(idx); }

uint64_t SlotsAllocated(const PaxTable *store) {
  return store->slots_allocated();
}

size_t GroupCount(const PaxTable *store) { return store->group_count(); }

bool ColumnRange(const PaxTable *store, size_t field, int64_t *lo,
                 int64_t *hi) {
  return store->range(field, lo, hi);
}

bool ColumnDistinct(const PaxTable *store, size_t field, uint64_t *ndv) {
  return store->distinct(field, ndv);
}


}  // namespace pax
}  // namespace helios::storage
