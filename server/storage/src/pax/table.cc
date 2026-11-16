/**
 * @file server/storage/src/pax/table.cc
 * Slot allocation and the row copies in and out of the PAX strips.
 */

#include "pax/table.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>

#include "common/typed_cell.h"

namespace helios::storage {
namespace pax {

namespace {

// Row bytes follow plugin/helios_field.cc: a width byte, that many LE length
// bytes, then the payload; width 0xFF marks an empty field. The first field is
// the SQL NULL flags, and a typed field's payload is its cell.

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

  for (size_t f = 0; f < fields; ++f) {
    auto &field = out.fields[f];
    const FieldType type = schema.type_of(f);
    if (type == FieldType::kUntyped) {
      if (field.len > schema.field_max_bytes[f] || field.len > UINT16_MAX)
        return false;
    } else if (field.len != 0) {
      // A typed field is its cell. typed_value keeps it, sign-extended, for
      // ScatterRow to copy and for the column statistics.
      if (field.len != PackLength(type)) return false;
      field.typed_value = static_cast<uint64_t>(
          helios::rpc::load_cell(field.payload, field.len));
    } else if (!schema.has_len(f)) {
      return false;  // a cell without a length cannot hold an empty value
    }
  }
  out.size = size;
  return true;
}

PaxGroup::Layout::Layout(const TableSchema &schema,
                         std::vector<uint32_t> widths)
    : cell_bytes(std::move(widths)) {
  len_bytes.resize(cell_bytes.size());
  strip_offset.resize(cell_bytes.size());
  size_t total = 0;
  for (size_t f = 0; f < cell_bytes.size(); f++) {
    len_bytes[f] =
        cell_bytes[f] != 0 && schema.has_len(f) ? kCellLenBytes : 0;
    total = (total + kStripAlign - 1) & ~size_t{kStripAlign - 1};
    strip_offset[f] = total;
    total += static_cast<size_t>(stride(f)) * kRows;
  }
  arena.reset(new std::byte[total]());
}

PaxGroup::PaxGroup(const TableSchema &schema, PaxTable &table,
                   std::vector<uint32_t> cell_bytes, uint32_t index,
                   uint32_t size_class)
    : schema_(schema),
      table_(table),
      layout_(schema, std::move(cell_bytes)),
      index_(index),
      size_class_(size_class) {
  // Allocate visibility flags for this group's slots.
  visible_.reset(new std::atomic<uint64_t>[kRows / kVisibilityWordBits]());
}

PaxGroup::~PaxGroup() { ForgetGroup(this); }

bool PaxGroup::ScatterRow(uint32_t slot, const Row &row, bool compare,
                          uint64_t &changed) {
  assert(slot < kRows);
  const size_t field_count = schema_.field_count();
  assert(row.fields.size() == field_count);
  const Layout &layout = layout_;
  // Refuse before writing, so the row's old cells stay intact for its move.
  for (size_t f = 0; f < field_count; f++) {
    if (schema_.type_of(f) == FieldType::kUntyped &&
        row.fields[f].len > layout.cell_bytes[f])
      return false;
  }

  changed = compare ? 0 : ~uint64_t{0};
  for (size_t field_index = 0; field_index < field_count; field_index++) {
    // unpack_row admits only an empty value to a field without a strip.
    if (layout.stride(field_index) == 0) continue;
    const auto &field = row.fields[field_index];
    std::byte *cell = layout.arena.get() + layout.strip_offset[field_index] +
                      static_cast<size_t>(layout.stride(field_index)) * slot;
    // Typed cells take the binary value; untyped bytes and NULL markers need
    // no numeric conversion.
    uint16_t len = static_cast<uint16_t>(field.len);
    const void *src = field.payload;
    if (schema_.type_of(field_index) != FieldType::kUntyped && field.len != 0) {
      len = static_cast<uint16_t>(schema_.field_max_bytes[field_index]);
      src = &field.typed_value;
    }

    const bool has_len = schema_.has_len(field_index);
    std::byte *payload = has_len ? cell + kCellLenBytes : cell;

    // Leave a cell that already holds these bytes untouched.
    if (compare) {
      uint16_t old = len;
      if (has_len) std::memcpy(&old, cell, sizeof(old));
      if (old == len && (len == 0 || std::memcmp(payload, src, len) == 0))
        continue;
      changed |= uint64_t{1} << std::min<size_t>(field_index, 63);
    }
    if (src == &field.typed_value)
      table_.observe(field_index, static_cast<int64_t>(field.typed_value));
    if (has_len) std::memcpy(cell, &len, sizeof(len));
    if (len > 0) std::memcpy(payload, src, len);
  }
  // Publish this slot to strip-direct readers after the cells are written.
  visible_[slot / kVisibilityWordBits].fetch_or(
      uint64_t{1} << (slot % kVisibilityWordBits), std::memory_order_release);
  return true;
}

void PaxGroup::update_max_column_tid(uint64_t fields, uint64_t tid) {
  // A CAS, not a store: installs on different rows arrive out of TID order.
  for (; fields != 0; fields &= fields - 1) {
    auto &column = column_tid_[__builtin_ctzll(fields)];
    uint64_t old = column.load(std::memory_order_relaxed);
    while (old < tid && !column.compare_exchange_weak(old, tid)) {
    }
  }
}

uint64_t PaxGroup::max_column_tid(uint64_t fields) const {
  uint64_t max = 0;
  for (; fields != 0; fields &= fields - 1)
    max = std::max(max, column_tid_[__builtin_ctzll(fields)].load(
                            std::memory_order_acquire));
  return max;
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
  const Layout &layout = this->layout();
  size_t off = 0;
  for (size_t f = 0; f < fields; f++) {
    // cell() clamps the length: a torn read can produce garbage but must stay
    // memory-safe. The caller's TID re-check rejects torn rows.
    const std::string_view value = layout.cell(f, slot);
    // Emit the empty-field marker.
    if (value.empty()) {
      if (off + 1 > expected_size) return off;
      dst[off++] = kNoValue;
      continue;
    }
    // Write the length prefix and the cell's bytes.
    const uint32_t len = static_cast<uint32_t>(value.size());
    const uint32_t prefix = LengthPrefixBytes(len);
    if (off + 1 + prefix + len > expected_size) return off;
    dst[off++] = static_cast<std::byte>(prefix);
    for (uint32_t i = 0; i < prefix; i++) {
      dst[off++] = static_cast<std::byte>((len >> (8 * i)) & 0xFF);
    }
    std::memcpy(dst + off, value.data(), len);
    off += len;
  }
  return off;
}

PaxTable::PaxTable(TableSchema schema) : schema_(std::move(schema)) {
  const size_t fields = schema_.field_count();
  // Four classes per doubling from 16 bytes, up to the widest UNTYPED width.
  uint32_t top = 0;
  for (size_t f = 0; f < fields; f++) {
    if (schema_.type_of(f) == FieldType::kUntyped)
      top = std::max(top, schema_.field_max_bytes[f]);
  }
  for (uint32_t c = 16; c < top; c += (1u << (31 - __builtin_clz(c))) / 4)
    class_bytes_.push_back(c);
  class_bytes_.push_back(top);
  classes_.reset(new SizeClass[class_bytes_.size()]);
  dir_.reset(new std::atomic<PaxGroup *>[kMaxGroups]());
  range_.reset(new ValueRange[fields]);
  sketch_.reset(new HyperLogLog[fields]);
}

PaxTable::~PaxTable() {
  for (size_t i = 0; i < group_count(); ++i) delete group(i);
}

PaxGroup *PaxTable::add_group(uint32_t cls) {
  std::lock_guard<std::mutex> lk(dir_mutex_);
  const size_t idx = group_count_.load(std::memory_order_relaxed);
  if (idx >= kMaxGroups) return nullptr;
  std::vector<uint32_t> cell_bytes = schema_.field_max_bytes;
  for (size_t f = 0; f < cell_bytes.size(); f++) {
    if (schema_.type_of(f) == FieldType::kUntyped)
      cell_bytes[f] = std::min(cell_bytes[f], class_bytes_[cls]);
  }
  auto *grp = new PaxGroup(schema_, *this, std::move(cell_bytes),
                           static_cast<uint32_t>(idx), cls);
  dir_[idx].store(grp, std::memory_order_release);
  group_count_.store(idx + 1, std::memory_order_release);
  return grp;
}

std::pair<PaxGroup *, uint32_t> PaxTable::allocate(uint32_t cls,
                                                   EpochNumber epoch) {
  const uint32_t top = static_cast<uint32_t>(class_bytes_.size() - 1);
  // Group 0 is a top-class group; create it before any narrower one.
  if (cls != top && group_count() == 0) {
    SizeClass &t = classes_[top];
    std::lock_guard<std::mutex> lk(t.mutex);
    if (t.groups.load(std::memory_order_relaxed) == 0) {
      PaxGroup *grp = add_group(top);
      t.groups.fetch_add(1, std::memory_order_relaxed);
      t.slot.store(uint64_t{grp->index()} << 32, std::memory_order_release);
    }
  }

  SizeClass &c = classes_[cls];
  for (;;) {
    // Take a fresh slot without the lock; acquire pairs with the release
    // store that names the group, so its directory entry is visible.
    const uint64_t w = c.slot.fetch_add(1, std::memory_order_acquire);
    if (static_cast<uint32_t>(w) < PaxGroup::kRows)
      return {group(w >> 32), static_cast<uint32_t>(w)};

    PaxGroup *grp;
    {
      std::lock_guard<std::mutex> lk(c.mutex);
      // A group installed since the fetch_add has fresh slots left.
      const uint64_t cur = c.slot.load(std::memory_order_relaxed);
      if (static_cast<uint32_t>(cur) < PaxGroup::kRows) continue;
      // Pull the slot back to kRows so misses cannot carry into the index.
      c.slot.store(cur >> 32 << 32 | PaxGroup::kRows,
                   std::memory_order_relaxed);

      // Checked against this commit's epoch, which is within one of the
      // global epoch.
      if (!c.retired.empty() && c.retired.front().first + 2 <= epoch) {
        const auto slot = c.retired.front().second;
        c.retired.pop_front();
        return slot;
      }
      grp = c.spare.exchange(nullptr);
      if (grp == nullptr) grp = add_group(cls);
      if (grp == nullptr) return {nullptr, 0};
      c.groups.fetch_add(1, std::memory_order_relaxed);
      c.slot.store(uint64_t{grp->index()} << 32 | 1, std::memory_order_release);
    }

    // A class on its first group gets no spare, so a sparse class holds one.
    if (c.groups.load(std::memory_order_relaxed) > 1)
      spare_wanted_.store(true);
    return {grp, 0};
  }
}

void PaxTable::BuildSpares() {
  if (!spare_wanted_.exchange(false)) return;
  for (uint32_t cls = 0; cls < class_bytes_.size(); cls++) {
    SizeClass &c = classes_[cls];
    // One builder a class at a time.
    bool idle = false;
    if (c.groups.load(std::memory_order_relaxed) <= 1 ||
        c.spare.load() != nullptr ||
        !c.building.compare_exchange_strong(idle, true))
      continue;
    if (c.spare.load() == nullptr) c.spare.store(add_group(cls));
    c.building.store(false);
    // A spare taken before `building` cleared left its request unserved.
    if (c.spare.load() == nullptr) spare_wanted_.store(true);
  }
}

std::pair<PaxGroup *, uint32_t> PaxTable::AllocateSlot(const Row &row,
                                                       EpochNumber epoch) {
  uint32_t len = 0;
  for (size_t f = 0; f < row.fields.size(); f++) {
    if (schema_.type_of(f) == FieldType::kUntyped)
      len = std::max(len, row.fields[f].len);
  }
  const auto cls =
      std::lower_bound(class_bytes_.begin(), class_bytes_.end(), len) -
      class_bytes_.begin();
  return allocate(static_cast<uint32_t>(cls), epoch);
}

void PaxTable::Retire(PaxGroup *group, uint32_t slot, EpochNumber epoch) {
  group->RetireSlot(slot);
  SizeClass &c = classes_[group->size_class()];
  std::lock_guard<std::mutex> lk(c.mutex);
  c.retired.push_back({epoch, {group, slot}});
}

uint64_t PaxTable::slots_allocated() const {
  // Every earlier group of a class was filled before the next was installed.
  uint64_t n = 0;
  for (size_t i = 0; i < class_bytes_.size(); i++) {
    const uint64_t w = classes_[i].slot.load(std::memory_order_acquire);
    const uint32_t groups = classes_[i].groups.load(std::memory_order_relaxed);
    if (groups == 0) continue;
    n += uint64_t{groups - 1} * PaxGroup::kRows +
         std::min(static_cast<uint32_t>(w), PaxGroup::kRows);
  }
  return n;
}

void PaxTable::observe(size_t field, int64_t value) {
  range_[field].add(value);
  sketch_[field].add(value);
}

bool PaxTable::range(size_t field, int64_t *lo, int64_t *hi) const {
  if (field >= schema_.field_count()) return false;
  return range_[field].read(lo, hi);
}

bool PaxTable::distinct(size_t field, uint64_t *ndv) const {
  if (field >= schema_.field_count()) return false;
  return sketch_[field].estimate(ndv);
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
