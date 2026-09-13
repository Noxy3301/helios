/**
 * @file server/storage/include/storage/pax.h
 * The PAX row format a table declares at creation, the strips a reader scans
 * in place, and the before-image surface that resolves a row a writer changed
 * under an open read view.
 */

#ifndef HELIOS_STORAGE_INCLUDE_STORAGE_PAX_H
#define HELIOS_STORAGE_INCLUDE_STORAGE_PAX_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace helios::storage {
namespace pax {

/**
 * @brief How a PAX cell stores a field: its storage type, not its SQL type.
 *
 * @details kUntyped keeps the cell verbatim: the field bytes the query
 * layer wrote, unchanged. A typed field stores the value as a fixed-width
 * little-endian binary payload of `field_max_bytes[f]` bytes (4 or 8). A
 * typed cell's u16 length prefix is 0 for SQL NULL and the binary width for a
 * present value, so "empty cell == NULL" still holds. ScatterRow parses that
 * text once and returns false without touching the slot when it does not
 * parse or does not fit; the row then overflows to the heap. GatherRow
 * reformats a typed cell back into the exact bytes it was given.
 */
enum class FieldType : uint8_t {
  kUntyped = 0,    // verbatim bytes (default; strings, floats, untyped DECIMAL)
  kInt32 = 1,      // 4-byte LE signed int   (TINY/SHORT/INT24/LONG)
  kInt64 = 2,      // 8-byte LE signed int   (LONG UNSIGNED, BIGINT)
  kDate = 3,       // 4-byte LE YYYYMMDD int (DATE)
  kDecimal64 = 4,  // 8-byte LE scaled int   (DECIMAL(p,s); scale=field_scale)
};

/**
 * @brief Returns the fixed binary width of a typed cell, 0 for kUntyped.
 */
inline uint32_t PackLength(FieldType type) {
  switch (type) {
    case FieldType::kInt32:
    case FieldType::kDate:
      return 4;
    case FieldType::kInt64:
    case FieldType::kDecimal64:
      return 8;
    default:
      return 0;
  }
}

/**
 * @brief Describes the row format fields a PAX-enabled table stores in strips.
 *
 * @details A row is one null-flags field followed by one field per column;
 * PaxGroup uses the maximum payload byte widths here to split it into
 * fixed-width cells. UNTYPED cells store `[u16 len][payload up to
 * field_max_bytes]`; typed cells store `[u16 len][fixed-width LE binary]`
 * with `field_max_bytes` equal to the binary width (4/8).
 */
struct TableSchema {
  // Max payload bytes per field, starting with the null-flags field.
  std::vector<uint32_t> field_max_bytes;
  // Per-field storage type (see FieldType). Empty => every field untyped
  // (byte-identical to the untyped layout). Same length as field_max_bytes.
  std::vector<FieldType> field_type;
  // Per-field DECIMAL scale for kDecimal64 (else 0). Same length when present.
  std::vector<int8_t> field_scale;
  // Table name, carried for diagnostics (overflow logging).
  std::string table_name;

  size_t field_count() const { return field_max_bytes.size(); }

  /**
   * @brief Returns the storage type of field `f`, or kUntyped when
   *        `field_type` does not cover it.
   *
   * @details A type whose declared width is not the width that type stores
   * degrades to kUntyped, so no reader parses a cell in a shape never written.
   */
  FieldType type_of(size_t f) const {
    const FieldType type =
        f < field_type.size() ? field_type[f] : FieldType::kUntyped;
    if (type == FieldType::kUntyped) return FieldType::kUntyped;
    return f < field_max_bytes.size() && field_max_bytes[f] == PackLength(type)
               ? type
               : FieldType::kUntyped;
  }

  /**
   * @brief Returns the DECIMAL scale of field `f`, or 0 when `field_scale`
   *        does not cover it.
   */
  int scale_of(size_t f) const {
    return f < field_scale.size() ? field_scale[f] : 0;
  }
};

class PaxTable;

/**
 * @brief Stores a fixed-size row group as per-field PAX strips.
 *
 * @details Each logical row occupies the same slot number in every field
 * strip. The row's bytes are stored in the strip cells; `DataItem` continues
 * to own the transaction id word, so Silo validation remains outside this
 * storage class. Callers invoke `ScatterRow()` only while holding the row's
 * existing Silo TID lock. `GatherRow()` is memory-safe under a concurrent
 * scatter to the same slot, and callers reject torn rows with the normal TID
 * re-check. Strip-direct readers resolve concurrent writers through the
 * columnar read view surface below.
 */
class PaxGroup {
 public:
  // Number of row slots in one group.
  static constexpr uint32_t kRows = 8192;

  // Number of bytes used for the little-endian uint16_t cell length.
  static constexpr uint32_t kCellLenBytes = 2;

  // Width of one word of the slot visibility bitmap.
  static constexpr uint32_t kVisibilityWordBits = 64;

  /**
   * @brief Creates an empty group whose strips are sized from `schema`.
   *
   * @param schema Table schema owned by `PaxTable`; must outlive this group.
   */
  PaxGroup(const TableSchema &schema, PaxTable *store);

  /**
   * @brief Scatters one row into this group's strip cells.
   *
   * @param slot Target slot inside this group.
   * @param row Row bytes.
   * @param size Number of bytes in `row`.
   * @return false without writing any cell when the payload does not match
   * the schema shape, the table has more than 512 fields, an UNTYPED field
   * exceeds its cell width, or a typed field fails its parse or range check.
   */
  bool ScatterRow(uint32_t slot, const std::byte *row, size_t size);

  /**
   * @brief Marks one slot as invisible to strip-direct readers.
   *
   * @param slot Target slot inside this group.
   */
  void RetireSlot(uint32_t slot);

  /**
   * @brief Gathers one slot's strip cells back into a byte-identical row.
   *
   * @param slot Source slot inside this group.
   * @param dst Destination buffer with room for `expected_size` bytes.
   * @param expected_size Row payload length tracked in `DataBuffer::size`.
   * @return Number of bytes written, equal to `expected_size` when the slot is
   * quiet and the stored row is intact.
   */
  size_t GatherRow(uint32_t slot, std::byte *dst, size_t expected_size) const;

  /**
   * @brief Gathers the null-flags field and selected columns into `out`.
   *
   * @param slot Source slot inside this group.
   * @param columns Zero-based MySQL column indexes to gather.
   * @param n_columns Number of entries in `columns`.
   * @param out Destination string; gathered bytes are appended.
   * @return false when a column index is outside this group's schema; `out`
   * may already hold the null-flags field and the columns before it.
   */
  bool GatherRowProjected(uint32_t slot, const uint32_t *columns,
                          size_t n_columns, std::string &out) const;

  /**
   * @brief Gathers a row-shaped payload with unselected columns masked out.
   *
   * @details The null-flags field is always gathered. Zero-based MySQL columns
   * listed in `columns` are gathered from their strips; unlisted columns are
   * emitted as one-byte empty fields, preserving field indexes without reading
   * unused strips.
   *
   * @param slot Source slot inside this group.
   * @param columns Zero-based MySQL column indexes to materialize, in ascending
   * order.
   * @param n_columns Number of entries in `columns`.
   * @param out Destination string; gathered bytes are appended.
   */
  void GatherRowMasked(uint32_t slot, const uint32_t *columns, size_t n_columns,
                       std::string &out) const;

  /**
   * @brief Returns whether strip-direct readers should consider `slot` live.
   *
   * @param slot Slot inside this group.
   */
  bool IsVisible(uint32_t slot) const {
    // Read this slot's visibility flag for strip-direct scans.
    return (visible_[slot / kVisibilityWordBits].load(
                std::memory_order_acquire) >>
            (slot % kVisibilityWordBits)) &
           1u;
  }

  /**
   * @brief Returns a memory-safe view of one cell payload.
   *
   * @details Verbatim bytes for an UNTYPED cell, little-endian binary for a
   * typed one. This does not reformat a typed cell into row-format bytes;
   * AppendCellField does.
   *
   * @param field Field index, where 0 is the null-flags field and MySQL column
   * i is field i + 1.
   * @param slot Slot inside this group.
   */
  std::string_view cell(size_t field, uint32_t slot) const {
    const std::byte *base = arena_.get() + strip_offset_[field] +
                            static_cast<size_t>(stride_[field]) * slot;
    uint16_t len;
    std::memcpy(&len, base, sizeof(len));
    if (len > schema_.field_max_bytes[field]) len = 0;
    return std::string_view(
        reinterpret_cast<const char *>(base) + kCellLenBytes, len);
  }

  const std::byte *strip(size_t field) const {
    return arena_.get() + strip_offset_[field];
  }

  uint32_t stride(size_t field) const { return stride_[field]; }

  const TableSchema &schema() const { return schema_; }

  PaxTable *table() const { return table_; }

 private:
  /**
   * @brief Appends one field's row-format value into `out`.
   *
   * @details Verbatim for an UNTYPED cell; reformatted to the exact val_str
   * ASCII for a typed present cell. An empty cell is emitted as a NULL field.
   *
   * @param field Field index, where 0 is the null-flags field and MySQL column
   * i is field i + 1.
   * @param slot Slot inside this group.
   * @param out Destination string; the packed field is appended.
   */
  void AppendCellField(size_t field, uint32_t slot, std::string &out) const;

  const TableSchema &schema_;  // Owned by PaxTable; outlives all groups.
  PaxTable *table_;
  std::vector<uint32_t> stride_;
  std::vector<size_t> strip_offset_;
  std::unique_ptr<std::byte[]> arena_;
  // Slot visibility flags for strip-direct scans.
  std::unique_ptr<std::atomic<uint64_t>[]> visible_;
};

/**
 * @brief Returns the schema every group in `store` is sized from.
 */
const TableSchema &Schema(const PaxTable *store);

/**
 * @brief Returns group `idx` of `store`, or nullptr if it is not allocated.
 */
PaxGroup *Group(const PaxTable *store, size_t idx);

/**
 * @brief Returns slots handed out, an upper bound on populated rows.
 */
uint64_t SlotsAllocated(const PaxTable *store);

/**
 * @brief Returns the number of row groups that may contain allocated slots.
 */
size_t GroupCount(const PaxTable *store);

/**
 * @brief Returns the number of rows that overflowed to the heap.
 */
uint64_t OverflowCount(const PaxTable *store);

// ---------------------------------------------------------------------------
// Columnar read view surface.
//
// While a read view acquired through Database::AcquirePaxView is active,
// every PAX install captures the replaced row image into a per-group undo
// map before its first strip mutation, or fails the capture for the active
// generation when it cannot (src/pax/version_store.h holds the full
// contract). A reader at snapshot epoch se uses the oldest before-image
// whose writer epoch exceeds se. was_visible == false means the slot held
// no row. With no such entry, the reader uses the strip in place.
// ---------------------------------------------------------------------------

/**
 * @brief One captured before-image for a (group, slot).
 */
struct UndoEntry {
  uint32_t writer_epoch;  // commit epoch of the install that captured this
  bool was_visible;       // false: the slot held no visible row before it
  std::string old_row;    // the row it held; empty when !was_visible
};

/**
 * @brief Tests whether writer_epoch is after snapshot epoch se.
 *
 * @details Plain unsigned comparison on purpose: acquisition refuses near
 * the epoch high-water mark and read views expire well inside that margin,
 * so both operands lie in one wrap-free window. A modular comparison would
 * misread old entries as newer than se once a view outlives half the range.
 */
inline bool EpochAfterSnapshot(uint32_t writer_epoch, uint32_t snapshot_epoch) {
  return writer_epoch > snapshot_epoch;
}

/**
 * @brief Returns the monotonic capture counter of `group`'s undo map.
 *
 * @details 0 when nothing captured into the group this generation. The
 * counter increments after an entry is appended and before the writer's
 * first strip mutation; an unchanged value across an in-place read means
 * no concurrent capture.
 */
uint64_t UndoCount(const PaxGroup *group);

/**
 * @brief Copies every undo entry recorded for `group`, keyed by slot.
 *
 * @details Entries per slot are ordered as captured, and per-slot install
 * order is epoch-non-decreasing. The copy is immune to concurrent capture.
 */
std::unordered_map<uint32_t, std::vector<UndoEntry>> UndoGroupEntries(
    const PaxGroup *group);

/**
 * @brief Copies the undo entries recorded for one (group, slot).
 *
 * @details Ordered as captured and epoch-non-decreasing, and immune to a
 * concurrent capture, as UndoGroupEntries is.
 */
std::vector<UndoEntry> UndoSlotEntries(const PaxGroup *group, uint32_t slot);

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_STORAGE_PAX_H
