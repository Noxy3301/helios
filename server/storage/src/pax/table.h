/**
 * @file server/storage/src/pax/table.h
 * The append-only group directory behind one table's PAX strips.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_TABLE_H
#define HELIOS_STORAGE_SRC_PAX_TABLE_H

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "helios/pax.h"

namespace helios::storage {
namespace pax {

/**
 * @brief One row's values in PAX cell representation.
 * @details Typed fields hold binary values; other fields borrow the input
 * bytes. The write set owns this row until commit finishes. Input bytes and
 * the schema used by unpack_row must remain unchanged until ScatterRow ends.
 */
struct Row {
  struct Field {
    const std::byte *payload = nullptr;
    uint32_t len = 0;
    uint64_t typed_value = 0;
  };
  std::vector<Field> fields;
  size_t size = 0;
};

/**
 * @brief Unpacks input row bytes into the cell values used by the write set.
 * @return False if the input cannot be represented by the schema; discard out.
 */
bool unpack_row(const TableSchema &schema, const std::byte *value, size_t size,
               Row &out);

/**
 * @brief Owns all PAX row groups for one Helios table.
 *
 * @details `PaxTable` assigns append-only `(group, slot)` locations;
 * transaction visibility and index publication stay in the row path.
 */
class PaxTable {
 public:
  // `262,144 groups x 8,192 rows = 2^31 slots` per table.
  static constexpr size_t kMaxGroups = 1u << 18;

  /**
   * @brief Takes ownership of the table schema used by subsequently allocated
   * groups.
   *
   * @param schema Schema copied into the store and referenced by its groups.
   */
  explicit PaxTable(TableSchema schema);
  ~PaxTable();

  /**
   * @brief Allocates the next append-only PAX slot.
   *
   * @details Slots are append-only and are not reused.
   *
   * @return `{nullptr, 0}` when the table has exhausted its slot directory.
   */
  std::pair<PaxGroup *, uint32_t> AllocateSlot();

  /**
   * @brief Returns the schema used to size every group in this store.
   */
  const TableSchema &schema() const { return schema_; }

  /**
   * @brief Returns group `idx`, or nullptr if it has not been allocated yet.
   *
   * @param idx Group index in the append-only directory. The group is owned
   * by this store and lives as long as it does.
   */
  PaxGroup *group(size_t idx) const {
    return dir_[idx].load(std::memory_order_acquire);
  }

  /**
   * @brief Returns reserved slots, including absent rows and aborted writes.
   */
  uint64_t slots_allocated() const {
    return next_slot_.load(std::memory_order_acquire);
  }

  /**
   * @brief Returns the number of row groups that may contain allocated slots.
   */
  size_t group_count() const {
    const uint64_t slots = slots_allocated();
    const uint64_t groups = (slots + PaxGroup::kRows - 1) / PaxGroup::kRows;
    // AllocateSlot counts past a full directory; the bound wins.
    return static_cast<size_t>(std::min<uint64_t>(groups, kMaxGroups));
  }

 private:
  TableSchema schema_;
  // Fixed at kMaxGroups entries; the groups it points at are owned here and
  // are freed only when the store is.
  std::unique_ptr<std::atomic<PaxGroup *>[]> dir_;
  std::atomic<uint64_t> next_slot_{0};
  // Serializes the first touch of a directory entry, not the directory
  // itself, which never grows.
  std::mutex alloc_mutex_;
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_TABLE_H
