/**
 * @file server/storage/src/pax/table.h
 * The group directory, size classes and retired slots behind one table's
 * PAX strips.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_TABLE_H
#define HELIOS_STORAGE_SRC_PAX_TABLE_H

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "helios/pax.h"
#include "pax/hyperloglog.h"
#include "pax/value_range.h"
#include "util/epoch.h"

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
 * @details Groups are numbered in creation order in a directory of at most
 * kMaxGroups entries and live as long as the table.
 *
 * Each group belongs to one size class, which fixes the width of its UNTYPED
 * cells to min(declared width, class width). Classes are spaced as jemalloc
 * spaces its size classes, four per doubling: each step is a quarter of the
 * largest power of two not above the class. The ladder starts at 16, stops
 * at the first class that reaches the widest declared UNTYPED width, and that
 * width is the top class; a 176 B column gets 16, 20, ..., 160, 176. Group 0
 * is a top-class group, and a class gets a group only when a row lands in it.
 * @verbatim
 *   16  20  24  28 | 32  40  48  56 | 64  80  96 112 | 128 160 ... | top
 *   step 4         | step 8         | step 16        | step 32
 * @endverbatim
 *
 * A retired slot is handed out again only to a commit whose epoch `w`
 * satisfies `r + 2 <= w` for the retire epoch `r`, so a slot's epoch images
 * keep non-decreasing writer epochs. Two epochs suffice because every joined
 * commit's epoch `e_w` is within one of the global epoch `E` (`E < e_w + 2`).
 *
 * Transaction visibility and index publication stay in the row path.
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
   * @brief Allocates a slot in the narrowest size class that holds `row`.
   *
   * @details Takes the next fresh slot of the class's newest group, else a
   * reclaimed slot of the class, else creates a group of the class.
   *
   * @param row Unpacked row the slot is for.
   * @param epoch Commit epoch of the allocating write; 0 reclaims nothing.
   * @return `{nullptr, 0}` when the table has exhausted its slot directory.
   */
  std::pair<PaxGroup *, uint32_t> AllocateSlot(const Row &row,
                                               EpochNumber epoch);

  /**
   * @brief Hides a slot from strip scans and queues it for reuse.
   *
   * @details The slot must no longer be attached to any row record.
   *
   * @param epoch Commit epoch of the write that retired it.
   */
  void Retire(PaxGroup *group, uint32_t slot, EpochNumber epoch);

  void count_move() { moves_.fetch_add(1, std::memory_order_relaxed); }

  // Rows moved to a wider size class.
  uint64_t moves() const { return moves_.load(std::memory_order_relaxed); }

  /**
   * @brief Returns the schema used to size every group in this store.
   */
  const TableSchema &schema() const { return schema_; }

  /**
   * @brief Returns group `idx`, or nullptr if it has not been allocated yet.
   *
   * @param idx Group index in the directory. The group is owned by this
   * store and lives as long as it does.
   */
  PaxGroup *group(size_t idx) const {
    return dir_[idx].load(std::memory_order_acquire);
  }

  /**
   * @brief Returns fresh slots handed out, an upper bound on populated rows.
   */
  uint64_t slots_allocated() const;

  /**
   * @brief Records one typed value written to field `field`.
   *
   * @details Widens the field's range and feeds its distinct-value sketch.
   * The range covers every value written, and a delete or an overwrite leaves
   * it wider than the live rows need. The sketch keeps historical values too,
   * and its estimate errs in either direction.
   */
  void observe(size_t field, int64_t value);

  /**
   * @brief Returns the value range written to field `field`, or false when it
   * has none.
   */
  bool range(size_t field, int64_t *lo, int64_t *hi) const;

  /**
   * @brief Returns the distinct-value estimate for field `field`, or false
   * when it has none.
   */
  bool distinct(size_t field, uint64_t *ndv) const;

  size_t group_count() const {
    return group_count_.load(std::memory_order_acquire);
  }

 private:
  // One size class: its newest group and the slots it can hand out again.
  struct SizeClass {
    std::mutex mutex;
    // (directory index of the newest group << 32) | its next fresh slot. A
    // slot at or past kRows sends the allocator to the locked path.
    std::atomic<uint64_t> slot{PaxGroup::kRows};
    // (retire epoch, slot) in retire order.
    std::deque<std::pair<EpochNumber, std::pair<PaxGroup *, uint32_t>>> retired;
    std::atomic<uint32_t> groups{0};
  };

  TableSchema schema_;
  std::vector<uint32_t> class_bytes_;
  std::unique_ptr<SizeClass[]> classes_;
  // Fixed at kMaxGroups entries; the groups it points at are owned here and
  // are freed only when the store is.
  std::unique_ptr<std::atomic<PaxGroup *>[]> dir_;
  std::atomic<size_t> group_count_{0};
  std::atomic<uint64_t> moves_{0};
  // Serializes appends to the directory; taken after a class mutex.
  std::mutex dir_mutex_;
  // One value range a field. An untyped field keeps the empty range.
  std::unique_ptr<ValueRange[]> range_;
  // One distinct-value sketch a field.
  std::unique_ptr<HyperLogLog[]> sketch_;

  std::pair<PaxGroup *, uint32_t> allocate(uint32_t cls, EpochNumber epoch);
  PaxGroup *add_group(uint32_t cls);
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_TABLE_H
