/**
 * @file server/storage/src/pax/epoch_image_buffer.h
 * The epoch images a writer preserves for the slots a columnar read view is
 * looking at.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_EPOCH_IMAGE_BUFFER_H
#define HELIOS_STORAGE_SRC_PAX_EPOCH_IMAGE_BUFFER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "lineairdb/pax.h"

namespace helios::storage {
namespace pax {

/**
 * @brief The epoch images that keep a columnar read view consistent.
 *
 * @details An epoch image is the row a slot held before an install, tagged
 * with that install's commit epoch. A read view at snapshot epoch `se` reads
 * the oldest image whose writer epoch exceeds `se`, and the strip in place
 * when none exists. While at least one read view is open (`open_views_ > 0`),
 * every PAX install preserves the replaced row image before its first
 * strip-cell or visibility-bit mutation; a first install preserves an empty
 * was_visible=false image. With no read view open the writer side pays one
 * atomic load per installed row.
 */
class EpochImageBuffer {
 public:
  /**
   * @brief Per-group state: the preserved images and their preserve counter.
   */
  struct GroupState {
    mutable std::mutex mutex;
    // Monotonic preserve counter, incremented after the image is appended
    // and before the writer's first strip mutation; readers sample it to
    // detect concurrent writers.
    std::atomic<uint64_t> preserve_count{0};
    std::unordered_map<uint32_t, std::vector<EpochImage>> images;
  };

  /**
   * @brief Returns the process-wide buffer instance.
   */
  static EpochImageBuffer &Global();

  // Writer side ------------------------------------------------------

  /**
   * @brief Returns whether at least one read view is open.
   */
  bool HasOpenView() const {
    return open_views_.load(std::memory_order_seq_cst) > 0;
  }

  /**
   * @brief Preserves the epoch image of (group, slot).
   *
   * @details Must run before the install's first strip-cell or
   * visibility-bit mutation. A first install into a fresh slot passes
   * was_visible=false and an empty row. With no read view open this
   * returns without preserving.
   *
   * @param writer_epoch The commit epoch of the install being made.
   */
  void Preserve(PaxGroup *group, uint32_t slot, uint32_t writer_epoch,
                bool was_visible, std::string old_row);

  // Reader side ------------------------------------------------------

  /**
   * @brief Identifies one open read view's registration.
   */
  struct ReadViewToken {
    uint64_t id = 0;
    bool valid = false;
  };

  /**
   * @brief Opens a read view; the caller performs the epoch fence itself.
   *
   * @return The registration to pass to Close.
   */
  ReadViewToken Open();

  /**
   * @brief Closes one read view; call exactly once per valid token.
   *
   * @details The close of the last open read view clears every group's
   * images. Token ids are diagnostic; a double or stale close is not
   * detected.
   */
  void Close(const ReadViewToken &token);

  /**
   * @brief Returns the group's preserve counter, 0 when the buffer holds no
   * state for the group.
   */
  uint64_t GroupPreserveCount(const PaxGroup *group) const;

  /**
   * @brief Copies the images for (group, slot); empty if none.
   *
   * @details Copies keep callers immune to concurrent vector reallocation.
   */
  std::vector<EpochImage> SlotImages(const PaxGroup *group,
                                     uint32_t slot) const;

  /**
   * @brief Copies the group's whole slot->images map in one locking pass.
   */
  std::unordered_map<uint32_t, std::vector<EpochImage>> GroupImages(
      const PaxGroup *group) const;

 private:
  // seq_cst on both sides is load-bearing for the fence proof; do not
  // weaken.
  std::atomic<uint64_t> open_views_{0};

  // shared: Preserve; exclusive: Open, Close and the clear the last close
  // performs. Const readers take neither.
  mutable std::shared_mutex registry_mutex_;
  // Lock order: registry_mutex_, then this, then GroupState::mutex.
  mutable std::mutex groups_mutex_;
  std::unordered_map<const PaxGroup *, std::unique_ptr<GroupState>> groups_;

  uint64_t next_view_id_ = 1;

  EpochImageBuffer() = default;
  // The group pointer is a key here and is never dereferenced.
  GroupState *GetOrCreateGroup(const PaxGroup *group);
  const GroupState *FindGroup(const PaxGroup *group) const;
  void ClearAllLocked();
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_EPOCH_IMAGE_BUFFER_H
