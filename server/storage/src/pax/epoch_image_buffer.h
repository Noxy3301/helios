/**
 * @file server/storage/src/pax/epoch_image_buffer.h
 * The epoch images a writer preserves for the slots a read view is
 * looking at.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_EPOCH_IMAGE_BUFFER_H
#define HELIOS_STORAGE_SRC_PAX_EPOCH_IMAGE_BUFFER_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "helios/pax.h"

#include "util/epoch.h"

namespace helios::storage {

namespace epoch {
class Framework;
}

namespace pax {

/**
 * @brief Per-group epoch image state: the images the group's slots hold and
 * the preserve counter a reader compares its in-place reads against.
 *
 * @details Created by the group's first preserve and published into
 * PaxGroup::image_state, so every later lookup is one acquire load. One
 * state per group address, allocated once and never freed (see Forget).
 */
struct GroupImageState {
  mutable std::mutex mutex;
  // Preserve counter, incremented after the image is appended or skipped
  // and before the writer's first strip mutation. Readers compare it with
  // the value they sampled; only Forget resets it.
  std::atomic<uint64_t> preserve_count{0};
  std::unordered_map<uint32_t, std::vector<EpochImage>> images;
};

/**
 * @brief The epoch images that keep a read view consistent.
 *
 * @details An epoch image is the row a slot held before an install made
 * under a later epoch. Each open read view registers its snapshot epoch
 * `se`. A writer at commit epoch `w` replacing a version from epoch `r`
 * preserves the row only when a view with `r <= se < w` is open and no image
 * of that slot already serves that view, before its first strip-cell or
 * visibility-bit mutation, and drops that slot's images with
 * `writer_epoch <= min se` first. A reader at `se` uses the oldest
 * image whose writer epoch exceeds `se`, and the strip in place when none
 * exists. The last close clears every image. With no view open the writer
 * pays one atomic load per installed row.
 */
class EpochImageBuffer {
 public:
  /**
   * @brief Returns the process-wide buffer instance.
   */
  static EpochImageBuffer &Global();

  // Writer side ------------------------------------------------------

  /**
   * @brief Returns whether at least one read view is open.
   */
  bool HasOpenView() const {
    return open_count_.load(std::memory_order_seq_cst) > 0;
  }

  /**
   * @brief Returns whether an open view reads the row this install replaces.
   *
   * @details True when a view is open at some `se` with
   * `old_epoch <= se < writer_epoch`. A caller that gets false skips copying
   * the row.
   *
   * @param old_epoch The epoch of the row the slot holds, 0 when it holds none.
   * @param writer_epoch The commit epoch of the install being made.
   */
  bool NeedsImage(EpochNumber old_epoch, EpochNumber writer_epoch) const;

  /**
   * @brief Preserves the row a slot held before this install when an open
   * read view reads it and no image the slot holds serves that view, and
   * drops the slot's leading images no open view reads.
   *
   * @details Must run before the install's first strip-cell or
   * visibility-bit mutation. A slot that holds no row passes
   * was_visible=false and an empty row.
   *
   * @param old_epoch The epoch of the row the slot holds, 0 when it holds none.
   * @param writer_epoch The commit epoch of the install being made.
   */
  void Preserve(PaxGroup *group, uint32_t slot, EpochNumber old_epoch,
                EpochNumber writer_epoch, bool was_visible,
                std::string old_row);

  /**
   * @brief Clears the state of a group being destroyed (see ForgetGroup).
   *
   * @details The group's images and its preserve counter reset, and the
   * GroupImageState object stays allocated for the life of the process: a
   * running scan reaches it by raw pointer, which no scan-shutdown barrier
   * bounds, so no state is ever freed. Groups die only with their table,
   * when no view is open and no commit is in flight.
   */
  void Forget(const PaxGroup *group);

  // Reader side ------------------------------------------------------

  /**
   * @brief Registers one read view and returns its snapshot epoch.
   *
   * @details Samples the global epoch under the registry lock, so a writer
   * that sees the registration also sees the epoch. The caller performs the
   * read view fence itself; Close may run on another thread.
   */
  EpochNumber Open(const epoch::Framework &epoch);

  /**
   * @brief Closes one read view; call exactly once per Open, with the epoch
   * that Open returned.
   *
   * @details The close of the last open read view clears every group's
   * images. A close with an epoch no open view holds is a no-op.
   */
  void Close(EpochNumber se);

  /**
   * @brief Returns the images and bytes held and the open view count.
   */
  ImageBufferStats Stats() const;

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

 private:
  // seq_cst on both sides is load-bearing for the read view fence proof; do
  // not weaken.
  std::atomic<uint64_t> open_count_{0};
  // What the buffer holds, for attributing the process's memory. Moved
  // wherever an image is appended, trimmed or dropped.
  std::atomic<uint64_t> images_held_{0};
  std::atomic<uint64_t> bytes_held_{0};

  // shared: NeedsImage, Preserve; exclusive: Open, Close and the clear the
  // last close performs. Const readers take neither.
  mutable std::shared_mutex registry_mutex_;
  // Snapshot epochs of the open views, guarded by registry_mutex_.
  std::multiset<EpochNumber> open_views_;
  // Creation and the last close's enumeration only; every lookup goes
  // through PaxGroup::image_state. Lock order: registry_mutex_, then this,
  // then GroupImageState::mutex.
  mutable std::mutex groups_mutex_;
  std::unordered_map<const PaxGroup *, std::unique_ptr<GroupImageState>>
      groups_;

  EpochImageBuffer() = default;
  GroupImageState *GetOrCreateGroup(const PaxGroup *group);
  void ClearAllLocked();
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_EPOCH_IMAGE_BUFFER_H
