/**
 * @file server/storage/src/pax/epoch_image_buffer.cc
 * How a writer preserves an epoch image, how a reader looks the images up,
 * and the clear the last read view close performs.
 */

#include "pax/epoch_image_buffer.h"

#include <algorithm>
#include <cassert>

#include "util/epoch_framework.h"

namespace helios::storage {
namespace pax {

EpochImageBuffer &EpochImageBuffer::Global() {
  static EpochImageBuffer instance;
  return instance;
}

EpochImageBuffer::GroupState *EpochImageBuffer::GetOrCreateGroup(
    const PaxGroup *group) {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto &state = groups_[group];
  if (!state) state = std::make_unique<GroupState>();
  return state.get();
}

const EpochImageBuffer::GroupState *EpochImageBuffer::FindGroup(
    const PaxGroup *group) const {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto it = groups_.find(group);
  return it == groups_.end() ? nullptr : it->second.get();
}

bool EpochImageBuffer::NeedsImage(EpochNumber old_epoch,
                                  EpochNumber writer_epoch) const {
  if (!HasOpenView()) return false;
  std::shared_lock<std::shared_mutex> lk(registry_mutex_);
  auto it = open_views_.lower_bound(old_epoch);
  return it != open_views_.end() && EpochAfterSnapshot(writer_epoch, *it);
}

void EpochImageBuffer::Preserve(PaxGroup *group, uint32_t slot,
                                EpochNumber old_epoch, EpochNumber writer_epoch,
                                bool was_visible, std::string old_row) {
  // Shared lock: keeps the zero-transition clear (exclusive) from destroying
  // image vectors under an in-flight Preserve call.
  std::shared_lock<std::shared_mutex> lk(registry_mutex_);
  // A view NeedsImage saw may have closed in between.
  auto it = open_views_.lower_bound(old_epoch);
  if (it == open_views_.end() || !EpochAfterSnapshot(writer_epoch, *it)) {
    return;
  }
  const EpochNumber min_se = *open_views_.begin();

  GroupState *state = GetOrCreateGroup(group);
  {
    std::lock_guard<std::mutex> glk(state->mutex);
    auto &images = state->images[slot];
    // Writer epochs are non-decreasing, so the images no open view reads are
    // a prefix.
    const auto kept = std::find_if(
        images.begin(), images.end(), [min_se](const EpochImage &image) {
          return EpochAfterSnapshot(image.writer_epoch, min_se);
        });
    images.erase(images.begin(), kept);
    // A view an existing image already serves does not need this one.
    const EpochNumber uncovered =
        images.empty() ? old_epoch
                       : std::max(old_epoch, images.back().writer_epoch);
    auto view = open_views_.lower_bound(uncovered);
    if (view != open_views_.end() && EpochAfterSnapshot(writer_epoch, *view)) {
      images.push_back(
          EpochImage{writer_epoch, was_visible, std::move(old_row)});
    } else if (images.empty()) {
      state->images.erase(slot);
    }
  }
  // Count after the append, or the skip, and before the caller mutates any
  // strip cell.
  // Readers that sample preserve_count, read the strip cells, then sample
  // again finished their in-place reads before this writer's first cell
  // write.
  state->preserve_count.fetch_add(1, std::memory_order_release);
}

EpochNumber EpochImageBuffer::Open(const epoch::Framework &epoch) {
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  // seq_cst is load-bearing for the read view fence; do not weaken. The count
  // store precedes the epoch load.
  open_count_.fetch_add(1, std::memory_order_seq_cst);
  const EpochNumber se = epoch.GetGlobalEpoch();
  open_views_.insert(se);
  return se;
}

void EpochImageBuffer::Close(EpochNumber se) {
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  auto it = open_views_.find(se);
  assert(it != open_views_.end());
  if (it == open_views_.end()) return;
  open_views_.erase(it);
  open_count_.fetch_sub(1, std::memory_order_seq_cst);
  if (open_views_.empty()) ClearAllLocked();
}

uint64_t EpochImageBuffer::GroupPreserveCount(const PaxGroup *group) const {
  const GroupState *state = FindGroup(group);
  return state == nullptr
             ? 0
             : state->preserve_count.load(std::memory_order_acquire);
}

std::vector<EpochImage> EpochImageBuffer::SlotImages(const PaxGroup *group,
                                                     uint32_t slot) const {
  const GroupState *state = FindGroup(group);
  if (state == nullptr) return {};
  std::lock_guard<std::mutex> glk(state->mutex);
  auto it = state->images.find(slot);
  if (it == state->images.end()) return {};
  return it->second;
}

std::unordered_map<uint32_t, std::vector<EpochImage>>
EpochImageBuffer::GroupImages(const PaxGroup *group) const {
  const GroupState *state = FindGroup(group);
  if (state == nullptr) return {};
  std::lock_guard<std::mutex> glk(state->mutex);
  return state->images;
}

void EpochImageBuffer::ClearAllLocked() {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  for (auto &[group, state] : groups_) {
    std::lock_guard<std::mutex> glk(state->mutex);
    state->images.clear();
  }
}

uint64_t GroupPreserveCount(const PaxGroup *group) {
  return EpochImageBuffer::Global().GroupPreserveCount(group);
}

std::unordered_map<uint32_t, std::vector<EpochImage>> GroupImages(
    const PaxGroup *group) {
  return EpochImageBuffer::Global().GroupImages(group);
}

std::vector<EpochImage> SlotImages(const PaxGroup *group, uint32_t slot) {
  return EpochImageBuffer::Global().SlotImages(group, slot);
}

}  // namespace pax
}  // namespace helios::storage
