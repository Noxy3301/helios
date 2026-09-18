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
  // Never destroyed: a PaxGroup destructor reaches the buffer, and group
  // lifetimes are not ordered against static destruction.
  static EpochImageBuffer *instance = new EpochImageBuffer();
  return *instance;
}

GroupImageState *EpochImageBuffer::GetOrCreateGroup(const PaxGroup *group) {
  // The state pointer is published once and stays until Forget.
  if (auto *s = group->image_state.load(std::memory_order_acquire)) return s;
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto &state = groups_[group];
  if (!state) state = std::make_unique<GroupImageState>();
  // Publish the built state; readers reach it with an acquire load.
  group->image_state.store(state.get(), std::memory_order_release);
  return state.get();
}

namespace {

// An image costs its row bytes plus the record that holds them.
uint64_t ImageBytes(const EpochImage &image) {
  return sizeof(EpochImage) + image.old_row.size();
}

}  // namespace

void EpochImageBuffer::Forget(const PaxGroup *group) {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  group->image_state.store(nullptr, std::memory_order_release);
  auto it = groups_.find(group);
  if (it == groups_.end()) return;
  std::lock_guard<std::mutex> glk(it->second->mutex);
  // Take the group's images out of the held counters.
  uint64_t count = 0, bytes = 0;
  for (const auto &[slot, slot_images] : it->second->images) {
    count += slot_images.size();
    for (const EpochImage &image : slot_images) bytes += ImageBytes(image);
  }
  images_held_.fetch_sub(count, std::memory_order_relaxed);
  bytes_held_.fetch_sub(bytes, std::memory_order_relaxed);
  // The map entry keeps the state object: a scan holds it by raw pointer for
  // the length of its read. Clearing it leaves a PaxGroup allocated at this
  // address no image of the group that held the address before it.
  it->second->images.clear();
  it->second->preserve_count.store(0, std::memory_order_release);
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

  GroupImageState *state = GetOrCreateGroup(group);
  {
    std::lock_guard<std::mutex> glk(state->mutex);
    auto &images = state->images[slot];
    // Writer epochs are non-decreasing, so the images no open view reads are
    // a prefix.
    const auto kept = std::find_if(
        images.begin(), images.end(), [min_se](const EpochImage &image) {
          return EpochAfterSnapshot(image.writer_epoch, min_se);
        });
    uint64_t trimmed_bytes = 0;
    for (auto it = images.begin(); it != kept; ++it) {
      trimmed_bytes += ImageBytes(*it);
    }
    images_held_.fetch_sub(static_cast<uint64_t>(kept - images.begin()),
                           std::memory_order_relaxed);
    bytes_held_.fetch_sub(trimmed_bytes, std::memory_order_relaxed);
    images.erase(images.begin(), kept);
    // A view an existing image already serves does not need this one.
    const EpochNumber uncovered =
        images.empty() ? old_epoch
                       : std::max(old_epoch, images.back().writer_epoch);
    auto view = open_views_.lower_bound(uncovered);
    if (view != open_views_.end() && EpochAfterSnapshot(writer_epoch, *view)) {
      images.push_back(
          EpochImage{writer_epoch, was_visible, std::move(old_row)});
      images_held_.fetch_add(1, std::memory_order_relaxed);
      bytes_held_.fetch_add(ImageBytes(images.back()),
                            std::memory_order_relaxed);
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
  return pax::PreserveCount(ImageState(group));
}

std::vector<EpochImage> EpochImageBuffer::SlotImages(const PaxGroup *group,
                                                     uint32_t slot) const {
  return pax::SlotImages(ImageState(group), slot);
}

void EpochImageBuffer::ClearAllLocked() {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  for (auto &[group, state] : groups_) {
    std::lock_guard<std::mutex> glk(state->mutex);
    state->images.clear();
  }
  // Every group is cleared under the exclusive registry lock, so no Preserve
  // is in flight to have counted an image this misses.
  images_held_.store(0, std::memory_order_relaxed);
  bytes_held_.store(0, std::memory_order_relaxed);
}

ImageBufferStats EpochImageBuffer::Stats() const {
  std::shared_lock<std::shared_mutex> lk(registry_mutex_);
  return ImageBufferStats{images_held_.load(std::memory_order_relaxed),
                          bytes_held_.load(std::memory_order_relaxed),
                          static_cast<uint64_t>(open_views_.size())};
}

uint64_t PreserveCount(const GroupImageState *state) {
  return state == nullptr
             ? 0
             : state->preserve_count.load(std::memory_order_acquire);
}

std::unordered_map<uint32_t, std::vector<EpochImage>> GroupImages(
    const GroupImageState *state, uint64_t *imaged) {
  constexpr uint32_t kWordBits = PaxGroup::kVisibilityWordBits;
  std::fill_n(imaged, PaxGroup::kRows / kWordBits, uint64_t{0});
  if (state == nullptr) return {};
  std::lock_guard<std::mutex> glk(state->mutex);
  // A slot keeps its entry only while it holds an image, so the map keys are
  // the imaged slots.
  for (const auto &[slot, images] : state->images) {
    if (slot < PaxGroup::kRows) {
      imaged[slot / kWordBits] |= uint64_t{1} << (slot % kWordBits);
    }
  }
  return state->images;
}

std::vector<EpochImage> SlotImages(const GroupImageState *state,
                                   uint32_t slot) {
  if (state == nullptr) return {};
  std::lock_guard<std::mutex> glk(state->mutex);
  auto it = state->images.find(slot);
  if (it == state->images.end()) return {};
  return it->second;
}

uint64_t GroupPreserveCount(const PaxGroup *group) {
  return PreserveCount(ImageState(group));
}

std::unordered_map<uint32_t, std::vector<EpochImage>> GroupImages(
    const PaxGroup *group) {
  uint64_t imaged[PaxGroup::kRows / PaxGroup::kVisibilityWordBits];
  return GroupImages(ImageState(group), imaged);
}

std::vector<EpochImage> SlotImages(const PaxGroup *group, uint32_t slot) {
  return SlotImages(ImageState(group), slot);
}

void ForgetGroup(const PaxGroup *group) {
  EpochImageBuffer::Global().Forget(group);
}

ImageBufferStats ImageStats() { return EpochImageBuffer::Global().Stats(); }

}  // namespace pax
}  // namespace helios::storage
