/**
 * @file server/storage/src/pax/epoch_image_buffer.cc
 * How a writer preserves an epoch image, how a reader looks the images up,
 * and the clear the last read view close performs.
 */

#include "pax/epoch_image_buffer.h"

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

void EpochImageBuffer::Preserve(PaxGroup *group, uint32_t slot,
                                uint32_t writer_epoch, bool was_visible,
                                std::string old_row) {
  // Shared lock: keeps the zero-transition clear (exclusive) from destroying
  // image vectors under an in-flight Preserve call. A preserve racing the last
  // Close can append; the image is cleared by that same exclusive pass or
  // ignored by epoch filtering.
  std::shared_lock<std::shared_mutex> lk(registry_mutex_);
  if (open_views_.load(std::memory_order_seq_cst) == 0) return;

  GroupState *state = GetOrCreateGroup(group);
  {
    std::lock_guard<std::mutex> glk(state->mutex);
    state->images[slot].push_back(
        EpochImage{writer_epoch, was_visible, std::move(old_row)});
  }
  // Count after the append and before the caller mutates any strip cell.
  // Readers that sample preserve_count, read the strip cells, then sample
  // again finished their in-place reads before this writer's first cell
  // write.
  state->preserve_count.fetch_add(1, std::memory_order_release);
}

EpochImageBuffer::ReadViewToken EpochImageBuffer::Open() {
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  ReadViewToken token;
  token.id = next_view_id_++;
  token.valid = true;
  // seq_cst is load-bearing for the read view fence; do not weaken.
  open_views_.fetch_add(1, std::memory_order_seq_cst);
  return token;
}

void EpochImageBuffer::Close(const ReadViewToken &token) {
  if (!token.valid) return;
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  const auto remaining =
      open_views_.fetch_sub(1, std::memory_order_seq_cst) - 1;
  if (remaining == 0) ClearAllLocked();
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
    // The reset cannot confuse two read views: counter comparisons happen
    // between samples under one open read view and this clear runs only
    // while none is open. Without it a group that once held images would
    // lose in-place strip reads for the rest of the process lifetime.
    state->preserve_count.store(0, std::memory_order_release);
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
