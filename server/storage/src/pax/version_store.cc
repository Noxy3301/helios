/**
 * @file server/storage/src/pax/version_store.cc
 * Capture of a before-image, lookup of the images a reader resolves against,
 * and the clear the last read view release performs.
 */

#include "pax/version_store.h"

namespace helios::storage {
namespace pax {

VersionStore &VersionStore::Global() {
  static VersionStore instance;
  return instance;
}

VersionStore::GroupUndo *VersionStore::GetOrCreateUndo(const PaxGroup *group) {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto &undo = groups_[group];
  if (!undo) undo = std::make_unique<GroupUndo>();
  return undo.get();
}

const VersionStore::GroupUndo *VersionStore::FindUndo(
    const PaxGroup *group) const {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  auto it = groups_.find(group);
  return it == groups_.end() ? nullptr : it->second.get();
}

void VersionStore::Capture(PaxGroup *group, uint32_t slot,
                           uint32_t writer_epoch, bool was_visible,
                           std::string old_row) {
  // Shared lock: keeps the zero-transition clear (exclusive) from
  // destroying entry vectors under an in-flight capture. A capture racing
  // the last EndCapture may still append; the entry is cleared by that
  // same exclusive pass or ignored by epoch filtering.
  std::shared_lock<std::shared_mutex> lk(registry_mutex_);
  if (active_captures_.load(std::memory_order_seq_cst) == 0) return;

  GroupUndo *undo = GetOrCreateUndo(group);
  {
    std::lock_guard<std::mutex> glk(undo->mutex);
    undo->entries[slot].push_back(
        Entry{writer_epoch, was_visible, std::move(old_row)});
  }
  // Capture after the append and before the caller mutates any strip cell.
  // Readers that sample capture_count, read the strip cells, then sample
  // again finished their in-place reads before this writer's first cell
  // write.
  undo->capture_count.fetch_add(1, std::memory_order_release);
}

VersionStore::ReadViewToken VersionStore::BeginCapture() {
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  ReadViewToken token;
  token.id = next_view_id_++;
  token.valid = true;
  // seq_cst is load-bearing for the read view fence; do not weaken.
  active_captures_.fetch_add(1, std::memory_order_seq_cst);
  return token;
}

void VersionStore::EndCapture(const ReadViewToken &token) {
  if (!token.valid) return;
  std::unique_lock<std::shared_mutex> lk(registry_mutex_);
  const auto remaining =
      active_captures_.fetch_sub(1, std::memory_order_seq_cst) - 1;
  if (remaining == 0) ClearAllLocked();
}

uint64_t VersionStore::GroupCaptureCount(const PaxGroup *group) const {
  const GroupUndo *undo = FindUndo(group);
  return undo == nullptr ? 0
                         : undo->capture_count.load(std::memory_order_acquire);
}

std::vector<VersionStore::Entry> VersionStore::SlotEntries(
    const PaxGroup *group, uint32_t slot) const {
  const GroupUndo *undo = FindUndo(group);
  if (undo == nullptr) return {};
  std::lock_guard<std::mutex> glk(undo->mutex);
  auto it = undo->entries.find(slot);
  if (it == undo->entries.end()) return {};
  return it->second;
}

std::unordered_map<uint32_t, std::vector<VersionStore::Entry>>
VersionStore::GroupEntries(const PaxGroup *group) const {
  const GroupUndo *undo = FindUndo(group);
  if (undo == nullptr) return {};
  std::lock_guard<std::mutex> glk(undo->mutex);
  return undo->entries;
}

void VersionStore::ClearAllLocked() {
  std::lock_guard<std::mutex> lk(groups_mutex_);
  for (auto &[group, undo] : groups_) {
    std::lock_guard<std::mutex> glk(undo->mutex);
    undo->entries.clear();
    // The reset cannot confuse two read views: counter comparisons happen
    // between samples under one active read view and this clear runs only
    // while none is active. Without it a once-captured group would lose
    // in-place strip reads for the rest of the process lifetime.
    undo->capture_count.store(0, std::memory_order_release);
  }
}

uint64_t UndoCount(const PaxGroup *group) {
  return VersionStore::Global().GroupCaptureCount(group);
}

std::unordered_map<uint32_t, std::vector<UndoEntry>> UndoGroupEntries(
    const PaxGroup *group) {
  return VersionStore::Global().GroupEntries(group);
}

std::vector<UndoEntry> UndoSlotEntries(const PaxGroup *group, uint32_t slot) {
  return VersionStore::Global().SlotEntries(group, slot);
}

}  // namespace pax
}  // namespace helios::storage
