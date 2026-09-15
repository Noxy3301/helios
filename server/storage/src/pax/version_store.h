/**
 * @file server/storage/src/pax/version_store.h
 * The before-images a writer captures for the slots a columnar read view is
 * still looking at.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_VERSION_STORE_H
#define HELIOS_STORAGE_SRC_PAX_VERSION_STORE_H

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
 * @brief Before-image store that keeps a columnar read view consistent.
 *
 * @details A generation is the interval during which at least one read view
 * holds a registration. While one is active (`active_captures_ > 0`), every
 * PAX install captures the replaced row image before its first strip-cell
 * or visibility-bit mutation; a first install captures an empty
 * was_visible=false entry. An install that cannot capture (byte budget
 * exceeded, or epoch 0) fails the capture for the active generation
 * instead, and every result produced under it is discarded. A reader
 * at snapshot epoch `se` uses the oldest before-image whose writer epoch
 * exceeds `se` and reads the strip in place when no entry
 * qualifies. With no read view active the writer side pays one atomic load
 * per installed row.
 */
class VersionStore {
 public:
  // The stored form is the public reader-side type; readers receive copies.
  using Entry = UndoEntry;

  /**
   * @brief Per-group undo state: the captured entries and their capture
   * counter.
   */
  struct GroupUndo {
    mutable std::mutex mutex;
    // Monotonic capture counter, incremented after the entry is appended
    // and before the writer's first strip mutation; readers sample it to
    // detect concurrent writers.
    std::atomic<uint64_t> capture_count{0};
    std::unordered_map<uint32_t, std::vector<Entry>> entries;
  };

  /**
   * @brief Returns the process-wide store instance.
   */
  static VersionStore &Global();

  // Writer side ------------------------------------------------------

  /**
   * @brief Returns whether at least one read view is active.
   */
  bool CaptureActive() const {
    return active_captures_.load(std::memory_order_seq_cst) > 0;
  }

  /**
   * @brief Captures a before-image for (group, slot).
   *
   * @details Must run before the install's first strip-cell or
   * visibility-bit mutation. A first install into a fresh slot passes
   * was_visible=false and an empty row. With no read view active this
   * returns without capturing, and a capture that would take the store past
   * its byte budget fails the capture instead of appending.
   *
   * @param writer_epoch The commit epoch of the install being made.
   */
  void Capture(PaxGroup *group, uint32_t slot, uint32_t writer_epoch,
               bool was_visible, std::string old_row);

  // Reader side ------------------------------------------------------

  /**
   * @brief Identifies one active read view's capture registration.
   */
  struct ReadViewToken {
    uint64_t id = 0;
    bool valid = false;
  };

  /**
   * @brief Arms capturing; the caller performs the epoch fence itself.
   *
   * @return A token whose valid is false when a failed capture still
   * has registrations.
   */
  ReadViewToken BeginCapture();

  /**
   * @brief Releases one registration; call exactly once per valid token.
   *
   * @details The release of the last active registration clears every
   * group's entries and clears the capture failure. Token ids are diagnostic;
   * a double or stale release is not detected.
   */
  void EndCapture(const ReadViewToken &token);

  /**
   * @brief Returns whether the capture failed for the active generation;
   * every result that generation produced must be discarded.
   */
  bool CaptureFailed() const;

  /**
   * @brief Fails every active read view and rejects new ones until the last
   * active read view releases.
   *
   * @details Callers must fail the capture before mutating any cell the
   * failed capture should have covered. A failure racing the last release
   * may land on the next generation, whose results are then discarded;
   * both orderings fail closed.
   */
  void FailCapture(const char *reason);

  /**
   * @brief Returns the capture_count of the group's undo map, 0 if the
   * group has none.
   */
  uint64_t GroupCaptureCount(const PaxGroup *group) const;

  /**
   * @brief Copies the entries for (group, slot); empty if none.
   *
   * @details Copies keep callers immune to concurrent vector reallocation.
   */
  std::vector<Entry> SlotEntries(const PaxGroup *group, uint32_t slot) const;

  /**
   * @brief Copies the group's whole slot->entries map in one locking pass.
   */
  std::unordered_map<uint32_t, std::vector<Entry>> GroupEntries(
      const PaxGroup *group) const;

 private:
  // seq_cst on both sides is load-bearing for the fence proof; do not
  // weaken.
  std::atomic<uint64_t> active_captures_{0};
  // Byte budget for captured before-images. Exceeding it fails the capture,
  // and with it every active read view, instead of growing writer-side
  // memory without bound.
  static constexpr uint64_t kByteBudget = 256ull << 20;
  std::atomic<uint64_t> captured_bytes_{0};
  // Covers the whole active generation; resets when the last read view
  // releases.
  std::atomic<bool> capture_failed_{false};

  // shared: Capture; exclusive: BeginCapture, EndCapture and the clear the
  // last release performs. Const readers take neither.
  mutable std::shared_mutex registry_mutex_;
  // Lock order: registry_mutex_, then this, then GroupUndo::mutex.
  mutable std::mutex groups_mutex_;
  std::unordered_map<const PaxGroup *, std::unique_ptr<GroupUndo>> groups_;

  uint64_t next_view_id_ = 1;

  VersionStore() = default;
  // The group pointer is a key here and is never dereferenced.
  GroupUndo *GetOrCreateUndo(const PaxGroup *group);
  const GroupUndo *FindUndo(const PaxGroup *group) const;
  void ClearAllLocked();
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_VERSION_STORE_H
