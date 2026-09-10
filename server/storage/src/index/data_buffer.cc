/**
 * @file server/storage/src/index/data_buffer.cc
 * The PAX side of DataBuffer: gather on copy, scatter on install, the
 * before-image capture, and overflow to the heap when a row does not fit
 * its cell.
 */

#include "index/data_buffer.h"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "pax/table.h"
#include "pax/version_store.h"
#include "util/spdlog.h"

namespace helios::storage {

void DataBuffer::Reset(const DataBuffer &rhs) {
  if (!rhs.is_pax()) {
    Reset(rhs.value, rhs.size);
    return;
  }
  if (rhs.size == 0) {
    Reset(nullptr, 0);
    return;
  }
  if (is_pax()) {
    // PAX-to-PAX copy gathers first, then scatters into this row's slot.
    thread_local std::vector<std::byte> tmp;
    tmp.resize(rhs.size);
    rhs.GatherInto(tmp.data());
    Reset(tmp.data(), rhs.size);
    return;
  }
  // Gather straight into the heap array for a transaction-local copy.
  EnsureHeapCapacity(rhs.size);
  size = rhs.GatherInto(value);
}

std::string DataBuffer::toString() const {
  if (size == 0) return {};  // no payload, and `value` may be null
  if (!is_pax()) return std::string(reinterpret_cast<char *>(value), size);
  std::string out;
  out.resize(size);
  GatherInto(reinterpret_cast<std::byte *>(out.data()));
  return out;
}

void DataBuffer::CaptureBeforeImage() {
  auto &version_store = pax::VersionStore::Global();
  if (!version_store.CaptureActive()) return;
  const uint32_t epoch = pax::CurrentCommitEpoch::Get();
  if (epoch == 0) {
    // Fail closed: skipping silently would let cells change with no
    // entry and no count advance, and the reader's end recheck would
    // pass on a torn result. The writer proceeds.
    version_store.FailCapture(
        "PAX install without a commit epoch while a read view is active");
    return;
  }
  const bool visible = size != 0;
  version_store.Capture(pax_group(), pax_slot(), epoch, visible,
                        visible ? toString() : std::string());
}

void DataBuffer::ResetPax(const std::byte *row, const size_t len) {
  if (row == nullptr || len == 0) {
    if (pax_allocated() && size != 0) {
      CaptureBeforeImage();
      pax_group()->RetireSlot(pax_slot());
    }
    size = 0;  // tombstone; keep the slot for the (possible) re-insert
    return;
  }
  if (!pax_allocated()) {
    auto *store = pax_table();
    auto [group, slot] = store->AllocateSlot();
    if (group == nullptr) {
      OverflowToHeap(store, "no free slot", row, len);
      return;
    }
    assert((reinterpret_cast<uintptr_t>(group) & kPaxMask) == 0);
    value = reinterpret_cast<std::byte *>(reinterpret_cast<uintptr_t>(group) |
                                          kPaxTag | kPaxAllocated);
    capacity_or_slot = slot;
  }
  CaptureBeforeImage();
  if (pax_group()->ScatterRow(pax_slot(), row, len)) {
    size = len;
    return;
  }

  // Hide the abandoned slot: strip-direct scans must not read a row this
  // buffer now keeps on the heap.
  pax_group()->RetireSlot(pax_slot());
  OverflowToHeap(pax_group()->table(), "row wider than its cell", row, len);
}

void DataBuffer::OverflowToHeap(pax::PaxTable *store, const char *why,
                                const std::byte *row, const size_t len) {
  if (store->overflow_count() == 0) {
    SPDLOG_WARN(
        "PAX overflow for table '{}': {} (row size {}); the row stays on the "
        "heap",
        store->schema().table_name, why, len);
  } else {
    SPDLOG_DEBUG("PAX overflow for table '{}': {} (row size {})",
                 store->schema().table_name, why, len);
  }
  store->RecordOverflow();
  value = nullptr;
  capacity_or_slot = 0;
  Reset(row, len);
}

}  // namespace helios::storage
