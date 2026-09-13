/**
 * @file server/storage/src/index/data_item.cc
 * PAX slot allocation, value access and row updates.
 */

#include "index/data_item.h"

#include "pax/table.h"
#include "pax/version_store.h"

namespace helios::storage {

std::string DataItem::CopyValue() const {
  std::string out(size_, '\0');
  if (!out.empty()) {
    GatherInto(reinterpret_cast<std::byte *>(out.data()), out.size());
  }
  return out;
}

bool DataItem::AllocateSlot() {
  if (pax_allocated()) return true;
  if (location_ == 0) return false;
  auto *table = reinterpret_cast<pax::PaxTable *>(location_);
  const auto [group, slot] = table->AllocateSlot();
  if (group == nullptr) return false;
  location_ = reinterpret_cast<uintptr_t>(group) | kAllocated;
  slot_ = slot;
  return true;
}

void DataItem::CaptureBeforeImage() {
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
  const bool visible = size_ != 0;
  version_store.Capture(pax_group(), pax_slot(), epoch, visible,
                        visible ? CopyValue() : std::string());
}

void DataItem::Write(const pax::Row &row) {
  assert(pax_allocated() && row.size != 0);
  CaptureBeforeImage();
  pax_group()->ScatterRow(slot_, row);
  size_ = row.size;
}

void DataItem::Delete() {
  if (pax_allocated() && size_ != 0) {
    CaptureBeforeImage();
    pax_group()->RetireSlot(slot_);
  }
  size_ = 0;
}

}  // namespace helios::storage
