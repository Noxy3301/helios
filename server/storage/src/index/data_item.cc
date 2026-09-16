/**
 * @file server/storage/src/index/data_item.cc
 * PAX slot allocation, value access and row updates.
 */

#include "index/data_item.h"

#include "pax/epoch_image_buffer.h"
#include "pax/table.h"

namespace helios::storage {

std::string DataItem::CopyValue() const {
  std::string out(size_, '\0');
  if (!out.empty()) {
    GatherInto(reinterpret_cast<std::byte *>(out.data()), out.size());
  }
  return out;
}

bool DataItem::AllocateSlot(pax::PaxTable &table) {
  if (pax_allocated()) return true;
  const auto [group, slot] = table.AllocateSlot();
  if (group == nullptr) return false;
  group_ = group;
  slot_ = slot;
  return true;
}

void DataItem::PreserveImage(EpochNumber epoch) {
  assert(epoch != 0);
  auto &image_buffer = pax::EpochImageBuffer::Global();
  if (!image_buffer.HasOpenView()) return;
  const bool visible = size_ != 0;
  // The old row's epoch is its word's epoch. A slot without a row counts
  // from epoch 0: a delete moves the word without touching the slot.
  const EpochNumber old_epoch = visible ? transaction_id.load().epoch : 0;
  if (!image_buffer.NeedsImage(old_epoch, epoch)) return;
  image_buffer.Preserve(pax_group(), pax_slot(), old_epoch, epoch, visible,
                        visible ? CopyValue() : std::string());
}

void DataItem::InstallRow(const pax::Row &row, EpochNumber epoch) {
  assert(pax_allocated() && row.size != 0);
  PreserveImage(epoch);
  pax_group()->ScatterRow(slot_, row);
  size_ = row.size;
}

void DataItem::DeleteRow(EpochNumber epoch) {
  if (pax_allocated() && size_ != 0) {
    PreserveImage(epoch);
    pax_group()->RetireSlot(slot_);
  }
  size_ = 0;
}

}  // namespace helios::storage
