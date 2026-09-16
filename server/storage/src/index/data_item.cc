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
  auto &buffer = pax::EpochImageBuffer::Global();
  if (!buffer.HasOpenView()) return;
  assert(epoch != 0);
  const bool visible = size_ != 0;
  buffer.Preserve(pax_group(), pax_slot(), epoch, visible,
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
