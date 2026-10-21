/**
 * @file server/storage/src/index/index_record.cc
 * PAX slot allocation, value access and row updates, and the primary key of
 * a UNIQUE record.
 */

#include "index/index_record.h"

#include <cstdlib>
#include <cstring>
#include <new>

#include "index/masstree_index.h"
#include "pax/epoch_image_buffer.h"
#include "pax/table.h"

namespace helios::storage {

UniqueRecord::~UniqueRecord() { std::free(primary_key_.load()); }

std::string_view UniqueRecord::primary_key() const {
  const char *block = primary_key_.load();
  if (block == nullptr) return {};
  uint32_t len;
  std::memcpy(&len, block, sizeof(len));
  return {block + sizeof(len), len};
}

void UniqueRecord::set_primary_key(std::optional<std::string_view> key) {
  const std::string_view current = primary_key();
  char *old = primary_key_.load();
  if (key && old != nullptr && *key == current) return;

  char *block = nullptr;
  if (key) {
    const auto len = static_cast<uint32_t>(key->size());
    block = static_cast<char *>(std::malloc(sizeof(len) + len));
    if (block == nullptr) throw std::bad_alloc();
    std::memcpy(block, &len, sizeof(len));
    std::memcpy(block + sizeof(len), key->data(), len);
  }
  primary_key_.store(block);
  // A reader that loaded the old block may still be copying from it.
  if (old != nullptr) index::free_rcu(old, sizeof(uint32_t) + current.size());
}

std::string RowRecord::CopyValue() const {
  std::string out(size_, '\0');
  if (!out.empty()) {
    GatherInto(reinterpret_cast<std::byte *>(out.data()), out.size());
  }
  return out;
}

bool RowRecord::AllocateSlot(pax::PaxTable &table) {
  if (pax_allocated()) return true;
  const auto [group, slot] = table.AllocateSlot();
  if (group == nullptr) return false;
  group_ = group;
  slot_ = slot;
  return true;
}

void RowRecord::PreserveImage(EpochNumber epoch) {
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

uint64_t RowRecord::InstallRow(const pax::Row &row, EpochNumber epoch) {
  assert(pax_allocated() && row.size != 0);
  PreserveImage(epoch);
  const uint64_t changed = pax_group()->ScatterRow(slot_, row, size_ != 0);
  size_ = static_cast<uint32_t>(row.size);
  return changed;
}

void RowRecord::DeleteRow(EpochNumber epoch) {
  if (pax_allocated() && size_ != 0) {
    PreserveImage(epoch);
    pax_group()->RetireSlot(slot_);
  }
  size_ = 0;
}

}  // namespace helios::storage
