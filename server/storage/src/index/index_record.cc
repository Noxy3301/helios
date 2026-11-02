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

RowRecord::PaxAddress RowRecord::pax_address() const {
  const uint32_t word = address_.load(std::memory_order_acquire);
  if (word == 0) return {nullptr, 0};
  static_assert(pax::PaxGroup::kRows == 1u << kSlotBits);
  static_assert(pax::PaxTable::kMaxGroups == 1u << (31 - kSlotBits));
  return {table_->group((word & ~kAttached) >> kSlotBits),
          word & (pax::PaxGroup::kRows - 1)};
}

uint32_t RowRecord::address_word(const pax::PaxGroup *group, uint32_t slot) {
  return kAttached | (group->index() << kSlotBits) | slot;
}

bool RowRecord::AllocateSlot(pax::PaxTable &table, const pax::Row &row,
                             EpochNumber epoch) {
  if (pax_allocated()) return true;
  const auto [group, slot] = table.AllocateSlot(row, epoch);
  if (group == nullptr) return false;
  table_ = &table;
  address_.store(address_word(group, slot), std::memory_order_release);
  return true;
}

void RowRecord::PreserveImage(PaxAddress at, bool visible, EpochNumber epoch) {
  assert(epoch != 0);
  auto &image_buffer = pax::EpochImageBuffer::Global();
  if (!image_buffer.HasOpenView()) return;
  // The old row's epoch is its word's epoch. A slot without a row counts
  // from epoch 0: a delete moves the word without touching the slot.
  const EpochNumber old_epoch = visible ? transaction_id.load().epoch : 0;
  if (!image_buffer.NeedsImage(old_epoch, epoch)) return;
  image_buffer.Preserve(at.group, at.slot, old_epoch, epoch, visible,
                        visible ? CopyValue() : std::string());
}

uint64_t RowRecord::InstallRow(const pax::Row &row, EpochNumber epoch) {
  assert(pax_allocated() && row.size != 0);
  const PaxAddress at = pax_address();
  const bool live = size_ != 0;
  PreserveImage(at, live, epoch);
  uint64_t changed;
  if (!at.group->ScatterRow(at.slot, row, live, changed)) {
    // Outgrown: write the row to a wider class, then retire the old slot.
    const auto [group, slot] = table_->AllocateSlot(row, epoch);
    // A full directory during a move stops the process.
    if (group == nullptr) std::abort();
    PreserveImage({group, slot}, false, epoch);
    group->ScatterRow(slot, row, false, changed);
    address_.store(address_word(group, slot), std::memory_order_release);
    table_->Retire(at.group, at.slot, epoch);
    table_->count_move();
  }
  size_ = static_cast<uint32_t>(row.size);
  return changed;
}

void RowRecord::DeleteRow(EpochNumber epoch) {
  const PaxAddress at = pax_address();
  if (at.group != nullptr) {
    if (size_ != 0) PreserveImage(at, true, epoch);
    address_.store(0, std::memory_order_release);
    table_->Retire(at.group, at.slot, epoch);
  }
  size_ = 0;
}

}  // namespace helios::storage
