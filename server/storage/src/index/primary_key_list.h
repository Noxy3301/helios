/**
 * @file server/storage/src/index/primary_key_list.h
 * The primary keys one secondary key points at, packed into a single
 * shared allocation.
 */

#ifndef HELIOS_STORAGE_SRC_INDEX_PRIMARY_KEY_LIST_H
#define HELIOS_STORAGE_SRC_INDEX_PRIMARY_KEY_LIST_H

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace helios::storage {

/**
 * @brief Immutable, sorted, deduplicated, length-prefixed primary-key list
 *        held as `std::shared_ptr<const PrimaryKeyList>` in one allocation.
 *
 * @details Each key is stored packed: an unsigned LEB128 length followed by
 * that many key bytes. A null Ptr and `count == 0` are both empty.
 */
struct PrimaryKeyList {
  using Ptr = std::shared_ptr<const PrimaryKeyList>;
  class View;

  uint32_t count;
  // Byte length of the records after this header, not the allocation size.
  uint32_t bytes;

  /**
   * @brief Builds an immutable primary-key list from already sorted,
   * deduplicated keys.
   * @param keys Primary keys in strict lexicographic order.
   * @return Primary-key list containing exactly `keys`.
   * @throws std::length_error if the count or payload byte length exceeds
   * `uint32_t`.
   */
  static Ptr FromSortedDeduped(const std::vector<std::string> &keys) {
    size_t payload_bytes = 0;
    for (const auto &key : keys) {
      payload_bytes += PackedKey::PackedSize(key);
      CheckFitsUint32(payload_bytes, "packed primary-key list bytes");
    }

    auto packed = AllocateMutable(
        CheckFitsUint32(keys.size(), "packed primary-key list count"),
        static_cast<uint32_t>(payload_bytes));
    char *out = packed->MutablePackedKeys();
    for (const auto &key : keys) {
      out = PackedKey::Pack(out, key);
    }
    assert(out == packed->MutablePackedKeys() + packed->bytes);
    return packed;
  }

  /**
   * @brief Inserts a key while preserving sorted, deduplicated invariants.
   * @param keys Existing immutable primary-key list, or null for an empty
   * list.
   * @param key Primary key to insert.
   * @return The same `shared_ptr` when `key` is already present; otherwise a
   * new immutable primary-key list containing `key`.
   * @throws std::length_error if the updated count or payload byte length
   * exceeds `uint32_t`.
   */
  static Ptr Insert(const Ptr &keys, std::string_view key) {
    if (!keys) {
      return FromOne(key);
    }

    const char *const src_begin = keys->PackedKeys();
    const char *const src_end = src_begin + keys->bytes;
    const char *insert_pos = src_end;

    for (const char *cursor = src_begin; cursor != src_end;) {
      const PackedKey packed = PackedKey::Unpack(cursor);
      const std::string_view value(packed.value, packed.length);
      if (value == key) return keys;
      if (key < value) {
        insert_pos = packed.start;
        break;
      }
      cursor = packed.next;
    }

    const size_t added_bytes = PackedKey::PackedSize(key);
    const size_t new_bytes = static_cast<size_t>(keys->bytes) + added_bytes;
    auto next = AllocateMutable(
        CheckFitsUint32(static_cast<size_t>(keys->count) + 1,
                        "packed primary-key list count"),
        CheckFitsUint32(new_bytes, "packed primary-key list bytes"));

    char *out = next->MutablePackedKeys();
    const size_t prefix_bytes = static_cast<size_t>(insert_pos - src_begin);
    if (prefix_bytes != 0) {
      std::memcpy(out, src_begin, prefix_bytes);
      out += prefix_bytes;
    }
    out = PackedKey::Pack(out, key);
    const size_t suffix_bytes = static_cast<size_t>(src_end - insert_pos);
    if (suffix_bytes != 0) {
      std::memcpy(out, insert_pos, suffix_bytes);
      out += suffix_bytes;
    }
    assert(out == next->MutablePackedKeys() + next->bytes);
    return next;
  }

  /**
   * @brief Deletes a key while preserving sorted, deduplicated invariants.
   * @param keys Existing immutable primary-key list, or null for an empty
   * list.
   * @param key Primary key to delete.
   * @return The same `shared_ptr` when `keys` is null, empty, or does not
   * contain `key`; otherwise a new immutable primary-key list without `key`.
   */
  static Ptr Delete(const Ptr &keys, std::string_view key) {
    if (!keys || keys->count == 0) return keys;

    const char *const src_begin = keys->PackedKeys();
    const char *const src_end = src_begin + keys->bytes;

    for (const char *cursor = src_begin; cursor != src_end;) {
      const PackedKey packed = PackedKey::Unpack(cursor);
      const std::string_view value(packed.value, packed.length);
      if (value == key) {
        const size_t removed_bytes =
            static_cast<size_t>(packed.next - packed.start);
        const size_t new_bytes =
            static_cast<size_t>(keys->bytes) - removed_bytes;
        auto next = AllocateMutable(
            CheckFitsUint32(static_cast<size_t>(keys->count) - 1,
                            "packed primary-key list count"),
            static_cast<uint32_t>(new_bytes));

        char *out = next->MutablePackedKeys();
        const size_t prefix_bytes =
            static_cast<size_t>(packed.start - src_begin);
        if (prefix_bytes != 0) {
          std::memcpy(out, src_begin, prefix_bytes);
          out += prefix_bytes;
        }
        const size_t suffix_bytes = static_cast<size_t>(src_end - packed.next);
        if (suffix_bytes != 0) {
          std::memcpy(out, packed.next, suffix_bytes);
          out += suffix_bytes;
        }
        assert(out == next->MutablePackedKeys() + next->bytes);
        return next;
      }
      if (key < value) return keys;
      cursor = packed.next;
    }

    return keys;
  }

  /**
   * @brief Returns the packed keys after the fixed header.
   * @return Pointer to the `count` records that occupy `bytes` bytes.
   */
  const char *PackedKeys() const {
    return reinterpret_cast<const char *>(this) + sizeof(PrimaryKeyList);
  }

 private:
  /**
   * @brief One primary key as the list stores it: an unsigned LEB128 length,
   *        then the key bytes.
   *
   * @details Unpack locates the key at `start` and Pack writes one; neither
   * has anything to do with a table row.
   */
  struct PackedKey {
    const char *start;
    const char *value;
    const char *next;
    size_t length;

    static size_t PackedSize(std::string_view key) {
      return VarintSize(key.size()) + key.size();
    }

    // Writes one packed key at `out` and returns the byte after it.
    static char *Pack(char *out, std::string_view key) {
      out = WriteVarint(out, key.size());
      if (!key.empty()) {
        std::memcpy(out, key.data(), key.size());
        out += key.size();
      }
      return out;
    }

    // Reads the key packed at `start`. Pack is the only writer, so the length
    // always terminates and stays inside the allocation.
    static PackedKey Unpack(const char *start) {
      const char *cursor = start;
      size_t length = 0;
      for (unsigned shift = 0;; shift += 7) {
        const unsigned char byte = static_cast<unsigned char>(*cursor++);
        length |= static_cast<size_t>(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) {
          return PackedKey{start, cursor, cursor + length, length};
        }
      }
    }
  };

  struct Deleter {
    void operator()(PrimaryKeyList *keys) const noexcept {
      keys->~PrimaryKeyList();
      delete[] reinterpret_cast<std::byte *>(keys);
    }
  };

  using MutablePtr = std::shared_ptr<PrimaryKeyList>;

  PrimaryKeyList(uint32_t record_count, uint32_t payload_bytes)
      : count(record_count), bytes(payload_bytes) {}

  static MutablePtr AllocateMutable(uint32_t record_count,
                                    uint32_t payload_bytes) {
    const size_t allocation_size =
        sizeof(PrimaryKeyList) + static_cast<size_t>(payload_bytes);
    std::byte *raw = new std::byte[allocation_size];
    auto *packed = new (raw) PrimaryKeyList(record_count, payload_bytes);
    return MutablePtr(packed, Deleter{});
  }

  static Ptr FromOne(std::string_view key) {
    const size_t payload_bytes = PackedKey::PackedSize(key);
    auto packed = AllocateMutable(
        1, CheckFitsUint32(payload_bytes, "packed primary-key list bytes"));
    [[maybe_unused]] char *out =
        PackedKey::Pack(packed->MutablePackedKeys(), key);
    assert(out == packed->MutablePackedKeys() + packed->bytes);
    return packed;
  }

  static uint32_t CheckFitsUint32(size_t value, const char *field) {
    if (value > std::numeric_limits<uint32_t>::max()) {
      throw std::length_error(field);
    }
    return static_cast<uint32_t>(value);
  }

  // Unsigned LEB128: seven bits a byte, high bit continues.
  static size_t VarintSize(size_t value) {
    size_t size = 1;
    while (value >= 0x80) {
      value >>= 7;
      ++size;
    }
    return size;
  }

  static char *WriteVarint(char *out, size_t value) {
    while (value >= 0x80) {
      *out++ = static_cast<char>((value & 0x7f) | 0x80);
      value >>= 7;
    }
    *out++ = static_cast<char>(value);
    return out;
  }

  char *MutablePackedKeys() {
    return reinterpret_cast<char *>(this) + sizeof(PrimaryKeyList);
  }
};

static_assert(sizeof(PrimaryKeyList) == sizeof(uint32_t) * 2,
              "PrimaryKeyList must remain a two-field header");
static_assert(std::is_standard_layout<PrimaryKeyList>::value,
              "PrimaryKeyList must remain a standard-layout header");

/**
 * @brief Zero-copy view over a PrimaryKeyList.
 *
 * @details Holds no reference count: it stores the raw pointer of the list it
 * was made from. The list, and every string_view taken from the view, stay
 * valid only while the caller keeps its own Ptr alive.
 */
class PrimaryKeyList::View {
 public:
  /**
   * @brief Forward iterator yielding `std::string_view` values into the list.
   */
  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::string_view;
    using difference_type = std::ptrdiff_t;
    using reference = std::string_view;
    using pointer = void;

    iterator() = default;

    reference operator*() const {
      const auto packed = PrimaryKeyList::PackedKey::Unpack(cursor_);
      return std::string_view(packed.value, packed.length);
    }

    iterator &operator++() {
      assert(remaining_ != 0);
      const auto packed = PrimaryKeyList::PackedKey::Unpack(cursor_);
      cursor_ = packed.next;
      --remaining_;
      return *this;
    }

    iterator operator++(int) {
      iterator previous = *this;
      ++*this;
      return previous;
    }

    bool operator==(const iterator &rhs) const {
      return cursor_ == rhs.cursor_ && remaining_ == rhs.remaining_;
    }

    bool operator!=(const iterator &rhs) const { return !(*this == rhs); }

   private:
    friend class View;

    iterator(const char *cursor, uint32_t remaining)
        : cursor_(cursor), remaining_(remaining) {}

    const char *cursor_ = nullptr;
    uint32_t remaining_ = 0;
  };

  /**
   * @brief Constructs a view over a raw primary-key list pointer.
   * @param keys Primary-key list to view, or null for an empty view.
   */
  explicit View(const PrimaryKeyList *keys = nullptr) : keys_(keys) {}

  /**
   * @brief Constructs a view over a shared primary-key list.
   * @param keys Primary-key list to view, or null for an empty view.
   */
  explicit View(const PrimaryKeyList::Ptr &keys) : keys_(keys.get()) {}

  bool empty() const { return size() == 0; }

  size_t size() const { return keys_ ? keys_->count : 0; }

  iterator begin() const {
    if (!keys_) return iterator();
    const char *const start = keys_->PackedKeys();
    return iterator(start, keys_->count);
  }

  iterator end() const {
    if (!keys_) return iterator();
    return iterator(keys_->PackedKeys() + keys_->bytes, 0);
  }

  /**
   * @brief Finds the first key not less than `key`.
   *
   * @details Linear in the list length: the records are variable-length, so
   * there is no random access.
   *
   * @param key Primary key to search for.
   * @return Iterator to the first matching or greater key, or `end()`.
   */
  iterator lower_bound(std::string_view key) const {
    for (auto it = begin(); it != end(); ++it) {
      if (!(*it < key)) return it;
    }
    return end();
  }

  /**
   * @brief Checks whether `key` is present.
   * @param key Primary key to search for.
   * @return True when the view contains `key`.
   */
  bool contains(std::string_view key) const {
    const auto it = lower_bound(key);
    return it != end() && *it == key;
  }

  /**
   * @brief Compares this view with another key-by-key.
   * @param rhs View to compare with.
   * @return True when both views contain the same keys in the same order.
   */
  bool equals(View rhs) const {
    if (size() != rhs.size()) return false;
    auto lhs_it = begin();
    auto rhs_it = rhs.begin();
    for (; lhs_it != end(); ++lhs_it, ++rhs_it) {
      if (*lhs_it != *rhs_it) return false;
    }
    return true;
  }

 private:
  const PrimaryKeyList *keys_;
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_INDEX_PRIMARY_KEY_LIST_H
