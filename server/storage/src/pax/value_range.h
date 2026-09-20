/**
 * @file server/storage/src/pax/value_range.h
 * The lowest and highest value a table column has been given.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_VALUE_RANGE_H
#define HELIOS_STORAGE_SRC_PAX_VALUE_RANGE_H

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace helios::storage {
namespace pax {

/**
 * @brief The range of values written to one column, which never narrows.
 *
 * @details A reader may answer a filter from this range alone, both by
 * dropping a scan whose predicate falls outside it and by dropping a
 * predicate every value satisfies, so the range has to cover every value a
 * reader can reach. Covering more than that only costs a chance to skip work.
 *
 * The range is kept in kCopies copies, each on its own cache line, and a
 * thread writes to the copy it is handed at first use. read() folds them
 * together. InnoDB spreads its own counters the same way, in ut0counter.h.
 *
 * add() is safe from any number of threads. A writer has to finish add()
 * before it publishes the row holding that value: read() folds the copies
 * one at a time, and a copy caught between its two stores hands back a bound
 * missing one side.
 */
class ValueRange {
 public:
  // Copies of the range, one per cache line.
  static constexpr size_t kCopies = 64;

  ValueRange() {
    for (Copy &copy : copies_) {
      copy.lo.store(INT64_MAX, std::memory_order_relaxed);
      copy.hi.store(INT64_MIN, std::memory_order_relaxed);
    }
  }

  /**
   * @brief Widens the range to include `value`.
   */
  void add(int64_t value) {
    Copy &copy = copies_[mine()];
    int64_t low = copy.lo.load(std::memory_order_relaxed);
    while (value < low && !copy.lo.compare_exchange_weak(
                              low, value, std::memory_order_relaxed)) {
    }
    int64_t high = copy.hi.load(std::memory_order_relaxed);
    while (value > high && !copy.hi.compare_exchange_weak(
                               high, value, std::memory_order_relaxed)) {
    }
  }

  /**
   * @brief Returns the range, or false when nothing has been added.
   */
  bool read(int64_t *lo, int64_t *hi) const {
    int64_t low = INT64_MAX;
    int64_t high = INT64_MIN;
    for (const Copy &copy : copies_) {
      // An untouched copy holds the empty range, which folds in unchanged.
      const int64_t copy_lo = copy.lo.load(std::memory_order_relaxed);
      const int64_t copy_hi = copy.hi.load(std::memory_order_relaxed);
      if (copy_lo < low) low = copy_lo;
      if (copy_hi > high) high = copy_hi;
    }
    if (low > high) return false;
    *lo = low;
    *hi = high;
    return true;
  }

 private:
  struct alignas(64) Copy {
    std::atomic<int64_t> lo;
    std::atomic<int64_t> hi;
  };

  /**
   * @brief Returns the copy this thread writes to, fixed at first use.
   *
   * @details Handed out in turn rather than by core, so a thread keeps one
   * line for the life of its connection. Threads past kCopies share a line
   * with an earlier one, where the compare exchange keeps the value.
   */
  static size_t mine() {
    static std::atomic<size_t> next{0};
    static thread_local const size_t chosen =
        next.fetch_add(1, std::memory_order_relaxed) % kCopies;
    return chosen;
  }

  Copy copies_[kCopies];
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_VALUE_RANGE_H
