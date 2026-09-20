/**
 * @file server/storage/src/pax/hyperloglog.h
 * A fixed-size sketch of how many distinct values a column has been given.
 */

#ifndef HELIOS_STORAGE_SRC_PAX_HYPERLOGLOG_H
#define HELIOS_STORAGE_SRC_PAX_HYPERLOGLOG_H

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace helios::storage {
namespace pax {

/**
 * @brief Estimates how many distinct values were added, in fixed memory.
 *
 * @details A hash turns a value into uniform bits, and the run of zeros at
 * the top of that hash says how rare it was: a run of k has probability
 * 2^-k, so the rarest run seen bounds how many distinct values went past. One
 * such observation varies far too much to use, so the top bits of the same
 * hash pick one of kRegisters counters and each counter keeps its own rarest
 * run. The estimate is their harmonic mean.
 *
 * add() is safe from any number of threads. A register only ever rises, so
 * a value keeps being counted after its row is deleted, and a value landing
 * where another already reached is not counted twice.
 *
 * The estimator is the one of Flajolet, Fusy, Gandouet and Meunier with their
 * small-range correction, which errs in either direction by a few percent and
 * carries a bias around the point the correction takes over. Their
 * large-range correction is left out, as it starts to matter past 2^32/30
 * distinct values.
 */
class HyperLogLog {
 public:
  // Registers, one byte each.
  static constexpr size_t kBits = 10;
  static constexpr size_t kRegisters = size_t{1} << kBits;

  HyperLogLog() {
    for (std::atomic<uint8_t> &reg : registers_) {
      reg.store(0, std::memory_order_relaxed);
    }
  }

  /**
   * @brief Records one value.
   */
  void add(int64_t value) {
    const uint64_t h = mix64(static_cast<uint64_t>(value));
    const size_t index = static_cast<size_t>(h >> (64 - kBits));
    const uint64_t tail = h << kBits;
    const uint8_t rank = tail == 0
                             ? static_cast<uint8_t>(64 - kBits + 1)
                             : static_cast<uint8_t>(__builtin_clzll(tail) + 1);
    std::atomic<uint8_t> &reg = registers_[index];
    uint8_t seen = reg.load(std::memory_order_relaxed);
    while (rank > seen &&
           !reg.compare_exchange_weak(seen, rank, std::memory_order_relaxed)) {
    }
  }

  /**
   * @brief Returns the estimate, or false when nothing has been added.
   */
  bool estimate(uint64_t *ndv) const {
    double inverse = 0;
    size_t empty = 0;
    for (const std::atomic<uint8_t> &reg : registers_) {
      const uint8_t rank = reg.load(std::memory_order_relaxed);
      if (rank == 0) empty++;
      inverse += std::ldexp(1.0, -rank);
    }
    if (empty == kRegisters) return false;
    const double m = static_cast<double>(kRegisters);
    // Flajolet's alpha_m for the raw estimate.
    const double alpha = 0.7213 / (1.0 + 1.079 / m);
    double value = alpha * m * m / inverse;
    // At 2.5m and below the raw estimate is biased; the small-range
    // correction reads the empty registers instead.
    if (value <= 2.5 * m && empty > 0) {
      value = m * std::log(m / static_cast<double>(empty));
    }
    if (value < 1.0) value = 1.0;
    // Every register at its highest rank puts the raw estimate past 2^64,
    // which has no uint64_t to convert to.
    *ndv = value >= 0x1p64 ? UINT64_MAX : static_cast<uint64_t>(value);
    return true;
  }

 private:
  /**
   * @brief splitmix64's finalizer, which spreads keys differing in one bit
   * across the whole word. The register choice depends on that spread.
   */
  static uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
  }

  std::atomic<uint8_t> registers_[kRegisters];
};

}  // namespace pax
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_PAX_HYPERLOGLOG_H
