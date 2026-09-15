/**
 * @file server/storage/src/silo/read_set.h
 * The caller's read observations and their commit-time validation.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_READ_SET_H
#define HELIOS_STORAGE_SRC_SILO_READ_SET_H

#include <string>
#include <vector>

#include "lineairdb/commit.h"

namespace helios::storage {
class TableDictionary;
struct Tidword;
}  // namespace helios::storage

namespace helios::storage::silo {
class WriteSet;

/**
 * @brief Validates the caller's point and range observations without copying
 * them.
 * @details Input vectors must remain valid and unchanged throughout the commit.
 * Duplicate point reads are retained and checked individually.
 */
class ReadSet {
 public:
  /** @brief Borrows the observations supplied by the caller. */
  ReadSet(const std::vector<ExternalReadEntry> &point_reads,
          const std::vector<ExternalRangeReadEntry> &range_reads)
      : point_reads_(point_reads), range_reads_(range_reads) {}

  /**
   * @brief Checks that every range has its required exclusive end bound.
   * @return false with reason set if a bound is missing.
   */
  bool CheckRangeBounds(std::string &reason) const;

  /**
   * @brief Checks every point TID and replays ranges to compare their results.
   * @details Only locks owned by write_set are allowed during validation.
   * Range replay compares keys and order; consumed row TIDs are point reads.
   * @pre Range bounds were checked and the caller holds its write locks and
   * epoch.
   * @param[in,out] max_tid Raised to the largest version observed by
   * validation.
   * @param[out] reason The failed check's label, when validation returns false.
   * @return true if all observations validate. On false, the caller handles
   * abort.
   */
  bool Validate(TableDictionary &tables, const WriteSet &write_set,
                Tidword &max_tid, std::string &reason) const;

 private:
  const std::vector<ExternalReadEntry> &point_reads_;
  const std::vector<ExternalRangeReadEntry> &range_reads_;
};

}  // namespace helios::storage::silo
#endif
