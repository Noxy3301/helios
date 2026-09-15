/**
 * @file server/storage/src/silo/read_set.h
 * The caller's read observations and their commit-time validation.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_READ_SET_H
#define HELIOS_STORAGE_SRC_SILO_READ_SET_H

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
  /** @brief The outcome of validating the submitted read observations. */
  enum class Status {
    kValid,
    kTableMissing,
    kPointChanged,
    kPrimaryRangeChanged,
    kSecondaryRangeChanged,
  };

  /** @brief Borrows the observations supplied by the caller. */
  ReadSet(const std::vector<ExternalReadEntry> &point_reads,
          const std::vector<ExternalRangeReadEntry> &range_reads)
      : point_reads_(point_reads), range_reads_(range_reads) {}

  /**
   * @brief Checks that every range has its required exclusive end bound.
   * @return false if a bound is missing; the caller chooses the abort reason.
   */
  bool CheckRangeBounds() const;

  /**
   * @brief Checks every point TID and replays ranges to compare their results.
   * @details Only locks owned by write_set are allowed during validation.
   * Range replay compares keys and order; consumed row TIDs are point reads.
   * @pre Range bounds were checked and the caller holds its write locks and
   * epoch.
   * @param[in,out] max_tid Raised to the largest version observed by
   * validation.
   * @return kValid or the first validation failure. The caller chooses the
   * abort reason and handles abort.
   */
  Status Validate(TableDictionary &tables, const WriteSet &write_set,
                  Tidword &max_tid) const;

 private:
  const std::vector<ExternalReadEntry> &point_reads_;
  const std::vector<ExternalRangeReadEntry> &range_reads_;
};

}  // namespace helios::storage::silo
#endif
