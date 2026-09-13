/**
 * @file server/storage/src/wal/wal.h
 * The log file itself: the frame layout, the reserved capacity, and the
 * startup scan that decides what survived.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_WAL_H
#define HELIOS_STORAGE_SRC_WAL_WAL_H

#include <sys/types.h>

#include <cstdint>
#include <functional>
#include <map>
#include <string>

#include "util/epoch.h"
#include "wal/log_record.h"

namespace helios::storage {
namespace wal {

/**
 * @brief The complete WAL prefix recovered at startup.
 *
 * @details The scan stops at the first invalid frame, zeroes the remaining
 * written bytes and succeeds with the preceding frames. Recovery assumes
 * process crashes; media corruption and files from another database are
 * outside this contract. I/O failures stop recovery without allowing an
 * append at an unknown offset.
 */
struct WalScanResult {
  enum class Status { kOk, kIoError };

  // On kIoError, error_number and detail name the failed syscall. A failed
  // repair may have zeroed part of the suffix; its prefix is never erased.
  Status status{Status::kOk};
  EpochNumber last_epoch{0};
  LogRecords records;
  bool tail_zeroed{false};
  int error_number{0};
  std::string detail;
  // Frames not read because they are at or below min_epoch, and their
  // on-disk size. Skipped by header alone except the boundary frame.
  size_t frames_skipped{0};
  uint64_t bytes_skipped{0};
};

struct WalAppendResult {
  bool ok{true};
  int error_number{0};
};

/**
 * @brief Seam for the syscalls the append and capacity paths use, letting a
 * test inject a write, sync, initialisation or read failure.
 * @details `pwrite` and `fdatasync` carry a group; `initialise_pwrite`
 * carries the zeroes that reserve capacity, kept separate so a capacity
 * failure cannot consume an injection aimed at a group; `pread` is what
 * the scan and the repair-range check read through.
 * @note Everything else (open, flock, fstat, and the fsync that follows a
 * zero write) is always the real syscall.
 * @note HELIOS_WAL_FDATASYNC_FAIL_AFTER=<count> makes the fdatasync that
 * Posix() returns let that many calls through and fail every later call with
 * EIO. Each Posix() call creates one counter, shared by every copy of the
 * WalIo it returned. A value that is not decimal digits, or that does not
 * fit in a long, stops startup.
 */
struct WalIo {
  std::function<ssize_t(int, const void *, size_t, off_t)> pwrite;
  std::function<int(int)> fdatasync;
  std::function<ssize_t(int, const void *, size_t, off_t)> initialise_pwrite;
  std::function<ssize_t(int, void *, size_t, off_t)> pread;

  static WalIo Posix();
};

/**
 * @brief The single write-ahead log: one file of epoch frames, written by
 * one flusher.
 *
 * @details
 * Frames are written in place, at an offset the instance tracks, into a
 * region already written out with zeroes, so that a group's fdatasync
 * persists data and not the size, allocation or extent-state metadata a
 * growing file drags in (see Config::wal_initial_capacity_bytes).
 *
 * Two consequences run through the rest of this class. The end of the log
 * is not the end of the file: it is where the zeroes begin, which is why
 * nothing may be written before Scan has found it. And the zeroes
 * ahead of the log are an invariant, not an accident: they are what makes
 * an interrupted write recognisable, so a repair restores them.
 *
 * Frame layout, little-endian:
 *   magic(u32) version(u16) flags(u16) payload_len(u32) epoch(u32) crc32c(u32)
 *   payload
 *
 * The checksum covers the header up to but excluding the crc field, plus
 * the payload, so a bit flip in the epoch or length is detected too. The
 * payload is the msgpack packing of a non-empty record list carrying the
 * frame's epoch, and frame epochs are non-decreasing. No frame ever carries
 * an empty record list; the caller keeps empty buckets out of AppendGroup,
 * which refuses them rather than skipping them.
 *
 * @note An exclusive flock keeps out a second process; it does not make a
 * second flusher inside this process defined.
 */
class Wal {
 public:
  /**
   * @brief Opens the log and takes an exclusive lock on it.
   * @details The constructor neither preallocates nor reads. Scan
   * locates the end of the log first, then initialises the region past it.
   * @param[in] initial_capacity_bytes How much is made writable in place at
   * a time. It is a granularity rather than a limit: a log that outgrows it
   * is extended by the same amount again, at the price of one synchronous
   * initialisation. `kNoPreallocation` leaves the file to grow as it is
   * written.
   */
  Wal(const std::string &work_dir, WalIo io = WalIo::Posix(),
      uint64_t initial_capacity_bytes = kDefaultCapacityBytes);
  ~Wal();

  Wal(const Wal &) = delete;
  Wal &operator=(const Wal &) = delete;

  /**
   * @brief Reads the log from the beginning, repairs an interrupted tail,
   * and initialises the capacity beyond it.
   * @param[in] min_epoch Frames at or below this are counted but not read.
   * @return See WalScanResult; `Ok` carries the last epoch and the records.
   * @note Must succeed before the first append: it is what locates the end
   * of the log, and until the bytes of an interrupted write are overwritten
   * with zeroes a later shorter group would leave them behind as a frame
   * the next scan cannot place. A scan run after this instance has already
   * failed does not retry; it reports the failure again.
   *
   * A skipped frame still contributes the last epoch and the log end. Such a
   * frame is skipped by header alone, except the boundary frame, which is
   * read and checksummed in full. A header that fails to parse falls back to
   * a full scan from offset 0.
   */
  WalScanResult Scan(EpochNumber min_epoch = 0);

  /**
   * @brief Appends one frame per bucket whose epoch is at or below `target`,
   * in epoch order, as one group write followed by one fdatasync.
   * @param[in] buckets Records grouped by their commit epoch. Buckets above
   * `target` are ignored and stay the caller's to carry forward.
   * @param[in] target The highest epoch this call may write.
   * @return Failure is returned without having advanced anything the caller
   * may publish. A bucket that would produce a frame the scan rejects
   * (empty, epoch zero, an epoch below the log's last epoch, a record epoch
   * disagreeing with its bucket) fails with EINVAL before anything is
   * written, as does a payload too large to be framed with EOVERFLOW; both
   * leave the instance usable.
   * @note Calling this before a successful scan, or after a failure has
   * left the end of the log unknown, fail-stops the process rather than
   * returning: there is nothing trustworthy to append at.
   */
  WalAppendResult AppendGroup(const std::map<EpochNumber, LogRecords> &buckets,
                              EpochNumber target);

  const std::string &path() const { return path_; }

  /**
   * @brief Offset one past the last frame, which is where the next group
   *        lands.
   */
  off_t write_offset() const { return write_offset_; }

  /**
   * @brief How many times capacity had to be extended.
   * @details Each extension synchronously writes out a whole new zeroed
   * region before the group that needed it.
   */
  size_t extension_count() const { return extension_count_; }

  static constexpr uint32_t kMagic = 0x4c57414c;  // "LAWL"
  static constexpr uint16_t kVersion = 1;
  static constexpr uint16_t kFlags = 0;
  static constexpr size_t kHeaderSize = 20;
  static constexpr uint32_t kMaxPayloadSize = 256u * 1024u * 1024u;
  static constexpr uint64_t kDefaultCapacityBytes = 64ull * 1024ull * 1024ull;
  static constexpr uint64_t kNoPreallocation = 0;

 private:
  enum class State { kUnscanned, kReady, kFailed };
  WalScanResult IoFailure(const std::string &operation, int error);
  WalScanResult FinishScan(WalScanResult &&result, off_t end_of_log);
  bool FindLastNonZero(off_t from, off_t to, off_t &last_non_zero,
                       int &error) const;
  bool EnsureCapacityFor(off_t end_of_log, size_t group_size, int &error);
  bool WriteZeroesAndSync(off_t from, off_t to, int &error);
  bool WriteAllAt(const uint8_t *data, size_t size, off_t offset, int &error);

  /**
   * @brief Reads exactly size bytes from the WAL at offset into out.
   *
   * @details Retries short reads and EINTR without changing the file's
   * current position.
   *
   * @param[out] out Buffer with room for size bytes; may be partly filled
   * on failure.
   * @param[out] error Receives errno on failure, or EIO on unexpected EOF.
   * @return True when all requested bytes were read.
   */
  bool PreadAll(uint8_t *out, size_t size, off_t offset, int &error) const;

  std::string path_;
  WalIo io_;
  int fd_{-1};
  uint64_t initial_capacity_bytes_;
  State state_{State::kUnscanned};
  off_t write_offset_{0};
  EpochNumber last_epoch_{0};
  // The file's size, which under preallocation is also the offset below
  // which every block is allocated and holds written-out zeroes.
  off_t initialised_size_{0};
  size_t extension_count_{0};
};

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_WAL_H
