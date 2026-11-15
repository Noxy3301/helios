/**
 * @file server/storage/src/wal/wal.h
 * The log on disk: numbered segment files of epoch frames, the pool that
 * prepares and recycles them, and the startup scan that decides what survived.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_WAL_H
#define HELIOS_STORAGE_SRC_WAL_WAL_H

#include <sys/types.h>
#include <sys/uio.h>

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "helios/config.h"

#include "util/epoch.h"
#include "wal/log_record.h"

namespace helios::storage {
namespace wal {

/**
 * @brief What the startup scan of the WAL found.
 *
 * @details The scan stops at the first invalid record, zeroes the remaining
 * bytes of that segment and succeeds with the preceding frames. Recovery
 * assumes process crashes; media corruption and files from another database
 * are outside this contract. I/O failures, and a segment the log requires but
 * that is missing or has no valid header, stop recovery without allowing an
 * append at an unknown offset.
 */
struct WalScanResult {
  enum class Status { kOk, kIoError };

  // On kIoError, error_number and detail name the failed syscall. A failed
  // repair may have zeroed part of the suffix; its prefix is never erased.
  Status status{Status::kOk};
  EpochNumber last_epoch{0};
  bool tail_zeroed{false};
  int error_number{0};
  std::string detail;
  // Frames read but not replayed because they are at or below min_epoch, and
  // their on-disk size.
  size_t frames_skipped{0};
  uint64_t bytes_skipped{0};
};

// A place in the log. Offset 0 names the first record after the segment's
// header.
struct WalPosition {
  uint64_t segment_number{1};
  uint64_t offset{0};
};

// Receives the records of one frame the scan replays.
using Replay = std::function<void(const LogRecords &)>;

struct WalAppendResult {
  bool ok{true};
  int error_number{0};
};

/**
 * @brief Seam for the syscalls the append and preparation paths use, letting a
 * test inject a write, sync, initialisation or read failure.
 * @details `pwritev` and `fdatasync` carry a group; `initialise_pwrite`
 * carries the zeroes of a fresh segment or a repair and each segment's header,
 * kept separate so such a failure cannot consume an injection aimed at a
 * group; `pread` is what the scan and the repair-range check read through.
 * @note Everything else (open, rename, flock, fstat, and the fsync that follows
 * a preparation or a repair) is always the real syscall.
 * @note HELIOS_WAL_FDATASYNC_FAIL_AFTER=<count> makes the fdatasync that
 * Posix() returns let that many calls through and fail every later call with
 * EIO. Each Posix() call creates one counter, shared by every copy of the
 * WalIo it returned. A value that is not decimal digits, or that does not
 * fit in a long, stops startup.
 */
struct WalIo {
  std::function<ssize_t(int, const iovec *, int, off_t)> pwritev;
  std::function<int(int)> fdatasync;
  std::function<ssize_t(int, const void *, size_t, off_t)> initialise_pwrite;
  std::function<ssize_t(int, void *, size_t, off_t)> pread;

  static WalIo Posix();
};

/**
 * @brief The single write-ahead log: a chain of numbered segment files of epoch
 * records, written by one logger.
 *
 * @details
 * Records are written in place into segments whose blocks already exist, so
 * that a group's fdatasync persists data and not the size, allocation or
 * extent-state metadata a growing file drags in. A new segment is written out
 * with zeroes once; after that its file is recycled, overwriting whatever an
 * earlier segment left there.
 *
 * Record layout, little-endian:
 *   magic(u32) type(u16) segment_number(u64) physical_offset(u64)
 *   payload_len(u32) epoch(u32) crc32c(u32)
 *   payload
 *
 * The checksum covers the header up to the crc field, plus the payload. A
 * record is valid only at the segment and offset it names, so stale bytes of an
 * earlier segment are rejected however intact they are.
 *
 * Each segment opens with a header record filling its first block. A frame's
 * payload is the msgpack packing of a non-empty record list carrying the
 * frame's epoch; frame epochs strictly increase and an epoch never spans
 * segments. Padding records end each group on a block boundary, so a later
 * group never shares a block with one already acknowledged. An end record
 * closes a segment and is the only way into the next, which it reaches only
 * after both are durable.
 *
 * Segments below the checkpoint's redo start are covered: only those are
 * recycled. Ready segments have a durable header and name and wait for the
 * logger; a logger that finds none prepares one itself, which recycles only a
 * covered segment and so never overwrites what recovery still needs.
 *
 * @note An exclusive flock on the directory keeps out a second process; it does
 * not make a second logger inside this process defined.
 */
class Wal {
 public:
  enum class WorkerTask { kPrepare, kCheckpoint, kStop };

  /**
   * @brief Opens the directory and takes an exclusive lock on it.
   * @details The constructor reads nothing; Scan finds the end of the log.
   * @param[in] segment_bytes A multiple of the filesystem block.
   */
  Wal(const std::string &work_dir, WalIo io = WalIo::Posix(),
      uint64_t segment_bytes = kWalSegmentBytes);
  ~Wal();

  Wal(const Wal &) = delete;
  Wal &operator=(const Wal &) = delete;

  /**
   * @brief Reads the log from `start`, and on the first successful call makes
   * it ready to append.
   * @details That first call zeroes the discarded tail, syncs what the scan
   * accepted, deletes temporary files and segments past the end of the log,
   * syncs the directory and prepares a spare segment, so that a later crash
   * cannot expose stale records under a segment number used again or lose what
   * this scan reported. Segments below `start` become covered. Later calls only
   * read.
   * @param[in] min_epoch Frames at or below this are checked and counted but
   * not replayed.
   * @param[in] replay Called once per frame in log order with its records,
   * which are dropped after the call. Empty reads and checks every frame
   * without keeping its records.
   * @param[in] start Where the redo starts. A directory with no segment at all
   * starts a new log only from the default position.
   * @return See WalScanResult; `Ok` carries the last epoch.
   * @note A scan run after this instance has already failed reports that
   * failure again.
   */
  WalScanResult Scan(EpochNumber min_epoch = 0, const Replay &replay = {},
                     WalPosition start = {});

  /**
   * @brief Appends one frame per bucket whose epoch is at or below `target`, in
   * epoch order, as one group write followed by one fdatasync, and one more of
   * each per segment the group has to leave.
   * @param[in] buckets Packed records grouped by their commit epoch. Buckets
   * above `target` are ignored and stay the caller's to carry forward.
   * @param[in] target The highest epoch this call may write.
   * @return Failure is returned without having advanced anything the caller
   * may publish. A bucket that would produce a frame the scan rejects
   * (empty, epoch zero, an epoch at or below the log's last epoch, a record
   * epoch disagreeing with its bucket) fails with EINVAL before anything is
   * written, as does a frame too large for a segment with EOVERFLOW; both leave
   * the instance usable.
   * @note When no segment is ready, waits for a preparation already running
   * and takes its segment, and prepares one itself only when none is running
   * or the running one failed. Calling this before a successful scan, or after
   * a failure has left the end of the log unknown, fail-stops the process
   * rather than returning: there is nothing trustworthy to append at.
   */
  WalAppendResult AppendGroup(
      const std::map<EpochNumber, PackedLogRecords> &buckets,
      EpochNumber target);

  /**
   * @brief Prepares one segment if fewer than the spare target are ready,
   * recycling a covered file when there is one and writing a fresh one when
   * not.
   * @return False when the preparation fails, or when the last one failed and
   * none has succeeded since. A logger that runs out of ready segments
   * prepares one itself and fails only when that preparation fails.
   * @note Safe from any thread; preparations run one at a time.
   */
  bool PrepareSegment();

  /**
   * @brief Declares every segment below `redo_segment` covered by a published
   * checkpoint, and so free to recycle.
   */
  void MarkCovered(uint64_t redo_segment);

  /**
   * @brief Blocks the worker until there is something for it to do.
   * @details A due checkpoint comes first while a segment is ready; otherwise
   * the worker tops the pool up to two segments.
   */
  WorkerTask WaitForWork();

  /**
   * @brief Ends WaitForWork.
   */
  void StopWorker();

  /**
   * @brief Where the next record lands, as of the last completed fdatasync.
   * @details Every frame before this position was written before the call.
   */
  WalPosition durable_end();

  uint64_t appended_bytes();
  void set_checkpoint_threshold(uint64_t appended_bytes);

  uint64_t segment_bytes() const { return segment_bytes_; }

  /**
   * @brief Offset in the active segment where the next group lands, or 0 before
   * a successful scan.
   */
  uint64_t write_offset() const {
    return state_ == State::kReady ? active_.append_offset : 0;
  }

  static std::string SegmentFileName(uint64_t segment_number);

  static constexpr uint32_t kMagic = 0x4c415748;  // "HWAL"
  static constexpr size_t kHeaderSize = 34;
  static constexpr uint32_t kMaxPayloadSize = 256u * 1024u * 1024u;

 private:
  enum class State { kUnscanned, kReady, kFailed };

  struct Segment {
    int fd{-1};
    uint64_t segment_number{0};
    uint64_t append_offset{0};
  };

  // A record as ReadRecord found it. `invalid` says why it cannot be trusted,
  // and is null for a valid one.
  struct Record {
    const char *invalid{nullptr};
    uint16_t type{0};
    EpochNumber epoch{0};
  };

  std::string SegmentPath(uint64_t segment_number) const;
  std::string TemporaryPath(uint64_t segment_number) const;
  uint64_t PaddedEnd(uint64_t end) const;

  WalScanResult IoFailure(const std::string &operation, int error);
  WalScanResult FinishScan(WalScanResult &&result, bool fresh,
                           const WalPosition &start, const WalPosition &tail,
                           const std::vector<uint64_t> &segments,
                           const std::vector<std::string> &temporaries);
  bool ReadRecord(int fd, uint64_t segment_number, uint64_t offset,
                  std::vector<uint8_t> &payload, Record &record,
                  int &error) const;
  bool FindLastNonZero(int fd, off_t from, off_t to, off_t &last_non_zero,
                       int &error) const;
  bool WriteAt(int fd, const uint8_t *data, size_t size, off_t offset,
               int &error);
  bool WriteZeroesAndSync(int fd, off_t from, off_t to, int &error);
  bool WritePadding(int fd, uint64_t segment_number, uint64_t offset,
                    int &error);
  bool WriteAllAt(std::vector<iovec> &iov, off_t offset, int &error);
  bool Fdatasync(int fd, int &error);
  bool WriteAndSync(std::vector<iovec> &iov, uint64_t offset, int &error);
  bool FormatSegment(uint64_t segment_number, uint64_t recycled, int &fd,
                     int &error);
  bool Prepare(bool miss, int &error);
  bool AcquireReady(Segment &next, int &error);
  bool Rotate(std::vector<iovec> &iov, size_t group_size, int &error);
  void PublishEnd(const WalPosition &end, uint64_t written);

  /**
   * @brief Reads exactly size bytes from fd at offset into out.
   *
   * @details Retries short reads and EINTR without changing the file's
   * current position.
   *
   * @param[out] out Buffer with room for size bytes; may be partly filled
   * on failure.
   * @param[out] error Receives errno on failure, or EIO on unexpected EOF.
   * @return True when all requested bytes were read.
   */
  bool PreadAll(int fd, uint8_t *out, size_t size, off_t offset,
                int &error) const;

  std::string directory_;
  WalIo io_;
  const uint64_t segment_bytes_;
  const uint64_t spare_target_bytes_;
  uint64_t block_bytes_{0};
  // Holds the flock for the life of the instance.
  int directory_fd_{-1};
  State state_{State::kUnscanned};
  Segment active_;
  EpochNumber last_epoch_{0};
  // The payload of a padding record, which is at most one block.
  std::vector<uint8_t> padding_;

  // Protects everything below.
  std::mutex pool_mutex_;
  std::condition_variable pool_cv_;
  std::deque<Segment> ready_;
  std::deque<uint64_t> covered_;
  uint64_t oldest_retained_{1};
  uint64_t next_segment_{1};
  // The last preparation's failure, cleared by a success. Only a preparation
  // for a miss runs while it is set.
  int prepare_error_{0};
  // A preparation holds a segment number; the next waits until it ends.
  bool preparing_{false};
  bool worker_stopped_{false};
  WalPosition durable_end_;
  uint64_t appended_bytes_{0};
  // No checkpoint is due until the checkpointer sets a threshold.
  uint64_t checkpoint_threshold_{UINT64_MAX};
};

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_WAL_H
