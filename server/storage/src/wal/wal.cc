/**
 * @file server/storage/src/wal/wal.cc
 * The log file itself: the frame layout, the reserved capacity, and the
 * startup scan that decides what survived.
 */

#include "wal/wal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <msgpack.hpp>
#include <system_error>
#include <utility>
#include <vector>

#include "util/debug_sync.h"
#include "util/spdlog.h"
#include "wal/crc32c.h"
#include "wal/flush_trace.h"

namespace helios::storage {
namespace wal {

namespace {

// Frame header fields, in bytes from the start of the frame. The magic word
// opens it at offset 0.
constexpr size_t kOffFlags = 4;
constexpr size_t kOffPayloadSize = 6;
constexpr size_t kOffEpoch = 10;
constexpr size_t kOffCrc = 14;

// The header bytes the checksum covers: all of them but the checksum word,
// which sits last.
constexpr size_t kCrcCoverage = Wal::kHeaderSize - sizeof(uint32_t);

void PutLe16(uint8_t *out, uint16_t value) {
  out[0] = static_cast<uint8_t>(value & 0xffu);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xffu);
}

void PutLe32(uint8_t *out, uint32_t value) {
  out[0] = static_cast<uint8_t>(value & 0xffu);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xffu);
  out[2] = static_cast<uint8_t>((value >> 16) & 0xffu);
  out[3] = static_cast<uint8_t>((value >> 24) & 0xffu);
}

uint16_t GetLe16(const uint8_t *in) {
  return static_cast<uint16_t>(in[0]) |
         static_cast<uint16_t>(static_cast<uint16_t>(in[1]) << 8);
}

uint32_t GetLe32(const uint8_t *in) {
  return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) |
         (static_cast<uint32_t>(in[2]) << 16) |
         (static_cast<uint32_t>(in[3]) << 24);
}

int Fsync(int fd) {
  int rc;
  do {
    rc = ::fsync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
}

// Persists a directory entry: a file's own fsync does not make its name
// durable.
bool FsyncDirectory(const std::string &directory, int &error) {
  const int dir_fd =
      ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (dir_fd < 0) {
    error = errno;
    return false;
  }
  const bool ok = Fsync(dir_fd) == 0;
  if (!ok) error = errno;
  ::close(dir_fd);
  return ok;
}

}  // namespace

WalIo WalIo::Posix() {
  WalIo io;
  io.pwrite = [](int fd, const void *data, size_t size, off_t offset) {
    return ::pwrite(fd, data, size, offset);
  };
  io.initialise_pwrite = io.pwrite;
  io.pread = [](int fd, void *data, size_t size, off_t offset) {
    return ::pread(fd, data, size, offset);
  };

  // Armed from the environment, like a debug sync point, so an out-of-process
  // test can arrange an EIO. Unset means the bare syscall.
  const char *raw = std::getenv("HELIOS_WAL_FDATASYNC_FAIL_AFTER");
  if (raw == nullptr) {
    io.fdatasync = [](int fd) { return ::fdatasync(fd); };
    return io;
  }
  // Digits only. A value that does not parse stops startup rather than
  // arming a count no run reaches.
  errno = 0;
  char *end = nullptr;
  const long successes = std::strtol(raw, &end, 10);
  if (!std::isdigit(static_cast<unsigned char>(raw[0])) || *end != '\0' ||
      errno == ERANGE) {
    SPDLOG_CRITICAL(
        "Invalid HELIOS_WAL_FDATASYNC_FAIL_AFTER='{0}': expected a "
        "non-negative count of calls to let through",
        raw);
    exit(EXIT_FAILURE);
  }
  auto remaining = std::make_shared<std::atomic<long>>(successes);
  io.fdatasync = [remaining](int fd) -> int {
    // Zero is sticky: a plain decrement would wrap and eventually let a call
    // through again, making "fails every later call" false in the limit.
    long current = remaining->load(std::memory_order_relaxed);
    while (current > 0 &&
           !remaining->compare_exchange_weak(current, current - 1,
                                             std::memory_order_relaxed)) {
    }
    if (current <= 0) {
      errno = EIO;
      return -1;
    }
    return ::fdatasync(fd);
  };
  return io;
}

Wal::Wal(const std::string &work_dir, WalIo io, uint64_t initial_capacity_bytes)
    : io_(std::move(io)), initial_capacity_bytes_(initial_capacity_bytes) {
  // Resolve the path and create the directory.
  // Strip a trailing separator first; parent_path() below must name the
  // true parent, not the directory itself.
  std::filesystem::path directory(work_dir);
  if (!directory.has_filename()) directory = directory.parent_path();
  path_ = (directory / "wal.log").string();

  std::error_code ec;
  std::filesystem::create_directory(directory, ec);
  if (ec) {
    throw std::system_error(ec, "create_directory " + work_dir);
  }

  // Open or create, then take the exclusive lock.
  // O_TRUNC is never used: an existing log is the only record of what was
  // reported durable. O_APPEND is never used either, and cannot be:
  // under it a pwrite ignores the offset it is given and lands at the end
  // of the file.
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
  if (fd_ < 0 && errno == EEXIST) {
    fd_ = ::open(path_.c_str(), O_RDWR | O_CLOEXEC);
  }
  if (fd_ < 0) {
    throw std::system_error(errno, std::generic_category(), "open " + path_);
  }

  // Writing in place puts the offsets under this process's control, so a
  // second process holding the same log would overwrite frames rather than
  // interleave with them.
  if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(),
                            "lock " + path_ + " exclusively");
  }

  // Fsync the file and its parents.
  // Every name on the way to the log is made durable here, whether or not
  // this process is the one that created it. Two processes can race to
  // create the directory or the file, and the one that loses a create can
  // still win the lock; it is then the only one left to persist what the
  // loser was going to.
  if (Fsync(fd_) != 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(), "fsync " + path_);
  }
  {
    const std::string parent =
        directory.has_parent_path() ? directory.parent_path().string() : ".";
    int error = 0;
    if (!FsyncDirectory(directory.string(), error) ||
        !FsyncDirectory(parent, error)) {
      ::close(fd_);
      fd_ = -1;
      throw std::system_error(error, std::generic_category(),
                              "fsync the directories holding " + path_);
    }
  }

  struct stat file_stat {};
  if (::fstat(fd_, &file_stat) < 0) {
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    throw std::system_error(error, std::generic_category(), "fstat " + path_);
  }
  // fstat into initialised_size_.
  // The file's size stands in for how far an earlier incarnation got with
  // writing zeroes. That needs one filesystem ordering: a size that
  // survives a crash must not run ahead of the zeroes it covers (ext4
  // data=ordered, the supported configuration). Capacity is not extended
  // here: the region to initialise begins where the log ends, which is
  // what Scan establishes.
  initialised_size_ = file_stat.st_size;
}

Wal::~Wal() {
  if (fd_ >= 0) ::close(fd_);
}

WalScanResult Wal::IoFailure(const std::string &operation, int error) {
  state_ = State::kFailed;
  WalScanResult result;
  result.status = WalScanResult::Status::kIoError;
  result.error_number = error;
  result.detail = operation;
  return result;
}

bool Wal::WriteAllAt(const uint8_t *data, size_t size, off_t offset,
                     int &error) {
  while (size != 0) {
    const size_t chunk = std::min<size_t>(size, SSIZE_MAX);
    const ssize_t written = io_.pwrite(fd_, data, chunk, offset);
    if (written > 0) {
      data += written;
      offset += written;
      size -= static_cast<size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    error = written == 0 ? EIO : errno;
    return false;
  }
  return true;
}

bool Wal::PreadAll(uint8_t *out, size_t size, off_t offset, int &error) const {
  while (size != 0) {
    const ssize_t got = io_.pread(fd_, out, size, offset);
    if (got > 0) {
      out += got;
      offset += got;
      size -= static_cast<size_t>(got);
      continue;
    }
    if (got < 0 && errno == EINTR) continue;
    // A short read below the size fstat reported means the file changed
    // under the reader, which this design does not allow.
    error = got == 0 ? EIO : errno;
    return false;
  }
  return true;
}

// Writes out zeroes over `[from, to)` and persists them. The zeroes move any
// size, allocation, or extent-state metadata work out of the group-flush
// path (how much exists depends on the filesystem and device), and they
// mark a region that holds no frame, which is what lets the scan find the
// end of the log. Both capacity initialisation and tail repair are this
// one operation.
bool Wal::WriteZeroesAndSync(off_t from, off_t to, int &error) {
  if (to <= from) return true;

  constexpr size_t kChunkSize = 1ull << 20;
  const std::vector<uint8_t> zeroes(kChunkSize, 0);
  for (off_t offset = from; offset < to;) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, to - offset));
    size_t remaining = size;
    const uint8_t *data = zeroes.data();
    while (remaining != 0) {
      const ssize_t written =
          io_.initialise_pwrite(fd_, data, remaining, offset);
      if (written > 0) {
        data += written;
        offset += written;
        remaining -= static_cast<size_t>(written);
        continue;
      }
      if (written < 0 && errno == EINTR) continue;
      error = written == 0 ? EIO : errno;
      return false;
    }
  }

  // The size and the blocks have to reach the device here, so that none of
  // this initialisation work lands on a group's own fdatasync.
  if (Fsync(fd_) != 0) {
    error = errno;
    return false;
  }
  return true;
}

// Reports the offset of the last byte in `[from, to)` that is not zero, or
// `from - 1` when every byte is.
bool Wal::FindLastNonZero(off_t from, off_t to, off_t &last_non_zero,
                          int &error) const {
  constexpr size_t kChunkSize = 1ull << 20;
  std::vector<uint8_t> chunk(kChunkSize);
  last_non_zero = from - 1;

  for (off_t at = from; at < to; at += kChunkSize) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, to - at));
    if (!PreadAll(chunk.data(), size, at, error)) return false;
    for (size_t i = size; i > 0; --i) {
      if (chunk[i - 1] != 0) {
        last_non_zero = at + static_cast<off_t>(i) - 1;
        break;
      }
    }
  }
  return true;
}

bool Wal::EnsureCapacityFor(off_t end_of_log, size_t group_size, int &error) {
  const uint64_t limit =
      static_cast<uint64_t>(std::numeric_limits<off_t>::max());
  if (static_cast<uint64_t>(group_size) >
      limit - static_cast<uint64_t>(end_of_log)) {
    error = EFBIG;
    return false;
  }
  // Without preallocation the group's own write is what extends the file,
  // which is the behaviour this exists to avoid. Writing zeroes over the
  // region first would cost a second write and a second sync per group.
  if (initial_capacity_bytes_ == kNoPreallocation) return true;

  const uint64_t needed = static_cast<uint64_t>(end_of_log) + group_size;
  if (needed <= static_cast<uint64_t>(initialised_size_) &&
      static_cast<uint64_t>(initialised_size_) >= initial_capacity_bytes_) {
    return true;
  }

  // Round up to a whole number of capacity units, so that a log which
  // outgrows its capacity pays for initialisation once per unit rather than
  // once per group. Computed rather than counted: a capacity of a few bytes
  // would make counting cost a step per unit on every group.
  const uint64_t units = std::max<uint64_t>(
      1, needed / initial_capacity_bytes_ +
             (needed % initial_capacity_bytes_ != 0 ? 1 : 0));
  if (units > limit / initial_capacity_bytes_) {
    error = EFBIG;
    return false;
  }
  const uint64_t target = units * initial_capacity_bytes_;
  if (target <= static_cast<uint64_t>(initialised_size_)) return true;

  if (!WriteZeroesAndSync(initialised_size_, static_cast<off_t>(target),
                          error)) {
    return false;
  }
  initialised_size_ = static_cast<off_t>(target);
  return true;
}

// Publishes the end of the log and initialises the capacity beyond it: the
// last step of a successful scan, after which groups may be written.
WalScanResult Wal::FinishScan(WalScanResult &&result, off_t end_of_log) {
  int error = 0;
  if (!EnsureCapacityFor(end_of_log, 0, error)) {
    return IoFailure("initialise the capacity of " + path_, error);
  }
  // Published together with Ready: a scan that could not finish leaves no
  // offset behind to be mistaken for the end of the log.
  write_offset_ = end_of_log;
  last_epoch_ = result.last_epoch;
  state_ = State::kReady;
  return std::move(result);
}

WalScanResult Wal::Scan(EpochNumber min_epoch) {
  // Refuse a scan on an instance that already failed.
  if (state_ == State::kFailed) {
    return IoFailure("scan " + path_ + " after a failure", EIO);
  }
  state_ = State::kUnscanned;

  // Bound the scan by the file size fstat reports.
  struct stat file_stat {};
  if (::fstat(fd_, &file_stat) < 0) {
    return IoFailure("fstat " + path_, errno);
  }
  const off_t file_size = file_stat.st_size;
  initialised_size_ = std::max(initialised_size_, file_size);

  LogRecords records;
  EpochNumber last_epoch = 0;
  bool have_frame = false;
  size_t frames_skipped = 0;
  uint64_t bytes_skipped = 0;
  off_t offset = 0;

  // Skip checkpoint-covered frames before reading the remaining records.
  if (min_epoch != 0) {
    off_t boundary_offset = 0;
    uint32_t boundary_payload_size = 0;
    uint8_t boundary_header[kHeaderSize];
    bool retry_full_scan = false;
    int error = 0;

    // Follow frame headers to the first epoch after the checkpoint.
    while (offset < file_size) {
      if (file_size - offset < static_cast<off_t>(kHeaderSize)) {
        retry_full_scan = true;
        break;
      }
      uint8_t header[kHeaderSize];
      if (!PreadAll(header, kHeaderSize, offset, error)) {
        return IoFailure("pread header of " + path_, error);
      }
      const uint32_t magic = GetLe32(header);
      const uint16_t flags = GetLe16(header + kOffFlags);
      const uint32_t payload_size = GetLe32(header + kOffPayloadSize);
      const EpochNumber epoch = GetLe32(header + kOffEpoch);
      if (magic != kMagic || flags != kFlags ||
          payload_size > kMaxPayloadSize) {
        const bool empty_header =
            std::all_of(header, header + kHeaderSize,
                        [](uint8_t byte) { return byte == 0; });
        if (!empty_header) retry_full_scan = true;
        break;
      }
      const uint64_t frame_end =
          static_cast<uint64_t>(offset) + kHeaderSize + payload_size;
      if (frame_end > static_cast<uint64_t>(file_size) || epoch == 0 ||
          (have_frame && epoch < last_epoch)) {
        retry_full_scan = true;
        break;
      }
      if (epoch > min_epoch) break;

      boundary_offset = offset;
      boundary_payload_size = payload_size;
      std::memcpy(boundary_header, header, kHeaderSize);
      last_epoch = epoch;
      have_frame = true;
      ++frames_skipped;
      bytes_skipped += kHeaderSize + payload_size;
      offset = static_cast<off_t>(frame_end);
    }

    // Verify the last skipped frame before trusting the resume offset.
    if (!retry_full_scan && have_frame) {
      std::vector<uint8_t> payload(boundary_payload_size);
      if (!PreadAll(payload.data(), boundary_payload_size,
                    boundary_offset + static_cast<off_t>(kHeaderSize), error)) {
        return IoFailure("pread payload of " + path_, error);
      }
      Crc32c crc;
      crc.Update(boundary_header, kCrcCoverage);
      crc.Update(payload.data(), payload.size());
      if (crc.Finish() != GetLe32(boundary_header + kOffCrc)) {
        retry_full_scan = true;
      }
    }

    // Retry from offset zero if the header skip could not be verified.
    if (retry_full_scan) {
      SPDLOG_WARN(
          "Could not skip checkpointed frames in {0}; scanning from offset 0",
          path_);
      offset = 0;
      last_epoch = 0;
      have_frame = false;
      frames_skipped = 0;
      bytes_skipped = 0;
    }
  }

  uint8_t header[kHeaderSize];
  std::vector<uint8_t> payload;
  std::string stop_reason;

  // Read complete frames until the first invalid frame.
  while (offset < file_size) {
    // Validate the header before trusting its payload length.
    if (file_size - offset < static_cast<off_t>(kHeaderSize)) {
      stop_reason = "the file ends inside a frame header";
      break;
    }
    int error = 0;
    if (!PreadAll(header, kHeaderSize, offset, error)) {
      return IoFailure("pread header of " + path_, error);
    }

    const uint32_t magic = GetLe32(header);
    const uint16_t flags = GetLe16(header + kOffFlags);
    const uint32_t payload_size = GetLe32(header + kOffPayloadSize);
    const EpochNumber epoch = GetLe32(header + kOffEpoch);
    const uint32_t stored_crc = GetLe32(header + kOffCrc);

    if (magic != kMagic) {
      stop_reason = "frame magic mismatch";
    } else if (flags != kFlags) {
      stop_reason = "unknown frame flags";
    } else if (payload_size > kMaxPayloadSize) {
      stop_reason = "payload too large";
    }
    if (!stop_reason.empty()) break;

    const uint64_t frame_end =
        static_cast<uint64_t>(offset) + kHeaderSize + payload_size;
    if (frame_end > static_cast<uint64_t>(file_size)) {
      stop_reason = "the file ends inside a frame payload";
      break;
    }

    // Read the payload and verify the frame's checksum and epoch.
    payload.resize(payload_size);
    if (payload_size != 0 &&
        !PreadAll(payload.data(), payload_size, offset + kHeaderSize, error)) {
      return IoFailure("pread payload of " + path_, error);
    }

    Crc32c crc;
    crc.Update(header, kCrcCoverage);
    crc.Update(payload.data(), payload.size());
    if (crc.Finish() != stored_crc) {
      stop_reason = "frame checksum mismatch";
      break;
    }

    if (epoch == 0 || (have_frame && epoch < last_epoch)) {
      stop_reason = "frame epoch is zero or regressed";
      break;
    }

    // A full-scan fallback still omits records covered by the checkpoint.
    if (epoch <= min_epoch) {
      ++frames_skipped;
      bytes_skipped += kHeaderSize + payload_size;
      last_epoch = epoch;
      have_frame = true;
      offset = static_cast<off_t>(frame_end);
      continue;
    }

    // Unpack the records and require them to match the frame's epoch.
    LogRecords unpacked;
    try {
      size_t consumed = 0;
      auto handle =
          msgpack::unpack(reinterpret_cast<const char *>(payload.data()),
                          payload.size(), consumed);
      handle.get().convert(unpacked);
      if (consumed != payload.size()) {
        stop_reason = "frame payload has trailing bytes";
        break;
      }
    } catch (const msgpack::unpack_error &e) {
      stop_reason = std::string("frame payload does not unpack: ") + e.what();
      break;
    } catch (const msgpack::type_error &) {
      stop_reason = "frame payload has the wrong type";
      break;
    }
    if (unpacked.empty()) {
      stop_reason = "frame carries no record";
      break;
    }
    const bool epoch_mismatch = std::any_of(
        unpacked.begin(), unpacked.end(),
        [epoch](const LogRecord &record) { return record.epoch != epoch; });
    if (epoch_mismatch) {
      stop_reason = "record epoch disagrees with its frame";
      break;
    }

    // Keep this frame and advance the recovery boundary.
    records.insert(records.end(), std::make_move_iterator(unpacked.begin()),
                   std::make_move_iterator(unpacked.end()));
    last_epoch = epoch;
    have_frame = true;
    offset = static_cast<off_t>(frame_end);
  }

  WalScanResult result;
  result.status = WalScanResult::Status::kOk;
  result.last_epoch = last_epoch;
  result.frames_skipped = frames_skipped;
  result.bytes_skipped = bytes_skipped;

  // Zero only the written suffix after the first invalid frame.
  if (!stop_reason.empty()) {
    int error = 0;
    off_t last_non_zero = 0;
    if (!FindLastNonZero(offset, file_size, last_non_zero, error)) {
      return IoFailure("pread the tail of " + path_, error);
    }
    if (last_non_zero >= offset) {
      if (!WriteZeroesAndSync(offset, last_non_zero + 1, error)) {
        return IoFailure("zero the tail of " + path_, error);
      }
      SPDLOG_WARN(
          "Discarded the tail of {0} at offset {1} ({2}); the "
          "last epoch is {3}",
          path_, static_cast<long long>(offset), stop_reason, last_epoch);
      result.tail_zeroed = true;
    }
  }

  // Publish the recovered prefix and its append position.
  result.records = std::move(records);
  return FinishScan(std::move(result), offset);
}

WalAppendResult Wal::AppendGroup(
    const std::map<EpochNumber, LogRecords> &buckets, EpochNumber target) {
  // Where the log ends is what a successful scan establishes, and an
  // instance that never reached Ready, or that an earlier failure poisoned,
  // has nothing trustworthy to append at.
  if (state_ != State::kReady) {
    SPDLOG_CRITICAL(
        "Durability Error: a group was written to {0} while the end of the "
        "log was not established",
        path_);
    std::abort();
  }

  // Pack the buckets at or below target into one group.
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  const int64_t pack_begin = traced ? FlushTrace::Now() : 0;
  uint32_t packed_epochs = 0;
  std::vector<uint8_t> group;
  EpochNumber last_packed = last_epoch_;
  for (const auto &[epoch, records] : buckets) {
    if (epoch > target) break;
    ++packed_epochs;
    // A bucket the scan would reject is refused before anything is
    // written, which leaves the log's end known and this instance usable.
    if (records.empty() || epoch == 0) return {false, EINVAL};
    if (epoch < last_epoch_) return {false, EINVAL};
    for (const auto &record : records) {
      if (record.epoch != epoch) return {false, EINVAL};
    }
    last_packed = epoch;

    msgpack::sbuffer payload;
    msgpack::pack(payload, records);
    if (payload.size() > kMaxPayloadSize ||
        payload.size() > static_cast<size_t>(UINT32_MAX)) {
      return {false, EOVERFLOW};
    }

    const size_t frame_offset = group.size();
    group.resize(frame_offset + kHeaderSize + payload.size());
    uint8_t *frame = group.data() + frame_offset;
    PutLe32(frame, kMagic);
    PutLe16(frame + kOffFlags, kFlags);
    PutLe32(frame + kOffPayloadSize, static_cast<uint32_t>(payload.size()));
    PutLe32(frame + kOffEpoch, epoch);
    std::memcpy(frame + kHeaderSize, payload.data(), payload.size());

    Crc32c crc;
    crc.Update(frame, kCrcCoverage);
    crc.Update(payload.data(), payload.size());
    PutLe32(frame + kOffCrc, crc.Finish());
  }

  if (traced) {
    trace.GroupPack(pack_begin, FlushTrace::Now(), group.size(), packed_epochs);
  }

  if (group.empty()) return {true, 0};

  // Extend the zeroed region if the group does not fit.
  int error = 0;
  const off_t initialised_before = initialised_size_;
  if (!EnsureCapacityFor(write_offset_, group.size(), error)) {
    state_ = State::kFailed;
    return {false, error};
  }
  if (initialised_size_ != initialised_before) {
    ++extension_count_;
    SPDLOG_INFO("Extended {0} to {1} bytes ({2} extensions so far)", path_,
                static_cast<long long>(initialised_size_), extension_count_);
  }

  // Write, fdatasync, then publish the offset and the last epoch.
  const int64_t write_begin = traced ? FlushTrace::Now() : 0;
  if (!WriteAllAt(group.data(), group.size(), write_offset_, error)) {
    state_ = State::kFailed;
    return {false, error};
  }
  if (traced) trace.GroupWrite(write_begin, FlushTrace::Now());
  // The records are written but not yet known durable: a Sync commit
  // waiting on this group must not have been reported when this point
  // is reached.
  HELIOS_DEBUG_SYNC("wal.before_fdatasync");

  const int64_t sync_begin = traced ? FlushTrace::Now() : 0;
  int rc;
  do {
    rc = io_.fdatasync(fd_);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) {
    const int failure = errno;
    state_ = State::kFailed;
    return {false, failure};
  }
  if (traced) trace.GroupSync(sync_begin, FlushTrace::Now());

  write_offset_ += static_cast<off_t>(group.size());
  // Without preallocation the group carried the file's size with it.
  initialised_size_ = std::max(initialised_size_, write_offset_);
  last_epoch_ = last_packed;
  return {true, 0};
}

}  // namespace wal
}  // namespace helios::storage
