/**
 * @file server/storage/src/wal/wal.cc
 * The log on disk: numbered segment files of epoch frames, the pool that
 * prepares and recycles them, and the startup scan that decides what survived.
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
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <msgpack.hpp>
#include <stdexcept>
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

enum RecordType : uint16_t { kHeaderRecord, kFrame, kPadding, kEndRecord };

// Record header fields, in bytes from the start of the record. The magic word
// opens it at offset 0.
constexpr size_t kOffType = 4;
constexpr size_t kOffSegment = 6;
constexpr size_t kOffOffset = 14;
constexpr size_t kOffPayloadSize = 22;
constexpr size_t kOffEpoch = 26;
constexpr size_t kOffCrc = 30;

// How many segments the worker keeps ready ahead of the logger.
constexpr uint64_t kSpareSegments = 2;

// Alignment of the offset, length and buffer of an O_DIRECT write; it covers
// the 512 and 4096-byte logical blocks NVMe devices report.
constexpr uint64_t kDirectIoAlignment = 4096;

// The header bytes the checksum covers: all of them but the checksum word,
// which sits last.
constexpr size_t kCrcCoverage = Wal::kHeaderSize - sizeof(uint32_t);

// A frame's header followed by its record list's msgpack array header, which
// takes at most 5 bytes.
struct FrameHead {
  uint8_t bytes[Wal::kHeaderSize + 5];
  size_t size;
  uint32_t payload_size;
};

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

void PutLe64(uint8_t *out, uint64_t value) {
  PutLe32(out, static_cast<uint32_t>(value));
  PutLe32(out + 4, static_cast<uint32_t>(value >> 32));
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

uint64_t GetLe64(const uint8_t *in) {
  return static_cast<uint64_t>(GetLe32(in)) |
         (static_cast<uint64_t>(GetLe32(in + 4)) << 32);
}

// Fills in every header field but the checksum.
void PutRecordHeader(uint8_t *out, RecordType type, uint64_t segment_number,
                     uint64_t offset, uint32_t payload_size,
                     EpochNumber epoch) {
  PutLe32(out, Wal::kMagic);
  PutLe16(out + kOffType, type);
  PutLe64(out + kOffSegment, segment_number);
  PutLe64(out + kOffOffset, offset);
  PutLe32(out + kOffPayloadSize, payload_size);
  PutLe32(out + kOffEpoch, epoch);
}

// Builds the header of a record whose payload is `payload_size` bytes at
// `payload`.
void PutCheckedRecord(uint8_t *out, RecordType type, uint64_t segment_number,
                      uint64_t offset, const uint8_t *payload,
                      uint32_t payload_size) {
  PutRecordHeader(out, type, segment_number, offset, payload_size, 0);
  Crc32c crc;
  crc.Update(out, kCrcCoverage);
  crc.Update(payload, payload_size);
  PutLe32(out + kOffCrc, crc.Finish());
}

bool SetDirectIo(int fd, int &error) {
  const int flags = ::fcntl(fd, F_GETFL);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_DIRECT) != 0) {
    error = errno;
    return false;
  }
  return true;
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

// Closes a descriptor on every path out of a scope.
struct ScopedFd {
  explicit ScopedFd(int descriptor) : fd(descriptor) {}
  ~ScopedFd() {
    if (fd >= 0) ::close(fd);
  }
  ScopedFd(const ScopedFd &) = delete;
  ScopedFd &operator=(const ScopedFd &) = delete;
  int fd;
};

// The segment number a directory entry with this suffix names, or 0.
uint64_t SegmentNumberOf(const std::string &name, const char *suffix) {
  uint64_t number = 0;
  int consumed = 0;
  if (std::sscanf(name.c_str(), "wal-%" SCNu64 "%n", &number, &consumed) != 1 ||
      name.compare(consumed, std::string::npos, suffix) != 0) {
    return 0;
  }
  return number;
}

}  // namespace

WalIo WalIo::Posix() {
  WalIo io;
  io.pwritev = ::pwritev;
  io.initialise_pwrite = ::pwrite;
  io.pread = ::pread;

  // Armed from the environment, like a debug sync point, so an out-of-process
  // test can arrange an EIO. Unset means the bare syscall.
  const char *raw = std::getenv("HELIOS_WAL_FDATASYNC_FAIL_AFTER");
  if (raw == nullptr) {
    io.fdatasync = ::fdatasync;
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

std::string Wal::SegmentFileName(uint64_t segment_number) {
  char name[64];
  std::snprintf(name, sizeof(name), "wal-%016" PRIu64 ".log", segment_number);
  return name;
}

std::string Wal::SegmentPath(uint64_t segment_number) const {
  return directory_ + "/" + SegmentFileName(segment_number);
}

std::string Wal::TemporaryPath(uint64_t segment_number) const {
  char name[64];
  std::snprintf(name, sizeof(name), "/wal-%016" PRIu64 ".tmp", segment_number);
  return directory_ + name;
}

Wal::Wal(const std::string &work_dir, WalIo io, uint64_t segment_bytes,
         bool direct_io)
    : io_(std::move(io)),
      segment_bytes_(segment_bytes),
      spare_target_bytes_(kSpareSegments * segment_bytes),
      direct_io_(direct_io) {
  // Resolve the path and create the directory.
  // Strip a trailing separator first; parent_path() below must name the
  // true parent, not the directory itself.
  std::filesystem::path directory(work_dir);
  if (!directory.has_filename()) directory = directory.parent_path();
  directory_ = directory.string();

  std::error_code ec;
  std::filesystem::create_directory(directory, ec);
  if (ec) {
    throw std::system_error(ec, "create_directory " + work_dir);
  }

  // Segments come and go under their names, so the lock is on the directory.
  // Writing in place puts the offsets under this process's control: a second
  // process holding the same log would overwrite frames, not interleave.
  directory_fd_ =
      ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd_ < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "open " + directory_);
  }
  if (::flock(directory_fd_, LOCK_EX | LOCK_NB) != 0) {
    const int error = errno;
    ::close(directory_fd_);
    directory_fd_ = -1;
    throw std::system_error(error, std::generic_category(),
                            "lock " + directory_ + " exclusively");
  }

  // Every name on the way to the log is made durable here, whether or not
  // this process is the one that created it.
  const std::string parent =
      directory.has_parent_path() ? directory.parent_path().string() : ".";
  int error = 0;
  struct stat directory_stat {};
  if (!FsyncDirectory(directory_, error) || !FsyncDirectory(parent, error) ||
      ::fstat(directory_fd_, &directory_stat) != 0) {
    if (error == 0) error = errno;
    ::close(directory_fd_);
    directory_fd_ = -1;
    throw std::system_error(error, std::generic_category(),
                            "fsync the directories holding " + directory_);
  }
  block_bytes_ = static_cast<uint64_t>(directory_stat.st_blksize);
  padding_.resize(block_bytes_);

  // Room for the header block, a frame and the block the end record needs.
  if (segment_bytes_ % block_bytes_ != 0 || segment_bytes_ < 4 * block_bytes_) {
    ::close(directory_fd_);
    directory_fd_ = -1;
    throw std::invalid_argument("segment size");
  }
  // Groups start and end on blocks, which O_DIRECT then sees aligned.
  if (direct_io_ && block_bytes_ % kDirectIoAlignment != 0) {
    ::close(directory_fd_);
    directory_fd_ = -1;
    throw std::invalid_argument("filesystem block for O_DIRECT");
  }
}

Wal::~Wal() {
  for (const auto &segment : ready_) ::close(segment.fd);
  if (active_.fd >= 0) ::close(active_.fd);
  if (directory_fd_ >= 0) ::close(directory_fd_);
}

WalScanResult Wal::IoFailure(const std::string &operation, int error) {
  state_ = State::kFailed;
  WalScanResult result;
  result.status = WalScanResult::Status::kIoError;
  result.error_number = error;
  result.detail = operation;
  return result;
}

// A padding record needs room for its own header, so a gap smaller than that
// runs on to the end of the next block.
uint64_t Wal::PaddedEnd(uint64_t end) const {
  const uint64_t gap = -end & (block_bytes_ - 1);
  if (gap == 0) return end;
  return end + gap + (gap < kHeaderSize ? block_bytes_ : 0);
}

// Writes every byte `iov` names at `offset`, at most IOV_MAX iovecs a call; a
// short write resumes inside the iovec it stopped in, which `iov` records.
bool Wal::WriteAllAt(std::vector<iovec> &iov, off_t offset, int &error) {
  size_t next = 0;
  while (next != iov.size()) {
    const auto count =
        static_cast<int>(std::min<size_t>(iov.size() - next, IOV_MAX));
    const ssize_t written =
        io_.pwritev(active_.fd, iov.data() + next, count, offset);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) {
      error = written == 0 ? EIO : errno;
      return false;
    }

    offset += written;
    auto left = static_cast<size_t>(written);
    while (next != iov.size() && left >= iov[next].iov_len) {
      left -= iov[next].iov_len;
      ++next;
    }
    if (left != 0) {
      iov[next].iov_base = static_cast<uint8_t *>(iov[next].iov_base) + left;
      iov[next].iov_len -= left;
    }
  }
  return true;
}

bool Wal::PreadAll(int fd, uint8_t *out, size_t size, off_t offset,
                   int &error) const {
  while (size != 0) {
    const ssize_t got = io_.pread(fd, out, size, offset);
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

bool Wal::WriteAt(int fd, const uint8_t *data, size_t size, off_t offset,
                  int &error) {
  while (size != 0) {
    const ssize_t written = io_.initialise_pwrite(fd, data, size, offset);
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

// Writes out zeroes over `[from, to)` and persists them. For a fresh segment
// this moves size, allocation and extent-state metadata work out of the group
// flush; for a repair it removes records a shorter group would leave reachable.
bool Wal::WriteZeroesAndSync(int fd, off_t from, off_t to, int &error) {
  constexpr size_t kChunkSize = 1ull << 20;
  const std::vector<uint8_t> zeroes(kChunkSize, 0);
  for (off_t offset = from; offset < to;) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, to - offset));
    if (!WriteAt(fd, zeroes.data(), size, offset, error)) return false;
    offset += static_cast<off_t>(size);
  }

  if (Fsync(fd) != 0) {
    error = errno;
    return false;
  }
  return true;
}

// Reports the offset of the last byte in `[from, to)` that is not zero, or
// `from - 1` when every byte is.
bool Wal::FindLastNonZero(int fd, off_t from, off_t to, off_t &last_non_zero,
                          int &error) const {
  constexpr size_t kChunkSize = 1ull << 20;
  std::vector<uint8_t> chunk(kChunkSize);
  last_non_zero = from - 1;

  for (off_t at = from; at < to; at += kChunkSize) {
    const size_t size =
        static_cast<size_t>(std::min<off_t>(kChunkSize, to - at));
    if (!PreadAll(fd, chunk.data(), size, at, error)) return false;
    for (size_t i = size; i > 0; --i) {
      if (chunk[i - 1] != 0) {
        last_non_zero = at + static_cast<off_t>(i) - 1;
        break;
      }
    }
  }
  return true;
}

bool Wal::Fdatasync(int fd, int &error) {
  int rc;
  do {
    rc = io_.fdatasync(fd);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0) {
    error = errno;
    return false;
  }
  return true;
}

// Reads and checks the record at `offset`: its magic, the segment and offset it
// names, its bounds, and its checksum. False means an I/O failure.
bool Wal::ReadRecord(int fd, uint64_t segment_number, uint64_t offset,
                     std::vector<uint8_t> &payload, Record &record,
                     int &error) const {
  if (segment_bytes_ - offset < kHeaderSize) {
    record.invalid = "the segment ends inside a record header";
    return true;
  }
  uint8_t header[kHeaderSize];
  if (!PreadAll(fd, header, kHeaderSize, static_cast<off_t>(offset), error)) {
    return false;
  }

  const uint32_t payload_size = GetLe32(header + kOffPayloadSize);
  // The checks that need no payload come first: a stale header's length means
  // nothing, and must not cause a read.
  if (GetLe32(header) != kMagic) {
    record.invalid = "record magic mismatch";
  } else if (GetLe64(header + kOffSegment) != segment_number) {
    record.invalid = "record of another segment";
  } else if (GetLe64(header + kOffOffset) != offset) {
    record.invalid = "record of another offset";
  } else if (payload_size > kMaxPayloadSize ||
             payload_size > segment_bytes_ - offset - kHeaderSize) {
    record.invalid = "record payload past the segment";
  }
  if (record.invalid != nullptr) return true;

  payload.resize(payload_size);
  if (payload_size != 0 &&
      !PreadAll(fd, payload.data(), payload_size,
                static_cast<off_t>(offset + kHeaderSize), error)) {
    return false;
  }
  Crc32c crc;
  crc.Update(header, kCrcCoverage);
  crc.Update(payload.data(), payload.size());
  if (crc.Finish() != GetLe32(header + kOffCrc)) {
    record.invalid = "record checksum mismatch";
    return true;
  }
  record.type = GetLe16(header + kOffType);
  record.epoch = GetLe32(header + kOffEpoch);
  return true;
}

bool Wal::WritePadding(int fd, uint64_t segment_number, uint64_t offset,
                       int &error) {
  const uint64_t end = PaddedEnd(offset);
  std::vector<uint8_t> record(end - offset, 0);
  PutCheckedRecord(record.data(), kPadding, segment_number, offset,
                   record.data() + kHeaderSize,
                   static_cast<uint32_t>(end - offset - kHeaderSize));
  if (!WriteAt(fd, record.data(), record.size(), static_cast<off_t>(offset),
               error)) {
    return false;
  }
  if (Fsync(fd) != 0) {
    error = errno;
    return false;
  }
  return true;
}

// The last step of the first successful scan. A new group must not leave any
// earlier group's record reachable, and only then may work be accepted.
WalScanResult Wal::FinishScan(WalScanResult &&result, bool fresh,
                              const WalPosition &start, const WalPosition &tail,
                              const std::vector<uint64_t> &segments,
                              const std::vector<std::string> &temporaries) {
  int error = 0;
  Segment active{-1, tail.segment_number, tail.offset};
  if (!fresh) {
    active.fd =
        ::open(SegmentPath(tail.segment_number).c_str(), O_RDWR | O_CLOEXEC);
    if (active.fd < 0) {
      return IoFailure("open " + SegmentPath(tail.segment_number), errno);
    }
    active_.fd = active.fd;

    // Zero what follows the valid prefix.
    const auto from = static_cast<off_t>(tail.offset);
    off_t last_non_zero = 0;
    if (!FindLastNonZero(active.fd, from, static_cast<off_t>(segment_bytes_),
                         last_non_zero, error)) {
      return IoFailure("pread the tail of " + SegmentPath(tail.segment_number),
                       error);
    }
    if (last_non_zero >= from) {
      if (!WriteZeroesAndSync(active.fd, from, last_non_zero + 1, error)) {
        return IoFailure("zero the tail of " + SegmentPath(tail.segment_number),
                         error);
      }
      result.tail_zeroed = true;
    }

    // A group that tore can leave frames ending mid-block. They count as
    // durable now, so the next group starts in a block of its own.
    if (PaddedEnd(tail.offset) != tail.offset) {
      if (!WritePadding(active.fd, tail.segment_number, tail.offset, error)) {
        return IoFailure("pad the tail of " + SegmentPath(tail.segment_number),
                         error);
      }
      active.append_offset = PaddedEnd(tail.offset);
    }

    // A process crash leaves unsynced groups readable, and the scan counted
    // them as durable. A segment is written only after the one before it
    // synced, so only the tail and its predecessor can hold such a group.
    if (!Fdatasync(active.fd, error)) {
      return IoFailure("fdatasync " + SegmentPath(tail.segment_number), error);
    }
    if (tail.segment_number != start.segment_number) {
      const std::string path = SegmentPath(tail.segment_number - 1);
      const ScopedFd previous(::open(path.c_str(), O_RDWR | O_CLOEXEC));
      if (previous.fd < 0) return IoFailure("open " + path, errno);
      if (!Fdatasync(previous.fd, error)) {
        return IoFailure("fdatasync " + path, error);
      }
    }
  }

  // Sort the other files out, then make the deletions durable.
  for (const uint64_t number : segments) {
    if (number < start.segment_number) covered_.push_back(number);
    if (number > tail.segment_number &&
        ::unlink(SegmentPath(number).c_str()) != 0) {
      return IoFailure("unlink " + SegmentPath(number), errno);
    }
  }
  for (const auto &temporary : temporaries) {
    if (::unlink(temporary.c_str()) != 0) {
      return IoFailure("unlink " + temporary, errno);
    }
  }
  if (!FsyncDirectory(directory_, error)) {
    return IoFailure("fsync " + directory_, error);
  }

  // Prepare the active segment of a new log, then the spare.
  next_segment_ = fresh ? 1 : tail.segment_number + 1;
  oldest_retained_ = start.segment_number;
  if (fresh) {
    if (!AcquireReady(active, error)) {
      return IoFailure("prepare the first segment of " + directory_, error);
    }
    active_.fd = active.fd;
  }
  // The repairs above went through the page cache; appends bypass it.
  if (direct_io_ && !SetDirectIo(active.fd, error)) {
    return IoFailure("set O_DIRECT on " + SegmentPath(active.segment_number),
                     error);
  }
  if (!Prepare(false, error)) {
    return IoFailure("prepare a segment of " + directory_, error);
  }

  active_ = active;
  durable_end_ = {active.segment_number, active.append_offset};
  last_epoch_ = result.last_epoch;
  state_ = State::kReady;
  return std::move(result);
}

WalScanResult Wal::Scan(EpochNumber min_epoch, const Replay &replay,
                        WalPosition start) {
  // Refuse a scan on an instance that already failed.
  if (state_ == State::kFailed) {
    return IoFailure("scan " + directory_ + " after a failure", EIO);
  }

  // List what the directory holds.
  std::vector<uint64_t> segments;
  std::vector<std::string> temporaries;
  std::error_code ec;
  for (const auto &entry :
       std::filesystem::directory_iterator(directory_, ec)) {
    const std::string name = entry.path().filename().string();
    const uint64_t number = SegmentNumberOf(name, ".log");
    if (number != 0) segments.push_back(number);
    if (SegmentNumberOf(name, ".tmp") != 0) {
      temporaries.push_back(entry.path().string());
    }
  }
  if (ec) return IoFailure("list " + directory_, ec.value());

  const bool fresh =
      segments.empty() && start.segment_number == 1 && start.offset == 0;

  WalScanResult result;
  WalPosition tail = start;
  EpochNumber last_epoch = 0;
  std::vector<uint8_t> payload;
  std::string stop_reason;

  // Follow the chain of segments from the start through valid end records.
  for (bool next_segment = !fresh; next_segment;) {
    next_segment = false;
    const std::string path = SegmentPath(tail.segment_number);
    const ScopedFd file(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
    if (file.fd < 0) return IoFailure("open " + path, errno);
    struct stat file_stat {};
    if (::fstat(file.fd, &file_stat) < 0) {
      return IoFailure("fstat " + path, errno);
    }
    if (static_cast<uint64_t>(file_stat.st_size) != segment_bytes_) {
      return IoFailure("the size of " + path, EINVAL);
    }

    // A segment reaches the log only with a durable header.
    int error = 0;
    Record header;
    if (!ReadRecord(file.fd, tail.segment_number, 0, payload, header, error)) {
      return IoFailure("pread the header of " + path, error);
    }
    if (header.invalid != nullptr || header.type != kHeaderRecord) {
      return IoFailure("the header of " + path, EIO);
    }
    tail.offset = std::max<uint64_t>(tail.offset, kHeaderSize + payload.size());

    // Read records until the first invalid one or the end record.
    while (stop_reason.empty() && !next_segment) {
      Record record;
      if (!ReadRecord(file.fd, tail.segment_number, tail.offset, payload,
                      record, error)) {
        return IoFailure("pread " + path, error);
      }
      if (record.invalid != nullptr) {
        stop_reason = record.invalid;
        break;
      }
      const uint64_t record_size = kHeaderSize + payload.size();
      if (record.type == kPadding) {
        tail.offset += record_size;
        continue;
      }
      if (record.type == kEndRecord) {
        next_segment = true;
        continue;
      }
      if (record.type != kFrame) {
        stop_reason = "unexpected record type";
        break;
      }

      if (record.epoch <= last_epoch) {
        stop_reason = "frame epoch did not increase";
        break;
      }

      // Already covered by the checkpoint; nothing here needs replaying.
      if (record.epoch <= min_epoch) {
        ++result.frames_skipped;
        result.bytes_skipped += record_size;
        last_epoch = record.epoch;
        tail.offset += record_size;
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
      const EpochNumber epoch = record.epoch;
      const bool epoch_mismatch = std::any_of(
          unpacked.begin(), unpacked.end(),
          [epoch](const LogRecord &log) { return log.epoch != epoch; });
      if (epoch_mismatch) {
        stop_reason = "record epoch disagrees with its frame";
        break;
      }

      if (replay) replay(unpacked);
      last_epoch = epoch;
      tail.offset += record_size;
    }

    if (next_segment) {
      ++tail.segment_number;
      tail.offset = 0;
    }
  }

  result.status = WalScanResult::Status::kOk;
  result.last_epoch = last_epoch;

  // A later scan of a running log repairs nothing.
  if (state_ == State::kReady) return result;

  SPDLOG_INFO(
      "The log ends at segment {0} offset {1} ({2}); the last epoch is {3}",
      tail.segment_number, tail.offset, stop_reason, last_epoch);
  return FinishScan(std::move(result), fresh, start, tail, segments,
                    temporaries);
}

// Builds a segment under its temporary name, then publishes the numbered name.
// Until the directory sync is done nothing refers to the segment, and a crash
// at any step leaves a file the next start deletes or a covered one.
bool Wal::FormatSegment(uint64_t segment_number, uint64_t recycled, int &fd,
                        int &error) {
  const std::string temporary = TemporaryPath(segment_number);
  if (recycled != 0) {
    HELIOS_DEBUG_SYNC("wal.prepare.before_temp_rename");
    if (::rename(SegmentPath(recycled).c_str(), temporary.c_str()) != 0) {
      error = errno;
      return false;
    }
    HELIOS_DEBUG_SYNC("wal.prepare.after_temp_rename");
    fd = ::open(temporary.c_str(), O_RDWR | O_CLOEXEC);
    if (fd < 0) {
      error = errno;
      return false;
    }
  } else {
    fd =
        ::open(temporary.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
      error = errno;
      return false;
    }
    if (!WriteZeroesAndSync(fd, 0, static_cast<off_t>(segment_bytes_), error)) {
      return false;
    }
  }

  // The header fills the first block, so frames start on a block of their own.
  std::vector<uint8_t> header(block_bytes_, 0);
  PutCheckedRecord(header.data(), kHeaderRecord, segment_number, 0,
                   header.data() + kHeaderSize,
                   static_cast<uint32_t>(block_bytes_ - kHeaderSize));
  if (!WriteAt(fd, header.data(), header.size(), 0, error)) return false;
  HELIOS_DEBUG_SYNC("wal.prepare.after_header_write");
  if (Fsync(fd) != 0) {
    error = errno;
    return false;
  }

  HELIOS_DEBUG_SYNC("wal.prepare.before_rename");
  if (::rename(temporary.c_str(), SegmentPath(segment_number).c_str()) != 0) {
    error = errno;
    return false;
  }
  HELIOS_DEBUG_SYNC("wal.prepare.before_directory_sync");
  if (!FsyncDirectory(directory_, error)) return false;
  HELIOS_DEBUG_SYNC("wal.prepare.after_directory_sync");
  return true;
}

bool Wal::PrepareSegment() {
  int error = 0;
  return Prepare(false, error);
}

// A preparation for a miss fills the pool up to one segment, any other up to
// the spare target. Preparations run one at a time, and a miss takes the first
// segment the one under way pushes.
bool Wal::Prepare(bool miss, int &error) {
  Segment segment;
  uint64_t recycled = 0;
  {
    std::unique_lock<std::mutex> lock(pool_mutex_);
    while (preparing_ && !(miss && !ready_.empty())) pool_cv_.wait(lock);
    if (!miss && prepare_error_ != 0) {
      error = prepare_error_;
      return false;
    }
    const uint64_t target = miss ? segment_bytes_ : spare_target_bytes_;
    if (ready_.size() * segment_bytes_ >= target) return true;
    preparing_ = true;
    segment.segment_number = next_segment_++;
    if (!covered_.empty()) {
      recycled = covered_.front();
      covered_.pop_front();
    }
  }

  const bool formatted =
      FormatSegment(segment.segment_number, recycled, segment.fd, error);
  if (!formatted && segment.fd >= 0) ::close(segment.fd);
  {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (formatted) {
      segment.append_offset = block_bytes_;
      ready_.push_back(segment);
      prepare_error_ = 0;
    } else {
      // The next preparation takes the same number, so segments stay
      // consecutive.
      next_segment_ = segment.segment_number;
      prepare_error_ = error;
    }
    preparing_ = false;
  }
  pool_cv_.notify_all();
  return formatted;
}

// Takes the next ready segment, which is durable under its numbered name, and
// prepares one on a miss. Preparation recycles only covered segments, so
// nothing here can overwrite what recovery still needs.
bool Wal::AcquireReady(Segment &next, int &error) {
  std::unique_lock<std::mutex> lock(pool_mutex_);
  while (ready_.empty()) {
    lock.unlock();
    HELIOS_DEBUG_SYNC("wal.pool_miss");
    if (!Prepare(true, error)) return false;
    lock.lock();
  }
  next = ready_.front();
  ready_.pop_front();
  lock.unlock();
  pool_cv_.notify_all();
  return true;
}

void Wal::MarkCovered(uint64_t redo_segment) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  for (; oldest_retained_ < redo_segment; ++oldest_retained_) {
    covered_.push_back(oldest_retained_);
  }
}

Wal::WorkerTask Wal::WaitForWork() {
  std::unique_lock<std::mutex> lock(pool_mutex_);
  for (;;) {
    if (worker_stopped_) return WorkerTask::kStop;
    // A due checkpoint comes first while a segment is ready: only a checkpoint
    // frees segments, and zeroing fresh ones cannot outrun a saturated device.
    const bool checkpoint_due = appended_bytes_ >= checkpoint_threshold_;
    if (checkpoint_due && !ready_.empty()) return WorkerTask::kCheckpoint;
    if (prepare_error_ == 0 &&
        ready_.size() * segment_bytes_ < spare_target_bytes_) {
      return WorkerTask::kPrepare;
    }
    if (checkpoint_due) return WorkerTask::kCheckpoint;
    pool_cv_.wait(lock);
  }
}

void Wal::StopWorker() {
  {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    worker_stopped_ = true;
  }
  pool_cv_.notify_all();
}

WalPosition Wal::durable_end() {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  return durable_end_;
}

uint64_t Wal::appended_bytes() {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  return appended_bytes_;
}

void Wal::set_checkpoint_threshold(uint64_t appended_bytes) {
  std::lock_guard<std::mutex> lock(pool_mutex_);
  checkpoint_threshold_ = appended_bytes;
}

// Publishes where the log ends after an fdatasync, and wakes the worker when
// that makes a checkpoint due.
void Wal::PublishEnd(const WalPosition &end, uint64_t written) {
  bool due;
  {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    durable_end_ = end;
    appended_bytes_ += written;
    due = appended_bytes_ >= checkpoint_threshold_;
  }
  if (due) pool_cv_.notify_all();
}

// Writes `iov` at `offset` and persists it: nothing may report it durable
// before this returns.
bool Wal::WriteAndSync(std::vector<iovec> &iov, uint64_t offset, int &error) {
  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  const int64_t write_begin = traced ? FlushTrace::Now() : 0;
  // O_DIRECT takes the group as one aligned buffer. Zeroes pad it to a
  // multiple of the alignment, which only an end record leaves short.
  if (direct_io_) {
    size_t size = 0;
    for (const iovec &part : iov) size += part.iov_len;
    const size_t padded = (size + kDirectIoAlignment - 1) & -kDirectIoAlignment;
    if (direct_buffer_.size() < padded + kDirectIoAlignment) {
      direct_buffer_.resize(padded + kDirectIoAlignment);
    }
    auto *buffer = reinterpret_cast<uint8_t *>(
        (reinterpret_cast<uintptr_t>(direct_buffer_.data()) +
         kDirectIoAlignment - 1) &
        -kDirectIoAlignment);
    uint8_t *out = buffer;
    for (const iovec &part : iov) {
      std::memcpy(out, part.iov_base, part.iov_len);
      out += part.iov_len;
    }
    std::memset(out, 0, padded - size);
    iov.assign(1, {buffer, padded});
  }
  if (!WriteAllAt(iov, static_cast<off_t>(offset), error)) return false;
  if (traced) trace.GroupWrite(write_begin, FlushTrace::Now());
  // The records are written but not yet known durable: a Sync commit
  // waiting on this group must not have been reported when this point
  // is reached.
  HELIOS_DEBUG_SYNC("wal.before_fdatasync");

  const int64_t sync_begin = traced ? FlushTrace::Now() : 0;
  if (!Fdatasync(active_.fd, error)) return false;
  if (traced) trace.GroupSync(sync_begin, FlushTrace::Now());
  return true;
}

// Closes the active segment after the frames in `iov` and moves to the next.
// The next segment is taken first, so a durable end record always leads to a
// segment, and the old one is synced before anything lands in its successor.
bool Wal::Rotate(std::vector<iovec> &iov, size_t group_size, int &error) {
  Segment next;
  if (!AcquireReady(next, error)) return false;
  if (direct_io_ && !SetDirectIo(next.fd, error)) {
    ::close(next.fd);
    return false;
  }
  HELIOS_DEBUG_SYNC("wal.rotate.before_end_record");

  uint8_t end_record[kHeaderSize];
  PutCheckedRecord(end_record, kEndRecord, active_.segment_number,
                   active_.append_offset + group_size, nullptr, 0);
  iov.push_back({end_record, kHeaderSize});
  if (!WriteAndSync(iov, active_.append_offset, error)) {
    ::close(next.fd);
    return false;
  }
  HELIOS_DEBUG_SYNC("wal.rotate.after_end_sync");

  ::close(active_.fd);
  active_ = next;
  PublishEnd({active_.segment_number, active_.append_offset},
             group_size + kHeaderSize);
  return true;
}

WalAppendResult Wal::AppendGroup(
    const std::map<EpochNumber, PackedLogRecords> &buckets,
    EpochNumber target) {
  // Where the log ends is what a successful scan establishes, and an
  // instance that never reached Ready, or that an earlier failure poisoned,
  // has nothing trustworthy to append at.
  if (state_ != State::kReady) {
    SPDLOG_CRITICAL(
        "Durability Error: a group was written to {0} while the end of the "
        "log was not established",
        directory_);
    std::abort();
  }

  auto &trace = FlushTrace::Instance();
  const bool traced = trace.Enabled();
  const int64_t pack_begin = traced ? FlushTrace::Now() : 0;

  // A bucket the scan would reject is refused before anything is written, which
  // leaves the log's end known and this instance usable.
  // The iovecs point into `heads`, which therefore never reallocates.
  std::vector<FrameHead> heads;
  heads.reserve(buckets.size());
  EpochNumber last_packed = last_epoch_;
  for (const auto &[epoch, records] : buckets) {
    if (epoch > target) break;
    if (records.empty() || epoch == 0) return {false, EINVAL};
    if (epoch <= last_epoch_) return {false, EINVAL};
    size_t records_size = 0;
    for (const auto &record : records) {
      if (record.epoch != epoch) return {false, EINVAL};
      records_size += record.bytes.size();
    }
    last_packed = epoch;

    // The payload is the packing of the bucket as a record list: the list's
    // array header, then each record's bytes as the producer packed them.
    msgpack::sbuffer list_header(8);
    msgpack::packer<msgpack::sbuffer>(list_header)
        .pack_array(static_cast<uint32_t>(records.size()));
    const size_t payload_size = list_header.size() + records_size;
    if (payload_size > kMaxPayloadSize ||
        PaddedEnd(block_bytes_ + kHeaderSize + payload_size) + block_bytes_ >
            segment_bytes_) {
      return {false, EOVERFLOW};
    }

    FrameHead &head = heads.emplace_back();
    std::memcpy(head.bytes + kHeaderSize, list_header.data(),
                list_header.size());
    head.size = kHeaderSize + list_header.size();
    head.payload_size = static_cast<uint32_t>(payload_size);
  }
  if (heads.empty()) return {true, 0};

  // Frame the buckets, leaving a segment whenever the next whole frame, its
  // group's padding and the block held for the end record would not fit.
  int error = 0;
  std::vector<iovec> iov;
  size_t group_size = 0;
  size_t next_head = 0;
  uint32_t packed_epochs = 0;
  for (const auto &[epoch, records] : buckets) {
    if (epoch > target) break;
    ++packed_epochs;
    FrameHead &head = heads[next_head++];
    const size_t frame_size = kHeaderSize + head.payload_size;
    if (PaddedEnd(active_.append_offset + group_size + frame_size) +
            block_bytes_ >
        segment_bytes_) {
      if (!Rotate(iov, group_size, error)) {
        state_ = State::kFailed;
        return {false, error};
      }
      iov.clear();
      group_size = 0;
    }

    uint8_t *frame = head.bytes;
    PutRecordHeader(frame, kFrame, active_.segment_number,
                    active_.append_offset + group_size, head.payload_size,
                    epoch);

    // The checksum runs over the bytes the iovecs name, in their order.
    Crc32c crc;
    crc.Update(frame, kCrcCoverage);
    crc.Update(frame + kHeaderSize, head.size - kHeaderSize);
    iov.push_back({frame, head.size});
    for (const auto &record : records) {
      crc.Update(record.bytes.data(), record.bytes.size());
      iov.push_back(
          {const_cast<char *>(record.bytes.data()), record.bytes.size()});
    }
    PutLe32(frame + kOffCrc, crc.Finish());
    group_size += frame_size;
  }

  // End the group on a block boundary.
  uint8_t padding_head[kHeaderSize];
  const uint64_t group_end = active_.append_offset + group_size;
  const uint64_t padded_end = PaddedEnd(group_end);
  if (padded_end != group_end) {
    const auto padding_size =
        static_cast<uint32_t>(padded_end - group_end - kHeaderSize);
    PutCheckedRecord(padding_head, kPadding, active_.segment_number, group_end,
                     padding_.data(), padding_size);
    iov.push_back({padding_head, kHeaderSize});
    iov.push_back({padding_.data(), padding_size});
  }
  if (traced) {
    trace.GroupPack(pack_begin, FlushTrace::Now(), group_size, packed_epochs);
  }

  // Write, fdatasync, then publish the offset and the last epoch.
  if (!WriteAndSync(iov, active_.append_offset, error)) {
    state_ = State::kFailed;
    return {false, error};
  }
  const uint64_t written = padded_end - active_.append_offset;
  active_.append_offset = padded_end;
  last_epoch_ = last_packed;
  PublishEnd({active_.segment_number, active_.append_offset}, written);
  return {true, 0};
}

}  // namespace wal
}  // namespace helios::storage
