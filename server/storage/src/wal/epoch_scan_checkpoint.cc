/**
 * @file server/storage/src/wal/epoch_scan_checkpoint.cc
 * The checkpoint of the live rows, scanned while transactions keep running and
 * merged with the log at recovery, and the worker that takes it and keeps the
 * log's segment pool filled.
 */

#include "wal/epoch_scan_checkpoint.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <xmmintrin.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <msgpack.hpp>
#include <string>
#include <utility>
#include <vector>

#include "index/index_record.h"
#include "index/masstree_index.h"
#include "index/secondary_index.h"
#include "table/table.h"
#include "table/table_dictionary.h"
#include "util/debug_sync.h"
#include "util/epoch_framework.h"
#include "util/spdlog.h"
#include "wal/crc32c.h"
#include "wal/logger.h"

namespace helios::storage {
namespace wal {

namespace {

using Clock = std::chrono::steady_clock;

// How many times one row is re-read before it is set aside for the retry
// pass: a short spin covers a writer's install, and a longer wait belongs to
// the pass that runs without holding up the rest of the table.
constexpr unsigned kSpinAttempts = 64;
// The retry pass gives up eventually rather than scanning forever, because a
// row that never settles means the checkpoint cannot be written at all.
constexpr unsigned kRetryRounds = 200;
constexpr auto kRetryPause = std::chrono::milliseconds(25);
// The durable epoch advances once per epoch, so a wait beyond this means the
// logger is not running rather than that the epoch is slow.
constexpr auto kDurabilityWait = std::chrono::seconds(60);
// How soon a durability wait looks at the segment pool again.
constexpr auto kPoolPoll = std::chrono::milliseconds(1);
constexpr uint64_t kRowsPerPoolCheck = 4096;

// Checkpoint header fields, in bytes from the start of the file. The magic word
// opens it at offset 0 and the header checksum closes it at `kHeaderSize - 4`.
// The chunks follow, and the payload size counts them with their prefixes.
constexpr size_t kOffFlags = 4;
constexpr size_t kOffGeneration = 6;
constexpr size_t kOffStartEpoch = 14;
constexpr size_t kOffEndEpoch = 18;
constexpr size_t kOffPrimaryRows = 22;
constexpr size_t kOffSecondaryEntries = 30;
constexpr size_t kOffPayloadSize = 38;
constexpr size_t kOffRedoSegment = 46;
constexpr size_t kOffRedoOffset = 54;
constexpr size_t kChunkPrefixSize = 8;

void PutLe16(uint8_t *out, uint16_t value) {
  out[0] = static_cast<uint8_t>(value & 0xffu);
  out[1] = static_cast<uint8_t>((value >> 8) & 0xffu);
}

void PutLe32(uint8_t *out, uint32_t value) {
  for (size_t i = 0; i < 4; ++i) {
    out[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xffu);
  }
}

void PutLe64(uint8_t *out, uint64_t value) {
  for (size_t i = 0; i < 8; ++i) {
    out[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xffu);
  }
}

uint16_t GetLe16(const uint8_t *in) {
  return static_cast<uint16_t>(static_cast<uint16_t>(in[0]) |
                               static_cast<uint16_t>(in[1] << 8));
}

uint32_t GetLe32(const uint8_t *in) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(in[i]) << (8 * i);
  }
  return value;
}

uint64_t GetLe64(const uint8_t *in) {
  uint64_t value = 0;
  for (size_t i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(in[i]) << (8 * i);
  }
  return value;
}

int64_t ElapsedMs(Clock::time_point from) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                               from)
      .count();
}

bool WriteAll(int fd, const void *data, size_t size, off_t offset) {
  const auto *bytes = static_cast<const uint8_t *>(data);
  while (size != 0) {
    const ssize_t written = ::pwrite(fd, bytes, size, offset);
    if (written > 0) {
      bytes += written;
      offset += written;
      size -= static_cast<size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

bool ReadAll(int fd, void *data, size_t size, off_t offset) {
  auto *bytes = static_cast<uint8_t *>(data);
  while (size != 0) {
    const ssize_t got = ::pread(fd, bytes, size, offset);
    if (got > 0) {
      bytes += got;
      offset += got;
      size -= static_cast<size_t>(got);
      continue;
    }
    if (got < 0 && errno == EINTR) continue;
    return false;
  }
  return true;
}

/**
 * @brief Closes a descriptor on every path out of a function, including the
 *        one an allocation throws through.
 */
struct ScopedFd {
  explicit ScopedFd(int descriptor) : fd(descriptor) {}
  ~ScopedFd() {
    if (fd >= 0) ::close(fd);
  }
  ScopedFd(const ScopedFd &) = delete;
  ScopedFd &operator=(const ScopedFd &) = delete;
  int fd;
};

int Fsync(int fd) {
  int rc;
  do {
    rc = ::fsync(fd);
  } while (rc < 0 && errno == EINTR);
  return rc;
}

// A file's own fsync does not make its name durable, and the name is what the
// rename publishes.
bool FsyncDirectory(const std::string &directory) {
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) return false;
  const bool ok = Fsync(fd) == 0;
  const int fsync_errno = errno;
  ::close(fd);
  // The caller reports errno, and close is free to overwrite it.
  if (!ok) errno = fsync_errno;
  return ok;
}

// A checkpoint that is present but cannot be trusted.
EpochScanCheckpoint::LoadResult unusable(const std::string &detail) {
  EpochScanCheckpoint::LoadResult checkpoint;
  checkpoint.status = EpochScanCheckpoint::LoadResult::Status::kUnusable;
  checkpoint.detail = detail;
  return checkpoint;
}

bool refuse(std::string *detail, const std::string &reason) {
  *detail = reason;
  return false;
}

}  // namespace

/**
 * @brief Gathers a capture's writes into chunks of the working file, each
 *        `[u32 payload_len][u32 crc32c of payload][payload]`.
 *
 * @details The payload is one packed LogRecord. The descriptor stays owned by
 * the caller, and `off` is where the next chunk goes.
 */
struct EpochScanCheckpoint::ChunkWriter {
  int fd;
  LogRecord record{};
  size_t estimate{0};
  uint64_t off{kHeaderSize};
  bool failed{false};
  int error{0};

  // Returns false when the chunk this write completed could not be written.
  bool add(LogRecord::Write &&write) {
    estimate +=
        sizeof(LogRecord::Write) + write.key.size() + write.buffer.size();
    for (const auto &pk : write.primary_keys) estimate += pk.size();
    record.writes.emplace_back(std::move(write));
    return estimate < kChunkBytes || flush();
  }

  bool flush() {
    if (record.writes.empty()) return true;
    msgpack::sbuffer payload;
    msgpack::pack(payload, record);

    Crc32c crc;
    crc.Update(payload.data(), payload.size());
    uint8_t prefix[kChunkPrefixSize];
    PutLe32(prefix, static_cast<uint32_t>(payload.size()));
    PutLe32(prefix + sizeof(uint32_t), crc.Finish());

    bool written =
        WriteAll(fd, prefix, sizeof(prefix), off) &&
        WriteAll(fd, payload.data(), payload.size(), off + sizeof(prefix));
    // Sync per chunk: a single sync of the whole file stalls the log on the
    // same device.
    while (written && ::fdatasync(fd) != 0) {
      if (errno != EINTR) written = false;
    }
    if (!written) {
      failed = true;
      error = errno;
      return false;
    }

    off += sizeof(prefix) + payload.size();
    record.writes.clear();
    estimate = 0;
    return true;
  }
};

const char *EpochScanCheckpoint::CheckpointFileName() { return "checkpoint"; }
const char *EpochScanCheckpoint::WorkingFileName() {
  return "checkpoint.working";
}

/**
 * @brief Copies one row's bytes together with the version they belong to.
 *
 * @details Follows the read path's protocol: refuse a locked version, copy,
 * then confirm the version did not move. A row locked for the whole budget is
 * unstable rather than skipped, since its holder may abort and leave no record
 * of the value.
 *
 * @param retries Incremented once per rejected attempt.
 * @return kTaken, kSkipped when the absent flag is set, or kUnstable.
 */
EpochScanCheckpoint::CaptureResult EpochScanCheckpoint::CapturePrimaryRow(
    uint32_t table_id, std::string_view key, const RowRecord &item,
    LogRecord::Write &out, uint64_t &retries) {
  for (unsigned attempt = 0; attempt < kSpinAttempts; ++attempt) {
    const Tidword observed = item.transaction_id.load();
    if (observed.lock) {
      ++retries;
      _mm_pause();
      continue;
    }
    HELIOS_DEBUG_SYNC("checkpoint.before_row_copy");
    if (observed.absent) return EpochScanCheckpoint::CaptureResult::kSkipped;
    std::string bytes = item.CopyValue();
    if (item.transaction_id.load() != observed) {
      ++retries;
      continue;
    }
    out.key.assign(key.data(), key.size());
    out.buffer = std::move(bytes);
    out.transaction_id = observed;
    out.table_id = table_id;
    out.secondary_op = SecondaryIndexOp::kNone;
    return EpochScanCheckpoint::CaptureResult::kTaken;
  }
  return EpochScanCheckpoint::CaptureResult::kUnstable;
}

/**
 * @brief Copies one secondary record's primary key under the same protocol.
 *
 * @details The key is a complete posting list rather than a delta: recovery
 * expands it into adds and then applies the log's own adds and removes by
 * transaction id. A live record's list is its one primary key.
 *
 * @param retries Incremented once per rejected attempt.
 * @return kTaken, kSkipped when the absent flag is set, or kUnstable.
 */
EpochScanCheckpoint::CaptureResult EpochScanCheckpoint::CaptureSecondaryEntry(
    uint32_t table_id, const index::SecondaryIndex &index, std::string_view key,
    const IndexRecord &item, LogRecord::Write &out, uint64_t &retries) {
  for (unsigned attempt = 0; attempt < kSpinAttempts; ++attempt) {
    const Tidword observed = item.transaction_id.load();
    if (observed.lock) {
      ++retries;
      _mm_pause();
      continue;
    }
    if (observed.absent) return EpochScanCheckpoint::CaptureResult::kSkipped;
    const auto [secondary_key, primary_key] =
        index::split_entry(index, key, item);
    if (item.transaction_id.load() != observed) {
      ++retries;
      continue;
    }
    // The log keys a secondary write by the secondary key alone.
    out.key.assign(secondary_key);
    out.transaction_id = observed;
    out.table_id = table_id;
    out.index_id = index.id;
    out.primary_keys.emplace_back(primary_key);
    out.secondary_op = SecondaryIndexOp::kFull;
    return EpochScanCheckpoint::CaptureResult::kTaken;
  }
  return EpochScanCheckpoint::CaptureResult::kUnstable;
}

EpochScanCheckpoint::EpochScanCheckpoint(const Config &config,
                                         TableDictionary &tables,
                                         epoch::Framework &epoch_framework,
                                         Logger &logger)
    : config_(config),
      tables_(tables),
      epoch_framework_(epoch_framework),
      logger_(logger),
      checkpoint_path_(
          (std::filesystem::path(config.work_dir) / CheckpointFileName())
              .string()),
      working_path_((std::filesystem::path(config.work_dir) / WorkingFileName())
                        .string()) {
  logger_.wal().set_checkpoint_threshold(config.wal_budget_bytes / 2);
}

EpochScanCheckpoint::~EpochScanCheckpoint() { Stop(); }

void EpochScanCheckpoint::Start() {
  thread_ = std::thread(&EpochScanCheckpoint::Loop, this);
}

void EpochScanCheckpoint::Stop() {
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    stop_ = true;
  }
  logger_.wal().StopWorker();
  if (thread_.joinable()) thread_.join();
}

void EpochScanCheckpoint::Loop() {
  Wal &wal = logger_.wal();
  for (;;) {
    const Wal::WorkerTask task = wal.WaitForWork();
    if (task == Wal::WorkerTask::kStop) return;
    if (task == Wal::WorkerTask::kPrepare) {
      wal.PrepareSegment();
      continue;
    }
    RunOnce();
  }
}

bool EpochScanCheckpoint::RunOnce(Stats *out_stats) {
  Wal &wal = logger_.wal();
  std::unique_lock<std::mutex> guard(capture_mutex_, std::try_to_lock);
  if (!guard.owns_lock()) {
    SPDLOG_WARN("A checkpoint is already being written");
    wal.set_checkpoint_threshold(wal.appended_bytes() + wal.segment_bytes());
    return false;
  }

  Stats stats;
  stats.generation = ++generation_;

  // Sample the start epoch, then barrier until every commit at or below it has
  // installed.
  const auto barrier_begin = Clock::now();
  // Read before the start epoch: a frame before this position was flushed
  // below the global epoch of that moment, so it is at or below the start
  // epoch. The byte count read first is at most the one at the redo start.
  const uint64_t appended_at_start = wal.appended_bytes();
  stats.redo_start = wal.durable_end();
  // The start epoch is read before the barrier: once Sync returns, every commit
  // at or below it has installed its values. A start epoch taken after the scan
  // started would drop the commits still in flight at that moment.
  stats.start_epoch = epoch_framework_.GetGlobalEpoch();
  epoch_framework_.Sync();
  stats.barrier_ms = ElapsedMs(barrier_begin);

  // The walk streams its chunks into the working file; the header goes in the
  // space left before them once the walk is over.
  const ScopedFd file(::open(working_path_.c_str(),
                             O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644));
  if (file.fd < 0) {
    SPDLOG_WARN("Checkpoint {0} could not open {1} (errno {2})",
                stats.generation, working_path_, errno);
    wal.set_checkpoint_threshold(wal.appended_bytes() + wal.segment_bytes());
    if (out_stats != nullptr) *out_stats = stats;
    return false;
  }
  ChunkWriter writer{file.fd};
  writer.record.epoch = stats.start_epoch;

  // Walk every table.
  const auto scan_begin = Clock::now();
  bool abandoned = false;
  tables_.ForEachTable([&](Table &table) {
    if (abandoned) return;
    if (!CaptureTable(table, writer, stats)) abandoned = true;
  });

  // Drop the epoch pin the walk held; record the scan time and the end
  // epoch. Ending the reclamation critical section the pass held open from
  // its first walk is what lets a row retired during it be freed: no index
  // reclaims anything while a thread is inside a Masstree RCU enrolment.
  index::release_thread_epoch();

  if (!abandoned && !writer.flush()) abandoned = true;
  stats.scan_ms = ElapsedMs(scan_begin);
  // Every version in the checkpoint was published at or below this epoch, which
  // is what the durability gate below is asked about.
  stats.end_epoch = epoch_framework_.GetGlobalEpoch();

  // Keep the previous checkpoint when the scan could not settle or could not
  // be written out.
  if (abandoned) {
    ::unlink(working_path_.c_str());
    bool stopping = false;
    {
      std::lock_guard<std::mutex> lock(stop_mutex_);
      stopping = stop_;
    }
    if (writer.failed) {
      SPDLOG_WARN("Checkpoint {0} abandoned: writing {1} failed (errno {2})",
                  stats.generation, working_path_, writer.error);
    } else if (stopping) {
      SPDLOG_INFO("Checkpoint {0} abandoned: the capture thread is stopping",
                  stats.generation);
    } else {
      SPDLOG_WARN(
          "Checkpoint {0} abandoned: a row did not present a stable version",
          stats.generation);
    }
    wal.set_checkpoint_threshold(wal.appended_bytes() + wal.segment_bytes());
    if (out_stats != nullptr) *out_stats = stats;
    return false;
  }

  // Finish the file, wait for durability, and rename.
  const bool published = Publish(writer, stats);
  if (out_stats != nullptr) *out_stats = stats;
  if (!published) {
    wal.set_checkpoint_threshold(wal.appended_bytes() + wal.segment_bytes());
    return false;
  }
  // A published checkpoint leaves recovery only the log from its redo start,
  // so half the budget is counted from there.
  wal.set_checkpoint_threshold(appended_at_start +
                               config_.wal_budget_bytes / 2);

  SPDLOG_INFO(
      "Checkpoint {0} written: {1} rows, {2} index entries, {3} bytes, start "
      "epoch {4}, end epoch {5}, {6} ms at the barrier, {7} ms scanning, {8} "
      "ms writing, {9} ms waiting for the log, {10} version retries",
      stats.generation, stats.primary_rows, stats.secondary_entries,
      stats.checkpoint_bytes, stats.start_epoch, stats.end_epoch,
      stats.barrier_ms, stats.scan_ms, stats.write_ms, stats.durability_ms,
      stats.version_retries);
  return true;
}

bool EpochScanCheckpoint::CaptureTable(Table &table, ChunkWriter &writer,
                                       Stats &stats) {
  const uint32_t table_id = table.id();
  std::vector<std::string> unstable_rows;
  std::vector<std::pair<std::string, std::string>> unstable_entries;

  // Capture every primary record, deferring the keys that stayed locked (the
  // callback returns true to stop). The walk also serves the segment pool,
  // which keeps preparation off the logger thread.
  uint64_t walked = 0;
  table.GetPrimaryIndex().ForEach([&](std::string_view key, IndexRecord &item) {
    if (++walked % kRowsPerPoolCheck == 0) logger_.wal().PrepareSegment();
    LogRecord::Write write;
    switch (CapturePrimaryRow(table_id, key, static_cast<RowRecord &>(item),
                              write, stats.version_retries)) {
      case CaptureResult::kTaken:
        ++stats.primary_rows;
        if (!writer.add(std::move(write))) return true;
        break;
      case CaptureResult::kSkipped:
        break;
      case CaptureResult::kUnstable:
        unstable_rows.emplace_back(key.data(), key.size());
        break;
    }
    return false;
  });

  // Capture every secondary posting list the same way.
  table.ForEachSecondaryIndex(
      [&](const std::string &index_name, index::SecondaryIndex &index) {
        if (writer.failed) return;
        index.tree.ForEach([&](std::string_view key, IndexRecord &item) {
          if (++walked % kRowsPerPoolCheck == 0) logger_.wal().PrepareSegment();
          LogRecord::Write write;
          switch (CaptureSecondaryEntry(table_id, index, key, item, write,
                                        stats.version_retries)) {
            case CaptureResult::kTaken:
              ++stats.secondary_entries;
              if (!writer.add(std::move(write))) return true;
              break;
            case CaptureResult::kSkipped:
              break;
            case CaptureResult::kUnstable:
              unstable_entries.emplace_back(
                  index_name, std::string(key.data(), key.size()));
              break;
          }
          return false;
        });
      });
  if (writer.failed) return false;

  // Retry pass: resolve the deferred keys again and copy what has settled.
  // Rows held by a writer for the whole spin are resolved again by key: the
  // record they were in may have been purged and replaced meanwhile, and a
  // pointer kept across the pass would name the old one.
  bool stopped = false;
  for (unsigned round = 0; round < kRetryRounds; ++round) {
    if (unstable_rows.empty() && unstable_entries.empty()) break;
    {
      std::lock_guard<std::mutex> lock(stop_mutex_);
      stopped = stop_;
    }
    if (stopped) break;
    std::this_thread::sleep_for(kRetryPause);
    logger_.wal().PrepareSegment();

    std::vector<std::string> rows_left;
    for (const auto &key : unstable_rows) {
      auto *item = static_cast<RowRecord *>(table.GetPrimaryIndex().Get(key));
      if (item == nullptr) continue;
      LogRecord::Write write;
      switch (CapturePrimaryRow(table_id, key, *item, write,
                                stats.version_retries)) {
        case CaptureResult::kTaken:
          ++stats.primary_rows;
          if (!writer.add(std::move(write))) return false;
          break;
        case CaptureResult::kSkipped:
          break;
        case CaptureResult::kUnstable:
          rows_left.emplace_back(key);
          break;
      }
    }
    unstable_rows.swap(rows_left);

    std::vector<std::pair<std::string, std::string>> entries_left;
    for (const auto &[index_name, key] : unstable_entries) {
      index::SecondaryIndex *index = table.GetSecondaryIndex(index_name);
      if (index == nullptr) continue;
      IndexRecord *item = index->tree.Get(key);
      if (item == nullptr) continue;
      LogRecord::Write write;
      switch (CaptureSecondaryEntry(table_id, *index, key, *item, write,
                                    stats.version_retries)) {
        case CaptureResult::kTaken:
          ++stats.secondary_entries;
          if (!writer.add(std::move(write))) return false;
          break;
        case CaptureResult::kSkipped:
          break;
        case CaptureResult::kUnstable:
          entries_left.emplace_back(index_name, key);
          break;
      }
    }
    unstable_entries.swap(entries_left);
  }

  return !stopped && unstable_rows.empty() && unstable_entries.empty();
}

bool EpochScanCheckpoint::Publish(const ChunkWriter &writer, Stats &stats) {
  const auto write_begin = Clock::now();
  uint8_t header[kHeaderSize];
  std::memset(header, 0, sizeof(header));
  PutLe32(header, kMagic);
  PutLe16(header + kOffFlags, kFlags);
  PutLe64(header + kOffGeneration, stats.generation);
  PutLe32(header + kOffStartEpoch, stats.start_epoch);
  PutLe32(header + kOffEndEpoch, stats.end_epoch);
  PutLe64(header + kOffPrimaryRows, stats.primary_rows);
  PutLe64(header + kOffSecondaryEntries, stats.secondary_entries);
  PutLe64(header + kOffPayloadSize, writer.off - kHeaderSize);
  PutLe64(header + kOffRedoSegment, stats.redo_start.segment_number);
  PutLe64(header + kOffRedoOffset, stats.redo_start.offset);
  Crc32c crc;
  crc.Update(header, kHeaderSize - sizeof(uint32_t));
  PutLe32(header + kHeaderSize - sizeof(uint32_t), crc.Finish());

  if (!WriteAll(writer.fd, header, sizeof(header), 0) ||
      Fsync(writer.fd) != 0) {
    const int write_errno = errno;
    ::unlink(working_path_.c_str());
    SPDLOG_WARN("Checkpoint {0} could not be written to {1} (errno {2})",
                stats.generation, working_path_, write_errno);
    return false;
  }
  stats.write_ms = ElapsedMs(write_begin);

  // Publishing before the log covers the capture's end epoch would let a
  // version come back without its transaction. Recovery skips the working
  // file, so only the rename waits, and the wait keeps serving the pool.
  const auto gate_begin = Clock::now();
  auto result = Logger::WaitResult::kTimedOut;
  while (result == Logger::WaitResult::kTimedOut &&
         Clock::now() - gate_begin < kDurabilityWait) {
    result =
        logger_.WaitUntilDurable(stats.end_epoch, Clock::now() + kPoolPoll);
    if (result == Logger::WaitResult::kTimedOut) logger_.wal().PrepareSegment();
  }
  if (result != Logger::WaitResult::kDurable) {
    ::unlink(working_path_.c_str());
    SPDLOG_WARN(
        "Checkpoint {0} discarded: the log did not become durable through "
        "epoch {1}",
        stats.generation, stats.end_epoch);
    return false;
  }
  stats.durability_ms = ElapsedMs(gate_begin);
  stats.checkpoint_bytes = writer.off;

  if (::rename(working_path_.c_str(), checkpoint_path_.c_str()) != 0) {
    const int rename_errno = errno;
    ::unlink(working_path_.c_str());
    SPDLOG_WARN("Checkpoint {0} could not be published as {1} (errno {2})",
                stats.generation, checkpoint_path_, rename_errno);
    return false;
  }
  if (!FsyncDirectory(config_.work_dir)) {
    SPDLOG_WARN(
        "Checkpoint {0} was renamed but its directory entry is not durable "
        "(errno {1})",
        stats.generation, errno);
    return false;
  }
  logger_.wal().MarkCovered(stats.redo_start.segment_number);
  return true;
}

EpochScanCheckpoint::LoadResult EpochScanCheckpoint::Load(
    const std::string &work_dir) {
  LoadResult checkpoint;
  const std::string path =
      (std::filesystem::path(work_dir) / CheckpointFileName()).string();
  const ScopedFd file(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (file.fd < 0) {
    checkpoint.status = errno == ENOENT ? LoadResult::Status::kAbsent
                                        : LoadResult::Status::kUnusable;
    checkpoint.detail =
        "open " + path + " (errno " + std::to_string(errno) + ")";
    return checkpoint;
  }
  const int fd = file.fd;

  struct stat file_stat {};
  if (::fstat(fd, &file_stat) < 0)
    return unusable("the checkpoint cannot be sized");
  if (file_stat.st_size < static_cast<off_t>(kHeaderSize)) {
    return unusable("the checkpoint is shorter than its header");
  }

  uint8_t header[kHeaderSize];
  if (!ReadAll(fd, header, sizeof(header), 0)) {
    return unusable("the checkpoint header cannot be read");
  }
  if (GetLe32(header) != kMagic)
    return unusable("the checkpoint magic disagrees");
  if (GetLe16(header + kOffFlags) != kFlags) {
    return unusable("the checkpoint carries unknown flags");
  }
  Crc32c crc;
  crc.Update(header, kHeaderSize - sizeof(uint32_t));
  if (crc.Finish() != GetLe32(header + kHeaderSize - sizeof(uint32_t))) {
    return unusable("the checkpoint checksum does not hold");
  }
  const uint64_t payload_size = GetLe64(header + kOffPayloadSize);
  if (payload_size != static_cast<uint64_t>(file_stat.st_size) - kHeaderSize) {
    return unusable("the checkpoint length disagrees with its header");
  }

  const EpochNumber start_epoch = GetLe32(header + kOffStartEpoch);
  const EpochNumber end_epoch = GetLe32(header + kOffEndEpoch);
  const WalPosition redo_start{GetLe64(header + kOffRedoSegment),
                               GetLe64(header + kOffRedoOffset)};
  // Epoch 0 is not a start epoch any writer produces, and a scan never ends
  // before its start.
  if (start_epoch == 0 || end_epoch < start_epoch) {
    return unusable("the checkpoint epochs are not in order");
  }

  checkpoint.status = LoadResult::Status::kOk;
  checkpoint.redo_start = redo_start;
  checkpoint.start_epoch = start_epoch;
  checkpoint.end_epoch = end_epoch;
  return checkpoint;
}

bool EpochScanCheckpoint::ReadChunks(
    const std::string &work_dir,
    const std::function<void(const LogRecords &)> &apply, std::string *detail) {
  const std::string path =
      (std::filesystem::path(work_dir) / CheckpointFileName()).string();
  const ScopedFd file(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (file.fd < 0) {
    return refuse(detail,
                  "open " + path + " (errno " + std::to_string(errno) + ")");
  }

  uint8_t size_field[sizeof(uint64_t)];
  if (!ReadAll(file.fd, size_field, sizeof(size_field), kOffPayloadSize)) {
    return refuse(detail, "the checkpoint header cannot be read");
  }
  uint64_t remaining = GetLe64(size_field);
  off_t off = kHeaderSize;

  // A damaged chunk may ask for too much, and a damaged payload may not unpack.
  try {
    std::vector<uint8_t> payload;
    while (remaining != 0) {
      uint8_t prefix[kChunkPrefixSize];
      if (remaining < sizeof(prefix)) {
        return refuse(detail, "the checkpoint ends inside a chunk prefix");
      }
      if (!ReadAll(file.fd, prefix, sizeof(prefix), off)) {
        return refuse(detail, "a checkpoint chunk prefix cannot be read");
      }
      remaining -= sizeof(prefix);
      off += sizeof(prefix);

      const uint32_t payload_len = GetLe32(prefix);
      if (payload_len > remaining) {
        return refuse(detail, "a checkpoint chunk runs past the payload");
      }
      payload.resize(payload_len);
      if (!ReadAll(file.fd, payload.data(), payload_len, off)) {
        return refuse(detail, "a checkpoint chunk cannot be read");
      }
      remaining -= payload_len;
      off += payload_len;

      Crc32c crc;
      crc.Update(payload.data(), payload.size());
      if (crc.Finish() != GetLe32(prefix + sizeof(uint32_t))) {
        return refuse(detail, "a checkpoint chunk checksum does not hold");
      }

      size_t consumed = 0;
      auto handle =
          msgpack::unpack(reinterpret_cast<const char *>(payload.data()),
                          payload.size(), consumed);
      // A fresh record per chunk: converting into a used one keeps its strings.
      LogRecords records(1);
      handle.get().convert(records[0]);
      if (consumed != payload.size()) {
        return refuse(detail, "a checkpoint chunk has trailing bytes");
      }
      apply(records);
    }
  } catch (const std::exception &e) {
    return refuse(
        detail, std::string("a checkpoint chunk does not unpack: ") + e.what());
  } catch (...) {
    return refuse(detail, "a checkpoint chunk does not unpack");
  }
  return true;
}

}  // namespace wal
}  // namespace helios::storage
