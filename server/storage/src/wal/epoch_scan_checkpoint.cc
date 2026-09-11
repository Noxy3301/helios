/**
 * @file server/storage/src/wal/epoch_scan_checkpoint.cc
 * The checkpoint of the live rows, scanned while transactions keep running and
 * merged with the log at recovery.
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

#include "index/data_item.h"
#include "index/masstree_index.h"
#include "index/primary_index.h"
#include "index/secondary_index.h"
#include "silo/stable_read.h"
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
// flusher is not running rather than that the epoch is slow.
constexpr auto kDurabilityWait = std::chrono::seconds(60);

// Checkpoint header fields, in bytes from the start of the file. The magic word
// opens it at offset 0 and the checksum closes it at kHeaderSize - 4.
constexpr size_t kOffVersion = 4;
constexpr size_t kOffFlags = 6;
constexpr size_t kOffGeneration = 8;
constexpr size_t kOffCutEpoch = 16;
constexpr size_t kOffEndEpoch = 20;
constexpr size_t kOffWalLastEpoch = 24;
constexpr size_t kOffPrimaryRows = 28;
constexpr size_t kOffSecondaryEntries = 36;
constexpr size_t kOffPayloadSize = 44;

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

bool WriteAll(int fd, const void *data, size_t size) {
  const auto *bytes = static_cast<const uint8_t *>(data);
  while (size != 0) {
    const ssize_t written = ::write(fd, bytes, size);
    if (written > 0) {
      bytes += written;
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

}  // namespace

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
 * @return kTaken, kSkipped for an absent slot or a tombstone, or kUnstable.
 */
EpochScanCheckpoint::CaptureResult EpochScanCheckpoint::CapturePrimaryRow(
    const std::string &table_name, std::string_view key, const DataItem &item,
    LogRecord::Write &out, uint64_t *retries) {
  for (unsigned attempt = 0; attempt < kSpinAttempts; ++attempt) {
    const TransactionId observed = item.transaction_id.load();
    if (observed.tid & silo::kLockBit) {
      ++*retries;
      _mm_pause();
      continue;
    }
    HELIOS_DEBUG_SYNC("checkpoint.before_row_copy");
    if (!item.HasRow()) {
      // An absent slot or a tombstone, once the version confirms the emptiness
      // is not the middle of an install.
      if (item.transaction_id.load() == observed) {
        return EpochScanCheckpoint::CaptureResult::kSkipped;
      }
      ++*retries;
      continue;
    }
    std::string bytes = item.buffer.toString();
    if (item.transaction_id.load() != observed) {
      ++*retries;
      continue;
    }
    out.key.assign(key.data(), key.size());
    out.buffer = std::move(bytes);
    out.transaction_id = observed;
    out.table_name = table_name;
    out.secondary_op = SecondaryIndexOp::kNone;
    return EpochScanCheckpoint::CaptureResult::kTaken;
  }
  return EpochScanCheckpoint::CaptureResult::kUnstable;
}

/**
 * @brief Copies one secondary key's whole primary-key list under the same
 *        protocol.
 *
 * @details The list is a complete posting list rather than a delta: recovery
 * expands it into adds and then applies the log's own adds and removes by
 * transaction id.
 *
 * @param retries Incremented once per rejected attempt.
 * @return kTaken, kSkipped for an empty list, or kUnstable.
 */
EpochScanCheckpoint::CaptureResult EpochScanCheckpoint::CaptureSecondaryEntry(
    const std::string &table_name, const std::string &index_name,
    uint32_t index_type, std::string_view key, const DataItem &item,
    LogRecord::Write &out, uint64_t *retries) {
  for (unsigned attempt = 0; attempt < kSpinAttempts; ++attempt) {
    const TransactionId observed = item.transaction_id.load();
    if (observed.tid & silo::kLockBit) {
      ++*retries;
      _mm_pause();
      continue;
    }
    auto primary_keys = std::atomic_load(&item.primary_keys_);
    if (item.transaction_id.load() != observed) {
      ++*retries;
      continue;
    }
    const PrimaryKeyList::View keys(primary_keys);
    if (keys.empty()) return EpochScanCheckpoint::CaptureResult::kSkipped;
    out.key.assign(key.data(), key.size());
    out.transaction_id = observed;
    out.table_name = table_name;
    out.index_name = index_name;
    out.index_type = index_type;
    out.primary_keys.reserve(keys.size());
    for (std::string_view primary_key : keys) {
      out.primary_keys.emplace_back(primary_key.data(), primary_key.size());
    }
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
                        .string()) {}

EpochScanCheckpoint::~EpochScanCheckpoint() { Stop(); }

void EpochScanCheckpoint::Start() {
  if (config_.checkpoint_interval_ms == 0 &&
      config_.checkpoint_once_after_ms == 0) {
    return;
  }
  thread_ = std::thread([this]() { Loop(); });
}

void EpochScanCheckpoint::Stop() {
  {
    std::lock_guard<std::mutex> lock(stop_mutex_);
    stop_ = true;
  }
  stop_cv_.notify_all();
  if (thread_.joinable()) thread_.join();
}

bool EpochScanCheckpoint::ContinueAfter(uint64_t milliseconds) {
  std::unique_lock<std::mutex> lock(stop_mutex_);
  stop_cv_.wait_for(lock, std::chrono::milliseconds(milliseconds),
                    [this] { return stop_; });
  return !stop_;
}

void EpochScanCheckpoint::Loop() {
  if (config_.checkpoint_once_after_ms != 0) {
    if (!ContinueAfter(config_.checkpoint_once_after_ms)) return;
    RunOnce();
    if (config_.checkpoint_interval_ms == 0) return;
  }
  while (ContinueAfter(config_.checkpoint_interval_ms)) {
    RunOnce();
  }
}

bool EpochScanCheckpoint::RunOnce(Stats *out_stats) {
  std::unique_lock<std::mutex> guard(capture_mutex_, std::try_to_lock);
  if (!guard.owns_lock()) {
    SPDLOG_WARN("A checkpoint is already being written");
    return false;
  }

  Stats stats;
  stats.generation = ++generation_;

  // Sample the cut, then barrier until every commit at or below it has
  // installed.
  const auto barrier_begin = Clock::now();
  // The cut is read before the barrier: once Sync returns, every commit at or
  // below it has installed its values. A cut taken after the scan started
  // would drop the commits still in flight at that moment.
  stats.cut_epoch = epoch_framework_.GetGlobalEpoch();
  // A conservative default; the durable epoch can pass closed empty epochs
  // that wrote no frame, and Publish overwrites this with the frame-backed
  // last epoch once the durability gate has returned.
  stats.wal_last_epoch_at_publish = logger_.GetDurableEpoch();
  epoch_framework_.Sync();
  stats.barrier_ms = ElapsedMs(barrier_begin);

  // Walk every table.
  const auto scan_begin = Clock::now();
  LogRecords records;
  bool abandoned = false;
  tables_.ForEachTable([&](Table &table) {
    if (abandoned) return;
    LogRecord record;
    record.epoch = stats.cut_epoch;
    if (!CaptureTable(table, &record, &stats)) {
      abandoned = true;
      return;
    }
    if (!record.writes.empty()) {
      records.emplace_back(std::move(record));
    }
  });

  // Drop the epoch pin the walk held; record the scan time and the end
  // epoch. Ending the reclamation critical section the pass held open from
  // its first walk is what lets a row retired during it be freed: no index
  // reclaims anything while a thread is inside a Masstree RCU enrolment.
  index::MasstreeReleaseThreadEpoch();

  stats.scan_ms = ElapsedMs(scan_begin);
  // Every version in the checkpoint was published at or below this epoch, which
  // is what the durability gate below is asked about.
  stats.end_epoch = epoch_framework_.GetGlobalEpoch();

  // Keep the previous checkpoint when the scan could not settle.
  if (abandoned) {
    ::unlink(working_path_.c_str());
    const bool stopping = [&] {
      std::lock_guard<std::mutex> lock(stop_mutex_);
      return stop_;
    }();
    if (stopping) {
      SPDLOG_INFO("Checkpoint {0} abandoned: the capture thread is stopping",
                  stats.generation);
    } else {
      SPDLOG_WARN(
          "Checkpoint {0} abandoned: a row did not present a stable version",
          stats.generation);
    }
    if (out_stats != nullptr) *out_stats = stats;
    return false;
  }

  // Wait for durability, write, and rename.
  const bool published = Publish(records, &stats);
  if (out_stats != nullptr) *out_stats = stats;
  if (!published) return false;

  SPDLOG_INFO(
      "Checkpoint {0} written: {1} rows, {2} index entries, {3} bytes, cut "
      "epoch {4}, end epoch {5}, {6} ms at the barrier, {7} ms scanning, {8} "
      "ms writing, {9} ms waiting for the log, {10} version retries",
      stats.generation, stats.primary_rows, stats.secondary_entries,
      stats.checkpoint_bytes, stats.cut_epoch, stats.end_epoch,
      stats.barrier_ms, stats.scan_ms, stats.write_ms, stats.durability_ms,
      stats.version_retries);
  return true;
}

bool EpochScanCheckpoint::CaptureTable(Table &table, LogRecord *record,
                                       Stats *stats) {
  const std::string &table_name = table.Name();
  std::vector<std::string> unstable_rows;
  std::vector<std::pair<std::string, std::string>> unstable_entries;

  // Capture every primary slot; defer the keys that stayed locked. The
  // walk's callback returns true to stop, so a capture that wants every row
  // returns false.
  table.GetPrimaryIndex().ForEach([&](std::string_view key, DataItem &item) {
    LogRecord::Write write;
    switch (CapturePrimaryRow(table_name, key, item, write,
                              &stats->version_retries)) {
      case CaptureResult::kTaken:
        ++stats->primary_rows;
        record->writes.emplace_back(std::move(write));
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
        const uint32_t index_type = static_cast<uint32_t>(index.GetIndexType());
        index.ForEach([&](std::string_view key, DataItem &item) {
          LogRecord::Write write;
          switch (CaptureSecondaryEntry(table_name, index_name, index_type, key,
                                        item, write, &stats->version_retries)) {
            case CaptureResult::kTaken:
              ++stats->secondary_entries;
              record->writes.emplace_back(std::move(write));
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

  // Retry pass: resolve the deferred keys again and copy what has settled.
  // Rows held by a writer for the whole spin are resolved again by key: the
  // slot they were in may have been purged and replaced meanwhile, and a
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

    std::vector<std::string> rows_left;
    for (const auto &key : unstable_rows) {
      DataItem *item = table.GetPrimaryIndex().Get(key);
      if (item == nullptr) continue;
      LogRecord::Write write;
      switch (CapturePrimaryRow(table_name, key, *item, write,
                                &stats->version_retries)) {
        case CaptureResult::kTaken:
          ++stats->primary_rows;
          record->writes.emplace_back(std::move(write));
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
      DataItem *item = index->Get(key);
      if (item == nullptr) continue;
      LogRecord::Write write;
      switch (CaptureSecondaryEntry(
          table_name, index_name, static_cast<uint32_t>(index->GetIndexType()),
          key, *item, write, &stats->version_retries)) {
        case CaptureResult::kTaken:
          ++stats->secondary_entries;
          record->writes.emplace_back(std::move(write));
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

bool EpochScanCheckpoint::Publish(const LogRecords &records, Stats *stats) {
  msgpack::sbuffer payload;
  msgpack::pack(payload, records);

  // Publishing before the log covers the last epoch the scan could have
  // observed would let a version come back without its transaction. The wait
  // precedes the header build, which embeds the last epoch read once it
  // returns.
  const auto gate_begin = Clock::now();
  const auto result = logger_.WaitUntilDurable(stats->end_epoch,
                                               Clock::now() + kDurabilityWait);
  if (result != Logger::WaitResult::kDurable) {
    // Nothing was written this round; drop any working file an earlier
    // failed attempt left behind.
    ::unlink(working_path_.c_str());
    SPDLOG_WARN(
        "Checkpoint {0} discarded: the log did not become durable through "
        "epoch {1}",
        stats->generation, stats->end_epoch);
    return false;
  }
  stats->wal_last_epoch_at_publish = logger_.GetWalLastEpoch();
  stats->durability_ms = ElapsedMs(gate_begin);

  const auto write_begin = Clock::now();
  uint8_t header[kHeaderSize];
  std::memset(header, 0, sizeof(header));
  PutLe32(header, kMagic);
  PutLe16(header + kOffVersion, kVersion);
  PutLe16(header + kOffFlags, kFlags);
  PutLe64(header + kOffGeneration, stats->generation);
  PutLe32(header + kOffCutEpoch, stats->cut_epoch);
  PutLe32(header + kOffEndEpoch, stats->end_epoch);
  PutLe32(header + kOffWalLastEpoch, stats->wal_last_epoch_at_publish);
  PutLe64(header + kOffPrimaryRows, stats->primary_rows);
  PutLe64(header + kOffSecondaryEntries, stats->secondary_entries);
  PutLe64(header + kOffPayloadSize, static_cast<uint64_t>(payload.size()));
  Crc32c crc;
  crc.Update(header, kHeaderSize - sizeof(uint32_t));
  crc.Update(payload.data(), payload.size());
  PutLe32(header + kHeaderSize - sizeof(uint32_t), crc.Finish());
  stats->checkpoint_bytes = kHeaderSize + payload.size();

  const int fd = ::open(working_path_.c_str(),
                        O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    SPDLOG_WARN("Checkpoint {0} could not open {1} (errno {2})",
                stats->generation, working_path_, errno);
    return false;
  }
  const bool written = WriteAll(fd, header, sizeof(header)) &&
                       WriteAll(fd, payload.data(), payload.size()) &&
                       Fsync(fd) == 0;
  const int write_errno = errno;
  ::close(fd);
  if (!written) {
    ::unlink(working_path_.c_str());
    SPDLOG_WARN("Checkpoint {0} could not be written to {1} (errno {2})",
                stats->generation, working_path_, write_errno);
    return false;
  }
  stats->write_ms = ElapsedMs(write_begin);

  if (::rename(working_path_.c_str(), checkpoint_path_.c_str()) != 0) {
    const int rename_errno = errno;
    ::unlink(working_path_.c_str());
    SPDLOG_WARN("Checkpoint {0} could not be published as {1} (errno {2})",
                stats->generation, checkpoint_path_, rename_errno);
    return false;
  }
  if (!FsyncDirectory(config_.work_dir)) {
    SPDLOG_WARN(
        "Checkpoint {0} was renamed but its directory entry is not durable "
        "(errno {1})",
        stats->generation, errno);
    return false;
  }
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

  auto unusable = [&](const std::string &detail) {
    checkpoint.status = LoadResult::Status::kUnusable;
    checkpoint.detail = detail;
    checkpoint.records.clear();
    return checkpoint;
  };

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
  if (GetLe16(header + kOffVersion) != kVersion) {
    return unusable("the checkpoint version is not supported");
  }
  if (GetLe16(header + kOffFlags) != kFlags) {
    return unusable("the checkpoint carries unknown flags");
  }
  const uint64_t payload_size = GetLe64(header + kOffPayloadSize);
  if (payload_size != static_cast<uint64_t>(file_stat.st_size) - kHeaderSize) {
    return unusable("the checkpoint length disagrees with its header");
  }

  const EpochNumber cut_epoch = GetLe32(header + kOffCutEpoch);
  const EpochNumber end_epoch = GetLe32(header + kOffEndEpoch);
  const EpochNumber wal_last_epoch_at_publish =
      GetLe32(header + kOffWalLastEpoch);
  // Epoch 0 is not a cut any writer produces, and a scan never ends before
  // its cut.
  if (cut_epoch == 0 || end_epoch < cut_epoch) {
    return unusable("the checkpoint epochs are not in order");
  }

  // Everything from here allocates in proportion to the file, and a file that
  // is damaged in its length is exactly the one that would ask for too much.
  try {
    std::vector<uint8_t> payload(payload_size);
    if (payload_size != 0 &&
        !ReadAll(fd, payload.data(), payload.size(), kHeaderSize)) {
      return unusable("the checkpoint payload cannot be read");
    }
    Crc32c crc;
    crc.Update(header, kHeaderSize - sizeof(uint32_t));
    crc.Update(payload.data(), payload.size());
    if (crc.Finish() != GetLe32(header + kHeaderSize - sizeof(uint32_t))) {
      return unusable("the checkpoint checksum does not hold");
    }

    size_t consumed = 0;
    auto handle =
        msgpack::unpack(reinterpret_cast<const char *>(payload.data()),
                        payload.size(), consumed);
    handle.get().convert(checkpoint.records);
    if (consumed != payload.size()) {
      return unusable("the checkpoint payload has trailing bytes");
    }
  } catch (const std::exception &e) {
    return unusable(std::string("the checkpoint payload does not unpack: ") +
                    e.what());
  } catch (...) {
    return unusable("the checkpoint payload does not unpack");
  }

  checkpoint.status = LoadResult::Status::kOk;
  checkpoint.cut_epoch = cut_epoch;
  checkpoint.end_epoch = end_epoch;
  checkpoint.wal_last_epoch_at_publish = wal_last_epoch_at_publish;
  return checkpoint;
}

}  // namespace wal
}  // namespace helios::storage
