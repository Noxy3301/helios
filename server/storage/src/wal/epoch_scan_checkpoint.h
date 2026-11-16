/**
 * @file server/storage/src/wal/epoch_scan_checkpoint.h
 * The checkpoint of the live rows, scanned while transactions keep running and
 * merged with the log at recovery, and the worker that takes it and keeps the
 * log's segment pool filled.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_EPOCH_SCAN_CHECKPOINT_H
#define HELIOS_STORAGE_SRC_WAL_EPOCH_SCAN_CHECKPOINT_H

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "helios/config.h"

#include "util/epoch.h"
#include "wal/log_record.h"
#include "wal/wal.h"

namespace helios::storage {

struct RowRecord;
struct IndexRecord;
class Table;
class TableDictionary;
namespace epoch {
class Framework;
}  // namespace epoch
namespace index {
struct SecondaryIndex;
}  // namespace index

namespace wal {

class Logger;

/**
 * @brief A checkpoint of the live rows, written while transactions keep
 * running.
 *
 * The scan starts after a barrier, so every commit at or below the start epoch
 * is already in memory; rows committed during the scan may be captured too, and
 * recovery resolves that mixture by applying the checkpoint under the log's own
 * newest-transaction-id-wins rule and replaying everything above the start
 * epoch. Each row's copy is torn-free under the read path's version protocol,
 * and the checkpoint is published only once the log covers every epoch the scan
 * could have observed. A published checkpoint names where the redo starts, and
 * the segments before that are free to recycle.
 *
 * The worker takes one when the log appended since the capture start of the
 * last published checkpoint reaches half of Config::wal_budget_bytes, and
 * retries after one more segment when an attempt fails.
 */
class EpochScanCheckpoint {
 public:
  /**
   * @brief What one capture did, for the line it logs when it completes.
   */
  struct Stats {
    uint64_t generation{0};
    // Where the log ended before the start epoch was sampled, so every frame
    // above that epoch lies after it.
    WalPosition redo_start;
    // Global epoch sampled before the barrier; recovery replays the log above
    // it.
    EpochNumber start_epoch{0};
    // Global epoch sampled after the walk; the publish waits for the log
    // through it.
    EpochNumber end_epoch{0};
    uint64_t primary_rows{0};
    uint64_t secondary_entries{0};
    uint64_t checkpoint_bytes{0};
    uint64_t version_retries{0};
    int64_t barrier_ms{0};
    int64_t scan_ms{0};
    int64_t write_ms{0};
    int64_t durability_ms{0};
  };

  /**
   * @brief A published checkpoint as recovery receives it.
   *
   * `Absent` is answered with a full replay of the log. `Unusable` fails
   * recovery, since the log the checkpoint covered may be gone.
   */
  struct LoadResult {
    enum class Status { kOk, kAbsent, kUnusable };

    Status status{Status::kAbsent};
    WalPosition redo_start;
    EpochNumber start_epoch{0};
    EpochNumber end_epoch{0};
    std::string detail;
  };

  EpochScanCheckpoint(const Config &config, TableDictionary &tables,
                      epoch::Framework &epoch_framework, Logger &logger);
  ~EpochScanCheckpoint();

  EpochScanCheckpoint(const EpochScanCheckpoint &) = delete;
  EpochScanCheckpoint &operator=(const EpochScanCheckpoint &) = delete;

  /**
   * @brief Starts the worker, which prepares segments and captures when the log
   * has grown enough.
   * @pre epoch::Framework::Start has been called.
   */
  void Start();

  /**
   * @brief Stops the worker, waiting for a capture in progress to finish.
   */
  void Stop();

  /**
   * @brief Captures one checkpoint and publishes it, or abandons the attempt.
   * An abandoned attempt leaves the checkpoint published before it in place;
   * only a directory-sync failure after the atomic rename can report failure
   * with the new checkpoint already in place. Publication makes the segments
   * before the redo start free to recycle.
   * @param[out] out_stats What the capture did, when non-null.
   * @return Whether it published durably.
   */
  bool RunOnce(Stats *out_stats = nullptr);

  /**
   * @brief Validates the header of the published checkpoint of `work_dir`, if
   * there is a usable one. ReadChunks reads what it holds.
   * @param[in] work_dir The directory the database logs into.
   * @return The file's status and, when Ok, its epoch bounds.
   */
  static LoadResult Load(const std::string &work_dir);

  /**
   * @brief Reads the published checkpoint's chunks in file order, holding one
   * in memory at a time.
   * @pre Load returned kOk for `work_dir`.
   * @param[in] work_dir The directory the database logs into.
   * @param[in] apply Called once per chunk with its one record, which is valid
   * only during the call.
   * @param[out] detail Why the read failed, when it returns false. Not null.
   * @return Whether every chunk is intact and the chunks fill the payload.
   */
  static bool ReadChunks(const std::string &work_dir,
                         const std::function<void(const LogRecords &)> &apply,
                         std::string *detail);

  /**
   * @brief Name of the published checkpoint inside the working directory.
   */
  static const char *CheckpointFileName();
  /**
   * @brief Name of the file a capture writes before it publishes.
   */
  static const char *WorkingFileName();

  static constexpr uint32_t kMagic = 0x504b4348;  // "HCKP"
  static constexpr uint16_t kFlags = 0;
  static constexpr size_t kHeaderSize = 66;
  // The writes a chunk gathers before it is written, by estimated size, which
  // bounds what a capture and a recovery hold in memory.
  static constexpr size_t kChunkBytes = 32 * 1024 * 1024;

 private:
  /**
   * @brief What one attempt at one row produced.
   *
   * @details kTaken: a stable version was copied.
   * kSkipped: the absent flag was set.
   * kUnstable: no stable version was obtained within the retry limit.
   */
  enum class CaptureResult { kTaken, kSkipped, kUnstable };

  struct ChunkWriter;

  static CaptureResult CapturePrimaryRow(const std::string &table_name,
                                         std::string_view key,
                                         const RowRecord &item,
                                         LogRecord::Write &out,
                                         uint64_t &retries);
  static CaptureResult CaptureSecondaryEntry(
      const std::string &table_name, const std::string &index_name,
      const index::SecondaryIndex &index, std::string_view key,
      const IndexRecord &item, LogRecord::Write &out, uint64_t &retries);
  bool CaptureTable(Table &table, ChunkWriter &writer, Stats &stats);
  bool Publish(const ChunkWriter &writer, Stats &stats);
  void Loop();

  const Config &config_;
  TableDictionary &tables_;
  epoch::Framework &epoch_framework_;
  Logger &logger_;
  const std::string checkpoint_path_;
  const std::string working_path_;

  uint64_t generation_{0};
  // One capture at a time: two would share the working file, and the older
  // one's durability gate would publish the newer one's bytes.
  std::mutex capture_mutex_;
  std::mutex stop_mutex_;
  bool stop_{false};
  std::thread thread_;
};

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_EPOCH_SCAN_CHECKPOINT_H
