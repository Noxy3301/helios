/**
 * @file server/storage/src/wal/epoch_scan_checkpoint.h
 * The image of the live rows, scanned while transactions keep running and
 * merged with the log at recovery.
 */

#ifndef HELIOS_STORAGE_SRC_WAL_EPOCH_SCAN_CHECKPOINT_H
#define HELIOS_STORAGE_SRC_WAL_EPOCH_SCAN_CHECKPOINT_H

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "storage/config.h"
#include "util/epoch.h"
#include "wal/log_record.h"

namespace helios::storage {

struct DataItem;
class Table;
class TableDictionary;
namespace epoch {
class Framework;
}  // namespace epoch

namespace wal {

class Logger;

/**
 * @brief An image of the live rows, written while transactions keep running.
 *
 * The scan starts after a barrier, so every commit at or below the cut is
 * already in memory; rows committed during the scan may be captured too, and
 * recovery resolves that mixture by folding the image under the log's own
 * newest-transaction-id-wins rule and replaying everything above the cut.
 * Each row's copy is torn-free under the read path's version protocol, and
 * the image is published only once the log covers every epoch the scan could
 * have observed. What the image bounds is the replay, not the log on disk.
 */
class EpochScanCheckpoint {
 public:
  /**
   * @brief What one capture did, for the line it logs when it completes.
   */
  struct Stats {
    uint64_t generation{0};
    // Global epoch sampled before the barrier; recovery replays the log above
    // it.
    EpochNumber cut_epoch{0};
    // Global epoch sampled after the walk; the publish waits for the log
    // through it.
    EpochNumber end_epoch{0};
    // The epoch of the log's last frame once the durability wait returned.
    // Unlike the durable epoch it moves only when a frame is written, and it
    // is what recovery's acceptance gate compares the rescanned log against.
    EpochNumber wal_frontier_at_publish{0};
    uint64_t primary_rows{0};
    uint64_t secondary_entries{0};
    uint64_t image_bytes{0};
    uint64_t version_retries{0};
    int64_t barrier_ms{0};
    int64_t scan_ms{0};
    int64_t write_ms{0};
    int64_t durability_ms{0};
  };

  /**
   * @brief A published image as recovery receives it.
   *
   * `Absent` and `Unusable` are both answered with a full replay of the log;
   * they are distinguished so that a damaged image is reported rather than
   * ignored.
   */
  struct Image {
    enum class Status { kOk, kAbsent, kUnusable };

    Status status{Status::kAbsent};
    EpochNumber cut_epoch{0};
    EpochNumber end_epoch{0};
    EpochNumber wal_frontier_at_publish{0};
    LogRecords records;
    std::string detail;
  };

  EpochScanCheckpoint(const Config &config, TableDictionary &tables,
                      epoch::Framework &epoch_framework, Logger &logger);
  ~EpochScanCheckpoint();

  EpochScanCheckpoint(const EpochScanCheckpoint &) = delete;
  EpochScanCheckpoint &operator=(const EpochScanCheckpoint &) = delete;

  /**
   * @brief Starts the thread that captures on the configured interval.
   * A zero interval and no one-shot delay leaves the thread unstarted, which
   * is the default, and so does a durability contract that writes no log.
   * @pre epoch::Framework::Start has been called.
   */
  void Start();

  /**
   * @brief Stops the thread, waiting for a capture in progress to finish.
   */
  void Stop();

  /**
   * @brief Captures one image and publishes it, or abandons the attempt.
   * An abandoned attempt leaves the image published before it in place; only
   * a directory-sync failure after the atomic rename can report failure with
   * the new image already in place.
   * @param[out] out_stats What the capture did, when non-null.
   * @return Whether it published durably.
   */
  bool RunOnce(Stats *out_stats = nullptr);

  /**
   * @brief Reads the published image of `work_dir`, if there is a usable one.
   * @param[in] work_dir The directory the database logs into.
   * @return The file's status and, when Ok, the records and epoch bounds it
   * held.
   */
  static Image Load(const std::string &work_dir);

  /**
   * @brief Name of the published image inside the working directory.
   */
  static const char *ImageFileName();
  /**
   * @brief Name of the file a capture writes before it publishes.
   */
  static const char *WorkingFileName();

  static constexpr uint32_t kMagic = 0x504b434c;  // "LCKP"
  // Load refuses any other version, so a missing wal_frontier_at_publish is
  // never filled with a guessed bound.
  static constexpr uint16_t kVersion = 2;
  static constexpr uint16_t kFlags = 0;
  static constexpr size_t kHeaderSize = 56;

 private:
  /**
   * @brief What one attempt at one row produced.
   *
   * @details kTaken: a stable version was copied. kSkipped: the slot was
   * blank or a tombstone under a stable read. kUnstable: the lock bit stayed
   * set for the whole spin budget, so the row goes to the retry pass and, if
   * it never settles, the capture is abandoned.
   */
  enum class CaptureResult { kTaken, kSkipped, kUnstable };

  static CaptureResult CapturePrimaryRow(const std::string &table_name,
                                         std::string_view key,
                                         const DataItem &item,
                                         LogRecord::Write &out,
                                         uint64_t *retries);
  static CaptureResult CaptureSecondaryEntry(
      const std::string &table_name, const std::string &index_name,
      uint32_t index_type, std::string_view key, const DataItem &item,
      LogRecord::Write &out, uint64_t *retries);
  bool CaptureTable(Table &table, LogRecord *record, Stats *stats);
  bool Publish(const LogRecords &records, Stats *stats);
  void Loop();
  /**
   * @brief Waits for the interval, or for a stop.
   *
   * @return Whether the loop may continue, i.e. no stop was requested.
   */
  bool ContinueAfter(uint64_t milliseconds);

  const Config &config_;
  TableDictionary &tables_;
  epoch::Framework &epoch_framework_;
  Logger &logger_;
  const std::string image_path_;
  const std::string working_path_;

  uint64_t generation_{0};
  // One capture at a time: two would share the working file, and the older
  // one's durability gate would publish the newer one's bytes.
  std::mutex capture_mutex_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool stop_{false};
  std::thread thread_;
};

}  // namespace wal
}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_WAL_EPOCH_SCAN_CHECKPOINT_H
