/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

// Modified for Helios.

/**
 * @file server/storage/include/lineairdb/config.h
 * Startup configuration of a storage instance: where it keeps its files,
 * how long an epoch lasts, and which durability contract it runs under.
 */

#ifndef HELIOS_STORAGE_INCLUDE_LINEAIRDB_CONFIG_H
#define HELIOS_STORAGE_INCLUDE_LINEAIRDB_CONFIG_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace helios::storage {

/**
 * @brief Configuration and options for one storage instance.
 */
struct Config {
  /**
   * @brief How often the global epoch advances, in milliseconds.
   *
   * @details Transactions of one epoch are group-committed together, so a
   * longer duration raises throughput and raises response time with it.
   *
   * Default: 40 ms
   * @see [Tu13] https://dl.acm.org/doi/10.1145/2517349.2522713
   */
  size_t epoch_duration_ms = 40;

  /**
   * @brief Whether the instance replays its log at construction.
   *
   * @details The log is always scanned and its interrupted tail truncated,
   * since that tail has to go before the first append lands behind it. This
   * decides only whether the records the scan read are replayed, and whether
   * a published checkpoint is loaded at all.
   * PAX column definitions are always restored before the database starts.
   *
   * Default: true
   */
  bool enable_recovery = true;

  /**
   * @brief How much of the write-ahead log is made writable in place at a
   *        time.
   *
   * @details The log file is written out with zeroes to this size before any
   * record lands in it, and records are then written in place, so a commit's
   * fdatasync persists data and not the size, allocation, or extent-state
   * metadata a growing file drags in (the benefit is filesystem- and
   * device-specific). A log that grows past it is extended by the same
   * amount again: a granularity rather than a limit. Larger means fewer
   * synchronous extensions but a longer startup scan of the reserved
   * region. Zero disables reservation and lets the file grow as written.
   *
   * @note The storage stack must honour fsync/fdatasync; the supported
   * ext4 setup uses its default data ordering and barriers. A log from a
   * build that did not reserve is readable here; the reverse does not
   * hold (such a build reads the reserved zeroes as a broken frame and
   * refuses to start).
   *
   * Default: 64 MiB
   */
  uint64_t wal_initial_capacity_bytes = 64ull * 1024ull * 1024ull;

  /**
   * @brief How often, in milliseconds, a checkpoint of the live rows is
   *        written; zero writes none.
   *
   * The checkpoint is scanned while transactions keep running and is merged
   * with the log at recovery, which is what bounds the part of the log that has
   * to be replayed. It does not bound the log on disk: nothing is truncated.
   *
   * Requires a durability contract that writes a log, since the checkpoint
   * alone is not a recoverable state.
   *
   * Default: 0 (no checkpoint)
   */
  size_t checkpoint_interval_ms = 0;

  /**
   * @brief One checkpoint written this many milliseconds after startup; zero
   *        writes none.
   *
   * @details When checkpoint_interval_ms is also set, its timer starts after
   * this first checkpoint, not at startup.
   *
   * Default: 0 (no checkpoint)
   */
  size_t checkpoint_once_after_ms = 0;

  /**
   * @brief The working directory, which holds the log and every related
   *        file.
   *
   * Default: "./helios_wal"
   */
  std::string work_dir = "./helios_wal";
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_LINEAIRDB_CONFIG_H
