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
 * @file server/storage/include/helios/config.h
 * Startup configuration of a storage instance: where it keeps its files,
 * how long an epoch lasts, and which durability contract it runs under.
 */

#ifndef HELIOS_STORAGE_INCLUDE_HELIOS_CONFIG_H
#define HELIOS_STORAGE_INCLUDE_HELIOS_CONFIG_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace helios::storage {

// Fits a frame of the largest payload, so an epoch never needs splitting.
inline constexpr uint64_t kWalSegmentBytes = 512ull * 1024ull * 1024ull;

/**
 * @brief Configuration and options for one storage instance.
 */
struct Config {
  /**
   * @brief How often the global epoch advances, in milliseconds.
   *
   * @details A Sync commit or a read view that waits on an epoch advances it
   * sooner, so the duration mostly sets how soon an Async commit reaches the
   * device.
   *
   * Default: 40 ms
   * @see [Tu13] https://dl.acm.org/doi/10.1145/2517349.2522713
   */
  size_t epoch_duration_ms = 40;

  /**
   * @brief Size of each write-ahead log segment.
   *
   * @note Only tests change this, to keep their files small. The server always
   * runs with kWalSegmentBytes.
   */
  uint64_t wal_segment_bytes = kWalSegmentBytes;

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
   * Default: "./helios_data"
   */
  std::string work_dir = "./helios_data";
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_INCLUDE_HELIOS_CONFIG_H
