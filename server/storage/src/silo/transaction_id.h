/*
 *   Copyright (c) 2020 Nippon Telegraph and Telephone Corporation
 *   All rights reserved.

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
 * @file server/storage/src/silo/transaction_id.h
 * The Silo transaction id word: the epoch, the sequence, and the lock bit.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_TRANSACTION_ID_H
#define HELIOS_STORAGE_SRC_SILO_TRANSACTION_ID_H

#include <cstdint>
#include <msgpack.hpp>

#include "util/epoch.h"

namespace helios::storage {

struct TransactionId {
  EpochNumber epoch;
  // Silo's sequence within the epoch. Bit 0 is the row's lock bit, so a live
  // committed version is always even.
  uint32_t tid;

  TransactionId() noexcept : epoch(0), tid(0) {}
  TransactionId(const EpochNumber e, uint32_t t) : epoch(e), tid(t) {}
  TransactionId(const TransactionId &) = default;
  TransactionId &operator=(const TransactionId &) = default;
  bool operator==(const TransactionId &rhs) const noexcept {
    return epoch == rhs.epoch && tid == rhs.tid;
  }
  bool operator!=(const TransactionId &rhs) const noexcept {
    return !(*this == rhs);
  }
  bool operator<(const TransactionId &rhs) const noexcept {
    if (epoch == rhs.epoch) return tid < rhs.tid;
    return epoch < rhs.epoch;
  }
  bool IsEmpty() const noexcept { return epoch == 0 && tid == 0; }
  MSGPACK_DEFINE(epoch, tid);
};

}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_SILO_TRANSACTION_ID_H
