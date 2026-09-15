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
 * @file server/storage/src/silo/tidword.h
 * The Silo TID word: epoch, tid, and lock / latest / absent status bits.
 */

#ifndef HELIOS_STORAGE_SRC_SILO_TIDWORD_H
#define HELIOS_STORAGE_SRC_SILO_TIDWORD_H

#include <cstdint>
#include <msgpack.hpp>

#include "util/epoch.h"

namespace helios::storage {

struct Tidword {
  union {
    uint64_t obj;
    struct {
      bool lock : 1;      // Set while a committer holds the record.
      bool latest : 1;    // Set on commit; cleared on physical removal.
      bool absent : 1;    // The record holds no row (or no primary key).
      uint64_t tid : 29;  // Tid within the epoch.
      uint64_t epoch : 32;
    };
  };

  Tidword() : obj(0) {}
  explicit Tidword(uint64_t word) : obj(word) {}

  // A missing key and a newly allocated record have the same absent word.
  static Tidword Absent() {
    Tidword tid;
    tid.absent = true;
    return tid;
  }

  bool operator==(const Tidword &rhs) const { return obj == rhs.obj; }
  bool operator!=(const Tidword &rhs) const { return obj != rhs.obj; }
  // Orders by epoch, then tid, then the status bits.
  bool operator<(const Tidword &rhs) const { return obj < rhs.obj; }
  MSGPACK_DEFINE(obj);
};
static_assert(sizeof(Tidword) == 8, "the TID word is one 64-bit word");

}  // namespace helios::storage
#endif  // HELIOS_STORAGE_SRC_SILO_TIDWORD_H
