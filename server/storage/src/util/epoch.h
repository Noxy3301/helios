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
 * @file server/storage/src/util/epoch.h
 * The epoch counter every subsystem stamps its work with.
 */

#ifndef HELIOS_STORAGE_SRC_UTIL_EPOCH_H
#define HELIOS_STORAGE_SRC_UTIL_EPOCH_H

#include <cstdint>

namespace helios::storage {

using EpochNumber = uint32_t;

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_UTIL_EPOCH_H
