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
 * @file server/storage/src/util/thread_key_storage.h
 * Per-thread slots addressed by a key, walked by the epoch framework and the
 * logger, and reused after their thread exits.
 */

#ifndef HELIOS_STORAGE_SRC_UTIL_THREAD_KEY_STORAGE_H
#define HELIOS_STORAGE_SRC_UTIL_THREAD_KEY_STORAGE_H

#include <pthread.h>

#include <atomic>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>

namespace helios::storage {

/**
 * @brief Holds one T per calling thread in slots another thread can walk.
 *
 * @details Slots sit in blocks of 64, no two on one cache line, and are
 * never freed or moved while this object lives. A thread that exits leaves
 * its T in place; the next thread that needs a slot takes it over and sees the
 * T as the exited thread left it. Destroying this object destroys every T, so
 * no thread may call Get or ForEach afterwards.
 */
template <class T>
class ThreadKeyStorage {
  struct alignas(64) Slot {
    std::atomic<bool> owned{false};
    std::optional<T> payload;
  };

  struct Block {
    Slot slots[64];
    std::atomic<size_t> size{0};
    std::atomic<Block *> next{nullptr};
  };

 public:
  ThreadKeyStorage() {
    const int err = ::pthread_key_create(&key_, release);
    if (err != 0) {
      std::cerr << "::pthread_key_create failed: " << err << std::endl;
      std::abort();
    }
  }

  ~ThreadKeyStorage() {
    Block *block = first_.next.load();
    while (block != nullptr) {
      Block *next = block->next.load();
      delete block;
      block = next;
    }
    const int err = ::pthread_key_delete(key_);
    if (err != 0) {
      std::cerr << "::pthread_key_delete failed: " << err << std::endl;
    }
  }

  /**
   * @brief Returns this thread's object.
   *
   * @details The first call takes over the slot of an exited thread and keeps
   * the T found there, or constructs a T with `func` in a new slot.
   */
  template <class U>
  T *Get(std::function<U()> &&func) {
    auto *slot = static_cast<Slot *>(::pthread_getspecific(key_));
    if (slot == nullptr) {
      slot = install([&](Slot &fresh) { fresh.payload.emplace(func()); });
    }
    return &*slot->payload;
  }

  T *Get() {
    auto *slot = static_cast<Slot *>(::pthread_getspecific(key_));
    if (slot == nullptr) {
      slot = install([](Slot &fresh) { fresh.payload.emplace(); });
    }
    return &*slot->payload;
  }

  /**
   * @brief Invokes `f` on every T constructed so far, including those whose
   *        thread exited.
   */
  void ForEach(std::function<void(T *)> &&f) {
    for (Block *block = &first_; block != nullptr; block = block->next.load()) {
      const size_t size = block->size.load();
      for (size_t i = 0; i < size; ++i) f(&*block->slots[i].payload);
    }
  }

 private:
  static void release(void *ptr) {
    static_cast<Slot *>(ptr)->owned.store(false);
  }

  template <class Initialize>
  Slot *install(Initialize initialize) {
    std::lock_guard<std::mutex> lock(install_mutex_);
    for (Block *block = &first_;; block = block->next.load()) {
      const size_t size = block->size.load();
      for (size_t i = 0; i < size; ++i) {
        Slot &slot = block->slots[i];
        if (!slot.owned.load()) return bind(slot);
      }
      if (size < 64) {
        Slot &slot = block->slots[size];
        initialize(slot);
        bind(slot);
        block->size.store(size + 1);
        return &slot;
      }
      if (block->next.load() == nullptr) block->next.store(new Block());
    }
  }

  Slot *bind(Slot &slot) {
    const int err = ::pthread_setspecific(key_, &slot);
    if (err != 0) {
      std::cerr << "::pthread_setspecific failed: " << err << std::endl;
      std::abort();
    }
    slot.owned.store(true);
    return &slot;
  }

  pthread_key_t key_;
  std::mutex install_mutex_;
  Block first_;
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_UTIL_THREAD_KEY_STORAGE_H
