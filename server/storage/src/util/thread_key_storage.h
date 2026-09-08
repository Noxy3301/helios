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

/**
 * @file server/storage/src/util/thread_key_storage.h
 * Per-thread slots addressed by a key, with an enumeration over the live
 * threads that the epoch framework walks.
 */

#ifndef HELIOS_STORAGE_SRC_UTIL_THREAD_KEY_STORAGE_H
#define HELIOS_STORAGE_SRC_UTIL_THREAD_KEY_STORAGE_H

#include <pthread.h>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <utility>

namespace helios::storage {

/**
 * @brief Holds one T per calling thread, and links every T created so far so
 *        another thread can walk them.
 *
 * @details Get() stores the node in pthread TLS and prepends it to
 * head_node_. Destroying this object deletes every node, so no thread may
 * call Get afterwards.
 */
template <class T>
class ThreadKeyStorage {
  struct TlsNode {
    TlsNode *prev;
    T payload;
    TlsNode() : prev(nullptr), payload() {}
    template <class U>
    explicit TlsNode(std::function<U()> f)
        : prev(nullptr), payload(std::move(f())) {}
  };

 public:
  ThreadKeyStorage() : head_node_(nullptr) {
    int err = ::pthread_key_create(&key_, nullptr);
    if (err != 0) {
      // Every access below reads an uninitialized key otherwise.
      std::cerr << "::pthread_key_create failed: " << err << std::endl;
      std::abort();
    }
  }

  ~ThreadKeyStorage() {
    auto *ptr = head_node_.load();
    while (ptr != nullptr) {
      auto *prev = ptr->prev;
      delete ptr;
      ptr = prev;
    }
    int err = ::pthread_key_delete(key_);
    if (err != 0) {
      std::cerr << "::pthread_key_delete failed: " << err << std::endl;
    }
  }

  /**
   * @brief Returns this thread's object, constructing it with `func` on the
   *        first call.
   */
  template <class U>
  T *Get(std::function<U()> &&func) {
    void *ptr = ::pthread_getspecific(key_);
    if (ptr == nullptr) ptr = Install(new TlsNode(std::move(func)));
    return &reinterpret_cast<TlsNode *>(ptr)->payload;
  }

  T *Get() {
    void *ptr = ::pthread_getspecific(key_);
    if (ptr == nullptr) ptr = Install(new TlsNode());
    return &reinterpret_cast<TlsNode *>(ptr)->payload;
  }

  /**
   * @brief Invokes `f` on every published per-thread T, newest first.
   */
  void ForEach(std::function<void(T *)> &&f) {
    TlsNode *ptr = head_node_.load();
    while (ptr != nullptr) {
      f(&ptr->payload);
      ptr = ptr->prev;
    }
  }

 private:
  /**
   * @brief Binds `node` to this thread and publishes it to the walk.
   */
  TlsNode *Install(TlsNode *node) {
    const int err = ::pthread_setspecific(key_, node);
    if (err != 0) {
      std::cerr << "::pthread_setspecific failed: " << err << std::endl;
      std::abort();
    }
    for (;;) {
      TlsNode *old = head_node_.load();
      node->prev = old;
      if (head_node_.compare_exchange_weak(old, node)) return node;
    }
  }

  pthread_key_t key_;
  // pthread TLS cannot be iterated from another thread, so every node is
  // linked here too.
  std::atomic<TlsNode *> head_node_;
};

}  // namespace helios::storage

#endif  // HELIOS_STORAGE_SRC_UTIL_THREAD_KEY_STORAGE_H
