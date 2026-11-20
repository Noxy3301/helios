/**
 * @file plugin/shared_channel.hh
 * A connection to the storage server that many sessions share, each request
 * matched to its reply by id.
 */

#ifndef HELIOS_PLUGIN_SHARED_CHANNEL_H
#define HELIOS_PLUGIN_SHARED_CHANNEL_H

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

/**
 * @brief One TCP connection to the storage server, shared by the sessions of
 * a process that map to it.
 *
 * @details Every request carries an id, and its reply comes back under that
 * id in any order. A sender that finds no send in progress sends all pending
 * requests, its own and those appended meanwhile. One waiting session at a
 * time reads the socket and hands each reply to its owner; once its own
 * reply is in, it passes the reading role to one session still waiting in
 * the receive phase. A send or receive error, a malformed reply or a reply
 * to no pending request fails the channel, which logs the cause once: every
 * pending exchange returns false, and so does every later one. The socket is
 * shut down at once and closed with the last reference, so no thread can
 * reach a reused descriptor.
 */
class SharedChannel {
 public:
  // Takes over a socket connected to `host`:`port`.
  SharedChannel(int fd, const std::string &host, int port)
      : fd_(fd), host_(host), port_(port) {}
  ~SharedChannel();
  SharedChannel(const SharedChannel &) = delete;
  SharedChannel &operator=(const SharedChannel &) = delete;

  /**
   * @brief Returns the process-wide channel in `slot`.
   *
   * @details Opens a connection to `host`:`port` when the slot has no channel,
   * its channel failed or its peer closed the connection, or its channel goes
   * to another host or port. A caller keeps the returned channel until an
   * exchange on it fails, then asks again. Thread-safe.
   * @return nullptr when no connection could be opened.
   */
  static std::shared_ptr<SharedChannel> acquire(size_t slot,
                                                const std::string &host,
                                                int port);

  /**
   * @brief Sends one request and waits for its reply.
   *
   * @param envelope At most helios::rpc::kMaxPartBytes.
   * @param request_payload The flat request's body, or empty. At most
   * helios::rpc::kMaxPartBytes.
   * @param[out] reply Takes the reply envelope.
   * @param[out] payload Takes the bytes riding raw after the reply envelope.
   * @return false when the channel failed before the reply arrived. Both
   * outputs are then unspecified. Thread-safe.
   */
  bool exchange(std::string_view envelope, std::string_view request_payload,
                std::string &reply, std::string &payload);

 private:
  // One exchange in flight, on its caller's stack.
  struct Waiter {
    std::string *reply;
    std::string *payload;
    std::condition_variable cv;
    bool waiting = false;  // receiving, so it may take the reader role
    bool lead = false;     // handed the reader role
    bool done = false;     // reply delivered or channel failed
    bool ok = false;       // reply delivered
  };

  // A reply parsed out of the receive buffer, not yet delivered.
  struct Frame {
    uint64_t id;
    size_t env_off;
    uint32_t env_len;
    size_t pay_off;
    uint32_t pay_len;
    Waiter *owner;
  };

  const int fd_;
  const std::string host_;
  const int port_;

  // Guarded by mutex_.
  std::mutex mutex_;
  bool failed_ = false;
  bool flushing_ = false;  // a sender owns the socket's send side
  bool reading_ = false;   // a waiter owns the socket's receive side
  uint64_t next_id_ = 0;
  std::unordered_map<uint64_t, Waiter *> waiters_;
  std::string out_;  // frames not yet taken by a send

  // Owned by the sender while flushing_ is set.
  std::string sending_;

  // Owned by the reader while reading_ is set.
  std::string in_;
  size_t begin_ = 0;  // first unparsed byte
  size_t end_ = 0;    // one past the last received byte

  // Fails every pending exchange, shuts the socket down and logs `cause`,
  // unless the channel failed already. Holds mutex_.
  void fail(const std::string &cause);
};

#endif  // HELIOS_PLUGIN_SHARED_CHANNEL_H
