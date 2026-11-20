/**
 * @file plugin/shared_channel.cc
 * The shared channel: combined sends, one reader at a time that hands replies
 * out by id, and the process-wide pool of channels.
 */

#include "shared_channel.hh"

#include <arpa/inet.h>
#include <endian.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <vector>

#include "common/rpc_frame.h"
#include "log.hh"

namespace {

using helios::rpc::append_len;
using helios::rpc::kFrameHeadBytes;
using helios::rpc::kInitialBufferBytes;
using helios::rpc::kKeptBufferBytes;
using helios::rpc::kMaxPartBytes;
using helios::rpc::load_len;

// Guards the pool; the channels themselves are used without it.
std::mutex pool_mutex;
std::vector<std::shared_ptr<SharedChannel>> pool;

}  // namespace

SharedChannel::~SharedChannel() { close(fd_); }

std::shared_ptr<SharedChannel> SharedChannel::acquire(size_t slot,
                                                      const std::string &host,
                                                      int port) {
  std::lock_guard<std::mutex> lock(pool_mutex);
  if (slot >= pool.size()) pool.resize(slot + 1);
  std::shared_ptr<SharedChannel> &channel = pool[slot];
  if (channel != nullptr) {
    std::lock_guard<std::mutex> channel_lock(channel->mutex_);
    // With no reader, only a peek at the socket sees a peer that closed it
    // (a restarted server).
    if (!channel->failed_ && !channel->reading_) {
      char byte;
      const ssize_t n = recv(channel->fd_, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
      if (n == 0) {
        channel->fail("the storage server closed the connection");
      } else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        channel->fail(std::string("recv: ") + std::strerror(errno));
      }
    }
    // A channel to another host or port is replaced in the slot; the
    // sessions holding it keep it.
    if (!channel->failed_ && channel->host_ == host && channel->port_ == port) {
      return channel;
    }
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) return nullptr;
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return nullptr;
  if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    close(fd);
    return nullptr;
  }
  // Disable Nagle's algorithm for low-latency RPC
  int flag = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
  channel = std::make_shared<SharedChannel>(fd, host, port);
  return channel;
}

bool SharedChannel::exchange(std::string_view envelope,
                             std::string_view request_payload,
                             std::string &reply, std::string &payload) {
  Waiter self;
  self.reply = &reply;
  self.payload = &payload;

  std::unique_lock<std::mutex> lock(mutex_);
  if (failed_) return false;
  const uint64_t net_id = htobe64(next_id_);
  waiters_.emplace(next_id_++, &self);
  out_.append(reinterpret_cast<const char *>(&net_id), sizeof(net_id));
  append_len(out_, envelope.size());
  out_.append(envelope);
  append_len(out_, request_payload.size());
  out_.append(request_payload);

  // Send unless a send is in progress, whose sender takes this frame too:
  // each round sends everything appended before it.
  if (!flushing_) {
    flushing_ = true;
    while (!out_.empty() && !failed_) {
      sending_.swap(out_);
      lock.unlock();
      size_t sent = 0;
      int err = 0;
      while (sent < sending_.size()) {
        const ssize_t n = send(fd_, sending_.data() + sent,
                               sending_.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) {
          err = errno;
          break;
        }
        sent += static_cast<size_t>(n);
      }
      const bool ok = sent == sending_.size();
      if (sending_.capacity() > kKeptBufferBytes) {
        std::string().swap(sending_);
      } else {
        sending_.clear();
      }
      lock.lock();
      if (!ok) fail(std::string("send: ") + std::strerror(err));
    }
    flushing_ = false;
  }

  // Wait for the reply, taking the reader role when no one holds it.
  self.waiting = true;
  while (!self.done && !self.lead) {
    if (!reading_) {
      reading_ = true;
      self.lead = true;
    } else {
      self.cv.wait(lock);
    }
  }
  if (!self.lead) return self.ok;

  // As the reader, deliver replies until this session's own is in.
  std::vector<Frame> frames;
  while (!self.done) {
    lock.unlock();

    // Receive until the buffer holds a whole reply. `need` is the size the
    // first incomplete reply is known to have so far.
    frames.clear();
    std::string bad;  // why the channel fails; empty while it does not
    for (;;) {
      size_t need = kFrameHeadBytes;
      while (end_ - begin_ >= kFrameHeadBytes) {
        const char *p = in_.data() + begin_;
        Frame f{};
        uint64_t net;
        std::memcpy(&net, p, sizeof(net));
        f.id = be64toh(net);
        f.env_len = load_len(p + sizeof(net));
        if (f.env_len > kMaxPartBytes) {
          bad = "a reply part over the limit, " + std::to_string(f.env_len) +
                " bytes";
          break;
        }
        const size_t pay_at = kFrameHeadBytes + size_t{f.env_len};
        need = pay_at + sizeof(uint32_t);
        if (end_ - begin_ < need) break;
        f.pay_len = load_len(p + pay_at);
        if (f.pay_len > kMaxPartBytes) {
          bad = "a reply part over the limit, " + std::to_string(f.pay_len) +
                " bytes";
          break;
        }
        need += f.pay_len;
        if (end_ - begin_ < need) break;
        f.env_off = begin_ + kFrameHeadBytes;
        f.pay_off = begin_ + pay_at + sizeof(uint32_t);
        frames.push_back(f);
        begin_ += need;
        need = kFrameHeadBytes;
      }
      if (!frames.empty() || !bad.empty()) break;

      // Make room for the whole incomplete reply, then receive.
      if (begin_ == end_) {
        begin_ = end_ = 0;
        if (in_.size() > kKeptBufferBytes) std::string().swap(in_);
      }
      if (begin_ + need > in_.size()) {
        const size_t unread = end_ - begin_;
        if (need > in_.size()) {
          std::string grown(std::max(need, kInitialBufferBytes), '\0');
          std::memcpy(&grown[0], in_.data() + begin_, unread);
          in_.swap(grown);
        } else {
          std::memmove(&in_[0], in_.data() + begin_, unread);
        }
        begin_ = 0;
        end_ = unread;
      }
      const ssize_t got = recv(fd_, &in_[end_], in_.size() - end_, 0);
      if (got == 0) {
        bad = "the storage server closed the connection";
        break;
      }
      if (got < 0) {
        bad = std::string("recv: ") + std::strerror(errno);
        break;
      }
      end_ += static_cast<size_t>(got);
    }

    // Claim each reply's owner; an unknown id answers nothing sent here.
    lock.lock();
    size_t claimed = 0;
    for (; claimed < frames.size(); ++claimed) {
      const auto it = waiters_.find(frames[claimed].id);
      if (it == waiters_.end()) {
        bad = "a reply to unknown request id " +
              std::to_string(frames[claimed].id);
        break;
      }
      frames[claimed].owner = it->second;
      waiters_.erase(it);
    }

    // Copy without the lock: a claimed owner waits until it is marked done.
    if (claimed != 0) {
      lock.unlock();
      for (size_t i = 0; i < claimed; ++i) {
        const Frame &f = frames[i];
        f.owner->reply->assign(in_.data() + f.env_off, f.env_len);
        f.owner->payload->assign(in_.data() + f.pay_off, f.pay_len);
      }
      lock.lock();
      for (size_t i = 0; i < claimed; ++i) {
        Waiter *owner = frames[i].owner;
        owner->ok = true;
        owner->done = true;
        if (owner != &self) owner->cv.notify_one();
      }
    }
    if (!bad.empty()) fail(bad);
  }

  // Hand the reader role to one waiter still in its receive phase.
  reading_ = false;
  if (failed_) return self.ok;
  for (const auto &entry : waiters_) {
    Waiter *next = entry.second;
    if (next->waiting) {
      next->lead = true;
      reading_ = true;
      next->cv.notify_one();
      break;
    }
  }
  return self.ok;
}

void SharedChannel::fail(const std::string &cause) {
  if (failed_) return;
  failed_ = true;
  LOG_ERROR("The channel to the storage server at %s:%d failed: %s",
            host_.c_str(), port_, cause.c_str());
  shutdown(fd_, SHUT_RDWR);
  for (const auto &entry : waiters_) {
    entry.second->done = true;
    entry.second->cv.notify_one();
  }
  waiters_.clear();
  out_.clear();
}
