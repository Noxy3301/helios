#include "reactor.hh"

#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "../../common/log.h"
#include "lineairdb.pb.h"

namespace {

// Logs and returns false when a decoded frame header violates the payload
// cap or names an undefined opcode (0, or a gap in the generated enum).
// Shared by the per-header check inside the parse loop.
bool frame_header_ok(int reactor_id, int fd, uint32_t payload_size, uint32_t raw_type) {
  if (payload_size > kMaxRpcPayloadBytes) {
    LOG_ERROR("Reactor %d: fd=%d payload_size=%u exceeds cap %u (opcode=%u); closing",
              reactor_id, fd, payload_size, kMaxRpcPayloadBytes, raw_type);
    return false;
  }
  if (!LineairDB::Protocol::OpCode_IsValid(static_cast<int>(raw_type)) || raw_type == 0) {
    LOG_ERROR("Reactor %d: fd=%d undefined opcode=%u (payload_size=%u); closing",
              reactor_id, fd, raw_type, payload_size);
    return false;
  }
  return true;
}

}  // namespace

Reactor::Reactor(int id, int pin_to_core, IsFastPathFn is_fast_path, MigrateFn migrate)
    : id_(id), pin_to_core_(pin_to_core), is_fast_path_(std::move(is_fast_path)),
      migrate_(std::move(migrate)) {
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ < 0) {
    LOG_FATAL("Reactor %d: epoll_create1 failed: %s", id_, std::strerror(errno));
  }
  wake_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    LOG_FATAL("Reactor %d: eventfd failed: %s", id_, std::strerror(errno));
  }
  struct epoll_event ev {};
  ev.events = EPOLLIN;
  ev.data.fd = wake_fd_;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) != 0) {
    LOG_FATAL("Reactor %d: epoll_ctl(ADD wake_fd) failed: %s", id_, std::strerror(errno));
  }
  thread_ = std::thread(&Reactor::run, this);
}

Reactor::~Reactor() {
  // Not expected to run in practice: TcpServer::accept_clients_reactor owns
  // these in a vector alongside a `while (true)` accept loop that never
  // returns. Best-effort teardown only.
  stop_.store(true, std::memory_order_relaxed);
  if (thread_.joinable()) thread_.detach();
  if (epoll_fd_ >= 0) close(epoll_fd_);
  if (wake_fd_ >= 0) close(wake_fd_);
}

void Reactor::add_connection(int fd, std::unique_ptr<RpcDispatcher> dispatcher) {
  conn_count_.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(pending_mu_);
    pending_.push_back(PendingConn{fd, std::move(dispatcher)});
  }
  uint64_t one = 1;
  if (write(wake_fd_, &one, sizeof(one)) < 0 && errno != EAGAIN) {
    LOG_ERROR("Reactor %d: eventfd wake write failed: %s", id_, std::strerror(errno));
  }
}

void Reactor::drain_pending_adds() {
  std::vector<PendingConn> pending;
  {
    std::lock_guard<std::mutex> lock(pending_mu_);
    pending.swap(pending_);
  }
  for (auto &p : pending) {
    auto conn = std::make_unique<Connection>();
    conn->fd = p.fd;
    conn->dispatcher = std::move(p.dispatcher);

    struct epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = p.fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, p.fd, &ev) != 0) {
      LOG_ERROR("Reactor %d: epoll_ctl(ADD fd=%d) failed: %s", id_, p.fd, std::strerror(errno));
      close(p.fd);
      conn_count_.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }
    connections_.emplace(p.fd, std::move(conn));
  }
}

void Reactor::run() {
  if (pin_to_core_ >= 0) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(pin_to_core_, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      LOG_WARNING("Reactor %d: pthread_setaffinity_np(core=%d) failed: %s",
                  id_, pin_to_core_, std::strerror(rc));
    }
  }

  constexpr int kMaxEvents = 256;
  std::vector<struct epoll_event> events(kMaxEvents);

  while (!stop_.load(std::memory_order_relaxed)) {
    int n = epoll_wait(epoll_fd_, events.data(), kMaxEvents, -1);
    if (n < 0) {
      if (errno == EINTR) continue;
      LOG_ERROR("Reactor %d: epoll_wait failed: %s", id_, std::strerror(errno));
      break;
    }
    for (int i = 0; i < n; i++) {
      int fd = events[i].data.fd;
      uint32_t ev = events[i].events;

      if (fd == wake_fd_) {
        uint64_t ignore;
        while (read(wake_fd_, &ignore, sizeof(ignore)) > 0) {
        }
        drain_pending_adds();
        continue;
      }

      auto it = connections_.find(fd);
      if (it == connections_.end()) continue;  // already closed/migrated earlier this batch
      Connection &conn = *it->second;

      if (ev & (EPOLLHUP | EPOLLERR)) {
        close_connection(fd);
        continue;
      }
      if (ev & EPOLLOUT) {
        if (!flush_writes(conn)) {
          close_connection(fd);
          continue;
        }
      }
      if (ev & EPOLLIN) {
        on_readable(conn);
      }
    }
  }
}

void Reactor::on_readable(Connection &conn) {
  // Drain everything currently available (level-triggered epoll will simply
  // re-report readiness if more arrives before the next wakeup). Each chunk
  // is size-checked against the buffer cap before it is appended, then
  // parsed: every header is validated the moment it becomes visible, so
  // after a parse the buffer holds at most one partial frame whose header
  // already passed validation -- a bogus length can never keep streaming
  // bytes into the buffer.
  char buf[65536];
  while (true) {
    ssize_t n = recv(conn.fd, buf, sizeof(buf), 0);
    if (n > 0) {
      // Reject before appending: serving or migrating a frame that only fits
      // in the buffer because the cap was checked after the fact would let a
      // staged near-max frame plus a pipelined tail bypass the cap entirely.
      size_t current = conn.read_buf.size();
      size_t budget = current < kMaxRpcBufferBytes ? kMaxRpcBufferBytes - current : 0;
      if (static_cast<size_t>(n) > budget) {
        LOG_ERROR("Reactor %d: fd=%d read_buf %zu + chunk %zd bytes would exceed cap %zu; closing",
                  id_, conn.fd, current, n, kMaxRpcBufferBytes);
        close_connection(conn.fd);
        return;
      }
      conn.read_buf.append(buf, static_cast<size_t>(n));
      if (!parse_and_dispatch(conn)) return;  // migrated or closed inside
      if (conn.read_buf.size() > kMaxRpcBufferBytes) {
        // Backstop only: the pre-append check above already rejects any
        // chunk that would cross the cap, so this can't trigger on its own.
        LOG_ERROR("Reactor %d: fd=%d read_buf %zu bytes exceeds cap %zu; closing",
                  id_, conn.fd, conn.read_buf.size(), kMaxRpcBufferBytes);
        close_connection(conn.fd);
        return;
      }
      continue;
    }
    if (n == 0) {  // peer closed
      close_connection(conn.fd);
      return;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
    if (errno == EINTR) continue;
    LOG_ERROR("Reactor %d: recv fd=%d failed: %s", id_, conn.fd, std::strerror(errno));
    close_connection(conn.fd);
    return;
  }

  if (!flush_writes(conn)) close_connection(conn.fd);
}

bool Reactor::parse_and_dispatch(Connection &conn) {
  size_t cursor = 0;
  const std::string &buf = conn.read_buf;

  while (true) {
    size_t remaining = buf.size() - cursor;
    if (remaining < sizeof(MessageHeader)) break;

    MessageHeader hdr{};
    std::memcpy(&hdr, buf.data() + cursor, sizeof(hdr));
    uint32_t payload_size = ntohl(hdr.payload_size);
    MessageType message_type = static_cast<MessageType>(ntohl(hdr.message_type));

    // Validate length and opcode as soon as the header is visible, even for
    // a frame whose payload has not fully arrived yet.
    if (!frame_header_ok(id_, conn.fd, payload_size, static_cast<uint32_t>(message_type))) {
      close_connection(conn.fd);
      return false;
    }

    size_t frame_len = sizeof(MessageHeader) + payload_size;
    if (remaining < frame_len) break;  // wait for more bytes

    uint64_t sender_id = be64toh(hdr.sender_id);

    if (!is_fast_path_(message_type)) {
      migrate_connection(conn, cursor);  // erases conn; do not touch it after this
      return false;
    }

    std::string payload = buf.substr(cursor + sizeof(MessageHeader), payload_size);
    std::string result;
    conn.dispatcher->handle_rpc(sender_id, message_type, payload, result);
    if (!queue_response(conn, message_type, result)) {
      close_connection(conn.fd);  // erases conn; do not touch it after this
      return false;
    }

    cursor += frame_len;
  }

  if (cursor > 0) conn.read_buf.erase(0, cursor);
  return true;
}

bool Reactor::queue_response(Connection &conn, MessageType message_type, const std::string &result) {
  // Both caps are enforced before any append: an oversized response must
  // never be allocated into write_buf (nor have its length narrowed into the
  // u32 header field) before the check runs.
  if (result.size() > kMaxRpcPayloadBytes) {
    LOG_ERROR("Reactor %d: fd=%d response %zu bytes exceeds frame cap %u",
              id_, conn.fd, result.size(), kMaxRpcPayloadBytes);
    return false;
  }
  if (conn.write_buf.size() + sizeof(MessageHeader) + result.size() > kMaxRpcBufferBytes) {
    LOG_ERROR("Reactor %d: fd=%d write_buf %zu bytes would exceed cap %zu",
              id_, conn.fd, conn.write_buf.size() + sizeof(MessageHeader) + result.size(),
              kMaxRpcBufferBytes);
    return false;
  }

  // Same wire format as MessageHandler::send_response{,_writev} (responses
  // always carry sender_id=0); buffered so a burst of fast-path frames from
  // one wakeup shares a single flush.
  MessageHeader header{};
  header.sender_id = htobe64(0);
  header.message_type = htonl(static_cast<uint32_t>(message_type));
  header.payload_size = htonl(static_cast<uint32_t>(result.size()));
  conn.write_buf.append(reinterpret_cast<const char *>(&header), sizeof(header));
  conn.write_buf.append(result);
  return true;
}

bool Reactor::flush_writes(Connection &conn) {
  while (conn.write_cursor < conn.write_buf.size()) {
    ssize_t n = send(conn.fd, conn.write_buf.data() + conn.write_cursor,
                      conn.write_buf.size() - conn.write_cursor, MSG_NOSIGNAL);
    if (n > 0) {
      conn.write_cursor += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      if (!conn.want_epollout) {
        struct epoll_event ev {};
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.fd = conn.fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &ev);
        conn.want_epollout = true;
      }
      return true;  // partial write buffered, not an error
    }
    LOG_ERROR("Reactor %d: send fd=%d failed: %s", id_, conn.fd, std::strerror(errno));
    return false;
  }

  conn.write_buf.clear();
  conn.write_cursor = 0;
  if (conn.want_epollout) {
    struct epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = conn.fd;
    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &ev);
    conn.want_epollout = false;
  }
  return true;
}

void Reactor::close_connection(int fd) {
  epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
  close(fd);
  connections_.erase(fd);
  conn_count_.fetch_sub(1, std::memory_order_relaxed);
  LOG_INFO("Reactor %d: closed connection fd=%d", id_, fd);
}

void Reactor::migrate_connection(Connection &conn, size_t frame_start) {
  int fd = conn.fd;
  // Byte-exact handoff: the frame that forced migration plus whatever else
  // was already read after it.
  std::string primer = conn.read_buf.substr(frame_start);

  // Single owner from here on: detach from this reactor's epoll set before
  // anything else touches the fd.
  epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);

  // Restore blocking mode, then best-effort flush whatever this reactor
  // already owes for earlier frames in this same batch -- the legacy
  // thread's first read must be the primer, not a race with our own
  // trailing writes.
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags >= 0) fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);

  size_t off = conn.write_cursor;
  while (off < conn.write_buf.size()) {
    ssize_t n = send(fd, conn.write_buf.data() + off, conn.write_buf.size() - off, MSG_NOSIGNAL);
    if (n > 0) {
      off += static_cast<size_t>(n);
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    LOG_ERROR("Reactor %d: pre-migration flush failed fd=%d: %s", id_, fd, std::strerror(errno));
    break;  // best-effort; hand off regardless, legacy loop will surface any real break on recv
  }

  connections_.erase(fd);  // destroys `conn`; nothing below may touch it
  conn_count_.fetch_sub(1, std::memory_order_relaxed);

  LOG_INFO("Reactor %d: migrating fd=%d to a legacy thread (%zu primed bytes)",
           id_, fd, primer.size());
  migrate_(fd, std::move(primer));
}
