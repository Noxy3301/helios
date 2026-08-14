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
#include <utility>

#include "../../common/log.h"
#include "helper_pool.hh"
#include "lineairdb.pb.h"

namespace {

// Upper bound on this reactor's in-flight helper results (see
// completion_slots_used_'s comment in reactor.hh). Generous versus the
// general pool's queue depth + thread count (kHelperQueueDepth=8 +
// kHelperThreads=4 in tcp_server.cc): a reactor can have far more kSlow
// requests dispatched-but-not-yet-completed than the pool can be actively
// running or queuing at once, since "dispatched" also covers results still
// in flight back through deliver_completion().
constexpr int kCompletionSlots = 32;

// How often maybe_log_metrics() may emit a summary line, at most.
constexpr std::chrono::seconds kMetricsLogInterval{60};

// Logs and returns false when a decoded frame header violates the payload
// cap or names an undefined opcode (0, or a gap in the generated enum).
// Shared by the per-header check in parse_and_dispatch.
bool frame_header_ok(int reactor_id, int fd, uint64_t conn_id, uint32_t payload_size,
                      uint32_t raw_type) {
  if (payload_size > kMaxRpcPayloadBytes) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu payload_size=%u exceeds cap %u (opcode=%u); closing",
              reactor_id, fd, conn_id, payload_size, kMaxRpcPayloadBytes, raw_type);
    return false;
  }
  if (!LineairDB::Protocol::OpCode_IsValid(static_cast<int>(raw_type)) || raw_type == 0) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu undefined opcode=%u (payload_size=%u); closing",
              reactor_id, fd, conn_id, raw_type, payload_size);
    return false;
  }
  return true;
}

// Process-unique connection ids, monotonically increasing across all
// reactors and across fd reuse.
std::atomic<uint64_t> g_next_connection_id{1};

const char *lane_name(RpcLane lane) {
  switch (lane) {
    case RpcLane::kFast: return "fast";
    case RpcLane::kSlow: return "slow";
    case RpcLane::kConv: return "conv";
    case RpcLane::kMalformed: return "malformed";
  }
  return "unknown";
}

}  // namespace

Reactor::Reactor(int id, int pin_to_core, ClassifyFn classify, MigrateFn migrate,
                  HelperPool *general_pool, HelperPool *duckdb_pool)
    : id_(id), pin_to_core_(pin_to_core), classify_(std::move(classify)),
      migrate_(std::move(migrate)), general_pool_(general_pool),
      duckdb_pool_(duckdb_pool) {
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

void Reactor::add_connection(int fd, std::shared_ptr<RpcDispatcher> dispatcher) {
  conn_count_.fetch_add(1, std::memory_order_relaxed);
  uint64_t conn_id = g_next_connection_id.fetch_add(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(pending_mu_);
    pending_.push_back(PendingConn{fd, conn_id, std::move(dispatcher)});
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
    conn->connection_id = p.connection_id;
    conn->dispatcher = std::move(p.dispatcher);

    struct epoll_event ev {};
    ev.events = EPOLLIN;
    ev.data.fd = p.fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, p.fd, &ev) != 0) {
      LOG_ERROR("Reactor %d: fd=%d conn=%lu epoll_ctl(ADD) failed: %s",
                id_, p.fd, p.connection_id, std::strerror(errno));
      close(p.fd);
      conn_count_.fetch_sub(1, std::memory_order_relaxed);
      continue;
    }
    connections_.emplace(p.fd, std::move(conn));
    connection_id_to_fd_.emplace(p.connection_id, p.fd);
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
        drain_completions();
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
    // Piggyback on whatever just woke epoll_wait (client traffic, an accept,
    // or a helper completion) instead of a dedicated timer fd: an idle
    // reactor never reaches this line often enough to matter, and one
    // that's busy checks it for free on every iteration anyway.
    maybe_log_metrics();
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
      // Strict request-response, in-flight <= 1: a compliant client never
      // sends more bytes while a response is still owed.
      if (conn.state != State::kArmed) {
        LOG_ERROR("Reactor %d: fd=%d conn=%lu sent %zd bytes while not armed (state=%d); closing",
                  id_, conn.fd, conn.connection_id, n, static_cast<int>(conn.state));
        close_connection(conn.fd);
        return;
      }
      // Reject before appending: serving or migrating a frame that only fits
      // in the buffer because the cap was checked after the fact would let a
      // staged near-max frame plus a pipelined tail bypass the cap entirely.
      size_t current = conn.read_buf.size();
      size_t budget = current < kMaxRpcBufferBytes ? kMaxRpcBufferBytes - current : 0;
      if (static_cast<size_t>(n) > budget) {
        LOG_ERROR("Reactor %d: fd=%d conn=%lu read_buf %zu + chunk %zd bytes would exceed cap %zu; closing",
                  id_, conn.fd, conn.connection_id, current, n, kMaxRpcBufferBytes);
        close_connection(conn.fd);
        return;
      }
      conn.read_buf.append(buf, static_cast<size_t>(n));
      if (!parse_and_dispatch(conn)) return;  // migrated or closed inside
      if (conn.read_buf.size() > kMaxRpcBufferBytes) {
        // Backstop only: the pre-append check above already rejects any
        // chunk that would cross the cap, so this can't trigger on its own.
        LOG_ERROR("Reactor %d: fd=%d conn=%lu read_buf %zu bytes exceeds cap %zu; closing",
                  id_, conn.fd, conn.connection_id, conn.read_buf.size(), kMaxRpcBufferBytes);
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
    LOG_ERROR("Reactor %d: fd=%d conn=%lu recv failed: %s",
              id_, conn.fd, conn.connection_id, std::strerror(errno));
    close_connection(conn.fd);
    return;
  }

  if (!flush_writes(conn)) close_connection(conn.fd);
}

bool Reactor::parse_and_dispatch(Connection &conn) {
  // A dispatched frame always starts at offset 0: any earlier frame on this
  // connection was either erased after dispatch or closed the connection,
  // so read_buf never carries more than one request's worth of bytes across
  // calls.
  const std::string &buf = conn.read_buf;
  if (buf.size() < sizeof(MessageHeader)) return true;  // wait for more bytes

  MessageHeader hdr{};
  std::memcpy(&hdr, buf.data(), sizeof(hdr));
  uint32_t payload_size = ntohl(hdr.payload_size);
  MessageType message_type = static_cast<MessageType>(ntohl(hdr.message_type));

  // Validate length and opcode as soon as the header is visible, even for a
  // frame whose payload has not fully arrived yet.
  if (!frame_header_ok(id_, conn.fd, conn.connection_id, payload_size,
                        static_cast<uint32_t>(message_type))) {
    close_connection(conn.fd);
    return false;
  }

  size_t frame_len = sizeof(MessageHeader) + payload_size;
  if (buf.size() < frame_len) return true;  // wait for the rest of this frame

  uint64_t sender_id = be64toh(hdr.sender_id);

  std::string_view payload_view(buf.data() + sizeof(MessageHeader), payload_size);
  RpcClassification cls = classify_(message_type, payload_view);
  if (cls.lane == RpcLane::kMalformed) {
    // A kFast candidate whose body fails to parse is a protocol violation,
    // not work to hand to a legacy thread.
    LOG_ERROR("Reactor %d: fd=%d conn=%lu malformed body for opcode=%u "
              "(payload_size=%u); closing",
              id_, conn.fd, conn.connection_id, static_cast<uint32_t>(message_type), payload_size);
    close_connection(conn.fd);  // erases conn; do not touch it after this
    return false;
  }
  if (cls.lane == RpcLane::kConv) {
    migrate_connection(conn, 0, frame_len, cls.lane);  // erases conn; do not touch it after this
    return false;
  }
  if (cls.lane == RpcLane::kSlow) {
    // DB_FENCE/DB_CREATE_TABLE/DB_CREATE_SECONDARY_INDEX are exempt from the
    // SERVER_OVERLOADED response below: DDL and fence admission failures
    // migrate instead, because the proxy has no failure-propagation path
    // for these opcodes (a lost CREATE TABLE would look like a silent
    // success).
    bool ddl_exempt = message_type == MessageType::DB_FENCE ||
                       message_type == MessageType::DB_CREATE_TABLE ||
                       message_type == MessageType::DB_CREATE_SECONDARY_INDEX;
    return dispatch_to_helper(conn, message_type, sender_id, frame_len, cls.lane, ddl_exempt);
  }

  // kFast: payload_view stays valid across the call (read_buf isn't touched
  // until the erase below), so classify_rpc's parse is reused here instead
  // of copying the frame out just to reparse it in the handler.
  std::string result;
  if (!conn.dispatcher->handle_fast_rpc(sender_id, message_type, payload_view, cls.parsed.get(),
                                         result)) {
    // Hit its runtime budget (TX_EXECUTE_READ_PLAN only; every other
    // opcode's default handle_fast_rpc always returns true): discard
    // `result` and re-dispatch the same request to the general helper
    // pool, which runs it unbounded. No DDL exemption -- a budget hit
    // never comes from a DDL opcode.
    metrics_.budget_redispatched++;
    return dispatch_to_helper(conn, message_type, sender_id, frame_len, cls.lane,
                               /*ddl_exempt=*/false);
  }
  if (!queue_response(conn, message_type, result)) {
    close_connection(conn.fd);  // erases conn; do not touch it after this
    return false;
  }

  // In-flight <= 1: nothing may follow this request until its response is
  // delivered. Any leftover bytes -- a pipelined next request, or even a
  // partial tail -- are a protocol violation, not more work to do.
  conn.read_buf.erase(0, frame_len);
  if (!conn.read_buf.empty()) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu %zu bytes follow a request before its response "
              "was delivered (pipelining); closing",
              id_, conn.fd, conn.connection_id, conn.read_buf.size());
    close_connection(conn.fd);  // erases conn; do not touch it after this
    return false;
  }
  return true;
}

bool Reactor::queue_response(Connection &conn, MessageType message_type, const std::string &result) {
  // Both caps are enforced before any append: an oversized response must
  // never be allocated into write_buf (nor have its length narrowed into the
  // u32 header field) before the check runs.
  if (result.size() > kMaxRpcPayloadBytes) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu response %zu bytes exceeds frame cap %u",
              id_, conn.fd, conn.connection_id, result.size(), kMaxRpcPayloadBytes);
    return false;
  }
  if (conn.write_buf.size() + sizeof(MessageHeader) + result.size() > kMaxRpcBufferBytes) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu write_buf %zu bytes would exceed cap %zu",
              id_, conn.fd, conn.connection_id,
              conn.write_buf.size() + sizeof(MessageHeader) + result.size(), kMaxRpcBufferBytes);
    return false;
  }

  // Same wire format as MessageHandler::send_response{,_writev} (responses
  // always carry sender_id=0).
  MessageHeader header{};
  header.sender_id = htobe64(0);
  header.message_type = htonl(static_cast<uint32_t>(message_type));
  header.payload_size = htonl(static_cast<uint32_t>(result.size()));
  conn.write_buf.append(reinterpret_cast<const char *>(&header), sizeof(header));
  conn.write_buf.append(result);
  conn.state = State::kResponding;
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
    LOG_ERROR("Reactor %d: fd=%d conn=%lu send failed: %s",
              id_, conn.fd, conn.connection_id, std::strerror(errno));
    return false;
  }

  conn.write_buf.clear();
  conn.write_cursor = 0;
  // Only a drained response re-arms a connection: on_readable() calls
  // flush_writes() after every read regardless of lane, including a kSlow
  // dispatch that queued nothing, so kResponding is the only state this
  // may clear back to kArmed.
  if (conn.state == State::kResponding) conn.state = State::kArmed;
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
  uint64_t conn_id = 0;
  auto it = connections_.find(fd);
  if (it != connections_.end()) {
    conn_id = it->second->connection_id;
    it->second->state = State::kClosing;
  }

  // Best-effort: this fd may already be out of the epoll set (e.g. a
  // migration attempt that failed after its own EPOLL_CTL_DEL).
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0 && errno != EBADF && errno != ENOENT) {
    LOG_WARNING("Reactor %d: fd=%d conn=%lu epoll_ctl(DEL) failed during close: %s",
                id_, fd, conn_id, std::strerror(errno));
  }
  close(fd);
  connections_.erase(fd);
  connection_id_to_fd_.erase(conn_id);
  conn_count_.fetch_sub(1, std::memory_order_relaxed);
  LOG_INFO("Reactor %d: closed connection fd=%d conn=%lu", id_, fd, conn_id);
}

void Reactor::migrate_connection(Connection &conn, size_t frame_start, size_t frame_len,
                                  RpcLane lane) {
  int fd = conn.fd;
  uint64_t conn_id = conn.connection_id;

  // Strict protocol, in-flight <= 1: the handoff primer must be exactly the
  // triggering frame. An undrained response or any trailing bytes in
  // read_buf (a pipelined request, or a partial tail) mean the client broke
  // the contract -- refuse the migration and close instead.
  if (conn.write_cursor < conn.write_buf.size() || frame_start + frame_len != conn.read_buf.size()) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu protocol violation at migration "
              "(write_buf undrained=%zu bytes, read_buf trailing=%zu bytes); closing",
              id_, fd, conn_id, conn.write_buf.size() - conn.write_cursor,
              conn.read_buf.size() - (frame_start + frame_len));
    close_connection(fd);  // erases conn; do not touch it after this
    return;
  }

  // Byte-exact handoff: exactly the frame that forced migration.
  std::string primer = conn.read_buf.substr(frame_start, frame_len);

  // Single owner from here on: detach from this reactor's epoll set before
  // anything else touches the fd.
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) != 0) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu epoll_ctl(DEL) failed during migration: %s; closing",
              id_, fd, conn_id, std::strerror(errno));
    close_connection(fd);  // erases conn; do not touch it after this
    return;
  }

  // Restore blocking mode for the legacy thread's synchronous read/write.
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu fcntl(F_GETFL) failed during migration: %s; closing",
              id_, fd, conn_id, std::strerror(errno));
    close_connection(fd);  // erases conn; do not touch it after this
    return;
  }
  if (fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) != 0) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu fcntl(F_SETFL) failed during migration: %s; closing",
              id_, fd, conn_id, std::strerror(errno));
    close_connection(fd);  // erases conn; do not touch it after this
    return;
  }

  connections_.erase(fd);  // destroys `conn`; nothing below may touch it
  connection_id_to_fd_.erase(conn_id);
  conn_count_.fetch_sub(1, std::memory_order_relaxed);

  LOG_INFO("Reactor %d: migrating fd=%d conn=%lu to a legacy thread (%zu primed bytes) lane=%s",
           id_, fd, conn_id, primer.size(), lane_name(lane));
  migrate_(fd, std::move(primer));
}

bool Reactor::finish_slow_frame(Connection &conn, size_t frame_len) {
  conn.read_buf.erase(0, frame_len);
  if (!conn.read_buf.empty()) {
    LOG_ERROR("Reactor %d: fd=%d conn=%lu %zu bytes follow a slow request before "
              "its response was delivered (pipelining); closing",
              id_, conn.fd, conn.connection_id, conn.read_buf.size());
    close_connection(conn.fd);  // erases conn; do not touch it after this
    return false;
  }
  return true;
}

bool Reactor::dispatch_to_helper(Connection &conn, MessageType message_type, uint64_t sender_id,
                                  size_t frame_len, RpcLane lane, bool ddl_exempt) {
  // Reserve a completion slot before anything else so this reactor can't
  // accumulate more in-flight helper results than it can ever validate
  // (kCompletionSlots, generous vs the pool's queue depth + threads).
  int prev_slots = completion_slots_used_.fetch_add(1, std::memory_order_relaxed);
  if (prev_slots >= kCompletionSlots) {
    completion_slots_used_.fetch_sub(1, std::memory_order_relaxed);
    metrics_.reject_slot++;
    if (ddl_exempt) {
      fallback_migrated_++;
      migrate_connection(conn, 0, frame_len, lane);  // erases conn; do not touch it after this
      return false;
    }
    if (!respond_overloaded(conn)) return false;  // erased conn on failure
    return finish_slow_frame(conn, frame_len);
  }

  // DuckDB requests serialize on the executor's global catalog mutex; its
  // dedicated lane keeps a slow query from parking the general helpers
  // behind that lock.
  HelperPool *pool = message_type == MessageType::TX_EXECUTE_SQL_DUCKDB
                          ? duckdb_pool_
                          : general_pool_;
  // Queue capacity is reserved before the payload copy for the same reason
  // as the completion slot: a reactor shedding load must not pay a large
  // allocation per rejected request.
  if (!pool->try_reserve()) {
    completion_slots_used_.fetch_sub(1, std::memory_order_relaxed);
    metrics_.reject_queue++;
    if (ddl_exempt) {
      fallback_migrated_++;
      migrate_connection(conn, 0, frame_len, lane);  // erases conn; do not touch it after this
      return false;
    }
    if (!respond_overloaded(conn)) return false;  // erased conn on failure
    return finish_slow_frame(conn, frame_len);
  }

  conn.generation++;  // the completion must match this dispatch's generation

  HelperPool::Job job;
  job.connection_id = conn.connection_id;
  job.generation = conn.generation;
  job.type = message_type;
  job.sender_id = sender_id;
  job.payload = conn.read_buf.substr(sizeof(MessageHeader), frame_len - sizeof(MessageHeader));
  job.dispatcher = conn.dispatcher;  // shared_ptr copy: co-owns across the helper job
  job.owner = this;
  pool->submit(std::move(job));

  metrics_.dispatched++;
  if (!finish_slow_frame(conn, frame_len)) return false;  // erased conn on violation
  conn.state = State::kInHelper;
  return true;
}

bool Reactor::respond_overloaded(Connection &conn) {
  metrics_.overload_sent++;
  if (!queue_response(conn, MessageType::SERVER_OVERLOADED, std::string())) {
    close_connection(conn.fd);  // erases conn; do not touch it after this
    return false;
  }
  if (!flush_writes(conn)) {
    close_connection(conn.fd);  // erases conn; do not touch it after this
    return false;
  }
  return true;
}

void Reactor::deliver_completion(uint64_t connection_id, uint64_t generation, MessageType type,
                                  std::string result) {
  {
    std::lock_guard<std::mutex> lock(completion_mu_);
    completions_.push_back(Completion{connection_id, generation, type, std::move(result)});
  }
  uint64_t one = 1;
  if (write(wake_fd_, &one, sizeof(one)) < 0 && errno != EAGAIN) {
    LOG_ERROR("Reactor %d: eventfd wake write failed (completion delivery): %s",
              id_, std::strerror(errno));
  }
}

void Reactor::drain_completions() {
  std::deque<Completion> batch;
  {
    std::lock_guard<std::mutex> lock(completion_mu_);
    batch.swap(completions_);
  }

  for (auto &c : batch) {
    // Always released here: whether or not this completion matches a live
    // connection, the slot it was reserved under becomes free (see
    // dispatch_to_helper() for the reservation).
    completion_slots_used_.fetch_sub(1, std::memory_order_relaxed);

    auto fd_it = connection_id_to_fd_.find(c.connection_id);
    Connection *conn = nullptr;
    if (fd_it != connection_id_to_fd_.end()) {
      auto conn_it = connections_.find(fd_it->second);
      if (conn_it != connections_.end()) conn = conn_it->second.get();
    }

    // A completion is honored only if it matches a live connection's
    // identity, generation, and kInHelper state. A closed connection (fd
    // reused or not), a migrated connection, or a generation mismatch all
    // fall here and are discarded.
    if (conn == nullptr || conn->connection_id != c.connection_id ||
        conn->generation != c.generation || conn->state != State::kInHelper) {
      metrics_.stale_discarded++;
      LOG_INFO("Reactor %d: stale completion discarded conn=%lu generation=%lu "
               "(connection %s)",
               id_, c.connection_id, c.generation,
               conn == nullptr ? "not found" : "generation/state mismatch");
      continue;
    }

    metrics_.completed++;
    if (!queue_response(*conn, c.type, c.result)) {
      close_connection(conn->fd);  // erases conn; do not touch it after this
      continue;
    }
    if (!flush_writes(*conn)) {
      close_connection(conn->fd);  // erases conn; do not touch it after this
      continue;
    }
  }
}

void Reactor::maybe_log_metrics() {
  if (metrics_ == last_logged_metrics_) return;  // nothing to report

  auto now = std::chrono::steady_clock::now();
  if (now - last_metrics_log_ < kMetricsLogInterval) return;

  LOG_INFO("Reactor %d: helper metrics dispatched=%lu completed=%lu reject_queue=%lu "
           "reject_slot=%lu overload_sent=%lu stale_discarded=%lu budget_redispatched=%lu",
           id_, metrics_.dispatched, metrics_.completed, metrics_.reject_queue,
           metrics_.reject_slot, metrics_.overload_sent, metrics_.stale_discarded,
           metrics_.budget_redispatched);
  last_metrics_log_ = now;
  last_logged_metrics_ = metrics_;
}
