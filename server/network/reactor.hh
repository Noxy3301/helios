#ifndef HELIOS_NETWORK_REACTOR_HH
#define HELIOS_NETWORK_REACTOR_HH

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../protocol/message.hh"

// Minimal epoll multi-reactor transport, opt-in via HELIOS_TRANSPORT=reactor
// (see tcp_server.cc). Each Reactor owns one epoll fd, one physical-core-
// pinned thread, and a set of non-blocking connections.
//
// The wire protocol is strict request-response with in-flight <= 1 per
// connection, enforced here: at most one complete frame is dispatched per
// read, and any bytes trailing it (a pipelined next request, or a partial
// tail) are a protocol violation that closes the connection. Fast-path
// opcodes (TcpServer::is_reactor_fast_path) run inline on this thread via
// the connection's RpcDispatcher and their responses are buffered/flushed
// here. Any other opcode triggers a one-way migration: EPOLL_CTL_DEL,
// restore blocking mode, hand the fd plus a byte-exact primer (exactly the
// triggering frame -- the strictness check guarantees nothing follows it)
// to a dedicated legacy thread. This reactor never touches the fd again
// after that.
//
// There is no lane classification here; the inline fast set is deliberately
// narrow (two stateless opcodes), so every conversational opcode migrates.
class Reactor {
 public:
  // RPC dispatch surface a connection uses on the fast path. One instance is
  // created per accepted connection (TcpServer::create_dispatcher) and
  // reused for every fast-path frame on that connection.
  class RpcDispatcher {
   public:
    virtual ~RpcDispatcher() = default;
    virtual void handle_rpc(uint64_t sender_id, MessageType message_type,
                             const std::string &payload, std::string &result) = 0;
  };

  using IsFastPathFn = std::function<bool(MessageType)>;
  // Spawns the legacy per-connection thread for `fd`, primed with `primer`
  // bytes already read off the wire (see TcpServer::spawn_legacy_thread).
  using MigrateFn = std::function<void(int fd, std::string primer)>;

  Reactor(int id, int pin_to_core, IsFastPathFn is_fast_path, MigrateFn migrate);
  ~Reactor();

  Reactor(const Reactor &) = delete;
  Reactor &operator=(const Reactor &) = delete;

  // Approximate connection count, used only for accept-time load balancing
  // (fewest connections wins); safe to call from any thread.
  int connection_count() const { return conn_count_.load(std::memory_order_relaxed); }

  // Hands off an already-accepted, already-non-blocking socket. Safe to call
  // from any thread (the accept thread). `dispatcher` becomes this
  // connection's fast-path RPC handler.
  void add_connection(int fd, std::unique_ptr<RpcDispatcher> dispatcher);

 private:
  // kArmed: idle, no response owed; the only state in which inbound bytes
  // are legal. kResponding: a response is queued and/or still flushing.
  // kClosing: close_connection has claimed this connection (terminal).
  enum class State { kArmed, kResponding, kClosing };

  struct Connection {
    int fd = -1;
    uint64_t connection_id = 0;  // process-unique, assigned in add_connection
    uint64_t generation = 0;     // reserved: revalidates completions against
                                 // connection_id once work can leave this
                                 // thread; nothing bumps it yet
    State state = State::kArmed;
    std::string read_buf;
    std::string write_buf;
    size_t write_cursor = 0;  // bytes in write_buf[0, write_cursor) already sent
    bool want_epollout = false;
    std::unique_ptr<RpcDispatcher> dispatcher;
  };

  struct PendingConn {
    int fd;
    uint64_t connection_id;
    std::unique_ptr<RpcDispatcher> dispatcher;
  };

  void run();
  void drain_pending_adds();
  void on_readable(Connection &conn);
  // Parses and dispatches at most one complete frame from read_buf: the
  // wire protocol is strict request-response with in-flight <= 1, so any
  // bytes left after that frame (a pipelined request, or even a partial
  // tail) are a protocol violation, not more work to do. Returns false if
  // the connection is no longer owned by this reactor (migrated or closed)
  // -- caller must not touch `conn` again in that case.
  bool parse_and_dispatch(Connection &conn);
  // Buffers a response frame. Returns false if that would push
  // conn.write_buf over the frame-buffer cap, in which case the caller must
  // close the connection and not touch conn.write_buf again.
  bool queue_response(Connection &conn, MessageType message_type, const std::string &result);
  // Tries to flush conn.write_buf; arms/disarms EPOLLOUT as needed. Returns
  // false if the connection should be closed.
  bool flush_writes(Connection &conn);
  void close_connection(int fd);
  // `frame_start`/`frame_len` locate the frame that forced migration inside
  // conn.read_buf. Erases conn from this reactor and hands the fd off --
  // unless the handoff would not be byte-exact (undrained write_buf or
  // trailing read_buf bytes), in which case it closes instead.
  void migrate_connection(Connection &conn, size_t frame_start, size_t frame_len);

  int id_;
  int pin_to_core_;
  int epoll_fd_ = -1;
  int wake_fd_ = -1;  // eventfd: accept thread nudges epoll_wait after add_connection
  IsFastPathFn is_fast_path_;
  MigrateFn migrate_;

  std::atomic<int> conn_count_{0};
  // Reactor-thread-only after drain_pending_adds() inserts an entry.
  std::unordered_map<int, std::unique_ptr<Connection>> connections_;

  std::mutex pending_mu_;
  std::vector<PendingConn> pending_;

  std::thread thread_;
  std::atomic<bool> stop_{false};
};

#endif  // HELIOS_NETWORK_REACTOR_HH
