#ifndef HELIOS_NETWORK_REACTOR_HH
#define HELIOS_NETWORK_REACTOR_HH

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../protocol/message.hh"
#include "rpc_lane.hh"

class HelperPool;  // helper_pool.hh; only a pointer member is needed here,
                    // avoiding a Reactor <-> HelperPool header cycle (see
                    // helper_pool.hh, which includes this header fully).

// Minimal epoll multi-reactor transport, opt-in via HELIOS_TRANSPORT=reactor
// (see tcp_server.cc). Each Reactor owns one epoll fd, one physical-core-
// pinned thread, and a set of non-blocking connections.
//
// The wire protocol is strict request-response with in-flight <= 1 per
// connection, enforced here: at most one complete frame is dispatched per
// read, and any bytes trailing it (a pipelined next request, or a partial
// tail) are a protocol violation that closes the connection. Every frame is
// classified (TcpServer::classify_rpc, see rpc_lane.hh) once its header and
// payload are fully buffered: kFast runs inline on this thread via the
// connection's RpcDispatcher and its response is buffered/flushed here.
// kSlow is handed to the fixed HelperPool (helper_pool.hh) instead of
// running inline: the connection stays owned by this reactor (fd stays in
// the epoll set, state becomes kInHelper) while a helper thread computes the
// result and delivers it back via deliver_completion(), which this reactor
// validates before turning it into a response. kConv triggers a one-way
// migration: EPOLL_CTL_DEL, restore blocking mode, hand the fd plus
// a byte-exact primer (exactly the triggering frame -- the strictness check
// guarantees nothing follows it) to a dedicated legacy thread; this reactor
// never touches that fd again after that. kMalformed (a kFast candidate
// whose body fails to parse) closes the connection instead of migrating.
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

  using ClassifyFn = std::function<RpcLane(MessageType, std::string_view)>;
  // Spawns the legacy per-connection thread for `fd`, primed with `primer`
  // bytes already read off the wire (see TcpServer::spawn_legacy_thread).
  using MigrateFn = std::function<void(int fd, std::string primer)>;

  Reactor(int id, int pin_to_core, ClassifyFn classify, MigrateFn migrate,
          HelperPool *general_pool, HelperPool *duckdb_pool);
  ~Reactor();

  Reactor(const Reactor &) = delete;
  Reactor &operator=(const Reactor &) = delete;

  // Approximate connection count, used only for accept-time load balancing
  // (fewest connections wins); safe to call from any thread.
  int connection_count() const { return conn_count_.load(std::memory_order_relaxed); }

  // Hands off an already-accepted, already-non-blocking socket. Safe to call
  // from any thread (the accept thread). `dispatcher` becomes this
  // connection's RPC handler, on both the fast path (this reactor thread)
  // and the kSlow path (a helper-pool thread). A shared_ptr, not
  // unique_ptr: a helper Job co-owns it while it runs, so a connection close
  // mid-job can't dangle it -- strict in-flight <= 1 per connection
  // guarantees the dispatcher is never used by two threads at once
  // regardless of who holds a reference to it.
  void add_connection(int fd, std::shared_ptr<RpcDispatcher> dispatcher);

  // Thread-safe; called from helper-pool worker threads (see helper_pool.hh)
  // once a kSlow job finishes. Queues {connection_id, generation, type,
  // result} for this reactor's own thread to validate and turn into a
  // response; never touches the fd/Connection itself.
  void deliver_completion(uint64_t connection_id, uint64_t generation, MessageType type,
                           std::string result);

 private:
  // kArmed: idle, no response owed; the only state in which inbound bytes
  // are legal. kResponding: a response is queued and/or still flushing.
  // kInHelper: a kSlow op for this connection is executing on the helper
  // pool; inbound bytes are a protocol violation exactly like kResponding
  // (both are "not kArmed"). kClosing: close_connection has claimed this
  // connection (terminal).
  enum class State { kArmed, kResponding, kInHelper, kClosing };

  struct Connection {
    int fd = -1;
    uint64_t connection_id = 0;  // process-unique, assigned in add_connection
    // Advances once per kSlow admission attempt that passes completion-slot
    // reservation, including attempts the helper queue rejects;
    // drain_completions() checks a completion's generation against the
    // current value to discard stale results.
    uint64_t generation = 0;
    State state = State::kArmed;
    std::string read_buf;
    std::string write_buf;
    size_t write_cursor = 0;  // bytes in write_buf[0, write_cursor) already sent
    bool want_epollout = false;
    std::shared_ptr<RpcDispatcher> dispatcher;
  };

  struct PendingConn {
    int fd;
    uint64_t connection_id;
    std::shared_ptr<RpcDispatcher> dispatcher;
  };

  // One helper-pool result in transit back to this reactor. Carries
  // connection_id, not fd: a completion can arrive after the fd was closed
  // and reused by an unrelated accept, so fd would be an aliasing hazard
  // (see connection_id_to_fd_ below).
  struct Completion {
    uint64_t connection_id;
    uint64_t generation;
    MessageType type;
    std::string result;
  };

  // Periodic helper-pool metrics summary (see maybe_log_metrics()).
  struct Metrics {
    uint64_t dispatched = 0;
    uint64_t completed = 0;
    uint64_t reject_queue = 0;
    uint64_t reject_slot = 0;
    uint64_t overload_sent = 0;
    uint64_t stale_discarded = 0;

    bool operator==(const Metrics &o) const {
      return dispatched == o.dispatched && completed == o.completed &&
             reject_queue == o.reject_queue && reject_slot == o.reject_slot &&
             overload_sent == o.overload_sent && stale_discarded == o.stale_discarded;
    }
    bool operator!=(const Metrics &o) const { return !(*this == o); }
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
  // conn.read_buf; `lane` (kSlow or kConv) is logged so slow/conv routing
  // is observable. Erases conn from this reactor and hands the fd off --
  // unless the handoff would not be byte-exact (undrained write_buf or
  // trailing read_buf bytes), in which case it closes instead.
  void migrate_connection(Connection &conn, size_t frame_start, size_t frame_len, RpcLane lane);
  // Erases the just-dispatched frame from read_buf and enforces in-flight
  // <= 1 for a kSlow request: leftover bytes (a pipelined next request or a
  // partial tail) are a protocol violation, same discipline as the fast
  // path. Returns false (having closed the connection) on violation.
  bool finish_slow_frame(Connection &conn, size_t frame_len);
  // Sends a SERVER_OVERLOADED response (empty payload) through the normal
  // queue_response + flush_writes path; state round-trips kResponding ->
  // kArmed like any other response. Returns false if the connection was
  // closed (queue_response/flush_writes failure) -- caller must not touch
  // `conn` again in that case.
  bool respond_overloaded(Connection &conn);
  // Drains helper-pool completions queued by deliver_completion(), matching
  // each against connections_ (via connection_id_to_fd_, since a completion
  // can't carry an fd safely -- see Completion's comment) before turning it
  // into a response.
  void drain_completions();
  // Logs one INFO summary line of Metrics, at most once per 60s and only
  // when something changed since the last line (see reactor.cc). No timer
  // fd: piggybacks on whatever epoll_wait wakeup is already happening, so an
  // idle reactor logs nothing.
  void maybe_log_metrics();

  int id_;
  int pin_to_core_;
  int epoll_fd_ = -1;
  int wake_fd_ = -1;  // eventfd: accept thread and helper threads nudge epoll_wait
  ClassifyFn classify_;
  MigrateFn migrate_;
  HelperPool *general_pool_ = nullptr;  // not owned; lives for the process lifetime
  HelperPool *duckdb_pool_ = nullptr;   // not owned; dedicated TX_EXECUTE_SQL_DUCKDB lane

  std::atomic<int> conn_count_{0};
  // Reactor-thread-only after drain_pending_adds() inserts an entry.
  std::unordered_map<int, std::unique_ptr<Connection>> connections_;
  // Reactor-thread-only, kept in lockstep with connections_ (same
  // inserts/erases): lets drain_completions() find a connection by the id a
  // completion carries without risking an fd-reuse aliasing hazard.
  std::unordered_map<uint64_t, int> connection_id_to_fd_;

  std::mutex pending_mu_;
  std::vector<PendingConn> pending_;

  // Upper bound on this reactor's in-flight helper results (kSlow dispatches
  // whose completion hasn't been matched yet), generous versus the pool's
  // queue depth + thread count; see reactor.cc. Reservation happens in
  // parse_and_dispatch, release in drain_completions -- both
  // reactor-thread-only, but atomic per the ownership rules for this
  // counter's role as an admission gate.
  std::atomic<int> completion_slots_used_{0};
  std::mutex completion_mu_;
  std::deque<Completion> completions_;

  // Reactor-thread-only.
  Metrics metrics_;
  Metrics last_logged_metrics_;
  std::chrono::steady_clock::time_point last_metrics_log_ = std::chrono::steady_clock::now();
  // DDL (DB_FENCE/DB_CREATE_TABLE/DB_CREATE_SECONDARY_INDEX) kSlow requests
  // that fell back to migration under overload instead of being dropped
  // (see the rejection-policy comment in reactor.cc); not part of Metrics,
  // since the existing per-event migration log line already covers it.
  uint64_t fallback_migrated_ = 0;

  std::thread thread_;
  std::atomic<bool> stop_{false};
};

#endif  // HELIOS_NETWORK_REACTOR_HH
