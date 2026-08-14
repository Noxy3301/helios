#ifndef HELIOS_NETWORK_HELPER_POOL_HH
#define HELIOS_NETWORK_HELPER_POOL_HH

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../protocol/message.hh"
#include "reactor.hh"

/**
 * @brief Fixed pool of worker threads that runs slow single-shot RPCs off
 * the reactor threads and hands each result back through the owning
 * reactor's completion queue.
 *
 * Ownership stays split exactly like the reactor: a worker touches only
 * Job::dispatcher->handle_rpc; the connection's fd, epoll registration, and
 * write buffer stay reactor-thread-only.
 */
class HelperPool {
 public:
  // One unit of off-reactor-thread work: everything a worker needs to run
  // the RPC and everything Reactor::deliver_completion needs to route the
  // result back to the right connection on the right reactor, or discard it
  // safely if that connection is gone by the time the result comes back.
  struct Job {
    uint64_t connection_id;
    uint64_t generation;
    MessageType type;
    uint64_t sender_id;
    std::string payload;
    // Co-owns the dispatcher with the reactor's Connection so a connection
    // close mid-job can't dangle it; strict in-flight <= 1 per connection
    // guarantees no other thread touches this dispatcher while the job
    // runs.
    std::shared_ptr<Reactor::RpcDispatcher> dispatcher;
    Reactor *owner;
  };

  struct Stats {
    uint64_t submitted;
    uint64_t rejected;
    uint64_t completed;
    uint64_t queue_depth;
  };

  // `name` seeds worker thread names ("<name>-<index>", truncation-safe).
  HelperPool(const char *name, unsigned num_threads, size_t queue_depth);
  ~HelperPool();

  HelperPool(const HelperPool &) = delete;
  HelperPool &operator=(const HelperPool &) = delete;

  // Two-phase admission so callers can defer the (possibly large) payload
  // copy until capacity is guaranteed: try_reserve() takes one unit of
  // queue capacity (false = full, nothing charged), submit() consumes a
  // held reservation and never fails. Reservations count against
  // queue_depth alongside queued jobs.
  bool try_reserve();
  void submit(Job job);

  Stats snapshot() const;

 private:
  void worker_loop(unsigned index);

  std::string name_;
  size_t queue_depth_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Job> queue_;
  size_t reserved_ = 0;  // admissions granted but not yet submitted
  bool stop_ = false;

  std::vector<std::thread> workers_;

  std::atomic<uint64_t> submitted_{0};
  std::atomic<uint64_t> rejected_{0};
  std::atomic<uint64_t> completed_{0};
};

#endif  // HELIOS_NETWORK_HELPER_POOL_HH
