#include "helper_pool.hh"

#include <pthread.h>

#include <cstdio>
#include <utility>

HelperPool::HelperPool(const char *name, unsigned num_threads, size_t queue_depth)
    : name_(name), queue_depth_(queue_depth) {
  workers_.reserve(num_threads);
  for (unsigned i = 0; i < num_threads; i++) {
    workers_.emplace_back(&HelperPool::worker_loop, this, i);
  }
}

HelperPool::~HelperPool() {
  // Best-effort teardown, mirroring Reactor's: TcpServer::accept_clients_reactor's
  // accept loop never returns, so the pool lives for the process lifetime in
  // practice and this destructor isn't expected to run.
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto &t : workers_) {
    if (t.joinable()) t.join();
  }
}

bool HelperPool::try_reserve() {
  std::lock_guard<std::mutex> lock(mu_);
  if (queue_.size() + reserved_ >= queue_depth_) {
    rejected_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  reserved_++;
  return true;
}

void HelperPool::submit(Job job) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    reserved_--;
    queue_.push_back(std::move(job));
    // Inside the lock so snapshot() never sees completed ahead of submitted.
    submitted_.fetch_add(1, std::memory_order_relaxed);
  }
  cv_.notify_one();
}

HelperPool::Stats HelperPool::snapshot() const {
  size_t depth;
  {
    std::lock_guard<std::mutex> lock(mu_);
    depth = queue_.size();
  }
  return Stats{submitted_.load(std::memory_order_relaxed),
               rejected_.load(std::memory_order_relaxed),
               completed_.load(std::memory_order_relaxed), depth};
}

void HelperPool::worker_loop(unsigned index) {
  // Truncation-safe: snprintf always null-terminates within the 16-byte
  // Linux thread-name limit, even if name_ + index don't fit.
  char thread_name[16];
  std::snprintf(thread_name, sizeof(thread_name), "%s-%u", name_.c_str(), index);
  pthread_setname_np(pthread_self(), thread_name);

  while (true) {
    Job job;
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
      if (queue_.empty()) {
        if (stop_) return;
        continue;
      }
      job = std::move(queue_.front());
      queue_.pop_front();
    }

    std::string result;
    job.dispatcher->handle_rpc(job.sender_id, job.type, job.payload, result);
    completed_.fetch_add(1, std::memory_order_relaxed);
    job.owner->deliver_completion(job.connection_id, job.generation, job.type,
                                   std::move(result));
  }
}
