#include "rpc_timing.hh"

#include <csignal>
#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "../../common/log.h"

namespace rpc_timing {

namespace {

// Histogram edges in microseconds; bucket i holds (edge[i-1], edge[i]], the
// last bucket is (10000, +inf).
constexpr int64_t kBucketEdgesUs[] = {1,   2,   5,    10,   25,   50,   100,
                                       250, 500, 1000, 2500, 5000, 10000};
constexpr size_t kNumEdges = sizeof(kBucketEdgesUs) / sizeof(kBucketEdgesUs[0]);
constexpr size_t kNumBuckets = kNumEdges + 1;  // + the "+inf" bucket

// MessageType values run 0..36 with a small gap; size with headroom.
constexpr size_t kMaxOpcode = 48;

struct OpcodeStats {
  uint64_t count = 0;
  uint64_t sum_us = 0;
  uint64_t max_us = 0;
  std::array<uint64_t, kNumBuckets> buckets{};

  void record(uint64_t us) {
    count++;
    sum_us += us;
    if (us > max_us) max_us = us;
    size_t b = kNumEdges;  // default: "+inf" bucket
    for (size_t i = 0; i < kNumEdges; i++) {
      if (us <= static_cast<uint64_t>(kBucketEdgesUs[i])) {
        b = i;
        break;
      }
    }
    buckets[b]++;
  }

  void merge_from(const OpcodeStats &other) {
    count += other.count;
    sum_us += other.sum_us;
    if (other.max_us > max_us) max_us = other.max_us;
    for (size_t i = 0; i < kNumBuckets; i++) buckets[i] += other.buckets[i];
  }
};

using StatsTable = std::array<OpcodeStats, kMaxOpcode>;

std::mutex g_global_mutex;
StatsTable g_global_stats;

// Merges `table` into the global table under g_global_mutex, then clears
// `table` (safe to call more than once per thread).
void merge_into_global(StatsTable &table) {
  std::lock_guard<std::mutex> lock(g_global_mutex);
  for (size_t i = 0; i < kMaxOpcode; i++) {
    g_global_stats[i].merge_from(table[i]);
    table[i] = OpcodeStats{};
  }
}

struct ThreadLocalTable {
  StatsTable table{};
  ~ThreadLocalTable() { merge_into_global(table); }
};

thread_local ThreadLocalTable tl_table;

const char *opcode_name(MessageType type) {
  switch (type) {
    case MessageType::UNKNOWN: return "UNKNOWN";
    case MessageType::TX_BEGIN_TRANSACTION: return "TX_BEGIN_TRANSACTION";
    case MessageType::TX_ABORT: return "TX_ABORT";
    case MessageType::TX_READ: return "TX_READ";
    case MessageType::TX_WRITE: return "TX_WRITE";
    case MessageType::TX_DELETE: return "TX_DELETE";
    case MessageType::TX_READ_SECONDARY_INDEX: return "TX_READ_SECONDARY_INDEX";
    case MessageType::TX_WRITE_SECONDARY_INDEX: return "TX_WRITE_SECONDARY_INDEX";
    case MessageType::TX_DELETE_SECONDARY_INDEX: return "TX_DELETE_SECONDARY_INDEX";
    case MessageType::TX_UPDATE_SECONDARY_INDEX: return "TX_UPDATE_SECONDARY_INDEX";
    case MessageType::TX_GET_MATCHING_KEYS_IN_RANGE: return "TX_GET_MATCHING_KEYS_IN_RANGE";
    case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE:
      return "TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE";
    case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX:
      return "TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX";
    case MessageType::TX_FETCH_LAST_KEY_IN_RANGE: return "TX_FETCH_LAST_KEY_IN_RANGE";
    case MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX: return "TX_FETCH_FIRST_KEY_WITH_PREFIX";
    case MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX: return "TX_FETCH_NEXT_KEY_WITH_PREFIX";
    case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE:
      return "TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE";
    case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX:
      return "TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX";
    case MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE:
      return "TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE";
    case MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE:
      return "TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE";
    case MessageType::DB_FENCE: return "DB_FENCE";
    case MessageType::DB_END_TRANSACTION: return "DB_END_TRANSACTION";
    case MessageType::DB_CREATE_TABLE: return "DB_CREATE_TABLE";
    case MessageType::DB_SET_TABLE: return "DB_SET_TABLE";
    case MessageType::DB_CREATE_SECONDARY_INDEX: return "DB_CREATE_SECONDARY_INDEX";
    case MessageType::TX_BATCH_READ: return "TX_BATCH_READ";
    case MessageType::TX_BATCH_WRITE: return "TX_BATCH_WRITE";
    case MessageType::TX_STATELESS_READ: return "TX_STATELESS_READ";
    case MessageType::TX_STATELESS_BATCH_READ: return "TX_STATELESS_BATCH_READ";
    case MessageType::TX_VALIDATE_AND_COMMIT: return "TX_VALIDATE_AND_COMMIT";
    case MessageType::TX_EXECUTE_READ_PLAN: return "TX_EXECUTE_READ_PLAN";
    case MessageType::TX_GET_TABLE_STATS: return "TX_GET_TABLE_STATS";
    case MessageType::TX_EXECUTE_SQL_DUCKDB: return "TX_EXECUTE_SQL_DUCKDB";
  }
  return "UNKNOWN_OPCODE";
}

// Bucket upper edge (or max_us for the "+inf" bucket) as the percentile
// estimate -- the usual fixed-histogram approximation.
uint64_t percentile_from_buckets(const OpcodeStats &stats, double p) {
  if (stats.count == 0) return 0;
  uint64_t target = static_cast<uint64_t>(std::ceil(stats.count * p));
  if (target == 0) target = 1;
  uint64_t cum = 0;
  for (size_t b = 0; b < kNumBuckets; b++) {
    cum += stats.buckets[b];
    if (cum >= target) {
      return b < kNumEdges ? static_cast<uint64_t>(kBucketEdgesUs[b]) : stats.max_us;
    }
  }
  return stats.max_us;
}

// Writes the accumulated per-opcode table as a TSV to
// /tmp/helios_rpc_timing_<pid>.txt (path and column layout are a fixed
// format consumed by out-of-tree tooling: keep both in lockstep with any
// future column change), then logs the same table as one LOG_INFO line per
// opcode -- Log::write's format buffer is fixed at 1024 bytes, too small
// for the whole table in a single call.
void dump() {
  // Merge-under-mutex at clean shutdown; in practice a no-op here, since the
  // sigwait thread itself never calls a handler (see header comment).
  merge_into_global(tl_table.table);

  StatsTable snapshot;
  {
    std::lock_guard<std::mutex> lock(g_global_mutex);
    snapshot = g_global_stats;
  }

  std::vector<size_t> order;
  for (size_t i = 0; i < kMaxOpcode; i++) {
    if (snapshot[i].count > 0) order.push_back(i);
  }
  std::sort(order.begin(), order.end(),
            [&](size_t a, size_t b) { return snapshot[a].count > snapshot[b].count; });

  std::string path = "/tmp/helios_rpc_timing_" + std::to_string(getpid()) + ".txt";
  std::ofstream out(path);
  if (!out) {
    LOG_ERROR("HELIOS_RPC_TIMING: failed to open %s for writing", path.c_str());
  } else {
    out << "opcode_name\tcount\tmean_us\tp50_us\tp99_us\tmax_us"
           "\tbucket_le_1\tbucket_le_2\tbucket_le_5\tbucket_le_10\tbucket_le_25"
           "\tbucket_le_50\tbucket_le_100\tbucket_le_250\tbucket_le_500"
           "\tbucket_le_1000\tbucket_le_2500\tbucket_le_5000\tbucket_le_10000"
           "\tbucket_gt_10000\n";
    for (size_t idx : order) {
      const OpcodeStats &s = snapshot[idx];
      double mean_us = static_cast<double>(s.sum_us) / static_cast<double>(s.count);
      out << opcode_name(static_cast<MessageType>(idx)) << '\t' << s.count << '\t'
          << mean_us << '\t' << percentile_from_buckets(s, 0.50) << '\t'
          << percentile_from_buckets(s, 0.99) << '\t' << s.max_us;
      for (size_t b = 0; b < kNumBuckets; b++) out << '\t' << s.buckets[b];
      out << '\n';
    }
    out.close();
    LOG_INFO("HELIOS_RPC_TIMING: dumped per-opcode timing to %s", path.c_str());
  }

  if (order.empty()) {
    LOG_INFO("HELIOS_RPC_TIMING: no RPCs recorded");
    return;
  }

  LOG_INFO("HELIOS_RPC_TIMING: dumping %zu opcodes", order.size());
  for (size_t idx : order) {
    const OpcodeStats &s = snapshot[idx];
    double mean_us = static_cast<double>(s.sum_us) / static_cast<double>(s.count);
    LOG_INFO("HELIOS_RPC_TIMING: %-45s count=%lu mean_us=%.1f p50_us=%lu p99_us=%lu max_us=%lu",
             opcode_name(static_cast<MessageType>(idx)), s.count, mean_us,
             percentile_from_buckets(s, 0.50), percentile_from_buckets(s, 0.99), s.max_us);
  }
}

bool env_flag_is_one(const char *name) {
  const char *value = std::getenv(name);
  return value != nullptr && std::string_view(value) == "1";
}

}  // namespace

bool enabled() {
  static const bool value = env_flag_is_one("HELIOS_RPC_TIMING");
  return value;
}

void record(MessageType type, uint64_t elapsed_us) {
  size_t idx = static_cast<size_t>(type);
  if (idx >= kMaxOpcode) return;  // defensive: unexpected opcode value
  tl_table.table[idx].record(elapsed_us);
}

void install_shutdown_handler() {
  if (!enabled()) return;  // no-op: default SIGTERM/SIGINT disposition, unchanged

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGTERM);
  sigaddset(&set, SIGINT);
  // Block on the calling thread; every thread spawned after this inherits
  // the mask, so the signal stays pending for sigwait() below instead of
  // racing into an arbitrary handler thread.
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  std::thread([set]() mutable {
    int sig = 0;
    sigwait(&set, &sig);
    LOG_INFO("HELIOS_RPC_TIMING: received signal %d, dumping RPC timing and exiting", sig);
    dump();
    _exit(0);
  }).detach();
}

}  // namespace rpc_timing
