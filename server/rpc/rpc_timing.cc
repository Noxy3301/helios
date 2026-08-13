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
#include <unordered_map>
#include <vector>

#include "../../common/log.h"
#include "lineairdb.pb.h"

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
using VariantMap = std::unordered_map<uint32_t, OpcodeStats>;

// --- variant binning --------------------------------------------------
//
// Generic "n==0 -> bin 0; n<=edges[i] -> bin i+1; n>edges[last] -> overflow"
// ladder, parameterized per dimension below so each dimension keeps its own
// resolution.
size_t ceil_bin(uint64_t n, const uint64_t *edges, size_t num_edges) {
  if (n == 0) return 0;
  for (size_t i = 0; i < num_edges; i++) {
    if (n <= edges[i]) return i + 1;
  }
  return num_edges + 1;
}

// Byte size (in KiB, ceil) ladder shared by commit request/response/
// stats-snapshot size and read-plan response size. Top edge (8192 KiB =
// 8 MiB) matches the planned global frame cap.
constexpr uint64_t kSizeBinEdges[] = {1,   2,    4,    8,    16,   32,  64,
                                      128, 256,  512,  1024, 2048, 4096, 8192};
constexpr size_t kNumSizeBinEdges = sizeof(kSizeBinEdges) / sizeof(kSizeBinEdges[0]);
constexpr const char *kSizeBinLabels[] = {
    "0", "1", "2", "4", "8", "16", "32", "64", "128", "256", "512", "1024",
    "2048", "4096", "8192", "8192+"};

// Commit set-entries ladder (read + write + delete + secondary_index_ops +
// range_reads), widened around the commit fast-path admission boundaries.
constexpr uint64_t kCommitEntriesBinEdges[] = {128, 256, 384, 512, 768, 1024, 2048};
constexpr size_t kNumCommitEntriesBinEdges =
    sizeof(kCommitEntriesBinEdges) / sizeof(kCommitEntriesBinEdges[0]);
constexpr const char *kCommitEntriesBinLabels[] = {
    "0", "128", "256", "384", "512", "768", "1024", "2048", "2048+"};

// Read-plan steps ladder.
constexpr uint64_t kReadPlanStepsBinEdges[] = {8, 12, 16, 24, 32, 48, 64};
constexpr size_t kNumReadPlanStepsBinEdges =
    sizeof(kReadPlanStepsBinEdges) / sizeof(kReadPlanStepsBinEdges[0]);
constexpr const char *kReadPlanStepsBinLabels[] = {
    "0", "8", "12", "16", "24", "32", "48", "64", "64+"};

// Inspected/probed row count ladder -- see note_inspected_rows in
// rpc_timing.hh for what "inspected" measures here.
constexpr uint64_t kRowsBinEdges[] = {256, 512, 1024, 2048, 4096, 8192};
constexpr size_t kNumRowsBinEdges = sizeof(kRowsBinEdges) / sizeof(kRowsBinEdges[0]);
constexpr const char *kRowsBinLabels[] = {
    "0", "256", "512", "1024", "2048", "4096", "8192", "8192+"};

size_t kib_ceil(size_t bytes) { return (bytes + 1023) / 1024; }

// Sentinel variant key for "request failed to parse" (never a valid packed
// value: both keys stay well under 2^20). Calls that hit this are still
// counted at the opcode level via record(), just not broken into a bin.
constexpr uint32_t kVariantParseFailed = 0xFFFFFFFFu;

thread_local uint64_t tl_last_inspected_rows = 0;
thread_local uint64_t tl_last_commit_stats_bytes = 0;

// TX_VALIDATE_AND_COMMIT variant key: fence(1) | entries_bin(4) |
// frame_kib_bin(4) | response_kib_bin(4) | stats_kib_bin(4) = 17 bits.
// Entries = read + write + delete + secondary_index_ops + range_reads
// (deletes count as part of the write set, matching stateless_operations.cc's
// own treatment of merging deletes into the write buffer at handle time;
// range evidence counts too, since the range-work guard bounds evidence
// count together with the rest of the set instead of vetoing on range
// presence alone).
uint32_t commit_variant_key(const std::string &message, const std::string &result) {
  LineairDB::Protocol::TxValidateAndCommit::Request req;
  if (!req.ParseFromString(message)) return kVariantParseFailed;
  const bool fence = req.fence();
  const uint64_t entries = static_cast<uint64_t>(req.reads_size()) +
                           static_cast<uint64_t>(req.writes_size()) +
                           static_cast<uint64_t>(req.deletes_size()) +
                           static_cast<uint64_t>(req.secondary_index_ops_size()) +
                           static_cast<uint64_t>(req.range_reads_size());
  const size_t entry_bin =
      ceil_bin(entries, kCommitEntriesBinEdges, kNumCommitEntriesBinEdges);
  const size_t frame_bin =
      ceil_bin(kib_ceil(message.size()), kSizeBinEdges, kNumSizeBinEdges);
  const size_t resp_bin =
      ceil_bin(kib_ceil(result.size()), kSizeBinEdges, kNumSizeBinEdges);
  const size_t stats_bin =
      ceil_bin(kib_ceil(tl_last_commit_stats_bytes), kSizeBinEdges, kNumSizeBinEdges);
  return (fence ? 1u : 0u) | ((entry_bin & 0xFu) << 1) | ((frame_bin & 0xFu) << 5) |
         ((resp_bin & 0xFu) << 9) | ((stats_bin & 0xFu) << 13);
}

void unpack_commit_variant(uint32_t key, bool &fence, size_t &entry_bin, size_t &frame_bin,
                            size_t &resp_bin, size_t &stats_bin) {
  fence = key & 1u;
  entry_bin = (key >> 1) & 0xFu;
  frame_bin = (key >> 5) & 0xFu;
  resp_bin = (key >> 9) & 0xFu;
  stats_bin = (key >> 13) & 0xFu;
}

// TX_EXECUTE_READ_PLAN variant key: steps_bin(4) | unbounded(1) | rows_bin(3)
// | response_kib_bin(4) = 12 bits. "unbounded" means at least one scan step
// has scan_limit()==0 (protobuf's unset-uint64 default is also 0, so this
// only looks at steps where is_scan() is true; non-scan point-lookup steps
// carry no scan_limit signal at all).
uint32_t read_plan_variant_key(const std::string &message, const std::string &result) {
  LineairDB::Protocol::TxExecuteReadPlan::Request req;
  if (!req.ParseFromString(message)) return kVariantParseFailed;
  const size_t steps = static_cast<size_t>(req.steps_size());
  const size_t steps_bin =
      ceil_bin(steps, kReadPlanStepsBinEdges, kNumReadPlanStepsBinEdges);
  bool unbounded = false;
  for (const auto &step : req.steps()) {
    if (step.is_scan() && step.scan_limit() == 0) {
      unbounded = true;
      break;
    }
  }
  const size_t rows_bin = ceil_bin(tl_last_inspected_rows, kRowsBinEdges, kNumRowsBinEdges);
  const size_t resp_bin =
      ceil_bin(kib_ceil(result.size()), kSizeBinEdges, kNumSizeBinEdges);
  return (steps_bin & 0xFu) | ((unbounded ? 1u : 0u) << 4) | ((rows_bin & 0x7u) << 5) |
         ((resp_bin & 0xFu) << 8);
}

void unpack_read_plan_variant(uint32_t key, size_t &steps_bin, bool &unbounded,
                               size_t &rows_bin, size_t &resp_bin) {
  steps_bin = key & 0xFu;
  unbounded = (key >> 4) & 1u;
  rows_bin = (key >> 5) & 0x7u;
  resp_bin = (key >> 8) & 0xFu;
}

// --- global / thread-local state ---------------------------------------

std::mutex g_global_mutex;
StatsTable g_global_stats;
VariantMap g_global_commit_variants;
VariantMap g_global_read_plan_variants;

struct ThreadLocalTable {
  StatsTable table{};
  VariantMap commit_variants;
  VariantMap read_plan_variants;
  ~ThreadLocalTable();
};

// Merges `t` into the global tables under g_global_mutex, then clears `t`
// (safe to call more than once per thread).
void merge_into_global(ThreadLocalTable &t) {
  std::lock_guard<std::mutex> lock(g_global_mutex);
  for (size_t i = 0; i < kMaxOpcode; i++) {
    g_global_stats[i].merge_from(t.table[i]);
    t.table[i] = OpcodeStats{};
  }
  for (auto &[key, stats] : t.commit_variants) {
    g_global_commit_variants[key].merge_from(stats);
    stats = OpcodeStats{};
  }
  for (auto &[key, stats] : t.read_plan_variants) {
    g_global_read_plan_variants[key].merge_from(stats);
    stats = OpcodeStats{};
  }
}

ThreadLocalTable::~ThreadLocalTable() { merge_into_global(*this); }

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

void write_stats_columns(std::ofstream &out, const OpcodeStats &s) {
  double mean_us = static_cast<double>(s.sum_us) / static_cast<double>(s.count);
  out << s.count << '\t' << mean_us << '\t' << percentile_from_buckets(s, 0.50) << '\t'
      << percentile_from_buckets(s, 0.99) << '\t' << s.max_us;
  for (size_t b = 0; b < kNumBuckets; b++) out << '\t' << s.buckets[b];
}

constexpr const char *kStatsColumnHeader =
    "count\tmean_us\tp50_us\tp99_us\tmax_us"
    "\tbucket_le_1\tbucket_le_2\tbucket_le_5\tbucket_le_10\tbucket_le_25"
    "\tbucket_le_50\tbucket_le_100\tbucket_le_250\tbucket_le_500"
    "\tbucket_le_1000\tbucket_le_2500\tbucket_le_5000\tbucket_le_10000"
    "\tbucket_gt_10000";

// Appends a "# variant_table: TX_VALIDATE_AND_COMMIT" section (fence/
// entries/frame-size/response-size/stats-size breakdown), sorted by count
// descending, after a blank-line separator from the main opcode table.
void dump_commit_variants(std::ofstream &out, const VariantMap &variants) {
  std::vector<uint32_t> keys;
  for (const auto &[key, stats] : variants) {
    if (stats.count > 0) keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end(), [&](uint32_t a, uint32_t b) {
    return variants.at(a).count > variants.at(b).count;
  });
  out << "\n# variant_table: TX_VALIDATE_AND_COMMIT\n";
  out << "fence\tentries_bin\tframe_kib_bin\tresponse_kib_bin\tstats_kib_bin\t"
      << kStatsColumnHeader << '\n';
  for (uint32_t key : keys) {
    bool fence;
    size_t entry_bin, frame_bin, resp_bin, stats_bin;
    unpack_commit_variant(key, fence, entry_bin, frame_bin, resp_bin, stats_bin);
    out << (fence ? "true" : "false") << '\t' << kCommitEntriesBinLabels[entry_bin] << '\t'
        << kSizeBinLabels[frame_bin] << '\t' << kSizeBinLabels[resp_bin] << '\t'
        << kSizeBinLabels[stats_bin] << '\t';
    write_stats_columns(out, variants.at(key));
    out << '\n';
  }
}

// Appends a "# variant_table: TX_EXECUTE_READ_PLAN" section (steps/
// scan_limit/inspected-rows/response-size breakdown), same layout as above.
void dump_read_plan_variants(std::ofstream &out, const VariantMap &variants) {
  std::vector<uint32_t> keys;
  for (const auto &[key, stats] : variants) {
    if (stats.count > 0) keys.push_back(key);
  }
  std::sort(keys.begin(), keys.end(), [&](uint32_t a, uint32_t b) {
    return variants.at(a).count > variants.at(b).count;
  });
  out << "\n# variant_table: TX_EXECUTE_READ_PLAN\n";
  out << "steps_bin\tunbounded_scan\trows_bin\tresponse_kib_bin\t" << kStatsColumnHeader << '\n';
  for (uint32_t key : keys) {
    size_t steps_bin, rows_bin, resp_bin;
    bool unbounded;
    unpack_read_plan_variant(key, steps_bin, unbounded, rows_bin, resp_bin);
    out << kReadPlanStepsBinLabels[steps_bin] << '\t' << (unbounded ? "true" : "false") << '\t'
        << kRowsBinLabels[rows_bin] << '\t' << kSizeBinLabels[resp_bin] << '\t';
    write_stats_columns(out, variants.at(key));
    out << '\n';
  }
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
  merge_into_global(tl_table);

  StatsTable snapshot;
  VariantMap commit_snapshot;
  VariantMap read_plan_snapshot;
  {
    std::lock_guard<std::mutex> lock(g_global_mutex);
    snapshot = g_global_stats;
    commit_snapshot = g_global_commit_variants;
    read_plan_snapshot = g_global_read_plan_variants;
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
    out << "opcode_name\t" << kStatsColumnHeader << '\n';
    for (size_t idx : order) {
      out << opcode_name(static_cast<MessageType>(idx)) << '\t';
      write_stats_columns(out, snapshot[idx]);
      out << '\n';
    }

    // Variant breakdown tables for the two fast-path opcodes: a blank line
    // (the leading '\n' in each dump_*_variants call) separates them from
    // the main table above, which out-of-tree consumers rely on to know
    // where the fixed per-opcode summary ends.
    dump_commit_variants(out, commit_snapshot);
    dump_read_plan_variants(out, read_plan_snapshot);

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

void record_variant(MessageType type, uint64_t elapsed_us, const std::string &message,
                     const std::string &result) {
  record(type, elapsed_us);
  if (type == MessageType::TX_VALIDATE_AND_COMMIT) {
    uint32_t key = commit_variant_key(message, result);
    if (key != kVariantParseFailed) tl_table.commit_variants[key].record(elapsed_us);
  } else if (type == MessageType::TX_EXECUTE_READ_PLAN) {
    uint32_t key = read_plan_variant_key(message, result);
    if (key != kVariantParseFailed) tl_table.read_plan_variants[key].record(elapsed_us);
  }
}

void note_inspected_rows(uint64_t rows) { tl_last_inspected_rows = rows; }

void note_commit_stats_bytes(uint64_t bytes) { tl_last_commit_stats_bytes = bytes; }

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
