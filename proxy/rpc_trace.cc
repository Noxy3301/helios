#include "rpc_trace.hh"

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <unistd.h>

namespace {

// Format a system_clock time point as RFC3339 with microsecond precision.
std::string format_iso(std::chrono::system_clock::time_point tp) {
  using namespace std::chrono;
  auto t = system_clock::to_time_t(tp);
  auto us = duration_cast<microseconds>(tp.time_since_epoch()).count() % 1'000'000;
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[40];
  std::snprintf(buf, sizeof(buf),
                "%04d-%02d-%02dT%02d:%02d:%02d.%06ldZ",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec,
                static_cast<long>(us));
  return buf;
}

}  // namespace

const char* message_type_name(MessageType t) {
  switch (t) {
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
    case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE: return "TX_GET_MATCHING_KEYS_AND_VALUES_IN_RANGE";
    case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_IN_INDEX_RANGE: return "TX_GET_MATCHING_KEYS_AND_VALUES_IN_INDEX_RANGE";
    case MessageType::TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX: return "TX_GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX";
    case MessageType::TX_FETCH_LAST_KEY_IN_RANGE: return "TX_FETCH_LAST_KEY_IN_RANGE";
    case MessageType::TX_FETCH_FIRST_KEY_WITH_PREFIX: return "TX_FETCH_FIRST_KEY_WITH_PREFIX";
    case MessageType::TX_FETCH_NEXT_KEY_WITH_PREFIX: return "TX_FETCH_NEXT_KEY_WITH_PREFIX";
    case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE: return "TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE";
    case MessageType::TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX: return "TX_GET_MATCHING_PRIMARY_KEYS_FROM_PREFIX";
    case MessageType::TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE: return "TX_FETCH_LAST_PRIMARY_KEY_IN_SECONDARY_RANGE";
    case MessageType::TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE: return "TX_FETCH_LAST_SECONDARY_ENTRY_IN_RANGE";
    case MessageType::DB_FENCE: return "DB_FENCE";
    case MessageType::DB_END_TRANSACTION: return "DB_END_TRANSACTION";
    case MessageType::DB_CREATE_TABLE: return "DB_CREATE_TABLE";
    case MessageType::DB_SET_TABLE: return "DB_SET_TABLE";
    case MessageType::DB_CREATE_SECONDARY_INDEX: return "DB_CREATE_SECONDARY_INDEX";
    case MessageType::TX_BATCH_READ: return "TX_BATCH_READ";
    case MessageType::TX_BATCH_WRITE: return "TX_BATCH_WRITE";
  }
  return "UNDEFINED";
}

// Minimal JSON string escape with truncation. Non-printable bytes are
// emitted as \u00XX so the output is grep-friendly.
std::string json_escape(const std::string& s, size_t max_len) {
  std::string out;
  out.reserve(s.size() + 8);
  size_t n = std::min(s.size(), max_len);
  for (size_t i = 0; i < n; ++i) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        // Escape every byte outside printable ASCII so the output stays
        // valid JSON regardless of binary bytes in keys / values. Lossy
        // round-trip (\u00XX is a Unicode codepoint, not a literal byte)
        // but adequate for analysis.
        if (c < 0x20 || c >= 0x7f) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += static_cast<char>(c);
        }
    }
  }
  if (s.size() > max_len) out += "...";
  return out;
}

void TxRpcTrace::start(int64_t tx_id, std::thread::id tid) {
  if (!RpcTraceLogger::instance().enabled()) {
    active_ = false;
    return;
  }
  active_ = true;
  tx_id_ = tx_id;
  tid_ = tid;
  started_ = std::chrono::steady_clock::now();
  started_wall_ = std::chrono::system_clock::now();
  rpcs_.clear();
  statements_.clear();
  by_type_.clear();
}

void TxRpcTrace::on_stmt(const std::string& sql) {
  if (!active_) return;
  // Dedupe consecutive same-SQL boundaries (multiple external_lock calls
  // per statement when more than one table is touched).
  if (!statements_.empty() && statements_.back().sql == sql) return;
  uint64_t off = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - started_).count();
  StatementEntry s;
  s.sql = sql;
  s.started_off_us = off;
  s.first_rpc_idx = static_cast<uint32_t>(rpcs_.size());
  s.last_rpc_idx = s.first_rpc_idx;  // inclusive end maintained by record()
  statements_.push_back(std::move(s));
}

void TxRpcTrace::record(MessageType type, uint64_t us, uint32_t req_b,
                        uint32_t resp_b, const std::string& meta) {
  if (!active_) return;
  uint64_t off = std::chrono::duration_cast<std::chrono::microseconds>(
                     std::chrono::steady_clock::now() - started_).count();
  // The RPC's offset is relative to begin; subtract its duration to get the
  // entry timestamp (when send started). off captured here is post-recv.
  RpcEntry e;
  e.type = type;
  e.us = us;
  e.off_us = (off >= us) ? (off - us) : 0;
  e.req_b = req_b;
  e.resp_b = resp_b;
  e.stmt_idx = statements_.empty()
                   ? UINT32_MAX
                   : static_cast<uint32_t>(statements_.size() - 1);
  e.meta = meta;
  rpcs_.push_back(std::move(e));
  if (!statements_.empty()) {
    statements_.back().last_rpc_idx = static_cast<uint32_t>(rpcs_.size() - 1);
  }
  auto& a = by_type_[type];
  a.n++;
  a.us += us;
  a.req_b += req_b;
  a.resp_b += resp_b;
}

std::string TxRpcTrace::finalize_jsonl(bool committed) {
  std::ostringstream os;
  uint64_t total_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - started_).count();

  // Stringify thread id via std::ostringstream (cross-platform safe).
  std::ostringstream tid_os;
  tid_os << tid_;

  os << '{'
     << "\"tx_id\":" << tx_id_
     << ",\"thread_id\":\"" << tid_os.str() << '"'
     << ",\"started_at\":\"" << format_iso(started_wall_) << '"'
     << ",\"duration_us\":" << total_us
     << ",\"status\":\"" << (committed ? "committed" : "aborted") << '"'
     << ",\"rpc_count\":" << rpcs_.size()
     << ",\"stmt_count\":" << statements_.size();

  // statements: array
  os << ",\"statements\":[";
  for (size_t i = 0; i < statements_.size(); ++i) {
    const auto& s = statements_[i];
    if (i > 0) os << ',';
    os << '{'
       << "\"idx\":" << i
       << ",\"sql\":\"" << json_escape(s.sql, 1024) << '"'
       << ",\"started_off_us\":" << s.started_off_us
       << ",\"first_rpc\":" << s.first_rpc_idx
       << ",\"last_rpc\":" << s.last_rpc_idx
       << '}';
  }
  os << ']';

  // rpcs: array (full sequence, ordered)
  os << ",\"rpcs\":[";
  for (size_t i = 0; i < rpcs_.size(); ++i) {
    const auto& r = rpcs_[i];
    if (i > 0) os << ',';
    os << '{'
       << "\"i\":" << i
       << ",\"type\":\"" << message_type_name(r.type) << '"'
       << ",\"off_us\":" << r.off_us
       << ",\"us\":" << r.us
       << ",\"req_b\":" << r.req_b
       << ",\"resp_b\":" << r.resp_b
       << ",\"stmt\":";
    if (r.stmt_idx == UINT32_MAX) os << "null"; else os << r.stmt_idx;
    if (!r.meta.empty()) {
      os << ",\"meta\":\"" << json_escape(r.meta, 256) << '"';
    }
    os << '}';
  }
  os << ']';

  // summary_by_type
  os << ",\"summary_by_type\":{";
  bool first = true;
  for (const auto& kv : by_type_) {
    if (!first) os << ',';
    first = false;
    os << '"' << message_type_name(kv.first) << "\":{"
       << "\"n\":" << kv.second.n
       << ",\"us\":" << kv.second.us
       << ",\"req_b\":" << kv.second.req_b
       << ",\"resp_b\":" << kv.second.resp_b
       << '}';
  }
  os << '}';

  os << '}';
  active_ = false;
  return os.str();
}

RpcTraceLogger& RpcTraceLogger::instance() {
  static RpcTraceLogger inst;
  return inst;
}

RpcTraceLogger::RpcTraceLogger() {
  const char* env = std::getenv("ENABLE_RPC_TRACE");
  if (env == nullptr || env[0] == '\0' || std::string(env) == "0") {
    enabled_ = false;
    return;
  }
  // Path: ENABLE_RPC_TRACE_PATH overrides default; default is per-pid in /tmp.
  const char* path_env = std::getenv("ENABLE_RPC_TRACE_PATH");
  std::string path;
  if (path_env && path_env[0] != '\0') {
    path = path_env;
  } else {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/tmp/ordo_rpc_trace_%d.jsonl",
                  static_cast<int>(getpid()));
    path = buf;
  }
  file_.open(path, std::ios::out | std::ios::app);
  if (!file_.is_open()) {
    enabled_ = false;
    return;
  }
  enabled_ = true;
}

RpcTraceLogger::~RpcTraceLogger() {
  if (file_.is_open()) file_.close();
}

void RpcTraceLogger::log_line(const std::string& jsonl) {
  if (!enabled_) return;
  std::lock_guard<std::mutex> lk(mu_);
  file_ << jsonl << '\n';
  file_.flush();  // flush on every tx so ctrl-c'd runs leave usable data
}
