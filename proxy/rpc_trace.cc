#include "rpc_trace.hh"

#include "ha_helios.hh"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <unistd.h>
#include <utility>

namespace {

std::string format_iso(std::chrono::system_clock::time_point tp) {
  using namespace std::chrono;
  auto t = system_clock::to_time_t(tp);
  auto us = duration_cast<microseconds>(tp.time_since_epoch()).count() % 1000000;
  if (us < 0) us += 1000000;
  std::tm tm{};
  gmtime_r(&t, &tm);

  std::ostringstream os;
  os << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S")
     << '.' << std::setw(6) << std::setfill('0') << us << 'Z';
  return os.str();
}

}  // namespace

const char* rpc_op_name(RpcOp op) {
  const auto* field =
      Helios::Protocol::Request::descriptor()->FindFieldByNumber(
          static_cast<int>(op));
  return field != nullptr ? field->name().c_str() : "none";
}

std::string json_escape(const std::string& s, size_t max_len) {
  std::string out;
  out.reserve(s.size() + 8);
  const size_t n = std::min(s.size(), max_len);
  for (size_t i = 0; i < n; ++i) {
    unsigned char c = static_cast<unsigned char>(s[i]);
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
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

void TxRpcTrace::start(std::thread::id tid) {
  if (!RpcTraceLogger::instance().enabled()) {
    active_ = false;
    return;
  }
  active_ = true;
  tid_ = tid;
  started_ = std::chrono::steady_clock::now();
  started_wall_ = std::chrono::system_clock::now();
  rpcs_.clear();
  statements_.clear();
  local_view_entries_.clear();
  by_type_.clear();
  local_view_by_kind_.clear();
}

void TxRpcTrace::on_stmt(const std::string& sql) {
  if (!active_) return;
  if (!statements_.empty() && statements_.back().sql == sql) return;
  const uint64_t off = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - started_)
                           .count();
  StatementEntry s;
  s.sql = sql;
  s.started_off_us = off;
  s.first_rpc_idx = static_cast<uint32_t>(rpcs_.size());
  s.last_rpc_idx = s.first_rpc_idx;
  statements_.push_back(std::move(s));
}

void TxRpcTrace::record(RpcOp type, uint64_t us, uint32_t req_b,
                        uint64_t resp_b, const std::string& meta) {
  if (!active_) return;
  const uint64_t off = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - started_)
                           .count();

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

void TxRpcTrace::record_local_view(const std::string& kind) {
  if (!active_) return;
  const uint64_t off = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - started_)
                           .count();

  LocalViewEntry e;
  e.kind = kind;
  e.off_us = off;
  e.stmt_idx = statements_.empty()
                   ? UINT32_MAX
                   : static_cast<uint32_t>(statements_.size() - 1);
  local_view_entries_.push_back(std::move(e));
  local_view_by_kind_[kind]++;
}

std::string TxRpcTrace::finalize_jsonl(bool committed) {
  std::ostringstream os;
  const uint64_t total_us =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - started_)
          .count();

  std::ostringstream tid_os;
  tid_os << tid_;

  os << '{'
     << "\"thread_id\":\"" << tid_os.str() << '"'
     << ",\"started_at\":\"" << format_iso(started_wall_) << '"'
     << ",\"duration_us\":" << total_us
     << ",\"status\":\"" << (committed ? "committed" : "aborted") << '"'
     << ",\"rpc_count\":" << rpcs_.size()
     << ",\"stmt_count\":" << statements_.size();

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

  os << ",\"rpcs\":[";
  for (size_t i = 0; i < rpcs_.size(); ++i) {
    const auto& r = rpcs_[i];
    if (i > 0) os << ',';
    os << '{'
       << "\"i\":" << i
       << ",\"type\":\"" << rpc_op_name(r.type) << '"'
       << ",\"off_us\":" << r.off_us
       << ",\"us\":" << r.us
       << ",\"req_b\":" << r.req_b
       << ",\"resp_b\":" << r.resp_b
       << ",\"stmt\":";
    if (r.stmt_idx == UINT32_MAX) {
      os << "null";
    } else {
      os << r.stmt_idx;
    }
    if (!r.meta.empty()) {
      os << ",\"meta\":\"" << json_escape(r.meta, 256) << '"';
    }
    os << '}';
  }
  os << ']';

  os << ",\"local_view_events\":[";
  for (size_t i = 0; i < local_view_entries_.size(); ++i) {
    const auto& e = local_view_entries_[i];
    if (i > 0) os << ',';
    os << '{'
       << "\"i\":" << i
       << ",\"kind\":\"" << json_escape(e.kind, 128) << '"'
       << ",\"off_us\":" << e.off_us
       << ",\"stmt\":";
    if (e.stmt_idx == UINT32_MAX) {
      os << "null";
    } else {
      os << e.stmt_idx;
    }
    os << '}';
  }
  os << ']';

  os << ",\"summary_by_type\":{";
  bool first = true;
  for (const auto& kv : by_type_) {
    if (!first) os << ',';
    first = false;
    os << '"' << rpc_op_name(kv.first) << "\":{"
       << "\"n\":" << kv.second.n
       << ",\"us\":" << kv.second.us
       << ",\"req_b\":" << kv.second.req_b
       << ",\"resp_b\":" << kv.second.resp_b
       << '}';
  }
  os << '}';

  os << ",\"summary_local_view\":{";
  bool first_local = true;
  for (const auto& kv : local_view_by_kind_) {
    if (!first_local) os << ',';
    first_local = false;
    os << '"' << json_escape(kv.first, 128) << "\":" << kv.second;
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
  if (!srv_rpc_trace) {
    enabled_ = false;
    return;
  }

  std::string path;
  if (srv_rpc_trace_path != nullptr && srv_rpc_trace_path[0] != '\0') {
    path = srv_rpc_trace_path;
  } else {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "/tmp/helios_rpc_trace_%d.jsonl",
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
  file_.flush();
}
