#ifndef LINEAIRDB_RPC_TRACE_HH
#define LINEAIRDB_RPC_TRACE_HH

#include <chrono>
#include <cstdint>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "lineairdb_proxy.hh"  // MessageType

// Per-RPC record captured by send_message_with_header.
struct RpcEntry {
  MessageType type;
  uint64_t us;          // duration microseconds
  uint64_t off_us;      // offset from tx_begin in microseconds
  uint32_t req_b;       // serialized request bytes
  uint32_t resp_b;      // serialized response bytes
  uint32_t stmt_idx;    // index into statements_; UINT32_MAX if pre-stmt
  std::string meta;     // optional key, index name, prefix, etc.
};

// Per-statement record captured at statement boundaries.
struct StatementEntry {
  std::string sql;
  uint64_t started_off_us;
  uint32_t first_rpc_idx;
  uint32_t last_rpc_idx;
};

// Per-local-view decision captured before a point read falls back to RPC.
struct LocalViewEntry {
  std::string kind;
  uint64_t off_us;
  uint32_t stmt_idx;
};

// Per-LineairDBTransaction trace state.
class TxRpcTrace {
 public:
  void start(int64_t tx_id, std::thread::id tid);
  void set_tx_id(int64_t tx_id) { tx_id_ = tx_id; }
  void on_stmt(const std::string& sql);
  void record(MessageType type, uint64_t us, uint32_t req_b,
              uint32_t resp_b, const std::string& meta);
  void record_local_view(const std::string& kind);
  std::string finalize_jsonl(bool committed);

  bool active() const { return active_; }

 private:
  bool active_ = false;
  int64_t tx_id_ = -1;
  std::thread::id tid_;
  std::chrono::steady_clock::time_point started_;
  std::chrono::system_clock::time_point started_wall_;
  std::vector<RpcEntry> rpcs_;
  std::vector<StatementEntry> statements_;
  std::vector<LocalViewEntry> local_view_entries_;

  struct Agg {
    uint32_t n = 0;
    uint64_t us = 0;
    uint64_t req_b = 0;
    uint64_t resp_b = 0;
  };

  std::map<MessageType, Agg> by_type_;
  std::map<std::string, uint32_t> local_view_by_kind_;
};

// Singleton JSONL logger enabled by ENABLE_RPC_TRACE.
class RpcTraceLogger {
 public:
  static RpcTraceLogger& instance();
  bool enabled() const { return enabled_; }
  void log_line(const std::string& jsonl);

 private:
  RpcTraceLogger();
  ~RpcTraceLogger();

  bool enabled_ = false;
  std::mutex mu_;
  std::ofstream file_;
};

const char* message_type_name(MessageType t);
std::string json_escape(const std::string& s, size_t max_len = 1024);

#endif  // LINEAIRDB_RPC_TRACE_HH
