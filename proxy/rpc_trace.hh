#ifndef HELIOS_RPC_TRACE_HH
#define HELIOS_RPC_TRACE_HH

#include <chrono>
#include <cstdint>
#include <fstream>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "helios_proxy.hh"  // RpcOp

// Per-RPC record captured by exchange_message.
struct RpcEntry {
  RpcOp type;
  uint64_t us;          // duration microseconds
  uint64_t off_us;      // offset from the transaction start in microseconds
  uint32_t req_b;       // serialized request bytes
  uint64_t resp_b;      // serialized response bytes
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

// Per-HeliosTransaction trace state.
class TxRpcTrace {
 public:
  void start(std::thread::id tid);
  void on_stmt(const std::string& sql);
  void record(RpcOp type, uint64_t us, uint32_t req_b,
              uint64_t resp_b, const std::string& meta);
  void record_local_view(const std::string& kind);
  std::string finalize_jsonl(bool committed);

  bool active() const { return active_; }

 private:
  bool active_ = false;
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

  std::map<RpcOp, Agg> by_type_;
  std::map<std::string, uint32_t> local_view_by_kind_;
};

// Singleton JSONL logger enabled by helios_rpc_trace.
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

// The RPC's name, as protobuf spells the envelope arm it sets.
const char* rpc_op_name(RpcOp op);
std::string json_escape(const std::string& s, size_t max_len = 1024);

#endif  // HELIOS_RPC_TRACE_HH
