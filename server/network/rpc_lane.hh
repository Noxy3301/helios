#ifndef HELIOS_NETWORK_RPC_LANE_HH
#define HELIOS_NETWORK_RPC_LANE_HH

#include <string_view>

#include "../protocol/message.hh"

// Where a reactor thread sends one fully-buffered RPC request.
enum class RpcLane {
  kFast,       // stateless and shape-bounded; runs inline on the reactor thread
  kSlow,       // stateless but heavy/unbounded; served by the helper pool
  kConv,       // references cross-RPC transaction state; always migrates
  kMalformed,  // a kFast candidate whose body fails to parse; protocol violation
};

// Classifies one request from its opcode and full payload bytes. Pure
// function (no logging, no global state); safe to call on the reactor
// thread before any work is dispatched.
RpcLane classify_rpc(MessageType type, std::string_view payload);

#endif  // HELIOS_NETWORK_RPC_LANE_HH
