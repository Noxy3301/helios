#ifndef HELIOS_COMMON_RPC_FRAME_H
#define HELIOS_COMMON_RPC_FRAME_H

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace helios::rpc {

// One request or reply on the wire:
//
//   [u64 id][u32 env_len][envelope][u32 pay_len][payload]
//
// The id and both lengths are in network order. A reply carries the id of
// its request, so the replies on one connection may come back in any order.
// A protobuf envelope is a serialized Helios.Protocol.Request or Response,
// and among protobuf RPCs only the read-plan reply carries a payload. A flat
// request's envelope is one FlatOp byte and its payload the body;
// common/rpc_flat.h gives the reply layouts.

// Bytes before the envelope: the id and env_len.
constexpr uint32_t kFrameHeadBytes = sizeof(uint64_t) + sizeof(uint32_t);

// Largest part either side accepts, so a length word cannot ask for an
// arbitrary allocation.
constexpr uint32_t kMaxPartBytes = 1u << 30;

// A receive buffer starts at kInitialBufferBytes and grows to hold a whole
// frame. A send or receive buffer keeps up to kKeptBufferBytes between frames
// and frees a larger one.
constexpr size_t kInitialBufferBytes = 16 * 1024;
constexpr size_t kKeptBufferBytes = 1 << 20;

// Reads the length word at `p`.
inline uint32_t load_len(const char *p) {
  uint32_t net_len;
  std::memcpy(&net_len, p, sizeof(net_len));
  return ntohl(net_len);
}

// Appends `len` as a length word.
inline void append_len(std::string &out, size_t len) {
  const uint32_t net_len = htonl(static_cast<uint32_t>(len));
  out.append(reinterpret_cast<const char *>(&net_len), sizeof(net_len));
}

// Native-endian bytes spell "HELIOSRP", the first word of a flat read-plan
// result.
constexpr uint64_t kFlatMagic = 0x5052534F494C4548ull;

}  // namespace helios::rpc

#endif  // HELIOS_COMMON_RPC_FRAME_H
