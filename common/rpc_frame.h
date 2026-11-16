#ifndef HELIOS_COMMON_RPC_FRAME_H
#define HELIOS_COMMON_RPC_FRAME_H

#include <cstdint>

namespace helios::rpc {

// One request or reply on the wire:
//
//   [u32 env_len][envelope][u32 pay_len][payload]
//
// Both lengths are in network order. A protobuf envelope is a serialized
// Helios.Protocol.Request or Response, and among protobuf RPCs only the
// read-plan reply carries a payload. A flat request's envelope is one FlatOp
// byte and its payload the body; common/rpc_flat.h gives the reply layouts.

// Largest part either side accepts, so a length word cannot ask for an
// arbitrary allocation.
constexpr uint32_t kMaxPartBytes = 1u << 30;

// Native-endian bytes spell "HELIOSRP", the first word of a flat read-plan
// result.
constexpr uint64_t kFlatMagic = 0x5052534F494C4548ull;

}  // namespace helios::rpc

#endif  // HELIOS_COMMON_RPC_FRAME_H
