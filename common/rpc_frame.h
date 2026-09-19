#ifndef HELIOS_COMMON_RPC_FRAME_H
#define HELIOS_COMMON_RPC_FRAME_H

#include <cstdint>

namespace helios::rpc {

// One request or reply on the wire:
//
//   [u32 env_len][envelope][u32 pay_len][payload]
//
// Both lengths are in network order. The envelope is a serialized
// Helios.Protocol.Request or Response. The payload is empty on every RPC but
// the read-plan reply, which carries its flat result here rather than inside
// the envelope. A request always leaves the payload part empty.

// Largest part either side accepts, so a length word cannot ask for an
// arbitrary allocation.
constexpr uint32_t kMaxPartBytes = 1u << 30;

// Native-endian bytes spell "HELIOSRP", the first word of a flat read-plan
// result.
constexpr uint64_t kFlatMagic = 0x5052534F494C4548ull;

}  // namespace helios::rpc

#endif  // HELIOS_COMMON_RPC_FRAME_H
