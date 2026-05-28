// Memory probe (diagnostic, gated by HELIOS_MEMPROF). Reads jemalloc's live-heap
// accounting via mallctl, resolved with dlsym so there is NO link dependency on
// jemalloc (returns 0 if jemalloc isn't the active allocator). jemalloc's
// stats.allocated is the allocator's own view of live bytes — unlike RSS it is
// not distorted by swap or page reclaim, so it is the right "how much is
// actually allocated" number.
#pragma once
#include <cstddef>
#include <cstdint>
#include <dlfcn.h>

namespace helios_mem {

inline size_t je_stat(const char* name) {
  using mallctl_t = int (*)(const char*, void*, size_t*, void*, size_t);
  static mallctl_t mc =
      reinterpret_cast<mallctl_t>(dlsym(RTLD_DEFAULT, "mallctl"));
  if (!mc) return 0;
  uint64_t epoch = 1;
  size_t esz = sizeof(epoch);
  mc("epoch", &epoch, &esz, &epoch, esz);  // refresh so stats.* are current
  size_t v = 0;
  size_t vsz = sizeof(v);
  if (mc(name, &v, &vsz, nullptr, 0) != 0) return 0;
  return v;
}
inline size_t je_allocated() { return je_stat("stats.allocated"); }

}  // namespace helios_mem
