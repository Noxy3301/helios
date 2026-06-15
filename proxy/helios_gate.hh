#ifndef HELIOS_GATE_HH
#define HELIOS_GATE_HH
#pragma once
// Centralised env-var gate parsing for Helios optimizer features (Phase-22,
// Codex review #4). Every gate parses identically instead of each site rolling
// its own (`getenv != nullptr`, `e[0] == '1'`, `e[0] != '0'` were all in use,
// and the presence-based AGG gate treated `=0` as ENABLED — a footgun).
//
// Recognised tokens (case-insensitive): disable = {0,false,off,no};
// enable = {1,true,on,yes}. Unset or empty selects the gate's default;
// an unrecognised value also falls back to the default (never crashes).
#include <cctype>
#include <cstdlib>

namespace helios {

inline bool str_ieq(const char *a, const char *b) {
  if (a == nullptr || b == nullptr) return false;
  while (*a != '\0' && *b != '\0') {
    if (std::tolower((unsigned char)*a) != std::tolower((unsigned char)*b))
      return false;
    ++a;
    ++b;
  }
  return *a == *b;
}

inline bool env_is_disable(const char *e) {
  return str_ieq(e, "0") || str_ieq(e, "false") || str_ieq(e, "off") ||
         str_ieq(e, "no");
}
inline bool env_is_enable(const char *e) {
  return str_ieq(e, "1") || str_ieq(e, "true") || str_ieq(e, "on") ||
         str_ieq(e, "yes");
}

// Default-ON gate: enabled unless explicitly disabled. Unset/empty/unknown -> ON.
inline bool gate_default_on(const char *name) {
  const char *e = std::getenv(name);
  if (e == nullptr || e[0] == '\0') return true;
  return !env_is_disable(e);
}

// Opt-in (default-OFF) gate: enabled only by an explicit enable token.
// Unset/empty/unknown -> OFF.
inline bool gate_default_off(const char *name) {
  const char *e = std::getenv(name);
  if (e == nullptr || e[0] == '\0') return false;
  return env_is_enable(e);
}

}  // namespace helios

#endif  // HELIOS_GATE_HH
