/**
 * @file server/storage/src/util/debug_sync.h
 * Named synchronization points a test arms from the environment, so a race
 * can be reproduced in the binary that serves production traffic.
 */

#ifndef HELIOS_STORAGE_SRC_UTIL_DEBUG_SYNC_H
#define HELIOS_STORAGE_SRC_UTIL_DEBUG_SYNC_H

// Debug Sync facility, after MySQL's DEBUG_SYNC (sql/debug_sync.h):
// production code marks a named synchronization point with one macro line,
// and the point's behavior is injected from outside the binary. MySQL
// compiles its points out of release builds; here the points stay compiled
// in and are gated at runtime instead, so the exact binary under test is
// the one that serves production traffic. A process with no
// HELIOS_DEBUG_SYNC_* environment variables evaluates each point as a
// call into the cached enabled check plus one branch.
//
// Marking a point:
//   HELIOS_DEBUG_SYNC("silo_commit.between_row_installs");
//
// Activating a point (environment):
//   HELIOS_DEBUG_SYNC_SILO_COMMIT_BETWEEN_ROW_INSTALLS=sleep:1500
// The variable name is the point name upper-cased with '.' mapped to '_'.
// The prefix scan that answers "is anything armed" is cached at first use;
// an armed process re-reads the point's action on every hit.
//
// Supported actions:
//   sleep:<ms>
//     Sleeps, capped at 10000 ms. <ms> is decimal digits only. Orders
//     nothing: it widens the race, which is enough to provoke it
//     but never enough to prove an order.
//   arrive_and_wait:<arrived_write_fd>:<release_read_fd>
//     Writes one byte to the first descriptor and blocks until one byte can be
//     read from the second. The observer therefore knows the process is inside
//     the point, and the process stays there until the observer says otherwise,
//     which is what an ordering assertion needs. Both integers are descriptors
//     the process already holds, whether it opened them itself or inherited
//     them from the process that started it. A descriptor that cannot be used,
//     or an action that does not parse, is a broken test rather than a
//     production condition, and stops the process instead of continuing
//     unsynchronized.

#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

extern char **environ;

namespace helios::storage {
namespace util {

// Every point's environment variable starts with this.
inline constexpr char kEnvPrefix[] = "HELIOS_DEBUG_SYNC_";

// Longest sleep a point may ask for: a test that wants longer is a hang.
inline constexpr long kSleepCapMs = 10000;

// True when the environment holds any point's variable.
inline bool debug_sync_env_present() {
  for (char **e = environ; *e != nullptr; ++e) {
    if (std::strncmp(*e, kEnvPrefix, sizeof(kEnvPrefix) - 1) == 0) {
      return true;
    }
  }
  return false;
}

inline bool DebugSyncArmed() {
  static const bool armed = debug_sync_env_present();
  return armed;
}

[[noreturn]] inline void DebugSyncFatal(const char *point_name,
                                        const char *detail) {
  std::fprintf(stderr, "Helios debug sync point '%s': %s\n", point_name,
               detail);
  std::abort();
}

// Reads a nonnegative decimal from *cursor and leaves it past the separator.
// The grammar is digits only: no sign, no whitespace, nothing after the value
// but the separator. Anything else is a broken activation, not an unlucky one.
inline long DebugSyncParseNonnegative(const char *point_name,
                                      const char **cursor, char separator,
                                      const char *grammar) {
  if (!std::isdigit(static_cast<unsigned char>(**cursor))) {
    DebugSyncFatal(point_name, grammar);
  }
  errno = 0;
  char *end = nullptr;
  const long value = std::strtol(*cursor, &end, 10);
  if (*end != separator || errno == ERANGE) {
    DebugSyncFatal(point_name, grammar);
  }
  *cursor = (separator == '\0') ? end : end + 1;
  return value;
}

// A descriptor must additionally fit in an int.
inline int DebugSyncParseDescriptor(const char *point_name, const char **cursor,
                                    char separator) {
  constexpr char kGrammar[] = "expected arrive_and_wait:<fd>:<fd>";
  const long value =
      DebugSyncParseNonnegative(point_name, cursor, separator, kGrammar);
  if (value > INT_MAX) {
    DebugSyncFatal(point_name, kGrammar);
  }
  return static_cast<int>(value);
}

// Announces arrival on one descriptor and blocks on the other. Short I/O and
// EOF are failures: the point would otherwise continue as if released. A
// closed arrival pipe raises SIGPIPE, whose default action would end the
// process without the diagnostic, so the signal is blocked on this thread and
// the failure surfaces as EPIPE from write. The mask is deliberately not
// restored on the fatal path: restoring it first would deliver the pending
// signal and skip the diagnostic.
inline void DebugSyncArriveAndWait(const char *point_name, int arrived_fd,
                                   int release_fd) {
  sigset_t sigpipe;
  sigemptyset(&sigpipe);
  sigaddset(&sigpipe, SIGPIPE);
  sigset_t previous;
  ::pthread_sigmask(SIG_BLOCK, &sigpipe, &previous);
  const char arrived = 'a';
  ssize_t written = 0;
  do {
    written = ::write(arrived_fd, &arrived, 1);
  } while (written < 0 && errno == EINTR);
  if (written != 1) {
    DebugSyncFatal(point_name, "could not announce arrival");
  }
  ::pthread_sigmask(SIG_SETMASK, &previous, nullptr);

  char release = 0;
  ssize_t received = 0;
  do {
    received = ::read(release_fd, &release, 1);
  } while (received < 0 && errno == EINTR);
  if (received != 1) {
    DebugSyncFatal(point_name, "was never released");
  }
}

// Slow path: runs only when at least one point is activated.
inline void DebugSyncPoint(const char *point_name) {
  std::string var(kEnvPrefix);
  for (const char *p = point_name; *p != '\0'; ++p) {
    var.push_back(*p == '.' ? '_'
                            : static_cast<char>(std::toupper(
                                  static_cast<unsigned char>(*p))));
  }
  // A test hook, not server configuration: the suite arms a point per run,
  // so it stays in the environment.
  const char *action = std::getenv(var.c_str());
  if (action == nullptr) return;
  constexpr char kSleep[] = "sleep:";
  if (std::strncmp(action, kSleep, sizeof(kSleep) - 1) == 0) {
    const char *cursor = action + sizeof(kSleep) - 1;
    const long ms = DebugSyncParseNonnegative(point_name, &cursor, '\0',
                                              "expected sleep:<ms>");
    std::this_thread::sleep_for(
        std::chrono::milliseconds(std::min(ms, kSleepCapMs)));
    return;
  }

  constexpr char kArriveAndWait[] = "arrive_and_wait:";
  if (std::strncmp(action, kArriveAndWait, sizeof(kArriveAndWait) - 1) == 0) {
    const char *cursor = action + sizeof(kArriveAndWait) - 1;
    const int arrived_fd = DebugSyncParseDescriptor(point_name, &cursor, ':');
    const int release_fd = DebugSyncParseDescriptor(point_name, &cursor, '\0');
    DebugSyncArriveAndWait(point_name, arrived_fd, release_fd);
    return;
  }

  DebugSyncFatal(point_name, "unknown action");
}

}  // namespace util
}  // namespace helios::storage

/**
 * @brief Marks a named synchronization point whose action comes from the
 *        environment.
 */
#define HELIOS_DEBUG_SYNC(point_name)                      \
  do {                                                     \
    if (::helios::storage::util::DebugSyncArmed()) {       \
      ::helios::storage::util::DebugSyncPoint(point_name); \
    }                                                      \
  } while (0)

#endif  // HELIOS_STORAGE_SRC_UTIL_DEBUG_SYNC_H
