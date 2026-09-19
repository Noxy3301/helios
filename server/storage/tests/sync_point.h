/**
 * @file server/storage/tests/sync_point.h
 * What a test needs to drive a Debug Sync point: the pipes the handshake
 * action runs over, and the environment that arms the point.
 */

#ifndef HELIOS_STORAGE_TESTS_SYNC_POINT_H
#define HELIOS_STORAGE_TESTS_SYNC_POINT_H

#include <poll.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <string>
#include <vector>

#include "gtest/gtest.h"

// Closes both ends on scope exit so an assertion failure cannot leak them.
class Pipe {
 public:
  Pipe() { EXPECT_EQ(::pipe(fds_), 0); }
  ~Pipe() {
    close_read();
    close_write();
  }
  int read_fd() const { return fds_[0]; }
  int write_fd() const { return fds_[1]; }
  void close_read() { close(fds_[0]); }
  void close_write() { close(fds_[1]); }

 private:
  static void close(int &fd) {
    if (fd >= 0) {
      ::close(fd);
      fd = -1;
    }
  }
  int fds_[2] = {-1, -1};
};

// Writes the release byte on scope exit. Declared after the future so it runs
// first and the blocked point can always finish; the extra byte on the
// success path is never read and harmless.
struct ReleaseOnExit {
  int fd;
  ~ReleaseOnExit() { [[maybe_unused]] const ssize_t rc = ::write(fd, "r", 1); }
};

// True once `fd` has data to read, false if `timeout` passes first. Bounds an
// arrival wait that would otherwise block forever if the point never fires.
inline bool wait_readable(int fd, std::chrono::milliseconds timeout) {
  pollfd target{fd, POLLIN, 0};
  const int rc = ::poll(&target, 1, static_cast<int>(timeout.count()));
  return rc == 1 && (target.revents & POLLIN) != 0;
}

// Holds the points a test armed and unsets every one of them on scope exit,
// so an armed variable cannot reach the next test in the binary.
class ArmedSyncPoints {
 public:
  ~ArmedSyncPoints() {
    for (const auto &variable : armed_) ::unsetenv(variable.c_str());
  }

  void arm(const std::string &variable, const std::string &action) {
    ::setenv(variable.c_str(), action.c_str(), 1);
    armed_.push_back(variable);
  }

 private:
  std::vector<std::string> armed_;
};

// The facility decides once per process whether anything is armed. A suite
// that arms points only inside its cases sets this sentinel first, so an
// execution order that starts with an unarmed case cannot cache "nothing
// armed" for the rest of the binary.
inline void keep_sync_facility_armed() {
  ::setenv("HELIOS_DEBUG_SYNC_KEEPS_THE_FACILITY_ARMED", "sleep:0", 1);
}

#endif  // HELIOS_STORAGE_TESTS_SYNC_POINT_H
