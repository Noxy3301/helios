#!/bin/bash
# Configure, build and run the vendored storage's own test suite standalone.
# The storage CMakeLists only adds tests/ when it is the top-level project, so
# this configures server/storage directly rather than through the server build.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-/tmp/helios-storage-build}"

cmake -S "$ROOT_DIR/server/storage" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON
cmake --build "$BUILD_DIR" -j "$(nproc)"
# The tests share one working directory, so they run serially.
ctest --test-dir "$BUILD_DIR" -j1 --output-on-failure
