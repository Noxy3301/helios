#!/bin/bash
set -e

cd $(dirname $0)/..

echo "Building server..."
cd build/server
make -j `nproc`
cd ../..

echo "Building plugin..."

# Sync plugin sources to MySQL SE sources (do not edit third_party directly; this is a build-time copy)
ROOT_DIR=$(pwd)
SE_DIR="$ROOT_DIR/third_party/mysql-server/storage/helios"
echo "Syncing plugin sources into storage/helios ..."
rm -f "$SE_DIR"/*.cc "$SE_DIR"/*.hh "$SE_DIR"/*.h
cp -v "$ROOT_DIR"/plugin/*.cc "$ROOT_DIR"/plugin/*.hh "$ROOT_DIR"/plugin/*.h "$ROOT_DIR"/plugin/CMakeLists.txt "$SE_DIR/"
cp -v "$ROOT_DIR"/proto/helios.proto "$SE_DIR/proto/"
mkdir -p "$SE_DIR/common"
cp -a "$ROOT_DIR"/common/. "$SE_DIR/common/"

cd build
ninja ha_helios_storage_engine.so

# A shared module links with symbols left undefined, and one that nothing
# defines fails only when mysqld loads the module. Nothing outside the
# module defines a helios symbol, so an undefined one can never resolve.
PLUGIN="$ROOT_DIR/build/plugin_output_directory/ha_helios_storage_engine.so"
# Assigned on its own: inside a pipe, a failing nm would read as no undefined
# symbols. -D reads the dynamic table, which stripping keeps.
SYMBOLS=$(nm -DuC "$PLUGIN")
if MISSING=$(printf '%s\n' "$SYMBOLS" | grep -i helios); then
  echo "undefined helios symbols in $PLUGIN:" >&2
  echo "$MISSING" >&2
  exit 1
fi
