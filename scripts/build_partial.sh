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
