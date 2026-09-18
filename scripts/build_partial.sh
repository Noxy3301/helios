#!/bin/bash
set -e

cd $(dirname $0)/..

echo "Building server..."
cd build/server
make -j `nproc`
cd ../..

echo "Building proxy..."

# Sync proxy sources to MySQL SE sources (do not edit third_party directly; this is a build-time copy)
ROOT_DIR=$(pwd)
SE_DIR="$ROOT_DIR/third_party/mysql-server/storage/helios"
echo "Syncing proxy sources into storage/helios ..."
cp -v "$ROOT_DIR"/proxy/*.cc "$ROOT_DIR"/proxy/*.hh "$ROOT_DIR"/proxy/*.h "$ROOT_DIR"/proxy/CMakeLists.txt "$SE_DIR/"
cp -v "$ROOT_DIR"/proto/helios.proto "$SE_DIR/proto/"

cd build
ninja ha_helios_storage_engine.so
