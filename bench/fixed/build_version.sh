#!/bin/bash
# Build the plugin and the storage server of one commit into <outdir>, reusing
# this checkout's MySQL build (MySQL itself is not modified, so only the
# plugin and the storage server differ between commits).
#
#   [PATCH=<file>] build_version.sh <commit> <outdir>
#
# <outdir>/plugin/ha_helios_storage_engine.so  load with mysqld --plugin-dir
# <outdir>/server/helios-storage               the Release binary (-march=native)
# <outdir>/server_vg/helios-storage            the same with -mno-avx512f, for valgrind
#
# Serialize calls: the plugin is compiled through build/helios_plugin, which
# the MySQL tree links to.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
commit=${1:?commit}
out=$(realpath -m "${2:?outdir}")
wt="$out/src"

if [ ! -d "$wt" ]; then
  git -C "$ROOT" worktree add --detach "$wt" "$commit" >/dev/null
fi
git -C "$wt" checkout --detach -q -f "$commit"
# PATCH=<file>: apply a patch on top of the commit (a prototype to measure).
if [ -n "${PATCH:-}" ]; then
  git -C "$wt" apply "$(realpath "$PATCH")"
fi
# The submodules are the same at every commit compared here; reuse them.
for sm in server/storage/third_party/googletest server/storage/third_party/masstree \
          server/storage/third_party/msgpack server/storage/third_party/spdlog third_party/duckdb; do
  want=$(git -C "$wt" ls-tree HEAD "$sm" | awk '{print $3}')
  have=$(git -C "$ROOT" ls-tree HEAD "$sm" | awk '{print $3}')
  [ "$want" = "$have" ] || { echo "$sm differs at $commit" >&2; exit 1; }
  rm -rf "${wt:?}/$sm"
  ln -s "$ROOT/$sm" "$wt/$sm"
done

cmake -S "$wt/server" -B "$out/server" -DCMAKE_BUILD_TYPE=Release >/dev/null
make -C "$out/server" -j"$(nproc)" >/dev/null
cmake -S "$wt/server" -B "$out/server_vg" -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_FLAGS="-mno-avx512f" >/dev/null
make -C "$out/server_vg" -j"$(nproc)" >/dev/null

# SKIP_PLUGIN=1: the change touches the storage server only.
if [ -n "${SKIP_PLUGIN:-}" ]; then
  echo "built $commit ($(git -C "$wt" rev-parse --short HEAD)) server only into $out"
  exit 0
fi
P="$ROOT/build/helios_plugin"
rm -rf "$P"
mkdir -p "$P/proto" "$P/common"
cp -a "$wt/plugin/." "$P/"
cp -a "$wt/proto/." "$P/proto/"
cp -a "$wt/common/." "$P/common/"
ninja -C "$ROOT/build" helios_storage_engine >/dev/null
mkdir -p "$out/plugin"
cp "$ROOT/build/plugin_output_directory/ha_helios_storage_engine.so" "$out/plugin/"
echo "built $commit ($(git -C "$wt" rev-parse --short HEAD)) into $out"
