#!/usr/bin/env bash

# Package the locally built Helios binaries into a tar.gz for remote deploy;
# the nodes carry no toolchain. Usage: build_bundle.sh [out.tar.gz]; HELIOS_ROOT
# overrides the repository location. The archive extracts to ~/helios. No
# patchelf: the start scripts get an injected LD_LIBRARY_PATH and keep their
# jemalloc LD_PRELOAD. The server carries the build machine's -march=native ISA;
# for an older CPU run scripts/build_portable.sh and copy its binary into build/server/.

set -euo pipefail

HELIOS="${HELIOS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
OUT="${1:-$PWD/helios-bundle.tar.gz}"

# --- prerequisite check -------------------------------------------------------
req() { [ -e "$1" ] || { echo "ERROR: missing $1" >&2; exit 1; }; }
req "$HELIOS/build/runtime_output_directory/mysqld"
req "$HELIOS/build/runtime_output_directory/mysql"
req "$HELIOS/build/runtime_output_directory/mysqladmin"
req "$HELIOS/build/plugin_output_directory/ha_lineairdb_storage_engine.so"
req "$HELIOS/build/library_output_directory/libprotobuf-lite.so.24.4.0"
req "$HELIOS/build/library_output_directory/libprotobuf.so.24.4.0"
req "$HELIOS/build/share/english/errmsg.sys"
req "$HELIOS/build/server/lineairdb-server"
req "$HELIOS/third_party/duckdb/build/release/src/libduckdb.so"
req "$HELIOS/bench/benchbase-mysql/benchbase.jar"
req "$HELIOS/bench/benchbase-mysql/config/plugin.xml"
req "$HELIOS/bench/setup.sql"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
B="$STAGE/helios"

mkdir -p \
  "$B/scripts" \
  "$B/build/runtime_output_directory" \
  "$B/build/plugin_output_directory" \
  "$B/build/library_output_directory" \
  "$B/build/share/english" \
  "$B/build/server" \
  "$B/build/lib" \
  "$B/bench/config" \
  "$B/bench/benchbase-mysql/results" \
  "$B/lineairdb_logs"

# --- mysqld / client (query node) ------------------------------------------------
cp "$HELIOS/build/runtime_output_directory/"{mysqld,mysql,mysqladmin} \
   "$B/build/runtime_output_directory/"
cp "$HELIOS/build/plugin_output_directory/ha_lineairdb_storage_engine.so" \
   "$B/build/plugin_output_directory/"
cp "$HELIOS/build/library_output_directory/"{libprotobuf-lite.so.24.4.0,libprotobuf.so.24.4.0} \
   "$B/build/library_output_directory/"
cp "$HELIOS/build/share/english/errmsg.sys" "$B/build/share/english/"
# charsets aren't in the build tree (the built-in charset works fine), but
# dynamic charset lookups may need them, so bundle them from the source tree.
if [ -d "$HELIOS/third_party/mysql-server/share/charsets" ]; then
  cp -r "$HELIOS/third_party/mysql-server/share/charsets" "$B/build/share/charsets"
fi
# Runtime plugin-dir default is <basedir>/lib/plugin; reproduce it with a
# relative symlink.
ln -s ../plugin_output_directory "$B/build/lib/plugin"

# --- lineairdb-server (storage node) ------------------------------------------
cp "$HELIOS/build/server/lineairdb-server" "$B/build/server/"
# Its RUNPATH (…/third_party/duckdb/build/release/src) doesn't exist remotely;
# co-locate libduckdb.so in library_output_directory so one LD_LIBRARY_PATH
# entry covers it too.
cp "$HELIOS/third_party/duckdb/build/release/src/libduckdb.so" \
   "$B/build/library_output_directory/"

# --- benchbase (bench node) ---------------------------------------------------
cp "$HELIOS/bench/benchbase-mysql/benchbase.jar" "$B/bench/benchbase-mysql/"
cp -r "$HELIOS/bench/benchbase-mysql/lib"    "$B/bench/benchbase-mysql/lib"
cp -r "$HELIOS/bench/benchbase-mysql/config" "$B/bench/benchbase-mysql/config"
cp "$HELIOS/bench/config/"*.xml "$B/bench/config/"
cp "$HELIOS/bench/setup.sql" "$B/bench/"

# --- scripts (start/stop) -----------------------------------------------------
cp "$HELIOS/scripts/"{start_mysql.sh,stop_mysql.sh,start_server.sh,stop_server.sh} \
   "$B/scripts/"

# Inject a cd to the bundle root and an LD_LIBRARY_PATH export into the start
# scripts: lineairdb-server writes ./lineairdb_logs relative to cwd, and the
# library path replaces the build machine's absolute RUNPATH
for s in start_mysql.sh start_server.sh; do
  sed -i '1a\
# --- injected by build_bundle.sh (relocatable bundle) ---\
cd "$(dirname "${BASH_SOURCE[0]}")/.."\
export LD_LIBRARY_PATH="$PWD/build/library_output_directory${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\
# --------------------------------------------------------' "$B/scripts/$s"
done

# Ad hoc use: not needed for the mysql client alone, but source this when
# running mysqld etc. by hand.
cat > "$B/env.sh" <<'EOF'
# source me: Helios bundle runtime environment
export LD_LIBRARY_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/build/library_output_directory${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
EOF

# --- provenance metadata -------------------------------------------------------
{
  echo "built_at: $(date -Iseconds)"
  echo "built_on: $(hostname) ($(uname -sr), glibc $(ldd --version | head -1 | awk '{print $NF}'))"
  echo "helios:   $(git -C "$HELIOS" rev-parse --abbrev-ref HEAD)@$(git -C "$HELIOS" rev-parse --short HEAD)"
  for sub in third_party/LineairDB third_party/mysql-server third_party/duckdb third_party/benchbase; do
    [ -d "$HELIOS/$sub/.git" ] || [ -f "$HELIOS/$sub/.git" ] || continue
    echo "$(basename "$sub"): $(git -C "$HELIOS/$sub" rev-parse --short HEAD)"
  done
} > "$B/BUNDLE_INFO.txt"

# --- sanity check: every dependency .so resolves (system or bundled) ---------
check_ldd() {
  local f="$1"
  local miss
  miss=$(LD_LIBRARY_PATH="$B/build/library_output_directory" ldd "$f" 2>/dev/null \
         | grep 'not found' || true)
  if [ -n "$miss" ]; then
    echo "WARNING: unresolved deps in $f:" >&2
    echo "$miss" >&2
  fi
}
check_ldd "$B/build/runtime_output_directory/mysqld"
check_ldd "$B/build/plugin_output_directory/ha_lineairdb_storage_engine.so"
check_ldd "$B/build/server/lineairdb-server"

# --- tar -----------------------------------------------------------------------
# No -h: keep the build/lib/plugin symlink as a symlink
if command -v pigz >/dev/null 2>&1; then
  tar -C "$STAGE" -I pigz -cf "$OUT" helios
else
  tar -C "$STAGE" -czf "$OUT" helios
fi

echo "bundle : $OUT ($(du -h "$OUT" | cut -f1))"
echo "sha256 : $(sha256sum "$OUT" | cut -d' ' -f1)"
echo "raw    : $(du -sh "$B" | cut -f1)"
echo
echo "remote extract: tar -C ~ -xzf $(basename "$OUT")   # -> ~/helios"
echo "remote prereqs: sudo apt-get install -y libjemalloc2 libnuma1 libatomic1 libprotobuf32t64 sysstat, plus a Java 23 runtime"
