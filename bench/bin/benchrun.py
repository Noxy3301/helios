#!/usr/bin/env python3
"""
Helios benchmark runner — YCSB and TPC-C via BenchBase.

Usage:
  # Single run (default: tpcc, 64 terminals, SF=1)
  python3 bench/bin/benchrun.py tpcc --terminals 64

  # Sweep thread counts
  python3 bench/bin/benchrun.py tpcc --sweep 1,4,16,32,64

  # YCSB with profile
  python3 bench/bin/benchrun.py ycsb --profile a --terminals 8 --scalefactor 100

Prerequisites:
  - BenchBase built (bench/bin/build_benchbase.py)

Server lifecycle:
  By default, this script auto-starts lineairdb-server + mysqld at the
  beginning and stops them at the end. Pass --external-server to opt out
  (e.g. when running against a remote MySQL or when servers are already
  managed externally). Auto-detection: if mysqld is already listening on
  --mysql-port, the script falls back to external mode automatically.
"""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"

ROOT = Path(__file__).resolve().parents[2]
BENCHBASE_DIR = ROOT / "bench" / "benchbase-mysql"
MYSQL_BIN = ROOT / "build" / "runtime_output_directory" / "mysql"
LINEAIRDB_CTL = ROOT / "build" / "server" / "lineairdb-ctl"
# The kernel truncates the process name to 15 characters
SERVER_COMM = "lineairdb-serve"
LINEAIRDB_LOG_DIR = ROOT / "lineairdb_logs"

YCSB_PROFILES = {
    "a": "50,0,0,50,0,0",
    "b": "95,0,0,5,0,0",
    "c": "100,0,0,0,0,0",
    "e": "0,5,95,0,0,0",
    "f": "50,0,0,0,0,50",
}

os.environ["LD_PRELOAD"] = "/lib/x86_64-linux-gnu/libjemalloc.so.2"


def _is_port_open(host, port, timeout=1.0):
    """Return True if a TCP listener is accepting connections on host:port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False


def _wait_for_port(host, port, timeout=30):
    """Block until host:port is open or timeout expires. Returns True on success."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_port_open(host, port):
            return True
        time.sleep(0.5)
    return False


def _run_script(argv, timeout, env=None):
    """Run a launcher script with stdin closed and a hard timeout.

    The launcher scripts spawn long-lived background daemons that previously
    inherited our pipe and blocked subprocess drainage forever. The scripts now
    redirect those daemons to a log file, but we still close stdin and apply a
    timeout here as defense in depth.
    """
    try:
        return subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as e:
        print(f"  ERROR: launcher timed out after {timeout}s: {' '.join(argv)}", file=sys.stderr)
        if e.stdout:
            print(e.stdout[-1000:], file=sys.stderr)
        return None


def start_lineairdb_server(commit_durability=None):
    """Start lineairdb-server via scripts/start_server.sh and wait for port 9999.

    commit_durability overrides the server's startup contract for this run only;
    it reaches the daemon through the launcher's environment.
    """
    if _is_port_open("127.0.0.1", 9999):
        print("  lineairdb-server already running on port 9999, reusing")
        return True
    print("  Starting lineairdb-server...")
    env = None
    if commit_durability:
        env = dict(os.environ, LINEAIRDB_COMMIT_DURABILITY=commit_durability)
        print(f"  LINEAIRDB_COMMIT_DURABILITY={commit_durability}")
    result = _run_script([str(SCRIPTS_DIR / "start_server.sh")], timeout=30, env=env)
    if result is None or result.returncode != 0:
        if result is not None:
            print(f"  ERROR starting lineairdb-server:\n{result.stdout}", file=sys.stderr)
        return False
    if not _wait_for_port("127.0.0.1", 9999, timeout=30):
        print("  ERROR: lineairdb-server did not become ready within 30s", file=sys.stderr)
        return False
    print("  lineairdb-server ready (port 9999)")
    return True


def start_mysql_server(mysqld_port=3307, server_host="127.0.0.1", server_port=9999):
    """Start mysqld via scripts/start_mysql.sh."""
    if _is_port_open("127.0.0.1", mysqld_port):
        print(f"  mysqld already running on port {mysqld_port}, reusing")
        return True
    print(f"  Starting mysqld (port {mysqld_port})...")
    result = _run_script(
        [str(SCRIPTS_DIR / "start_mysql.sh"),
         "--mysqld-port", str(mysqld_port),
         "--server-host", server_host,
         "--server-port", str(server_port)],
        timeout=120,  # initialize-insecure on first run can be slow
    )
    if result is None or result.returncode != 0:
        if result is not None:
            print(f"  ERROR starting mysqld:\n{result.stdout[-1000:]}", file=sys.stderr)
        return False
    if not _wait_for_port("127.0.0.1", mysqld_port, timeout=30):
        print(f"  ERROR: mysqld did not become ready within 30s", file=sys.stderr)
        return False
    print(f"  mysqld ready (port {mysqld_port})")
    return True


def server_startup_contract():
    """Commit durability the newest server log reports at startup, or None."""
    logs = sorted(LINEAIRDB_LOG_DIR.glob("lineairdb_server_*.log"),
                  key=lambda p: p.stat().st_mtime, reverse=True)
    if not logs:
        return None
    for line in logs[0].read_text(errors="replace").splitlines():
        marker = "Commit durability:"
        if marker in line:
            return line.split(marker, 1)[1].split()[0]
    return None


def switch_commit_durability(mode):
    """Move the running storage server to `mode` once everything acknowledged
    under the previous contract is durable. Returns the elapsed seconds, or
    None if the switch did not happen."""
    print(f"  Switching durability to {mode}, waiting for the barrier...")
    started = time.time()
    result = subprocess.run(
        [str(LINEAIRDB_CTL), "--host", "127.0.0.1", "--port", "9999",
         "set-durability", mode],
        capture_output=True, text=True)
    elapsed = time.time() - started
    if result.returncode != 0 or result.stdout.strip() != f"ok mode={mode.upper()}":
        print(f"  ERROR: durability switch to {mode} failed: "
              f"{(result.stdout + result.stderr).strip()}", file=sys.stderr)
        return None
    return elapsed


def stop_all_servers():
    """Stop mysqld and lineairdb-server via the stop scripts."""
    print("  Stopping mysqld + lineairdb-server...")
    subprocess.run([str(SCRIPTS_DIR / "stop_mysql.sh")], capture_output=True)
    subprocess.run([str(SCRIPTS_DIR / "stop_server.sh")], capture_output=True)
    time.sleep(2)
    for f in ["/tmp/lineairdb_server.pid", "/tmp/mysql.pid"]:
        try:
            Path(f).unlink()
        except FileNotFoundError:
            pass


def cleanup_lineairdb_logs():
    """Remove local LineairDB log files after managed benchmark runs."""
    if not LINEAIRDB_LOG_DIR.exists():
        return

    # Match start_lineairdb_server()'s reuse predicate (port) and catch a
    # relative-path launch that is not listening yet. A launch racing the
    # unlink below stays possible; the bench launcher does not do that.
    if _find_pid("build/server/lineairdb-server") or _is_port_open("127.0.0.1", 9999):
        print("  Skipping lineairdb_logs cleanup: lineairdb-server is still running")
        return

    removed = 0
    for path in LINEAIRDB_LOG_DIR.iterdir():
        try:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed += 1
        except FileNotFoundError:
            continue

    if removed:
        print(f"  Cleaned lineairdb_logs ({removed} entries)")


def mysql_cmd(port, host, sql):
    """Execute SQL via mysql client."""
    cmd = [
        str(MYSQL_BIN), "-u", "root",
        "--protocol=TCP", "-h", host, "-P", str(port),
        "-e", sql,
    ]
    return subprocess.run(cmd, capture_output=True, text=True)


def update_xml(config_path, **kwargs):
    """Update XML config values using regex replacement."""
    text = config_path.read_text()
    for tag, value in kwargs.items():
        text = re.sub(rf"<{tag}>.*?</{tag}>", f"<{tag}>{value}</{tag}>", text)
    config_path.write_text(text)


def _parse_mysql_endpoints(host_arg, default_port):
    """Parse --mysql-host into [(host, port), ...].

    Accepts a single host or a comma-separated list of endpoints; each
    entry is either a bare host (uses default_port) or host:port.
    """
    endpoints = []
    for entry in host_arg.split(","):
        entry = entry.strip()
        if not entry:
            sys.exit(f"Empty endpoint in --mysql-host: {host_arg!r}")
        if "[" in entry or "]" in entry:
            sys.exit(f"Bracketed IPv6 is not supported in --mysql-host: {entry!r}")
        host, sep, port = entry.rpartition(":")
        if sep and ":" not in host:
            if not host:
                sys.exit(f"Empty host in --mysql-host entry: {entry!r}")
            if not port.isdigit():
                sys.exit(f"Bad port in --mysql-host entry: {entry!r}")
            endpoints.append((host, int(port)))
        else:
            # No port suffix, or a bare IPv6 literal: use the default port.
            endpoints.append((entry, default_port))
    return endpoints


def _benchbase_plugin(benchmark):
    """BenchBase plugin name for a benchrun benchmark name.

    tpcc-np reuses the tpcc plugin: workload mix differs via the config XML,
    but the BenchBase class and resource directory (DDL, dialects) are shared.
    """
    return "tpcc" if benchmark == "tpcc-np" else benchmark


def run_benchbase(benchmark, config_path, create=False, load=False, execute=False, prefetch=False):
    """Run BenchBase with given phases."""
    jar = BENCHBASE_DIR / "benchbase.jar"
    if not jar.exists():
        print(f"ERROR: {jar} not found. Run: python3 bench/bin/build_benchbase.py", file=sys.stderr)
        sys.exit(1)

    flags = f"--create={'true' if create else 'false'} --load={'true' if load else 'false'} --execute={'true' if execute else 'false'}"
    cmd = f"java -jar {jar} -b {_benchbase_plugin(benchmark)} -c {config_path} {flags}"

    env = {**os.environ, "HELIOS_PREFETCH_PLAN": "1"} if prefetch else None
    result = subprocess.run(
        cmd, shell=True, cwd=BENCHBASE_DIR,
        capture_output=True, text=True, env=env,
    )
    return result


def extract_throughput(output):
    """Parse throughput and goodput from BenchBase output."""
    match = re.search(
        r"Results\(.*?measuredRequests=(\d+)\)\s*=\s*([\d.]+)\s*requests/sec\s*\(throughput\),\s*([\d.]+)\s*requests/sec\s*\(goodput\)",
        output,
    )
    if match:
        return {
            "requests": int(match.group(1)),
            "throughput": float(match.group(2)),
            "goodput": float(match.group(3)),
        }
    return None


def extract_histograms(output):
    """Parse retry and error counts from BenchBase output."""
    info = {}
    for label, key in [
        ("Rejected Transactions (Server Retry)", "server_retry"),
        ("Unexpected SQL Errors", "unexpected_errors"),
    ]:
        match = re.search(rf"{re.escape(label)}.*?(?=\[0;1m|\Z)", output, re.DOTALL)
        if match:
            section = match.group(0)
            total = sum(int(n) for n in re.findall(r"\[\s*(\d+)\]", section))
            info[key] = total
    return info


def collect_results(result_dir):
    """Read summary.json from BenchBase results."""
    for f in sorted(result_dir.glob("*.summary.json"), reverse=True):
        with open(f) as fh:
            return json.load(fh)
    return None


def _find_pid(pattern, by_name=False):
    """Find PID of a process matching pattern: its whole name, or its cmdline."""
    try:
        result = subprocess.run(
            ["pgrep", "-x" if by_name else "-f", pattern],
            capture_output=True, text=True,
        )
        pids = result.stdout.strip().split()
        return pids[0] if pids else None
    except Exception:
        return None


def _start_metrics(metrics_dir):
    """Start background metrics samplers. Returns list of (name, Popen, file) to stop later."""
    metrics_dir.mkdir(parents=True, exist_ok=True)
    samplers = []
    interval = "1"

    # mpstat -P ALL (overall CPU per core)
    f = open(metrics_dir / "mpstat.log", "w")
    p = subprocess.Popen(["mpstat", "-P", "ALL", interval], stdout=f, stderr=subprocess.DEVNULL)
    samplers.append(("mpstat", p, f))

    # vmstat (memory, IO, CPU overview)
    f = open(metrics_dir / "vmstat.log", "w")
    p = subprocess.Popen(["vmstat", interval], stdout=f, stderr=subprocess.DEVNULL)
    samplers.append(("vmstat", p, f))

    # sar -w (system-wide context switches/s)
    f = open(metrics_dir / "sar-w.log", "w")
    p = subprocess.Popen(["sar", "-w", interval], stdout=f, stderr=subprocess.DEVNULL)
    samplers.append(("sar-w", p, f))

    # pidstat for lineairdb-server
    server_pid = _find_pid("/build/server/lineairdb-server")
    if server_pid:
        f = open(metrics_dir / "pidstat-server.log", "w")
        p = subprocess.Popen(["pidstat", "-u", "-w", "-p", server_pid, interval], stdout=f, stderr=subprocess.DEVNULL)
        samplers.append(("pidstat-server", p, f))

        f = open(metrics_dir / "pidstat-server-threads.log", "w")
        p = subprocess.Popen(["pidstat", "-t", "-u", "-p", server_pid, "5"], stdout=f, stderr=subprocess.DEVNULL)
        samplers.append(("pidstat-server-threads", p, f))

    # pidstat for mysqld — use PID file to get the actual mysqld process,
    # not the wrapper script that pgrep might pick up first.
    mysql_pid = None
    mysql_pidfile = Path("/tmp/mysql.pid")
    if mysql_pidfile.exists():
        mysql_pid = mysql_pidfile.read_text().strip()
    if not mysql_pid:
        mysql_pid = _find_pid("mysqld.*lineairdb")
    if mysql_pid:
        f = open(metrics_dir / "pidstat-mysql.log", "w")
        p = subprocess.Popen(["pidstat", "-u", "-w", "-p", mysql_pid, interval], stdout=f, stderr=subprocess.DEVNULL)
        samplers.append(("pidstat-mysql", p, f))

    return samplers


def _stop_metrics(samplers):
    """Stop all background metrics samplers."""
    import signal
    for name, proc, fh in samplers:
        try:
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
            proc.wait()
        fh.close()


def run_analyze(benchmark, mysql_host, mysql_port):
    """Refresh optimizer statistics with ANALYZE TABLE."""
    # Tx-scoped prefetch requires MySQL's chosen plan to match the @_tx_plan
    # DSL; without fresh stats MySQL can pick PRIMARY where the DSL expects a
    # secondary index and retry forever, so those runs analyze automatically.
    analyze_sql = {
        "tpcc":   "ANALYZE TABLE customer, district, history, item, new_order, oorder, order_line, stock, warehouse;",
        "tpcc-np": "ANALYZE TABLE customer, district, history, item, new_order, oorder, order_line, stock, warehouse;",
        "ycsb":   "ANALYZE TABLE usertable;",
        "tpch":   "ANALYZE TABLE customer, lineitem, nation, orders, part, partsupp, region, supplier;",
    }.get(benchmark)
    if not analyze_sql:
        return
    print("  Refreshing MySQL stats (ANALYZE TABLE)...")
    result = mysql_cmd(mysql_port, mysql_host, f"USE benchbase; {analyze_sql}")
    if result.returncode != 0:
        sys.exit(f"ANALYZE TABLE failed on {mysql_host}:{mysql_port}")


def url_with_unique_checks_off(text):
    """Config text with unique_checks=0 added to the JDBC URL, or None if there
    is no <url> element. sessionVariables is one comma-separated list, so an
    existing one is extended to carry both variables."""
    match = re.search(r"<url>(.*?)</url>", text, re.DOTALL)
    if match is None:
        return None
    url = match.group(1)
    if "sessionVariables=" in url:
        url = re.sub(r"sessionVariables=[^&]*", r"\g<0>,unique_checks=0", url, count=1)
    else:
        url += ("&amp;" if "?" in url else "?") + "sessionVariables=unique_checks=0"
    return text[:match.start(1)] + url + text[match.end(1):]


def _self_check():
    """Checks url_with_unique_checks_off on the URL shapes in bench/config"""
    def patched(url):
        return url_with_unique_checks_off(f"<url>{url}</url>")

    short = "jdbc:mysql://127.0.0.1:3307/benchbase?rewriteBatchedStatements=true&amp;sslMode=DISABLED"
    long_ = ("jdbc:mysql://127.0.0.1:3307/benchbase?rewriteBatchedStatements=true"
             "&amp;allowLoadLocalInfile=true&amp;allowPublicKeyRetrieval=true&amp;sslMode=DISABLED")
    with_vars = long_ + "&amp;sessionVariables=secondary_engine_cost_threshold=0"
    for url in (short, long_):
        assert patched(url) == f"<url>{url}&amp;sessionVariables=unique_checks=0</url>", url
    assert patched(with_vars) == f"<url>{with_vars},unique_checks=0</url>", with_vars
    bare = "jdbc:mysql://127.0.0.1:3307/benchbase"
    assert patched(bare) == f"<url>{bare}?sessionVariables=unique_checks=0</url>", bare
    assert url_with_unique_checks_off("<parameters/>") is None


def setup_benchmark(benchmark, config_path, mysql_host, mysql_port, log_dir=None, unique_checks_off=True):
    """Reset DB, create schema, load data. Returns load_time or None on failure."""
    db_name = "benchbase"

    print("  Resetting database...")
    mysql_cmd(mysql_port, mysql_host, f"DROP DATABASE IF EXISTS {db_name}; CREATE DATABASE {db_name};")

    if benchmark == "tpch":
        mysql_cmd(mysql_port, mysql_host,
                  "SET GLOBAL optimizer_switch='batched_key_access=on,mrr_cost_based=off,subquery_to_derived=off';"
                  "SET GLOBAL join_buffer_size=1073741824;")

    # Load through a throwaway copy so the JDBC session variable never reaches
    # the execute phase, which keeps using config_path.
    load_config = config_path
    if unique_checks_off:
        _self_check()
        patched = url_with_unique_checks_off(config_path.read_text())
        if patched is None:
            sys.exit(f"--load-defer-unique-checks on: no <url> element in {config_path}")
        load_config = config_path.with_suffix(".load.xml")

    try:
        if load_config != config_path:
            load_config.write_text(patched)
            print("  Load: unique_checks=0")
        print("  Creating schema + Loading data...")
        result = run_benchbase(benchmark, load_config, create=True, load=True, execute=False)
    finally:
        if load_config != config_path:
            load_config.unlink(missing_ok=True)
    if log_dir:
        try:
            (Path(log_dir) / "benchbase_load.log").write_text(result.stdout + result.stderr)
        except OSError as e:
            print(f"  WARNING: could not save load log: {e}", file=sys.stderr)
    if result.returncode != 0:
        print(f"  ERROR during create/load:\n{result.stdout[-500:]}\n{result.stderr[-500:]}", file=sys.stderr)
        return None

    load_match = re.search(r"Finished executing.*?\[time=([\d.]+)s\]", result.stdout)
    load_time = float(load_match.group(1)) if load_match else None
    if load_time:
        print(f"  Load time: {load_time:.1f}s")

    return load_time


def attach_secondary(benchmark, mysql_host, mysql_port):
    """Attach and load the columnar secondary engine before execution.

    Offload needs both; SECONDARY_LOAD state is process-local to mysqld, so
    this runs before every execute phase, not only after a fresh load."""
    if benchmark != "tpch":
        return
    print(f"  Attaching the columnar secondary engine ({mysql_host}:{mysql_port})...")
    # ALTER success alone proves nothing: SECONDARY_LOAD on an unavailable
    # engine succeeds with only a warning, and execution then silently falls
    # back to the primary engine. Verify the plugin and the per-table state.
    result = mysql_cmd(mysql_port, mysql_host,
                       "SELECT PLUGIN_NAME FROM information_schema.PLUGINS "
                       "WHERE PLUGIN_NAME='lineairdb_columnar' "
                       "AND PLUGIN_STATUS='ACTIVE';")
    if "lineairdb_columnar" not in result.stdout.lower():
        sys.exit(f"lineairdb_columnar plugin is not ACTIVE on {mysql_host}:{mysql_port}")
    for t in ("customer", "lineitem", "nation", "orders",
              "part", "partsupp", "region", "supplier"):
        result = mysql_cmd(mysql_port, mysql_host,
                           f"USE benchbase; ALTER TABLE {t} SECONDARY_ENGINE=lineairdb_columnar;")
        if result.returncode != 0 and "already has a secondary engine" not in result.stderr:
            sys.exit(f"SECONDARY_ENGINE attach failed for {t}: {result.stderr.strip()[-300:]}")
        result = mysql_cmd(mysql_port, mysql_host,
                           f"USE benchbase; ALTER TABLE {t} SECONDARY_LOAD;")
        if result.returncode != 0:
            sys.exit(f"SECONDARY_LOAD failed for {t}: {result.stderr.strip()[-300:]}")
        result = mysql_cmd(mysql_port, mysql_host,
                           "SELECT CREATE_OPTIONS FROM information_schema.TABLES "
                           f"WHERE TABLE_SCHEMA='benchbase' AND TABLE_NAME='{t}';")
        options = result.stdout.lower()
        if ('secondary_engine="lineairdb_columnar"' not in options or
                'secondary_load="1"' not in options):
            sys.exit(f"secondary engine state not verified for {t}: "
                     f"{result.stdout.strip()[-200:]}")


def run_execute(benchmark, config_path, terminals, result_base, prefetch=False):
    """Run execute phase with metrics collection. Returns result dict."""
    print(f"\n{'='*50}")
    print(f"  {benchmark.upper()} | Terminals: {terminals}")
    print(f"{'='*50}")

    update_xml(config_path, terminals=str(terminals))

    res_dir = result_base / f"thread_{terminals}"
    res_dir.mkdir(parents=True, exist_ok=True)

    # Clear BenchBase results from previous iteration
    bb_results = BENCHBASE_DIR / "results"
    if bb_results.exists():
        shutil.rmtree(bb_results)

    # Start system metrics (server + mysql)
    metrics_dir = res_dir / "metrics"
    samplers = _start_metrics(metrics_dir)

    # Launch BenchBase execute asynchronously, then attach pidstat to Java process
    print("  Executing benchmark...")
    jar = BENCHBASE_DIR / "benchbase.jar"
    bb_cmd = ["java", "-jar", str(jar), "-b", _benchbase_plugin(benchmark), "-c", str(config_path),
              "--create=false", "--load=false", "--execute=true"]
    env = {**os.environ, "HELIOS_PREFETCH_PLAN": "1"} if prefetch else None
    bb_proc = subprocess.Popen(bb_cmd, cwd=BENCHBASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    # bb_proc.pid IS the java process (no shell wrapper)
    f = open(metrics_dir / "pidstat-bench.log", "w")
    p = subprocess.Popen(["pidstat", "-u", "-w", "-p", str(bb_proc.pid), "1"], stdout=f, stderr=subprocess.DEVNULL)
    samplers.append(("pidstat-bench", p, f))
    print(f"  Metrics: {len(samplers)} samplers started")

    # Wait for BenchBase to finish
    stdout, stderr = bb_proc.communicate()

    # Stop metrics collection
    _stop_metrics(samplers)

    combined = stdout + stderr
    try:
        (res_dir / "benchbase_output.log").write_text(combined)
    except Exception:
        pass
    perf = extract_throughput(combined)
    histograms = extract_histograms(combined)

    if perf:
        print(f"  Throughput: {perf['throughput']:.1f} req/s")
        print(f"  Server Retry: {histograms.get('server_retry', 0)} | Unexpected Errors: {histograms.get('unexpected_errors', 0)}")
    else:
        print(f"  WARNING: Could not parse throughput from output")
        if bb_proc.returncode != 0:
            print(f"  BenchBase stderr (last 500 chars):\n{stderr[-500:]}")

    # Move BenchBase output files
    bb_results = BENCHBASE_DIR / "results"
    if bb_results.exists():
        for csv in bb_results.glob("*.csv"):
            shutil.move(str(csv), str(res_dir / csv.name))
        for jf in bb_results.glob("*.json"):
            shutil.move(str(jf), str(res_dir / jf.name))

    return {
        "terminals": terminals,
        **(perf or {}),
        **histograms,
    }


TX_TYPES = ["NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel"]
TX_COLORS = {
    "NewOrder": "#1f77b4",
    "Payment": "#ff7f0e",
    "OrderStatus": "#2ca02c",
    "Delivery": "#d62728",
    "StockLevel": "#9467bd",
}


def _plot_tpcc_latency(result_base, plot_dir, scalefactor):
    """Generate TPC-C per-tx-type latency distribution plot from local results."""
    import csv
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    # Collect latency data: {terminals: {tx_type: {p50, p95, p99}}}
    results = {}
    for thread_dir in sorted(result_base.glob("thread_*")):
        m = re.match(r"thread_(\d+)", thread_dir.name)
        if not m:
            continue
        terminals = int(m.group(1))
        for tx in TX_TYPES:
            csvs = list(thread_dir.glob(f"*.results.{tx}.csv"))
            if not csvs:
                continue
            with open(csvs[0]) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            if not rows:
                continue
            # Drop last row (may be partial window)
            data_rows = rows[:-1] if len(rows) > 2 else rows
            results.setdefault(terminals, {})[tx] = {
                "p50": np.mean([float(r["Median Latency (millisecond)"]) for r in data_rows]),
                "p95": np.mean([float(r["95th Percentile Latency (millisecond)"]) for r in data_rows]),
                "p99": np.mean([float(r["99th Percentile Latency (millisecond)"]) for r in data_rows]),
            }

    if not results:
        return

    terminals = sorted(results.keys())
    fig, axes = plt.subplots(1, len(TX_TYPES), figsize=(20, 5), sharey=True)

    for i, tx in enumerate(TX_TYPES):
        ax = axes[i]
        p50 = [results[t].get(tx, {}).get("p50", 0) for t in terminals]
        p95 = [results[t].get(tx, {}).get("p95", 0) for t in terminals]
        p99 = [results[t].get(tx, {}).get("p99", 0) for t in terminals]
        color = TX_COLORS[tx]

        ax.fill_between(terminals, p50, p95, alpha=0.15, color=color)
        ax.fill_between(terminals, p95, p99, alpha=0.08, color=color)
        ax.plot(terminals, p50, "o-", color=color, linewidth=2, markersize=4, label="p50")
        ax.plot(terminals, p95, "^-", color=color, linewidth=1, markersize=3, alpha=0.7, label="p95")
        ax.plot(terminals, p99, "s--", color=color, linewidth=1, markersize=3, alpha=0.5, label="p99")

        ax.set_title(tx, fontweight="bold")
        ax.set_xlabel("Terminals")
        if i == 0:
            ax.set_ylabel("Latency (ms)")
        ax.legend(fontsize=7, loc="upper left")
        ax.grid(True, alpha=0.2)

    fig.suptitle(f"TPC-C Latency Distribution (SF={scalefactor})", y=1.02)
    plt.tight_layout()
    path = plot_dir / "tpcc_latency.png"
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Plot saved: {path}")


def _parse_pidstat_cpu(path):
    """Parse pidstat -u output, return [cpu%]. Handles AM/PM and alternating -w lines."""
    samples = []
    cpu_col = None
    for line in path.read_text().splitlines():
        parts = line.split()
        if not parts or parts[0] == "Linux":
            continue
        if "%CPU" in parts:
            cpu_col = parts.index("%CPU")
            continue
        if cpu_col is not None and len(parts) > cpu_col:
            try:
                samples.append(float(parts[cpu_col]))
            except (ValueError, IndexError):
                pass
    return samples


def _parse_sar_w(path):
    """Parse sar -w output, return [cswch/s]. Handles AM/PM time format."""
    samples = []
    cswch_col = None
    for line in path.read_text().splitlines():
        parts = line.split()
        if not parts:
            continue
        if "cswch/s" in parts:
            cswch_col = parts.index("cswch/s")
            continue
        if cswch_col is None or line.startswith("Average") or line.startswith("Linux"):
            continue
        try:
            samples.append(float(parts[cswch_col]))
        except (ValueError, IndexError):
            continue
    return samples


def _parse_mpstat_cpu_count(path):
    """Read the measured host CPU count from an mpstat header."""
    try:
        for line in path.read_text().splitlines():
            m = re.search(r"\((\d+) CPU\)", line)
            if m:
                return int(m.group(1))
    except OSError:
        pass
    return None


def _detect_metrics_cpu_count(thread_dirs):
    """Use the benchmark host CPU count, not the plotting host CPU count."""
    import multiprocessing

    for td in thread_dirs:
        mpstat = td / "metrics" / "mpstat.log"
        if not mpstat.exists():
            continue
        nproc = _parse_mpstat_cpu_count(mpstat)
        if nproc:
            return nproc
    return multiprocessing.cpu_count()


def _plot_metrics(result_base, plot_dir):
    """Plot stacked CPU area + system-wide cswch/s as a 2-row dashboard."""
    import multiprocessing
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    thread_dirs = sorted(result_base.glob("thread_*"), key=lambda p: int(re.match(r"thread_(\d+)", p.name).group(1)))
    if not thread_dirs:
        return

    nproc = _detect_metrics_cpu_count(thread_dirs)
    cpu_max = nproc * 100

    terminals_list = []
    server_cpu = {}
    mysql_cpu = {}
    bench_cpu = {}
    cswch = {}

    for td in thread_dirs:
        m = re.match(r"thread_(\d+)", td.name)
        if not m:
            continue
        t = int(m.group(1))
        metrics_dir = td / "metrics"
        if not metrics_dir.exists():
            continue
        terminals_list.append(t)

        ps = metrics_dir / "pidstat-server.log"
        if ps.exists():
            server_cpu[t] = _parse_pidstat_cpu(ps)

        pm = metrics_dir / "pidstat-mysql.log"
        if pm.exists():
            mysql_cpu[t] = _parse_pidstat_cpu(pm)

        pb = metrics_dir / "pidstat-bench.log"
        if pb.exists():
            bench_cpu[t] = _parse_pidstat_cpu(pb)

        sw = metrics_dir / "sar-w.log"
        if sw.exists():
            cswch[t] = _parse_sar_w(sw)

    if not terminals_list:
        return

    n = len(terminals_list)
    fig, axes = plt.subplots(2, n, figsize=(5 * n, 7), squeeze=False,
                             sharey="row")

    for col, t in enumerate(terminals_list):
        # Row 0: Stacked CPU area (server + mysql + bench)
        ax = axes[0][col]
        s = server_cpu.get(t, [])
        m = mysql_cpu.get(t, [])
        b = bench_cpu.get(t, [])
        length = max(len(s), len(m), len(b))
        if length > 0:
            x = list(range(length))
            sv = (s + [0] * length)[:length]
            mv = (m + [0] * length)[:length]
            bv = (b + [0] * length)[:length]
            y1 = sv
            y2 = [a + b for a, b in zip(sv, mv)]
            y3 = [a + b for a, b in zip(y2, bv)]
            ax.fill_between(x, 0, y1, alpha=0.3, color="#1f78b4", label="LineairDB")
            ax.fill_between(x, y1, y2, alpha=0.3, color="#e31a1c", label="MySQL")
            ax.fill_between(x, y2, y3, alpha=0.3, color="#ff7f0e", label="BenchBase")
        ax.set_ylim(0, cpu_max)
        ax.set_title(f"{t}t")
        if col == 0:
            ax.set_ylabel(f"CPU % ({nproc} cores)")
            ax.legend(fontsize=8, loc="upper left")
        ax.grid(True, alpha=0.2)

        # Row 1: System-wide cswch/s (sar -w)
        ax = axes[1][col]
        if t in cswch and cswch[t]:
            ax.plot(cswch[t], color="#d95f02", linewidth=1)
        ax.set_xlabel("seconds")
        if col == 0:
            ax.set_ylabel("cswch/s (system)")
        ax.grid(True, alpha=0.2)

    fig.suptitle("System Metrics", fontweight="bold")
    plt.tight_layout()
    path = plot_dir / "metrics.png"
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Plot saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Helios benchmark runner")
    parser.add_argument("benchmark", choices=["tpcc", "tpcc-np", "tpch", "ycsb"], help="Benchmark type")
    parser.add_argument("--terminals", type=int, default=64, help="Number of terminals (default: 64)")
    parser.add_argument("--sweep", type=str, help="Comma-separated thread counts to sweep (e.g. 1,4,16,64)")
    parser.add_argument("--scalefactor", type=float, default=1, help="Scale factor (default: 1)")
    parser.add_argument("--time", type=int, default=60, help="Execute time in seconds (default: 60)")
    parser.add_argument("--profile", type=str, default="a", help="YCSB profile: a,b,c,e,f (default: a)")
    parser.add_argument("--mysql-host", type=str, default="127.0.0.1",
                        help="MySQL host, or comma-separated host[:port] endpoints for BenchBase "
                             "loadbalance (control-plane operations always target the first endpoint)")
    parser.add_argument("--mysql-port", type=int, default=3307,
                        help="Default port for --mysql-host entries without an explicit port (default: 3307)")
    parser.add_argument("--loader-threads", type=int, default=1, help="Number of parallel loader threads (default: 1)")
    parser.add_argument("--load-defer-unique-checks", choices=["on", "off"], default="on",
                        help="Defer UNIQUE secondary-index checks during the create and load "
                             "phases; the execute phase always keeps them on (default: on)")
    parser.add_argument("--load-durability", choices=["same", "async"], default="same",
                        help="Commit durability for the load phase: same keeps the server's "
                             "startup contract, async starts it under Async and switches it "
                             "to Sync before ANALYZE/execute (managed lifecycle only)")
    parser.add_argument("--exclude-queries", type=str, help="TPC-H: comma-separated query numbers to exclude (e.g. 15,21)")
    parser.add_argument("--no-setup", action="store_true", help="Skip setup (DROP+CREATE+LOAD), assume data exists")
    parser.add_argument("--no-load", action="store_true", help="Run setup with CREATE only, skip LOAD")
    parser.add_argument("--no-exec", action="store_true", help="Run setup only, skip execute phase")
    parser.add_argument("--analyze", action="store_true",
                        help="Run ANALYZE TABLE after load (automatic for tx-scoped --prefetch runs)")
    parser.add_argument("--external-server", action="store_true",
                        help="Skip auto start/stop of lineairdb-server and mysqld (assume already running)")
    parser.add_argument("--keep-lineairdb-logs", action="store_true",
                        help="Keep lineairdb_logs after the benchmark")
    parser.add_argument("--prefetch", action="store_true",
                        help="Enable Prefetch path: SET GLOBAL lineairdb_prefetch_execution=ON and "
                             "pass HELIOS_PREFETCH_PLAN=1 to BenchBase (TPC-C procedures inject @_tx_plan)")
    parser.add_argument("--prefetch-stmt", action="store_true",
                        help="Statement-scoped autogen prefetch: SET GLOBAL lineairdb_prefetch_execution=ON "
                             "WITHOUT HELIOS_PREFETCH_PLAN, so the proxy derives a per-statement read plan "
                             "from the QEP instead of the injected @_tx_plan DSL")
    args = parser.parse_args()

    # Validate
    if args.load_durability == "async":
        if args.external_server or args.no_setup:
            sys.exit("--load-durability async starts the storage server under the load "
                     "contract: not valid with --external-server or --no-setup")
        if not LINEAIRDB_CTL.exists():
            sys.exit(f"--load-durability async needs {LINEAIRDB_CTL} to end the load "
                     "contract; build it first")
        if "LINEAIRDB_COMMIT_DURABILITY" in os.environ:
            sys.exit("--load-durability async sets LINEAIRDB_COMMIT_DURABILITY itself; "
                     "unset it in the environment first")
    jar = BENCHBASE_DIR / "benchbase.jar"
    if not jar.exists():
        print(f"ERROR: {jar} not found.\nRun: python3 bench/bin/build_benchbase.py", file=sys.stderr)
        sys.exit(1)

    # Prepare config
    config_src = ROOT / "bench" / "config" / f"{args.benchmark}.xml"
    if not config_src.exists():
        print(f"ERROR: {config_src} not found", file=sys.stderr)
        sys.exit(1)

    # Work on a copy to avoid polluting the original
    config_dir = ROOT / "bench" / "config" / "generated"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_work = config_dir / f"{args.benchmark}.run.xml"
    shutil.copy2(config_src, config_work)

    # Update config: scalefactor, time, and JDBC URL (host:port)
    update_xml(config_work, scalefactor=str(args.scalefactor), time=str(args.time))
    # Rewrite the JDBC URL. --mysql-host may be comma-separated endpoints;
    # more than one switches to the loadbalance scheme for the execute phase
    # while control-plane operations keep targeting the first endpoint.
    endpoints = _parse_mysql_endpoints(args.mysql_host, args.mysql_port)
    text = config_work.read_text()
    if len(endpoints) > 1:
        hostports = ",".join(f"{h}:{p}" for h, p in endpoints)
        print(f"  Multi-endpoint MySQL: {hostports} (control-plane -> {endpoints[0][0]}:{endpoints[0][1]})")
        text = re.sub(
            r"jdbc:mysql://[^/]+/",
            f"jdbc:mysql:loadbalance://{hostports}/",
            text,
        )
    else:
        host, port = endpoints[0]
        text = re.sub(
            r"jdbc:mysql://[^/]+/",
            f"jdbc:mysql://{host}:{port}/",
            text,
        )
    config_work.write_text(text)
    # Collapse args.mysql_host/port to the first endpoint so every downstream
    # control-plane use (mysql_cmd calls, local-mode detection) is scoped to
    # it automatically. No-op for the single-endpoint case.
    args.mysql_endpoints = endpoints
    args.mysql_host, args.mysql_port = endpoints[0]
    # TPC-H with multiple terminals needs parallel mode (serial=false + time tag)
    if args.benchmark == "tpch" and (args.sweep or args.terminals > 1):
        text = config_work.read_text()
        text = text.replace("<serial>true</serial>", "<serial>false</serial>")
        if "<time>" not in text:
            text = text.replace(
                "<serial>false</serial>",
                f"<serial>false</serial>\n            <time>{args.time}</time>",
            )
        config_work.write_text(text)
    if args.loader_threads > 1:
        text = config_work.read_text()
        if "<loaderThreads>" in text:
            update_xml(config_work, loaderThreads=str(args.loader_threads))
        else:
            text = text.replace("</parameters>", f"    <loaderThreads>{args.loader_threads}</loaderThreads>\n</parameters>")
            config_work.write_text(text)
    if args.benchmark == "tpch" and args.exclude_queries:
        exclude_set = {int(q.strip()) for q in args.exclude_queries.split(",")}
        text = config_work.read_text()
        # TPC-H has 22 queries, weights is "1,1,...,1" (22 values)
        match = re.search(r"<weights>([\d,]+)</weights>", text)
        if match:
            weights = match.group(1).split(",")
            for q in exclude_set:
                if 1 <= q <= len(weights):
                    weights[q - 1] = "0"
            new_weights = ",".join(weights)
            text = text.replace(match.group(0), f"<weights>{new_weights}</weights>", 1)
            config_work.write_text(text)
            print(f"  Excluded TPC-H queries: {sorted(exclude_set)}")
    if args.benchmark == "ycsb":
        weights = YCSB_PROFILES.get(args.profile)
        if not weights:
            print(f"ERROR: Unknown YCSB profile '{args.profile}'. Options: {list(YCSB_PROFILES.keys())}", file=sys.stderr)
            sys.exit(1)
        update_xml(config_work, weights=weights)

    # Determine thread list
    if args.sweep:
        thread_list = [int(t.strip()) for t in args.sweep.split(",")]
    else:
        thread_list = [args.terminals]

    # Result directory
    now = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    result_base = ROOT / "bench" / "results" / now / args.benchmark.upper()
    result_base.mkdir(parents=True, exist_ok=True)

    print(f"Benchmark: {args.benchmark.upper()}")
    print(f"Threads:   {thread_list}")
    print(f"SF={args.scalefactor}, Time={args.time}s, MySQL={args.mysql_host}:{args.mysql_port}")
    print(f"Results:   {result_base}")

    # Decide whether to manage server lifecycle.
    # Skip management when --external-server is set, when targeting a remote
    # mysqld (--mysql-host != localhost), or when something is already
    # listening on the configured mysql port.
    managed = not args.external_server
    if managed and args.mysql_host not in ("127.0.0.1", "localhost"):
        print(f"  --mysql-host={args.mysql_host} is not local, switching to external mode")
        managed = False
    if managed and _is_port_open("127.0.0.1", args.mysql_port):
        print(f"  mysqld already listening on port {args.mysql_port}, switching to external mode")
        managed = False
    # stop_mysql.sh kills every local mysqld, not just the one started here.
    if managed and len(args.mysql_endpoints) > 1:
        sys.exit("Multi-endpoint --mysql-host requires --external-server: "
                 "the managed lifecycle starts one mysqld but stops them all.")
    # Setup through the loadbalance URL spreads DDL across endpoints, and an
    # index created on a non-loader node is silently absent elsewhere.
    if len(args.mysql_endpoints) > 1 and not args.no_setup:
        sys.exit("Multi-endpoint --mysql-host requires --no-setup: "
                 "set up the loader endpoint first, then create the schema "
                 "on each remaining endpoint, then rerun with --no-setup.")

    # async only means anything for a server this run starts itself, and
    # start_server.sh reuses one that is already up.
    if args.load_durability == "async":
        if not managed:
            sys.exit("--load-durability async needs the managed server lifecycle")
        if _is_port_open("127.0.0.1", 9999):
            sys.exit("--load-durability async has to start lineairdb-server itself, "
                     "but port 9999 already has a listener")
        running = _find_pid(SERVER_COMM, by_name=True)
        if running:
            try:
                cwd = os.readlink(f"/proc/{running}/cwd")
            except OSError:
                cwd = "unknown"
            sys.exit("--load-durability async has to start lineairdb-server itself, "
                     f"but pid {running} is already running (cwd {cwd})")
    if managed:
        # Wipe any WAL left by a previous run before starting a fresh server,
        # so stale logs never trigger recovery. No-op if lineairdb-server is
        # already running (reuse case, handled inside the function).
        cleanup_lineairdb_logs()
        load_durability = "async" if args.load_durability == "async" else None
        if not start_lineairdb_server(commit_durability=load_durability):
            sys.exit(1)
        # A server that was already up would have been reused: the log is the
        # only proof that the load contract is the one we asked for.
        startup_contract = server_startup_contract() if load_durability else None
        if load_durability and startup_contract != load_durability:
            print(f"  ERROR: the storage server did not start under "
                  f"{load_durability} (log says {startup_contract})",
                  file=sys.stderr)
            stop_all_servers()
            sys.exit(1)
        if not start_mysql_server(args.mysql_port, "127.0.0.1", 9999):
            stop_all_servers()
            sys.exit(1)

    try:
        _run_bench(args, config_work, thread_list, result_base)
    finally:
        if managed:
            stop_all_servers()
        if not args.keep_lineairdb_logs:
            cleanup_lineairdb_logs()


def _run_bench(args, config_work, thread_list, result_base):
    """Setup + execute sweep + summary + plots. Extracted so main() can wrap it."""
    # Toggle Prefetch sysvar explicitly to avoid stale state from prior runs.
    # The master sysvar is per-mysqld and volatile, so set it on every endpoint
    # and fail fast: an endpoint left on the wrong mode corrupts the run.
    prefetch_value = "ON" if (args.prefetch or args.prefetch_stmt) else "OFF"
    print(f"  Setting lineairdb_prefetch_execution={prefetch_value}")
    for host, port in args.mysql_endpoints:
        result = mysql_cmd(port, host,
                           f"SET GLOBAL lineairdb_prefetch_execution={prefetch_value};")
        if result.returncode != 0:
            sys.exit(f"Failed to set lineairdb_prefetch_execution on {host}:{port}")

    # Setup phase
    load_time = None
    if args.no_setup:
        print("  Skipping setup (--no-setup)")
        load_time = 0
    elif args.no_load:
        print("  Setup: CREATE only (--no-load)")
        db_name = "benchbase"
        # Create DB if it doesn't exist, but don't DROP (preserves stats/share state).
        mysql_cmd(args.mysql_port, args.mysql_host, f"CREATE DATABASE IF NOT EXISTS {db_name};")
        result = run_benchbase(args.benchmark, config_work, create=True, load=False, execute=False)
        if result.returncode != 0:
            print(f"  WARNING: CREATE had errors (may be OK for shared-storage)")
        load_time = 0
    else:
        load_time = setup_benchmark(args.benchmark, config_work, args.mysql_host, args.mysql_port,
                                    log_dir=result_base,
                                    unique_checks_off=(args.load_defer_unique_checks == "on"))
        if load_time is None:
            print("Setup failed.", file=sys.stderr)
            sys.exit(1)

    if args.load_durability == "async":
        elapsed = switch_commit_durability("sync")
        if elapsed is None:
            sys.exit(1)
        print(f"  Durability switch async -> sync: {elapsed:.2f}s")
        (result_base / "durability.txt").write_text(
            f"load_mode=async\nmeasured_mode=sync\nswitch_seconds={elapsed:.2f}\n")

    # A separate step so staged runs (--no-setup, --no-load) still get it.
    if args.analyze or args.prefetch:
        run_analyze(args.benchmark, args.mysql_host, args.mysql_port)

    if args.no_exec:
        print("  Skipping execute (--no-exec)")
        return

    # Every endpoint executes queries, so every endpoint needs the load.
    # After --no-exec, so a load-only stage can still add indexes: MySQL
    # rejects most DDL once a secondary engine is defined.
    for host, port in args.mysql_endpoints:
        attach_secondary(args.benchmark, host, port)

    # Execute: sweep terminal counts (data is reused)
    all_results = []
    for terminals in thread_list:
        result = run_execute(args.benchmark, config_work, terminals, result_base, prefetch=args.prefetch)
        if result:
            result["load_time"] = load_time
            all_results.append(result)

    # Summary
    print(f"\n{'='*60}")
    print(f"  SUMMARY: {args.benchmark.upper()} SF={args.scalefactor}")
    print(f"{'='*60}")
    print(f"{'Threads':>8} {'Throughput':>12} {'Goodput':>10} {'Retry':>8} {'Errors':>8}")
    print(f"{'-'*8:>8} {'-'*12:>12} {'-'*10:>10} {'-'*8:>8} {'-'*8:>8}")
    for r in all_results:
        print(f"{r['terminals']:>8} {r.get('throughput', 0):>12.1f} {r.get('goodput', 0):>10.1f} {r.get('server_retry', 0):>8} {r.get('unexpected_errors', 0):>8}")

    # Save summary CSV
    csv_path = result_base / "summary.csv"
    with open(csv_path, "w") as f:
        f.write("terminals,throughput,goodput,server_retry,unexpected_errors,load_time\n")
        for r in all_results:
            f.write(f"{r['terminals']},{r.get('throughput',0):.1f},{r.get('goodput',0):.1f},{r.get('server_retry',0)},{r.get('unexpected_errors',0)},{r.get('load_time','')}\n")
    print(f"\nResults saved: {csv_path}")

    # Generate plots
    if len(all_results) > 1:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            plot_dir = result_base / "_plot"
            plot_dir.mkdir(parents=True, exist_ok=True)

            # Throughput plot (always): solid = throughput, dashed = goodput
            terminals = [r["terminals"] for r in all_results]
            throughput = [r.get("throughput", 0) for r in all_results]
            goodput = [r.get("goodput", 0) for r in all_results]

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(terminals, throughput, "b-o", label="Throughput", linewidth=2)
            ax.plot(terminals, goodput, "b--s", label="Goodput", linewidth=1.5, markersize=5, alpha=0.8)
            ax.set_xlabel("Terminals")
            ax.set_ylabel("req/s")
            ax.set_title(f"{args.benchmark.upper()} SF={args.scalefactor}")
            ax.legend()
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig(plot_dir / "throughput.png", dpi=150)
            plt.close()
            print(f"Plot saved: {plot_dir / 'throughput.png'}")

            # TPC-C latency distribution plot (also for NP variant)
            if args.benchmark in ("tpcc", "tpcc-np"):
                _plot_tpcc_latency(result_base, plot_dir, args.scalefactor)

            # Metrics plots (CPU, context switches, per-process)
            _plot_metrics(result_base, plot_dir)

        except ImportError:
            pass


if __name__ == "__main__":
    main()
