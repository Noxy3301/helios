#!/usr/bin/env python3
"""TPC-H per-query baseline orchestrator with strict timeout.

Runs each TPC-H query individually under a per-query time budget, parses
BenchBase output, and writes a CSV summary. Manages lineairdb-server +
mysqld lifecycle unless --external-server is set.

Designed for Phase-1 baseline diagnosis when some queries (NLJ without
prefetch) are catastrophically slow and the whole-suite benchrun.py
approach gets stuck on a single in-flight query.

Usage:
  python3 bench/bin/phase1_tpch_per_query.py \\
    --scalefactor 0.01 --time 30 --hard-timeout 60 \\
    --exclude-queries 15,21 --label nlj_baseline

  # only Q14:
  python3 bench/bin/phase1_tpch_per_query.py --include-queries 14 ...

  # with oneshot toggle (note: TPC-H has no DSL injection yet, so this
  # just flips the GLOBAL lineairdb_oneshot_execution sysvar):
  python3 bench/bin/phase1_tpch_per_query.py --oneshot ...
"""

import argparse
import csv
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / "scripts"
BENCHBASE_DIR = ROOT / "bench" / "benchbase-mysql"
CONFIG_SRC = ROOT / "bench" / "config" / "tpch.xml"
MYSQL_BIN = ROOT / "build" / "runtime_output_directory" / "mysql"

os.environ.setdefault("LD_PRELOAD", "/lib/x86_64-linux-gnu/libjemalloc.so.2")


def is_port_open(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False


def wait_for_port(host, port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_port_open(host, port):
            return True
        time.sleep(0.5)
    return False


def start_servers(mysqld_port=3307, server_host="127.0.0.1", server_port=9999):
    """Start lineairdb-server then mysqld. Returns True on success."""
    print(f"[lifecycle] starting lineairdb-server (port {server_port})...")
    r = subprocess.run([str(SCRIPTS_DIR / "start_server.sh")],
                       stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        print(f"  ERROR: start_server.sh rc={r.returncode}\n{r.stdout}\n{r.stderr}", file=sys.stderr)
        return False
    if not wait_for_port(server_host, server_port, timeout=30):
        print(f"  ERROR: lineairdb-server port {server_port} not open", file=sys.stderr)
        return False

    print(f"[lifecycle] starting mysqld (port {mysqld_port})...")
    r = subprocess.run([str(SCRIPTS_DIR / "start_mysql.sh"),
                        "--mysqld-port", str(mysqld_port),
                        "--server-host", server_host,
                        "--server-port", str(server_port)],
                       stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        print(f"  ERROR: start_mysql.sh rc={r.returncode}\n{r.stdout}\n{r.stderr}", file=sys.stderr)
        return False
    if not wait_for_port("127.0.0.1", mysqld_port, timeout=60):
        print(f"  ERROR: mysqld port {mysqld_port} not open", file=sys.stderr)
        return False
    time.sleep(1)
    return True


def stop_servers():
    print("[lifecycle] stopping mysqld + lineairdb-server...")
    subprocess.run([str(SCRIPTS_DIR / "stop_mysql.sh")],
                   stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=30)
    subprocess.run([str(SCRIPTS_DIR / "stop_server.sh")],
                   stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=30)


def mysql_exec(sql, mysqld_port=3307, db="benchbase"):
    """Run an SQL statement via the mysql client, return CompletedProcess."""
    cmd = [str(MYSQL_BIN), "-u", "root", "-h", "127.0.0.1",
           "-P", str(mysqld_port), "--protocol=TCP", "-e", sql]
    if db:
        cmd[1:1] = []  # no -D, we put db at the end if needed
        cmd.append(db)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=10)


def set_oneshot_global(enabled, mysqld_port=3307):
    val = "ON" if enabled else "OFF"
    cmd = [str(MYSQL_BIN), "-u", "root", "-h", "127.0.0.1",
           "-P", str(mysqld_port), "--protocol=TCP",
           "-e", f"SET GLOBAL lineairdb_oneshot_execution={val};"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    if r.returncode != 0:
        print(f"  WARN: failed to set lineairdb_oneshot_execution={val}: {r.stderr}", file=sys.stderr)
    else:
        print(f"[oneshot] GLOBAL lineairdb_oneshot_execution = {val}")


def make_per_query_config(query_num, time_budget, scalefactor, terminals,
                          mysql_host, mysql_port, out_path, serial=True):
    """Generate a tpch.xml with only one query defined and weight=1.

    BenchBase's serial+weight=0 combination throws 'Thread tried executing
    disabled phase!' for non-last queries — even with weights=[0..0,1,0..0]
    targeting that one query. Workaround: strip transactiontypes down to just
    the target so the weights array size matches and no 'disabled' indices
    exist for the worker to stumble onto.

    Special case: query_num == 0 means 'keep all 22 transactiontypes' — used
    for the LOAD invocation where no specific query needs to be picked.
    """
    tree = ET.parse(CONFIG_SRC)
    root = tree.getroot()

    # scalefactor / terminals
    sf = root.find("scalefactor")
    if sf is not None:
        sf.text = str(scalefactor)
    t = root.find("terminals")
    if t is not None:
        t.text = str(terminals)
    # JDBC URL host:port
    url = root.find("url")
    if url is not None:
        new_url = url.text.replace("localhost:3307", f"{mysql_host}:{mysql_port}")
        new_url = new_url.replace("127.0.0.1:3307", f"{mysql_host}:{mysql_port}")
        url.text = new_url

    # Restrict transactiontypes to the target query only AND renumber id=1.
    # Reason: BenchBase ResultWriter indexes per-Q ArrayLists by transactiontype
    # id-1, so a single trimmed entry must have id=1 (not the original 14, etc.)
    # otherwise writeRaw blows up with IndexOutOfBoundsException after a
    # successful execution.
    if query_num != 0:
        tx_types = root.find("transactiontypes")
        if tx_types is not None:
            for tt in list(tx_types):
                name_el = tt.find("name")
                if name_el is None or name_el.text != f"Q{query_num}":
                    tx_types.remove(tt)
                else:
                    id_el = tt.find("id")
                    if id_el is not None:
                        id_el.text = "1"

    works = root.find("works")
    for w in list(works):
        works.remove(w)
    work = ET.SubElement(works, "work")
    ET.SubElement(work, "time").text = str(time_budget)
    # serial=true runs queries one-at-a-time (latency probing); serial=false is
    # time-based concurrent execution across `terminals` workers (throughput /
    # scaling measurement).
    ET.SubElement(work, "serial").text = "true" if serial else "false"
    ET.SubElement(work, "rate").text = "unlimited"
    weights_count = 22 if query_num == 0 else 1
    if query_num == 0:
        weights = ["1"] * 22
    else:
        weights = ["1"]
    ET.SubElement(work, "weights").text = ",".join(weights)

    tree.write(out_path, xml_declaration=True, encoding="unicode")


def parse_benchbase_summary(result_subdir):
    """Locate and parse summary.json under result_subdir.

    Returns dict with keys count, throughput, p50_ms, p99_ms — or None on miss."""
    if not result_subdir.exists():
        return None
    candidates = list(result_subdir.glob("**/*.summary.json"))
    if not candidates:
        return None
    summary = json.loads(candidates[0].read_text())
    # BenchBase summary has "Throughput (requests/second)" and "Latency Distribution"
    out = {
        "count": None,
        "throughput": summary.get("Throughput (requests/second)"),
        "p50_ms": None,
        "p99_ms": None,
    }
    latency = summary.get("Latency Distribution", {})
    p50_us = latency.get("50th Percentile Latency (microseconds)") or latency.get("Median Latency (microseconds)")
    p99_us = latency.get("99th Percentile Latency (microseconds)")
    if p50_us is not None:
        out["p50_ms"] = p50_us / 1000.0
    if p99_us is not None:
        out["p99_ms"] = p99_us / 1000.0
    # also try the per-tx samples csv
    for csv_path in result_subdir.glob("**/*.samples.csv"):
        rows = csv_path.read_text().splitlines()
        # header + data lines; data line per second of execution
        if len(rows) > 1:
            out["count"] = sum(1 for _ in rows[1:])  # rough
        break
    return out


def run_query(query_num, args, result_root, oneshot):
    """Run a single TPC-H query. Returns dict with metrics."""
    cfg_dir = result_root / "configs"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    cfg = cfg_dir / f"q{query_num}.xml"
    make_per_query_config(query_num, args.time, args.scalefactor, args.terminals,
                          args.mysql_host, args.mysql_port, cfg,
                          serial=not args.parallel)

    out_subdir = result_root / f"q{query_num:02d}"
    out_subdir.mkdir(parents=True, exist_ok=True)

    cmd = ["java", "-jar", str(BENCHBASE_DIR / "benchbase.jar"),
           "-b", "tpch", "-c", str(cfg),
           "--create=false", "--load=false", "--execute=true",
           "-d", str(out_subdir)]
    env = {**os.environ}
    # NOTE: we no longer inject the per-query hardcoded @_ldb_plan
    # (HELIOS_ONESHOT_PLAN). Those legacy DSL plans (e.g. Q9's
    # "...FES:lineitem:l_partkey:B2.K.0.8...") use coarse byte-slice bindings
    # that over-scan and run 10-30x slower at SF=1. With oneshot enabled and NO
    # plan injected, the engine AUTO-GENERATES the prefetch plan from the QEP,
    # which is clean 2-RPC and fast. So --oneshot now only flips the GLOBAL
    # sysvar (set_oneshot_global) and lets pure auto-gen run.

    start = time.time()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              timeout=args.hard_timeout, cwd=str(BENCHBASE_DIR), env=env)
        elapsed = time.time() - start
        status = "OK" if proc.returncode == 0 else "FAIL"
        # always persist stdout+stderr for debugging
        (out_subdir / "_run.stdout").write_text(proc.stdout or "")
        (out_subdir / "_run.stderr").write_text(proc.stderr or "")
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "")[-400:]
            print(f"     stderr_tail={tail!r}")
    except subprocess.TimeoutExpired as e:
        elapsed = time.time() - start
        status = "TIMEOUT"
        (out_subdir / "_timeout.stderr").write_bytes(e.stderr or b"")
        (out_subdir / "_timeout.stdout").write_bytes(e.stdout or b"")

    metrics = parse_benchbase_summary(out_subdir) or {}
    metrics.update({"query": f"Q{query_num}", "wall_sec": round(elapsed, 2),
                    "status": status})
    return metrics


def kill_orphan_benchbase():
    subprocess.run(["pkill", "-f", "java -jar.*benchbase"],
                   capture_output=True, timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scalefactor", type=float, default=0.01)
    parser.add_argument("--terminals", type=int, default=1)
    parser.add_argument("--time", type=int, default=30,
                        help="BenchBase per-query time budget (s)")
    parser.add_argument("--hard-timeout", type=int, default=60,
                        help="subprocess kill timeout per query (s)")
    parser.add_argument("--exclude-queries", type=str, default="15,21",
                        help="comma-separated query numbers to skip")
    parser.add_argument("--include-queries", type=str, default="",
                        help="if set, only these query numbers run")
    parser.add_argument("--oneshot", action="store_true",
                        help="set HELIOS_ONESHOT_PLAN=1 + GLOBAL lineairdb_oneshot_execution=ON")
    parser.add_argument("--parallel", action="store_true",
                        help="non-serial time-based concurrent execution (throughput/scaling) "
                             "instead of one-query-at-a-time serial latency probing")
    parser.add_argument("--no-load", action="store_true",
                        help="skip DROP/CREATE/LOAD, assume data exists from previous run")
    parser.add_argument("--external-server", action="store_true",
                        help="skip server start/stop, assume already running")
    parser.add_argument("--label", type=str, default="phase1",
                        help="label appended to result dir name")
    parser.add_argument("--mysql-host", type=str, default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3307)
    parser.add_argument("--server-host", type=str, default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=9999)
    args = parser.parse_args()

    excluded = set(int(x) for x in args.exclude_queries.split(",") if x.strip())
    if args.include_queries:
        included = set(int(x) for x in args.include_queries.split(",") if x.strip())
    else:
        included = set(range(1, 23)) - excluded

    queries_to_run = sorted(q for q in included if 1 <= q <= 22)
    if not queries_to_run:
        print("No queries to run after include/exclude filters", file=sys.stderr)
        sys.exit(2)

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    result_root = ROOT / "bench" / "results" / f"phase1_{timestamp}_{args.label}"
    result_root.mkdir(parents=True, exist_ok=True)
    print(f"=== Phase-1 TPC-H per-query bench ===")
    print(f"  SF={args.scalefactor}, terminals={args.terminals}, "
          f"bench_time={args.time}s, hard_timeout={args.hard_timeout}s, "
          f"oneshot={'ON' if args.oneshot else 'OFF'}, label={args.label}")
    print(f"  queries: {queries_to_run}")
    print(f"  result_root: {result_root}")

    own_servers = False
    if not args.external_server:
        if is_port_open(args.mysql_host, args.mysql_port):
            print("[lifecycle] servers already up — switching to external mode")
            args.external_server = True
        else:
            if not start_servers(args.mysql_port, args.server_host, args.server_port):
                stop_servers()
                sys.exit(1)
            own_servers = True

    if args.oneshot:
        set_oneshot_global(True, args.mysql_port)

    try:
        if not args.no_load:
            print("[setup] DROP+CREATE+LOAD via benchbase.jar...")
            cfg_dir = result_root / "configs"
            cfg_dir.mkdir(parents=True, exist_ok=True)
            cfg = cfg_dir / "load.xml"
            # query_num=0 keeps all 22 transactiontypes (load doesn't pick queries)
            make_per_query_config(0, args.time, args.scalefactor, args.terminals,
                                  args.mysql_host, args.mysql_port, cfg)
            load_cmd = ["java", "-jar", str(BENCHBASE_DIR / "benchbase.jar"),
                        "-b", "tpch", "-c", str(cfg),
                        "--create=true", "--load=true", "--execute=false"]
            start = time.time()
            r = subprocess.run(load_cmd, capture_output=True, text=True,
                               cwd=str(BENCHBASE_DIR), timeout=600)
            print(f"[setup] load done in {time.time()-start:.1f}s rc={r.returncode}")
            if r.returncode != 0:
                print(f"  ERROR: load failed\n{r.stderr[-800:]}", file=sys.stderr)
                sys.exit(1)

        results = []
        print(f"\n{'Query':>6} {'Wall(s)':>10} {'p50(ms)':>10} {'p99(ms)':>10} {'tput':>8} {'Status':>10}")
        print("-" * 60)
        for q in queries_to_run:
            sys.stdout.write(f"  Q{q:02d}  ")
            sys.stdout.flush()
            kill_orphan_benchbase()  # defense
            m = run_query(q, args, result_root, args.oneshot)
            p50 = m.get("p50_ms") if m.get("p50_ms") is not None else "n/a"
            p99 = m.get("p99_ms") if m.get("p99_ms") is not None else "n/a"
            tp = m.get("throughput") if m.get("throughput") is not None else "n/a"
            print(f"{m['wall_sec']:>10}  {str(p50):>10}  {str(p99):>10}  {str(tp):>8}  {m['status']:>10}")
            results.append(m)
        print("-" * 60)

        # write CSV
        csv_path = result_root / "summary.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["query", "wall_sec", "status",
                                              "count", "throughput", "p50_ms", "p99_ms"])
            w.writeheader()
            for r in results:
                w.writerow({k: r.get(k) for k in w.fieldnames})
        print(f"\nSummary CSV: {csv_path}")

        n_ok = sum(1 for r in results if r["status"] == "OK")
        n_timeout = sum(1 for r in results if r["status"] == "TIMEOUT")
        n_fail = sum(1 for r in results if r["status"] == "FAIL")
        print(f"OK={n_ok}, TIMEOUT={n_timeout}, FAIL={n_fail}")
    finally:
        if own_servers:
            stop_servers()


if __name__ == "__main__":
    main()
