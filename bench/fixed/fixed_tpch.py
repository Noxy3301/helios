#!/usr/bin/env python3
"""Fixed-sample TPC-H: run the checked-in Q1-Q22 one at a time against a
running stack and record, per query and run, the wall time and the CPU time
mysqld and the storage server spent (from /proc), plus the md5 of the sorted
result lines (the same digest tpch_wall.sh takes).

Usage: fixed_tpch.py <out.json> [--runs 3] [--warm 1] [--only 1,6,18]
       [--engine duckdb|plan] [--hook "<cmd>"]

--engine plan sets use_secondary_engine=OFF for the session, so the query
runs through the plugin's read plans instead of DuckDB.
--hook is run as "<cmd> begin <label>" and "<cmd> end <label>" around each
measured run (cg_hook.sh for callgrind).
"""

import argparse
import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MYSQL = ROOT / "build" / "runtime_output_directory" / "mysql"
QDIR = ROOT / "bench" / "queries" / "tpch"


def pid_of(pattern):
    out = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else None


def cpu_ns(pid):
    """Sum of per-thread on-CPU ns (schedstat), plus utime+stime ticks."""
    ns = 0
    for t in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{t}/schedstat") as f:
                ns += int(f.read().split()[0])
        except OSError:
            pass
    with open(f"/proc/{pid}/stat") as f:
        s = f.read()
    fields = s[s.rindex(")") + 2:].split()
    return ns, int(fields[11]) + int(fields[12])


def run_query(sql, engine, extra_pre=""):
    pre = "SET SESSION use_secondary_engine=OFF;\n" if engine == "plan" else ""
    pre += extra_pre
    t0 = time.monotonic_ns()
    p = subprocess.run([str(MYSQL), "-u", "root", "--socket=/tmp/mysql.sock", "-N", "-B", "benchbase"],
                       input=pre + sql, capture_output=True, text=True)
    wall = time.monotonic_ns() - t0
    lines = sorted(p.stdout.splitlines())
    md5 = hashlib.md5(("\n".join(lines) + "\n").encode()).hexdigest()
    return wall, md5, p.returncode, p.stderr.strip()[:300], len(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--warm", type=int, default=1)
    ap.add_argument("--only", default="")
    ap.add_argument("--engine", choices=["duckdb", "plan"], default="duckdb")
    ap.add_argument("--hook", default=None)
    ap.add_argument("--pre", default="",
                    help="SQL run in the same session before each query, e.g. "
                         "'SET SESSION secondary_engine_cost_threshold=0;'")
    a = ap.parse_args()

    qs = [int(x) for x in a.only.split(",")] if a.only else list(range(1, 23))
    pids = {"storage": pid_of("helios-storage --config"),
            "mysqld": pid_of("runtime_output_directory/mysqld")}
    res = {"engine": a.engine, "runs": a.runs, "queries": []}
    for q in qs:
        sql = (QDIR / f"q{q}.sql").read_text()
        for _ in range(a.warm):
            run_query(sql, a.engine, a.pre)
        rows = []
        for r in range(a.runs):
            label = f"q{q}_r{r}"
            if a.hook:
                subprocess.run(f"{a.hook} begin {label}", shell=True, check=True)
            c0 = {k: cpu_ns(v) for k, v in pids.items()}
            wall, md5, rc, err, n = run_query(sql, a.engine, a.pre)
            c1 = {k: cpu_ns(v) for k, v in pids.items()}
            if a.hook:
                subprocess.run(f"{a.hook} end {label}", shell=True, check=True)
            rows.append({"wall_ns": wall, "md5": md5, "rc": rc, "err": err, "rows": n,
                         "cpu_ns": {k: c1[k][0] - c0[k][0] for k in pids},
                         "cpu_ticks": {k: c1[k][1] - c0[k][1] for k in pids}})
        walls = sorted(x["wall_ns"] for x in rows)
        print(f"q{q:<3} median {walls[len(walls)//2]/1e6:9.1f} ms  "
              f"storage {sorted(x['cpu_ns']['storage'] for x in rows)[len(rows)//2]/1e6:9.1f} ms  "
              f"mysqld {sorted(x['cpu_ns']['mysqld'] for x in rows)[len(rows)//2]/1e6:8.1f} ms  "
              f"md5 {rows[0]['md5'][:8]} rc {rows[0]['rc']}", flush=True)
        res["queries"].append({"q": q, "runs": rows})
    Path(a.out).write_text(json.dumps(res, indent=1))


if __name__ == "__main__":
    main()
