#!/usr/bin/env python3
"""Replay a fixed sample on one connection and report per-process CPU time.

Usage: replay.py <sample.json[.gz]> [--port 3307] [--warm K] [--limit N]
         [--pids mysqld=PID,storage=PID] [--before CMD] [--after CMD] [--digest]

The first --warm transactions run unmeasured. --before/--after run a shell
command around the measured part (for example callgrind_control -i on / -d).
For each named pid it reports on-CPU time of the live threads (schedstat, ns)
and user/system time of the whole process (/proc/<pid>/stat, exited threads
included, 10 ms ticks). --digest hashes every result set, so two replays from
the same starting state can be checked for identical answers.
"""
import argparse
import gzip
import hashlib
import json
import os
import subprocess
import time

import mysql.connector

CLK = os.sysconf("SC_CLK_TCK")


def cpu(pid):
    """On-CPU time summed over the live threads of pid, in seconds, from
    /proc/<pid>/task/*/schedstat (nanoseconds, unlike the 10 ms ticks of stat)."""
    total = 0
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            with open(f"/proc/{pid}/task/{tid}/schedstat") as f:
                total += int(f.read().split()[0])
        except OSError:
            pass
    return total / 1e9


def ustime(pid):
    """(utime, stime) in seconds for the whole process, exited threads included."""
    with open(f"/proc/{pid}/stat") as f:
        x = f.read().rsplit(")", 1)[1].split()
    return int(x[11]) / CLK, int(x[12]) / CLK


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sample")
    ap.add_argument("--port", type=int, default=3307)
    ap.add_argument("--warm", type=int, default=0, help="transactions run before measuring")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--pids", default="")
    ap.add_argument("--before", default="")
    ap.add_argument("--after", default="")
    ap.add_argument("--digest", action="store_true")
    a = ap.parse_args()

    opener = gzip.open if a.sample.endswith(".gz") else open
    with opener(a.sample, "rt") as f:
        s = json.load(f)
    txns = s["txns"][: a.limit] if a.limit else s["txns"]
    pids = dict(kv.split("=") for kv in a.pids.split(",") if kv)

    conn = mysql.connector.connect(host="127.0.0.1", port=a.port, user="root",
                                   database="benchbase", autocommit=False, ssl_disabled=True)
    cur = conn.cursor()
    for st in s["setup"]:
        try:
            cur.execute(st)
            if cur.with_rows:
                cur.fetchall()
        except mysql.connector.Error:
            pass

    h = hashlib.sha256()
    errors = 0

    def run(t):
        nonlocal errors
        for st in t["stmts"]:
            try:
                cur.execute(st)
                if cur.with_rows:
                    rows = cur.fetchall()
                    if a.digest:
                        h.update(repr(rows).encode())
            except mysql.connector.Error as e:
                errors += 1
                if a.digest:
                    h.update(f"ERR{e.errno}".encode())
        if t["end"] == "commit":
            conn.commit()
        else:
            conn.rollback()

    for t in txns[: a.warm]:
        run(t)
    measured = txns[a.warm:]
    if a.before:
        subprocess.run(a.before, shell=True, check=True)
    c0 = {k: cpu(v) for k, v in pids.items()}
    u0 = {k: ustime(v) for k, v in pids.items()}
    w0 = time.perf_counter()
    for t in measured:
        run(t)
    w1 = time.perf_counter()
    c1 = {k: cpu(v) for k, v in pids.items()}
    u1 = {k: ustime(v) for k, v in pids.items()}
    if a.after:
        subprocess.run(a.after, shell=True, check=True)
    n = len(measured)
    out = {"txns": n, "wall_s": round(w1 - w0, 4), "errors": errors,
           "wall_us_per_txn": round((w1 - w0) / max(n, 1) * 1e6, 1)}
    for k in pids:
        out[f"cpu_us_per_txn_{k}"] = round((c1[k] - c0[k]) / max(n, 1) * 1e6, 1)
        out[f"user_us_per_txn_{k}"] = round((u1[k][0] - u0[k][0]) / max(n, 1) * 1e6, 1)
        out[f"sys_us_per_txn_{k}"] = round((u1[k][1] - u0[k][1]) / max(n, 1) * 1e6, 1)
    if a.digest:
        out["digest"] = h.hexdigest()[:16]
    print(json.dumps(out))


if __name__ == "__main__":
    main()
