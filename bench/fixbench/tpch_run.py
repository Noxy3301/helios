#!/usr/bin/env python3
"""Run the 22 fixed TPC-H queries on one connection, the way bench/config/tpch.xml
does (autocommit, SERIALIZABLE, secondary_engine_cost_threshold=0), and report per
query: wall ms, on-CPU ms of mysqld and storage, rows, md5 of the result and whether
the query was offloaded to the secondary engine.

Usage: tpch_run.py [--qdir bench/queries/tpch] --sf 0.1 [--runs 3] [--only 1,6]
         [--pids mysqld=P,storage=P] [--before CMD] [--after CMD]
--before/--after run around each query ({q} is replaced by the query number).
Q11's FRACTION is scaled by 1/SF, as the TPC-H specification defines it.
"""
import argparse, hashlib, json, os, re, subprocess, sys, time
import mysql.connector

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from replay import cpu  # noqa: E402


def load_query(qdir, q, sf):
    sql = open(os.path.join(qdir, f"q{q}.sql")).read()
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    if q == 11:  # FRACTION = 0.0001 / SF in the TPC-H spec
        sql = re.sub(r"0\.0001\b", repr(0.0001 / sf), sql)
    return [s.strip() for s in sql.split(";") if s.strip()]


def main():
    ap = argparse.ArgumentParser()
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ap.add_argument("--qdir", default=os.path.join(root, "bench", "queries", "tpch"))
    ap.add_argument("--sf", type=float, required=True)
    ap.add_argument("--runs", type=int, default=3); ap.add_argument("--only", default="")
    ap.add_argument("--pids", default=""); ap.add_argument("--before", default=""); ap.add_argument("--after", default="")
    a = ap.parse_args()
    pids = dict(kv.split("=") for kv in a.pids.split(",") if kv)
    qs = [int(x) for x in a.only.split(",")] if a.only else list(range(1, 23))
    c = mysql.connector.connect(host="127.0.0.1", port=3307, user="root", database="benchbase",
                                autocommit=True, ssl_disabled=True)
    cur = c.cursor()
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
    cur.execute("SET SESSION secondary_engine_cost_threshold=0")

    def sec_count():
        cur.execute("SHOW SESSION STATUS LIKE 'Secondary_engine_execution_count'")
        return int(cur.fetchall()[0][1])

    for q in qs:
        stmts = load_query(a.qdir, q, a.sf)
        for r in range(a.runs):
            cur.execute("DROP VIEW IF EXISTS revenue0")
            if a.before:
                subprocess.run(a.before.replace("{q}", str(q)), shell=True, check=True)
            s0 = sec_count()
            c0 = {k: cpu(v) for k, v in pids.items()}
            h = hashlib.md5(); nrows = 0; err = None
            w0 = time.perf_counter()
            try:
                for st in stmts:
                    cur.execute(st)
                    if cur.with_rows:
                        rows = cur.fetchall(); nrows += len(rows); h.update(repr(rows).encode())
            except mysql.connector.Error as e:
                err = f"{e.errno}"
            w1 = time.perf_counter()
            c1 = {k: cpu(v) for k, v in pids.items()}
            off = sec_count() - s0
            if a.after:
                subprocess.run(a.after.replace("{q}", str(q)), shell=True, check=True)
            out = {"q": q, "run": r, "wall_ms": round((w1 - w0) * 1e3, 2), "rows": nrows,
                   "md5": h.hexdigest()[:12], "offloaded": off, "err": err}
            for k in pids:
                out[f"cpu_ms_{k}"] = round((c1[k] - c0[k]) * 1e3, 2)
            print(json.dumps(out), flush=True)


if __name__ == "__main__":
    main()
