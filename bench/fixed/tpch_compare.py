#!/usr/bin/env python3
"""Compare fixed_tpch.py outputs of two builds, query by query.

Usage: tpch_compare.py --a a1.json a2.json --b b1.json b2.json
Per query: the median wall time and the median CPU time (utime+stime ticks,
which keep threads that exited, such as DuckDB's) of each build over all its
runs, the ratio b/a, and whether every run of both builds gave the same md5.
"""
import argparse
import json
import statistics as st

ap = argparse.ArgumentParser()
ap.add_argument("--a", nargs="+", required=True)
ap.add_argument("--b", nargs="+", required=True)
a = ap.parse_args()


def load(paths):
    out = {}
    for p in paths:
        for q in json.load(open(p))["queries"]:
            out.setdefault(q["q"], []).extend(q["runs"])
    return out


A, B = load(a.a), load(a.b)
print(f"{'q':>3} | {'wall a':>8} {'wall b':>8} {'b/a':>5} | {'st cpu a':>8} {'st cpu b':>8} {'b/a':>5} | "
      f"{'my cpu a':>8} {'my cpu b':>8} | md5")
tot = [0, 0, 0, 0]
for q in sorted(A):
    ra, rb = A[q], B.get(q, [])
    wa = st.median(r["wall_ns"] for r in ra) / 1e6
    wb = st.median(r["wall_ns"] for r in rb) / 1e6
    sa = st.median(r["cpu_ticks"]["storage"] for r in ra) * 10
    sb = st.median(r["cpu_ticks"]["storage"] for r in rb) * 10
    ma = st.median(r["cpu_ticks"]["mysqld"] for r in ra) * 10
    mb = st.median(r["cpu_ticks"]["mysqld"] for r in rb) * 10
    md5 = {r["md5"] for r in ra + rb}
    tot = [tot[0] + wa, tot[1] + wb, tot[2] + sa, tot[3] + sb]
    print(f"{q:>3} | {wa:8.1f} {wb:8.1f} {wb / wa:5.2f} | {sa:8.0f} {sb:8.0f} {(sb / sa if sa else 0):5.2f} | "
          f"{ma:8.0f} {mb:8.0f} | {'same' if len(md5) == 1 else 'DIFF ' + str(len(md5))}")
print(f"sum | {tot[0]:8.1f} {tot[1]:8.1f} {tot[1] / tot[0]:5.2f} | {tot[2]:8.0f} {tot[3]:8.0f} {tot[3] / tot[2]:5.2f} |")
