#!/usr/bin/env python3
"""Summarize FixedTpcc JSON outputs: per block, the CPU time per transaction
of each process and the latency, as the mean and spread over the given runs.

Usage: summarize.py run1.json [run2.json ...]
"""
import json
import statistics as st
import sys

runs = [json.load(open(p)) for p in sys.argv[1:]]
labels = [b["label"] for b in runs[0]["blocks"]]
print(f"{'block':12} {'n':>4} | {'storage us/tx':>18} | {'mysqld us/tx':>18} | {'p50 ms':>14} | {'mean ms':>8}")
for i, lab in enumerate(labels):
    bs = [r["blocks"][i] for r in runs]
    n = bs[0]["count"]
    def per(k):
        v = [b["cpu"][k]["sched_ns"] / b["count"] / 1e3 for b in bs]
        m = st.mean(v)
        sp = (max(v) - min(v)) / m * 100 if m else 0
        return f"{m:9.0f} ±{sp:5.1f}%"
    p50 = [b["lat_p50_ns"] / 1e6 for b in bs]
    mean = st.mean(b["lat_mean_ns"] / 1e6 for b in bs)
    print(f"{lab:12} {n:4} | {per('storage'):>18} | {per('mysqld'):>18} | "
          f"{st.mean(p50):6.2f} ±{(max(p50)-min(p50))/st.mean(p50)*100:4.1f}% | {mean:8.2f}")
