#!/usr/bin/env python3
"""Aggregate Helios per-tx RPC trace JSONL (ENABLE_RPC_TRACE) into a
per-section time breakdown.

Usage:
  rpc_trace_agg.py TRACE.jsonl [TRACE2.jsonl ...]

For each file prints:
  - tx counts (committed/aborted), duration percentiles
  - RPC time by message type (n, total, mean, bytes)
  - non-RPC sections (autogen compile, staging, cache lookups) and counters
  - local-view event counts (cache hits/misses/abort reasons)
  - residual = duration - rpc - sections (MySQL executor + proxy CPU)
  - top statements by attributed wall time (gap to next statement start)
"""

import json
import re
import sys
from collections import defaultdict


def normalize_sql(sql):
    s = re.sub(r"'[^']*'", "?", sql)
    s = re.sub(r"\b\d+\b", "?", s)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > 90:
        s = s[:90] + "..."
    return s


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0
    idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * p))
    return sorted_vals[idx]


def fmt_us(us):
    if us >= 1_000_000:
        return f"{us / 1_000_000:.2f}s"
    if us >= 1_000:
        return f"{us / 1_000:.1f}ms"
    return f"{us}us"


# Sections that overlap RPC time already counted in summary_by_type; they are
# excluded from the non-RPC sections sum (decode-only is derived instead).
OVERLAPPING_SECTIONS = {"stage_rpc_decode", "stage_rpc_and_decode"}


def aggregate(path):
    n_tx = 0
    n_committed = 0
    durations = []
    rpc_by_type = defaultdict(lambda: [0, 0, 0, 0])  # n, us, req_b, resp_b
    sections = defaultdict(lambda: [0, 0])  # n, us
    local_view = defaultdict(int)
    stmt_time = defaultdict(lambda: [0, 0])  # n, attributed us
    sum_rpc_us = 0
    sum_section_us = 0
    sum_duration_us = 0
    rpc_per_tx = []

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                tx = json.loads(line)
            except json.JSONDecodeError:
                continue
            n_tx += 1
            if tx.get("status") == "committed":
                n_committed += 1
            dur = tx.get("duration_us", 0)
            durations.append(dur)
            sum_duration_us += dur
            rpc_per_tx.append(tx.get("rpc_count", 0))

            for t, agg in tx.get("summary_by_type", {}).items():
                a = rpc_by_type[t]
                a[0] += agg.get("n", 0)
                a[1] += agg.get("us", 0)
                a[2] += agg.get("req_b", 0)
                a[3] += agg.get("resp_b", 0)
                sum_rpc_us += agg.get("us", 0)

            for k, agg in tx.get("sections", {}).items():
                s = sections[k]
                s[0] += agg.get("n", 0)
                s[1] += agg.get("us", 0)
                if k not in OVERLAPPING_SECTIONS:
                    sum_section_us += agg.get("us", 0)

            for k, n in tx.get("summary_local_view", {}).items():
                # collapse parameterized kinds (counts carry table names)
                base = k.split(":", 1)[0] if k.startswith(
                    ("plan_request", "plan_fetch", "plan_scan", "use_",
                     "abort_prefetch_cache_miss", "abort_validate",
                     "use_si_scan")) else k
                local_view[base] += n

            stmts = tx.get("statements", [])
            for i, s in enumerate(stmts):
                start = s.get("started_off_us", 0)
                end = (stmts[i + 1].get("started_off_us", dur)
                       if i + 1 < len(stmts) else dur)
                key = normalize_sql(s.get("sql", ""))
                st = stmt_time[key]
                st[0] += 1
                st[1] += max(0, end - start)

    if n_tx == 0:
        print(f"== {path}: no transactions ==")
        return

    durations.sort()
    print(f"== {path} ==")
    print(f"tx: {n_tx} (committed {n_committed}, aborted {n_tx - n_committed})")
    print(f"duration: mean {fmt_us(sum_duration_us // n_tx)}  "
          f"p50 {fmt_us(pct(durations, 0.50))}  "
          f"p95 {fmt_us(pct(durations, 0.95))}  "
          f"p99 {fmt_us(pct(durations, 0.99))}")
    print(f"rpc/tx: mean {sum(rpc_per_tx) / n_tx:.1f}")
    resid = sum_duration_us - sum_rpc_us - sum_section_us
    print(f"time split per tx: rpc {fmt_us(sum_rpc_us // n_tx)}  "
          f"sections {fmt_us(sum_section_us // n_tx)}  "
          f"residual(mysql+proxy cpu) {fmt_us(max(0, resid) // n_tx)}")

    print("\n-- RPC by type (sorted by total time) --")
    print(f"{'type':46} {'n':>9} {'n/tx':>7} {'total':>10} {'mean':>9} "
          f"{'req_b/tx':>10} {'resp_b/tx':>10}")
    for t, (n, us, req_b, resp_b) in sorted(rpc_by_type.items(),
                                            key=lambda kv: -kv[1][1]):
        print(f"{t:46} {n:>9} {n / n_tx:>7.2f} {fmt_us(us):>10} "
              f"{fmt_us(us // max(1, n)):>9} {req_b // n_tx:>10} "
              f"{resp_b // n_tx:>10}")

    if sections:
        print("\n-- sections (non-RPC; counters have us=0; *=overlaps RPC) --")
        print(f"{'kind':30} {'n':>11} {'n/tx':>9} {'total':>10} {'mean':>9}")
        for k, (n, us) in sorted(sections.items(), key=lambda kv: -kv[1][1]):
            mark = "*" if k in OVERLAPPING_SECTIONS else ""
            print(f"{k + mark:30} {n:>11} {n / n_tx:>9.2f} {fmt_us(us):>10} "
                  f"{fmt_us(us // max(1, n)):>9}")
        for k in OVERLAPPING_SECTIONS & sections.keys():
            plan_rpc_us = rpc_by_type.get("TX_EXECUTE_READ_PLAN",
                                          [0, 0, 0, 0])[1]
            decode_only = max(0, sections[k][1] - plan_rpc_us)
            print(f"{'derived: decode_only':30} {'':>11} {'':>9} "
                  f"{fmt_us(decode_only):>10} (={k} - TX_EXECUTE_READ_PLAN)")

    print("\n-- local view events --")
    for k, n in sorted(local_view.items(), key=lambda kv: -kv[1])[:25]:
        print(f"{k:46} {n:>11} {n / n_tx:>9.2f}/tx")

    print("\n-- top statements by attributed wall time --")
    for sql, (n, us) in sorted(stmt_time.items(), key=lambda kv: -kv[1][1])[:15]:
        print(f"{fmt_us(us):>10} total {fmt_us(us // max(1, n)):>9}/exec "
              f"n={n:<8} {sql}")
    print()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    for path in sys.argv[1:]:
        aggregate(path)


if __name__ == "__main__":
    main()
