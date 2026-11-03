#!/usr/bin/env python3
"""Turn a MySQL general log captured during a BenchBase run into a fixed
sample: a JSON list of transactions, each a list of SQL statements.

Usage: parse_glog.py <general.log> <out.json> [--max N]

A transaction ends at COMMIT or ROLLBACK on the same connection. Statements
issued before the first transaction boundary (connection setup) are kept in
"setup" so the replayer can issue them once.
"""
import json
import re
import sys

LINE = re.compile(r"^(\S+)\s+(\d+)\s+(Query|Connect|Quit|Init DB|Prepare|Execute|Close stmt)\t?(.*)$")


def parse(path):
    conns = {}
    cur = None
    with open(path, errors="replace") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            m = LINE.match(raw)
            if m:
                _, cid, kind, text = m.groups()
                cur = (cid, kind)
                conns.setdefault(cid, []).append([kind, text])
            elif cur is not None and conns.get(cur[0]):
                # continuation of a multi-line statement
                conns[cur[0]][-1][1] += "\n" + raw
    return conns


def split(stmts):
    setup, txns, cur = [], [], []
    seen_boundary = False
    for kind, text in stmts:
        if kind != "Query":
            continue
        t = text.strip()
        low = re.sub(r"^/\*.*?\*/", "", t, flags=re.S).strip().lower()
        if low in ("commit", "rollback"):
            if cur:
                txns.append({"stmts": cur, "end": low})
            cur = []
            seen_boundary = True
            continue
        # Connection setup: driver queries and SET statements before the first
        # transaction. A user variable (SET @...) belongs to the transaction.
        is_setup = (low.startswith(("set ", "select @@", "show ")) or "information_schema" in low) \
            and not low.startswith("set @")
        if not seen_boundary and is_setup and all(
                x.lower().startswith(("set ", "/*", "select @@")) for x in cur):
            setup.extend(cur); cur = []
            setup.append(t)
            continue
        cur.append(t)
    return setup, txns


def main():
    path, out = sys.argv[1], sys.argv[2]
    maxn = int(sys.argv[sys.argv.index("--max") + 1]) if "--max" in sys.argv else None
    conns = parse(path)
    best = None
    for cid, stmts in conns.items():
        setup, txns = split(stmts)
        if best is None or len(txns) > len(best[1]):
            best = (setup, txns)
    setup, txns = best
    if maxn:
        txns = txns[:maxn]
    json.dump({"setup": setup, "txns": txns}, open(out, "w"), indent=0)
    print(f"{out}: setup={len(setup)} txns={len(txns)} "
          f"stmts={sum(len(t['stmts']) for t in txns)} "
          f"rollbacks={sum(t['end'] == 'rollback' for t in txns)}")


if __name__ == "__main__":
    main()
