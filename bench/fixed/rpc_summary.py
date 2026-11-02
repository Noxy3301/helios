#!/usr/bin/env python3
"""Per transaction type: statements, RPCs by type, request and response bytes,
from a helios_rpc_trace JSONL of a FixedTpcc run.

Usage: rpc_summary.py <trace.jsonl> [--last N]
The type is told from the statements' SQL; the digest queries of
fixed_tpcc.sh come out as "other".
"""
import collections
import json
import sys


def kind(d):
    s = " ".join(x["sql"] for x in d["statements"])
    u = s.upper()
    if "STOCK_COUNT" in s:
        return "StockLevel"
    if "NO_O_ID" in u and "DELETE" in u:
        return "Delivery"
    if "H_AMOUNT" in u or "W_YTD" in u:
        return "Payment"
    if "S_QUANTITY" in u and "INSERT" in u:
        return "NewOrder"
    if "O_ENTRY_D" in u or "OL_DELIVERY_D" in u:
        return "OrderStatus"
    return "other"


L = [json.loads(l) for l in open(sys.argv[1])]
if "--last" in sys.argv:
    L = L[-int(sys.argv[sys.argv.index("--last") + 1]):]
agg = collections.defaultdict(collections.Counter)
n = collections.Counter()
for d in L:
    k = kind(d)
    n[k] += 1
    agg[k]["stmts"] += d["stmt_count"]
    agg[k]["rpcs"] += d["rpc_count"]
    for t, v in d["summary_by_type"].items():
        agg[k]["n:" + t] += v["n"]
        agg[k]["req:" + t] += v["req_b"]
        agg[k]["resp:" + t] += v["resp_b"]
for k in ["NewOrder", "Payment", "OrderStatus", "Delivery", "StockLevel", "other"]:
    if not n[k]:
        continue
    print(f"== {k} x{n[k]}: stmts/tx {agg[k]['stmts'] / n[k]:.1f}, rpcs/tx {agg[k]['rpcs'] / n[k]:.1f}")
    for t in sorted({x.split(":", 1)[1] for x in agg[k] if ":" in x}):
        print(f"   {t:24} n {agg[k]['n:' + t] / n[k]:6.2f}  req {agg[k]['req:' + t] / n[k]:9.0f} B"
              f"  resp {agg[k]['resp:' + t] / n[k]:9.0f} B")
