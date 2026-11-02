#!/usr/bin/env python3
"""Summarize callgrind dumps (one per FixedTpcc block or TPC-H query).

For each dump: total Ir, Ir per transaction (with --per N or a JSON from the
driver), the self cost of the top functions, and the inclusive cost of the
functions that match each category regex.

Usage:
  cg_summary.py <dump> [<dump> ...] [--per <n>] [--top 25] [--cats storage|mysqld]
  cg_summary.py --json out.json <dump> ...   machine-readable
"""
import argparse
import collections
import json
import re
import sys

CATS = {
    "storage": [
        ("read plan", r"HeliosRpc::handleTxExecuteReadPlan\("),
        ("  GatherRow (PAX row build)", r"PaxGroup::GatherRow\("),
        ("  FormatTyped", r"FormatTyped\("),
        ("  point Read", r"helios::storage::Database::Read\("),
        ("point read RPC", r"HeliosRpc::handleTxRead\("),
        ("commit RPC", r"HeliosRpc::handleTxCommit\("),
        ("  ValidateReads", r"Transaction::ValidateReads\("),
        ("  WAL/logger", r"wal::|Logger::"),
        ("request parse", r"Protocol::Request::_InternalParse\(|MessageLite::ParseFromString"),
        ("request destroy", r"Protocol::Request::~Request\(\)$|Protocol::Request::clear_body"),
        ("recv/send", r"MessageHandler::receive_message|MessageHandler::send_response"),
        ("epoch", r"epoch::|Epoch"),
        ("malloc/free", r"^(operator new|operator delete|malloc|free)\b"),
    ],
    "mysqld": [
        ("parse", r"^parse_sql\("),
        ("prepare", r"Sql_cmd_dml::prepare\("),
        ("optimize", r"^JOIN::optimize\("),
        ("execute", r"Sql_cmd_dml::execute_inner\("),
        ("commit", r"^ha_commit_trans\(|^trans_commit\("),
        ("plugin: send_request", r"HeliosProxy::send_request\("),
        ("plugin: prefetch (build + run)", r"compile_and_execute_prefetch|maybe_prefetch_for_single_table_dml_handler"),
        ("plugin: execute read plan", r"execute_read_plan"),
        ("plugin: rpc_trace", r"TxRpcTrace::|RpcTraceLogger::"),
        ("protobuf", r"google::protobuf::"),
        ("net read/write", r"^(my_net_read|net_write_command|my_net_write|net_flush)"),
        ("dispatch_command", r"^dispatch_command\("),
        ("malloc/free", r"^(operator new|operator delete|malloc|free|my_malloc|my_free)\b"),
    ],
}


def parse(path):
    """Return (total, self_cost{fn}, incl_cost{fn}) from a callgrind file."""
    names = {}
    self_c = collections.Counter()
    incl_c = collections.Counter()
    fn = None
    cfn = None
    pending_call = False
    total = 0
    # Inclusive cost of a function = self + inclusive of its calls, but a
    # recursive cycle would double count; callgrind writes the call costs per
    # caller, so summing self and outgoing calls is right outside cycles.
    def name(tok):
        m = re.match(r"\((\d+)\)(?: (.*))?", tok)
        if not m:
            return tok
        i, n = m.group(1), m.group(2)
        if n is not None:
            names[i] = n
        return names.get(i, i)

    with open(path, errors="replace") as f:
        for line in f:
            if line.startswith("fn="):
                fn = name(line[3:].strip())
                continue
            if line.startswith("cfn="):
                cfn = name(line[4:].strip())
                continue
            if line.startswith(("cfi=", "cfl=", "fl=", "fi=", "fe=", "ob=", "cob=")):
                if line[:3] in ("fl=", "fi=", "fe="):
                    name(line[3:].strip())
                continue
            if line.startswith("calls="):
                pending_call = True
                continue
            if line.startswith("summary:"):
                total = int(line.split()[1])
                continue
            if line and (line[0].isdigit() or line[0] in "+-*"):
                parts = line.split()
                if len(parts) < 2:
                    continue
                try:
                    c = int(parts[1])
                except ValueError:
                    continue
                if pending_call:
                    if cfn != fn:
                        incl_c[(fn, cfn)] += c
                    pending_call = False
                else:
                    self_c[fn] += c
    incl = collections.Counter(self_c)
    for (caller, callee), c in incl_c.items():
        incl[caller] += c
    return total, self_c, incl


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dumps", nargs="+")
    ap.add_argument("--per", type=float, default=1)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--cats", choices=list(CATS), default="storage")
    ap.add_argument("--json")
    a = ap.parse_args()
    res = []
    for d in a.dumps:
        total, self_c, incl = parse(d)
        label = d
        with open(d, errors="replace") as f:
            for line in f:
                if line.startswith("desc: Trigger:"):
                    label = line.split("dump", 1)[-1].strip()
                    break
        cats = {}
        for cname, rx in CATS[a.cats]:
            r = re.compile(rx)
            # The outermost matching function's inclusive cost: take the max,
            # since nested matches (a recursive name) would double count.
            cats[cname] = max([v for k, v in incl.items() if k and r.search(k)] or [0])
        res.append({"dump": d, "label": label, "total": total,
                    "cats": cats, "top_self": self_c.most_common(a.top)})
        if not a.json:
            print(f"== {label}  total {total:,} Ir  ({total / a.per:,.0f} per tx)")
            for cname, v in cats.items():
                print(f"   {cname:32} {v / a.per:12,.0f}  {v / total * 100 if total else 0:5.1f}%")
            print("   top self:")
            for k, v in self_c.most_common(a.top):
                print(f"   {v / a.per:12,.0f}  {v / total * 100:5.1f}%  {k[:110]}")
    if a.json:
        json.dump(res, open(a.json, "w"), indent=1)


if __name__ == "__main__":
    main()
