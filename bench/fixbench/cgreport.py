#!/usr/bin/env python3
"""Summarize callgrind dumps written by run.sh or tpch.sh.

  cgreport.py <dir> <ntxn>                       instructions per transaction for the busiest
                                                 mysqld and storage thread, and all threads
  cgreport.py <dir> <ntxn> --incl RE [RE ...]     inclusive instructions/txn of the largest
                                                 function matching each regex
  cgreport.py <dir> <ntxn> --calls RE [RE ...]    calls/txn into functions whose name matches

Only dumps made by callgrind_control -d are read (file names <prefix>.<pid>.<n>-<thread>).
--incl and --calls look at the busiest thread of each process.
"""
import collections
import glob
import os
import re
import subprocess
import sys


def totals(d, proc):
    out = {}
    for f in glob.glob(f"{d}/{proc}.*"):
        if not re.search(r"\.\d+\.\d+-\d+$", f):
            continue
        # "totals:" is the sum of the cost lines. "summary:" can wrap around in
        # the second and later dumps of a run that toggles instrumentation.
        for line in open(f, errors="replace"):
            if line.startswith("totals:"):
                out[f] = int(line.split()[1])
                break
    return out


def inclusive(f):
    text = subprocess.run(["callgrind_annotate", "--inclusive=yes", "--threshold=100", f],
                          capture_output=True, text=True).stdout
    total, rows = None, []
    for line in text.splitlines():
        m = re.match(r"\s*([\d,]+) \(\s*([\d.]+)%\)\s+(.*)$", line)
        if not m:
            continue
        v = int(m.group(1).replace(",", ""))
        if "PROGRAM TOTALS" not in m.group(3):
            rows.append((v, m.group(3)))
    for line in open(f, errors="replace"):
        if line.startswith("totals:"):
            total = int(line.split()[1])
            break
    return total, rows


def calls(f):
    names, counts, cfn = {}, collections.Counter(), None
    for line in open(f, errors="replace"):
        m = re.match(r"(c?fn)=\((\d+)\)(?: (.*))?", line)
        if m:
            kind, i, nm = m.groups()
            if nm:
                names[i] = nm
            if kind == "cfn":
                cfn = i
            continue
        if line.startswith("calls=") and cfn is not None:
            counts[cfn] += int(line.split("=")[1].split()[0])
            cfn = None
    return names, counts


def main():
    d, n = sys.argv[1], int(sys.argv[2])
    mode = sys.argv[3] if len(sys.argv) > 3 else None
    pats = sys.argv[4:]
    for proc in ("mysqld", "storage"):
        tot = totals(d, proc)
        if not tot:
            continue
        busiest = max(tot, key=tot.get)
        print(f"{proc}: busiest thread {tot[busiest] / n / 1e3:,.1f} K instr/txn, "
              f"all threads {sum(tot.values()) / n / 1e3:,.1f} K ({os.path.basename(busiest)})")
        if mode == "--incl":
            total, rows = inclusive(busiest)
            for p in pats:
                best = max(((v, nm) for v, nm in rows if re.search(p, nm)), default=None)
                if best:
                    print(f"  {p:50s} {best[0] / n / 1e3:10,.1f} K/txn {best[0] / total * 100:5.1f}%")
        elif mode == "--calls":
            names, counts = calls(busiest)
            for p in pats:
                c = sum(v for i, v in counts.items() if re.fullmatch(p, names.get(i, "")))
                print(f"  {p:50s} {c / n:10,.1f} calls/txn")


if __name__ == "__main__":
    main()
