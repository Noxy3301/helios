#!/usr/bin/env python3
"""Compare tpch_wall.sh result TSVs: min-of-N per query, totals, ratios, md5 diff.

Usage: tpch_wall_report.py A.tsv B.tsv [C.tsv ...]
The first file is the baseline for ratio columns.
"""
import sys
from pathlib import Path

CHAMPION_INNODB_S = {  # .note/session/2026-06-27_innodb_champion_sf1.md (SF=1, fixed target)
    1: 8.48, 2: 0.64, 3: 3.50, 4: 0.55, 5: 1.60, 6: 1.03, 7: 0.16, 8: 1.50,
    9: 8.38, 10: 1.44, 11: 0.55, 12: 1.64, 13: 4.95, 15: 2.50, 16: 0.33,
    17: 0.40, 18: 1.60, 19: 0.20, 20: 0.54, 21: 1.85, 22: 0.37,  # q14 artifact
}


def load(path):
    out = {}
    for line in Path(path).read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split("\t")
        q = parts[0]
        if len(parts) < 2 or parts[1] in ("ERR", "MISSING"):
            out[q] = {"err": parts[2] if len(parts) > 2 else "?"}
            continue
        times = [int(x) for x in parts[1].split(",")]
        md5 = parts[2].removeprefix("md5=") if len(parts) > 2 else ""
        out[q] = {"min": min(times), "times": times, "md5": md5}
    return out


def main():
    files = sys.argv[1:]
    data = {Path(f).stem: load(f) for f in files}
    labels = list(data)
    base = labels[0]
    hdr = "q     " + "".join(f"{l:>10}" for l in labels) + "".join(
        f"  {l}/{base}" for l in labels[1:]) + "   vs INN(fixed)"
    print(hdr)
    totals = {l: 0 for l in labels}
    n_common = 0
    md5_mismatch = []
    for qn in range(1, 23):
        q = f"q{qn}"
        row = f"{q:<6}"
        vals = {}
        for l in labels:
            e = data[l].get(q, {})
            if "min" in e:
                vals[l] = e["min"]
                row += f"{e['min']:>9}m"
            else:
                row += f"{'ERR':>10}"
        if len(vals) == len(labels):
            n_common += 1
            for l in labels:
                totals[l] += vals[l]
        for l in labels[1:]:
            if base in vals and l in vals and vals[base] > 0:
                row += f"  {vals[l]/vals[base]:7.2f}x"
            else:
                row += f"  {'-':>8}"
        inn = CHAMPION_INNODB_S.get(qn)
        if inn and base in vals:
            row += f"   {vals[base]/1000/inn:6.2f}x"
        print(row)
        m5s = {l: data[l][q]["md5"] for l in labels if q in data[l] and data[l][q].get("md5")}
        if len(set(m5s.values())) > 1:
            md5_mismatch.append((q, m5s))
    print("-" * len(hdr))
    trow = f"TOT({n_common}q)"
    for l in labels:
        trow += f"{totals[l]:>9}m"
    for l in labels[1:]:
        if totals[base]:
            trow += f"  {totals[l]/totals[base]:7.2f}x"
    print(trow)
    if md5_mismatch:
        print("\n!!! md5 mismatches:")
        for q, m in md5_mismatch:
            print(f"  {q}: {m}")
    else:
        print("\nmd5: all labels identical on common queries")


if __name__ == "__main__":
    main()
