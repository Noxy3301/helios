#!/usr/bin/env python3
"""Load a BenchBase workload into the running stack the way bench/bin/benchrun.py
does (DROP/CREATE benchbase, load with unique_checks=0, attach the DuckDB secondary
engine for TPC-H). The stack keeps running, so the optimizer statistics the load
built stay in place, as in a benchrun.py run.

Usage: load.py tpcc|tpch [scalefactor]
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bench" / "bin"))
import benchrun  # noqa: E402


def main():
    bench = sys.argv[1]
    text = (ROOT / "bench" / "config" / f"{bench}.xml").read_text()
    if len(sys.argv) > 2:
        import re
        text = re.sub(r"<scalefactor>[^<]*</scalefactor>", f"<scalefactor>{sys.argv[2]}</scalefactor>", text)
    work = ROOT / "bench" / "results" / "fixbench"
    work.mkdir(parents=True, exist_ok=True)
    cfg = work / f"load_{bench}.xml"
    cfg.write_text(text)
    t = benchrun.setup_benchmark(bench, cfg, "127.0.0.1", 3307)
    if t is None:
        sys.exit(1)
    if bench == "tpch":
        benchrun.attach_secondary(bench, "127.0.0.1", 3307)
    print("loaded", t)


if __name__ == "__main__":
    main()
