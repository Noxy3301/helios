#!/usr/bin/env python3
"""Run TPC-H Q1-Q22 individually with per-query timeout."""

import subprocess
import sys
import time
import shutil
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCHBASE_DIR = ROOT / "bench" / "benchbase-mysql"
CONFIG_SRC = ROOT / "bench" / "config" / "tpch.xml"
TIMEOUT = 120  # seconds per query

def make_query_config(query_num, out_dir):
    """Create a config with only one query enabled."""
    tree = ET.parse(CONFIG_SRC)
    root = tree.getroot()

    # Remove all existing <works> and replace with single query
    works = root.find("works")
    for work in list(works):
        works.remove(work)

    work = ET.SubElement(works, "work")
    ET.SubElement(work, "serial").text = "true"
    ET.SubElement(work, "rate").text = "unlimited"
    weights = ["0"] * 22
    weights[query_num - 1] = "1"
    ET.SubElement(work, "weights").text = ",".join(weights)

    config_path = out_dir / f"tpch_q{query_num}.xml"
    tree.write(config_path, xml_declaration=True, encoding="unicode")
    return config_path


def run_query(query_num, out_dir, result_dir):
    """Run a single TPC-H query and return (elapsed_seconds, status)."""
    config = make_query_config(query_num, out_dir)
    cmd = [
        "java", "-jar", str(BENCHBASE_DIR / "benchbase.jar"),
        "-b", "tpch",
        "-c", str(config),
        "--create=false", "--load=false", "--execute=true",
        "-d", str(result_dir / f"q{query_num}"),
    ]

    start = time.time()
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=TIMEOUT, cwd=str(BENCHBASE_DIR))
        elapsed = time.time() - start
        if proc.returncode == 0:
            return elapsed, "OK"
        else:
            # Check for specific errors
            err = proc.stdout + proc.stderr
            if "Got error" in err or "SQLException" in err:
                return elapsed, "ERROR"
            return elapsed, "FAIL"
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        return elapsed, "TIMEOUT"


def main():
    result_dir = ROOT / "bench" / "results" / f"tpch_individual_{time.strftime('%Y%m%d_%H%M%S')}"
    result_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = result_dir / "configs"
    tmp_dir.mkdir(exist_ok=True)

    print(f"TPC-H Individual Query Benchmark (SF=0.1, timeout={TIMEOUT}s)")
    print(f"Results: {result_dir}")
    print(f"{'Query':>7} {'Time (s)':>10} {'Status':>10}")
    print("-" * 30)

    results = []
    for q in range(1, 23):
        sys.stdout.write(f"  Q{q:02d}   ")
        sys.stdout.flush()
        elapsed, status = run_query(q, tmp_dir, result_dir)
        print(f"{elapsed:10.2f} {status:>10}")
        results.append((q, elapsed, status))

    print("-" * 30)

    # Summary
    ok = [(q, t) for q, t, s in results if s == "OK"]
    timeout = [(q, t) for q, t, s in results if s == "TIMEOUT"]
    error = [(q, t) for q, t, s in results if s in ("ERROR", "FAIL")]

    print(f"\nCompleted: {len(ok)}/22")
    if timeout:
        print(f"Timeout:   {', '.join(f'Q{q}' for q, _ in timeout)}")
    if error:
        print(f"Error:     {', '.join(f'Q{q}' for q, _ in error)}")

    # Write CSV
    csv_path = result_dir / "summary.csv"
    with open(csv_path, "w") as f:
        f.write("query,time_sec,status\n")
        for q, t, s in results:
            f.write(f"Q{q},{t:.2f},{s}\n")
    print(f"\nCSV: {csv_path}")


if __name__ == "__main__":
    main()
