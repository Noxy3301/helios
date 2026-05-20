#!/usr/bin/env python3
"""
Build BenchBase from third_party/benchbase and stage it under bench/.

Research patches live in the fork (Noxy3301/benchbase, research/helios) and
ship as part of the submodule, so this script only runs the Maven build and
extracts the resulting zip into bench/benchbase-mysql/.

Usage:
  python3 bench/bin/build_benchbase.py
"""

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BENCHBASE_SRC = ROOT / "third_party" / "benchbase"
EXTRACT_DIR = ROOT / "bench" / "benchbase-mysql"


def build_benchbase():
    print("Building BenchBase...")
    result = subprocess.run(
        ["./mvnw", "-DskipTests", "-P", "mysql", "clean", "package"],
        cwd=BENCHBASE_SRC,
    )
    if result.returncode != 0:
        print("ERROR: BenchBase build failed", file=sys.stderr)
        sys.exit(1)

    zip_path = BENCHBASE_SRC / "target" / "benchbase-mysql.zip"
    if EXTRACT_DIR.exists():
        shutil.rmtree(EXTRACT_DIR)
    EXTRACT_DIR.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        # Zip already contains a top-level "benchbase-mysql/" directory.
        zf.extractall(EXTRACT_DIR.parent)

    jar = EXTRACT_DIR / "benchbase.jar"
    if jar.exists():
        print(f"\nBuild successful: {jar}")
    else:
        print("ERROR: benchbase.jar not found after build", file=sys.stderr)
        sys.exit(1)


def main():
    if not BENCHBASE_SRC.exists():
        print(
            f"ERROR: {BENCHBASE_SRC} not found. Run: git submodule update --init",
            file=sys.stderr,
        )
        sys.exit(1)
    build_benchbase()


if __name__ == "__main__":
    main()
