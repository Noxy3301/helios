# Benchmark

## Quick Start

### 1. Start servers

```bash
./scripts/start_server.sh
./scripts/start_mysql.sh --mysqld-port 3307 --server-host 127.0.0.1 --server-port 9999
```

### 2. Build BenchBase (first time only)

```bash
python3 bench/bin/build_benchbase.py
```

### 3. Run benchmark

```bash
# TPC-C
python3 bench/bin/benchrun.py tpcc --terminals 64

# YCSB
python3 bench/bin/benchrun.py ycsb --profile a --terminals 8 --scalefactor 100

# TPC-H
python3 bench/bin/benchrun.py tpch --terminals 1 --scalefactor 0.1 --time 300
```
