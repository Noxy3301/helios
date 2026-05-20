# Helios

## Prerequisites

```bash
# Build tools and libraries
sudo apt-get update && sudo apt-get install -y \
    build-essential cmake ninja-build \
    protobuf-compiler libprotobuf-dev \
    libssl-dev pkg-config libncurses-dev \
    libnuma-dev libtirpc-dev \
    bison flex \
    libjemalloc2 \
    unzip maven

# Java 23 (required by BenchBase)
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 23-open
```

## Quick Start

```bash
git clone --recursive https://github.com/Noxy3301/helios.git
cd helios
./scripts/build.sh
```

### Start Servers

```bash
# Terminal 1: Start Helios Server
./scripts/start_server.sh

# Terminal 2: Start MySQL (auto-initializes data directory and installs LineairDB plugin)
./scripts/start_mysql.sh --mysqld-port 3307 --server-host 127.0.0.1 --server-port 9999
```

### Simple Example

```bash
./build/runtime_output_directory/mysql -u root --protocol=TCP -h 127.0.0.1 -P 3307
```

```sql
DROP DATABASE IF EXISTS lineairdb_test;
CREATE DATABASE lineairdb_test;
USE lineairdb_test;

CREATE TABLE test (
    id INT PRIMARY KEY,
    name VARCHAR(20)
) ENGINE=LINEAIRDB;

INSERT INTO test VALUES (1, 'hello');
INSERT INTO test VALUES (2, 'world');

SELECT * FROM test;
SELECT * FROM test WHERE id = 1;
```

## Benchmark

```bash
# Patch BenchBase (first time only)
python3 bench/bin/patch_benchbase.py

# TPC-C
python3 bench/bin/benchrun.py tpcc --terminals 64

# YCSB
python3 bench/bin/benchrun.py ycsb --profile a --terminals 8 --scalefactor 100

# TPC-H
python3 bench/bin/benchrun.py tpch --terminals 1 --scalefactor 0.1 --time 300
```
