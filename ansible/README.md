# Ansible for Helios distributed benchmarks

## Prerequisites

### AMI pre-bake (Ubuntu 24.04)

These are installed once when creating the AMI. Run all commands as `ubuntu` user.

> **Disk**: Root EBS volume must be **50GB+** (mysql-server submodule ~3GB, build artifacts ~10GB).

#### 1. System packages

```bash
sudo apt-get update && sudo apt-get install -y \
    build-essential cmake ninja-build \
    protobuf-compiler libprotobuf-dev \
    libssl-dev pkg-config libncurses-dev \
    libnuma-dev libtirpc-dev \
    bison flex \
    libjemalloc2 \
    unzip maven \
    sysstat haproxy \
    python3 python3-pip \
    wget curl git zip
```

#### 2. Java 23 (BenchBase runtime)

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 23-open
```

Add to `~/.bashrc`:

```bash
echo 'source "$HOME/.sdkman/bin/sdkman-init.sh"' >> ~/.bashrc
```

#### 3. Clone and build Helios

```bash
cd ~
git clone --recursive https://github.com/Noxy3301/helios.git
cd helios
./scripts/build.sh
```

#### 4. Patch and build BenchBase

```bash
cd ~/helios
python3 bench/bin/patch_benchbase.py
```

#### 5. Verify build

```bash
# Server binary exists
ls ~/helios/build/server/lineairdb-server

# MySQL binary exists
ls ~/helios/build/runtime_output_directory/mysqld

# BenchBase JAR exists
ls ~/helios/third_party/benchbase/benchbase-mysql/benchbase.jar

# Java version
java --version
```

#### 6. Snapshot AMI

Stop the instance and create an AMI snapshot. All nodes (lineairdb, mysql, haproxy, bench) use the same AMI.

### Ansible deploy-time (IP-dependent, runs each deployment)


| Playbook        | What it does                                                   |
| --------------- | -------------------------------------------------------------- |
| `lineairdb.yml` | Rebuild + start Helios server                                  |
| `mysql.yml`     | Start MySQL, create users (HAProxy health check, bench access) |
| `haproxy.yml`   | Deploy HAProxy config with backend IPs, restart                |
| `benchbase.yml` | Create schema on each MySQL, load data                         |


## Quick start (bench_aws.py)

`bench_aws.py` automates the entire lifecycle: launch spot instances, deploy, run benchmarks, collect results, and clean up.

```bash
cd ~/helios/ansible

# TPC-C (default SF=1, terminal sweep)
python3 py/bench_aws.py --bench-type tpcc --bench-terms 1,8,32,64,128,256 --bench-time 60

# YCSB
python3 py/bench_aws.py --bench-type ycsb --bench-terms 1,8,32,64

# TPC-H serial (22 queries once each, SF=0.1)
python3 py/bench_aws.py --bench-type tpch --bench-scalefactor 0.1

# TPC-H parallel scalability
python3 py/bench_aws.py --bench-type tpch --bench-serial false --bench-scalefactor 0.1 --bench-terms 1,2,4,8,16,32

# Dry run (show what would be launched)
python3 py/bench_aws.py --dry-run

# Skip cleanup (keep instances running for debugging)
python3 py/bench_aws.py --bench-type tpcc --skip-cleanup
```

Logs are saved to `logs/<timestamp>/`. Results and plots go to `result/<run_id>/<machine_spec>/`.

Cluster configuration is defined in `CLUSTER` dict at the top of `bench_aws.py`. Default:

| Role      | Instance type  | Count |
|-----------|---------------|-------|
| lineairdb | c6i.16xlarge  | 1     |
| mysql     | c6i.4xlarge   | 8     |
| haproxy   | c6i.4xlarge   | 1     |
| benchbase | c6i.4xlarge   | 1     |

## Manual operation

For debugging or running individual steps, you can use Ansible playbooks directly against pre-existing instances.

### 1. Generate inventory from AWS tags

```bash
python3 py/update_inventory.py
```

> Tag instances: `Name=helios-lineairdb`, `Name=helios-mysql`, `Name=helios-haproxy`, `Name=helios-bench`, `Project=Helios`

### 2. Deploy infrastructure

```bash
ansible -i inventory.ini all -m ping           # Connectivity check
ansible-playbook -i inventory.ini site.yml      # Full deploy (lineairdb → mysql → haproxy → benchbase)
```

### 3. Run benchmarks

#### YCSB

```bash
# Setup (schema + data, SF=10 by default)
ansible-playbook -i inventory.ini benchbase.yml \
  -e "bench_type=ycsb"

# Execute with monitoring
ansible-playbook -i inventory.ini measure_usage.yml \
  -e "bench_type=ycsb bench_profile=b run_id=$(date +%Y%m%d-%H%M%S)"
```

#### TPC-C

```bash
# Setup (SF=10 by default)
ansible-playbook -i inventory.ini benchbase.yml \
  -e "bench_type=tpcc"

# Execute
ansible-playbook -i inventory.ini measure_usage.yml \
  -e "bench_type=tpcc bench_time=60 run_id=$(date +%Y%m%d-%H%M%S)"

# Custom terminal sweep
ansible-playbook -i inventory.ini measure_usage.yml \
  -e "bench_type=tpcc bench_terms=[1,16,64,128] run_id=$(date +%Y%m%d-%H%M%S)"
```

#### TPC-H serial (22 queries once each)

```bash
# Setup (SF=0.1 by default)
ansible-playbook -i inventory.ini benchbase.yml \
  -e "bench_type=tpch"

# Execute (bench_serial defaults to true for tpch, bench_terms defaults to [1])
ansible-playbook -i inventory.ini measure_usage.yml \
  -e "bench_type=tpch run_id=$(date +%Y%m%d-%H%M%S)"
```

#### TPC-H parallel (scalability test)

```bash
# Setup (small SF for fast load)
ansible-playbook -i inventory.ini benchbase.yml \
  -e "bench_type=tpch bench_scalefactor=0.01"

# Execute with terminal sweep
ansible-playbook -i inventory.ini measure_usage.yml \
  -e "bench_type=tpch bench_serial=false bench_scalefactor=0.01 bench_time=60 bench_terms=[1,2,4,8,16,32] run_id=$(date +%Y%m%d-%H%M%S)"
```

## Variables reference


| Variable            | Default                                         | Description                                 |
| ------------------- | ----------------------------------------------- | ------------------------------------------- |
| `bench_type`        | `ycsb`                                          | Benchmark: `ycsb`, `tpcc`, `tpch`           |
| `bench_scalefactor` | `10` (ycsb/tpcc) / `0.1` (tpch)                 | Scale factor                                |
| `bench_profile`     | `b`                                             | YCSB profile: `a`, `b`, `c`, `e`, `f`       |
| `bench_serial`      | `true` for tpch                                 | TPC-H: serial (22 queries once) vs parallel |
| `bench_time`        | `60`                                            | Execution time in seconds                   |
| `bench_terms`       | `[1]` (tpch serial) / `[1,32,...,256]` (others) | Terminal counts to sweep                    |
| `bench_sync`        | `true`                                          | Synchronize start across bench nodes        |
| `bench_sync_buffer` | `5`                                             | Sync buffer (seconds)                       |
| `run_id`            | `run`                                           | Identifier for log filenames                |
| `sample_interval`   | `1`                                             | CPU sampling interval (seconds)             |


## Results

Results are stored under `result/<run_id>/<machine_spec>/`:

```
result/
  20260310-143000/
    lineairdb-128x1_mysql-128x2_haproxy-48x1_benchbase-128x1/
      throughput/
        throughput_raw.csv       # host,terminals,throughput,goodput
      cpu/
        <host>/cpu-<run_id>.log
      bench/
        <host>/bench-<run_id>.tgz
      lineairdb/
        <host>/{pidstat,sar-*,mpstat-irq,softnet,interrupts}-<run_id>.log
      haproxy/
        <host>/haproxy-<run_id>.csv
      mysql/
        <host>/mysql-status-<run_id>.txt
```

### Plot results

```bash
python3 py/plot_throughput.py    # Throughput plot
python3 py/plot_cpu.py           # CPU usage plot
python3 py/plot_tpch.py          # TPC-H per-query latency (auto-called for tpch benchmarks)
```

## Playbook reference


| Playbook            | Purpose                                                   |
| ------------------- | --------------------------------------------------------- |
| `site.yml`          | Master: lineairdb → mysql → haproxy → benchbase           |
| `lineairdb.yml`     | Start Helios server (rebuild + ulimit)                    |
| `mysql.yml`         | Start MySQL with LineairDB proxy, create users            |
| `haproxy.yml`       | Deploy HAProxy config (L4 MySQL load balancing)           |
| `benchbase.yml`     | Create schema + load data (supports ycsb/tpcc/tpch)       |
| `measure.yml`       | Run benchmark terminal sweep                              |
| `measure_term.yml`  | Single terminal count execution (included by measure.yml) |
| `measure_usage.yml` | measure.yml wrapped with CPU/network monitoring           |


> **Caution**: HAProxy stats auth is `admin:password` in [templates/haproxy.cfg.j2](templates/haproxy.cfg.j2). Change before use.

## Important notes

- **DDL doesn't sync**: `benchbase.yml` runs `--create` on each MySQL node separately because Helios DDL is not replicated across MySQL instances.
- **Data is shared**: `--load` runs once; data goes to shared LineairDB storage.
- **TPC-H optimizer settings**: `benchbase.yml` and `measure.yml` both set hash join / subquery optimizer flags on each MySQL node. This is needed because Helios's RPC-based architecture makes hash join vastly faster than nested-loop with PK lookups.
- **Server restart clears data**: LineairDB is in-memory. Restarting the Helios server loses all data. Re-run `benchbase.yml` after restart.

