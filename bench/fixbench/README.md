# Fixed-sample benchmarks

BenchBase runs many terminals for a fixed time, so two runs never execute the same
transactions and their throughput moves by a few percent from run to run. That is
too much noise to see a 1-2% change. This directory replays a fixed set of
statements on one connection and counts what the servers do for it, so two builds
can be compared on exactly the same work.

It follows the approach SQLite and rustc use to track performance:
- SQLite measures CPU cycles with cachegrind on its fixed workload `speedtest1`,
  because cachegrind repeats to 7 or more significant digits where wall time
  repeats to about one ([Measuring and Reducing CPU Usage in SQLite](https://www3.sqlite.org/cpu.html)).
- rustc's perf.rust-lang.org defaults to instruction counts for the same reason
  ([discussion](https://internals.rust-lang.org/t/what-is-perf-rust-lang-org-measuring-and-why-is-instructions-u-the-default/9815)).

## What is fixed

- **TPC-C:** `samples/tpcc_sf1_<type>.json.gz` holds 300 transactions of each of the five
  types. The SQL is BenchBase's own: `capture.sh` records it with the MySQL general log
  while BenchBase runs one type at one terminal against `bench/config/tpcc.xml`.
  Every run starts from an empty stack and loads SF 1 with BenchBase, which is
  deterministic apart from timestamps. Two replays return identical result sets.
  `*_txplan` samples were recorded with `HELIOS_PREFETCH_PLAN=1` (what
  `benchrun.py --tx-plan` sets), so each transaction starts with its `SET @_tx_plan`.
- **TPC-H:** the 22 queries of `bench/queries/tpch`, run the way
  `bench/config/tpch.xml` runs them (autocommit, SERIALIZABLE,
  `secondary_engine_cost_threshold=0`). Q11's fraction is scaled to the scale factor.

The replay never restarts the stack after the load, so it sees the same optimizer
statistics as a `benchrun.py` run. A storage restart loses the row counts and
changes plans (see the issue on row counts lost on restart).

## Usage

```bash
# Once: a storage build callgrind can run. Valgrind does not decode AVX-512,
# which the Release build's -march=native emits.
mkdir -p build-prof/server && cd build-prof/server && \
  cmake ../../server -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-mno-avx512f -g" && make -j && cd ../..
```

```bash
bench/fixbench/run.sh callgrind neworder 120     # 20 warm-up + 100 measured transactions
```

```bash
bench/fixbench/cgreport.py bench/results/fixbench/cg/tpcc_sf1_neworder 100
```

```bash
bench/fixbench/cgreport.py bench/results/fixbench/cg/tpcc_sf1_neworder 100 --incl ParseFromString FormatTyped
```

```bash
bench/fixbench/run.sh native payment              # wall, CPU, user and system time per txn
```

```bash
bench/fixbench/tpch.sh callgrind 0.1              # instructions per TPC-H query
```

BenchBase needs JDK 23 or later. Output goes to `bench/results/fixbench` (git-ignored).

## Which number to trust for what

Measured on a 4-vCPU cloud VM at `f5b4536`:

| Metric | Run-to-run spread |
|---|---|
| Instructions per transaction of the busiest thread (callgrind), NewOrder, two separate loads | mysqld 6,366 K vs 6,367 K, storage 833 K vs 833 K: below 0.02% |
| On-CPU time of the same replay, five fresh loads | CV 2-15% depending on the type |
| BenchBase TPC-C throughput, 4 terminals × 30 s, five fresh loads | 430-460 req/s, CV 2.7% |
| BenchBase TPC-C throughput, 1 terminal × 30 s, three fresh loads | 160-171 req/s, CV 3.5% |

Telling a change of δ from a spread with coefficient of variation CV needs about
16·(CV/δ)² runs on each side (two-sample test, α = 0.05, power 0.8). With CV 2.7%,
that is 5 runs for a 5% change, 29 for 2%, and about 115 for 1%. So use instruction
counts to accept or reject small CPU changes, and BenchBase only for effects the
replay cannot show.

Instruction counts are user space only and single-threaded. They do not show:
- **Kernel time.** At one terminal the storage server spends 70% of its NewOrder
  CPU time in the kernel (1,214 of 1,746 µs; each Sync commit triggers its own
  epoch advance and fdatasync, and each RPC costs system calls). A change that
  saves instructions but adds RPCs or system calls can lose. `cgreport.py --calls
  'HeliosRpc::handle_rpc.*'` counts RPCs per transaction, which is deterministic.
- **Contention.** Aborts, retries and lock waits appear only with many terminals.
- **Memory effects.** Cache misses and IPC do not show in instruction counts
  (rustc's discussion above names the same limit).
- **The allocator.** Under callgrind both servers use glibc malloc. Production
  preloads jemalloc, which makes each call cheaper, but the number of calls stays
  the same (`--calls malloc`).

A change is safe to take when the instructions and RPCs of every type do not
rise, and BenchBase at the target terminal count, with enough runs for the effect
size you expect, does not fall.

## Files

| File | Does |
|---|---|
| `stack.sh` | wipe, start (native or under callgrind) and stop the stack |
| `load.py` | load TPC-C or TPC-H with benchrun.py's setup, without a restart |
| `capture.sh`, `parse_glog.py` | record a new sample from BenchBase |
| `replay.py` | replay a sample on one connection and measure |
| `run.sh` | one TPC-C measurement, native or callgrind |
| `tpch_run.py`, `tpch.sh` | the 22 TPC-H queries, native or callgrind |
| `cgreport.py` | instructions, inclusive costs and call counts from the dumps |
