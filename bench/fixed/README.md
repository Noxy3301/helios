# Fixed samples

BenchBase measures throughput under concurrency, so two runs of the same build
differ by several percent, and at 32 or 64 terminals by 10 to 20 percent. These
scripts instead replay a fixed sample: the same statements with the same
parameters in the same order, on one connection, from the same loaded state.
Two runs then do the same work, and what a change costs or saves per
transaction can be read from instruction counts, which repeat to within 0.5
percent between runs.

They answer "how much work does one transaction of each type cost each layer,
and where". They do not measure contention, retries, or how throughput scales
with terminals. BenchBase is still the tool for those.

## Pieces

| File | What it does |
|---|---|
| `FixedTpcc.java` | Runs the BenchBase TPC-C procedure classes (the same SQL, through Connector/J with `rewriteBatchedStatements=true`) with inputs from a seeded `Random`, in blocks of one transaction type. Between blocks it reads the CPU time of the processes named by `--pid` from `/proc` and runs `--hook`. |
| `fixed_tpcc.sh` | `template` initializes a MySQL data dir with the plugin installed. `run` loads TPC-C SF 1 (BenchBase's loader is deterministic per unit), replays the sample on the same processes, and writes a digest of the final state. `tpch` loads TPC-H with the 23 postload indexes and the DuckDB secondary engine and runs Q1-Q22. |
| `fixed_tpch.py` | Runs the checked-in Q1-Q22 one at a time, with the wall time, the CPU time of mysqld and the storage server, and the md5 of the sorted result. |
| `cg_hook.sh` | The hook that zeroes callgrind's counters at the start of a block and dumps them at its end. |
| `cg_summary.py` | Reads the callgrind dumps: total, per transaction, inclusive cost by category, and the top functions by self cost. |
| `summarize.py` | CPU time per transaction and latency per block, the mean and spread over several runs. |
| `rpc_summary.py` | Statements, RPCs by type and bytes per transaction type, from a `--rpc-trace` run. |
| `perf_split.py` | Splits a `--perf` profile into user and kernel time and attributes the kernel time to the user frame that entered it (send, recv, WAL). |
| `tpch_compare.py` | Compares two builds' `fixed_tpch.py` outputs query by query (wall, CPU time, md5). |
| `build_version.sh` | Builds the plugin and the storage server of another commit (optionally with a patch) into a directory of its own, reusing this checkout's MySQL build. |

## Use

```bash
# once
javac -cp "bench/benchbase-mysql/benchbase.jar:bench/benchbase-mysql/lib/*" \
  -d bench/fixed/classes bench/fixed/FixedTpcc.java
bench/fixed/fixed_tpcc.sh template ~/fixed/tmpl

# CPU time and latency per block
bench/fixed/fixed_tpcc.sh run ~/fixed/tmpl ~/fixed/out/run1.json
python3 bench/fixed/summarize.py ~/fixed/out/run*.json

# instruction counts of the storage server per block (callgrind)
P=idle:2000,mix:100,NewOrder:100,Payment:100,OrderStatus:100,Delivery:20,StockLevel:100
STORAGE_BIN=build/server_vg/helios-storage \
  bench/fixed/fixed_tpcc.sh run ~/fixed/tmpl ~/fixed/out/cg.json --plan $P --valgrind storage
python3 bench/fixed/cg_summary.py ~/fixed/out/cg.json.storage.cg.3 --per 100

# another commit
bench/fixed/build_version.sh c2bb4d9 ~/fixed/ver/c2bb4d9
STORAGE_BIN=~/fixed/ver/c2bb4d9/server/helios-storage PLUGIN_DIR=~/fixed/ver/c2bb4d9/plugin \
  bench/fixed/fixed_tpcc.sh run ~/fixed/tmpl ~/fixed/out/c2bb4d9.json
```

`--plan` lists the blocks as `label:count`. A label is a transaction type, or
`mix` for the standard 45/43/4/4/4 mix, or `idle` to sleep `count`
milliseconds (what the processes burn with no transactions). Each block draws
from a `Random` seeded by `--seed` and the block's position, so a block's
inputs do not depend on the blocks before it, but the database state does:
compare runs with the same plan.

## Things to know

- **Load every run.** The storage server keeps its per-table row counts in
  memory and does not rebuild them on recovery. After a restart the optimizer
  sees 2-row tables, and StockLevel becomes a hash join over all of stock (100
  times slower). So `run` loads afresh instead of restoring a copy of the data
  dirs.
- **valgrind and AVX-512.** The storage server builds with `-march=native`;
  valgrind cannot decode AVX-512. `build_version.sh` also builds a copy with
  `-mno-avx512f` (`server_vg/`) to run under callgrind. Compare instruction
  counts only between binaries built with the same flags.
- **Instructions are not cycles.** Instruction counts are stable, but they
  overweight code that runs many cheap instructions (the SQL parser) and
  underweight code that waits on memory (index lookups on a large table).
  Check a conclusion against a time-based profile (`--perf`) before acting on
  it.
- **Callgrind runs the process on one core.** Do not read latency or
  throughput from a callgrind run. With `--instr-atstart=no` the load runs at
  close to native speed and only the replay is instrumented.
- **Time is mostly not instructions.** On the cloud VM these were written on,
  58% of the storage server's CPU time was kernel time, mostly the wakeups of
  the request/response round trips; the transaction-scoped read plans
  (`HELIOS_PREFETCH_PLAN=1`, which FixedTpcc honours as BenchBase does) cut
  the storage CPU time per transaction by a third with the same instruction
  count. Fewer instructions do not by themselves mean less CPU time.
- **perf.** `--perf` uses the `perf` under `/usr/lib/linux-tools/` unless
  `PERF` names one. The storage server builds without frame pointers, so its
  user-space call chains break; read its profile by self cost and by
  `perf_split.py`.
- **Same work.** `run` writes `<out>.digest` (sums over district, oorder,
  new_order, order_line, customer and history). Two runs of the same sample
  should give the same digest; across commits the same digest also says the
  two commits computed the same result.
