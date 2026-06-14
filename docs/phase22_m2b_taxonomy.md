# Phase 22 — M2b grounding: measured RPC taxonomy per access-class × prefetch regime

_Empirical foundation for the M2b calibration redesign. Surfaced by the M2b design
dual-review (both Codex and the grounded Claude panel flagged that the v1 probe
design assumed a proxy-side per-RPC decomposition that may not match the deployment
execution path). Measured directly with `ENABLE_RPC_TRACE` on the live noidx-SF1
server (mysqld restarted with COST_V2 on, AGG_PUSHDOWN/SEMIJOIN OFF to isolate the
raw access paths). Driver: scripts/dev/m2b_taxonomy.sh._

## What was measured

Six probes, each run once under `lineairdb_prefetch_execution=ON` and once OFF,
parsed from the trace's `summary_by_type` (data-path RPCs only; control RPCs
TX_BEGIN/DB_END/DB_FENCE/TX_VALIDATE_AND_COMMIT excluded):

| probe | prefetch **ON** (deployment) | prefetch **OFF** (fallback) |
|---|---|---|
| P1 full scan (`SUM(s_acctbal)`, IGNORE INDEX) | **1× TX_EXECUTE_READ_PLAN** (419 KB) | 1× GET_MATCHING_KEYS_AND_VALUES_FROM_PREFIX (1.64 MB) |
| P2 covering 2ary range (`SELECT s_nationkey … BETWEEN`) | **1× TX_EXECUTE_READ_PLAN** (104 KB) | TX_BATCH_READ + GET_MATCHING_PRIMARY_KEYS_IN_RANGE |
| P3 non-covering 2ary range standalone (`SUM(s_acctbal) … BETWEEN`) | **1× TX_EXECUTE_READ_PLAN** (122 KB) | TX_BATCH_READ + GET_MATCHING_PRIMARY_KEYS_IN_RANGE |
| P4 **eq_ref join by PK** (driver ⋈ orders ON o_orderkey) | **1× TX_EXECUTE_READ_PLAN** (50 KB) | **506 RPC: TX_READ×503** + 1 range |
| P5 single point read (`s_suppkey=42`) | **1× TX_EXECUTE_READ_PLAN** (153 B) | TX_READ×1 |
| P6 non-cov 2ary range → join inner (nation ⋈ supplier) | **1× TX_EXECUTE_READ_PLAN** (122 KB) | TX_BATCH_READ×5 + ranges |

## The decisive finding

**Under prefetch ON (the deployment + acceptance regime), EVERY access class
collapses to exactly ONE `TX_EXECUTE_READ_PLAN` round-trip.** Full scan, covering
range, non-covering range, eq_ref join, point read, and non-covering-range-join-
inner are ALL one staged RPC. The proxy stages the whole read plan; the server
executes it and ships the result set in a single bulk response.

Corollary 1 — **per-row PK materialisation (`TX_READ` per row) is a prefetch-OFF
phenomenon, not a deployment one.** P4 (the exact eq_ref-join-by-PK shape that
`C_materialise` is meant to price) issues **503 TX_READ** with prefetch OFF but
**one** TX_EXECUTE_READ_PLAN with prefetch ON. The per-row round-trip the cost
model's read_cost charge (`ceil(rows/B_eff)·C_rpc + rows·C_materialise`) is built
around **does not occur in the deployment regime.**

Corollary 2 — **under prefetch ON the RPC count carries almost no discriminating
signal** (it is ~1 for every plan). What differs between plans is (a) **bytes
shipped** (resp_b: 419 KB full scan vs 122 KB non-cov range vs 153 B point) and
(b) **server-side CPU** to execute the staged plan (scan N rows vs walk a 2ary
range + materialise R base rows by PK), which surfaces only in the staged RPC's
`us` and end-to-end latency — NOT in any proxy-observable RPC count.

## What this means for the cost model (and M2b)

The model under calibration (proxy/ha_lineairdb.hh:336–417) prices a non-covering
range as `helios_ref_cost + ceil(rows/B_eff)·C_rpc + rows·C_materialise`. Measured
against the deployment regime:

1. The `ceil(rows/B_eff)·C_rpc` IO term **does not correspond to real RPCs** under
   prefetch ON (actual = 1). It is a **steering proxy**: it inflates the estimated
   cost of a large non-covering range so the optimizer prefers full-scan+prefetch.
   With B_eff=1024 and R up to millions this term is large and does much of the
   steering — which is exactly why M1's `C_materialise=8.0` flipped q3/q5/q7/q8
   (it makes the to-be-rejected plan look expensive), even though physically both
   the rejected and chosen plans are one staged RPC.
2. `rows·C_materialise` is best read as a **server-side per-row materialisation
   CPU** cost (the server, inside the staged plan, fetches R base rows by PK), NOT
   a proxy-side per-row round-trip. It is real and ∝ R, but it lives on the server.
3. The **physically-grounded discriminators in deployment are bytes (C_byte, the
   one term that maps cleanly to a measurable, resp_b) and server-side per-row CPU
   (scan vs materialise).** A defensible calibration must fit these from end-to-end
   latency under prefetch ON, and must DOCUMENT the `ceil/C_rpc` term as a steering
   proxy (state its effective magnitude and that it does not count real RPCs),
   rather than pretend it is a measured RPC fanout.

This reframes M2b from "decompose proxy-side RPC counts" (v1, wrong for deployment)
to **"calibrate, under prefetch ON, the server-side per-row CPU slopes and the byte
term that actually separate plans, using end-to-end latency vs known cardinality;
treat C_rpc as the fixed one-staged-RPC overhead and the read_cost ceil-term as an
acknowledged steering proxy."** The prefetch-OFF regime (where TX_READ-per-row is
real) remains available as a secondary bench to price the fallback path and to
sanity-check the raw per-row materialise cost in isolation.

## Open question this raises (for the design v2 + reviewers)

If, under prefetch ON, the rejected (non-covering range) and chosen (full-scan+
prefetch) plans both execute as one staged RPC, **is the non-covering-range plan
actually slower in deployment, or only in the prefetch-OFF fallback?** If the
server executes a staged non-covering range as efficiently as a staged full scan,
the M1 penalty may be steering away from a plan that is fine under prefetch — OR
the server-side per-row PK materialisation genuinely is the bottleneck even when
staged. M2b must MEASURE the staged non-covering-range vs staged-full-scan latency
at matched output to decide whether C_materialise prices a real deployment cost or
a fallback-only cost. This is the single most important experiment for the paper's
cost-model claim and is promoted to the top of the design v2.
