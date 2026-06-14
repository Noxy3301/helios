# Phase 22 — M1: non-covering range/ref per-row materialization cost (read_cost)

_The primary lever identified by M0. Cost-only change (results unaffected). Dual-review GO (grounded Claude arbiter + Codex)._

## The fix

`proxy/ha_lineairdb.hh` `read_cost(index, ranges, rows)` — the NON-covering branch
(`handler::multi_range_read_info_const` dispatches covering→`index_scan_cost`,
non-covering→`read_cost` via `HA_MRR_INDEX_ONLY`, `sql/handler.cc:6291`). A
non-covering scan materialises every qualifying row from the remote server by PK,
which the prior `helios_ref_cost` omitted. Added (non-covering only; covering
`index_scan_cost` untouched, so index-only scans stay cheap):

```cpp
Cost_estimate c = helios_ref_cost(ranges, rows);
c.add_io (std::ceil(rows / kEffBatch()) * kC_rpc());   // batched PK-materialisation RPCs
c.add_cpu(rows * kC_materialise());                    // per-row remote fetch; env HELIOS_C_MATERIALISE
return c;
```

`kC_materialise` is an env-tunable constant (`HELIOS_C_MATERIALISE`), default **8.0**.

## Why it works (M0 trace)

optimizer_trace for q7 at SF1: the `l_sd` range was estimated at 300k rows /
cost 330k vs lineitem `table_scan` cost 3.31M, so the range scan won by 10× in
the model — yet executed 3–8× SLOWER (per-row PK materialisation = ~per-row RPC,
not bulk-prefetched), because the cost omitted the materialisation. The added
per-row term makes full-scan+prefetch win once R is large.

## Calibration (M1 quick sweep; M2 does it rigorously)

`scripts/dev/m1_sweep.sh` + the q15 probe, all on the LIVE SF1 server by mysqld
restart only (the env-tunable constant means NO rebuild / NO re-backfill per value):

| kC_materialise | q7 | q5 | q8 | q15 |
|---|---|---|---|---|
| 0.5 (≈ off) | 13.1 | 3.50 | 8.50 | 1.54 |
| 4 | **1.69** | 3.46 | 8.57 | 1.56 |
| **8** (default) | **1.61** | **0.94** | **2.67** | **1.47** |
| 11 | 1.66 | 1.03 | 2.78 | **9.96 (regressed)** |

q7 flips at 4; q5/q8 at 8; **q15 breaks at ≥11**. Safe band ≈ **[8, 10]**.

**The q15 lesson (drives M2):** q15's view aggregates lineitem over an `l_shipdate`
range (`SUM … GROUP BY l_suppkey`). Its non-covering range is **bulk-prefetched /
server-aggregated, NOT per-row materialised**, so the per-row charge is *wrong*
for it and an over-large value (≥11) flips it to a worse full scan. A single
row-scaled constant cannot distinguish a JOIN (materialises rows → charge right)
from an AGG-pushdown/GroupedSummary scan (server aggregates → charge wrong) —
hence the narrow window. **M2 widens it with a prefetch/agg-aware gate** (skip or
reduce the materialisation charge when the scan feeds a pushed-down aggregate).

## Result (SF1, default 8, full 22-suite, all OK)

q3 6.06→**1.17**, q5 3.61→**0.96**, q7 14.23→**1.74**, q8 9.31→**2.84**; q15
**1.52 (not regressed)**; q18 25.3 (unchanged — M5 pushdown target); no collateral
regression. **fullidx suite 82.85s → 55.01s (−34%), 22/22 OK.** Correctness:
cost-only change cannot alter results; the chosen full-scan plans were already
md5-verified in the noidx config (Phase 21).

## Scale-invariance note (answers "isn't 8.0 SF-specific?")

`kC_materialise` is a per-ROW ratio: the range-vs-fullscan crossover is `rows ×
kC_mat > table_scan ≈ N × per_row_scan`, i.e. a fixed FRACTION of the table,
independent of N (scale). So it is a SYSTEM/hardware constant (like PostgreSQL
`random_page_cost`), not an SF constant. Caveat: it was fit against SF1's
(3–10× underestimated) cardinality — the wide-ish band gives margin. M2 derives
it rigorously and scale-invariantly (separate methodology research underway).

## Status
M1 COMPLETE (dual-review GO, validated, no collateral). Remaining Phase-22:
**M2** (rigorous NNLS calibration + the prefetch/agg gate to widen the q15
window), **M5** (q18 pushdown-abandonment), then the engine_cost secondary lever
and `HELIOS_COST_V2` default-ON milestones.
