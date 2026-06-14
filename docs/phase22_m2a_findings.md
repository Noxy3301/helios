# Phase 22 — M2a: access-class gate widens the materialization-charge band

_Removes the discrete plan-shape flag that M1's `kC_materialise` was implicitly
encoding, so the constant becomes a genuine (wide-band) hardware constant. Cost-
only change. Dual-review: grounded investigation (file:line-verified) + Codex
(CHANGES → two "validate-this" blockers, both empirically resolved below)._

## The gate

`read_cost()` (non-covering branch) now applies the per-row materialization
charge **only when** `helios_charge_materialise()` is true. The predicate
(`proxy/ha_lineairdb.cc`, defined out-of-line because TABLE is incomplete in the
header) returns **false (skip charge)** iff, from prepare-time state available at
`best_access_path`/`read_cost` time:

- plain read-only SELECT (`sql_command==SQLCOM_SELECT`, `!is_explain()`, `reginfo.lock_type<=TL_READ`), AND
- `lineairdb_predict_prefetch_mode(thd)` (bulk prefetch active), AND
- the owning `table->pos_in_table_list->query_block` is **single-table grouped**
  (`leaf_table_count==1 && is_grouped()`, no DISTINCT/window/ROLLUP).

This is the **necessary precondition shared by both execution recognizers**
(`helios_try_register_gs` and `helios_offloadable_shape` both start from
`leaf_table_count==1 && is_grouped()`), so cost and execution agree on which
scans are per-row-materialized. **Conservative default = charge** (any missing
context / multi-table join / non-prefetch / locked read → charge), which never
under-prices a genuine per-row join scan.

**Timing rationale (grounded):** `read_cost` runs at `JOIN::optimize` make_join_plan;
agg-pushdown/GS registration + `override_executor_func` run later at
`push_to_engine`. So `pushed_aggregate_`/GS-registration/finalized-QEP are
unknown at cost time — but the aggregation *shape* (`query_block` group/leaf
state) is prepare-time and fully readable. The gate reads ONLY that.

## Result — the band widened from [8,10] to [8,24+]

Widen-window sweep on SF1 (gate ON), `scripts/dev/m2a_validate.sh`. Pre-gate
(M1) q15 broke at ≥11 (1.47s@8 → 9.96s@11). With the gate:

| kC_materialise | q15 | q3 | q5 | q7 | q8 |
|---|---|---|---|---|---|
| 8 | 1.46 | 1.20 | 0.97 | 1.56 | 2.87 |
| 11 | **1.49** | 1.16 | 0.97 | 1.56 | 2.71 |
| 16 | **1.52** | 1.22 | 0.97 | 1.58 | 2.76 |
| 24 | **1.48** | 1.15 | 0.93 | 1.64 | 2.64 |

**q15 stays ~1.5s at every value** (decoupled from the constant); the JOIN
regressors stay flipped/fast; collateral (q1/q9/q13/q21) stable throughout.

**Full 22-suite at kC_materialise=16: 22/22 OK, 52.11s** (vs M1's 55.01s at 8) —
no regression, no false-skip under-pricing.

## Codex's two blockers — empirically resolved

- **BI-2 (q15 query_block must resolve to the inner aggregating unit):** q15
  stays ~1.5s at 11/16/24 → the gate correctly reaches the inner grouped unit
  (had it resolved to the outer block, q15 would have been false-charged and
  regressed). Confirmed.
- **BI-1 (false-skip under-pricing — the skip is the recognizer *prefix*, not a
  sufficient predicate):** the full 22-suite at the widened value (16) is 22/22
  OK with no regression → no query is harmfully under-priced. The prefix +
  prefetch-mode is empirically sufficient here because a single-table grouped
  read-only SELECT range is bulk-staged (q15) rather than per-row materialized;
  the conservative charge-by-default covers the rest.

## Status
M2a COMPLETE. The constant is now a wide-band hardware constant (interior, not a
cliff). Default stays 8 (M2b derives the principled value). Next:
- **M2b** — rigorous NNLS micro-benchmark calibration of all constants
  (C_rpc/C_byte/C_rowremote/C_probe/C_materialise/B_eff) against TRUE
  cardinalities, with the scale-invariance check at ≥2 SFs (per
  `docs/phase22_m2_methodology.md`); this re-derives C_materialise from physics
  (does it land near 8?) with cardinality-bias margin.
- **M5** — q18 (still 24s) pushdown-abandonment.
