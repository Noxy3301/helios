# Phase 22 — cardinality milestone: findings (feasibility CONFIRMED)

_Implements design v2 (docs/phase22_card_design.md §8). Question this branch answers
(user framing: FEASIBILITY): does correcting the q3/q10 RANGE-cardinality
underestimate help, and does it let the C_materialise steering inflation (M2b: 8.0 ≈
20–40× physical) retire? **Answer: YES on both.** Scripts: scripts/dev/card_{qerror,
accept}.sh. LineairDB on branch helios/range-histogram (263b1f6)._

## 1. What was built

A server-precomputed per-index **equi-depth boundary histogram** on the leading key
part, searched LOCALLY in `records_in_range` (zero plan-time RPC), gate
`HELIOS_RANGE_HIST` (default OFF), fail-closed:
- **LineairDB** `ComputeIndexHistogram` (database_impl.h): one extra ordered
  live-filtered pass, independent of the int-only NDV gate (type-agnostic leading-part
  parse → DATE+int), boundaries `(leading_prefix_key, cum_live_rows)`.
- **proto** `IndexNdv.hist_bounds/hist_cum/hist_available` (piggyback GET_TABLE_STATS).
- **server** `hist_cache_` (same key/mutex/ANALYZE-bust as NDV).
- **proxy** parses → seeds `LineairDB_share::index_hist_` once in info() → records_in_range
  binary-searches it (rank_le over ascending bounds + cum), `estimate = rank(max) −
  rank(min)`, applied as a **FLOOR** (never below the 5%/10% heuristic).

### 1.1 The load-bearing bug (and fix)
First run: estimates didn't move. Debug (`[RANGEHIST]`) showed `total=2406` for orders
o_od (not 1.5M). Root cause: a LineairDB **secondary index is key→PK-list**, so a Scan
visits one entry per DISTINCT key (o_orderdate has ~2406 distinct days), and the
histogram was counting DISTINCT KEYS, not ROWS. **Encoding/rank/binary-search were
already correct** — the fraction 1147/2406 = 47.7% matched the true 48.5%. Fix: weight
each secondary key by its live PK count (`primary_keys().size()`; primary key weighs 1)
so the equi-depth histogram is over ROWS. After the fix `total` = 1.5M / 6.0M (correct).

## 2. Result 1 — q-error fixed (the direct cardinality win)

EXPLAIN-trace estimate the optimizer uses for the q3/q10 date ranges (SF1 fullidx):

| range predicate | est OFF | est ON | actual | q-error OFF → ON |
|---|---|---|---|---|
| orders `o_orderdate < d` (1-sided) | 150000 | **726663** | 727305 | 4.85× → **1.001** |
| lineitem `l_shipdate > d` (1-sided) | 600121 | **3281887** | 3241776 | 5.4× → **1.012** |
| orders 1993Q4 (2-sided) | 75000 | 75000* | 57069 | 1.31× → 1.31× |

*the histogram's own estimate was 70123 but FLOOR-only kept the (higher) 75000 — the
documented floor-only limitation; harmless (2-sided 5% was already ~right, q10).

## 3. Result 2 — the co-tuning headline: cardinality retires the steering inflation

C_materialise sweep WITH the histogram ON vs the M2b sweep (histogram OFF), fullidx SF1
regressors (median-of-3 s):

| C_mat | q3 ON/OFF | q5 ON/OFF | q7 ON/OFF | q8 ON/OFF |
|---|---|---|---|---|
| 0.27 | **1.16** / 5.51 | 3.57 / 3.35 | **1.70** / 12.37 | **0.46** / 8.34 |
| 1    | 1.29 / 5.55 | 3.58 / 3.40 | 1.68 / 12.33 | 0.45 / 8.53 |
| 2    | 1.18 / 1.14 | **0.93** / 3.42 | 1.67 / 12.27 | 0.44 / 8.38 |
| 4    | 1.13 / 1.14 | 0.93 / 3.48 | 1.67 / 1.53 | 0.45 / 8.42 |
| 8    | 1.19 / 1.15 | 0.96 / 0.93 | 1.68 / 1.53 | 0.45 / 2.65 |

**With cardinality corrected, the C_materialise lower band edge drops from ~8 (M2b) to
~2.** q3/q7/q8 are fast at EVERY C_mat including 0.27 — their entire steering need is
**retired by cardinality alone** (q7: 12.37 s @ 0.27 OFF → 1.70 s ON; q8: 8.34 → 0.46).
Only q5 retains a residual, needing C_mat ≥ 2 (down from 8). So **cardinality explains
the bulk of the steering gap (band edge 8 → 2); the residual (~2 vs physical ~0.27) is
the join-NLJ context M2b hypothesised** — now measured. This is exactly Leis ("fix
cardinality first; cost is the guardrail"), confirmed in Helios.

## 4. Result 3 — acceptance: net-positive + correct

Shipped config gate ON + C_materialise=4 (interior of the new [2,8] band), fullidx SF1,
full deployment, vs InnoDB:3308 SF1:
- **full 22-suite = 28.42 s (22/22 OK)** — vs the M2b baseline 31.75 s (gate OFF,
  C_mat=8) → **−10.5%**, and well below noidx 39.00 s. Net-positive AND improved.
- **md5 = OK 22 / MISMATCH 0 / ERR 0** — correctness preserved despite the plan flips
  the corrected cardinality induces.

## 5. Invariants (held)
MySQL core UNMODIFIED (proxy/server/LineairDB only); LineairDB on a feature branch;
1SR/OCC — the histogram rides the same non-transactional stats path as NDV, never
entering any OCC read-set; **zero plan-time RPC** (boundaries are SHARE-resident, info()
fetches once; records_in_range does an in-proxy O(log B) binary search, no RPC/repair);
fail-closed to 5%/10% on any absence/encode-mismatch; gated (HELIOS_RANGE_HIST) default
OFF; md5 22/22.

## 6. Feasibility verdict + scope notes
**FEASIBLE and net-positive.** Correcting range cardinality is the higher-leverage lever
M2b/Leis predicted: it fixes the q3/q10 q-error (≈1.0×), retires most of the
C_materialise steering inflation (band edge 8→2), and improves the suite (−10.5%) with
correctness intact.

Scope (feasibility, not production-complete): single-column leading INT/DATE ranges
(the q3/q10 case; TPC-H date cols are NOT NULL); B=64; two-pass build; floor-only
(won't lower a 2-sided over-estimate); NDV-drift staleness gate reused; live-PK weight
uses `primary_keys().size()` (≈ live count for read-only TPC-H). Production hardening
(NULLs, all key_range flags, composite trailing ranges, sub-bucket interpolation,
histogram-specific freshness, configurable B, 1-pass build) is documented in the design
review (docs/phase22_card_design.md §7) and deferred.
