# Phase 22 — M2b findings: physical calibration of the cost constants

_Executes design v4 (docs/phase22_m2b_design.md, dual-review GO). Measures the
PHYSICAL per-row/per-byte/fixed-RPC costs under prefetch ON from server-side
`TX_EXECUTE_READ_PLAN.us` against true (probe-table) cardinalities; anchors them to
the cost-model units; quantifies the gap vs the current defaults; and (§7.1) tests
whether the physical constants reproduce the M1/M2a win on the 22-suite. Scripts:
scripts/dev/m2b_{probe_gen.py,taxonomy.sh,fit.py,accept.sh}. Raw: /tmp/m2b_struct.jsonl._

## 1. Method (design §3–§5)

Probe tables (Wu calibration queries, exact COUNT(*)): `cal_n_<N>` (narrow: pk,k),
`cal_w_<N>` (wide: +pad VARCHAR(512), HIGH-ENTROPY to defeat projection-pushdown
LZ4 so resp_b reflects true bytes), pk=1..N dense, k=pk so `k BETWEEN 1 AND R`
returns EXACTLY R rows. Measured under **prefetch ON** (deployment regime) via the
server-side `us` of the single staged RPC (excludes client fetch). Probes are
NON-GROUPED (charge regime; recall is_grouped()=group_list>0||m_agg_func_used,
sql_lex.h:1283). C_mat probe is a standalone non-covering SECONDARY RANGE — the only
access class read_cost()/C_mat governs. (Evidence, repo-local: only read_cost()
adds C_materialise (ha_lineairdb.hh:385-407); index_scan_cost (covering, :413-418)
and helios_ref_cost (eq_ref/ref, :364-372) do NOT; and a ref/eq_ref is priced by the
un-overridden page_read_cost, not read_cost — design §4-note. The upstream MySQL
line citations there (find_cost_for_ref / multi_range_read_info_const) are against an
un-vendored tree and are corroborated by this proxy-side fact, not independently
pinned.)

**Engine fact discovered during impl:** this engine has NO index-only execution
(index_flags lacks HA_KEYREAD_ONLY, hh:251-255; EXPLAIN of a covering `SELECT k`
shows `Using where`, not `Using index`). So the covering-vs-non-covering difference
is a BYTE difference, not a materialise-vs-not step. C_mat is therefore measured as
the **random-materialise PREMIUM of a range over a sequential scan** (the
PostgreSQL random_page_cost analog), not as covering−non-covering.

**Contiguity confound resolved by a scattered-PK probe (cal_w_shuf, k=LCG
permutation so `k BETWEEN 1 AND R` selects R rows with SCATTERED base PKs).** The
k=pk probe materialises a CONTIGUOUS PK range (best-case locality), so its premium is
a lower bound. Re-measuring with scattered PKs (same N, same R-sweep):
scattered per-row 1491 ns vs contiguous 1431 ns → premium 656 vs 597 ns/row =
**1.10× — the contiguity confound is only ~10%**. The reason is structural: LineairDB
is fully in-memory, so a "random" PK fetch has NO disk-seek penalty (unlike the
disk-based random_page_cost regime); the random premium is a small CPU-cache effect.
So the physical C_mat premium is **measured (not merely lower-bounded) at ~600–800
ns/row across runs and contiguous/scattered access** (run-to-run variance ~25%).

## 2. Physical constants (server-side, prefetch ON; m2b_fit.py)

Measured per-row/per-byte slopes (jackknife-over-design-point 95% CI):

| quantity | physical | note |
|---|---|---|
| S_scan (sequential scan) | **439 ns/row** [324, 554] | cal_n narrow N-sweep 1e3..1e6 (OLS; per-point mean 365 — see §3) |
| B_ship (server per byte) | **1.83 ns/byte** | from noncov−cov byte delta |
| non-cov range per-row (contiguous) | 1787 ns/row [1715, 1859] | walk + PK materialise + ship |
| non-cov range per-row (SCATTERED) | ~1491 ns/row | cal_w_shuf; random PK (separate run) |
| fullscan marginal ship/row | 835–994 ns/row | sequential materialise + ship (run-variant) |
| **C_mat = random-materialise premium** | **~600–800 ns/row** | = noncov − fullscan_ship; scattered 656, contiguous 597–793 |
| C_rpc (1 staged RPC fixed) | **81 µs** | 1-row point read |

**Estimator caveats (disclosed for paper-defensibility; all move C_mat by <~2× and
none reverse the conclusion):** (1) noncov ships a constant +16 B/row vs fullscan
(byte-correcting drops C_mat ~29 ns/row, ≈3.7%). (2) the fullscan SLOPE (994 ns/row)
is the marginal emit+ship of one OUTPUT row, not the sequential per-row VISIT cost
(which lives in the ~58 ms full-scan intercept ≈ 583 ns/row); so C_mat ≈ walk +
PK-fetch − emit, a GENEROUS (upper-leaning) estimate of the pure premium. (3) the
contiguity confound is measured at ~10% (scattered probe, §1). Net: the physical
C_mat premium is small and bounded — **~0.2–0.33 cost-units** after anchoring (§3).

## 3. Anchor + gap vs current defaults — the headline result

Anchor (design §1.1, no rescale; a UNIT CONVENTION, not an independent calibration):
the model's `table_scan` CPU = N·(C_ROW+C_REMOTE) = N·0.15 cost-units ↔ measured
S_scan ⟹ 1 cost-unit ≈ 2400–2928 ns. The band reflects two sensitivities: the
S_scan estimate (OLS slope 439 ns/row, pulled by the N=1e6 point, vs per-point mean
365) and the fact that cal_n narrow ships ~44 B/row whose byte cost the model charges
separately, so a byte-corrected anchor is ~2391 ns/cu. Converting the physical
constants and comparing to the env defaults (range = anchor + estimator sensitivity):

| constant | physical (cu) | default (cu) | default / physical |
|---|---|---|---|
| C_RPC         | ~28   | 50.0   | **~1.8×** (same order — defensible) |
| C_BYTE        | ~0.00062| 0.0008 | **~1.3×** (≈ right) |
| **C_MATERIALISE** | **~0.2–0.33** | **8.0** | **~20–40× (order of magnitude)** |

**So C_byte and C_rpc are physically reasonable (within ~1.3–1.8× of the measured
values); only C_materialise is far off — the production value 8.0 is roughly an
ORDER OF MAGNITUDE (~20–40×, anchor/run-dependent) above the physical random-
materialise premium (~0.2–0.33 cu).** The decision-relevant C_mat/S_scan RATIO
(~1.8× physical vs ~53× in the model) is anchor-free and makes the same point. This
answers the project's founding question ("does 8.0 re-derive from physics?"): **NO —
8.0 is an order-of-magnitude steering inflation, not a hardware constant.**

### 3.1 Corroboration with the M1 sweep (independent)

M1's whole-query sweep (phase22_m1_findings.md) found the JOIN regressors q3/q5/q7/q8
only flip to full-scan+prefetch at **C_mat ≈ 4–8**; below that they regress. The
physical premium is **~0.2–0.33 cu**. The two independent methods agree the
production need (~8) is **an order of magnitude (~20–40×) above the physical value**
— the gap is real, not a fitting artifact.

### 3.2 Access crossover s* (single-table, the SF-independent quantity)

Measured single-table range-vs-fullscan crossover **s*_access ≈ 73.5% selectivity**
(N=100000): a non-covering range beats full-scan+prefetch until it touches ~3/4 of
the table. A model carrying the PHYSICAL C_mat (~0.2–0.33 cu) predicts a crossover
near this measured value; the default 8.0 predicts a far LOWER crossover (it flips to
full-scan for much more selective ranges) — i.e. **8.0 deliberately over-penalises
single-table ranges.**
That over-penalisation is wrong for single-table access but is what the JOIN
regressors need — the gap is the join/cardinality context (§4, §5).

## 4. Interpretation — why 8.0 ≫ physical (the central thesis, now quantified)

The order-of-magnitude gap is **consistent with** two effects the single-access-path
cost model cannot see (this is a HYPOTHESIS — M2b measures the gap but cannot
numerically apportion it between the two; see the caveat below):
1. **Cardinality underestimation (grounded, likely substantial).** The repo records
   q3/q10 range cardinalities estimated **3–10× too LOW** (phase22_m0_findings.md:
   44-48, grounded in M0 EXPLAIN-ANALYZE actuals). The model prices a range as
   R_est·C_mat; if R_est is 3–10× low, C_mat must be ~3–10× high to recover the
   correct total penalty — accounting for a large part of the gap.
2. **Join-NLJ amplification (unmeasured residual).** The regressors are joins; a
   non-covering range feeding a join inner plausibly costs more than the standalone
   single-table range the premium was measured on (per-driver work, prefetch
   staging). This is the residual the cardinality factor does not cover, but M2b did
   NOT measure it directly.

**Apportionment caveat (review r4):** M2b data establishes the GAP (order of
magnitude) and grounds the cardinality factor in M0 actuals, but cannot split the gap
between cardinality and join-context — the join-NLJ term is an unmeasured residual. A
future micro-experiment (the same non-covering range standalone vs as a join inner at
matched cardinality) would apportion it; a counterfactual run with corrected
cardinalities would test factor (1) directly. Neither is claimed here.

**Conclusion: `C_materialise` functions as a workload STEERING parameter, not a
re-derivable hardware constant** — it compensates for current cardinality and
join-cost errors. This is precisely why M2a couples it to an executor-aware GATE
(charge only where a per-row materialise steers a join/non-grouped plan) rather than
shipping it as a universal per-row cost. **The higher-leverage next milestone (Leis;
the goal's own note) is to correct the q3/q10 cardinality underestimate**; after
that, whether a near-physical C_mat suffices is a TESTABLE hypothesis (untested here),
not an established result.

This is the **FAIL branch of the §0 thesis, and it is the publishable result**:
physically-calibrated constants do NOT reproduce the production plan choices; the
order-of-magnitude gap (~0.2–0.33 vs 8.0 cu) is the quantified evidence that
C_materialise is context-dependent. C_byte and C_rpc, by contrast, ARE physically
grounded and could be tightened to their measured values (1.3–1.8× moves) as a minor,
defensible recalibration (verify no suite regression first).

## 5. Scale-invariance (design §6)

S_scan across the 1e3→1e6 probe sweep: 385/281/358/437 ns/row — stable within ~1.5×,
NOT a clean constant. The variation is an **in-memory residency effect** (CPU-cache/
NUMA/allocator locality at larger N), exactly the regime design §6 says remains even
without buffer-pool disk spill. The per-row constants are scale-invariant to within
this ~1.5× in-memory-residency band; this is reported honestly, not dismissed as
noise. (The decision-relevant s* is a ratio and is more stable than the absolute
slopes.)

## 6. §7.1 hold-out confirmation (fullidx SF1 suite) — DONE, thesis CONFIRMED

Pre-registered C_mat sweep on the backfilled fullidx SF1 server (full deployment:
prefetch+agg+semijoin ON; governor/C6 pinned; mysqld-restart-only per value, server/
data preserved). Regressors q3/q5/q7/q8 + gate-skip q15/q1/q6 (median-of-3, s):

| C_MAT | q3 | q5 | q7 | q8 | q15 | q1 | q6 |
|---|---|---|---|---|---|---|---|
| **0.27 (physical)** | 5.51 | 3.35 | **12.37** | **8.34** | 1.40 | 0.75 | 0.37 |
| 1 | 5.55 | 3.40 | 12.33 | 8.53 | 1.41 | 0.74 | 0.37 |
| 2 | 1.14 | 3.42 | 12.27 | 8.38 | 1.46 | 0.76 | 0.37 |
| 4 | 1.14 | 3.48 | 1.53 | 8.42 | 1.39 | 0.76 | 0.38 |
| **8 (default)** | 1.15 | 0.93 | 1.53 | **2.65** | 1.42 | 0.74 | 0.35 |

**Result — the §0 thesis FAIL branch, demonstrated:**
1. **Physical-ish C_mat (0.27) regresses the suite** — q7=12.4 s, q8=8.3 s, q3=5.5 s,
   q5=3.3 s (the 2× regression returns). Physically-calibrated constants do NOT
   reproduce the production plan choices.
2. **The lower band edge is bracketed in (4, 8]** — each regressor flips to its fast
   plan at a different value (q3 in (1,2], q7 in (2,4], q5 & q8 in (4,8]); ALL four
   are fast only at the tested 8 (the grid is coarse; the edge is bracketed, not
   pinned, and the upper limit ~11 where q15 breaks is carried over from M1, not
   re-measured here). So the production need is an order of magnitude above the
   physical premium (~0.2–0.33 cu).
3. **Triple corroboration of the gap (order of magnitude):** physical fit (~20–40×,
   §3) ≈ §7.1 lower band edge in (4,8] ≈ M1's independent whole-query sweep
   (q7@4, q5/q8@8, §3.1).
4. **M2a gate confirmed:** the gate-skip queries q15/q1/q6 are FLAT across the entire
   C_mat range (1.40/0.75/0.37) — C_materialise does not touch them, exactly as the
   executor-coupled gate intends. The steering parameter is correctly scoped.
5. At C_mat=8.0 the regressors return to their M1/M5 recovered values (q3 1.15, q5
   0.93, q7 1.53, q8 2.65); the full 22-suite at 8.0 = 31.75 s, md5 22/22 vs InnoDB
   SF1 is already established (phase22_verification.md) — the shipped default holds.

This is the publishable result: **a physically-grounded per-row materialise cost
(~0.2–0.33 cu) cannot steer the production plans; the value that does (8.0) is an
order of magnitude (~20–40×) larger,
and that gap is the quantified evidence that C_materialise is a context-dependent
steering parameter (cardinality bias 3–10× + join-NLJ amplification), not a hardware
constant** — which is exactly why it is executor-gated (M2a) rather than universal.

## 7. Recommendations
- **Ship:** keep `C_MATERIALISE=8.0` as a DOCUMENTED steering default, executor-gated
  by M2a (provenance: this finding — physical ~0.2–0.33 cu, steering 8.0, gap is an
  order of magnitude, consistent with cardinality 3–10× + join context). Optionally
  tighten C_BYTE→~0.00065 and C_RPC→~28 to their physical values (minor, within-band;
  verify no suite regression first).
- **Next high-leverage milestone (beyond M2b):** correct the q3/q10 cardinality
  underestimate (records_in_range / rec_per_key), per Leis — this is where the real
  error lives, and it would let C_materialise approach its physical value.
