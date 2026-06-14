# Phase 22 — M2b: rigorous calibration (DESIGN v2, post-grounding, pre-review)

_v1 (proxy-side per-RPC NNLS decomposition) was NO-GO'd by both Codex and the
grounded Claude panel; their core objection — the probes assumed an execution
shape that does not match the deployment regime — was then CONFIRMED by direct
measurement (docs/phase22_m2b_taxonomy.md + the pivotal latency sweep). v2 is
re-grounded on that measurement. Status: **draft for design dual-review round 2.**_

## 0. What changed from v1 (the grounding that forced a reframe)

Measured with ENABLE_RPC_TRACE on the live SF1 server (scripts/dev/m2b_taxonomy.sh):

1. **Under prefetch ON (the deployment + acceptance regime) EVERY access class —
   full scan, covering range, non-covering range, eq_ref join, point read — issues
   exactly ONE `TX_EXECUTE_READ_PLAN` RPC.** The per-row `TX_READ` round-trip that
   the cost model's read_cost charge (`ceil(rows/B_eff)·C_rpc + rows·C_mat`) is
   built around occurs ONLY with prefetch OFF (eq_ref join = 503 TX_READ OFF vs 1
   staged RPC ON). So v1's per-RPC-class decomposition is invalid for deployment.
2. **Pivotal latency sweep (prefetch ON, single-table, agg/semijoin OFF):** the
   2ary range is ~linear in R (~4 µs/row) and BEATS the flat 19.2 s full scan up
   to ~70% selectivity; the non-covering vs covering latency gap is the per-row
   server-side materialise cost ≈ **0.5 µs/row** (1.75 s over 3.6M rows). So the
   physical per-row materialise cost is small and measurable — yet M1 found
   `C_materialise=8.0` must make full-scan+prefetch WIN at far lower selectivity
   for the JOIN regressors. The reconciliation: full-scan wins in deployment not
   on raw single-table access cost but because **(a) agg-pushdown collapses a full
   scan's transfer to ~zero, and (b) a range feeding a join inner runs a per-driver
   NLJ** — neither of which a single-table isolated probe exercises.

**Reframe.** `C_materialise` is not a pure hardware constant; it is entangled with
downstream context (agg-pushdown, join strategy) — exactly what M2a's gate already
encodes and what the methodology flagged as the "structural-flag" risk, now proven
by measurement. v2 therefore (i) calibrates the PHYSICAL per-row/per-byte/fixed-RPC
costs that ARE scale-invariant, from end-to-end latency under prefetch ON; (ii)
treats the read_cost `ceil/C_rpc` term as an explicit, documented STEERING PROXY,
not a measured RPC fanout; and (iii) lets **the 22-query suite be the arbiter** of
whether physically-grounded constants reproduce the M1/M2a net-positive result —
a decisive, paper-defensible outcome either way (8.0 confirmed-from-physics, or
8.0 demonstrated to be an irreducible steering parameter with the gap quantified).

## 1. The model form (VERIFIED proxy/ha_lineairdb.hh:336–417)

```
full scan   (table_scan_cost):  io = C_rpc·1              + bytes·C_byte
                                cpu = N·(C_row + C_remote)
covering ref(index_scan_cost):  io = (ranges|1)·(C_rpc/B_eff) + bytes·C_byte
  = helios_ref_cost             cpu = rows·(C_probe + C_row)
non-cov ref (read_cost):        helios_ref_cost
  (charge-materialise gate ON)  + io: ceil(rows/B_eff)·C_rpc     ← STEERING PROXY
                                + cpu: rows·C_materialise         ← server per-row CPU
```
Env knobs (helios_cost_param): C_RPC=50, C_BYTE=0.0008, C_ROW=0.10, C_PROBE=0.05,
C_REMOTE=0.05, C_MATERIALISE=8.0, BATCH=1024. `total_cost()=io+cpu+import`, a plain
linear sum, equal-weighted (VERIFIED handler.h:3717); `multiply()` scales all three
uniformly (:3746) — so a uniform µs→cost-unit rescale is plan-invariant IN
ISOLATION; commensurability with MySQL's NON-rescalable server_cost (row_evaluate
=0.1) is what the anchor (§5) fixes. The `(ranges>0?ranges:1)` floor (hh:368) means
the covering io term is `C_rpc/B_eff` (~0.05) even at ranges=0 — negligible.

### 1.1 Identifiable quantities (reconciled with the measured regime)

Runtime cannot separate C_row from C_remote (only their sum is ever multiplied) nor
C_probe from C_row. We fit the composite slopes the code multiplies; we recover env
knobs by IMPOSING the anchor as a DEFINITION (no division, no rescale — per both
reviewers): **C_row := 0.10 by fiat; C_remote := max(0, S_scan − 0.10);
C_probe := max(0, S_ref − 0.10).** State explicitly in the paper that
(C_row,C_remote) and (C_probe,C_row) are individually non-identifiable; only the
sums S_scan, S_ref are data-constrained.

| fit symbol | physical meaning (prefetch-ON, server-side) | env mapping |
|---|---|---|
| `C_rpc`  | fixed cost of one staged TX_EXECUTE_READ_PLAN (the UNIT) | `HELIOS_C_RPC` |
| `C_byte` | per byte shipped in the staged response (resp_b) | `HELIOS_C_BYTE` |
| `S_scan` | full-scan server CPU per row scanned (≈3.2 µs/row prelim) | `C_REMOTE=S_scan−0.10` |
| `S_ref`  | covering-range server CPU per row | `C_PROBE=S_ref−0.10` |
| `C_mat`  | per-row server-side base-row PK materialise CPU (≈0.5 µs/row prelim) | `HELIOS_C_MATERIALISE` |
| `B_eff`  | steering-proxy batch divisor (NOT a measured RPC count under prefetch) | `HELIOS_BATCH` |

`B_eff` note (reviewer MAJOR): one env knob drives TWO terms — per-RANGE in
helios_ref_cost and per-ROW in read_cost. Under prefetch ON neither is a real RPC
count; B_eff is a steering-proxy magnitude. We do NOT "measure" it from a trace; we
treat the read_cost `ceil(rows/B_eff)·C_rpc` term as a steering contribution and
choose B_eff (with C_rpc) by the plan-choice acceptance test (§7), documenting it as
such. T0 (per-statement fixed overhead) is REAL in latency but has NO env mapping —
it is measured-and-subtracted, then DISCARDED (§5), never folded into any C_*.

## 2. Instrumentation & the measured n-vector

`ENABLE_RPC_TRACE` → `summary_by_type{TYPE:{n,us,req_b,resp_b}}` (rpc_trace.cc:
241–340). Under prefetch ON the data-path n-vector is dominated by ONE
`TX_EXECUTE_READ_PLAN` whose `resp_b` is the bytes term and whose `us`+end-to-end
latency carry the server-side CPU. The full MessageType taxonomy (rpc_trace.cc:
30–99) and which classes each regime emits are tabulated in
docs/phase22_m2b_taxonomy.md. Timing: `bench/bin/tpch_matrix.sh` (suite),
`scripts/dev/m2b_taxonomy.sh` (probes). Fit: scipy `nnls`, `numpy.linalg.cond` on a
COLUMN-STANDARDIZED matrix, VIF=1/(1−R²_j) by per-column OLS, CIs by jackknife over
DESIGN POINTS (not only reps) + bootstrap over reps for pure-error.

## 3. Calibration target: prefetch-ON end-to-end latency vs KNOWN cardinality

y = end-to-end statement latency (median over ≥15 reps, 2 warm dropped; report
median+p95+inter-rep CV), measured **trace OFF** in the **deployment regime
(prefetch ON)**; n = analytically-known counts from probe construction
(COUNT(*)-exact tables), cross-checked per RPC-type against a ≥3-rep trace pass
(trace `us` used for NOTHING in the fit — only n/bytes, which are instrumentation-
invariant). Governor=performance + C6 off (cstate_guard) recorded in provenance.

## 4. Bench battery (prefetch ON, latency-based, one coefficient each)

Probe tables (Wu calibration queries, exact COUNT(*)): `cal_n(pk BIGINT PK, k INT,
KEY sk(k))` and `cal_w(pk BIGINT PK, k INT, pad VARCHAR(512), KEY sk(k))`, sizes
N∈{1e3,1e4,1e5,1e6}; `k` gives known range cardinality R. ALL probes verified by
BOTH `EXPLAIN` AND the trace's realised RPC classes (reject the row unless the
expected class fired and unexpected ones — agg/GS in summary_local_view — did not).
AGG_PUSHDOWN/SEMIJOIN OFF for the isolating probes (documented); the suite
acceptance (§7) runs them ON.

| # | coeff | probe (prefetch ON, forced+trace-verified) | swept | isolation |
|---|---|---|---|---|
| a | `C_rpc` (+T0) | one staged scan returning ~1 row (tiny range) | — | intercept of latency at ~0 work = T0 + one-staged-RPC fixed cost |
| b | `C_byte` | full scan of fixed-N cal_w, project narrow vs wide | width W (resp_b) | fixed N, one staged RPC; Δlatency/Δresp_b. Fit against the MODEL's bytes basis (rows·floored mean_rec_length) AND raw resp_b; report both |
| c | `S_scan` | full scan cal_n, narrow | N∈{1e3..1e6} | Δlatency/ΔN minus byte term = server scan CPU/row |
| d | `S_ref` | covering range `SELECT k … BETWEEN` (index-only, trace: no base-row fetch) | R | Δlatency/ΔR minus byte = covering-range server CPU/row |
| e | `C_mat` | **non-covering range on the SAME cal_w, SAME k-range, projection-only diff** (`SELECT k,pad` vs `SELECT k`) | R | (e_latency − d_latency)/R = server per-row materialise CPU. Prelim ≈0.5 µs/row. SAME table/index/range — only the projected non-covering column differs (reviewer fix) |
| f | per-row regime | (secondary, prefetch OFF) eq_ref join → R×TX_READ | R | the FALLBACK per-row cost, for the prefetch-OFF cost path + a physical cross-check of C_mat in true per-row execution |

T0 isolation (a): run a SELECT-1 / 1-row point-read baseline at high rep, take
median as T0_hat with bootstrap CI, SUBTRACT from every y before the marginal fit;
the residual single-staged-RPC intercept = C_rpc. C_rpc collinearity with T0 (both
reviewers BLOCKING) is thereby removed by construction, not fit.

## 5. Fit (identifiable composites; anchor as definition; gated)

1. Subtract T0_hat from all y. NNLS on the column set the code multiplies
   (`[bytes, n_scanrow, n_refrow, n_matrow]`; the staged-RPC fixed term is the T0-
   removed intercept = C_rpc). Also run UNCONSTRAINED OLS; flag any coefficient
   negative or within 1 CI of 0 as near-boundary (NNLS-clip bias warning).
2. Impose the anchor as DEFINITION (no rescale): C_row:=0.10, C_remote:=max(0,
   S_scan−0.10), C_probe:=max(0,S_ref−0.10). Report identifiable composites
   (C_rpc,C_byte,S_scan,S_ref,C_mat) with jackknife-over-design-point CIs.
3. Identifiability gates on the COLUMN-STANDARDIZED design matrix: report cond(),
   per-column VIF, and per-RPC-type measured-vs-analytical n (must match the
   designed INTEGER exactly, control RPCs excluded). Reject+redesign any probe
   whose target coefficient has VIF>10 or a CI spanning an order of magnitude.
4. engine_cost SEPARATELY (eq_ref micro-bench), NOT in this NNLS (methodology BI-7).

## 6. Scale-invariance (three tiers; honest about in-memory residency)

- **Linearity / no-residency-cliff (in-session):** the N∈{1e3..1e6} sweep — refit
  on {1e3,1e4} vs {1e5,1e6}; require CI overlap. Plot residuals vs N/bytes; if
  S_scan or C_mat drift, attribute to CPU-cache/NUMA/scan-thread saturation/OCC
  working-set (NOT dismiss as noise) and report the working-set size of the knee.
  (The "fully in-memory ⇒ no residency regime" claim is SOFTENED per reviewers:
  no buffer-pool DISK spill, but in-memory residency regimes remain and ARE what
  the SF re-fit tests. Pin server thread count + NUMA in provenance.)
- **Scale-invariance (primary):** re-run the §4 battery at TPC-H SF0.1 and SF1
  (SF3 if time); require fitted constants stable within jackknife CI.
- **Headline (decision-relevant):** hold N, sweep selectivity R/N across the
  crossover; report s* = the measured selectivity where range latency = full-scan+
  prefetch latency IN DEPLOYMENT (agg ON) — the dimensionless, SF-independent
  quantity the paper should report instead of "8.0". Verify the calibrated model
  predicts s* and that s* is invariant across SF.

## 7. Acceptance — THE SUITE DECIDES (the decisive experiment)

Plug the physically-derived constants into the env knobs; run the **full 22-query
suite, fullidx SF1, full deployment (prefetch+agg-pushdown+semijoin ON)**, back-to-
back same-session vs the M5 baseline (governor/C6 pinned; relative delta, never a
stored cross-session number — reviewer). Outcomes:

- **PASS** (suite ≤ M5 baseline 31.75 s ±noise, md5 22/22 vs InnoDB:3308): the
  physically-grounded constants reproduce the M1/M2a win → 8.0 (or its physical
  replacement) is "right for the right reason." Ship with provenance.
- **FAIL** (physical constants regress the suite): this is itself a *result* —
  C_materialise is an irreducible steering parameter, not a hardware constant.
  Quantify the gap (which queries flip, by how much), attribute it to the un-modeled
  downstream context (agg-pushdown scan-collapse / join NLJ), and EITHER keep 8.0 as
  a documented, plan-choice-calibrated steering default OR (out of M2b scope) propose
  restructuring the cost model to model that context. Do NOT ship a regressing value.

Per-constant robustness (replaces v1's vacuous ±50% band): for each constant sweep
until the FIRST plan flip in the regressor set (q3/q5/q7/q8) and report the distance
to that edge (plan-stability margin); + leave-one-query-out CV + 2–10× prefix_rowcount
/rec_per_key perturbation that must not flip scan-vs-NLJ. Reconcile the M1 [8,10] vs
M2a "16-OK" band move (the gate changed the band — document it).

## 8. Operational plan
1. Build probe generator; load cal_n/cal_w (1e3..1e6) into the live server.
2. Bench (§4) prefetch ON, trace-verified n; `m2b_fit.py` → constants+CIs+cond/VIF.
3. Scale-invariance tiers (§6): in-session sweep now; SF0.1/SF1 re-fit needs server
   reloads (server-only, mysqld stays); s* crossover under agg ON.
4. Acceptance (§7): reload fullidx SF1 (server SF1 + 23-idx backfill ~30s+~30min)
   once; env-sweep via mysqld restart only; back-to-back vs M5.
5. engine_cost eq_ref bench; decide lever (keep gated so it can't flip q8); re-confirm.
6. Findings + provenance; update constants ONLY where §7 PASSES; commit on branch.

## 9. Provenance (per shipped constant)
date, host CPU/RAM, governor/C-state, server thread count + NUMA, server git SHA +
build flags, RPC codec, prefetch regime, SF, fitted value, jackknife CI, cond/VIF,
plan-stability margin, originating probe id. Re-derive on config/record-layout
change; NEVER per-SF.

## 10. Residual risks the reviewers must still pressure-test
1. Is letting "the suite decide" (§7) a sound substitute for a clean per-constant
   physical derivation, or does it re-introduce over-fitting to the 22 queries?
2. C_mat measured standalone (§4e) vs applied only in the join/non-grouped regime
   (M2a gate, ha_lineairdb.cc:4117) — is the per-row server materialise cost the
   SAME in a join (per-driver-row) as in a standalone range? (§4f cross-checks.)
3. The `ceil/C_rpc` steering-proxy framing — is documenting it honest enough for a
   paper, or must the model be restructured to remove the fiction before publishing?
4. Synthetic probe row layout → mean_rec_length → bytes term: validate ≥1 constant
   (C_byte or S_scan) on a known-COUNT(*) real TPC-H table (mandatory, not optional).
5. Anything that makes a "physical" constant non-scale-invariant that §6 misses.
