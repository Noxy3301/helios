# Phase 22 — M2b: rigorous calibration (DESIGN v4, post-grounding, post-3-rounds-review)

_v1 (proxy-side per-RPC NNLS) NO-GO'd → measured the RPC taxonomy → v2 reframe
(prefetch-ON latency, steering-proxy, suite-decides) → v3 (broke circularity, anchor-
as-definition, T0/clip/cond fixes). v3 round-3 review: Codex GO, statistical lens GO,
**confounds lens NO-GO on a code-grounded BLOCKING** — v3's PK-join C_mat anchor is
priced by un-overridden page_read_cost, so it never exercises read_cost()/C_mat;
also the pivotal probe used an implicitly-grouped aggregate (gate SKIPS it). v4 fixes
both: C_mat anchor = standalone NON-GROUPED non-covering SECONDARY RANGE (the ONLY
access class read_cost governs, §4-note), pivotal probe forced non-grouped, plus the
statistical/Codex MINOR+wording items. Status: **draft for confounds-lens
re-confirmation.**_

## 0. Reframe (unchanged from v2, validated by review) + the central thesis

Measured (docs/phase22_m2b_taxonomy.md, scripts/dev/m2b_taxonomy.sh): under
**prefetch ON (deployment) every access class issues ONE `TX_EXECUTE_READ_PLAN`**;
per-row `TX_READ` is prefetch-OFF-only (eq_ref join: 503 TX_READ OFF vs 1 staged
RPC ON). Pivotal sweep: 2ary range ~linear ~4 µs/row, beats the flat 19.2 s full
scan to ~70% selectivity; per-row server materialise ≈ 0.5 µs/row. So the read_cost
`ceil(rows/B_eff)·C_rpc` IO term counts NO real RPC in deployment (a STEERING PROXY)
and `C_materialise` is entangled with downstream context (agg-pushdown collapses a
scan's transfer; a join inflates a range via NLJ).

**Central thesis (promoted from v2 §10 to the headline, per reviewers):** in a
disaggregated engine the per-row remote-materialisation cost is **context-dependent
on the execution alternative**, so it cannot be a single hardware constant; Helios
prices it via an **executor-coupled gate** (M2a, helios_charge_materialise()), and
the paper's evidence is the **gap between the measured physical slope (~0.5 µs/row)
and the steering magnitude (8.0 cost-units)**. M2b's job is to (i) MEASURE the
physical slopes that ARE scale-invariant, (ii) DERIVE the steering parameter from
those physical slopes (not from the suite), and (iii) use the 22-suite ONLY as a
pre-registered HOLD-OUT confirmation. PASS ⇒ physics reproduces the win and the
calibrated model is VALIDATED; FAIL ⇒ a NEGATIVE calibration result that validates
ONLY the context-dependence finding (the physical ~0.5 µs/row slope does NOT explain
the production steering need) — NOT a validated cost model. Both are publishable; we
state which is being claimed in each branch (Codex r3 #9).

## 1. Model form (VERIFIED ha_lineairdb.hh:336–417)

```
full scan   (table_scan_cost):  io = C_rpc·1              + bytes·C_byte
                                cpu = N·(C_row + C_remote)
covering ref(index_scan_cost):  io = (ranges|1)·(C_rpc/B_eff) + bytes·C_byte
  = helios_ref_cost             cpu = rows·(C_probe + C_row)
non-cov ref (read_cost):        helios_ref_cost
  (charge-materialise gate ON)  + io: ceil(rows/B_eff)·C_rpc   ← STEERING PROXY (not a real RPC count under prefetch ON)
                                + cpu: rows·C_materialise        ← server per-row materialise CPU
```
`total_cost()=io+cpu+import`, equal-weighted (VERIFIED handler.h:3717); `multiply()`
scales all three uniformly (:3746). **Because total_cost sums io+cpu+import 1:1, the
assignment of a fitted latency slope to the io vs cpu bucket is cost-neutral at plan
time; we keep the code's existing buckets for readability and the fit targets
total_cost** (closes review item B3). Uniform µs→cost-unit scaling is therefore
plan-invariant in isolation; commensurability with MySQL's NON-rescalable
ROW_EVALUATE_COST=0.10 (VERIFIED opt_costconstants) is what the anchor pins.

### 1.1 Identifiable quantities + env mapping (anchor as definition, no rescale)

Only the SUMS S_scan=C_row+C_remote, S_ref=C_probe+C_row are data-constrained;
C_row/C_remote/C_probe are individually non-identifiable. We IMPOSE the anchor as a
pure partition: **C_row := 0.10; C_remote := S_scan − 0.10; C_probe := S_ref −
0.10**, with the clip handled as a FALSIFICATION TRIP-WIRE (§5.3), not silent
max(0,·). Substituting these reproduces the fitted S_scan/S_ref exactly.

| fit symbol | physical meaning (prefetch-ON, server-side) | env mapping |
|---|---|---|
| `C_rpc`  | measured fixed cost of one staged TX_EXECUTE_READ_PLAN | `HELIOS_C_RPC` (physically frozen, §5.1) |
| `C_byte` | per byte the optimizer prices = per `rows·floored mean_rec_length` (§4b) | `HELIOS_C_BYTE` |
| `S_scan` | full-scan server CPU/row (≈3.2 µs/row prelim) | `C_REMOTE=S_scan−0.10` |
| `S_ref`  | covering-range server CPU/row | `C_PROBE=S_ref−0.10` |
| `C_mat`  | per-row server materialise CPU **in the JOIN regime it governs** (§4e′) | `HELIOS_C_MATERIALISE` |
| `B_eff`  | the SINGLE steering DoF — set so the proxy term reproduces the **physically-derived** s*_access (§5.4), NOT tuned on the suite | `HELIOS_BATCH` |

Two-constant honesty (review E/Codex#6): `C_rpc` appears as BOTH the measured
one-staged-RPC overhead (physical) AND inside the read_cost steering term. We report
them as two CONCEPTUAL quantities — `C_rpc_phys` (measured, §4a) and the optimizer
penalty magnitude `P_steer = C_rpc/B_eff per row` (derived, §5.4) — even though the
code reuses one knob.

## 2. Instrumentation & measured n-vector
`ENABLE_RPC_TRACE` → `summary_by_type{TYPE:{n,us,req_b,resp_b}}` (rpc_trace.cc:
241–340). Under prefetch ON the data path is one TX_EXECUTE_READ_PLAN; resp_b = the
byte term; us+latency = server CPU. **Realised-class trace gate caveat (review,
confounds MAJOR):** under prefetch ON the trace CANNOT distinguish covering (d) from
non-covering (e) — both are 1× TX_EXECUTE_READ_PLAN. So the realised-class trace
gate validates only scan-vs-range-vs-point; **covering-vs-non-covering is gated by
EXPLAIN `Extra` (`Using index`) AND a prefetch-OFF trace pass** (where P2 emits
GET_MATCHING_PRIMARY_KEYS_IN_RANGE-only and P3 adds TX_BATCH_READ — taxonomy:19-20).
Fit: scipy `nnls`; `numpy.linalg.cond` + Belsley condition-index on the COLUMN-
STANDARDIZED matrix (gate: cond<30 AND VIF<10); CIs by jackknife-over-design-points
+ bootstrap-over-reps.

## 3. Calibration target
y = end-to-end latency (median over ≥15 reps, 2 warm dropped; report median+p95+CV),
trace OFF, prefetch ON; n = analytically-known COUNT(*)-exact counts, cross-checked
per RPC-type on a ≥3-rep trace pass (trace `us` used for NOTHING in the fit).
governor=performance + C6 off, recorded in provenance.

## 4. Bench battery (prefetch ON, latency-based)

Probe tables (exact COUNT(*)): `cal_n(pk BIGINT PK, k INT, KEY sk(k))`,
`cal_w(pk BIGINT PK, k INT, pad VARCHAR(512), KEY sk(k))`, N∈{1e3,1e4,1e5,1e6}; `k`
gives known range cardinality R. Probes verified by EXPLAIN + the §2 trace gate.

| # | coeff | probe (prefetch ON) | swept | isolation |
|---|---|---|---|---|
| a | `C_rpc_phys` (+T0) | T0 baseline = `SELECT 1` (NO table, trace shows ZERO TX_EXECUTE_READ_PLAN); then a 1-row staged point read (exactly one TX_EXECUTE_READ_PLAN) | — | C_rpc_phys = (1-row staged latency − T0_hat). T0 issues no data-path RPC (review confounds MINOR) |
| b | `C_byte` | full scan cal_w, project narrow vs wide | resp_b | fit/ship against the CODE's basis `rows·floored mean_rec_length`; raw resp_b is a DIAGNOSTIC only (review: code only ever multiplies the floored basis) |
| c | `S_scan` | full scan cal_n narrow | N | Δlatency/ΔN − byte term |
| d | `S_ref` | covering range `SELECT k … BETWEEN` | R | index-only (EXPLAIN+OFF-trace gated); Δlatency/ΔR − byte |
| **e** | `C_mat` **(PRIMARY)** | **standalone NON-GROUPED non-covering SECONDARY RANGE** `SELECT pad FROM cal_w FORCE INDEX(sk) WHERE k BETWEEN a AND b` (no aggregate/GROUP BY → is_grouped()=false → gate CHARGES, ha_lineairdb.cc:4117); swept R | R | (e_latency − d_latency)/R = per-row server materialise CPU. **MANDATORY optimizer_trace verification that the inner cost is computed by read_cost()/helios_ref_cost (the range/MRR path), NOT page_read_cost** — see §4-note. This is the C_mat anchor. |
| e′ | `C_mat` cross-checks | (1) the ACTUAL q3/q5/q7/q8 regressor access (optimizer_trace: confirm it is the SAME non-covering secondary RANGE→read_cost path); (2) prefetch-OFF execution (per-row TX_READ, 503-style) | R | reconcile A(standalone staged) vs B(regressor staged) vs C(prefetch-OFF per-row). Agreement ⇒ hardware constant; disagreement IS the context-dependence result (§7/§0 thesis) |
| f | regime check | re-run c/d/e with AGG_PUSHDOWN+SEMIJOIN ON | — | slopes must be within jackknife CI of the OFF fit; else constants are "regime-dependent," not "irreducible" (review confounds MAJOR) |

**§4-note — where read_cost/C_mat actually fires (code-grounded, review r3 confounds
BLOCKING):** the optimizer reaches the overridden `read_cost()` (hence the C_mat
charge) ONLY for a NON-COVERING SECONDARY **RANGE** access (range optimizer → MRR
→ read_cost). It does NOT fire for a `ref`/`eq_ref` lookup: `find_cost_for_ref`
(sql_planner.cc:157–163) routes a ref to `read_cost()` only when
`keyno==primary_key && primary_key_is_clustered()`, but the proxy never overrides
`primary_key_is_clustered()` (default false, handler.h:5875), so PK and secondary
ref lookups fall to `page_read_cost` — which Helios deliberately does NOT override
(hh:420–424). Therefore (i) the C_mat probe MUST be a non-covering secondary RANGE,
not a PK/secondary join-ref (v3's PK-join anchor was wrong); (ii) `C_materialise`
governs exactly the non-covering-secondary-range access class and nothing else —
state this scope precisely in the paper.

§4e paired measurement (review statistical MINOR): e and its covering counterpart
are measured as INTERLEAVED paired reps; C_mat's CI = bootstrap over per-rep
DIFFERENCES (cancels shared T0/RPC variance), report paired-difference CV.

**§4f predeclaration (Codex r3 #4 + review r3 statistical power):** an agg/semijoin
ON-vs-OFF slope drift does NOT block shipping the gate-based approach; it only
changes the CLAIM from "irreducible hardware constant" to "regime-dependent
constant." Because the §4e C_mat CI can be wide (it is a subtraction), the §4f
"within jackknife CI" test has low power; report the §4f test's CI half-width and
treat a WIDE-CI pass as "inconclusive," not "regime-invariant" (cite the half-width
in §9 provenance).

## 5. Fit — DERIVATION stands alone; suite is hold-out (circularity broken)

1. **C_rpc_phys frozen from §4a**, S_scan/S_ref/C_byte/C_mat from §4 micro-bench CIs
   ALONE. These ARE the paper's claim; they do not consult the suite. NNLS on the
   columns the code multiplies; ALSO unconstrained OLS — flag any coefficient <0 or
   within 1 CI of 0 as near-boundary (NNLS-clip warning). The 1-CI flag is a
   DIAGNOSTIC (~68%, weak) that feeds §5.3, NOT a ship gate; the ship gate is the
   §5.3 anchor-consistency trip-wire + the §7.1 pre-registered hold-out (review r3).
2. T0 measured-and-subtracted, then DISCARDED (no env mapping). **Propagate T0
   uncertainty** (review statistical MAJOR): bootstrap T0_hat jointly (resample the
   SELECT-1 reps inside each bootstrap iteration and re-subtract) so C_rpc_phys's CI
   includes Var(T0_hat); OR fit T0+C_rpc jointly with a 'staged-RPC-fired' indicator.
3. **Clip = falsification trip-wire:** report raw unclipped S_scan/S_ref with CI; if
   (S−0.10)<0 or its CI admits C_remote/C_probe<0, do NOT clip-and-ship — surface
   "anchor inconsistent with data," report the mis-scale, re-anchor. The clip is a
   trip-wire, not a coefficient transform.
4. **B_eff = the single steering DoF, derived from physics not the suite
   (PRE-REGISTERED equation, Codex r3 #1):** with C_rpc frozen at §4a, the
   physically-implied access crossover is the selectivity s*_access where
   range-cost(R=s*·N) = fullscan-cost, i.e. (from §4 slopes) the R solving
   `helios_ref_cost(R) + ceil(R/B_eff)·C_rpc + R·C_mat = table_scan_cost(N)`. SET
   `B_eff := the value making that equality hold at the MEASURED crossover
   R* = s*_access·N` (s*_access from §4c/§4d/§4e physical slopes, §7.0). Propagate
   the §4 slope CIs into B_eff's CI. Report the physically-implied s* and the
   proxy-implied s* SEPARATELY — they must agree WITHOUT consulting the 22-suite
   (review B/Codex#5). C_rpc stays frozen at §4a.
5. engine_cost SEPARATELY (eq_ref micro-bench), not in this NNLS.

## 6. Scale-invariance (two distinct s*, regime-separated)

- **s*_access (defensible SF-independent headline):** the agg-OFF single-table
  range-vs-fullscan crossover, which the access-cost model CAN legitimately predict
  from physical C_mat/C_byte/S_scan. Report it; verify the calibrated model predicts
  it; verify invariance across SF0.1/SF1(/SF3).
- **s*_deploy (a DIFFERENT regime, NOT predicted by the access model):** the agg-ON
  deployment crossover, governed by agg-pushdown; the 22-suite (§7) is its arbiter,
  the access-cost model is NOT asked to predict it (review scale-inv MAJOR).
- **Tiers:** (1) N∈{1e3..1e6} sweep = linearity/no-residency-cliff (residuals vs
  N/bytes; attribute any knee to CPU-cache/NUMA/scan-thread/OCC working-set, report
  the size; NOT "no residency regime" — softened); pin server threads+NUMA in
  provenance. (2) SF0.1/SF1 re-fit of §4 = primary empirical scale-invariance.

## 7. Acceptance — pivotal gate, then pre-registered hold-out

**7.0 Gating precondition (run FIRST, review r3 confounds MAJOR + r2 scale-inv/G):**
staged non-covering-range vs staged-full-scan latency at MATCHED OUTPUT cardinality,
prefetch ON. **The probe MUST be in the CHARGE regime — NON-GROUPED** (no aggregate,
no GROUP BY): recall `is_grouped()=(group_list>0 || m_agg_func_used)` (sql_lex.h:1283),
so a single-table `SUM(...)…BETWEEN` is implicitly grouped → gate SKIPS it
(ha_lineairdb.cc:4117) → it measures the wrong (un-charged) regime. (NB: the
preliminary pivotal sweep in docs/phase22_m2b_taxonomy.md used `SUM(...)` and is
therefore a SKIP-regime diagnostic only; §7.0 must re-run it non-grouped.) If
staged non-covering-range is genuinely slower per output row in the CHARGED regime,
that slope IS the physical C_mat and the design is on solid ground. If NOT slower,
the penalty is a **prefetch-OFF-fallback-only** cost and the paper says so — this
gates the entire PASS/FAIL interpretation below.

**7.1 Hold-out confirmation (pre-registered):** fix the §5 physically-derived
constants; PRE-REGISTER the PASS threshold BEFORE running — the registered baseline
is the same-session re-verification run **31.75 s** (docs/phase22_verification.md:30,
NOT the 31.65 s of phase22_m5_findings.md:63 which is a different run; cite the exact
commit), and the noise band is the §3 inter-run CV measured back-to-back (state the
numeric ±band, do not let PASS drift to the looser number — review r3 MINOR); require
md5 22/22; run the 22-query suite fullidx SF1 full-deployment
(prefetch+agg+semijoin ON), back-to-back same-session vs M5 (governor/C6 pinned,
relative delta). **If FAIL, do NOT search B_eff/C_rpc to make it pass** — report the
0.5-vs-8.0 gap as the context-dependence result (§0 thesis).

**7.2 Steering attribution (review E):** with C_mat held at its measured slope,
report what fraction of EACH regressor's (q3/q5/q7/q8) plan flip is carried by the
cpu term (C_mat) vs the io steering term (ceil/C_rpc). If the io term is load-bearing
(taxonomy predicts it is), the honest headline is the COMBINED per-row steering
magnitude — the read_cost term `ceil(rows/B_eff)·C_rpc` (i.e. per-row `C_rpc/B_eff`)
plus `C_mat` — reported as the primary lever with its own plan-stability margin
(Codex r3 #5: `B_eff×C_rpc` is dimensionally wrong; the magnitude is `C_rpc/B_eff`
per row). NB (review r3 statistical): at plan time only the SUM
(`C_mat + C_rpc/B_eff`)·rows is identifiable from plan behaviour; `C_mat`'s SEPARATE
identification rests SOLELY on the §4e physical latency slope, never on the suite.

**7.3 Robustness:** per-constant plan-stability margin (sweep each until the FIRST
regressor plan flip; report distance to edge) + leave-one-query-out CV + 2–10×
prefix_rowcount/rec_per_key perturbation that must not flip scan-vs-NLJ + an
io/cpu-apportionment perturbation (reassign the same total between buckets; must not
flip any plan — closes B3). **Also exercise the gate SKIP branch** (review cost-form
MINOR): require q15/q1/q6 to keep helios_charge_materialise()=false AND not flip
across the C_mat sweep — tests the gate predicate the methodology calls load-bearing.

## 8. Operational plan
1. probe generator; load cal_n/cal_w (1e3..1e6) into the live server.
2. §4 bench prefetch ON, trace-gated; `m2b_fit.py` → constants+CIs+cond/VIF + s*.
3. §6 scale tiers (in-session sweep now; SF0.1/SF1 re-fit = server-only reload).
4. §7.0 pivotal gate; reload fullidx SF1 (server SF1 + 23-idx backfill, once);
   §7.1 pre-registered hold-out via mysqld-restart env-sweep; §7.2/7.3.
5. engine_cost eq_ref bench; decide lever (gated, must not flip q8); re-confirm.
6. findings + provenance; update constants ONLY where §7 PASSES; commit on branch.

## 9. Provenance (per shipped constant)
date, host CPU/RAM, governor/C-state, server threads+NUMA, server SHA+flags, RPC
codec, prefetch regime, SF, fitted value, jackknife CI, cond/VIF, plan-stability
margin, originating probe id. Re-derive on config/record-layout change; never per-SF.

## 10. Residual risks for the focused re-confirmation
1. Does the §4e join-regime C_mat anchor + §4e′ A/B/C reconciliation fully close the
   "calibrated where the gate skips / applied where it charges" gap?
2. Is the §5.4 "derive B_eff from physical s*, suite is hold-out" genuinely
   non-circular, or does deriving B_eff from C_rpc (frozen) + physical slopes still
   smuggle a free knob?
3. Is the §7.0 pivotal gate the right precondition, and is the §0 PASS/FAIL thesis
   (gate + 0.5-vs-8.0 gap as the central result) defensible for publication?
4. Anything in §4f (agg ON/OFF regime-transfer check) that would still leave a
   "regime-dependent vs irreducible" ambiguity unresolved.
