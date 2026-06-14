# Phase 22 — M0 findings (reproduce + instrument + call-target proof)

_Read-only milestone. No cost code changed. Driver: `scripts/dev/m0_callsite_probe.sh`, raw log `/tmp/m0_run.log`._

## Environment (asserted)

- mysqld env: `HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_OPT_STATS=1` (live `/proc/<pid>/environ` confirmed).
- server: `HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1`. prefetch_execution=ON, prefetch_ro_novalidate=ON.
- Data: **SF0.1** (lineitem 600,572 rows). `hypergraph_optimizer=off` (classic planner governs — matches the design's load-bearing assumption).
- **optimizer_switch reset to MySQL stock for the baseline**: `mrr_cost_based=on, batched_key_access=off` (the running instance had leftover `mrr_cost_based=off, batched_key_access=on` from a prior session; those two are Phase-22 *levers*, so the pre-M1 baseline must not pre-apply them).
- `mysql.engine_cost`: only the two `default`-engine rows seeded; **no `LINEAIRDB` row** → the engine prices `page_read_cost` at the default `io_block_read_cost=1.0` / `memory_block_read_cost=0.25` (RPC-cheap), exactly as the diagnosis predicts.

## Headline: the regression does NOT reproduce at SF0.1 — it is SF1-specific

| config | OK-sum (22q) | notes |
|---|---|---|
| noidx (PK only) | **68.33s** | dominated by q17 **TIMEOUT(120s)** + q20 58.21s + q9 4.04s |
| fullidx (std 23-index) | **6.53s** | all 22 OK |

At SF0.1 the standard index set is a **large net win**, not a regression. The noidx losers (q17, q20, q9) are correlated-subquery / heavy queries that *need* an index regardless of disaggregation. The only SF0.1 regressors are **q18 0.23→1.92s (8.3×)** and q12 0.08→0.13s.

This contradicts the documented **SF1** result (Phase 21: suite 39→77s; q18 2.4→24s; q3/q5/q7/q8 regress; q21 improves via SIP). Reconciliation: the per-row NLJ penalty **scales with N**. At SF0.1, N is small and — critically — the optimizer picks **Index RANGE scans** (e.g. `o_od` on orderdate), not per-row ref-NLJ, for q3/q5/q7/q8. So there is *nothing to flip* at SF0.1 for those four. The pathology is a **scale-dependent crossover** that manifests at SF1.

### Plan-shape diff (noidx → fullidx, SF0.1)
- q3/q5/q8/q10: `Table scan on orders` → `Index range scan on orders using o_od` (orderdate range — legitimate, selective).
- q7: full scan → `Index range scan on lineitem using l_sd` (shipdate) + PRIMARY single-row lookups.
- q18: `Index scan on lineitem using PRIMARY` → `Index scan on lineitem using l_ok` (the one query that regresses at SF0.1).

**Implication:** Phase 22 cannot iterate purely at SF0.1 for this issue. **q18 is the only SF0.1-reproducible regressor → use it as the fast SF0.1 smoke signal; full validation must be at SF1.** The design's M1 gate ("SF0.1: q3/q5/q7/q8/q18 flip away from per-row ref-NLJ") is re-specified accordingly (see Adjustments).

## Call-target sentinel — PASS (validates the lever + the BI-2 delegation premise)

Raising the `LINEAIRDB` `engine_cost` `io_block_read_cost`/`memory_block_read_cost` from 1.0/0.25 → **50/50** (pure SQL, `FLUSH OPTIMIZER_COSTS`, reconnect) flipped **q3, q5, q7, q8, q10, q18** from `{Single-row index lookup … using PRIMARY} + {Nested loop inner join}` → `{Table scan on …} + {Hash}`.

This empirically confirms:
1. `engine_cost` **reaches the probe pricing** for both eq_ref and (non-covering) secondary ref — i.e. the design's **BI-2 delegation premise** (default `handler::page_read_cost` delegates to the same `engine_cost` constants) holds *in this build*.
2. `engine_cost` is an **effective, no-code lever** to steer NLJ→hash. This is the M1 mechanism, validated.

(Full per-site *orthogonality* — engine_cost moving only eq_ref while a `page_read_cost` override governs secondary ref — remains an M4 proof; M0 only establishes that the lever reaches the sites.)

## Cardinality health — 3–10× underestimates present

`EXPLAIN ANALYZE` (prefetch OFF so per-row execution yields true actual rows; plan unchanged):
- **q3**: est 750 vs actual 3321 (4.4×); est 1500 vs actual 15224 (**10×**); est 15000 vs actual 72678 (4.8×).
- **q10**: est 3750 vs actual 11439 (3×); est 7500 vs actual 5677 (ok).
- **q7**: EXPLAIN ANALYZE **timed out** under prefetch-OFF (per-row RPC too slow at SF0.1) — need a different actual-row capture for heavy queries (e.g. instrumented row counters, or measure at lower scale).

ANALYZE TABLE histograms already run (via `prewarm_stats.sh`) yet estimates remain 3–10× low — TPC-H correlated predicates are classically hard. Open risk per Leis (cost=guardrail, cardinality=steering): a 10× underestimate could defeat the cost fix for q3-class queries. Needs an explicit decision (column histograms vs. accept-because-relative-ranking-preserved) — referred to Codex.

## Codex consult (decision input — confirmed)

Codex independently reached the same read and added specifics (it reasoned from the facts/log; its local sandbox couldn't open files):
1. **Scale-dependent crossover — accept, but keep alternatives alive**: the stock `optimizer_switch` reset, SF0.1 range-access masking, and cardinality-steering are not yet excluded. Classify q3/q5/q7/q8 as **SF1-only regressors until proven**, q18 as **cross-scale**.
2. **q18 = valid smoke, not a sufficient gate**: optimizing q18 alone risks missing the q3/q5/q7/q8 crossover. Fast inner loop = SF0.1 q18; **required** loop = SF1 EXPLAIN for q3/q5/q7/q8/q18 after every meaningful change; timed validation = indexed SF1 regressors only; optional SF0.3/0.5 midpoint only if it actually reproduces the bad shapes.
3. **Never run full SF1 noidx** (q17/q20 correlated subqueries → 6M-row repeated scans → WSL OOM). Safe SF1 = **plan-only EXPLAIN for the noidx hazard set (q17/q20)** + **timed indexed regressors** + log `optimizer_switch` every run + memory watch with kill-on-threshold + fresh client session per query.
4. **Extend M0 before M1**; re-spec M1 gate (below). Keep the engine_cost sentinel as the reachability proof.
5. **10× cardinality underestimate is a threat, not fatal**: the sentinel flip shows relative ranking is still movable by cost. Core fix = make per-row remote probe cost scale with **RPCs+bytes** (not the fixed `ranges*(50/1024)` batch proxy) so it dominates plausible 3–10× cardinality error; histograms (on `o_orderdate`/`l_shipdate`/`c_mktsegment`-class predicates) are a **minimal assist**, not the primary fix.

## Adjustments to the Phase-22 plan (confirmed)

1. **Extend M0** with an OOM-safe **SF1 plan-shape capture** for q3/q5/q7/q8/q10/q18 (baseline + engine_cost sentinel) — EXPLAIN only is no-execution/no-OOM — to confirm the hostile per-row ref/eq_ref-NLJ pattern is the real SF1 target. Time only the indexed regressors at SF1; never execute q17/q20 noidx.
2. **Re-specified M1 gate**: "**q18 flips at SF0.1** (fast smoke) **AND q3/q5/q7/q8/q18 flip toward scan/hash / reduce RPC-probe count at SF1**; no SF0.1 regression gate on the index-needers q17/q20 (noidx is catastrophically worse there, so any index is a win)."
3. **Cardinality-health precondition** into M2: q3/q10 (3–10× under) are the canonical stress cases; the cost fix must hold the scan-vs-NLJ ranking under a 2–10× perturbation regardless.
4. **Over-correction watch** (engine_cost is coarse — moves all probe access together): explicitly verify the lever does not push the index-needers (q17/q20/q9) off their required indexes. (Probe `scripts/dev/m0_lever_overcorrect.sh`.)

### Over-correction probe result (SF0.1, engine_cost sweep none/2/5/50) — two findings

- **No over-correction at SF0.1**: the index-needers q9/q17/q20 stay fast at *every* level (q17 0.03–0.04s, q20 0.04–0.08s, q9 0.23–0.30s). Raising engine_cost shifts some small-table joins to `Table scan + Hash` (e.g. q7 n1/n2, q20 part) but the **times do not regress** — the coarseness hazard (Reviewer-2 BI-1) did not materialise at this scale. Mild de-risk for M1.
- **engine_cost does NOT fix q18** (stays 1.90s at none/2/5/50). q18's regression is **pushdown-abandonment** (lineitem secondary-index scan that loses GroupedSemijoin / agg-pushdown), i.e. an **M5 (pushdown-credit) target, not M1 (probe-pricing)**. 
- **Consequence**: at SF0.1 there is **no query the M1 engine_cost lever visibly fixes** — the q3/q5/q7/q8 regressors use range-scans (nothing to flip) and q18 is M5. Therefore **M1 must be validated at SF1**, and **q18 is the M5 smoke, not the M1 smoke**. This sharpens Codex point 3 / the M1 gate re-spec.

## SF1 characterization — the DECISIVE result (driver `scripts/dev/m0_sf1_characterize.sh`, log `/tmp/m0_sf1.log`)

Fresh SF1 stack (lineitem 6,001,215), stock optimizer_switch, standard 23-index set backfilled (30m28s), prefetch ON. Timed only the indexed regressors (joins, RSS-guarded); never executed the q17/q20 noidx OOM hazard.

**Baseline plan shapes + times (engine_cost=none):**
- q7 `range scan on lineitem using l_sd` + PRIMARY eq_ref chain → **13.24s**
- q8 `range scan on orders using o_od` + Hash + PRIMARY chain → **8.73s**
- q3 `range scan on orders using o_od` + customer PRIMARY → **6.10s**; q5 → 3.57s; q18 `Table scan on orders` + customer PRIMARY → **23.34s**

**engine_cost=50 (the M1 lever) — FAILS at SF1:** plans flip (dimension-table PRIMARY lookups → Table scan + Hash) but **times do NOT improve and q8 REGRESSES 8.73→17.78s**; q7 13.24→13.22 (flat), q18 22.83 (flat). engine_cost touches only the dimension eq_ref pricing (`Cost_model_table::page_read_cost`), not the fact-table range-scan cost — and flipping the small-table joins to hash is a net loss for q8.

**Fullscan-recovery test (driver `scripts/dev/m0_sf1_fullscan_test.sh`, log `/tmp/m0_sf1_fs.log`) — proves the mechanism:** dropping the fact-table date range-scan indexes (`l_sd`,`l_cd`,`l_rd`,`o_od`) forces full-scan+prefetch and the regressors recover **3–8×**:

| query | range-scan (date idx) | full-scan (idx dropped) | speedup |
|---|---|---|---|
| q3 | 6.17s | **1.20s** | 5.1× |
| q5 | 3.67s | **0.99s** | 3.7× |
| q7 | 13.28s | **1.74s** | 7.6× |
| q8 | 9.19s | **2.89s** | 3.2× |

## Refined diagnosis (corrected by data) & milestone reorder

**The SF1 regression is NOT primarily a non-covering-secondary-ref or eq_ref pricing problem. It is the optimizer choosing an index RANGE SCAN on the big fact tables (lineitem `l_sd`, orders `o_od`) over full-scan+prefetch.** A disaggregated index range scan materialises rows by per-row PK lookups (per-row RPC); full-scan+prefetch is 1–2 bulk RPCs. The range scan is priced through `read_cost`/`index_scan_cost` → **`helios_ref_cost`** (the `rpc = ranges*(50/1024)` fixed-batch under-charge = design CAUSE 2), which makes the range scan look cheaper than the (correctly RPC-priced) full `table_scan_cost`.

Consequences for the Phase-22 plan:
1. **The primary lever is `helios_ref_cost`, NOT `engine_cost`.** Fix `read_cost`/`index_scan_cost` (and `multi_range_read_info*`) so a **non-covering** range/ref over R rows is charged the per-row materialisation RPC cost (≈ R individual fetches unless genuinely PK-MRR batched), so full-scan+prefetch wins when R is large. This is design CAUSE 2; M0 promotes it from "secondary" to **M1**.
2. **`engine_cost` (design CAUSE 1 / the "no-code first lever") is demoted**: at SF1 it does not fix the regression and can backfire (q8 +2×). Keep it as a *secondary, careful* lever for the dimension-table eq_ref pricing only, applied after the range/ref fix and gated so it does not flip beneficial small-table joins to hash. The engine_cost *sentinel* remains valid as a reachability proof, not as the fix.
3. **q18 is a third, separate axis** (pushdown abandonment → M5), unaffected by either lever.
4. The covering-vs-non-covering split the design stresses is exactly the hinge: the fix must charge the **per-row PK materialisation** of a *non-covering* range/ref, while leaving covering index scans cheap.

## Status — M0 COMPLETE
All read-only gate items satisfied: regression reproduced & mechanism isolated (SF0.1 net-positive = scale crossover; SF1 range-scan-vs-fullscan proven by 3–8× recovery), hypergraph OFF asserted, EXPLAIN/plan-shape recorded at both scales + under the sentinel, call-target sentinel PASS (engine_cost reachability), over-correction probe (no SF0.1 harm), cardinality recorded (q3/q10 3–10× under), baselines captured, **no cost code changed**. M0 delivered a data-driven correction to the design's lever priority. Next: design-review the `helios_ref_cost` fix (revised M1) before implementing.
