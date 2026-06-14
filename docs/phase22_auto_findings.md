# Phase 22 — default-ON attempt 2 (AUTO gate): also blocked; the real blocker is cardinality

_Follows docs/phase22_default_on_findings.md (a naive global default-ON = ~32× TPC-C
regression). Codex recommended an AUTO gate (apply V2 only to analytical-shaped
statements + a table-scan size floor). Implemented + validated: it FIXES OLTP but
REGRESSES TPC-H 41%, because no simple size/shape signal separates TPC-H's small-table
analytical scans from OLTP. The re-diagnosis shows the OLTP regression's true root cause
is a COMPOSITE-CARDINALITY mis-estimate — the same lever as the cardinality milestone.
The AUTO-gate code was REVERTED; COST_V2 stays opt-in (the validated-safe state)._

## 1. The AUTO gate (built, then reverted)

`helios_auto_v2_applies()` (SELECT-only, non-locking, prefetch-eligible, analytical
shape: grouped/distinct/window/olap) gating the V2 overrides, PLUS a table-scan size
floor (`stats.records >= HELIOS_CHEAP_SCAN_MIN`, default 1e6); COST_V2 + OPT_STATS
flipped default-ON.

## 2. Result — fixes OLTP, but REGRESSES TPC-H 41%

| workload | metric | un-gated bundle | **AUTO gate** | target |
|---|---|---|---|---|
| **TPC-C OLTP** | throughput | 43.5 req/s (32× regress) | **1279 req/s (−2.6% vs stock 1314, retries identical)** | ✅ parity |
| **TPC-H** | suite (md5 22/22) | 31.75 s (net-positive) | **44.67 s (+41% REGRESS)** | ❌ |

TPC-H regressors under the gate (per-query): **q9 10.21 s** (was ~3.49) and **q3 4.91 s**
(was ~1.15) dominate. Cause: the **table-scan size floor** turns OFF the V2 cheap-scan
for `partsupp` (800 k), `part` (200 k), `customer` (150 k) — all < 1 M — which q9/q3
need; but `order_line` (427 k, the OLTP offender) is in the SAME size band, so no size
threshold separates them. The shape gate doesn't help either (both TPC-H q9/q3 and the
TPC-C stock-level join are grouped/aggregate). **Simple size/shape signals cannot
separate TPC-H analytical from OLTP** here.

## 3. Re-diagnosis — the OLTP regression is a CARDINALITY mis-estimate, not a cost bug

The TPC-C stock-level query's `order_line` access is `ol_w_id=1 AND ol_d_id=1 AND ol_o_id
BETWEEN 80 AND 100` — an EQUALITY-PREFIX (w_id,d_id) + trailing RANGE (ol_o_id).
`records_in_range` estimates this via `rec_per_key[eq_parts] / 2` =
(427 k rows / NDV(w_id,d_id)=10) / 2 ≈ **21 350** — it takes the whole district's rows
and halves it, IGNORING the `ol_o_id` range selectivity. The ACTUAL is ~20 rows
(~20 orders × ~10 lines). So the range is over-estimated **~1000×**; the cost model then
correctly concludes "a 21 k-row range is not obviously cheaper than a 427 k scan" and
(with V2's cheap scan) flips to `ALL`. **With an accurate ~20-row estimate the range is
trivially cheapest and V2 keeps it** — no gate needed.

This is the EQUALITY-PREFIX + TRAILING-RANGE case the cardinality design explicitly
deferred (docs/phase22_card_design.md §7: the leading-column histogram does not cover a
trailing range under an equality prefix). It is the SAME lever as the cardinality
milestone (Leis: fix cardinality, not cost).

## 4. Conclusion — default-ON is blocked by composite cardinality; stays opt-in

default-ON cannot be made safe by crude cost-model gating: un-gated regresses OLTP 32×,
AUTO-gated regresses TPC-H 41%. The principled path is to **extend the range histogram
to the equality-prefix + trailing-range case** (condition the trailing-column histogram
on the eq-prefix), after which the `order_line` range is correctly cheap and COST_V2
could default-ON without harming OLTP. That is a substantial extension (deferred). Until
then, **HELIOS_COST_V2 stays OPT-IN (default OFF)** — the validated-safe state; enable it
per-deployment/session for analytical workloads (the measurement env already does).

**Milestone status:** "HELIOS_COST_V2 default-ON" is resolved as a validated NEGATIVE
result with a clear next lever (composite cardinality), consistent with the whole Phase
22 thesis that cardinality — not cost-constant/gating tweaks — is the lever. The AUTO
gate was reverted (it regresses TPC-H whenever COST_V2 is on). engine_cost remains
deprioritized (M2b: cost-constant ROI is low).
