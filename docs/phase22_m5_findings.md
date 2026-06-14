# Phase 22 — M5: q18 GS reactivation under indexes (the net-positive result)

_Plan-staging change (results unchanged). Dual-review GO: grounded investigation
(file:line-verified) + Codex (GO). Recovers the single biggest regressor and
takes the standard index set NET-POSITIVE vs noidx._

## Root cause

q18's IN-subquery (`SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING
SUM(l_quantity)>300`, no WHERE) is the case Helios's **GroupedSummary (GS)**
mechanism is built for (serve ~57 synthetic group rows instead of scanning 6M).
Under the standard index set the optimizer compiles the subquery as a full
ordered `Index scan on lineitem using l_ok` (l_ok = the l_orderkey index, ordered
= no sort for GROUP BY) — cost-equal to a PRIMARY scan (~3.32e6) so the optimizer
picks the secondary. **GS only activated for a PRIMARY full scan**: the GS-skip
pass (`lineairdb_autogen.cc`) dropped a leaf's staged scan step (handing it to GS)
only if `index_name.empty()`. That clause (commit ceb5a50) was added to protect
q15 (whose view aggregates over an `l_shipdate` RANGE — a keyed step that must
stay staged), but it was over-broad: it also kept q18's full l_ok secondary scan
staged → the literal 6M-row scan ran → **24.66s**.

Empirical proof: forcing the subquery to PRIMARY (`IGNORE INDEX`) → `Index scan
using PRIMARY` → **3.07s** (same EXPLAIN structure, GS activates).

## The fix (Option B — extend GS-skip; Option A rejected)

A full ordered `INDEX_SCAN` for GROUP BY is **not** costed via the MRR trio
(`table_scan_cost`/`read_cost`/`index_scan_cost`) — it uses the deprecated
`read_time()` which returns the same value for PK and secondary, so a cost lever
(Option A) cannot see l_ok is non-covering, and the M1 materialise charge +M2a
gate are unreachable/exempt for this shape. Option B is surgical and re-enables
the GS path the code already serves.

In the GS-skip predicate (`lineairdb_autogen.cc`), replace `index_name.empty()`
with a **full-scan-shape** test:

```cpp
steps[i].key_prefix.empty() &&
steps[i].end_key_prefix == lineairdb_keyenc::scan_end_sentinel() &&
steps[i].serialized_filter.empty() &&
```

A full scan (PRIMARY or secondary) compiles to `key_prefix.clear()` +
`end_key_prefix = scan_end_sentinel()` (`lineairdb_autogen.cc:734-735`) and is
driven by `index_first`/`rnd` with `key==nullptr` → `gs_fill_buffers` serves it.
A keyed/range secondary scan (q15's l_sd: non-empty `key_prefix` / non-sentinel
end) is driven with `key!=nullptr` → never takes the GS branch → must stay staged.
The per-alias `gs_registration` guard is unchanged.

**Correctness:** `gs_fill_buffers` aggregates the FULL leaf under `reg->filter`
(the server-side WHERE), NOT the index range. q18's inner has no WHERE so
`reg->filter` is empty → GS over the full table yields the identical `l_orderkey`
group set the dropped l_ok scan would have. md5-safe by construction.

## Result (SF1, live stack, default kC_materialise=8)

- **q18: 24.66s → 2.94s** (8.4×). EXPLAIN still shows `Index scan using l_ok`
  (the plan is unchanged; GS-skip is execution-layer).
- **md5 == forced-PRIMARY known-correct** (`d41e45ddb08a8ac3a5e90dc1bfbde965`).
- **Non-regression:** q15=1.48, q1=0.78, q6=0.40 (unchanged). q1 (2 GROUP-BY
  cols → GS not registered) and q6 (no GROUP BY) are structurally untouched;
  q15's range stays staged.
- **Full 22-suite: 22/22 OK, 31.65s.**

## 🎯 The Phase-22 thesis lands: standard indexes are now NET-POSITIVE

| config | SF1 suite | vs noidx |
|---|---|---|
| noidx (Phase 21) | 39.14s | — |
| fullidx **unfixed** (Phase 21) | 77.19s | **+97% (2× regression)** |
| fullidx + M1 | 55.01s | +41% |
| fullidx + M1+M2a | 52.11s | +33% |
| **fullidx + M1+M2a+M5** | **31.65s** | **−19% (net-POSITIVE)** |

The standard secondary-index set went from a 2× regression to a **net win** vs
no indexes on the disaggregated engine — the Phase-22 goal. (The 39.14s noidx
baseline is the Phase-21 measurement; re-verify in-session for the paper with the
OOM-safe protocol since q17/q20 are correlated-subquery hazards at SF1 noidx.)

## Status
M5 COMPLETE. Remaining Phase-22: **M2b** (rigorous NNLS calibration + scale-
invariance at ≥2 SFs — the methodological rigor for the paper; perf is already
here), the **engine_cost** secondary lever, and the **HELIOS_COST_V2 default-ON**
finalization milestone.
