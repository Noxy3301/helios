# SIMD/AVX2 filter feasibility spike (branch `helios/pax-simd-spike`)

**Date:** 2026-07-03  **Branch:** `helios/pax-simd-spike` (base = `helios/pax-qep-guided`
HEAD `d724264`).  **Status:** spike — measured, NOT merged.

## Question

`server/rpc/query_block_executor.cc` `RunScan` evaluates pushed filters
row-at-a-time via `PredicateEvaluator` over the length-prefixed byte cells of
each `PaxGroup` strip. perf attributes 12–26% of a filtered scan to that filter
evaluation, and re-parses every cell (`strtod`/`strtoll`) on every row. The known
endgame is *typed cells* as the receptacle for SIMD. This spike measures the
ceiling: build int64 typed arrays per filter column and evaluate simple
comparison filters over them with (a) a scalar typed loop and (b) an AVX2 kernel,
so we can separate the "typed" win from the "SIMD" win.

## What was built (server-only, gated by env `LDBC_SIMD`, default off)

`server/rpc/simd_scan.{hh,cc}` + a fast path in `RunScan`:

1. **Typed column arrays.** Lazily build (cached per `(PaxStore*, column)`,
   invalidated on `group_count()` change) an `int64[n_groups*8192]` per filter
   column, with a per-column *kind* detected from the visible cells:
   - **INT** — every cell a full `from_chars` int.
   - **DATE** — `YYYY-MM-DD` → `YYYYMMDD` int (string order ≡ int order, so ≡ the
     byte path's string compare).
   - **DECIMAL** with uniform scale `s` — all digits as a scaled int (`0.06`→`6`).
   A column where any visible cell fails detection (or mixed kind/scale, or empty
   /NULL) is **UNTYPED** → the scan keeps the byte path.
2. **Constant conversion, comparison-preserving.** For `col OP const`, the const
   is converted into the column's int64 domain. INT/DATE convert exactly. DECIMAL
   is the trap: the byte path compares `strtod(cell)` as a **double** against the
   folded double constant. q6's `BETWEEN '0.06'-0.01 AND '0.06'+0.01` folds to
   `[0.0499…96, 0.0699…983]`, which **excludes** discount `0.07` (double
   `0.0700…067 > 0.0699…983`). Naïve `llround(0.0699…983*100)` snaps to `7` and
   wrongly includes it (caught by the md5 gate: revenue 1193053 vs 734493). The
   fix computes the *exact* integer boundary: `f(I)=(double)I/10^s` equals
   `strtod(cell)` and is monotone, so a ±1 search around `llround(c*10^s)` finds
   the true cut point (`dec_lower` for `< / >= / BETWEEN-lo`, `dec_upper` for
   `> / <= / BETWEEN-hi`).
3. **AVX2 kernel** (`__attribute__((target("avx2")))`, guarded by
   `__builtin_cpu_supports("avx2")`, scalar fallback). Per 64-slot block: 16×
   `_mm256_cmpgt_epi64`/`cmpeq` quads → `_mm256_movemask_pd` → 64-bit match mask,
   ANDed across conjuncts, then ANDed with the visibility mask; survivors pushed
   in the same `(group, slot)` order as the byte path.

Engaged only for plain filtered scans whose whole filter is an AND of simple
comparisons over typed columns (no semi/ext key filter) — every other scan is
byte-path-identical. `LDBC_SIMD`: unset→byte path, `scalar`→typed scalar loop,
`avx2`/`1`→typed + AVX2.

## Numbers (SF=1, single stream, warm, min-of-3; Xeon Gold 6326, AVX2)

| query | baseline (byte) | typed-scalar | typed+AVX2 |
|------:|----------------:|-------------:|-----------:|
| q6    | 55 ms           | **32 ms** (−42%) | 35 ms (−36%) |
| q1    | 228 ms          | **188 ms** (−18%) | 186 ms (−18%) |

- **q6** (4 conjuncts: shipdate DATE ≥/<, discount DECIMAL BETWEEN, quantity
  DECIMAL <; ~2% selective) is filter-bound → typing helps a lot.
- **q1** (1 conjunct: shipdate ≤, ~98% pass; GROUP BY over decimal SUM/AVG of
  6M rows) is aggregation-bound → the filter is a small share; the ~40 ms saved
  is the eliminated per-row shipdate re-parse, the rest is untouched aggregation.

### Correctness

All md5 MATCH vs primary (`use_secondary_engine=OFF`) in **every** config; the q6
/q1 SEC md5s are byte-identical across baseline/scalar/avx2. Full **22/22
md5 gate MATCH at SF=1 in AVX2 mode** (every non-q6/q1 query correctly falls back
to the byte path). SF=0.01 22/22 gate also MATCH in all modes.

### Build cost & memory (deliverables)

| metric | value |
|---|---|
| typed-array build (q6, 3 cols × 6.0M rows, first scan) | **291 ms** (scalar) / 361 ms (avx2) — same code, run-to-run noise; ~100 ms/column |
| extra memory (arrays) | **137.4 MB** = 3 × 45.8 MB (int64 × 6.0M slots/col) |
| lineairdb-server RSS delta (after-load → after-queries) | baseline **0 MB**, typed-scalar/avx2 **+137 MB** (== array bytes) |

## Feasibility conclusion

**Two findings, one clear.**

1. **Typing pays; SIMD does not.** typed-scalar is *as fast or faster* than
   typed+AVX2 on both queries (q6 32 vs 35 ms, q1 188 vs 186 ms). Once the cells
   are int64, the filter is **memory-bandwidth bound** — streaming a 137 MB array
   dominates, and vectorizing the compare (plus `movemask` + bit assembly
   overhead) buys nothing; the bottleneck simply moved from CPU (per-row
   `strtod`/tree-walk) to memory. A tuned kernel might recover a few ms but cannot
   beat the memory wall. **The endgame value is "typed cells", not "SIMD over
   typed cells."**

2. **Rebuild-per-query is the wrong shape.** The 291 ms build for q6's 3 columns
   *dwarfs* the 23 ms/query it saves — it only pays off amortized across many
   queries (this spike caches per table, so runs 2-3 are free). The honest
   endgame is to make the typing **the storage format** — shred typed values at
   scatter time (the "typed cell" receptacle) so there is no build step and no
   137 MB shadow, and so OLTP writes maintain it incrementally. That is a real
   storage-layer change, not a scan-time add-on.

**Verdict for full typed-cell migration:** *worth it for the typing, on
filter-bound scans (q6-class: up to ~1.7× on the query, larger on the isolated
filter), but do it as a scatter-time typed storage format, not as a per-scan
build; and do NOT expect AVX2 to add value — a scalar typed compare captures the
entire ceiling on this bandwidth-bound workload.* SIMD would only start to matter
if the typed values were narrower (e.g. int32/int16 packed strips, 2–4× less
bandwidth) and/or the compare were fused with the aggregation to avoid a second
pass — both are follow-on storage-format questions, not reasons to add an AVX2
kernel to today's byte-cell path.

## Repro

`scripts` (scratchpad): `smoke.sh` (SF=0.01, 3 modes + 22-gate) and
`simd_spike.sh` (SF=1, 3 configs, q6/q1 min-of-3 + md5 + RSS + `[SIMD]` log +
SF=1 22-gate). Each config restarts the server with a different `LDBC_SIMD` and
reloads (env is read once per process). AVX2 support verified via
`__builtin_cpu_supports`.
