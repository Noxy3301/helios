# Phase 22 — cardinality milestone ① research (root cause + approach)

_Why: M2b proved C_materialise=8.0 is ~20–40× its physical premium because it
compensates for q3/q10 RANGE-cardinality underestimation; M2b names cardinality as
the higher-leverage next lever (Leis). ① research = Codex (approach ranking) + a
3-lens grounded Claude panel (path/root-cause, server feasibility, literature). All
four CONVERGED on one design. Raw: workflow wf_7255c390-b29; Codex
/tmp/card_codex_research_out.txt._

## 1. Root cause — CONFIRMED live (not just hypothesised)

`records_in_range` (proxy/ha_lineairdb.cc:3784-3792): for a PURE RANGE (no equality
prefix) it returns a distribution-blind fixed fraction — **one-sided = total/10
(10%)** (line 3790), two-sided = total/20 (5%) (line 3788). The equality-prefix
branch (3772-3783) uses real NDV via `rec_per_key` (set_generic_rec_per_key, under
HELIOS_OPT_STATS), but a date `BETWEEN`/`<`/`>` never reaches it.

Live evidence (SF1 fullidx, HELIOS_OPT_STATS+COST_V2, classic optimizer, optimizer_trace):
- **Q3 one-sided (the dominant regressor):** `o_orderdate < '1995-03-15'` → est **150000**
  (=1.5M/10) vs **actual 727,305 (48.5%) → 4.85× LOW**; `l_shipdate > '1995-03-15'`
  → est 600121 (=6M/10) vs actual 3,241,776 (54%) → **5.4× LOW**.
- **Q10 two-sided (3-month window):** est 75000 (=1.5M/20=5%) vs actual 57,069 →
  1.31× HIGH — roughly right. So **the damage is the one-sided /10 branch**; the
  two-sided 5% is incidentally near-correct for q10's narrow window.
- Corroborates M0 (phase22_m0_findings.md:44-48: q3 4.4–10×, q10 3×) and M2b
  (C_materialise=8.0 is the steering knob masking exactly this).

**MySQL gets the range-access row count EXCLUSIVELY from `records_in_range`** (via
multi_range_read_info_const, index_range_scan_plan.cc:628; no index dives for this
engine; hypergraph OFF). So records_in_range OWNS this number — the fix must live there.

**MySQL histograms do NOT solve it** (verified empirically + WL#9223): an
`ANALYZE … UPDATE HISTOGRAM` correctly captures the distribution (filtered=48.78% for
a pure filter) but for a range-ACCESSED column the optimizer uses records_in_range
(filtered=10%), bypassing the histogram. Histograms feed only the post-access
`filtered` % of non-range-accessed predicates (item_cmpfunc.cc get_filtering_effect);
wiring them into range access would require modifying the MySQL core (forbidden).
Not populated on benchbase anyway. → REJECTED as the primary fix.

## 2. Feasibility — the cheap data source already exists

- **Index = Masstree** (ordered B+tree/trie hybrid, NOT hash;
  third_party/LineairDB/src/index/impl/masstree_index.cpp:55-65) → a logical range
  maps to a contiguous ordered key range; range estimation is well-posed.
- **Key encoding is ORDER-PRESERVING** (proxy/lineairdb_keyenc.cc:380-446:
  sign-flip+big-endian ints, big-endian DATE) → lexicographic byte order == logical
  column order → a proxy-side `memcmp` binary search over boundary keys is valid, and
  `convert_key_to_ldbformat` (:511-583) already encodes MySQL key bytes the same way
  records_in_range's min/max_key would.
- The server ALREADY performs a **FULL ORDERED single-pass scan** of each index in
  `ComputeIndexNdvInt` (third_party/LineairDB/src/database_impl.h:414-503) to compute
  NDV, shipped via the existing **GET_TABLE_STATS RPC** (server/rpc/lineairdb_rpc.cc:
  970, handleTxGetTableStats; cached in ndv_cache_, ANALYZE-bustable) that the proxy
  calls ONCE at plan-cold time (info(), ha_lineairdb.cc:3143-3156). **Equi-depth
  bucket boundaries can be emitted in that SAME pass at ~zero extra cost** (record the
  encoded key at every live_count/B-th live key).
- Exact per-call range COUNT is feasible (Masstree Scan returns a visited count) but
  is **O(matching rows)** — wrong for plan time. The precomputed-boundary approach is
  O(log B) in-proxy and sidesteps it. (The range-lock index variant is a `std::map`,
  also no O(log n) rank — same conclusion.)

## 3. Design direction (UNANIMOUS rank-1, for ②design)

**Server-precomputed per-index equi-depth boundary histogram, shipped via the
existing stats RPC, binary-searched LOCALLY in records_in_range (zero plan-time RPC).**

1. **LineairDB (feature branch — submodule rule):** extend `ComputeIndexNdvInt`
   (database_impl.h:414-503) to also accumulate B≈32–128 equi-depth boundary keys +
   cumulative live-counts per index leading column, over the SAME ordered pass.
2. **proto:** add `repeated bytes histogram_bounds` + `repeated uint64 cum_counts`
   (or a nested message) to `GetTableStats.IndexNdv` (proto/lineairdb.proto ~124-128)
   — piggyback the existing stats RPC, no new message type.
3. **server:** cache the boundaries in `ndv_cache_` alongside NDV (same key, same
   ANALYZE/force-recompute busting; lineairdb_rpc.cc:986-1015).
4. **proxy SHARE:** seed the boundaries next to `index_ndv_` once in info()
   (ha_lineairdb.cc:3143-3156); reuse the >20%-row-drift refetch (3059-3076) for
   staleness.
5. **records_in_range pure-range branch (3784-3792):** when boundaries exist for this
   single-column index prefix, encode min/max_key and binary-search the boundaries →
   `rows ≈ (matched_bucket_fraction)·total_records`. Apply as a **FLOOR** (never below
   the current fixed fraction) to stay conservative; **fail-closed fallback** to the
   5%/10% heuristic when boundaries are absent/non-integer/multi-part.
6. **Gate behind a new env `HELIOS_RANGE_HIST`** (like prior levers), default OFF.

Rank-2 (interim, if Rank-1 is staged): proxy-only uniform interpolation from per-index
NDV + min/max key (TPC-H dates are near-uniform → q3 ~5×→<1.5×); needs min/max key in
the NDV payload. Rank-3 (rejected): MySQL histograms (don't feed range access).

## 4. The load-bearing risk (must shape ②design)

**The cardinality fix MUST be co-tuned with LOWERING C_materialise.** A more accurate
(higher) range estimate with C_materialise=8.0 still in place can DOUBLE-CORRECT —
over-favouring full scans and flipping plans the wrong way (the net-negative
secondary-index trap, [[helios-secondary-index-cost-model-trap]]). So ②design must
specify: ship the histogram AND re-run the M2b C_materialise sweep together; accept
only a (cardinality, C_materialise) PAIR that keeps the 22-suite net-positive and
md5 22/22. A flipped-but-wrong plan shows only in md5 → md5 is mandatory acceptance.
Other risks (all manageable, from the panel): plan-time cost regression IF
mis-built as per-call RPC (mitigation: precomputed boundaries + in-proxy search,
zero RPC in records_in_range — the single biggest impl risk); staleness (reuse the
NDV drift gate); exact key-encoding/collation/NULL/inclusive-bound/composite
semantics MUST match execution; advisory stats MUST NOT enter OCC validation/read-set
(1SR); duplicate secondary keys rank over (secondary_key, primary_key).

## 5. Validation bar (for ③/④)
- **q-error:** EXPLAIN ANALYZE est-vs-actual on q3/q5/q10 range rows before/after;
  target the one-sided estimates move from ~10% toward the true 48–54% (q-error 3–10×
  → ≤~2×).
- **The headline experiment:** re-run the M2b C_materialise sweep (m2b_accept.sh) with
  the histogram ON; show C_materialise can DROP toward its physical ~0.2–0.33 cu while
  the 22-suite stays net-positive and md5 22/22 — i.e. the steering inflation retires
  once n is correct. This closes the M2b → cardinality narrative for the paper.
- **Correctness:** md5 22/22 vs InnoDB SF1 on every plan flip; validate wall-clock +
  RPC counters (a better estimate could flip into a worse prefetch path), not
  cardinality alone. EXPLAIN-both-SF (0.1 + 1) policy.
