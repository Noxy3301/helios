# Phase 22 — cardinality milestone: DESIGN v1 (equi-depth range histogram)

_Implements the unanimous ① research direction (docs/phase22_card_research.md): fix
the q3/q10 RANGE-cardinality underestimate at its source (records_in_range), so the
C_materialise steering inflation can retire. Status: **draft for design dual-review
(Claude grounded + Codex) until GO.**_

## 0. Objective + the co-tuning thesis

Replace the distribution-blind fixed-fraction range estimate in `records_in_range`
(one-sided /10, two-sided /20) with a **server-precomputed per-index equi-depth
boundary histogram**, searched LOCALLY in the proxy at plan time (zero per-call RPC).
Target: the one-sided q3/q10 estimates move from ~10% toward the true 48–54%
(q-error 3–10× → ≤~2×).

**Co-tuning thesis (load-bearing, from ① research §4):** a more accurate (higher)
range estimate is NOT shippable alone — with C_materialise=8.0 still in place it
DOUBLE-CORRECTS (over-prices the range twice: once via the now-larger R, once via the
inflated per-row steering), over-favouring full scans and risking wrong plan flips.
**The deliverable is a (histogram, C_materialise) PAIR**: ship the histogram AND
re-run the M2b C_materialise sweep; accept only the pair that keeps the 22-suite
net-positive AND md5 22/22 vs InnoDB SF1. Success = C_materialise drops from 8.0
toward its physical ~0.2–0.33 cu while perf holds — the M2b→cardinality narrative.

## 1. What the optimizer actually consumes (VERIFIED, ① research §1)

MySQL obtains the range-access row count EXCLUSIVELY from `handler::records_in_range`
(via multi_range_read_info_const, index_range_scan_plan.cc:628; classic optimizer;
no index dives for this engine). Column histograms COMPLEMENT (post-access
`filtered`%) but do NOT override records_in_range for range access (WL#9223, verified
empirically). So the single correct insertion point is the pure-range branch of
records_in_range (ha_lineairdb.cc:3784-3792). rec_per_key (equality-prefix) and
ANALYZE histograms are NOT the fix.

## 2. Design — server equi-depth boundaries, proxy-local interpolation

### 2.1 LineairDB (feature branch per submodule rule; current working point per [[helios-lineairdb-submodule-branch-rule]])
Extend the EXISTING ordered single-pass index scan in `ComputeIndexNdvInt`
(third_party/LineairDB/src/database_impl.h:414-503) — which already visits every
live key in sorted order to compute NDV — to ALSO accumulate, per **single-column
leading prefix** of each index, an equi-depth boundary array: record the encoded key
at every ⌈live_count/B⌉-th live key, B≈64 (tunable). Emit `B+1` boundary keys + the
total live count. ~Zero extra cost (same pass, same key-prefix parse at db_impl.h:
432-443). Multi-part prefixes: emit boundaries for the leading single column only
(the q3/q10 case); higher prefixes fall back (§2.5).

### 2.2 proto
Add to `GetTableStats.IndexNdv` (proto/lineairdb.proto ~124-128):
`repeated bytes histogram_bounds; uint64 hist_total; uint32 hist_buckets;`
Piggyback the existing stats RPC — NO new message type.

### 2.3 server cache
Cache boundaries in `ndv_cache_` alongside NDV (same cache key
table\0index\0parts; same ANALYZE / force_recompute busting;
server/rpc/lineairdb_rpc.cc:986-1015).

### 2.4 proxy SHARE + info()
Seed the boundaries into the SHARE next to `index_ndv_`, loaded ONCE at plan-cold
time in info() (ha_lineairdb.cc:3143-3156). Reuse the existing >20%-row-drift refetch
(3059-3076) so boundaries refresh with NDV. Store as a sorted vector<encoded-key> +
cumulative counts per (index, leading-col).

### 2.5 records_in_range integration (the estimator)
In the pure-range branch (3784-3792), gated by `HELIOS_RANGE_HIST` (default OFF):
when boundaries exist for THIS index's leading single column AND the range is on that
column:
1. Encode min_key/max_key via the existing `convert_key_to_ldbformat`
   (lineairdb_keyenc.cc:511-583) — the SAME order-preserving encoding the boundaries
   were built in (verified order-preserving: int sign-flip+BE :380-427, DATE BE
   :436-446).
2. Binary-search (`memcmp`) the lower and upper bound positions in the boundary
   array; `matched_fraction = (pos_hi − pos_lo) / B` (handle one-sided: open end →
   pos at array end/start; inclusive/exclusive per key_range flag).
3. `estimate = matched_fraction · hist_total`.
4. **FLOOR semantics (conservative):** `return max(estimate, current_fixed_fraction)`
   — never estimate BELOW the old heuristic, so the change can only raise an
   underestimate, never introduce a new underestimate. (Reviewers: is floor-only the
   right guard, or should it replace symmetrically? See §6.)
5. **Fail-closed fallback** to the existing 5%/10% when: boundaries absent, index
   multi-part beyond leading col, non-integer/non-order-preserving key type, encode
   mismatch, or any uncertainty. Identical behaviour to today when gate OFF.

## 3. Correctness & invariants (must hold)
- **MySQL core UNMODIFIED** (only proxy + server + LineairDB-on-feature-branch).
- **1SR / OCC:** the histogram is ADVISORY optimizer stats — it MUST NOT enter any
  transaction read-set or OCC validation (it is fetched via the stats RPC outside the
  txn read path, like NDV today). No cross-tx cache of tuples.
- **Encoding fidelity:** boundary keys and records_in_range's min/max_key MUST use the
  identical encoding/collation/NULL-ordering/sign handling; mismatch → fail-closed.
  Duplicates in a secondary index rank over the physical (secondary_key) prefix as the
  scan visits them; the leading-col boundary captures that.
- **Fail-closed everywhere:** gate OFF or any doubt → exact current behaviour.
- **Plan flips show only in md5** → md5 22/22 is MANDATORY acceptance on every flip.

## 4. The biggest implementation risk (① research §4) + mitigation
**Plan-time cost:** if mis-built as a per-call RPC or per-call scan, it slows EVERY
optimize() call. MITIGATION (load-bearing): boundaries are seeded into the SHARE ONCE
(piggyback the NDV scan) and consumed by an in-proxy O(log B) binary search — ZERO
RPC inside records_in_range. This is non-negotiable in the design.
**Double-correction:** see §0 co-tuning — never ship the histogram with C_materialise
left at 8.0; the acceptance is the (histogram, C_mat) pair.

## 5. Validation protocol (③/④)
1. **q-error:** EXPLAIN ANALYZE est-vs-actual on q3/q5/q10 range rows, gate ON vs OFF;
   require one-sided estimates move from ~10% toward true 48–54% (q-error ≤~2×).
2. **Headline experiment (closes the narrative):** with the histogram ON, re-run the
   M2b C_materialise sweep (scripts/dev/m2b_accept.sh, extended to also sweep low
   C_mat); show C_materialise can drop toward physical ~0.2–0.33 cu while the 22-suite
   stays net-positive (≤ M5 baseline) and md5 22/22.
3. **Correctness:** md5 22/22 vs InnoDB SF1 at the shipped (histogram, C_mat) pair;
   validate wall-clock + RPC counters (a better estimate could flip into a worse
   prefetch path — perf, not cardinality, is the gate). EXPLAIN-both-SF (0.1 + 1).
4. **No regression with gate OFF:** byte-identical plans to today when HELIOS_RANGE_HIST
   unset.

## 6. Open questions for the reviewers
1. **Floor-only vs symmetric replace (§2.5.4):** floor-only fixes underestimates but
   leaves the two-sided 5% in place where it's ~right (q10 1.31× high). Is floor-only
   the safe choice, or does leaving overestimates uncorrected matter? (q10 is two-sided
   and chosen; raising its estimate could help or hurt.)
2. **B (bucket count) + boundary memory:** B≈64 per single-column index prefix ×
   #indexed columns × #tables — memory in the SHARE/cache. Is 64 enough resolution for
   48–54% selectivity ranges? Cost of larger B?
3. **Co-tuning mechanics:** should C_materialise be re-derived analytically once
   cardinality is fixed (physical ~0.3 cu), or swept empirically to the new net-positive
   value? Is there a risk the new optimum is still >> physical (residual join-NLJ)?
4. **Equality-prefix + trailing range** (e.g. composite index, eq on col0 + range on
   col1): currently rec_per_key/2 (3781-3783). Out of scope for v1 (leading-col only),
   or does q-something need it?
5. **Staleness/plan-stability:** reusing the NDV >20%-drift gate — adequate for
   boundaries, or do boundaries need their own freshness criterion?
6. Anything that breaks the zero-plan-time-RPC or fail-closed guarantees.

## 7. Dual-review v1 outcome — NO-GO → v2 fix list (Codex + Claude panel 3/3 NO-GO)

The direction is endorsed by all; v1's CONCRETE mechanics had real holes. Verdicts:
Codex NO-GO (2 BLOCKING), Claude panel 3/3 NO-GO (4 BLOCKING / 4 MAJOR / 9 MINOR).
The design review did its job — the most important defect (DATE-parser) was invisible
to file-blind Codex and caught by the grounded panel. **v2 must address:**

BLOCKING:
- **B1 — DATE parser:** `ComputeIndexNdvInt` (database_impl.h:432-443) parses INT
  (tag 0x10) ONLY and returns `available=false` for DATE (tag 0x30); the q3/q10 target
  indexes (o_od/l_sd) are single-column DATE, so there is NOTHING to piggyback today.
  v2 must EXTEND count_key to parse 0x30 (2-byte BE length-prefixed) before recording
  boundaries; the "~zero-cost piggyback" is really "extend the parser + add the
  accumulator." Re-confirm available flips true for o_od/l_sd.
- **B2 — boundaries carry CUMULATIVE COUNTS + explicit rank fns:** value-only
  `(pos_hi-pos_lo)/B` is wrong with duplicate/all-equal runs; store
  `(boundary_key, cum_count)` and define `rank_lt/le/gt/ge` over encoded keys with
  duplicate-run handling. Degenerate equi-depth (all-equal leading col) needs an
  explicit branch.
- **B3 — co-tuning headline reframed:** do NOT assert "C_materialise drops toward
  physical." M2b §7.1 (old cardinality) shows the regressors need C_mat∈(4,8], partly
  from join-NLJ which a cardinality fix does NOT remove. v2 frames the headline as an
  EXPERIMENT whose outcome may still be > physical (residual join-NLJ); reset C_mat
  from physical FIRST, then measure how far it can drop with a stated residual.

MAJOR:
- records_in_range must call the FREE `convert_key_to_ldbformat(table, inx, …)` (NOT
  the member bound to `active_index`), building keypart_map from key_parts_used's
  leading col.
- Add a distinct `hist_available` signal (separate from NDV `available`); verify that
  enabling DATE-NDV does NOT perturb rec_per_key on DATE indexes (md5 risk) — or keep
  NDV "unavailable" for non-int while still emitting boundaries.
- Name the estimate a STEP function + bound its error, or add sub-bucket interpolation
  for numeric/date (B=64 → ±1.56%).
- Exact `key_range` flag mapping (HA_READ_KEY_EXACT/AFTER_KEY/BEFORE_KEY, prefix
  lengths, sentinels) + NULL ordering/selectivity (record null_count; fail-closed for
  nullable until tested).
- Histogram-specific staleness signal (ANALYZE epoch / sampled-boundary checksum), not
  just the NDV >20%-row-drift gate.
- records_in_range must be strictly READ-ONLY wrt the stats cache (no RPC / lazy fill /
  recompute); add a zero-stats-RPC-during-optimize counter/test.

RESOLVED (Claude/Codex split, grounded-reviewer arbitration):
- **Floor-only: KEEP** (grounded estimator lens: floor-only is correct — the dominant
  damage is the one-sided UNDERESTIMATE; q10's two-sided 1.31× overestimate is benign
  and lowering it risks harm. Codex's "symmetric replace" is overruled). Document the
  limitation: floor-only won't fix a true value BELOW the old fraction (acceptable).

MINOR: B configurable (default 64, test 128/256); proto field tags 4/5/6 + rebuild
(server+proxy) + in-memory reload note; monotonicity fail-closed checks
(bounds[i]≤bounds[i+1], nonzero total) + fallback counter; md5 22/22 is necessary but
NOT sufficient — also gate on wall-clock + RPC counters (worse-prefetch-path risk);
leading-column-only scope explicit in the EXPLAIN validation list.

_Status: v1 NO-GO. v2 (incorporating the above) pending — see the implementation-arc
note: v2 + impl is a multi-component LineairDB-submodule change (DATE parser + boundary
histogram + proto + server cache + proxy SHARE + records_in_range rank logic + rebuild
+ co-tuning sweep + reload)._
