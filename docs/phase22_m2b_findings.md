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
access class read_cost()/C_mat governs (ref/eq_ref go to page_read_cost; design
§4-note, verified handler.cc:6291-6296 / sql_planner.cc:157-163).

**Engine fact discovered during impl:** this engine has NO index-only execution
(index_flags lacks HA_KEYREAD_ONLY, hh:251-255; EXPLAIN of a covering `SELECT k`
shows `Using where`, not `Using index`). So the covering-vs-non-covering difference
is a BYTE difference, not a materialise-vs-not step. C_mat is therefore measured as
the **random-materialise PREMIUM of a range over a sequential scan** (the
PostgreSQL random_page_cost analog), not as covering−non-covering.

## 2. Physical constants (server-side, prefetch ON; m2b_fit.py)

Measured per-row/per-byte slopes (jackknife-over-design-point 95% CI):

| quantity | physical | note |
|---|---|---|
| S_scan (sequential scan) | **439 ns/row** [324, 554] | cal_n narrow N-sweep 1e3..1e6 |
| B_ship (server per byte) | **1.83 ns/byte** | from noncov−cov byte delta |
| non-cov range per-row | 1787 ns/row [1715, 1859] | walk + random PK materialise + ship |
| fullscan marginal ship/row | 994 ns/row | sequential materialise + ship |
| **C_mat = random-materialise premium** | **793 ns/row** | = noncov_per_row − fullscan_ship_per_row |
| C_rpc (1 staged RPC fixed) | **81 µs** | 1-row point read |

## 3. Anchor + gap vs current defaults — the headline result

Anchor (design §1.1, no rescale): the model's `table_scan` CPU = N·(C_ROW+C_REMOTE)
= N·0.15 cost-units ↔ measured S_scan 439 ns/row ⟹ **1 cost-unit ≈ 2928 ns**.
Converting the physical constants to cost-units and comparing to the env defaults:

| constant | physical (cu) | default (cu) | default / physical |
|---|---|---|---|
| C_RPC         | 27.7   | 50.0   | **1.8×** (same order — defensible) |
| C_BYTE        | 0.00062| 0.0008 | **1.3×** (≈ right) |
| **C_MATERIALISE** | **0.27** | **8.0** | **29.5× — grossly inflated** |

**So C_byte and C_rpc are physically reasonable (within ~1.3–1.8× of the measured
values); only C_materialise is far off — the production value 8.0 is ~30× the
physical random-materialise premium (0.27 cu).** This is the precise answer to the
project's founding question ("does 8.0 re-derive from physics?"): **NO — 8.0 is a
~30× steering inflation, not a hardware constant.**

### 3.1 Corroboration with the M1 sweep (independent)

M1's whole-query sweep (phase22_m1_findings.md) found the JOIN regressors q3/q5/q7/q8
only flip to full-scan+prefetch at **C_mat ≈ 4–8**; below that they regress. The
physical premium is **0.27**. The two independent methods agree the production need
(~8) is **~15–30× the physical value** — the gap is real, not a fitting artifact.

### 3.2 Access crossover s* (single-table, the SF-independent quantity)

Measured single-table range-vs-fullscan crossover **s*_access ≈ 73.5% selectivity**
(N=100000): a non-covering range beats full-scan+prefetch until it touches ~3/4 of
the table. A model carrying the PHYSICAL C_mat (0.27) predicts ~this crossover; the
default 8.0 predicts a far LOWER crossover (it flips to full-scan for much more
selective ranges) — i.e. **8.0 deliberately over-penalises single-table ranges.**
That over-penalisation is wrong for single-table access but is what the JOIN
regressors need — the gap is the join/cardinality context (§4, §5).

## 4. Interpretation — why 8.0 ≫ physical (the central thesis, now quantified)

The 30× gap is explained by two effects the single-access-path cost model cannot
see, exactly as design §0 predicted:
1. **Cardinality underestimation (dominant, per Leis).** The repo records q3/q10
   range cardinalities estimated **3–10× too LOW** (phase22_m0_findings.md:44-48).
   The model prices a range as R_est·C_mat; if R_est is 3–10× low, C_mat must be
   3–10× high to recover the correct total penalty. This alone covers a large part
   of the 30×.
2. **Join-NLJ amplification.** The regressors are joins; a non-covering range
   feeding a join inner costs more than the standalone single-table range the
   premium was measured on (per-driver work, prefetch staging). The standalone
   physical premium under-counts the join-context cost.

**Conclusion: `C_materialise` is an irreducible STEERING parameter, not a hardware
constant** — it compensates for cardinality bias + unmodeled join context. This is
precisely why M2a couples it to an executor-aware GATE (charge only where a per-row
materialise actually steers a join/non-grouped plan) rather than shipping it as a
universal per-row cost. **The higher-leverage fix (Leis; the goal's own note) is to
correct the q3/q10 cardinality underestimate**, after which a near-physical C_mat
(~0.3 cu) would suffice and the steering inflation could be retired.

This is the **FAIL branch of the §0 thesis, and it is the publishable result**:
physically-calibrated constants do NOT reproduce the production plan choices; the
gap (0.27 vs 8.0) is the quantified evidence of context-dependence. C_byte and C_rpc,
by contrast, ARE physically grounded and could be tightened to their measured values
(1.3–1.8× moves) as a minor, defensible recalibration.

## 5. Scale-invariance (design §6)

S_scan across the 1e3→1e6 probe sweep: 385/281/358/437 ns/row — stable within ~1.5×,
NOT a clean constant. The variation is an **in-memory residency effect** (CPU-cache/
NUMA/allocator locality at larger N), exactly the regime design §6 says remains even
without buffer-pool disk spill. The per-row constants are scale-invariant to within
this ~1.5× in-memory-residency band; this is reported honestly, not dismissed as
noise. (The decision-relevant s* is a ratio and is more stable than the absolute
slopes.)

## 6. §7.1 hold-out confirmation (fullidx SF1 suite) — PENDING

Pre-registered: plug C_mat = physical (0.27) and sweep up to the default (8.0) on the
fullidx SF1 suite (full deployment); the registered baseline is 31.75 s (same-session
re-run, phase22_verification.md). Expected (to be confirmed): physical 0.27 regresses
the join regressors (range plans return); the lower band edge sits at ~4–8 (≫ 0.27),
quantifying the steering gap on the actual suite; 8.0 holds at ~31–32 s with md5 22/22.
[Results filled in after scripts/dev/m2b_accept.sh on the backfilled fullidx server.]

## 7. Recommendations
- **Ship:** keep `C_MATERIALISE=8.0` as a DOCUMENTED steering default, executor-gated
  by M2a (provenance: this finding — physical 0.27, steering 8.0, gap = cardinality
  3–10× + join context). Optionally tighten C_BYTE→~0.00065 and C_RPC→~28 to their
  physical values (minor, within-band; verify no suite regression).
- **Next high-leverage milestone (beyond M2b):** correct the q3/q10 cardinality
  underestimate (records_in_range / rec_per_key), per Leis — this is where the real
  error lives, and it would let C_materialise approach its physical value.
