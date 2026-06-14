# Phase 22 — adversarial logic audit (M0–M5)

_6-reviewer audit (3 Claude grounded + 3 Codex reasoned, workflow wf_30a2e2ed-884),
each instructed to REFUTE a slice; then synthesis. Verdict: **logic SOUND; two
publish-gating VERIFICATION gaps** (not demonstrated bugs). No Claude/Codex
disagreements._

## Confirmed SOUND (both audits independently, no disagreement)

1. **All five invariants hold.** MySQL core untouched (diffs only in proxy/);
   1SR/Silo (M1/M2a are pure Cost_estimate → plan choice only; M5 GS-skip + all
   GS registration gated to `SQLCOM_SELECT && tx_ro_novalidate`, DML never
   hijacked, override still scans every base row so the read-set is recorded);
   no cross-tx caching (gs_regs_/gs_skipped_ are per-tx, cleared per-statement);
   fail-closed prefetch (gs_fill_buffers errors if gs_skipped-but-unregistered;
   drop only when gs_registration!=nullptr for every alias); M2a gate
   fail-closed-to-CHARGE on every uncertain branch.
2. **M1 causal diagnosis correct.** The regression is the non-covering secondary
   RANGE scan whose `read_cost` lacked the per-row materialization charge —
   confined to the non-covering branch (handler.cc:6291), which rules out
   join-order/cardinality alternatives; corroborated by the engine_cost-sentinel
   failure, the 3.2–7.6× fullscan-recovery (join graph unchanged), and SF0.1
   non-reproduction.
3. **read_cost is cost-only.** No caller stores the Cost_estimate into anything
   feeding execution; an arbitrarily wrong cost can only mis-rank plans, never
   produce wrong rows. (Empirically: md5 stable across kC_materialise 8/11/16/24.)
4. **M2a gate timing correct.** Reads only prepare-time Query_block/THD state
   valid at read_cost (before push_to_engine); for q15 the leaf's query_block IS
   the inner aggregating unit (q15 flat ~1.5s across the sweep).
5. **M5 md5-safe for q18 by construction.** GS registration rejects every
   divergence hazard (inner WHERE rejected → reg.filter empty; single non-null
   group col; live server-side HAVING; single SUM over a non-null col; read_set
   {group,sum}; fails closed). q18 md5 == forced-PRIMARY.
6. **M5 no orphan; q1/q6 untouched.** Drop excludes for_each/referenced steps; a
   dropped full PRIMARY (rnd_init) / full secondary (index_first, key==nullptr)
   both reach gs_fill_buffers. q1 (2 group cols) / q6 (no GROUP BY) never
   register GS → never dropped.
7. **M0→M5 narrative consistent.** M2a (cost, read_cost branch) and M5
   (execution, autogen GS-skip) fix different things via non-overlapping paths —
   q18's inner full INDEX_SCAN is costed via the deprecated read_time()
   (PK/secondary-symmetric), which the M2a gate's read_cost branch cannot reach,
   exactly why M5 was needed.

## BLOCKING (verification gaps — being closed; NOT demonstrated bugs)

- **B1 — correctness evidence gap.** md5 was proven only for q18; the suite
  "22/22 OK" is exit-code only (`tpch_matrix.sh` does no result comparison), and
  the fallback (noidx md5 already verified) was SF0.1 with prefetch OFF, whereas
  M1/M2a deliberately flip q3/q5/q7/q8 to full-scan+prefetch+hash at SF1 —
  those exact plans were never result-checked at SF1 with prefetch ON. The
  cost-only argument is necessary-not-sufficient (a plan flip is the canonical
  trigger for a latent wrong-but-non-erroring bug RC-checks miss). **Blast radius
  refined:** the M5 GS drop is NOT suite-wide (gs_registration guard + strict
  shapes ⇒ only q15/q18-shaped tables; q18 is md5-verified, q15 never dropped) —
  the residual gap is the M1/M2a SF1 plan flips. **Remedy:** full 22-suite md5
  vs SF1 InnoDB (or forced-PRIMARY) under M5 build + fullidx + prefetch ON.
- **B2 — perf headline cross-session.** "fullidx 31.65s < noidx 39.14s (−19%)"
  compares fresh in-session fullidx to the Phase-21 noidx; with ~3.7× C6 throttle
  variance documented, −19% sits within plausible cross-session drift. The
  DIRECTION (net-positive) is robust (Phase-22 changes are inert for noidx: M1/M2a
  live in the secondary-index read_cost branch noidx lacks; M5's PRIMARY drop
  already fired in Phase-21). **Remedy:** re-measure noidx in the SAME M5
  build/session (governor + C6 pinned) vs the same-session fullidx.

→ Both remedies run in `scripts/dev/phase22_verify.sh`.

## Minor / hardening

- **M5 dead clauses (fixed: comment).** `serialized_filter.empty()` /
  `semijoins.empty()` in the GS-skip predicate are always-true at that pass
  (filter/semijoin attachment runs later); the real discriminator is the
  full-scan shape + the per-alias gs_registration guard. Comment corrected to
  credit the guard and label the clauses forward-guards; "EXACTLY selects full
  scans" relaxed (a both-unbounded range also matches — harmless, GS-correct).
- **`referenced[]` doesn't track `semijoins[].source_step`** — safe only because
  semijoin attachment runs after the drop pass; documented so a future reorder
  can't silently drop a semijoin source.
- **M2a gate is the recognizer PREFIX, not the full offload predicate** — a
  single-table grouped RO SELECT the executor won't offload can match the
  skip-shape and under-price a non-covering range (latent PERF risk only;
  cost-only ⇒ never wrong rows; not exercised by TPC-H q1/q6/q15). Optional:
  tighten to mirror offload preconditions.
- **Doc:** kC_materialise=8.0 is provisional (fit vs SF1 cardinalities known
  3–10× biased; M2b NNLS pending); scale-invariance is *approximate* (the
  ceil()-based RPC term adds a bounded sub-dominant sawtooth near batch edges).
