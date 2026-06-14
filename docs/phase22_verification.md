# Phase 22 — verification (closes logic-audit B1 + B2)

_Resolves the two publish-gating gaps the adversarial logic audit
(docs/phase22_logic_audit.md) raised. Driver: scripts/dev/phase22_verify.sh
(B1 + fullidx) and scripts/dev/b2_fresh_noidx.sh (corrected B2)._

## B1 — correctness: full 22-suite md5 vs SF1 InnoDB — PASS

M5 build, fullidx (base + standard afterload index set), **prefetch ON**, SF1,
compared row-for-row (normalized + md5) against an InnoDB:3308 SF1 reference:

```
OK=22  MISMATCH=0  ERR=0
```

All 22 queries produce identical results to InnoDB — not just q18. This closes
the audit's B1 concern: the suite's "22/22 OK" was previously only an exit-code
signal, and the SF1 fullidx + prefetch + full-scan/hash plans that M1/M2a newly
select (q3/q5/q7/q8) were never result-checked at SF1. They are now md5-verified.

## B2 — perf: in-session noidx vs fullidx — net-POSITIVE confirmed

The Phase-21 "noidx 39.14s" was a cross-session number; the audit (B2) required an
in-session re-measurement. Both now measured on the **same M5 build, same session,
performance governor + C6 disabled (cstate_guard), prefetch ON**:

| config | SF1 suite | notes |
|---|---|---|
| **noidx** (base load) | **39.00s** (22/22 OK) | matches Phase-21's 39.14s |
| **fullidx** (base + afterload) | **31.75s** (22/22 OK, md5-clean) | −18.6% vs noidx |

**fullidx 31.75s < noidx 39.00s → the standard secondary-index set is net-positive
(−18.6%), confirmed in-session.** (vs the pre-Phase-22 fullidx 77.19s = +97%.)

### Methodological note (a real trap, worth recording for the paper)

"noidx" here = the **base benchbase TPC-H load**, which already carries 7 FK
indexes from `ddl-mysql.sql`; "fullidx" = base + the 23-index `postload-mysql.sql`
afterLoad set. The correct noidx baseline is therefore a **FRESH base load**, NOT
"fullidx with all secondary indexes dropped." The first B2 attempt dropped *all*
secondary indexes (including the 7 base FK indexes that q9/q17/q20 genuinely need)
and re-ANALYZEd; that stale-stats, truly-zero-index state mispriced plans and gave
q17/q20 = 150s TIMEOUT, q9 = 56s, suite ~130s+ — an artifact, NOT true noidx. A
fresh base load reproduced Phase-21's 39.00s exactly (q9 3.27, q17 0.22, q20 0.41),
confirming the artifact and the real baseline. Lesson: re-measure a baseline by
**reloading it**, not by mutating a different state into it.

## Net result — Phase 22 thesis, airtight

| config | SF1 suite | vs noidx 39.00 |
|---|---|---|
| noidx (base) | 39.00s | — |
| fullidx unfixed (Phase 21) | 77.19s | +97% (2× regression) |
| fullidx + M1 | 55.01s | +41% |
| fullidx + M1+M2a | 52.11s | +33% |
| **fullidx + M1+M2a+M5** | **31.75s** | **−18.6% (net-POSITIVE)** |

Standard secondary indexes went from a 2× regression to a net win on the
disaggregated engine, with correctness md5-verified 22/22 vs InnoDB at SF1.

## Audit minor items — status
- M5 GS-skip dead-clause comment: **fixed** (commit 6ff3681).
- `referenced[]` / semijoins-source forward-guard: **documented** (6ff3681).
- M2a gate = recognizer prefix (latent perf-only risk, not exercised by TPC-H):
  deferred to M2b (tighten or document).
- kC_materialise=8.0 provisional / scale-invariance approximate: M2b.
