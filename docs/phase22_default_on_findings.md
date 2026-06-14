# Phase 22 — HELIOS_COST_V2 default-ON: validation finding (default-ON is UNSAFE)

_The final goal-listed milestone was "HELIOS_COST_V2 default-ON" (ship the recalibrated
disaggregated cost model enabled by default). Per the invariant "TPC-C/TATP must not
regress", default-ON requires an OLTP non-regression gate. Codex (consulted on the
design) advised CAUTION: ship the coupled bundle via a real sysvar, keep the range
histogram separate, and validate TPC-C/TATP FIRST. **The OLTP validation FAILED: the
cost-model bundle is a ~32× TPC-C regression. default-ON is NOT shipped.**_

## 1. The experiment

TPC-C SF1 (1 warehouse), terminals=8, 60 s, `--external-server`, prefetch ON in BOTH,
governor/C6 pinned. The ONLY difference is the cost-model bundle env
(`HELIOS_COST_V2 + HELIOS_OPT_STATS + HELIOS_AGG_PUSHDOWN + HELIOS_ENABLE_SEMIJOIN`);
`HELIOS_RANGE_HIST` excluded (Codex: keep separate). Same loaded data (mysqld restarted
between configs; server preserved).

| config | TPC-C throughput |
|---|---|
| **bundle OFF (stock cost)** | **1388.4 req/s** |
| **bundle ON (cost-model V2)** | **43.5 req/s** (re-confirm 23.3, Server Retry 205) |

**~32× regression.** Catastrophic, reproduced.

## 2. Root cause (diagnosed via EXPLAIN, ON vs OFF)

PK point lookups are UNAFFECTED — `customer` (c_w_id,c_d_id,c_id) is `eq_ref PRIMARY
rows=1` in both. The damage is a JOIN access-path flip on the stock-level query:

| table | OFF (stock) | ON (cost-model V2) |
|---|---|---|
| order_line (ol_o_id range under w_id,d_id eq-prefix) | `range PRIMARY rows=1` | **`ALL` (full scan) key=NULL rows=427965** |
| stock | `ref PRIMARY` | `eq_ref PRIMARY` |

**COST_V2 makes `order_line` do a FULL TABLE SCAN (427 k rows) instead of the indexed
range** — exactly Codex's predicted OLTP risk ("a lower range/scan floor makes scans
look attractive vs ref access"). The cost model is tuned so a full scan + prefetch is
cheap (the analytical win for TPC-H q3/q5/q7/q8); for an OLTP small-range join that
assumption is inverted and pathological. (The range histogram does NOT help here — this
range is an EQUALITY-PREFIX + trailing range, costed via rec_per_key, not the pure-range
branch the histogram touches.)

## 3. Conclusion — do NOT default-ON; the cost model is workload-specific

**The recalibrated disaggregated cost model is ANALYTICAL-SPECIFIC.** It is net-positive
for TPC-H (the whole M0–cardinality arc) but a ~32× regression for TPC-C OLTP, because
its core lever — "full-scan + prefetch is cheap" — is correct for analytical scans and
catastrophic for OLTP indexed access. A GLOBAL default-ON would destroy any OLTP
workload sharing the disaggregated server. **HELIOS_COST_V2 therefore stays GATED /
opt-in (default OFF)** — the existing, validated-safe state. The "default-ON" milestone
is resolved as a NEGATIVE result: the OLTP gate Codex mandated caught a harmful change
before it shipped. This is itself a paper-relevant finding (a disaggregated cost-model
recalibration cannot be a single global default; cost is workload-shaped).

## 4. The path to safely default-on (deferred future work — Codex's AUTO)

To enable the cost model by default WITHOUT the OLTP regression, it must be query-shape
gated — Codex's `AUTO`: apply the V2 bundle only to analytical-shaped statements (large
scans/joins/aggregates) and keep stock pricing for PK/unique point lookups, eq_ref, and
small equality/range OLTP access. This is a substantial design (a per-statement
recognizer + a plugin sysvar `lineairdb_optimizer = compatible|v2|auto` with flush
semantics + an SQL OFF escape) and is deferred. Until then:
- Ship: KEEP gated (default OFF). Enable the bundle per-deployment/per-session for
  analytical workloads (the TPC-H measurement env already does this).
- The cardinality range histogram (HELIOS_RANGE_HIST) likewise stays a separate gate.

## 5. Stack note
The OLTP test dropped the TPC-H DB (benchrun create=true → DROP DATABASE) and loaded
TPC-C SF1; the prior TPC-H net-positive result (28.42 s, md5 22/22) is already committed
and validated (docs/phase22_card_findings.md). Re-validating TPC-H needs a reload.
