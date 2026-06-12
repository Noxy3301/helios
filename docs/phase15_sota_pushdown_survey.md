# Phase-15 SOTA Survey — Query Pushdown & Execution Optimization for Disaggregated Databases

**Date**: 2026-06-12
**Method**: deep-research fan-out — 7 parallel research agents (one per research question, ~220 web searches/fetches
total, primary sources preferred: paper PDFs / official vendor docs) + 2 adversarial verification agents that
re-fetched primary sources for the load-bearing claims. Corrections found during verification are incorporated
below; remaining unverified items are marked **UNVERIFIED**.
**Relation to prior surveys**: extends `docs/phase10_pushdown_sweep.md` (canonical pushdown list; conclusion:
row-reduction pushdown mostly harvested, residual = server scan/agg CPU) and
`docs/phase10_grouped_summary_sota_survey.md` (aggregation pushdown is an established technique with a common
industry safety envelope). This survey deliberately goes *beyond* those: OLTP round-trip reduction, validation
cost, SIP variants, wire/data movement, optimizer robustness, and post-2022 disaggregation work.

**日本語TL;DR**: 7領域を一次資料で踏査した結論 — (1) TPC-C coverage gap への正解は Calvin OLLP / Chardonnay
(OSDI'23) 型の「偵察実行で tx 単位 read-plan を作り、OCC validation を安全網にする」方式で、Helios は両者より
構造的に有利。(2) 巨大 scan read-set の検証は HyPer 流 precision locking(検証コスト O(|R|)→O(同時コミット書込))
が最強の公表手法で、user 発案の range-hash レシートは**公表例が無い=新規性あり**(正直な位置づけは「Hekaton 流
re-scan の O(1) 通信版」)。(3) SIP は Predicate Transfer (CIDR'24, Bloom join 比 3.3x) の forward/backward pass を
one-RPC 内の plan step として実装できる点が Helios 固有の新規性(単一 shipment SIP は文献に存在しない)。
(4) wire LZ4 は loopback では文献的に負け筋 — 代わりに軽量列エンコードが転送・CPU・staging メモリを同時に削る。
(5) server scan CPU 床には storage-side zone maps (Exadata storage index / Snowflake pruning 79-99%) が直撃。
ランキング表は §8。

---

## 1. Disaggregated / storage-pushdown systems (industrial)

What each system actually pushes to the storage layer, with verified evidence.

| System | What is pushed/offloaded to storage | Reported payoff | Safety conditions |
|---|---|---|---|
| **Aurora** (SIGMOD 2017) | Redo-log application only ("the log is the database"); no query operators | ~35x sysbench writes vs mirrored MySQL (UNVERIFIED exact figure) | quorum/log semantics |
| **Aurora Parallel Query** (AWS docs, verified) | Projection (column extraction), WHERE **and join-clause** filtering, **function processing**; storage returns "compact tuples" | "order-of-magnitude in many cases" (vendor claim, no std. benchmark) | cost-based opt-in; **PQ bypasses the buffer pool entirely** (every run re-scans cold) |
| **PolarDB Serverless** (SIGMOD 2021) | Page materialization from redo at storage; one-sided RDMA page reads; **no operator pushdown** (explicitly future work) | <30% drop vs local-memory PolarDB; 5.3x faster recovery | synchronous `page_invalidate` before redo flush; global SMO latches |
| **PolarDB + CSD** (FAST 2020) | **Table scan** down to FPGA computational-storage drives: Snappy decompress (2.3–2.8 GB/s) + **arbitrary AND/OR predicate trees** | >30% latency cut on 12/22 TPC-H; PCIe traffic −97% (Q6); network −70% on 12 queries | filter-only on immutable compressed blocks; visibility resolved above |
| **PolarDB-IMCI** (SIGMOD 2023, *not* VLDB) | Nothing to storage — columnar index on RO nodes fed by redo | up to 149x scan-heavy TPC-H 100GB, 5.56x geomean; <5% OLTP perturbation | visibility = redo apply lag |
| **Redshift AQUA** (SIGMOD 2022 paper + AWS docs) | "Push-down accelerator for complex scans and aggregations"; "functional interface, not a storage interface"; FPGA VLIW pipelines; filter acceleration limited to LIKE/SIMILAR_TO | "up to 10x" (vendor) | per-node compiler decides CPU vs accelerator |
| **Redshift Spectrum / federation** (SIGMOD 2022) | Scans + aggregations over S3 (1:10 fan-out fleet); federated subqueries with filters/aggs pushed into OLTP sources with semantic-correctness rewrites; **adaptive runtime Bloom filters at scan source** (sized from build cardinality, disabled when rejection ratio low) | — | correctness-first rewrites |
| **S3 Select / PushdownDB** (ICDE 2020) | Filter/proj/simple agg primitive; PushdownDB synthesizes on top: **Bloom join (BF encoded as predicate string), 2-phase group-by, 2-round sampling top-K, S3-side index** | **6.7x faster, 30% cheaper** vs no pushdown (TPC-H subset, 10GB) | decomposable aggs only; BF FPs affect transfer not correctness |
| **FlexPushdownDB** (VLDB 2021) | **Separable operators** split per-segment between local cache and storage pushdown, merged in one hybrid scan; Weighted-LFU cache (weights = pushdown cost saved) | **hybrid beats caching-only and pushdown-only by 2.2x** (SSB); W-LFU +37% vs LFU | separability = result is union over disjoint partitions |
| **Snowflake** (SIGMOD 2016 / NSDI 2020 / SIGMOD-C 2025) | **Nothing pushed to S3**; reduction is metadata-side min/max pruning + compute-side SSD cache (0.1% of data cached → 80% RO hit rate) | join pruning ("value summaries" vs probe-side min/max metadata) prunes **79% of probe-side micro-partitions avg, up to 99.99%; 99.4% of data skipped platform-wide** (verified) | immutable micro-partitions; pruning-tree evaluation **adaptively reordered** by observed pruning ratio |
| **FarView** (CIDR 2022) | FPGA smartNIC in front of disaggregated DRAM: projection ("smart addressing" = per-column reads), selection/regex, group-by/agg, (de)encryption — at line rate; joins stay at compute | competitive with *local* buffer cache on predicate-heavy scans | buffer-cache semantics; DB owns versions |
| **AlloyDB columnar engine** (Google docs) | Nothing to storage — in-instance columnar cache; SIMD filters, min/max skipping, vectorized agg/join | "up to 100x" (marketing) | transactionally consistent cache; silent fallback to row engine |
| **TiDB/TiFlash** (VLDB 2020 + docs) | **Plan subtrees to TiKV coprocessors**: predicates, "in some cases aggregations and TopN", vectorized; partial aggs merged at SQL layer; **per-expression pushdown allowlists**; TiFlash = Raft-learner columnar replicas (MPP) | HTAP isolation quantified (OLAP −<5%, OLTP −10%); per-operator payoff not reported | snapshot ts reads; learner waits for raft index covering read ts |
| **SingleStore** (SIGMOD 2022) | **Nothing — blob storage is passive**; leaves do compute; blob holds cold data + history | commits never wait on blob; economics, not pushdown | local disk+replication = commit domain |
| **Exadata** (Oracle docs; beyond phase-10 coverage) | **Hash-join Bloom filters offloaded to cells** (23ai/ESS 24.1: larger BFs); **storage indexes** = automatic in-memory min/max per 1MB region per column, composable with BFs; decryption offload | vendor-grade claims only | cells return raw blocks when consistent-read cannot be decided at storage (silent fallback) |
| **MySQL HeatWave** (Oracle technical brief + docs; **no top-venue academic paper exists** — verified by PVLDB vol.13–18 + DBLP sweep) | Whole-query offload to secondary engine iff (1) all operators/functions supported AND (2) estimated HeatWave time < estimated MySQL time; silent InnoDB fallback; ≥9.0 "dynamic offload" also weighs propagation lag, queueing, InnoDB index availability; Autopilot learns stats from execution history | vendor claims only | dual cost estimate + support gate |

**Takeaways for Helios.**
- The field splits cleanly into *function-shipping* systems (Aurora PQ, Exadata, TiDB coprocessor, AQUA,
  PushdownDB, FarView, PolarDB-CSD — Helios's camp) and *byte-shipping + compute-side cache* systems (Snowflake,
  SingleStore, IMCI, AlloyDB). Helios's strict-serializability requirement rules the second camp's consistency
  model out for RW work, which is the architectural justification to state in any writeup.
- **Economics caution**: S3 Select was closed to new customers on 2024-07-25 (AWS blog, verified); AQUA was
  quietly absorbed (docs deleted, verified via Wayback). Separate acceleration *tiers* lose to integrated
  compute+cache; Helios avoids this by pushing into the mandatory storage server, not a third tier.
- HeatWave is the closest production analog (MySQL plugin + full-query offload + dual-cost gate + silent
  fallback) — and it has *no* academic paper, leaving the "transactionally consistent (OCC) MySQL offload with
  1SR" slot open for Helios.
- Aurora PQ's documented buffer-pool bypass and Exadata's CR-block fallback form a citable "consistency-condition
  catalog" into which Helios's OCC validation slots as the strictly-serializable variant.

## 2. One-shot / deterministic transaction execution (→ TPC-C coverage gap)

The internal problem (phase-14): prefetch covers ~10% of TPC-C transactions because plans are staged
once-per-transaction and the PK-MRR path was unstaged; multi-statement JDBC transactions don't fit a
1-statement/tx prefetch design.

- **H-Store / VoltDB** (VLDB 2007/2008): one-shot = transaction decomposable into single-site sub-plans, no
  cross-site intermediates, no user stalls. **82x TPC-C vs a commercial RDBMS** (verified: 70,416 vs 850 tps).
  Lesson: *the transaction, not the statement, must be the shipping unit*. The price is stored-procedure
  conversion — exactly what Helios's `@_tx_plan` avoids by declaring the plan instead of the code.
- **Calvin** (SIGMOD 2012, §3.2.1 verified): deterministic execution needs read/write sets up front; dependent
  transactions use **OLLP** — a "low-isolation, unreplicated, read-only reconnaissance query" discovers the set;
  execution re-checks the reconnoitered reads and deterministically restarts if stale. Restarts are rare because
  set-determining reads are typically low-volatility secondary-index lookups (their example: TPC-C Payment
  by-name never restarts). **SLOG** (VLDB 2019) and **Detock** (SIGMOD 2023) extend this geo-distributed
  (order-of-magnitude latency/throughput gains over strictly-serializable geo baselines).
- **Chardonnay** (OSDI 2023, fully verified from the PDF): every transaction runs twice — a lock-free **dry run**
  "to (approximately) compute and prefetch the transaction's read set" and pin it in memory, then the actual
  execution under 2PL+2PC with locks in ascending key order (deadlock-free). The dry run is **advisory only** —
  stale predictions cost a cold read, never correctness. Epoch service (~10 ms ticks) gives lock-free strictly
  serializable read-only snapshot queries. **Throughput under extreme contention only 15% below low-contention,
  vs >85% drop for System-R*-style baselines.** Follow-up: Chablis (CIDR 2024 best paper) takes the design
  geo-distributed.
- **Aria** (VLDB 2020): deterministic batches *without* a-priori sets — execute speculatively against a snapshot,
  observe the actual R/W sets, deterministically resolve. Up to 1.7x over Calvin. Legitimizes "the observed read
  set of execution N is the predicted plan for execution N+1".
- **Janus** (OSDI 2016): one-shot dependency-graph transactions commit in 1 RTT conflict-free / ≤2 RTT under
  conflict — evidence that a one-shot-decomposable tx needs no more round trips than Helios already pays
  (1 plan-RPC + 1 validate-RPC).
- **Morty** (EuroSys 2023): speculative execution for *interactive* transactions (CPS-rewritten so only invalid
  suffixes re-execute). **Forerunner** (SOSP 2021): speculative pre-execution with recorded *constraints* under
  which speculative results stay valid (8.39x avg live speedup) — the constraint idea is exactly "prefetch
  validity receipts".
- **Application-side automation**: Sloth (SIGMOD 2014, lazy batching of queries via program analysis, up to 3x
  page-load), Pyxis (PVLDB 2012, auto-partition app code into stored procedures), DBridge / "Holistic
  optimization by prefetching query results" (SIGMOD 2012, static analysis inserts earliest-safe prefetches),
  QURO (PVLDB 2016, reorder statements inside tx code to shrink contention windows, up to 6.53x). These show the
  client-side `@_tx_plan` population could be automated without touching app semantics.
- Gap scan: no 2023–2026 top-venue paper does *learned* read-set prediction for interactive OCC transactions
  (UNVERIFIED as an absence claim, but three targeted searches surfaced nothing closer than Morty/Chardonnay) —
  open niche for Helios's positioning.

**Helios mapping.** Helios is structurally *better* placed than both Calvin and Chardonnay: Silo commit
validation makes any misprediction a **performance event, not a correctness event** (Calvin must restart;
Chardonnay must fall back to wound-wait). Concrete design: the first execution of a TPC-C statement template
acts as the reconnaissance/dry run; the proxy records the union read-plan per transaction template
(parameterized by w_id/d_id/c_id bindings — like existing B*.K join bindings, *not* concrete key lists, per
Calvin's low-volatility argument); subsequent executions of the same template ship the whole tx-scoped plan in
one RPC, with per-statement fallback for uncovered reads. 1SR is untouched because validation is unchanged.

## 3. Sideways information passing & join reduction

- **LIP** (PVLDB 10(8) 2017, verified with corrections): Bloom-filter chains from all dimension tables applied to
  the fact scan, **adaptively reordered at runtime by observed selectivity**. The headline is *robustness*, not
  raw speed: SSB worst-vs-best plan gap drops from 14 s to **150 ms**; best-plan times improve ~13% avg; a TPC-H
  Q8 subplan-space experiment shows 4.0x geomean (1.2–18x). FP-handling: joins re-verify; filters are
  performance-only.
- **Bitvector-aware query optimization** (SIGMOD 2020, MSR; system anonymized as DBMS-X): make the *optimizer*
  cost plans with bitvector filters; minimal-cost plan with bitvectors lies in a linear-size family. **22–64%
  total CPU reduction on TPC-DS/JOB/CUSTOMER** (verified — not TPC-H), up to 2 orders of magnitude per query.
  Lesson: once SIP exists, plan choice should anticipate it.
- **Predicate Transfer** (CIDR 2024, Yang/Zhao/Yu/Koutris — *not* Stonebraker; verified): generalize Bloom join
  to the whole join graph — forward pass + backward pass of Bloom filters along join edges (Yannakakis with
  Blooms), then any join order runs on pre-filtered inputs. **3.3x avg over Bloom join (up to 61x), 4.8x avg over
  Yannakakis (up to 47x) on TPC-H** (verified). **Robust Predicate Transfer** (SIGMOD 2025, in DuckDB, verified):
  transfer-schedule algorithms make it provably robust for acyclic queries — worst/best join-order ratio drops to
  **1.6x** (close to 1 for most queries) on TPC-H/JOB/TPC-DS.
- **Yannakakis revival**: Yannakakis+ (SIGMOD 2025; 2–5x over classic Yannakakis, avg 2.41x over native
  DuckDB/PG/Spark plans on 160/162 queries); TreeTracker (TODS 2026); "I Can't Believe It's Not Yannakakis"
  (CIDR 2026, Microsoft): SQL Server's bitmap pre-filtering + Cascades already embodies Yannakakis and is
  "semi-robust" — semijoin reduction is converging from theory and practice simultaneously.
- **Production runtime filters**: Snowflake build-side "value summaries" overlapped with min/max metadata
  (verified numbers in §1); Spark 3.3 row-level Bloom join filters (up to ~10x on TPC-DS variants, vendor blog);
  Trino dynamic filtering (IN-list → min/max above threshold); Impala Bloom + min/max with wait-timeout
  fallback; DuckDB (2024) hash-join min/max + small IN-list filters + dynamic perfect hash join — cheapest wins
  come from **min/max + small IN-sets consumed at zone/range granularity**.
- **Magic sets** (PODS 1986; cost-based: SIGMOD 1996): compile-time SIP for correlated subqueries; the 1996
  lesson is that binding-set transfer must be *cost-gated* (large binding sets lose).
- **Semi-join reducers** (Bernstein & Chiu, JACM 1981; SDD-1, TODS 1981): semijoin profitable iff shipping the
  join-column projection costs less than the bytes it removes — disaggregation has reinstated 1981's economics.
- **Anti-join asymmetry**: a Bloom filter proves *non-membership* exactly — for NOT EXISTS, Bloom misses are
  immediately emittable and only hits need exact verification. No dedicated anti-join runtime-filter paper
  exists; ExactAntiMap with a Bloom front-end is well-founded and unclaimed territory. Diamond Hardened Joins
  (PVLDB 17, 2024, Umbra) decomposes hash join into Lookup (existence) / Expand sub-operators — same principle
  as Helios's SharedScan dedup + probe steps, with worst-case guarantees.
- **The single-shipment gap (novelty claim)**: every published SIP variant needs either an extra round trip
  (PushdownDB builds the BF client-side), a compute-side build (Exadata), or a shared-memory engine (PT, LIP).
  **No published system builds AND consumes runtime filters inside one storage-side request.** Helios's one-RPC
  plan executor can express a semijoin program (Bernstein-Chiu) as ordered plan steps with filter slots — a
  publishable claim.

## 4. Wire / data-movement optimization

- **"Don't Hold My Data Hostage"** (PVLDB 10(10) 2017): protocol overhead dissection; explicit guidance —
  **on localhost, no compression wins; on LAN/WAN, compression wins**; heavyweight codecs almost always lose;
  **column-specialized lightweight compression (PFOR/binpacking) dominates Snappy in every scenario**. A
  column-chunked binary protocol gives an order of magnitude over row-based text protocols. → Helios's flat
  codec follows the prescription; **generic wire LZ4 on the loopback/10GbE-class link is a literature-supported
  "don't"** — the remaining lever is lightweight encodings, which cut transfer, serialization CPU, *and* staged
  memory simultaneously.
- **Arrow Flight / Flight SQL**: localhost TCP >2–3 GB/s single-stream; parallel streams to 95% of NIC bandwidth
  (benchmark paper, ICPE 2022 ws). The verified essence of its "zero-copy": eliminate *serialization
  transformation*, not memcpy — the receive buffer is referenced directly as columnar data. That is precisely
  the borrowed-span pattern for Helios's staging caches (the 5–6x copy problem).
- **Late materialization** (ICDE 2007; Selective Late Materialization, VLDB 2025 for cloud DBs): fetch IDs/filter
  columns first, materialize survivors. For Helios (not transfer-bound at SF=1) the win is **staged
  working-set reduction**, not latency.
- **Semantic caching / result reuse**: Dar et al. (VLDB 1996) remainder queries; Snowflake result cache requires
  *syntactic* match + unchanged data (no subsumption; verified from docs); MonetDB Recycler (SIGMOD 2009).
  **Oracle Client Result Cache** (docs, verified): consistency via invalidations piggybacked on every round
  trip; queries touching self-modified objects bypass the cache — the pessimistic answer to what Helios's
  receipts answer optimistically (validate staged data at commit, no invalidation traffic).
- **Shared / cooperative scans**: QPipe (SIGMOD 2005, 2x TPC-H throughput under concurrency), Cooperative Scans
  (VLDB 2007, relevance-ordered chunk scheduling + mid-scan join-in), Crescando (PVLDB 2009, clock scan with
  one-rotation latency bound), SharedDB (PVLDB 2012), DB2 scan sharing. Directly applicable to "many MySQL
  proxies share one storage server": merge concurrent read-plan scans of the same table server-side; pairs
  naturally with the morsel-parallel scan work (Track-B).
- **Lightweight encodings**: BtrBlocks (SIGMOD 2023 — motivation: at NVMe+100GbE, scans become
  *decompression-CPU-bound*; cascaded RLE/FOR/dict/FSST selected by sampling), FastLanes (PVLDB 16, 2023 —
  >100 billion ints/s decode, ISA-portable). These are "compression that is free to scan" — usable as a combined
  wire+staging format, and potentially comparable-while-encoded for OCC metadata.

## 5. Optimizer integration for remote storage

- **Calibration lineage**: Du/Krishnamurthy/Shan (VLDB 1992) — calibrate cost-model coefficients of an opaque
  remote DBMS by running a synthetic calibrating database. `postgres_fdw` exposes exactly Helios-shaped knobs:
  `fdw_startup_cost` (default 100; per-remote-RPC overhead), `fdw_tuple_cost` (default 0.2; per-tuple transfer),
  `use_remote_estimate` (ask the remote for estimates). HELIOS_COST_V2 (RPC + transfer-proportional) is this
  pattern; Helios controls the server, so a "server returns its own scan-cost/cardinality estimates" API
  (use_remote_estimate analog, once per plan staging) is a natural extension.
- **Presto** (ICDE 2019): connector declares physical properties/statistics; engine and connector co-decide
  pushdown — same two-party negotiation as Helios handler ↔ plan compiler; Trino docs codify correctness-first
  refusal (connector declines → engine executes). TiDB maintains per-expression allowlists. → versioned
  capability negotiation between proxy and server is the operational hardening pattern.
- **Robustness for whole-plan shipping**: Plan Bouquets (SIGMOD 2014) / SpillBound — abandon selectivity
  estimation, execute with doubling cost budgets, with worst-case suboptimality (MSO) guarantees; Smooth Scan
  (ICDE 2015) — morph index↔scan access paths at runtime; SQL Server batch-mode **adaptive join** (docs) —
  embed *both* join strategies in one plan with a threshold row count, switch at runtime without re-reading;
  Spark AQE — re-plan at materialization (shuffle) boundaries. **POP / POP-FED** (SIGMOD 2004 / VLDB 2006,
  federated!) — embed *validity ranges* for cardinality estimates into the plan; execution stops and
  re-optimizes when actuals leave the range; ~100x on a customer OLAP case study.
- **Adaptive fallback in production pushdown**: Exadata silently degrades to block I/O (documented conditions:
  cell memory pressure, LOBs, non-direct reads…) and even *hedges at runtime* (tries a few block I/Os and
  abandons smart scan if faster); exposes fallback-reason counters (`cell num bytes in passthru`). HeatWave's
  dual-cost + support gate (§1). **No published system implements a "result-size circuit breaker"
  (mid-flight abort of a pushdown when results exceed a budget) — a publishable gap** that also serves Helios's
  hard memory constraint.
- **Learned steering** (peripheral): Bao (SIGMOD 2021) steers an existing optimizer via hint sets with
  bandit selection — Helios's gate set (AGG_PUSHDOWN/SEMIJOIN/COST_V2/…) is literally a hint-set space; a
  future-work note, not a current lever.

## 6. Serializable read validation at scale

The internal pain: per-key TID materialization for huge scans (6M+ keys, the 20x memory bloat) and O(|R|)
validation; RANGEHASH_OCC receipts exist for the read-only scope; extension to RW needs own-write
reconciliation. All mechanisms below verified from primary PDFs.

- **The O(|R|) wall is universal in classic OCC**: Hekaton (PVLDB 2011: visibility re-check of every read
  version + "walks its ScanSet and repeats each scan" for phantoms), Silo (per-record TID compare), TicToc
  (SIGMOD 2016: per-read `wts/rts` check + rts-extension CAS *writes* to read tuples; phantoms explicitly
  deferred to future work), ERMIA/SSN (SIGMOD 2016: O(|R|) pstamp *writes* at commit), FaRM (SOSP 2015: O(|R|)
  one-sided version re-reads; adaptive RDMA-vs-RPC threshold t_r=4 per primary; **no published range/phantom
  protocol** — the B-tree description is omitted). None escape O(|R|); they optimize constants and abort rates.
- **The two published escapes**:
  1. **Precision locking revived — HyPer** (Neumann/Mühlbauer/Kemper, SIGMOD 2015): log reads as *predicates*
     (64-bit comparison summaries per attribute); at commit, check the **undo buffers of concurrently committed
     transactions** against the predicate tree (insert satisfying a read predicate ⇒ phantom abort; updates
     check before- and after-images). **Validation is O(|W_concurrent|), independent of |R|** — "no matter how
     large the read set… was". Measured: 5–7% logging overhead on TPC-C, 1–2% TATP, PT build 2–24 µs.
     Phantoms included by construction; own writes trivially skipped.
  2. **Structure-version validation — Silo node-sets** (SOSP 2013): scans record overlapping B+-tree leaf nodes
     + versions; commit re-checks node versions → phantom check compressed from O(keys) to O(leaf nodes).
     **Membership only** — per-record TIDs are still validated per key (in-place updates don't change node
     versions). Own-insert reconciliation rule: upgrade own node-set entry v_old→v_new, abort on concurrent
     change — *the published template for own-write reconciliation in range validation*.
- **Snapshot escapes for read-only work**: Silo snapshot epochs (~1 s; RO transactions "commit without checking;
  never abort"; bounded multi-versioning only at snapshot boundaries), Cicada (SIGMOD 2017: RO transactions
  "do not track or validate the read set"; index node wts/rts = MVCC node-set), Chardonnay epochs (§2),
  Hekaton old-version reads. None give snapshot scans *inside* a RW transaction under 1SR for free — the
  published cheapest compromise is HyPer's: execute reads at start-time snapshot, validate predicates against
  concurrently committed writes only.
- **Digest/hash validation — the gap is real (searched hard)**: no SIGMOD/VLDB/SOSP/OSDI system validates OCC
  reads by comparing a hash of (key,version) sets. Nearest relatives solve different problems (Merkle
  authenticated indexes = integrity vs untrusted servers; FoundationDB resolvers = range-conflict checks,
  precision-locking-flavored, not hashed — UNVERIFIED detail). **Helios's RANGEHASH receipt is publishable as
  novel**, honestly framed as "Hekaton-style scan repetition with O(1) communication instead of O(N) transfer,
  traded against server re-scan CPU", positioned against (i) node-version sets (membership-only) and
  (ii) precision locking (no re-scan, O(writes), the strongest rival baseline to implement and compare).
- **Scale evidence**: Staring into the Abyss (PVLDB 8(3) 2014): OCC at 1000 cores bottlenecks on local copying,
  abort cost, timestamp allocation; "no single CC scheme performed best for all workloads". MOCC (PVLDB 10(2)):
  per-page temperature decides optimistic vs pessimistic reads — precedent for *per-range validation-policy
  selection* (small range → per-key; large cold → receipt; large hot → predicate check / snapshot).

## 7. Post-2022 operator pushdown to disaggregated memory/storage (CXL era)

- **Smart-memory camp (Helios-aligned)**: TELEPORT (SIGMOD 2022, verified): OS-level `pushdown(func)` to memory-
  pool CPUs with pushdown-aware coherence; simple memory-intensive operators (selection/projection/aggregation);
  2.1–5.5x per-operator vs LegoOS, "order of magnitude" system-level headline. FarView (CIDR 2022) → PULSE
  (ASPLOS 2025): pointer traversals pushed to FPGA NICs — academic confirmation that **index traversal is the
  canonical un-cacheable pushdown** (Helios's probe steps already exploit this in software). λ-IO (FAST 2023):
  *dynamic, cost-based host-vs-device dispatch* of filter/transform kernels — the lesson for Helios's static
  gates. DDS (PVLDB 17, 2024, Microsoft): DPU-offloaded storage data path (order-of-magnitude latency cut, tens
  of host cores saved) — attacks RPC-path CPU, orthogonal to plan content. HTAP-on-CSD (PVLDB 16, 2023): naive
  scan offload breaks freshness/consistency unless coordinated with the buffer pool — same bug class Helios
  already fixed (filter-rejected rows still OCC-validated).
- **Passive-memory camp (one-sided RDMA, anti-pushdown)**: Sherman (SIGMOD 2022), SMART (OSDI 2023), ROLEX
  (FAST 2023 best paper), FUSEE (FAST 2023), Ditto (SOSP 2023), Motor (OSDI 2024, one-sided MVCC), CHIME
  (SOSP 2024: 5.1x at equal cache or 8.7x less cache — quantifies the cost of passivity as read-amplification
  vs cache footprint), SepHash (PVLDB 17), DEX (VLDB 2024: 1.7–56.3x; *cost-aware selective offloading*).
  Premise (no CPU at memory side) is false for Helios; what transfers is the **compute-side hot-index caching
  discipline** (CHIME/DEX) — proxy-side analog of negative caching/FER covering.
- **CXL**: Lerner & Alonso, "CXL and the Return of Scale-Up Database Engines" (PVLDB 17, 2024): at CXL latencies
  (~300–500 ns vs RDMA 1–5 µs) operator placement loses importance — the strongest argument *against* pushdown,
  but only inside a hardware-coherent pod. PolarCXLMem (SIGMOD 2025 industrial best paper: CXL-switch memory
  for PolarDB, 2.1x vs RDMA pooling — memory stays passive), Tigon (OSDI 2025: CXL pod DB using CXL *atomics*
  only — 2.8x/14.4x), SAP HANA CXL study (PVLDB 17). Score: 4-of-5 verified CXL-DB papers place no compute near
  memory. **Net: Helios's regime (μs-RPC software server) is exactly where pushdown wins; if the storage tier
  ever becomes CXL-attached, the one-RPC plan executor loses differentiation — a positioning risk to state, not
  an action item.**
- Nonexistent / do-not-cite (verification): "PolarDB-CX", "GaussDB fragment shipping", "The Case Against
  One-sided" (no such post-2022 titles); FilterJoin (no such system). Survey of record: Yu et al.,
  "Disaggregation: A New Architecture for Cloud Databases" (PVLDB 18, vision).

---

## 8. Ranked Helios-applicable proposals

Legend — Size: S (<1 wk), M (1–3 wk), L (multi-week / cross proxy+server / CC core). Risk: to 1SR / server-memory
constraints. "In Helios": ✅ present, ◐ partial, ✗ absent.

| # | Technique | Source(s) | Helios mapping (sketch) | Expected benefit | Size | Risk | In Helios |
|---|---|---|---|---|---|---|---|
| 1 | **Tx-scoped read-plan via reconnaissance/dry-run** | Calvin OLLP (SIGMOD'12), Chardonnay (OSDI'23), H-Store (VLDB'07) | First execution of a TPC-C statement/tx template = recon; proxy records the union read-plan as a *parameterized template* (bindings on w_id/d_id/c_id, like B*.K); later executions ship it via `@_tx_plan` in one RPC; uncovered reads fall back per-statement; Silo validation = safety net (misprediction → perf, never correctness) | TPC-C: coverage ~10% → most of NewOrder/Payment/OrderStatus; goodput recovery (137→74 regression reversed); round trips → ~2/tx | L | Low (validation unchanged); must fix MRR staging defect first | ◐ `@_tx_plan` path + per-statement plans exist; once-per-tx staging is the defect |
| 2 | **Precision-locking validation (predicate check vs epoch write window)** | HyPer (SIGMOD'15); precision locks (SIGMOD'81); FoundationDB resolvers (analogous) | Ship the scan's range/filter predicates (already compiled for pushdown!) as the read footprint; at commit, server checks predicates against write-sets of transactions committed in [begin,commit] (LineairDB epoch buffers ≈ undo-buffer analog); validation O(concurrent writes), independent of scan size; phantoms included; own writes skipped trivially | Kills per-key range materialization (the 20x / 6M-key bloat) AND validation CPU for analytic scans in RW txns; TPC-C: small constant overhead (HyPer: 5–7%) | L (LineairDB core; submodule branch rule) | Med impl. risk; 1SR sound (published proof); needs retained per-epoch write-sets (bounded) | ✗ (rangehash receipt is the in-house alternative) |
| 3 | **RANGEHASH receipt → RW extension (own-write overlay)** | Gap verified (no published digest-OCC); Silo node-set v_old→v_new rule (SOSP'13) as the reconciliation template; Hekaton scan-repeat as the honest framing | Commit re-scan computes digest over (key,tid,found) with the tx's own write-set overlaid (skip own-TID rows / substitute pre-images); per-range policy à la MOCC/FaRM-t_r: small→per-key, large cold→receipt, hot→predicate/per-key | OLTP validation payload O(1) per range; proxy/server memory; enables tx-plan (#1) to validate cheaply | M | Hash-collision soundness argument required (prior Codex NO-GO) — must be addressed head-on (e.g., 256-bit digest + formal collision bound); 1SR otherwise sound | ◐ read-only scope implemented (HELIOS_RANGEHASH_OCC) |
| 4 | **In-RPC predicate transfer (forward+backward filter passes as plan steps)** | Predicate Transfer (CIDR'24: 3.3x vs Bloom join), RPT (SIGMOD'25: 1.6x worst/best), LIP (PVLDB'17: adaptive ordering), Snowflake/Redshift adaptive BFs | Add "build-filter" plan steps (scan → emit min/max + small IN-set or Bloom on join keys) wired into later steps' filter slots; forward pass = topological step order, backward = second pass over reduced tables; server reorders filter application by observed selectivity; cost-gate per magic-sets lesson | TPC-H q21/q18/q5-class row reduction beyond current one-direction semijoin; join-order robustness; **single-shipment SIP = novelty claim** | M | Low (filters conservative; OCC re-verifies; Bloom interacts with validation like filter-rejected rows — precedent exists) | ◐ semijoin/membership reduction = exact-set forward-only; SharedScan dedup |
| 5 | **Storage-side zone maps (per-range min/max "storage indexes")** | Exadata storage indexes (docs), Snowflake pruning (SIGMOD-C'25: 79–99.99% probe pruning), DuckDB zone-map consumption | LineairDB maintains automatic per-key-range min/max for hot columns (built during scans, no DDL); scan steps + runtime filters (#4) skip ranges before per-row filter eval; PK-ordered ranges play the micro-partition role | Attacks the measured residual bottleneck (server scan CPU: q1 4.0s at zero transfer); multiplies #4's effect | M | Low; small bounded server memory (must budget; Exadata uses 1MB regions) | ✗ |
| 6 | **Lightweight column encodings as wire+staging format (drop generic LZ4)** | "Don't Hold My Data Hostage" (PVLDB'17: localhost→no compression; PFOR/binpack dominate Snappy), BtrBlocks (SIGMOD'23), FastLanes (PVLDB'23) | Replace "LZ4 wire compression" lever with dict/FOR/RLE/bit-packing in the flat codec; keep rows encoded in proxy staging caches (decode lazily at MySQL row materialization) | Simultaneously: ship bytes, serialization CPU, and staged working-set memory (the 5–6x copy problem); literature says generic LZ4 would *lose* on loopback | M–L | None to 1SR; engineering risk in codec | ◐ flat codec + projection pushdown exist; **revises the internal LZ4 lever: deprioritize** |
| 7 | **Validity-ranges + result-size budget with adaptive requery** | POP/POP-FED (SIGMOD'04/VLDB'06), SQL Server adaptive join (docs), Smooth Scan (ICDE'15), Plan Bouquets (SIGMOD'14); circuit-breaker = published gap | Stamp each plan step with expected-cardinality range + byte budget; server detects violation mid-plan, stops, returns partial stats; proxy recompiles (≤k re-RPCs, MSO-style bound); optional threshold-branch steps (point-reads vs scan) decided server-side | Robustness of whole-QEP shipping without magic tuning; hard cap on server/proxy memory per statement (the OOM class); **"result-size circuit breaker" is a publishable gap** | M | Low; fallback machinery exists | ◐ abort→fallback + full-S downgrades exist (static, not feedback-driven) |
| 8 | **Epoch snapshot reads for RO statements (mechanism, not gate)** | Silo snapshots (SOSP'13), Cicada RO path (SIGMOD'17), Chardonnay epochs (OSDI'23) | Bounded multi-versioning at epoch boundaries in LineairDB (epochs already exist); RO autocommit statements read snap(e) and skip validation *by construction*; RW transactions keep current path (or pair with #2 for in-RW scans) | Replaces operator-gated RO_NOVALIDATE with a principled 1SR-consistent mechanism (defensible in a paper); zero validation payload for analytics | L (CC core) | Version-store memory must stay bounded (Silo: versions preserved only across snapshot boundaries); 1SR sound for RO | ◐ RO_NOVALIDATE gate exists (no mechanism) |
| 9 | **Borrowed-span / zero-copy staging** | Arrow Flight (verified: zero-*transformation*, receive buffer referenced directly), FlatBuffers-style access | Proxy references the RPC receive buffer as the staging cache backing store (span + lifetime mgmt); server-side: serve scan results from record memory without intermediate materialization (stream) | Proxy ingest CPU + the 5–6x working-set copies | M | None to 1SR; lifetime bugs | ◐ identified lever; A(server stream)/B1(proto removal) done, B2(zero-copy) open |
| 10 | **Per-segment hybrid cache+pushdown (cost-weighted retention)** | FlexPushdownDB (VLDB'21: hybrid 2.2x over either pure strategy; W-LFU +37%), Oracle CRC (pessimistic analog) | Proxy retains hot table segments across statements/transactions; read-plan covers only uncovered ranges; staged segments validated at commit via receipts (#3) — the OCC answer to semantic-cache invalidation | TPC-C re-reads (warehouse/district rows), repeated analytic templates; less server scan work | L | Proxy memory must be budgeted; correctness rests on #3 | ◐ statement-scoped staging + negative cache + FER covering exist (no cross-statement reuse) |
| 11 | **Cross-query cooperative scans at the server** | Cooperative Scans (VLDB'07), Crescando (PVLDB'09), QPipe (SIGMOD'05: 2x), SharedDB (PVLDB'12) | Server merges concurrent read-plan scan steps over the same table (relevance-ordered chunks, mid-scan join-in); natural extension of morsel-parallel Track-B + SharedScan to *inter-query/inter-proxy* | Server scan CPU under concurrency — the shared-storage-many-proxies scenario; today's benefit limited (single-proxy benches) | L | Snapshot consistency per joining plan must be respected (epoch-tagged chunks); none to 1SR (validation unchanged) | ◐ SharedScan = intra-query only; morsel-parallel = intra-scan |
| 12 | **Cost-feedback calibration loop (per-template stats piggyback)** | Du et al. (VLDB'92), postgres_fdw `use_remote_estimate`, HeatWave Autopilot (docs), Spark AQE (cross-query analog), POP validity ranges | RPC responses carry observed per-step cardinalities/bytes; proxy persists per-template stats to refine COST_V2 constants and plan-shape choice on next execution (between-query AQE — no mid-plan reopt needed) | Fewer bad pushdown shapes; TPC-C/TPC-H templates repeat heavily; no magic tuning (self-calibrating) | M | None | ◐ COST_V2 statically calibrated |
| 13 | **Capability-negotiated pushdown allowlist** | TiDB per-expression allowlists (docs), Trino connector refusal (docs), Exadata passthru counters | Versioned proxy↔server capability handshake for expression/step types; per-reason fallback counters exposed via SHOW STATUS (formalizes the phase-14 investigation method) | Operational hardening + experiment observability; protects against silent semantic drift | S | None | ◐ ad-hoc gates + full-S fallback exist |
| 14 | **Two-round sampling top-N pushdown** | PushdownDB (ICDE'20) | Round 1: server-side sample estimates k-th cutoff; round 2 (same or next RPC): filter ≥cutoff | TopN under filter without server sort — low value on TPC-H (LIMIT sits above aggs), possible TATP/utility value | S | Low; 2nd RPC conflicts with one-shot purity (gate it) | ✗ (topN/limit pushdown exists for simple cases) |
| 15 | **GroupedSummary / ExactAntiMap (assessment update)** | Magic sets cost-gating (SIGMOD'96), anti-join Bloom asymmetry (§3), Diamond joins (PVLDB'24), partial-agg envelope (phase-10 survey) | As roadmapped (S2/S3); new inputs: front ExactAntiMap with a Bloom (misses emit immediately), cost-gate GroupedSummary per magic-sets lesson | Memory (hundreds of MB ship on q15/q18/q21) more than latency (scan-floor-bound, ~11% suite) — matches internal measurement | L | Wire/codec changes (supervised, per internal note) | ◐ designed, partially Codex-GO'd, unimplemented |

**Suggested order of attack** (respecting measured bottlenecks): #1 (+ the phase-14 MRR fix) for TPC-C;
#3 then #2-as-baseline for the validation/memory axis (they are rival mechanisms — implementing #2 as the
comparison baseline strengthens #3's paper); #4+#5 together for the TPC-H scan floor; #6/#9 for the memory axis;
#7/#12/#13 as low-risk hardening that also generates paper-grade observability.

## 9. Cross-check: proposals already (partially) present in Helios

Explicit mapping per the deliverable:

- **Already implemented (✅)**: filter pushdown (expression trees server-side), projection pushdown (multi-ref
  union + LZ4'd metadata), top-level single-table aggregation pushdown (proxy- and server-side exact-decimal),
  semijoin/membership reduction incl. scalar-subquery extension, SharedScan intra-query dedup, probe dedup,
  negative caching (non-existence proofs from result_keys), disaggregated cost model (COST_V2), flat >2GB codec,
  morsel-parallel server scans (Track-B), statement-scoped one-RPC plans, OCC-meta strip for agg steps,
  RANGEHASH receipts (read-only).
- **Partially present (◐)** — see table column; the notable *reassessments* this survey forces:
  - **LZ4 wire compression (internal lever) → deprioritize**: VLDB'17 protocol study says no compression on
    localhost-class links; lightweight column encodings (#6) replace it and also attack staging memory.
  - **Bloom-filter semijoin (internal lever) → upgrade, don't restate**: the win shape per literature is
    min/max + small IN-set consumed at *range/zone* granularity (#4+#5), with adaptive ordering and cost
    gating; plain Bloom alone is the weakest variant.
  - **Aggregation/GroupedSummary** — confirmed established (phase-10 survey); new evidence says its Helios value
    is memory, not latency (#15), consistent with internal TIMEPROF findings.
- **Genuinely absent and high-value (✗→)**: precision-locking validation (#2), storage-side zone maps (#5),
  validity-range/result-budget requery (#7), per-template cost feedback (#12).

## 10. Positioning notes for a publication

1. **Novelty claims that survived adversarial verification**: (a) single-shipment SIP — build *and* consume
   runtime filters inside one storage-side plan execution (no published equivalent); (b) digest/receipt-based
   OCC read validation (no published OCC system validates via version-set hashes); (c) result-size circuit
   breaker for pushdown (no published equivalent); (d) strictly-serializable RW transactions over a one-RPC
   compiled read-plan against disaggregated memory — not replicated in any verified 2022–2026 top-venue system
   (TELEPORT is OS-level/non-transactional; the one-sided RDMA camp is KV-level; HeatWave is RO-analytics
   offload without an academic paper).
2. **Strongest rival baselines to implement/compare**: HyPer precision locking (validation), FlexPushdownDB
   hybrid (caching-vs-pushdown), Predicate Transfer/RPT (SIP), Chardonnay (tx-plan prefetch).
3. **Honest threat to the thesis**: the CXL camp (Lerner & Alonso, PVLDB'24) argues placement stops mattering at
   ~300 ns; Helios should scope its claims to the μs-RPC software-disaggregation regime — where FarView, TELEPORT,
   PushdownDB, and Aurora-PQ all independently confirm pushdown pays.

## Appendix: verification log (adversarial pass)

- CONFIRMED: Chardonnay dry-run/epoch/15%-vs-85% (OSDI'23 PDF); H-Store 82x (70,416 vs 850 tps, VLDB'07);
  HeatWave dual-condition offload gate (Oracle technical brief); S3 Select closed to new customers 2024-07-25
  (AWS blog); AQUA quietly absorbed, docs deleted (Wayback); Aurora PQ pushes projection + WHERE/join filtering +
  function processing, returns "compact tuples", bypasses buffer pool (AWS docs); Snowflake 79%/99.99%/99.4%
  pruning figures (SIGMOD-C'25); TELEPORT mechanism + order-of-magnitude headline (2.1–5.5x per-operator).
- CORRECTED (figures fixed in this report): Predicate Transfer = **3.3x** avg vs Bloom join (not 3.1x), authors
  Yang/Zhao/Yu/Koutris (no Stonebraker); RPT worst/best ratio = **1.6x** (not 2.8x); LIP = robustness result
  (14 s → 150 ms gap; ~13% avg best-time improvement on SSB; 4.0x geomean only on the TPC-H Q8 subplan space);
  Bitvector-aware QO = 22–64% CPU on **TPC-DS/JOB/CUSTOMER** (not TPC-H), system anonymized as DBMS-X;
  PolarDB-IMCI venue = SIGMOD 2023 (PACMMOD), not VLDB; TELEPORT title = "Optimizing Data-intensive Systems in
  Disaggregated Data Centers with TELEPORT".
- REFUTED / do-not-cite: top-venue "MySQL HeatWave" academic paper (does not exist; PVLDB 13–18 + DBLP sweep);
  "PolarDB-CX"; "GaussDB fragment shipping"; "The Case Against One-sided"; "FilterJoin".
- UNVERIFIED (flagged inline): Aurora '17 exact multipliers; VoltDB 45x; Sprint EuroSys'07 multipliers; Morty
  exact gain range; Pyxis 3x; InfluxDB IOx Flight adoption; Redy/Rcmp/Outback/UDON/Oasis/Pixels-Turbo venues;
  FoundationDB resolver detail; Wu et al. MVCC-eval detail; DirectCXL detail; Starling numbers; Cicada contended
  TPC-C exact figure; SmartSSD post-2022 SQL offload.

### Primary sources (selection)

Chardonnay https://www.usenix.org/system/files/osdi23-eldeeb.pdf · Calvin https://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf ·
H-Store https://hstore.cs.brown.edu/papers/hstore-endofera.pdf · Aria http://www.vldb.org/pvldb/vol13/p2047-lu.pdf ·
Janus https://www.usenix.org/system/files/conference/osdi16/osdi16-mu.pdf · Morty https://dl.acm.org/doi/10.1145/3552326.3567500 ·
Forerunner https://yajin.org/papers/sosp2021_forerunner.pdf · QURO https://www.vldb.org/pvldb/vol9/p444-yan.pdf ·
Sloth https://people.eecs.berkeley.edu/~akcheung/papers/tods16.pdf · HyPer MVCC https://db.in.tum.de/~muehlbau/papers/mvcc.pdf ·
Silo https://people.csail.mit.edu/stephentu/papers/silo.pdf · TicToc https://people.csail.mit.edu/sanchez/papers/2016.tictoc.sigmod.pdf ·
Cicada https://hyeontaek.com/papers/cicada-sigmod2017.pdf · ERMIA https://www.cs.sfu.ca/~tzwang/ermia.pdf ·
MOCC http://www.vldb.org/pvldb/vol10/p49-wang.pdf · FaRM https://pdos.csail.mit.edu/6.824/papers/farm-2015.pdf ·
Hekaton CC http://vldb.org/pvldb/vol5/p298_per-akelarson_vldb2012.pdf · Abyss https://www.vldb.org/pvldb/vol8/p209-yu.pdf ·
LIP https://www.vldb.org/pvldb/vol10/p889-zhu.pdf · Bitvector QO https://www.microsoft.com/en-us/research/wp-content/uploads/2020/05/bqo_sigmod_cr.pdf ·
Predicate Transfer https://www.cidrdb.org/cidr2024/papers/p22-yang.pdf · RPT https://arxiv.org/abs/2502.15181 ·
Yannakakis+ https://arxiv.org/abs/2504.03279 · Diamond joins https://www.vldb.org/pvldb/vol17/p3215-birler.pdf ·
Not-Yannakakis CIDR'26 https://www.vldb.org/cidrdb/papers/2026/p29-zhao.pdf · Bernstein-Chiu https://dl.acm.org/doi/pdf/10.1145/322234.322238 ·
Protocol study https://www.vldb.org/pvldb/vol10/p1022-muehleisen.pdf · Arrow Flight bench https://arxiv.org/abs/2204.03032 ·
Cooperative Scans https://www.vldb.org/conf/2007/papers/research/p723-zukowski.pdf · QPipe http://www.cs.cmu.edu/~StagedDB/papers/qpipe.pdf ·
BtrBlocks https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf · FPDB https://vldb.org/pvldb/vol14/p2101-yang.pdf ·
PushdownDB https://arxiv.org/abs/2002.05837 · Redshift https://assets.amazon.science/93/e0/a347021a4c6fbbccd5a056580d00/sigmod22-redshift-reinvented.pdf ·
Snowflake pruning https://arxiv.org/html/2504.11540v1 · PolarDB Serverless https://users.cs.utah.edu/~lifeifei/papers/polardbserverless-sigmod21.pdf ·
PolarDB CSD https://www.usenix.org/conference/fast20/presentation/cao-wei · FarView https://www.cidrdb.org/cidr2022/papers/p11-korolija.pdf ·
TiDB https://www.vldb.org/pvldb/vol13/p3072-huang.pdf · SingleStore https://dl.acm.org/doi/10.1145/3514221.3526055 ·
Aurora PQ docs https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-mysql-parallel-query.html ·
Exadata offload docs https://docs.oracle.com/en/engineered-systems/exadata-database-machine/sagug/offloading-data-search-and-retrieval-processing.html ·
HeatWave brief https://www.oracle.com/a/ocom/docs/mysql-heatwave-technical-brief.pdf · postgres_fdw https://www.postgresql.org/docs/current/postgres-fdw.html ·
POP-FED https://www.vldb.org/conf/2006/p1175-kache.pdf · Plan Bouquets https://dsl.cds.iisc.ac.in/publications/conference/bouquet.pdf ·
Du-Krishnamurthy-Shan https://www.vldb.org/conf/1992/P277.PDF · Presto https://trino.io/Presto_SQL_on_Everything.pdf ·
Bao https://dl.acm.org/doi/10.1145/3448016.3452838 · TELEPORT https://www.cs.rice.edu/~angchen/papers/sigmod-2022.pdf ·
DDS https://arxiv.org/abs/2407.13618 · λ-IO https://www.usenix.org/conference/fast23/presentation/yang-zhe ·
CXL scale-up https://www.vldb.org/pvldb/vol17/p2568-lerner.pdf · Tigon https://www.usenix.org/conference/osdi25/presentation/huang-yibo ·
PolarCXLMem https://dl.acm.org/doi/10.1145/3722212.3724460 · CHIME https://dl.acm.org/doi/10.1145/3694715.3695959 ·
DEX https://arxiv.org/abs/2405.14502 · Disaggregation vision https://www.vldb.org/pvldb/vol18/p5527-xiangyao.pdf
