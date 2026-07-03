# TPC-H SF=5 analysis — PAX secondary engine vs DuckDB, loss & memory decomposition

Measurement + analysis only (no optimization commits). Build = `helios/pax-qep-guided`
HEAD `8bb3074` (E17). Box: Intel Xeon Gold 6326 (64 vCPU, governor=performance),
125 GB RAM. Method matches the champion recipe (`bench/bin/tpch_wall.sh`,
single-stream, 1 warm + 3 timed runs, min-of-3, external mysql-client wall clock;
secondary engine reached via the session default `use_secondary_engine=ON`, which
EXPLAIN confirms offloads every heavy block to `LINEAIRDB_COLUMNAR`). DuckDB 1.5.4
(in-process, 64 threads, official `tpch` extension queries, `CALL dbgen(sf=5)`,
min-of-3). The two engines were measured **sequentially** (DuckDB first, connection
closed to free RAM, then a fresh stack + SF=5 load).

SF=5 load: fresh restart → benchbase load (loader-threads 16) **229.9s** →
23-index postload + secondary attach/load + ANALYZE → total setup **588s**.
`lineitem = 29,999,795` rows. **Correctness** (SF=5 too large for a full 22-query
md5 vs the LineairDB row engine): q1/q6/q17 md5 FORCED-vs-primary(OFF) all **MATCH**;
the SF=0.01 / SF=1 gates (22/22) carry the rest. All 22 PAX-SE-SF5 md5s are stable
across the 3 runs (q10 tie-boundary aside). **q11 returns 0 rows on FORCED = ON =
OFF alike** — benchbase hardcodes the q11 threshold `0.0001` (TPC-H spec is
`0.0001/SF`), so at SF=5 no partkey qualifies; this is a query-text quirk, not a
PAX defect (both engines agree).

---

## (a) SF=5 PAX-SE vs DuckDB — per query, totals, trend

Times are min-of-3 in **ms**. `r5` = PAX/DuckDB at SF=5, `r1` = same at SF=1
(PAX = E17 `PAX-SE-E17-sf1.tsv`; DuckDB = `duckdb_sf1.log`). `PAXscale` = PAX
SF5/SF1 (5× more data).

| q   | PAX SF5 | Duck SF5 | r5 (PAX/Duck) | PAX SF1 | Duck SF1 | r1 | PAXscale |
|-----|--------:|---------:|-------:|--------:|---------:|-----:|--------:|
| q1  | 1164 | 35  | 33.3× | 248  | 14 | 17.7× | 4.7× |
| q2  | 72   | 23  | 3.1×  | 38   | 12 | 3.2×  | 1.9× |
| q3  | 947  | 51  | 18.6× | 192  | 15 | 12.8× | 4.9× |
| q4  | 1829 | 61  | 30.0× | 358  | 16 | 22.4× | 5.1× |
| q5  | 518  | 43  | 12.0× | 116  | 14 | 8.3×  | 4.5× |
| q6  | 196  | 8   | 24.5× | 78   | 3  | 26.0× | 2.5× |
| q7  | 1182 | 54  | 21.9× | 231  | 17 | 13.6× | 5.1× |
| q8  | 664  | 48  | 13.8× | 143  | 17 | 8.4×  | 4.6× |
| q9  | 965  | 83  | 11.6× | 220  | 48 | 4.6×  | 4.4× |
| q10 | 1594 | 75  | 21.3× | 363  | 33 | 11.0× | 4.4× |
| q11 | 287* | 9   | –     | 87   | 8  | –     | –    |
| q12 | 276  | 38  | 7.3×  | 90   | 15 | 6.0×  | 3.1× |
| q13 | 1740 | 107 | 16.3× | 234  | 41 | 5.7×  | 7.4× |
| q14 | 266  | 43  | 6.2×  | 97   | 12 | 8.1×  | 2.7× |
| q15 | 735  | 23  | 32.0× | 158  | 6  | 26.3× | 4.7× |
| q16 | 454  | 79  | 5.7×  | 131  | 36 | 3.6×  | 3.5× |
| q17 | 156  | 42  | 3.7×  | 52   | 17 | 3.1×  | 3.0× |
| q18 | 9086 | 109 | **83.4×** | 1680 | 28 | 60.0× | 5.4× |
| q19 | 1980 | 86  | 23.0× | 350  | 25 | 14.0× | 5.7× |
| q20 | 2685 | 30  | **89.5×** | 563  | 15 | 37.5× | 4.8× |
| q21 | 5822 | 224 | 26.0× | 826  | 60 | 13.8× | 7.0× |
| q22 | 282  | 37  | 7.6×  | 74   | 22 | 3.4×  | 3.8× |
| **TOTAL** | **32.90 s** | **1.31 s** | **25.2×** | **6.33 s** | **0.47 s** | **13.4×** | **5.2×** |

\* q11 = 0 rows at SF=5 (benchbase threshold quirk, see above); excluded from ratios.

**Trend (the headline).** PAX total scales **5.2×** for 5× data (slightly
super-linear); DuckDB scales **2.8×** (0.47 → 1.31 s — strongly sub-linear:
its vectorized, radix-partitioned, 64-thread engine amortizes fixed cost and keeps
finding parallelism as cardinality grows). Consequently the **PAX/DuckDB gap nearly
doubles from 13.4× (SF=1) to 25.2× (SF=5)**. The queries that widen most are the
high-cardinality aggregations: **q20 (89×), q18 (83×)**, then the scan-bound
q1/q15/q4 (30-33×). The queries that scale worst *within PAX* are q13 (7.4×) and
q21 (7.0×) — multi-stage group-by / multi-semijoin. PAX stays within ~4-8× of
DuckDB only where the answer is tiny and index-driven (q17 3.7×, q16 5.7×,
q14 6.2×, q12 7.3×).

---

## (b) Loss decomposition — where the PAX time goes (perf, SF=5, FORCED)

`perf record -F 999 --call-graph dwarf` on `lineairdb-server` while hammering each
query. Percentages are **self-time** attribution mapped onto named buckets (q18/q20
sample across a worker + main thread, so their absolute self-% is spread thinner and
these are directional). Two query *shapes* emerge.

**Scan/filter/scalar-agg shape (q1, q6): dominated by the ASCII tax.** Cells store
`val_str` **text**, so every scanned row re-parses its numeric columns
(`dec_parse`, `strtod`/`extract_value`) and re-runs a generic predicate evaluator.

| bucket | q1 (1164ms) | q6 (196ms) |
|---|---:|---:|
| ASCII numeric parse (`dec_parse`, `strtod`, `extract_value`, mpn/round) | 25% | 29% |
| decimal arithmetic (`eval_arith`, `dec_addsub` — SUM(expr)) | 27% | 2% |
| predicate eval (`evaluate`, `compare`, `memcmp`, temp string) | 4% | 35% |
| scan visibility + cell fetch (`RunScan`, `set_row_from_pax_cols`, `value_of`) | 19% | 29% |
| hash agg (`AccumulateRangeT`, GroupMap, string key build) | 14% | ~0% |
| memcpy / page-fault / other | ~11% | ~5% |

For q1 the SUM over four DECIMAL columns means ~**52% is ASCII-decimal parse +
decimal arithmetic** (`dec_parse` 22% + `eval_arith` 19% + `dec_addsub` 8%). For q6
(pure filter+scalar-SUM) ~**58% is ASCII parse + predicate eval** and ~**29% is cell
fetch**; there is essentially no aggregation. **A typed-cell store deletes the top
two buckets of both queries** — parse-once/native-decimal, and it is the same change
as memory candidate #2 below.

**High-cardinality GROUP BY shape (q18, q20): dominated by the hash table.**
Keys are already typed (int64 for q18's `o_orderkey`, packed `Int2Key` for q20's
`(l_partkey,l_suppkey)`), so the cost is the hash aggregation machinery itself over
1.5M / large group counts, plus the sideways semi-filter probes.

| bucket | q18 (9086ms) | q20 (2685ms) |
|---|---:|---:|
| hash agg (group map find/insert, `AccumulateRange`, `AggregateAndEmit`, `AggSlot`, `dec_format`) | ~40%† | ~23% |
| semi key collection / probe (`_Hashtable<long,long>`, string set) | ~7% | ~12% |
| scan visibility + cell fetch | ~5% | ~11% |
| ASCII numeric parse | ~3% | ~9% |
| predicate eval | – | ~7% |
| join (`RunJoin`) | – | ~2% |
| sub-block / RPC / merge / lock / other | ~45% | ~35% |

† q18 children-view: `AccumulateRange` 17%, `AggregateAndEmit` 12%, `RunSubBlock`
17% — the group-by pipeline over ~1.5M groups (build + per-worker merge) is the
bottleneck, not ASCII parse (keys are int, few decimals per row). This is the one
query where the loss is genuinely the **serial-per-morsel hash aggregate**; DuckDB's
radix-partitioned parallel aggregate is why it wins q18/q20 by ~85×.

**Combined "why we lose where we lose"** (PAX SF=5 time × dominant cause × DuckDB
SF=5 time):

| query | PAX ms | DuckDB ms | ratio | dominant PAX cost | the fix |
|---|---:|---:|---:|---|---|
| q6  | 196  | 8   | 24× | ASCII parse + predicate over 30M rows | typed cells (candidate #2) |
| q1  | 1164 | 35  | 33× | ASCII-decimal parse + decimal arithmetic | typed cells (#2) |
| q20 | 2685 | 30  | 89× | 2-int group hash agg + semi probe | parallel/radix agg (engine) |
| q18 | 9086 | 109 | 83× | 1.5M-group hash agg + merge | parallel/radix agg (engine) |

---

## (c) Memory model vs measured + ranked compression candidates

### PAX cell layout (from the code)

`PaxGroup` (row group = 8192 rows) lays out one fixed-width column strip per field:
`stride[f] = 2 (u16 len prefix) + field_max_bytes[f]`; row footprint = Σ strides
(64B strip alignment + a 1024B visibility bitmap per group are negligible per row).
`field_max_bytes` is set at CREATE by `compute_pax_field_widths`
(`ha_lineairdb.cc:3657`) from `Field::val_str` upper bounds: **INT → 21**
(ASCII digits+sign), **DECIMAL(15,2) → 19**, **DATE → 32**, **CHAR/VARCHAR →
`field_length` = the utf8mb4 octet length = 4 × declared chars**. Everything is
stored as ASCII text (this is what the loss decomposition re-parses).

### Per-table arena model (Σ rows × Σ strides)

| table | rows (SF=5) | row bytes | arena GB |
|---|---:|---:|---:|
| lineitem | 30.0M | 614 | 18.42 |
| orders   | 7.5M  | 574 | 4.31 |
| partsupp | 4.0M  | 890 | 3.56 |
| customer | 750K  | 907 | 0.68 |
| part     | 1.0M  | 673 | 0.67 |
| supplier | 50K   | 801 | 0.04 |
| nation/region | 30 | ~750 | ~0.00 |
| **arena total** | | | **27.68** |

### Measured vs model (SF=5)

RSS was probed at two points during the one-shot load, which cleanly splits the
budget:

| component | GB | % of RSS | basis |
|---|---:|---:|---|
| **PAX arena** (row bytes in strips) | 27.68 | 72% | model above (exact) |
| **Primary index** (DataItem 48B + PL-hash `TableNode` 64B + slot array + PK strings + malloc) | 6.97 | 18% | RSS after primary load (34.65) − arena |
| **15 secondary indexes** (per-key DataItem + `PackedPrimaryKeys` PK-lists; 8 on lineitem) | 3.83 | 10% | final (38.48) − after-primary (34.65) |
| **measured RSS after load** | **38.48** | 100% | `VmRSS` |

`DataItem` is a hard 48 B/row (slim-layout `static_assert`); PAX rows keep the
`DataBuffer` as a tagged pointer into the strip (bit0=PAX) with **no heap row bytes**
— the 1-copy invariant holds (the arena is the only copy of the payload). Metadata
is ~161 B/row of primary index over 43.3M rows plus compact secondary PK-lists.
(SF=1 reference on the pre-existing long-lived stack was 10.28 GB — arena model 5.54
GB + ~4.7 GB metadata; a *clean* SF=1 load would be ~7.7 GB by the SF=5 ratios, the
delta being allocator/fragmentation baggage on the old stack. The naive
"10.3 GB × 5 = 52 GB" extrapolation overshoots because only the arena scales 5×.)

### Ranked compression candidates (arena is 72% of RSS; DO NOT implement here)

| # | candidate | SF=5 GB saved | → RSS | perf risk | notes |
|---|---|---:|---:|---|---|
| 1 | **Size string strides to bytes, not utf8mb4 octets** | **13.09** | 38.5→25.4 (−34%) | **minimal** | Strides reserve 4×chars (utf8mb4) but TPC-H data is pure ASCII: `l_comment` observed max **43 B** vs 176 B stride. Fix = declare tables `latin1`/`ascii` (zero engine change) *or* cap `field_max_bytes` at char-length (genuine multibyte rows take the existing safe heap fallback). Narrower strides also **speed** scans (less memory streamed). Highest leverage / lowest risk. |
| 2 | **Typed fixed-width numeric cells** (INT 21→4/8, DATE 32→4, DECIMAL 19→8 scaled-int) | +6.89 (19.98 total) | 38.5→18.5 (−52%) | medium | The planned typed-cell migration. Touches scatter/gather (OLTP write path) — hence medium — but **also deletes the #1 runtime bucket** (ASCII `dec_parse`/`strtod`/`extract_value` = 25-52% of q1/q6) and is the SIMD substrate. Single highest-leverage change for **both** memory and speed. |
| 3 | **Dictionary-encode low-cardinality strings** | +~1.5 (on top of #1) | ~17 (−56%) | medium | l_shipinstruct(4), l_shipmode(7), l_returnflag(3), l_linestatus(2), o_orderstatus(3), o_orderpriority(5), c_mktsegment(5), p_brand(25), p_container(40) → 1-byte codes. Most of the win is l_shipinstruct/l_shipmode on 30M lineitem. Small *after* #1 (those columns are already ≤25 B once utf8mb4 pad is gone) and adds scan indirection + write-side dictionary upkeep. Lowest priority. |

Candidates compose: #1+#2 shrink the arena from 27.68 → 7.71 GB (−72%) and the whole
server from 38.5 → ~18.5 GB (−52%), while #2 simultaneously removes the dominant
scan-time bucket. #1 alone is a near-free 34% RSS cut. #3 is optional polish.

---

## Headline conclusions

1. **The gap widens with scale.** PAX 6.33 s → 32.90 s (5.2×) vs DuckDB 0.47 s →
   1.31 s (2.8×); PAX/DuckDB 13.4× → **25.2×** from SF=1 to SF=5. PAX pays a
   per-row tax (ASCII parse, scalar hash-agg) that grows with cardinality while
   DuckDB's vectorized parallel engine amortizes it away — so raw data growth is
   PAX's worst case, concentrated in q18/q20 (~85×).

2. **Two distinct losses, one shared root.** Scan/scalar queries (q1,q6) lose
   25-52% to **ASCII value parsing** of text-encoded cells; high-cardinality
   group-bys (q18,q20) lose ~25-40% to the **serial-per-morsel hash aggregate**.
   The first is cured outright by typed cells; the second needs a
   radix-partitioned parallel aggregate (DuckDB's design).

3. **Memory is 72% row bytes, and most of that is avoidable.** Measured SF=5 RSS
   38.5 GB = arena 27.7 (72%) + primary index 7.0 (18%) + secondary indexes 3.8
   (10%), 1-copy intact (no heap row bytes). The arena is inflated by two
   ASCII/charset artifacts: **utf8mb4 4× string padding (13 GB) and text-encoded
   numerics (7 GB)**. Sizing strides to bytes (#1, near-zero risk) plus the typed
   migration (#2) cut the arena 72% and total RSS 52% — and #2 is the same change
   that removes the biggest runtime bucket. **Typed cells are the one lever that
   pays in both dimensions.**
