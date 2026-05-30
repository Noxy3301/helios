# Phase-10 Pushdown Sweep（正準リスト・Helios対応・実測・結論）

席外し中の自律調査（2026-05-31）。確立した pushdown 技術を網羅し、Helios に採用可能なものを
プロトタイプ&計測した記録。Codex(gpt-5.5)と私(web)の調査一致。

## 1. 正準 Pushdown リスト（Trino connector SPI / Spark DataSourceV2 が事実上の標準）
filter / projection / dereference / aggregation / join / semi-join(runtime filter/Bloom) /
anti-join(existence) / TopN / limit / sort / distinct / computed-expression / sample /
metadata(zone-map)pruning / flexible(separable)operator。
出典: Trino `optimizer/pushdown` + connector SPI(applyFilter/applyAggregation/applyTopN/applyLimit/applyJoin),
Spark `SupportsPushDown{Filters,Aggregates,Limit,TopN,Join,RequiredColumns,TableSample}`,
Calcite CoreRules, PushdownDB/FlexPushdownDB(VLDB), MS Data-Induced Predicates(VLDB'20).

## 2. Helios 対応（状態 / 効くTPC-H / 行数削減? / 難度）
| カテゴリ | 状態 | 行数削減 | 備考 |
|---|---|---|---|
| Filter/predicate | ✅(単一表WHERE + ネストOR必要条件) | 〇 | |
| Projection | ✅ | ✕(列幅) | 時間ROI低 |
| Aggregation(top-level単一表) | ✅(SUM/COUNT/AVG, Phase A/B) | 〇 | q1/q6 |
| **subquery/CTE/multi-table aggregate** | ❌ | 〇〇 | q11/q15/q18/q20。GroupedSummary=materialization intercept(重) |
| Join | ✅(oneshot prefetch=join pushdown) | 〇 | |
| Semi-join/runtime filter | ✅(P0/P1, nested-OR transfer) | 〇 | q5/q7/q8/q9/q10。q9 6M→319k実績 |
| **Anti-join/existence(NOT EXISTS)** | ❌ | 〇 | q4/q16/q21/q22。ExactAntiMap(重, NULL-aware) |
| Duplicate-fetch dedup(scan sharing/CSE) | ✅(同一(table,key)畳み込み) | 〇 | q21 3→1, 27→13s |
| TopN/Limit | ✅部分(単一表・順序付きLIMIT) | △ | TPC-HのLIMITは集約/join後で効きにくい |
| Sort/Distinct/Sample/Dereference | 部分/N-A | — | TPC-H低ROI |
| **OCC-meta strip(RO時)** | ✅実装(agg step限定, commit 031904a) | 転送/メモリ | 下記 |
| Metadata/zone-map pruning | ❌ | 〇 | server storage変更要。l_shipdate等の選択scan |

## 3. 試して計測したこと
### (a) agg step での OCC メタ strip【commit 031904a, 採用】
RO_NOVALIDATE時、aggregate step の range_versions/scan_tids/index_reads は proxy が捨てる無駄。
q1/q6 は server 集約(4 group値)なのに**範囲検証用 result_keys 6M鍵=257MBをship**していた(serialize の大半)。
agg step限定でstrip(他stepはnegative-cacheがresult_keys使用で不可)→ **q1/q6 転送 257MB→0, 22/22 md5一致**。
q1 5.1→4.0s(InnoDB超え0.7x), q6 3.5→2.7s。**但し suite合計(3.1x)・peak memory(14.6GB)はほぼ不変**
(peakは最重量 q21/q18/q15 が決め、これらは agg step でないので対象外)。

### (b) 重要な発見: 残存 latency は server側 scan+集約 CPU、転送でない
TIMEPROF分解(SF=1): full-lineitem系の server内訳 = db scan 1.2-1.6s + **集約/serialize処理 1.2-3.0s**(6M行のexact-decimal集約CPU等) + send。
q1 は転送0でも 4.0s = **server側で6M行をscan+集約するCPUが床**。pushdown(行数削減)では消せない。
→ q11/q22の高比率も「転送byte律速」でなく「行数(per-row CPU: server scan + proxy ingest)律速」。
別実験(全step OCC strip)は転送-14〜27%でも**時間0%変化**、かつ q18 md5破綻(negative-cache)で確認済。

### (c) TopN/Limit は既実装・TPC-H低価値、subquery agg(GroupedSummary)とanti-join(ExactAntiMap)が未実装の本命
だが両方 row-count を減らしても、上記(b)の通り**server側 scan+集約 CPU 床**が残るため、
期待値は「ship/ingest分の削減(数秒)」で、qごとの絶対床(~3-4s for 6M行)は残る。

## 4. 結論・推奨（戻った時の判断材料）
- pushdown(行数削減)の主要分は概ね harvest 済(semijoin/dedup/agg/OCC-strip)。suite 8x→3.1x。
- **残存 latency の支配は server側の大表 scan + exact-decimal 集約 CPU(~3-4s/重query)**。これは pushdown でなく
  **server 実行エンジンの高速化**(並列scan, 高速集約, columnar, zone-map pruning で scan 自体を減らす)が次の軸。
- pushdown で更に取るなら **GroupedSummary(q15/q18, materialization intercept=重)** と **ExactAntiMap(q22/q21)**。
  ただし上記床により latency 効果は限定的(ship/ingest削減分)。**メモリ削減**には効く(q18/q15/q21 の数百MB ship を群/存在集合に)。
- **メモリ 5x(14.6 vs 2.7GB)** は peak を決める q18/q15/q21 の大表 ship が主因 → GroupedSummary/ExactAntiMap が効く軸。
