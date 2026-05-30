# Phase-10 Pushdown 設計（現状棚卸し + SOTA + 実装案）

disaggregated Helios（MySQL front-end proxy = LineairDB SE plugin ⇄ remote in-memory server over protobuf RPC）の
対InnoDB残差は **cost式/join順でなく転送・リモートscan律速**であることを SF=0.1 実測で確定
（plan は 19/22 で InnoDB と一致、それでも warm 3.8x / SF=1 で 8x）。残差を縮める梃子は **pushdown（転送の物理削減）**。
本書は現状の pushdown 実装の棚卸しと、SOTA に接地した実装ロードマップ。

---

## 1. 現状の pushdown（コード精読、ファイル:行）

### 1.1 Predicate pushdown
- **ワイヤ**: `proto/lineairdb.proto:812-839` `FilterExpr`（再帰式）+ `PushedPredicate`。比較6種/AND/OR/NOT/BETWEEN/IN/LIKE/ISNULL/算術。DATE は CONST_STRING "YYYY-MM-DD"（row value が ASCII 格納、lexicographic=時系列）。
- **proxy 生成**: `serialize_item()` (`proxy/ha_lineairdb.cc:1747`)。SELECT 経路は `build_single_table_filter()` (`:3354`) → `collect_driver_atoms()` (`:3326`) が **top-level AND のうち `used_tables()==me`（自テーブルのみ参照）の conjunct だけ**抽出。driver S: / FER step に late-stamp（`lineairdb_transaction.cc:359-414`）。FES は除外（`:3068`、result_keys 再構築と commit 再walk整合のため）。
- **server 評価**: `server/rpc/predicate_evaluator.cc:258`。reject 行は `route_filtered_row()` (`lineairdb_rpc.cc:41`) で filtered_keys/tids に積み OCC 検証へ。
- **できる**: 単一テーブル WHERE 範囲述語（`l_shipdate BETWEEN`）→ q1/q6/q14 の主述語は下push 済。
- **できない**: ❌ **join 由来の絞り込み**。`collect_driver_atoms` は `used_tables()==me` の atom しか残さないので、q7 の `customer.c_nationkey = nation.n_nationkey` + `nation IN (FRANCE,GERMANY)` は **drop**。→ customer の driver scan が自テーブル述語ゼロで **full(15000行)取得**、cascade で orders 全件・lineitem 大量 ship（PLANSZ 実測 resp 71MB）。

### 1.2 Aggregate pushdown（gate `HELIOS_AGG_PUSHDOWN`）
- install: `lineairdb_push_to_engine()`→`join->override_executor_func = helios_override_executor`（`:1064-1070`）。
- **発火条件** `helios_offloadable_shape()` (`:493`): SELECT のみ / **top-level query block 限定**（`outer_query_block()==nullptr` `:511`）/ UNION 無 / **単一 base table** (`:516`) / grouped。
- **除外**: DISTINCT/HAVING(`:519`)/LIMIT/ROLLUP/window / **GROUP BY キーは文字列 base column 限定**（`:529-535`、数値キー未対応）。集約は bare COUNT/SUM/AVG over INT/REAL/DECIMAL のみ。
- **Phase B（server 集約, exact int128 decimal）** `:663-877`: `tx_ro_novalidate()` 必須 / nullable group キー不可 / SUM/AVG は arg が `helios_serialize_arith()`(COLUMN_REF・整数定数・+,-,*,neg)可能時のみ。server `server_aggregate_scan()` (`lineairdb_rpc.cc:313`) が group 行を返す。
- **Phase A（proxy 集約）** `:880-1061`: 全 base 行を prefetch scan → 手元 `std::map` 集計（fallback）。
- **server は数値 group キーを raw bytes で扱える**（`:327`）が **proxy whitelist(`:532`) が文字列限定で到達しない**。HAVING は server に概念なし。
- **発火**: q1（`GROUP BY l_returnflag,l_linestatus` 文字列）✅。**q15/q18 は不発**（CTE/subquery 内集約・数値キー・HAVING）。

### 1.3 Semijoin reduction（gate `HELIOS_ENABLE_SEMIJOIN`、`:2949-3060`）
- join ref(`Index_lookup`)の equality を **Field\* 上 union-find**（`:2962`）。FER/FES（`for_each && is_scan`、`:2987`）probe step に、同 class で**より早い filter 付き source** から `SemijoinFilter` を attach（`:3005-3026`）。server `:1136-1172` が source の値集合 membership を作り、probe 非該当行を drop。
- **正当性バグ**: ❌ **inner-equi-join か anti/相関 subquery かの判別が無い**（`:3005-3026` に whitelist 不在）。NOT IN/NOT EXISTS/相関に semijoin が誤適用 → **q22 over-count（numcust ~3x）/ q21 rows=0**（観測）。docs `:77-78` に既知。
- **射程制限**: ❌ driver S: step / ref step には適用されない（`for_each && is_scan` 限定）→ q7 の customer 絞りに届かない。server S: 経路（`:1394`）も semijoin set 未配線。

### 1.4 gate 一覧
`HELIOS_AGG_PUSHDOWN`(presence) / `HELIOS_COST_V2`(=1必須) / `HELIOS_ENABLE_SEMIJOIN`(presence) / `HELIOS_OPT_STATS` / `HELIOS_RO_NOVALIDATE` / `HELIOS_RPC_COMPRESS` 他。

---

## 2. SOTA（disaggregated pushdown）

| 技術 | 要点 | Helios への含意 |
|---|---|---|
| **Predicate Transfer (CIDR'24)** | semijoin reduction の多join一般化（Yannakakis系）。join graph 上で **bloom/value-set を前向き・後向き2パスで伝播**し、join 前に**全テーブルを相互 pre-filter**。selectivity でランク、cost-aware に転送選択。2-10x。disaggregated では storage 側に bloom を push してから scan。 | **q7/join-heavy の本命**。既存 semijoin を「driver/ref step にも適用し、equality class 経由で value-set を伝播する」形に一般化すれば Predicate Transfer になる。 |
| **PushdownDB (VLDB'20)** | filter/projection/aggregation を S3 Select に push、**join は Bloom join に再実装**して S3 へ bloom を渡す。 | bloom を SemijoinFilter の表現に採用すれば大集合でもメモリ効率良。 |
| **FlexPushdownDB (VLDB'21)** | **separable operators**: 1 演算子を storage 側部分と compute 側部分に分割。filter/projection/aggregation/bloom を push、cache とのhybrid。 | agg pushdown の Phase A/B 切替＝まさに separable。CTE/subquery 集約も「materialize 時に server 部分集約 + proxy 仕上げ」の separable 化で実現可能。 |
| **PolarDB Serverless (SIGMOD'21) / FAST'20** | 「データの在る場所で処理」。filter/agg を storage へ push、各層で網羅 2-3x 削減。 | 方針一致。転送削減が王道。 |

**結論**: SOTA の答えは一貫して「(a) predicate/semijoin transfer で join 前に大テーブルを絞る」「(b) filter/projection/aggregation を storage 側で実行」。Helios は両方の**部分実装**を既に持つが、**join-filter transfer が driver に届かず、agg が単一 top-level table に限定**されている。

---

## 3. 実装ロードマップ（gap → SOTA → 触る箇所 → 正当性）

### P0（前提・正当性）: semijoin の anti-join/相関 whitelist
- **gap**: q22 over-count / q21 0-rows。**何かを実装する前にこれを直さないと semijoin 系は使えない**。
- **実装**: `ha_lineairdb.cc:2982-3057` の probe/source 選定に **inner-equi-join 限定 whitelist**。判別材料 = leaf の所属 query block が top-level inner join か（semi/anti/correlated subquery 由来を除外）。`TABLE_LIST`/`Query_block` の semijoin・subquery フラグ。
- **検証**: gate ON で 22/22 md5 一致を回復（特に q21/q22）。

### P1（最大ROI）: semijoin → Predicate Transfer 化（q7 等の join 絞り push）
- **gap**: driver/ref step に semijoin が適用されず、q7 の customer/orders が full ship。
- **実装**:
  - probe step 限定（`for_each && is_scan` `:2987`）を緩和し **driver S: step / ref step も probe 対象**に。
  - server S: 経路（`lineairdb_rpc.cc:1394`）に **semijoin membership 適用**を配線（現状 filter のみ）。
  - source value-set の表現を value-list に加え **bloom filter**（PushdownDB流）も可能に（大集合対策）。
  - （将来）equality class を辿る多段伝播＝完全な Predicate Transfer。まず1ホップ（nation→customer）から。
- **効果見込み**: q7 customer 15000→~1200、cascade で orders/lineitem 激減、resp 71MB→大幅減。
- **正当性**: inner-equi-join のみ（P0 の whitelist 前提）。MySQL の WHERE 再評価が安全網。

### P2: Aggregate pushdown 拡張（q15/q18）
- **P2a 数値 GROUP BY キー**: whitelist `:529-535` を緩和し、order-preserving typed key encoding（int/decimal/date）を Phase A group-key 生成（`:913-937`）と server group key（`:326-331` は raw bytes で既に可）に整合。→ q15(l_suppkey)/q18(l_orderkey)。
- **P2b HAVING**: whitelist `:519` 解除 + emit ループ（`:844-872`/`:1014-1060`）に HAVING 評価挿入（server は不変、proxy 仕上げ）。→ q18。
- **P2c subquery/CTE/derived 集約**: override_executor_func は top-level しか honor しない（`:506`）ので **別経路**。prefetch plan の **materialized-temp source step** 生成（`ha_lineairdb.cc:2548-2632`）に `AggregateSpec` を直接載せ、server 部分集約 + proxy 仕上げ（FlexPushdownDB の separable）。→ q15 `revenue0` view, q18 IN-subquery。最も重いが q15/q18 の本丸。

### 優先順位
**P0（正当性）→ P1（predicate transfer、最大ROI・SOTA本命）→ P2a/P2b（小・確実）→ P2c（重・q15/q18本丸）**。

---

## 4. Codex への確認事項（GO 判定依頼）
1. **優先順位**: P0→P1→P2 で妥当か。特に P1(predicate transfer 化) を P2c より先にやる判断は正しいか。
2. **P1 の正当性**: driver S: step に inner-equi-join semijoin membership を適用する設計で、OCC（range validation / filtered_keys 経路）の健全性は保てるか。source step が probe step より先に実行完了している保証（plan step 順）は read-plan 上どう担保するか。
3. **P0 whitelist の判別材料**: MySQL 8 の `Query_block`/`TABLE_LIST` で「inner-equi-join 由来の leaf か（semi/anti/correlated でない）」を正しく判別する最良の API は何か。
4. **P2c**: materialized-temp source step への server 部分集約注入は、CTE 二重参照（q15 は revenue0 を join と max subquery で2回参照）でも安全か（temp 実体化が1回なら集約も1回で済むか）。
5. 見落とし・落とし穴（特に正当性を壊す経路）。

---

## 5. Codex レビュー結果（**条件付きGO**, 2026-05-31, gpt-5.5/xhigh + 実コード検証）

### 優先順位
**P0→P1→P2a/b→P2c は妥当。P1 を P2c より先は正しい**（ROI + P2c は executor/materialization 介入で重い）。

### 設計書への訂正（実コード差分）
- **§1.3 一部古い**: server 側 semijoin 評価は FE/FER/FES に**既に入っている**（`server/rpc/lineairdb_rpc.cc:1121,1194`）。未対応は (a) proxy が FER/FES にしか attach しない（`ha_lineairdb.cc:2987`）、(b) **通常 S: 経路に semijoin 評価が無い**点。
- **§3 P2c は書き方のまま NO-GO**: 現コードに「materialized-temp source step」は**存在しない**。temp leaf は skip（`:2587`）、ref source が temp なら bound step を作らず target full S: に逃がす（`:2632`）。`AggregateSpec` の server 出力は synthetic group row（`rpc:313,378`）で、通常 read-plan ingest は base row として cache するため**そのまま materialization 内 step に載せると壊れる**。安全に読めるのは override executor の `agg_next_raw()`（`:3707`）のみ → **P2c は再設計が必要**。
- **q15 の前提が誤り**: q15 は `WITH` CTE でなく **BenchBase の `CREATE VIEW revenue0`**。MySQL は view の複数参照を independent objects 扱い（`table.h:4363`）→「temp 実体化1回」前提は本 workload に合わない。

### 確認事項への回答
1. （上記）優先順位 OK。
2. **P1 OCC**: 条件付き健全。S: step に semijoin membership を入れるなら、**filter と同様に reject した probe 行の key/tid を `route_filtered_row()` に流す**実装が必須（`rpc:1394,1409` / proxy `lineairdb_transaction.cc:534`）。**`HELIOS_RO_NOVALIDATE=1` は OCC を捨てる測定モードなので P1 の正当性根拠には使えない**。source<probe 完了保証は read-plan の step 順（`collect_qep_leaves` 外→内 `:2417`、attach も `src_step<probe_step` `:3008`、server は `previous_results` のみ参照 `rpc:1136`）で担保。**ただし driver S: が step0 で source が後続なら保証なし → attach しない or 二相化**。
3. **P0 whitelist API**: query-block 全体フラグでなく **leaf ごとの `Table_ref`** を見る。`Field->table->pos_in_table_list` で `Table_ref` 取得 → source/target が同一 top-level `Query_block`（`table.h:3633`）→ `Table_ref::embedding` を辿り `is_sj_or_aj_nest()` を除外（`table.h:2982,3767`）→ outer join inner table 除外（`table.h:3408`）。加えて **REF_OR_NULL・nullable join key・型/照合不一致の equi-join を除外**（server semijoin は raw bytes 比較 `rpc:1157`）。
4. **P2c**: 上記 NO-GO（再設計要）。
5. **落とし穴**:
   - **projection pushdown と semijoin source が干渉**（proxy は source_filter 時 projection clear するが planner が再設定し得る `:3043,3237`、server は full ordinal で列抽出 `rpc:1150`）→ **P1 前に修正必須**。
   - S: に semijoin 追加時は `scan_limit` 再適用順・filtered row routing・rangehash 対象範囲を揃える。
   - local range cache key が filter/semijoin signature を含まない既知 caveat（`lineairdb_transaction.cc:393`）。
   - **P2a は whitelist 緩和だけでは不足**。Phase B group passthrough が string cache 前提（`:850`）→ numeric key の sort/emit/cache store も直す。

### 最終判定: **条件付きGO**
1. P0 whitelist を **leaf-level `Table_ref`** ベースで（semi/anti/correlated/outer/ref_or_null/nullable/型不一致を除外）。
2. P1 の S: semijoin は reject 行を必ず `route_filtered_row()` へ。
3. source は `source_step < probe_step` の時だけ許可。後続 source が要る driver S: は対象外。
4. semijoin source/probe 列を projection planner に認識させる or 対象 step の projection を確実に禁止。
5. P2c は再設計（現 read-plan ingest と非互換）。
