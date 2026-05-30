# Phase-10 GroupedSummary 設計（subquery/CTE 集約の server 側実行）

disaggregated Helios の対InnoDB残差(SF=1 4.0x)の中で、subquery/CTE 内の単一表 GROUP BY 集約が
生 base 行を proxy へ ship してから集約しているのが q18(8.8s)/q15(3.9s) の主因。これを **server 側で
GROUP BY 集約して grouped 行だけ返す**(Phase-8 full aggregation pushdown の一般化)のが GroupedSummary。

## 1. 対象クエリの実構造(EXPLAIN FORMAT=TREE, SF=1 実測)

### q18 (8.8s)
```sql
... where o_orderkey in
  (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 300) ...
```
- 内側 = **単一表 lineitem の GROUP BY l_orderkey + HAVING sum(l_quantity)>300**(非相関)。MySQLは materialize。
- 現状: lineitem 全行(6M)を proxy へ ship してから集約。server 側で per-orderkey sum を計算し HAVING 通過 orderkey(少数)だけ返せば転送激減。
- 数値 GROUP BY キー(l_orderkey) + HAVING。

### q15 (3.9s)
```sql
with revenue0 as (select l_suppkey, sum(l_extendedprice*(1-l_discount)) total
                  from lineitem where l_shipdate in [Q1-1996] group by l_suppkey)
select ... from supplier, revenue0 where s_suppkey=supplier_no and total = (select max(total) from revenue0) ...
```
- 内側(BenchBase では `CREATE VIEW revenue0`) = **単一表 lineitem の filter + GROUP BY l_suppkey + SUM**。**view を2回参照**(join と max subquery)。
- 数値 GROUP BY キー(l_suppkey)。MySQL は view 参照を独立 materialize 扱い(参照2回 = 2回 materialize の可能性)。

### 参考(既に速い/別primitive)
- q2(MIN)/q17(AVG)/q20(SUM) は相関スカラ集約だが小 index probe で既に速い(0.1〜1.15s)→ ROI 低。
- q21/q22 は anti-join(別primitive: ExactAntiMap)。

## 2. 既存 Phase-8 agg pushdown との関係・差分
Phase-8(`helios_offloadable_shape`/`helios_override_executor`, ha_lineairdb.cc)は **top-level query block・単一表・
文字列 GROUP BY キー・HAVING 無し**の grouped 集約を `JOIN::override_executor_func` で server 化(Phase B exact-decimal)。
q18/q15 は: (i) **非 top-level**(derived/CTE/IN-subquery), (ii) **数値 GROUP BY キー**, (iii) **HAVING(q18)** で全て弾かれる。
GroupedSummary = この3制約を解く拡張。

## 3. 最大の未解決点 = 「ingest 機構」(Codex P2c NO-GO 指摘)
Codex 過去レビュー: 現 read-plan に「materialized-temp source step」は無く、temp leaf は skip(ha_lineairdb.cc:2587)、
ref-from-temp は full S: に逃げる(:2632)。`AggregateSpec` の server 出力は synthetic group 行で、通常 read-plan ingest は
base 行として cache するため、そのまま subquery step に載せると壊れる。安全に読めるのは override executor の
`agg_next_raw()` のみ。

→ **核心の問い: `Query_expression::ExecuteIteratorQuery` は derived table / materialized subquery / CTE の
query block に対しても `override_executor_func` を honor するか?**
- **YES なら**: `helios_offloadable_shape` の top-level 制約(:511)を「derived/CTE/materialized-subquery の単一表
  grouped 集約も可」に緩和し、override executor をそのブロックにも install。grouped 行は override executor 経由で
  正しく emit され、MySQL はそれを temp table に materialize する → 既存機構の素直な拡張。q18/q15 を直撃。
- **NO なら**: 別機構が要る。候補:
  - (a) **IN-subquery 特化(q18)**: server 側 GROUP BY+HAVING で qualifying l_orderkey 集合を作り、semijoin/
    ValueSet source として outer に渡す(GroupedSummary を semijoin source 化)。
  - (b) **derived materialization intercept**: derived table の materialize 経路(TableScan+Aggregate→temp)で、
    base scan step に AggregateSpec を載せ、proxy 側で grouped 行を temp に流す専用 ingest を作る。

## 4. 副次課題(YES 経路でも必要)
- **数値 GROUP BY キー**: whitelist(:529-535) を緩和。Phase A group-key 生成(strnxfrm 前提, :850 付近)と server
  group key(raw bytes, rpc:327)を、int/decimal/date の order/emit/cache に整合させる(Codex 既出: P2a は whitelist 緩和だけでは不足)。
- **HAVING(q18)**: whitelist(:519) 解除 + group 行確定後に proxy/override executor の emit ループで HAVING 評価。
- **view 2回参照(q15)**: 2回 materialize されても各々 server 集約すれば正しい(冪等)。1回共有できれば SharedScan と併せ更に得だが必須でない。
- **OCC**: Phase B は `tx_ro_novalidate()` 必須(synthetic group 行は per-row TID 検証不能)。同制約を継承。

## 5. Codex への確認(GO 判定依頼)
1. **機構**: `override_executor_func` は derived/CTE/materialized-subquery の Query_expression でも呼ばれるか(MySQL 8 source で確認)。YES なら §3-YES 経路、NO なら (a)/(b) のどちらが筋か。
2. q18(IN-subquery GROUP BY+HAVING) と q15(CTE/view 単一表 GROUP BY) の両方をカバーする最小機構は何か。
3. 数値 GROUP BY キーの order-preserving encoding 実装の要点(int/decimal/date)。
4. 正当性の落とし穴(OCC、view 2回参照、HAVING、NULL group key、IN の NULL semantics)。
5. 優先実装順と GO / 条件付きGO / NO-GO。

---

## 6. 検証結果・判定（2026-05-31, Codex + 経験的プローブ）

### §3-YES 経路は **NO-GO（実証済）**
プローブ: top-level 制約を gate で外し override executor 先頭に発火ログを仕込み、
`SELECT * FROM (SELECT l_returnflag, COUNT(*) FROM lineitem GROUP BY l_returnflag) t` を実行。
- `[AGGPD] installing override_executor_func` は出た（derived ブロックの JOIN に install された）
- **`[GSPROBE] override FIRED` は出なかった** = override は呼ばれない。
理由（Codex source 確認）: MySQL 8 iterator executor は derived/CTE を `GetAccessPathForDerivedTable()`→
`NewMaterializeAccessPath()`→`MaterializeIterator` で materialize し、子の iterator を直接 Init/Read で回すため
`Query_expression::ExecuteIteratorQuery()`(override を見る唯一の場所, sql_union.cc:1711)を**通らない**。
(`Table_ref::materialize_derived`→`unit->execute` 経路は存在するが iterator executor の本クエリでは使われない。)

### 実機構 = **materialization intercept**（Codex 条件付きGO）
`MaterializeIterator::MaterializeQueryBlock()` 付近で「単一表 GROUP BY の derived/CTE/materialized-IN」を検出し、
server 集約結果を temp table に直接流し込む専用経路。現 read-plan は temp leaf を skip(:2626)、ref-from-temp は
full scan に逃げる(:2671)ので、その経路新設が要る。

### 条件付きGO の条件・スコープ（初期）
- 対象を **non-null INT group key**(l_orderkey/l_suppkey) + DECIMAL SUM/COUNT + DISTINCT/ROLLUP/window/LIMIT 無し に限定。
- **数値 key**: emit 順序用に typed order-preserving encoding（INT=sign-bit flip + big-endian, ha_lineairdb.cc:5900 と同形）。server group key は raw bytes(rpc:321)なので proxy 側 sort/emit/cache を typed 化。
- **q18 HAVING**: `SUM(l_quantity)>300` は visible output でない hidden aggregate → 計算して HAVING 通過後 `l_orderkey` だけ temp へ。
- **q18 IN の NULL semantics**: MySQL が temp 側で `mat_table_has_nulls` を管理(item_subselect.cc:3387,3611)。独自 ValueSet 化するとここを壊すので、temp materialization 経路を使う方が安全。
- **OCC**: server 集約は synthetic group row(TID=0)で per-row 検証不能 → `HELIOS_RO_NOVALIDATE=1` 必須（Phase B と同制約）。view 2回参照は no-validate 前提なら snapshot 差リスクは計測モードでは許容。

### 優先実装順（Codex）
1. materialization intercept 経路 + INT non-null group key + DECIMAL SUM/COUNT（DISTINCT/ROLLUP/window/LIMIT 無し）
2. q18 HAVING hidden aggregate
3. q15 filter + SUM
4. nullable key / DECIMAL key / date key / 共有 materialization 最適化

### 最終判定
- 「override_executor_func 一般化」: **NO-GO**（実証）。
- 「materialization intercept にスコープ変更 + INT non-null 限定」: **条件付きGO**。新機構ぶん P0/P1 より重い。
- q18 だけなら IN→semijoin/ValueSet source 化も可だが NULL semantics 再実装が要り、q15(CTE/view)はカバーできない。
