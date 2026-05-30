# Phase-8: aggregation pushdown via JOIN::override_executor_func (Phase A done)

計測条件: SF=1、`HELIOS_AGG_PUSHDOWN`(新規 env gate, default OFF)、`HELIOS_RO_NOVALIDATE=1`
`HELIOS_OPT_STATS=1`、oneshot ON、hypergraph off。MySQL は third_party/mysql-server **無改変**。
変更は `proxy/ha_lineairdb.cc` / `proxy/ha_lineairdb.hh` のみ。

## 目的と機構
disaggregated TPC-H の最大コストは「集約・フィルタ前の作業集合(数百万行)を server→proxy→MySQL
へ運ぶ」こと(Phase-7 attribution)。本命の対策が **aggregation pushdown**。だが helios は
**primary storage engine plugin** で MySQL の集約 executor の下層にいるため、server 側で集約して
少数行を返しても **MySQL が再集約して COUNT/AVG/式集約が壊れる**。

解法: MySQL 8.0.43 の `JOIN::override_executor_func`(secondary engine 用に宣言された
内部フック。HeatWave が使うが OSS に使用例ゼロ。NDB/InnoDB は未使用)を、primary plugin から
**実行時に 1 行代入**することで installed。`Query_expression::ExecuteIteratorQuery`
(sql_union.cc:1711)が secondary 判定なしにこれを呼び、iterator pipeline を bypass して
helios が最終行だけ `Query_result::send_data` で返す。

配線(全て `proxy/ha_lineairdb.cc`):
- `ha_lineairdb::hton_supporting_engine_pushdown()` override → gate ON 時 `lineairdb_hton` を返す(NDB と同じ公開 API)
- `lineairdb_hton->push_to_engine = lineairdb_push_to_engine`(公開 handlerton メンバ)
- `lineairdb_push_to_engine()`: offloadable shape なら `join->override_executor_func = &helios_override_executor`
- `helios_override_executor()`: base scan を ha_rnd_init/next/end で自前駆動(helios prefetch を発火、
  OCC read footprint も通常通り記録)→ WHERE 自前評価(FILTER iterator bypass のため)→ GROUP BY
  キーで bucket → COUNT/SUM/AVG を自前 accumulator(DECIMAL は my_decimal)→ Item_cache で send_data。

## アーキ上の位置づけ(prior art 調査)
全分散DB(TiDB/TiKV coprocessor、CockroachDB DistSQL、Redshift Spectrum、PolarDB-X)は
**two-phase aggregation(storage 側 partial → gateway 側 final merge)+ plan-shipping coprocessor**。
ただし全て SQL executor を**所有**するか MySQL を**fork**(Aurora Parallel Query は専用 "Aggregator"
をソース追加し predicate/projection を push、full agg は非対応)。helios は無改変 primary plugin の
ため `override_executor_func` で「Aurora が fork で得た final-aggregation 段を fork せずに得る」形。
= MySQL を fork せず full aggregation pushdown する、が新規性。

## OCC 整合性
override は iterator 実行のみ bypass し、文末 handlerton commit は通常実行される。Phase A は base 行を
全て proxy で scan するので read footprint(range_versions 等)は通常通り記録 → serializable も崩れない。
read-only scope(SQLCOM_SELECT)限定で有効化。`HELIOS_RO_NOVALIDATE=1` 自体は無競合 TPC-H 計測用で
serializable ではない点は別途明記。Phase B(server 側集約)では footprint を別送する必要がある。

## whitelist(Codex レビュー P1 反映で厳格化)
override は metadata 送出後に発火し fallback 不可なので、**正しく実行できる形だけ**を受理:
- read-only SELECT、非 EXPLAIN、**top-level query block のみ**(サブクエリ/派生表は除外 — q17/q20 の
  相関集約サブクエリ誤 hijack を防止)、UNION/set 無し、単一 base table、grouped、非 DISTINCT、
  非 HAVING、非 LIMIT、非 ROLLUP、非 window。
- 出力列: bare `Item_sum`(COUNT(*)/SUM/AVG, 結果 INT/REAL/DECIMAL)、または bare `Item_field`
  passthrough(非 temporal/JSON/BIT/GEOMETRY、非集約)。`SUM(x)+1` 等の wrapper は reject。
- GROUP BY 列は文字列 base 列のみ(strnxfrm で collation 準拠キー)。数値/日付グループは defer。
- ORDER BY は無し or GROUP BY 列の昇順一致のみ(std::map のキー順で出力するため)。
- COUNT(*)/COUNT(非NULL const) のみ。COUNT(col)/COUNT(NULL) は reject。
- 暗黙集約(GROUP BY 無し)は出力が集約のみ(passthrough 禁止)。
- SUM の 0 行/全 NULL は NULL(SQL 準拠)。

## 正答性
- skeleton: gate ON で grouped クエリが空結果(bypass 実証)、gate OFF 完全無害。
- Phase A: **全 22-suite md5 = InnoDB と完全一致(gate ON)**。q1/q6 が override 経由、他 20 は素通り。
  gate OFF も 22/22(=既存挙動不変)。
- 修正したバグ: (1) `join->fields` は temp-table 再バインドで Item_sum を持たない → `query_block->fields`
  を使用。(2) `my_decimal_add(&a,&a,d)` の in-place エイリアスで多ワード和のキャリー破損 → temp 経由。
  (3) サブクエリ JOIN の誤 hijack → top-level guard。

## 計測(SF=1, Phase A = proxy 側集約)
```
query  helios-ON(override)  helios-OFF(baseline)  InnoDB    | mem ON(mysqld/server)
q1     13199ms              14765ms               5005ms    | 6.13 / 11.95 GB
q6      3278ms               2701ms                753ms    | 4.45 / 11.10 GB
(InnoDB mem: 3.37 / 8.18(idle baseline) GB)
```
- **Phase A 単体の効果は小**: q1 は ~1.5s 改善(MySQL の temp-table 集約 iterator を除去)、q6 は微増
  (暗黙集約は元々 1 accumulator で安く、自前 scan+my_decimal のオーバヘッドが勝る)。**メモリ不変**。
- 理由: Phase A は依然 5.9M 行を proxy で scan(rnd_next ~6.6s 相当が残る)。中間行の ship/materialize を
  消せていない。

## 結論と次段(Phase B)
Phase A は「**MySQL 無改変・primary plugin から executor override で full aggregation を正しく実行できる**」
ことを実証した(正答性・非回帰・Codex P1 反映済み)。**perf 本丸は Phase B = server 側 partial aggregation**:
server が scan 中に group/集約まで畳んで partial group 行だけ ship → rpc_exec・rnd_next・set_fields・
メモリ(+3.8GB working set)を一掃。Phase A の proxy 集約コードはそのまま「final merge 段」に再利用。
Phase B には (a) server 側の算術式評価+集約(FilterExpr IR を arithmetic に拡張)、(b) OCC footprint の
別送(range-hash/TID)が必要。

## Phase B(server 側 partial aggregation)= 完了

集約を **server 側**に移し、partial group 行だけ ship。helios は server 1台・scan 1回なので group は
**final**(分散の cross-node merge 不要)。AVG だけ proxy で sum/count(my_decimal_div = Phase A と同じ)。

実装:
- **proto**: PlanStep に `AggregateSpec aggregate=13`(group_columns + aggs[{kind COUNT/SUM/AVG,
  arg=FilterExpr IR, result_scale}])。FilterExpr に算術 op `OP_ADD/SUB/MUL/DIV/NEG` 追加。**応答は
  既存 scan_values を再利用**(server が group 行を通常の行フォーマットで詰める)→ flat codec/応答 proto 無改変。
- **proxy**(override Phase B ブロック): offloadable 時に AggregateSpec 構築、SUM/AVG の arg を
  `helios_serialize_arith`(COLUMN_REF/CONST_INT/+,-,*)で IR 化。`tx_set_pushed_aggregate`(choose_table
  も呼び db_table_key 確定)→ execute_read_plan が primary S: step に stamp、projection 無効化。
  handler `agg_next_raw` で group 行の生バイトを取得 → parse → strnxfrm キーで merge → Item_cache で emit。
  server 非対応(REAL/非対応式)は Phase A に fallback。
- **server**(`server_aggregate_scan`): filter 後の行を group_columns で hash-group。SUM/AVG は
  **exact decimal**: `Dec{__int128 mantissa, int scale}` で arg 式を評価(`+,-,*` は丸めなし、mul scale=s1+s2)。
  ASCII decimal で格納 → proxy が str2my_decimal で復元。group 行を [null_flags][group cols][per-agg
  value,count] で scan_values に詰める。range_versions は従来通り(OCC 不変、read-only scope)。

**md5 一致の根拠**: decimal `+,-,*` は exact(丸めなし)。server の per-row arg 値の scale 規則
(mul=s1+s2 等)が MySQL の my_decimal と一致(q1 で s1=scale2, s3=scale4, s4=scale6 を検算)。
SUM は per-row 値の単純和なので scale 保存。proxy で ASCII→my_decimal は exact。AVG は proxy で
my_decimal_div(Phase A と同一)。

**結果**: q1(SUM×4[式集約]+AVG×3+COUNT)を server 側集約で **InnoDB と md5 完全一致**。全 22-suite 22/22
(q1/q6 が Phase B 経路、他は素通り)、gate OFF 無害。

**計測(SF=1)**:
```
query  Phase B(server)  Phase A(proxy)  InnoDB   | mem mysqld(B) / mysqld(A)
q1     5256ms           13199ms         5604ms   | 1.71GB / 6.13GB
q6     3672ms            3278ms          845ms   | 1.71GB / 4.45GB
```
- **q1: Phase A 13.2s → Phase B 5.26s(2.5x)、InnoDB(5.6s)を上回る。mysqld mem 6.1GB→1.7GB**
  (proxy が 5.9M 行を materialize しなくなった = 4 行 ship)。
- q6 は scan 律速(元々 1 行 ship)なので Phase B の恩恵は小。
- server mem は 11.95GB→7.69GB に減るが、まだ高い: `StatelessRangeScan` が全 6M 行を `result.rows` に
  materialize してから集約するため(Codex 指摘)。**完全な server memory 削減には scan-callback 集約**
  (集約しながら行を捨てる)の第2版が必要 = 今後。

## Codex レビュー P1 反映(Phase B を gated scope で健全化)
判定は「条件付き GO(q1/q6・SF1・NOT NULL group・read-only)/ 一般機能は NO-GO」。以下を反映:
- **空入力の暗黙集約**: 0 行でも 1 行(COUNT=0/SUM=NULL)。`server_aggregate_scan` が n_grp==0 &&
  groups 空なら 1 group を seed。検証: `COUNT(*) WHERE 1=0` → 1 行 c=0(InnoDB 一致)。
- **OCC を read-only scope に明示ゲート**: server は group 行のみ返し per-row TID footprint が無いので、
  集約列の concurrent UPDATE を range_versions では検出不可。Phase B(server-agg)は
  **`HELIOS_RO_NOVALIDATE=1` の時だけ有効**、それ以外は Phase A(全行 scan で footprint 記録+検証)に
  fallback。検証: RO_NOVALIDATE 無しで q1 が Phase A に落ち md5 一致。
- **nullable GROUP BY 列を Phase B から除外**(server の extract_value_column が NULL/空を区別しないため)。
  Phase A は NULL marker で正しく処理。
- **group row の field 数 strict check**(`1+ng+2*n_aggs` 以外は query error)。

## 残課題
- **int128 overflow**: SF1 q1(mantissa scale6 ~5.6e16 ≪ 1.7e38)は余裕だが一般 DECIMAL(65) は未防御。
  schema precision から上限証明して fallback、または checked arithmetic / multiprecision が必要(今後)。
- **server memory**: scan-callback 内集約(全行 materialize を廃し集約しながら捨てる)で 7.7GB をさらに削減。
- 数値/日付 GROUP BY 列(order-preserving typed key encoding)。現状は文字列列のみ。
- REAL 集約・複雑式(DIV 等)は Phase A fallback のまま。
- P2: val_*() 後の thd->is_error() チェック、ha_rnd_end の RAII 化。
- AGGSRV/AGGEXEC デバッグ出力は `HELIOS_FE_DEBUG` gated のまま残置。
