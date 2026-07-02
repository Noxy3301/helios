# LINEAIRDB_COLUMNAR Phase B 設計 — vectorized query-block executor

Date: 2026-07-02
Status: 設計 (実装は次セッション)。Phase A は動作済み (commit cbf28c4, q6 一致)。
根拠: SOTA 実行エンジン実装調査 (`.note/session/2026-07-02_sota-olap-executor-survey.md`、
HeatWave/TiFlash/ClickHouse/DuckDB/Velox/DataFusion/X100/HyPer-Umbra) +
secondary engine API 解剖 (se-api-anatomy レポート)。

## 0. 目標

Phase A の単表集計に加えて **JOIN を含むクエリブロック全体** をサーバへオフロードし、
PAX strips 上のベクトル化実行で q3/q5/q10/q12/q13/q18 級の「MySQL 側 join/agg + proxy staging」
律速を解消する。狙い: TPC-H SF=1 合計で InnoDB/LDB に**桁を意識した**差をつける
(現状 PAX+q18 36.1s → 目標 ~15-20s 圏)。

## 1. SOTA 調査からの設計決定 (確定)

| 論点 | 決定 | 根拠 |
|---|---|---|
| 実行モデル | **Push 型パイプライン (Sink/Source/Operator) + morsel 駆動** | DuckDB/Leis et al.; TiFlash の「クエリ毎スレッド生成は破綻」教訓 |
| JIT | **組まない** (vectorized-interpreted) | VLDB2018: JOIN/agg 重い TPC-H では compiled に劣らない (Q3/Q9 で逆転)。byte-identical 検証面の最小化 |
| ベクトル | 1024-2048 行 (キャッシュ軸) / **morsel = PAX グループ 8192 行** (スケジューリング軸) | X100→VLDB2018 で 13 年再確認された普遍値。二軸は別物 (Leis) |
| 式評価 | **ベクトル先頭で一度だけデコード** (val_str ASCII → int64 / int128 固定小数点 / date int32)、以降は型付きベクトル演算 | 全 SOTA が型付きベクトル。ストレージ変更なしで最大レバレッジ |
| filter 伝播 | **selection vector (index 配列)**、dense/indexed dual-path | X100。ClickHouse の物理コピー(+後付け lazy materialization)は反面教師 |
| NULL | 別ビットマップ、全行無条件計算 + bitwise OR | per-tuple 分岐より 3-8 倍速 (X100) |
| hash join/agg | スレッドローカル build → パーティション境界 merge。salted pointer array + 線形プロービング (DuckDB)。**整数キー dense-array 高速パス** (nation=25/region=5/orderkey 密) は実行時 min/max 判定 | DuckDB PR#14971 / ClickHouse parallel_hash |
| 正当性戦略 | **HeatWave 流 allowlist + fallback**: 安全に保証できる型/collation/関数の組み合わせのみ受理、範囲外は primary へ再 prepare (Phase A で実証済みの機構) | byte-identical を無限のエッジケース追跡なしで満たす唯一の現実解 |
| スケジューラ | 固定常駐プール (≤64) + 共有タスクキュー + work-stealing。現行の固定 8-way 並列は 64 コアを遊ばせている | Morsel 論文 / TiFlash |
| NOT build | JIT / spill (SF≤10 は in-memory) / 分散 MPP / Arrow / window 関数 / micro-adaptivity | 投資対効果ゼロ領域の明示 |

## 2. アーキテクチャ

### 2.1 IR (proxy → server): `TxExecuteQueryBlock`

TiFlash の教訓 (1 スキーマに flat/tree の 2 形状) を採り、protobuf の演算子木:

```
QueryBlockIR {
  repeated TableDesc tables;            // name, field widths (既知)
  Operator root;                        // tagged union tree
}
Operator = Scan { table, filter: FilterExpr, projection: cols, zone_hint }
         | HashJoin { build: Operator, probe: Operator, build_keys, probe_keys, join_type: INNER|SEMI|ANTI|LEFT }
         | HashAgg { input, group_cols(型付き), aggs: AggFunc[] (SUM/COUNT/AVG/MIN/MAX), having: FilterExpr }
         | Sort { input, keys+dirs (int/binary-safe のみ) }
         | Limit { input, limit, offset }
         | Project { input, exprs: FilterExpr[] }
```

- 構築: `OptimizeSecondaryEngine` で optimizer の JOIN order (`join->best_positions` /
  AccessPath 木) を歩き、対応形状なら IR へ変換。allowlist 違反はその場で reject → primary。
- 出力行は最終 Operator の列を proxy 行フォーマットで返す (Phase A と同じ
  Item_columnar_value 経由の send_data。ORDER BY をサーバでやれば行順も決定的)。

### 2.2 サーバ側 executor (server/exec/ 新設)

- `ColumnBatch`: 型付きベクトル (int64/int128+scale/int32date/string_view) + null bitmap +
  selection vector。**ソースは PAX strip → バッチ先頭で一括デコード**
  (strip セルは val_str ASCII のまま — ストレージ変更なし)。
- Pipeline 分解: 各 HashJoin build / HashAgg が pipeline breaker。
  morsel (=PAX group) 単位のタスクを共有キューに投入、既存の
  visible-bitmap/write-counter 静止チェック (M2 実装) をソースで流用。
- 正しさ: 読みは ro_novalidate クエリ限定 (Phase A と同じ環境ゲート)。
  グループ静止チェック失敗時はクエリ全体を reject → primary 再実行。

### 2.3 段階計画

| 段 | 内容 | ゲート |
|---|---|---|
| B0 | executor 骨組み: Scan→Filter→HashAgg (型付きバッチ、morsel 並列)。Phase A の集計 RPC を置換 | q1/q6 一致 + q1 で現行 strip 集計以上 |
| B1 | HashJoin (INNER/SEMI) + 2 表形状 (q12/q14 形) | q12/q14 一致・短縮 |
| B2 | 多表 join tree + ORDER BY/LIMIT + AVG/MIN/MAX + HAVING | q3/q5/q10/q13/q18 |
| B3 | allowlist 拡張 (DECIMAL 精度・日付関数・LIKE)、22 本カバレッジ測定 | TPC-H 3-engine 最終戦 |

## 3. リスク

1. **decimal/collation の再現**: allowlist を極小 (INT/DECIMAL(≤18)/DATE/binary 比較) から
   始め、md5 ゲートで拡張の都度検証。合わないものは容赦なく primary へ。
2. **ORDER BY の安定性**: MySQL の filesort と同順を返す必要 (md5)。int/binary キーのみ許可。
3. **EXPLAIN / prepared statements**: Phase A と同じ制約。ベンチは text protocol のみ。
4. Item_columnar_value は text protocol 前提 (binary protocol = prepared stmt では未対応)。
