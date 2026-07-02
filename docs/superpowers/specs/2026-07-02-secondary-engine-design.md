# LineairDB Columnar Secondary Engine 設計 (Phase A)

Date: 2026-07-02
Branch: `feasibility/pax-1copy` (継続)
前提: 1-copy PAX (同ブランチ、win-condition 検証済み) + MySQL 8.0.43 secondary engine API

## 0. 構想 (ユーザー指示)

- OLTP は primary (ha_lineairdb, tuple-at-a-time) のまま。
- OLAP (read-only autocommit SELECT) は secondary engine `LINEAIRDB_COLUMNAR` 経由で
  クエリブロックごとサーバにオフロードし、PAX strips 上で実行する。
- 将来的に primary 側に堆積した pushdown 機構 (autogen prefetch / pushdown_rules /
  aggregate_pushdown / GS) を secondary 側に移して primary を単純化する。
- HeatWave との差別化: HeatWave は InnoDB→RAPID の 2 コピー + 明示ロード。
  こちらは**同一 LineairDB の 1-copy PAX を primary/secondary が共有** — SECONDARY_LOAD は
  実質 no-op、鮮度ラグゼロ。

## 1. API 要点 (se-api-anatomy 調査より; MySQL 8.0.43 実コード確認済み)

- 乗っ取り: `optimize_secondary_engine` フック内で `join->override_executor_func`
  (`bool(*)(JOIN*, Query_result*)`, sql_optimizer.h:326) をセット。実行時
  `Query_expression::ExecuteIteratorQuery` (sql_union.cc:1710) がメタデータ送信後に
  これを呼び、エンジンは `query_result->send_data(thd, items)` で行を直接返す
  (send_eof は呼び出し側)。`join->send_records` に行数を記録。
- オフロード資格はサーバが管理: 明示トランザクション内・LOCK TABLES・ストアド
  プロシージャは `THD::is_secondary_storage_engine_eligible` が自動拒否 →
  **read-only autocommit SELECT だけが候補** (ユーザー方針と一致)。
  `use_secondary_engine` sysvar (OFF/ON/FORCED, 既定 ON) +
  `secondary_engine_cost_threshold` (既定 1e5) で点読み系は自然に primary 残留。
- 失敗時のフォールバックは標準装備: フック内で reject (true 返却/my_error) →
  `ER_PREPARE_FOR_SECONDARY_ENGINE` 状態機械が primary で再実行 (sql_parse.cc:1518)。
- SECONDARY_LOAD: `ALTER TABLE t SECONDARY_ENGINE='LINEAIRDB_COLUMNAR'` →
  `ALTER TABLE t SECONDARY_LOAD` → handler::load_table(const TABLE&)。
  1-copy なので**行コピーせずテーブル登録のみ** (サーバに PAX スキーマがあることを確認)。
- 必須 handlerton/handler 実装は se-api-anatomy レポート §A のチェックリストに従う
  (HTON_IS_SECONDARY_ENGINE、stats は ha_get_primary_handler() へ委譲、HA_NO_INDEX_ACCESS)。

## 2. Phase A スコープ: 単表集計 (q1/q6 形) のオフロード

**認識**: OptimizeSecondaryEngine で JOIN を検査 —
単一 base table / WHERE が FilterExpr 化可能 / SELECT+GROUP BY が AggregateSpec 化可能
(既存 lineairdb_aggregate_pushdown.cc のビルダー資産を流用)。非対応形状は reject →
primary で再実行 (自動)。

**実行** (`override_executor_func`):
1. 既存 `TxExecuteReadPlan` の aggregate ステップ (full scan + FilterExpr + AggregateSpec)
   を 1 RPC 発行 — サーバ側は既存の `parallel_primary_pax_aggregate_scan`
   (strip 直接 fold) がそのまま動く。
2. 返ったグループ行 ([null][group cols][value,count]*) をデコードし、
   **Item_cache 列** (join->fields の各 Item から `Item_cache::get_cache`) に値を格納して
   `query_result->send_data(thd, cache_items)`。
   (send_result_set_metadata は元の fields で送信済み — send_data は値の
   シリアライズのみなので型互換の別 Item で良い。)
3. AVG は proxy 側で sum/count から合成 (既存 agg デコーダと同じ規約)。

**接続**: secondary handler は同一 LineairDB サーバへの既存 RPC クライアント
(LineairDBProxy) を利用。read-only なので tx なし (stateless plan RPC のみ)。

**配置**: proxy/ha_lineairdb_columnar.{cc,hh} を既存プラグイン .so に同居
(`mysql_declare_plugin` に 2 つ目のエントリ)。INSTALL PLUGIN を start_mysql.sh に追加。

**ゲート**:
- q1/q6 が secondary で md5 一致 (LDB ベースラインと)
- 非対応クエリ (q7 等) が primary へ自動フォールバックして md5 一致
- TPC-C 無回帰 (secondary 未 LOAD またはコスト閾値以下 → primary)

## 3. Phase B 以降 (SOTA 調査レポート待ち分を含む)

- QueryBlock IR (protobuf): scan/filter/project/hash-join/hash-agg/sort/limit/semijoin
  のツリーを 1 RPC で送る `TxExecuteQueryBlock`。
- サーバ側 vectorized executor: PAX strips → 型付き vector (scan 時デコード)、
  morsel-driven 並列。詳細は olap-engine-survey の推奨 (TiFlash/ClickHouse/DuckDB/
  HeatWave/Velox 実装調査) を反映して別ドキュメント化。
- 型付きセル (ストレージ側) + SIMD は Phase B 後半。
- primary 側 pushdown 機構の撤去は Phase C。

## 4. リスク

1. **旧 optimizer との相性**: `USE_EXTERNAL_EXECUTOR` フラグ/iterator 生成スキップは
   hypergraph 前提の可能性。旧 optimizer でも sql_union.cc:1710 の分岐は通る
   (iterator は無駄に作られるが実行は乗っ取れる) — 実装時に検証。
2. **Item_cache 差し替えの型/collation 整合**: send_data 側の Protocol は Item の
   data_type に従う。DECIMAL/DATE 系の cache 型を fields と一致させること。
3. **EXPLAIN**: secondary オフロード時の EXPLAIN 表示は乱れる可能性 (HeatWave も
   専用表示)。ベンチは結果 md5 のみ見るので影響なし。
4. **プラグイン 2 個同居**: INSTALL PLUGIN 2 回 (同一 so)。アンインストール順序に注意。
