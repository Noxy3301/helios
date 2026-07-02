# LineairDB-PAX OLAP executor — 設計系譜議事録 (論文用)

Date: 2026-07-02 (以後、設計判断のたびに追記する)
目的: secondary engine executor の各設計判断について「どの SOTA
システム/論文の指針に相乗りしたか」を記録する。論文の related work /
design rationale の一次ソース。

## 主参照系譜の選定

**主軸: DuckDB — すなわち HyPer/Umbra 系譜 (TU München, Neumann-Kemper
school) の OSS 代表。**

選定理由:
1. 実行モデルの全指針 (push 型 morsel 駆動並列・vectorized-interpreted・
   サブクエリ unnesting・mark join・hash table 設計) が**単一系譜で一貫して
   揃う**唯一の OSS。TiFlash は実行系 (ClickHouse フォーク) と unnesting
   (TiDB オプティマイザ側) が別系統に分散し、系譜として引きにくい。
2. 本研究の新規性は「Silo OCC の 1-copy PAX ストア上でこれを成立させる」
   側にあり、OLAP 実行戦略そのものは確立された系譜に相乗りするのが
   主張として最も明快 (実行戦略の再発明を主張しない)。

## 採用済みの設計判断と出典

| # | 設計判断 | 出典 (系譜) | 実装箇所 |
|---|---|---|---|
| 1 | JIT を組まず vectorized-interpreted 実行 | Kersten et al. "Everything You Always Wanted to Know About Compiled and Vectorized Queries But Were Afraid to Ask" (VLDB 2018): JOIN/agg 重心の TPC-H では interpreted が compiled に劣らない (Q3/Q9 で逆転)。DuckDB も同判断 | executor 全体 |
| 2 | morsel 駆動並列 (スケジューリング粒度=PAX group 8192 行) | Leis et al. "Morsel-Driven Parallelism" (SIGMOD 2014)。Vector(キャッシュ粒度)と Morsel(スケジューリング粒度)は別軸 | RunScan/RunJoin/Accumulate の chunk 並列 |
| 3 | クエリブロック全体を演算子木 IR でオフロード、演算子単位の all-or-nothing 受理 | TiFlash tipb.DAGRequest / MySQL HeatWave の文単位 admission | TxExecuteQueryBlock proto |
| 4 | allowlist + fallback による byte-identical 保証 (安全に保証できる型/collation/関数のみ受理) | MySQL HeatWave/RAPID の closed-world 原則 | recognize_* (proxy) |
| 5 | late materialization (タプル=表ごとの row-ref、列は必要時 gather) | C-Store/MonetDB 系の遅延実体化 (Abadi et al. "Materialization Strategies in a Column-Oriented DBMS", ICDE 2007) | NodeResult (refs per table) |
| 6 | INNER hash join の build/probe を実サイズで選択 | 全 hash join 文献の基本 + DuckDB の実行時適応 (perfect hash 判定も実行時) の精神 | RunJoin swap |
| 7 | 中間結果の cardinality 上限で fan-out 爆発を fail-fast | Umbra/HyPer の robustness 志向 (楽観実行+安全弁) | RunJoin 64M cap |
| 8 | 集計はスレッドローカル HT → merge (partition-then-merge) | DuckDB / DataFusion の 2 段階並列集計 | AccumulateRange + MergeGroups |
| 9 | EXTRACT(YEAR) を正規形文字列の接頭バイトへ落とす | (独自の 1-copy 適応 — val_str 正規形セルの性質を利用。論文では PAX セル表現の帰結として記述) | QbColumnRef.prefix_len |
| 10 | 除算・AVG の exact decimal (int128 mantissa) | MySQL my_decimal 互換要件 (md5 ゲート駆動) | Dec/dec_divide |

## Phase C で採用予定 (実装時に確定・追記)

| # | 設計判断 | 出典 (系譜) | 対象クエリ |
|---|---|---|---|
| 11 | 相関サブクエリの unnesting: 相関キー GROUP BY 集計 + join への変換 | Neumann & Kemper "Unnesting Arbitrary Queries" (BTW 2015) — Umbra/DuckDB (FlattenDependentJoins) の標準 | q2 q17 q20 |
| 12 | EXISTS/NOT EXISTS/IN → semi/anti (mark) join | 同上 + DuckDB の mark join セマンティクス (Neumann 系譜) | q4 q16 q18 q21 q22 |
| 13 | residual 述語付き semi/anti join (同キー内追加条件) | hash join residual predicate の標準形 (DuckDB PhysicalHashJoin の non-equi 条件) | q21 |
| 14 | 非相関スカラサブクエリ = サブプラン先行実行 → 定数注入 | 全エンジン共通 (uncorrelated scalar = init-once) | q11 q15 q22 |
| 15 | 複数表 OR-of-ANDs は join 後の tuple filter として評価 | 実行系標準 (union 書き換えはオプティマイザ最適化であり必須でない) | q7 q19 |
| 16 | COUNT(DISTINCT) はグループ内 distinct set (小規模) | (SF≤10 規模判断; DuckDB も非近似はソート/セット) | q16 |

## 論文主張の構造 (メモ)

- 新規性: (a) Silo OCC row-store の**完全置換**としての 1-copy PAX
  (visibility bitmap + write-counter 静止検査で OCC と共存)、
  (b) MySQL secondary engine API を**コピーレス** (SECONDARY_LOAD =
  登録のみ) で成立させた点、(c) val_str 正規形セル上の exact-decimal
  interpreted 実行で byte-identical を保証したまま InnoDB の 2.6 倍。
- 実行戦略: DuckDB/HyPer-Umbra 系譜に準拠 (上表)。「確立された
  vectorized-interpreted + morsel + unnesting 系譜が、OLTP 最適化された
  1-copy PAX ストア上でもそのまま成立する」ことが検証内容。
