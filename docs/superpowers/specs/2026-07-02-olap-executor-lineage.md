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

## 追記 (2026-07-02 Phase C 中盤)

| # | 設計判断 | 出典 (系譜) | 対象クエリ |
|---|---|---|---|
| 17 | sj/aj nest → SEMI/ANTI join ノード (キーは sj_outer/inner_exprs、WHERE 残差は per-match residual) | Neumann & Kemper unnesting (#12) + MySQL optimizer の nest 正規化に相乗り | q4 q21 |
| 18 | **サブクエリの残りは全て「derived サブブロックの再帰実行 + virtual table join」に一本化** — secondary prepare 時に MySQL が NOT EXISTS/相関スカラを derived+outer join へ自動変換する (HeatWave 向け transform) ことを発見。自前 decorrelation を書かず、この変換済みプランを受ける | HeatWave の secondary-engine transform + Neumann-Kemper (変換の意味論は同じ decorrelation) | q2 q11 q15 q16 q17 q18 q20 q21 q22 |

実装形: IR に QbSubBlock (再帰 Request)。サーバは実表 table_idx 空間の後ろに
virtual 表 (サブブロック結果行) を追加し、cell アクセスを value_of() で統一。

## 追記 2: SOTA 調査 (olap-engine-survey 追加調査) と実装の照合 (2026-07-02)

22/22 達成後に受領した詳細調査との照合。実装判断が系譜の結論と一致することの
検証記録 (論文の design-validation 節の一次ソース)。

| 実装済みの判断 | 調査の裏付け (出典) | 一致 |
|---|---|---|
| 相関スカラ集約 = 相関キー GROUP BY derived + join (MySQL の secondary 変換を受ける) | Neumann&Kemper BTW2015 の Γ 拡張則そのもの。MySQL WL#13520 が同変換を実装 (調査が同 WL を発見) | ✓ |
| EXISTS/NOT EXISTS = 素の hash semi/anti (3 値マーク不要) | 「EXISTS は NULL 比較を含まず常に 2 値」(BTW2015 simple unnesting; DuckDB/Velox/Spark の実装差の比較から) | ✓ |
| q21 残余述語: 同キー候補列挙→残余評価→全候補失敗のみ ANTI keep | DuckDB ScanStructure::ResolvePredicates / issue #4950 (これを欠くと TPC-H Q21 類似で性能崩壊) | ✓ (LEFT+IS NULL 形で同値) |
| 非相関スカラ = 1 回実行して定数注入 (1 行 derived の keyless join + MIN 登録) | PostgreSQL InitPlan / DuckDB は依存結合の自由変数空ケースとして自動帰着。GROUP BY なし集約は常に 1 行なので cardinality check 不要 | ✓ |
| OR-of-ANDs = join 後の残差ブール式木評価 (UNION 書き換えしない) | MySQL Index Merge Union は単一表アクセスパス限定; 残差式木評価が標準 (Dreseler et al. VLDB2020 のチョークポイント分析) | ✓ |
| COUNT(DISTINCT) = per-group set + union merge | ClickHouse uniqExact 型。調査推奨は Spark RewriteDistinctAggregates の 2 段 GROUP BY (スケール時の代替として記録) | ✓ (SF≤10 で等価) |

**要注意事項 (調査指摘)**: q16 の NOT IN は理論上 has_null 1 ビットが必要
(NULL-aware anti join)。本実装は MySQL の secondary-engine 変換が生成する
derived+ANTI 形を受けており、TPC-H スキーマ (ps_suppkey 非 NULL) では正しいが、
nullable 列の NOT IN を許すなら has_null ガードを allowlist 条件に追加すべき
(現状は列 nullable なら reject 側に倒れる)。

追加出典: DuckDB flatten_dependent_join.cpp / join_hashtable.cpp、
MySQL WL#13520、Neumann BTW2025 (Umbra も 2015 アルゴリズム使用と明言)、
Birler&Neumann CIDR2026 (mark join 線形時間)、Spark SPARK-32290/32494。
