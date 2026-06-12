# Phase-10 GroupedSummary 着手前 議事録 — aggregation pushdown の SOTA 裏取り

**日付**: 2026-06-01
**目的**: GroupedSummary（subquery/CTE 内の単一表 GROUP BY 集約を server 側で実行し grouped 行だけ返す、q18/q15 直撃）に
着手する前に、「集約を storage/source 側に降ろす」のが **業界で実在し、許容される（generally accepted）pushdown か**を
一次情報で裏取りする。user の懸念=「どこまで computation を下に降ろしていいかが怖い」への回答。

**調査方法**: multi-agent fan-out（10 エージェント = 5 システム × {一次情報リサーチ, adversarial 検証}）。
リサーチ側は公式 doc / ソースコード / spec を優先取得、検証側は独立に一次 URL を再 fetch し「集約（filter/projection
でなく）が本当に降りているか」「誇張がないか」を懐疑的に確認。

## 結論（先に）

**5/5 システムで「集約（aggregation）の pushdown」が一次情報で確証された**（全件 `claim_holds=true` /
`aggregation_genuinely_pushed=true` / `overstated=false` / confidence=high、検証エージェント追認）。
**集約 pushdown は SOTA で確立された一般的技術**であり、Helios の GroupedSummary は新規・異端な発明ではなく
既存系譜の disaggregated 版である、と裏が取れた。さらに各システムが課す**安全条件が共通パターン**を成しており、
それが Helios の条件付きGOスコープ（[[helios-phase10-pushdown-roadmap]] / 設計書 §6）とほぼ一致する。

## システム別結果

### 1. Oracle Exadata Smart Scan / cell offload — ✔ 集約降りる（IMA/vector transform 経路）
- ベースの Smart Scan は **predicate filter + column projection** のみ storage cell に offload。
- **集約 offload は In-Memory Aggregation（vector / KEY VECTOR transform）経路に限定**: star-join + SUM/MIN/MAX/COUNT
  を key-vector 形に rewrite し In-Memory columnar で cell scan する場合のみ「processing of aggregation work」を offload。
- 安全条件: direct-path read 限定 / **完全な read-consistency（undo・MVCC）を保持** / uncommitted data に当たると
  offload は静かに DB tier へ fallback（best-effort）。
- 一次情報: docs.oracle.com … `exadata-offload-enhancements-ima-whats-new-12.2.1.1.0.html`（"vector transformed
  queries that scan data in in-memory columnar format on the storage server can offload processing of aggregation work"）

### 2. AWS Redshift Spectrum / S3 Select — ✔ 集約降りる（partial aggregation）
- Redshift Spectrum: EXPLAIN に **`S3 HashAggregate`** ノードが出て GROUP BY + COUNT/SUM/AVG/MIN/MAX を S3/Spectrum 層で
  **partial 集約** → Redshift compute 層（`XN HashAggregate`）で finalize。**DISTINCT / ORDER BY は降ろせない**。
- S3 Select: 単一 S3 オブジェクトに対し AVG/COUNT/MAX/MIN/SUM（+GROUP BY）。immutable な S3 file scan で一貫性確保。
- 一次情報: docs.aws.amazon.com … `c-spectrum-external-performance.html`

### 3. Apache Spark DataSourceV2 — ✔ 集約降りる（公式 SPI）
- `SupportsPushDownAggregates`（Spark 3.2.0+, `org.apache.spark.sql.connector.read`）= **公式の集約 pushdown mix-in**。
- `pushAggregation(Aggregation)` が aggregate 関数 + GROUP BY 式を source に渡す。`supportCompletePushDown` が false なら
  Spark が上位で再集約（= source は **mergeable な partial 集約**を返せばよい / group key 重複出力が許される）。
- 安全条件: partial pushdown は **非 DISTINCT の MIN/MAX/SUM/COUNT/COUNT(\*) のみ**（mergeable=distributive）。
  **AVG は SUM/COUNT に分解**して降ろし上位で再構成。filter を先に push、scan 出力列順は grouping→aggregate 固定。
- 一次情報: github.com/apache/spark `.../read/SupportsPushDownAggregates.java`（v3.5.1）

### 4. Trino / Presto — ✔ 集約降りる（Connector SPI）
- `ConnectorMetadata.applyAggregation(...)`（release 335, PR #3697）が aggregate 関数 + groupingSets を connector に提示、
  connector が source 集約に書き換えるか `Optional.empty()` で engine に戻す。
- ops: count/sum/min/max/avg + 統計系（stddev/variance/covar/corr/regr）。
- 安全条件（**correctness-first**）: ROLLUP/CUBE/GROUPING SETS 除外 / aggregate 内の式（`sum(a*b)`）除外 /
  暗黙 cast 除外 / 「正しさを損なう恐れがあれば速くても connector が pushdown を拒否」。
- 一次情報: trino.io/docs/current/optimizer/pushdown.html + `ConnectorMetadata.java`

### 5. PostgreSQL postgres_fdw — ✔ 集約降りる（remote full-aggregate）
- PG 10+（`foreign_grouping_ok`, commit 7012b132d）が GROUP BY / aggregate / pushable HAVING を Remote SQL に deparse し
  **local Aggregate ノードを消して grouped 行だけ転送**。
- 安全条件: shippable（built-in or allow-listed extension, **IMMUTABLE**）/ **`AGGSPLIT_SIMPLE`（非 split 全集約）のみ** /
  collation safety / GROUPING SETS 除外 / grouped relation 単位で all-or-nothing / **remote tx の MVCC snapshot 内で実行
  ＝ local と remote の結果が一致保証**。
- 一次情報: postgres_fdw.c / deparse.c + postgresql.org/docs/current/postgres-fdw.html

## 横断的に見えた「安全に降ろせる条件」（業界共通エンベロープ）

1. **一貫性スナップショット内で実行** — Oracle は undo/MVCC 保持、postgres_fdw は remote tx の MVCC snapshot、Spectrum は
   immutable S3。集約結果が base 一貫性と矛盾しない保証が必須。
2. **mergeable / distributive な aggregate に限定** — SUM/COUNT/MIN/MAX は partial 可。**AVG は SUM/COUNT に分解**（Spark）。
3. **グローバル性を要する操作は除外** — DISTINCT / ORDER BY / ROLLUP/CUBE/GROUPING SETS は降ろさない（Spectrum/Trino/fdw）。
4. **式・cast・collation の落とし穴を避ける** — aggregate 内の式や暗黙 cast、collation 不一致は pushdown 対象外（Trino/fdw）。
5. **best-effort / correctness-first fallback** — 安全でなければ静かに engine 側集約へ戻す（Oracle/Trino/Spark の全てが採用）。

## Helios GroupedSummary への含意

設計書 §6 の **条件付きGO スコープ**（non-null INT group key + DECIMAL SUM/COUNT、DISTINCT/ROLLUP/window/LIMIT 無し、
`HELIOS_RO_NOVALIDATE=1`）は、上記 5 条件と**整合**している:
- (1) 一貫性 → Helios は `tx_ro_novalidate()` の read-only scope に限定（Phase-8 と同制約）。synthetic group 行は per-row TID
  検証不能なので no-validate 必須、という Helios の制約は postgres_fdw の「remote snapshot 内」要請と同型の保守化。
- (2) mergeable → DECIMAL SUM/COUNT に限定（exact-decimal）。MIN/MAX も distributive で将来安全に拡張可。AVG は未対応だが
  Spark 同様 SUM/COUNT 分解で後日拡張できる道がある。
- (3) グローバル操作除外 → DISTINCT/ROLLUP/window/LIMIT を最初から除外スコープに。業界と一致。
- (4) 式・cast → INT non-null group key に限定し、order-preserving typed encoding を厳密化（Codex 既出の P2a 課題）。
- (5) fallback → 既存 prefetch の hardening（[[helios-prefetch-general-hardening]]）と同様、非対応 shape は full-S/通常経路へ。

**結論: GroupedSummary は SOTA で確立された aggregation pushdown の disaggregated 実装であり、設計済みの安全スコープも
業界の安全エンベロープと一致する。着手して問題ない。**

## 次アクション
1. （済）proxy helper `execute_read_plan_raw_values`（base-row cache に ingest しない grouped 行取得）— 未コミット作業ツリー。
2. shape recognizer + executor 本体 = `ha_lineairdb.cc` の **materialization intercept**（`MaterializeQueryBlock` 付近で
   単一表 GROUP BY の derived/CTE/materialized-IN を検出 → server 集約結果を temp に直接流す）。設計書 §6 の優先実装順に従う:
   (1) intercept 経路 + INT non-null key + DECIMAL SUM/COUNT → (2) q18 HAVING hidden aggregate → (3) q15 filter+SUM。
3. 各段階で 22-suite md5 一致を維持（[[helios-agg-pushdown-override]] の Phase-8 と同じ検証規律）。

## 付録: 調査メタ
- Workflow run: `pushdown-sota-verification`（10 agents, ~221k subagent tokens, 93 tool uses, ~232s）。
- 全 finding が adversarial verify を通過（overstated=false × 5）。一次情報は official-doc / source-code / spec 中心。
