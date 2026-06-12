# Phase-15 Prefetch 極限最適化 作業ログ(議事録)

> **ゴール(user, 2026-06-12)**: 現状の Prefetch モードで TPC-C / TPC-H / TATP をそれぞれ極限まで最適化。
> Baseline 計測 → 実装精査 → 改善洗い出し → 実装。TPC-H は read-only で validation 不要、
> pushdown は正確性を損なわない限り自由。**全変更を本議事録に追記**(あとから追跡可能に)。
>
> **制約**: feat/prefetch-autogen から新ブランチ(→ `claude/prefetch-maxopt`)、push/hard-reset 禁止。
> サーバ側(LineairDB)のメモリ過剰使用による性能劣化は不可。メモリ不足ならデータサイズ縮小可。
> 「早ければ正しい」が、その実験でしか使えない恣意的チューニングは不可。

---

## ブランチ・トポロジ(着手時点の確定事項)

- 基点 = `origin/feat/prefetch-autogen`(7860ed4)。user の最新 WIP:
  - oneshot→prefetch リネーム、ha_lineairdb.cc のモジュール分割
    (lineairdb_autogen / lineairdb_prefetch / lineairdb_pushdown / lineairdb_keyenc / lineairdb_index_search)
  - **statement-scoped autogen prefetch**(b53a77d)= phase14 欠陥②への対処
  - **MRR read path を default DS-MRR 経由で prefetch 下に対応**(173f14f)= phase14 欠陥①への対処
  - prefetch cache miss を non-retryable abort に(216d5a0)
  - legacy 単表 UPDATE/DELETE の autogen plan(dba6c5c)
  - RPC: range read を proxy 側で組み立て、physical form 廃止(e598be0)— server/rpc が大幅縮小
  - submodule: LineairDB → 4852e52(refactor/stateless)、benchbase → 34b8ac94(@_tx_plan リネーム)
- 旧調査ブランチ `claude/investigate/tpcc-prefetch`(719bcc7)とは **e8784ec で分岐**(46 vs 29 コミット)。
  - 旧ブランチ側にのみ存在: **TPC-H Phase 7-10 最適化群**(range-hash OCC default ON、alloc churn、
    NDV stats、cost model V2、semijoin reduction、agg pushdown(override_executor_func)、SharedScan、
    OCC-meta strip、morsel 並列 server scan/agg、並列 TPC-H load(benchbase 1d63786)等)
  - → TPC-H 改善の主弾はこのスタックの**新ブランチへの移植**(マージ or cherry-pick、リネーム/分割跨ぎ)。
- 旧ブランチの未コミット WIP(phase14 欠陥① fix、8行)は stash + `docs/phase14_defect1_mrr_stage_fix.patch`
  に保全(autogen 側 173f14f が同問題の別解のため移植不要見込み)。
- phase14 の TPC-C 調査結果は `docs/phase14_tpcc_prefetch_investigation.md` / `_worklog.md`(本 branch に持ち込み)。

## 作業ログ

### [2026-06-12] エントリ0: セットアップ
- `claude/prefetch-maxopt` を origin/feat/prefetch-autogen から作成(--no-track)。
- submodule 同期: LineairDB 4852e52 / benchbase 34b8ac94 / mysql-server 2d6d5e1(変更なし=mysqld 再ビルド不要)。
- LineairDB CMakeLists に build.sh 相当の fix(GNUInstallDirs / -Wno-error)再適用(submodule checkout で消えるため)。
- build/proxy を新ブランチソースで作り直し(モジュール分割で .cc 構成が変わったため rm -rf → cp)。
- server(build/server, make)+ proxy plugin(ninja ha_lineairdb_storage_engine.so)再ビルド開始。
- 並行リコン: (a) proxy prefetch 実装の深掘り (b) bench ハーネス + TATP 対応状況。

### [2026-06-12] エントリ1: リコン結果(実装の現状把握)

**ビルド**: server(make)+ plugin(ninja .so)成功。mysqld 本体は SHA 同一で再ビルド不要。
サービス起動済(lineairdb-server :9999 / mysqld :3307)。

**proxy prefetch 実装(本 branch)の要点**(詳細は agent recon、file:line は当該レポート参照):
- gate = `GLOBAL lineairdb_prefetch_execution`(bool, default OFF)。HELIOS_* env gate は無し(rpc trace 除く)。
- **statement-scoped autogen**(query_id 検知で毎 statement 再stage)= phase14 欠陥②解消済。
  MRR 経路も default DS-MRR 経由で対応(欠陥①解消済)。
- plan 生成カバレッジ: 単表 PK 点/範囲、二次索引 点/prefix/範囲、JOIN(NLJ/BKA/非パラメタ HASH、
  INT4 join 鍵のみ)、legacy 単表 UPDATE/DELETE(trigger/ORDER BY/LIMIT/subquery 無しに限る)。
- **NO-PLAN(=非リトライ abort, ER_NOT_SUPPORTED_YET)**: full TABLE_SCAN/INDEX_SCAN、
  reverse scan(index_last/kPrevKey/kPrefixLast)、REF_OR_NULL、FER/FES(部分鍵/二次 for_each probe)、
  パラメタ化 hash join、index merge、UNION。silent fallback は 216d5a0 で撤去(cache miss = 非リトライ abort)。
- validation: 全 prefetch RPC の read が単一 validation vector に蓄積され **commit RPC 1 本で一括検証**
  (phase14 で懸念した tx_occ_key_ スカラ多token問題は stateless 化で解消)。
- explicit plan 経路 = `SET @_tx_plan='R:table:k1:k2;...'`(tx-scoped、benchbase HELIOS_PREFETCH_PLAN=1 が発行)。

**bench ハーネス**:
- `bench/bin/benchrun.py {tpcc,tpcc-np,tpch,ycsb}` — TATP は choices に無い(benchbase 側に TATP 実装は有り、
  prefetch パッチ無し・config xml 無し → 追加要)。
- モード: 無印=stateful(OFF)/ `--prefetch`=explicit @_tx_plan / `--prefetch-stmt`=autogen。
- TPC-H: `--scalefactor`、`run_tpch_queries.py`(per-query 120s timeout)。md5 検証は組み込み無し(別途用意)。
- benchbase 並列 TPC-H load(1d637862)は本 branch の submodule に未取込(helios/parallel-tpch-load branch に有り)。

**TPC-H への含意(重大)**: full scan が NO-PLAN なので **prefetch ON では TPC-H はほぼ全滅(エラー)** のはず。
旧ブランチは full `S:` prefetch + FER/FES + 集約/semijoin pushdown 等で 22/22 を回していた。
→ TPC-H 戦線 = (1) read-only 文に限定した full-scan/scan-step prefetch の復活(user 指示で validation 不要)
+ (2) 旧ブランチ Phase7-10 スタックの選択的移植。

### [2026-06-12] エントリ2: TPC-C ベースライン(着手)
- 条件: 1 warehouse / 1 terminal / SERIALIZABLE / 30s、3 モード(OFF / explicit / autogen)。
- benchrun.py は毎回 DROP+CREATE+LOAD(fresh state、fair comparison)。

(以降追記)
