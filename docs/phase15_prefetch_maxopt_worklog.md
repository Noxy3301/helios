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
- ハーネス補修: sysstat 系(mpstat/sar/pidstat)不在で execute が落ちる → `_start_sampler` で
  欠損ツールを warning+skip に(commit f7c77b4)。TATP 用 config + choices 追加も同コミット。

### [2026-06-12] エントリ3: 【重大】二次索引 stale エントリ問題の発見と根因特定

**症状**: 初回ベースラインで stateful(OFF)が 0 req/s(benchbase が
`C_LAST=... not found!` で即死)。explicit 127 / autogen 122 req/s は「動いているように見えた」。

**調査過程**(再現→切り分け→バイセクト):
1. 手動再現: `SELECT ... WHERE c_w_id=1 AND c_d_id=5 AND c_last='X' ORDER BY c_first` が 0 行。
   同条件 COUNT(*)(PK-MRR 経路)= 5 行で正常 → 二次索引 range scan だけが異常。
2. rpc_trace: TX_GET_MATCHING_PRIMARY_KEYS_IN_RANGE が **26 PK** を返し(期待5)、batch read も
   26 行 fetch、しかし MySQL へは 0 行。FORCE INDEX(ref access)だと **26 行がそのまま見える**
   (別 last_name の行が WHERE 素通り = ref は SE の鍵一致契約を信頼するため)+ c_id=245 が ~10 重複。
   → range 経路の 0 行は「範囲外の先頭行で compare_key が即 EOF」、ref 経路は「junk 可視」。同一根因。
3. fresh load(ベンチ実行ゼロ)でも junk 再現。10行/3000行の合成テーブル(同型 DDL)では再現せず。
4. **バイセクト**(helios × LineairDB submodule の整合ペアで7点、各点 server+plugin 再ビルド+再ロード+プローブ):
   P1 c0ce5d1+2120d5a GOOD / P2' +f7f9a42 GOOD / P3 +435a7d5 GOOD / P4 +8833d1c GOOD /
   P5 +c4a5c86 GOOD / P6 HEAD+a9db890 GOOD / P7 HEAD+139b709 GOOD / 4852e52 は純リネーム diff
   → 「どのコミットでも壊れない」= バイセクトの前提(決定的再現)が崩れる。
5. 真相: バイセクト中は毎回 server を再起動していた(=1 server 寿命に 1 load)。元の失敗環境は
   **同一 server 寿命内に複数回 DROP+CREATE+LOAD**(初回計測の試行錯誤で 7 回)していた。
   同一寿命で 2 回ロード → 全名照合の強プローブ(index 経由件数 vs fullscan 件数の全 (d,last) 比較)で
   **via_idx ≈ 2×truth** を確認。**stale SI エントリの世代堆積**が根因と確定。

**根本原因**: `ha_lineairdb::delete_table` は **no-op**(server に何も伝えない)+
`create` は server 側 db_create_table の「already exists を無視して再利用」設計。
→ DROP+CREATE+LOAD で base row は同 PK 上書きで一見正常だが、**二次索引には旧世代の
(旧名→PK) エントリが残留・堆積**。scan が旧世代 PK を返し、現世代の行と名前が食い違う。
- 旧ブランチ(phase14)で顕在化しなかったのは計測手順の違い(server 再起動を挟む運用)による
  可能性が高い(LineairDB 側の table dict / MPMCConcurrentSet に erase API が無いのは共通)。

**対処(本セッション)**: 真の DROP 伝搬(LineairDB に安全な table 削除 + concurrent set erase +
epoch 回収が必要)は大工事のため本フェーズでは見送り、**ハーネスの実験プロトコルとして
「ロードを伴う setup の直前に lineairdb-server を再起動」**を benchrun.py に実装
(restart_lineairdb_server(); proxy は自動再接続することを確認済)。
- これは恣意的チューニングではなく in-memory store の正当なクリーン状態保証。
- server 側 DROP 未実装は**既知の defect として残置**(本 doc が記録)。恒久修正は別途。
- 初回ベースライン値(OFF 0 / explicit 127 / autogen 122)は**汚染データのため全て無効**。再計測する。
- ビルドは以後 `scripts/build_partial.sh` を使用(user 指示: server make + proxy ソース同期 + ninja 一括)。

### [2026-06-12] エントリ4: TPC-C クリーンベースライン + 「explicit が実は無効」発見

**クリーンベースライン**(1wh/1term/SERIALIZABLE/30s、ロード毎に server 再起動):

| モード | throughput | goodput | retry |
|---|---|---|---|
| stateful(OFF) | 142.0 | 145.0 | 24 |
| explicit(--prefetch) | 124.9 | 127.9 | **0** |
| autogen(--prefetch-stmt) | 123.5 | 125.6 | **0** |

- **prefetch 系は abort/retry ゼロ**= phase14 の abort storm(3334件)は本ブランチで構造的に解消済。
- 実行後の SI 強プローブ(全 (d,last) の index vs fullscan 件数照合)も完全一致 — write 経路健全。
- しかし prefetch が stateful より ~12% 遅い(旧ブランチ: explicit 202 vs stateful 136 で +49% だったのに)。

**rpc_trace プロファイル(--prefetch, 15s)**: NewOrder=**35 RPC/tx**(TX_EXECUTE_READ_PLAN×34+commit×1)、
Payment=7.1、Delivery=**71 RPC/tx**。explicit のはずが **per-statement autogen として動作**。

**根因**: `bench/benchbase-mysql/benchbase.jar` が 6/1 ビルドの古い版で、セッション変数が
旧名 `@_ldb_plan` のまま(submodule は `@_tx_plan` に改名済 a21a9821、proxy も `_tx_plan` を読む)。
→ proxy は plan を見つけられず全 statement autogen に silent degrade。
proxy 側の tx-plan 経路(maybe_prefetch_for_transaction @ get_transaction:1444)自体は配線済で healthy。

**対処**: build_benchbase.py で jar を submodule(34b8ac94)から再ビルド → explicit 再計測へ。
**教訓**: 成果物 jar と submodule の整合は計測前に必ず確認(jar 内 TPCCProcedure.class の strings で変数名を見る)。

### [2026-06-12] エントリ5: TPC-C 正式ベースライン(新 jar)

| モード | throughput | retry | unexpected errors |
|---|---|---|---|
| stateful(OFF) | 140.3 | 34 | 0 |
| **explicit(--prefetch)** | **189.0 (+35%)** | 0 | **258** |
| autogen(--prefetch-stmt) | 124.5 (−11%) | 0 | 0 |

- explicit の 2-RPC アーキテクチャが新 jar で復活(140→189)。
- **explicit の残エラー 258 件は全て同一形状**: OrderStatus の最新注文取得
  `SELECT O_ID,O_CARRIER_ID,O_ENTRY_D ... ORDER BY O_ID DESC LIMIT 1`(oorder 二次索引の
  reverse+limit scan)が prefetch cache miss → 非リトライ abort。
  = phase14 Class-B(極値 scan covering)残課題。staged 側は reverse=1/limit=1 で持っているが、
  runtime の lookup_secondary_scan_cache が (reverse=false, limit=0) 固定 + reverse 系 handler
  経路(index_last/kPrevKey/kPrefixLast)が prefetch 下で未対応。
- autogen は per-statement RPC(NewOrder 35 RPC/tx、Delivery 71 RPC/tx)なので構造的に
  stateful(per-row RPC)と同程度+plan 生成 CPU 分やや遅い。TPC-C での本命は explicit。
- **TPC-C 改善バックログ**: (1) Class-B reverse+limit covering(explicit エラー 0 化、上の 258 件)
  (2) autogen の per-statement plan 生成コスト削減(plan cache)— 優先度低
  (3) 二次 scan ステップの行 payload 同梱(SI scan→batch read の 2 RPC を 1 に)— 要調査

### [2026-06-12] エントリ6: TATP ベースライン

| モード | throughput | retry | errors |
|---|---|---|---|
| stateful(OFF) | 1334.9 | 0 | 0 |
| **autogen(--prefetch-stmt)** | **1590.7 (+19%)** | 0 | 0 |

- TATP(SF=1=100k subscribers, 1term/30s)は **autogen が無修正で全カバー+19% 勝ち**。
  短 tx(1-3 statement)では per-statement autogen の RPC 数が stateful の per-row より少ないため。
- TATP procedures には benchbase 側 explicit plan パッチ無し(=--prefetch は autogen と同義)。

### [2026-06-12] エントリ7: TPC-C Class-B 実装(explicit の DESC LIMIT 1 covering)

**変更**(commit 予定):
1. `LineairDBTransaction::get_matching_primary_keys_in_range` に `row_limit/reverse_scan`
   パラメータ追加(default 0/false で既存呼び出し不変)。prefetch 分岐で staged エントリ照合に
   (reverse,limit) を透過し、miss 時は (false,0)(autogen の全範囲 staged scan)へフォールバック
   (全集合は部分集合を被覆、呼び出し側が tail に position するため安全)。
2. `execute_prefix_last` materialize-mode 非 primary 分岐: primary 分岐と同じ
   `range_scan_limit_for_order` ガード(SQL が単表 SELECT + LIMIT 1 + DESC 一致 + 残余フィルタ無し)
   を通った時のみ (reverse=1,limit=1) で cache 照合。
   → explicit plan の `S:oorder:o_w_id:limit=1:reverse=1` staged エントリが OrderStatus の
   `ORDER BY o_id DESC LIMIT 1` にヒットするようになる。
3. stateful 経路は従来通り全範囲 fetch(hint 無視)で意味不変。
- validation 健全性: lookup が返す cached エントリは staged の reverse=1/limit=1 を保持
  → commit 時 RangeReadEntry(row_limit/reverse_scan 対応済)で server が同条件 replay 検証。

### [2026-06-12] エントリ8: Class-B 計測確定 + TPC-H M0 実装

**Class-B 後の explicit TPC-C**: **191.3 req/s, retry 0, errors 0**(stateful 140.3 比 +36%)。
C1-C4 整合 0 violation、SI 強プローブ clean。commit d946546。

**TPC-H 戦線 M0(full-scan prefetch)実装**(commit 予定):
1. `compile_leaf`(lineairdb_autogen.cc): full scan 拒否を条件付き許可に。
   gate = 純 SELECT(`sql_command==SQLCOM_SELECT` かつ `reginfo.lock_type<=TL_READ`、
   FOR UPDATE/SHARE と DML は従来通り拒否)かつ primary 順(TABLE_SCAN または primary INDEX_SCAN)。
   step = `{is_scan, key_prefix="", end_key_prefix=sentinel}`(全表 staged scan)。
2. `get_matching_keys_and_values_from_prefix`(serving): 空 prefix(=rnd full scan)を
   `in_range("", sentinel)` へルーティング(従来は next_lex("")="" で無条件 abort)。
- 既知の制限(次マイルストーン): MATERIALIZE(派生表/サブクエリ)QEP node は未対応のため
  q2/4/7/8/9/11/13/15/16/17/18/20/21/22 あたりは NO-PLAN のはず。filter pushdown も plan step に
  未配線(全表転送)。validation は全 range replay(read-only no-validate gate は M2 で導入予定)。
- 並行で bench/queries/q1-22.sql(spec qualification 固定パラメータ版、md5 検証用)を生成中。

### [2026-06-12] エントリ9: M2(read-only no-validation)実装 + md5 検証準備

**M2 実装**(commit 5cfcf3d): GLOBAL sysvar `lineairdb_prefetch_ro_novalidate`(default OFF)。
ON のとき、autocommit 単文 SELECT(prefetch)に限り:
- read-set/range-read-set の蓄積を skip(proxy CPU/メモリ削減 — full scan で 60万 key の蓄積が消える)
- commit 時 validation RPC を完全省略 → **SELECT が 1 RPC で完結**
- write を持つ tx / multi-statement tx は従来通り full validation(gate はそれらに触れない)
- 根拠: user 指示「TPC-H は read-only で validation 不要」。並行 writer 無し環境でのみ健全(help text 明記)。

**md5 検証ハーネス**: bench/queries/q1..q22.sql(spec qualification 固定パラメータ、benchbase 発行と
同一 SQL)+ bench/bin/tpch_md5.sh(target vs InnoDB 参照、ソート後 md5 比較)。
q6 は benchbase 由来の `'0.06'`(文字列)を忠実再現(両エンジン同条件なので比較は公正)。

**TPC-H stateful ベースライン(進行中)**: SF=0.1 で q5/q7/q8/q9/q10 が 120s タイムアウトの模様
(重 join。Phase7-10 スタック不在の本ブランチでは想定通り)。完走後に確定表を記載。

### [2026-06-12] エントリ10: TPC-H stateful ベースライン確定 + M0/M2/M3 動作確認

**stateful(OFF)SF=0.1 22-suite**(120s timeout、結果 bench/results/tpch_individual_20260612_102531):
OK 13/22。TIMEOUT: q5,7,8,9,10,17,18,20,21(重 join 系)。OK 組も q3 36.5s / q4 80.9s / q13 94.5s。
軽量組: q1 4.7s, q6 2.7s, q2 2.7s, q11 2.5s, q19 1.8s, q22 2.9s 等。

**M0+M2 単体確認(q6, SF=0.1)**: OFF 1.26s / ON+validation 2.87s / ON+novalidate 2.04s、結果一致。
→ ON が OFF より遅い理由: OFF は ECP(server 側フィルタ)済み行のみ転送、M0 は全表転送。
**M1(plan step への filter pushdown)が transfer 削減の本命**と確定。

**M3 実装**: collect_qep_leaves に TEMPTABLE_AGGREGATE / MATERIALIZE を追加(subquery_path +
table_path を再帰、temp table leaf は compile loop で skip)。q1 が prefetch ON で完走(結果妥当)。

**q3 の新ブロッカ**: NLJ inner が orders を二次索引 REF probe → **FES(for_each secondary probe)
が server 未対応**で NO-PLAN。対応案: (a) server for_each に secondary range probe 追加(wire 拡張要)
(b) cost model 較正で hash join に倒す(旧ブランチ HELIOS_COST_V2 の移植)。実測インパクト順で判断。

**運用教訓**: `build_partial.sh` は稼働中の lineairdb-server バイナリを上書きする → 直後にサーバが
silent crash(10:47 の障害の真因)。**ビルド後は必ず server 再起動+リロード**。

### [2026-06-12] エントリ11: TPC-H カバレッジ攻略 — FER/FES 実装

**M0+M2+M3 後のカバレッジ**(固定パラメータ 22 本, prefetch ON+novalidate): q1/q6 のみ OK。
残り 20 本は全て **FER(for_each primary range probe)/ FES(for_each secondary probe)未対応**
(=NLJ の inner を outer 行ごとに部分鍵/二次索引で引く形)による NO-PLAN。
COST_V2(旧ブランチ移植、env gate)を ON にした実験では plan が別の未対応形状
(secondary INDEX_SCAN / missing root_access_path / q6 まで REF 化)へ移るだけで改善せず
→ **カバレッジ欠落を直すのが先**、cost model は全形状対応後に再評価(ポートは gated でコミット済)。

**FER/FES 実装**(proto+server+proxy+autogen):
1. proto StepResult += `group_sizes/group_start_keys/group_end_keys`(flat 配列の per-probe スライス)。
2. server handleTxExecuteReadPlan: for_each && is_scan のとき、source 行ごとに
   `[row_key, next_lex(row_key))` の prefix range を StatelessRangeScan(FER)/
   StatelessSecondaryRangeScan(FES)で実行し group 化して返す。**probe key で dedup**
   (旧ブランチ「foreach 重複 14x」教訓: 同一 join 鍵の重複 probe は 1 回だけ実行)。
   point for_each も同様に dedup(record_row_cache は key ベースで冪等)。
3. proxy execute_read_plan: group ごとに LocalRangeScanEntry / LocalSecondaryScanEntry を staging
   (runtime の per-outer-row probe が exact-range で hit)。
4. **scan cache に exact-start ハッシュ索引**(`table\x01index\x01start`)を追加。
   FER/FES は probe 数万件の staging になるため線形 lookup だと O(N²) で破綻する。
   push_range/secondary_scan_cache ヘルパで索引維持、drop_secondary_scan_cache で再構築。
5. autogen compile_ref_lookup: FER/FES 拒否を撤廃し is_scan for_each step を発行
   (secondary は index_name 付与)。
- validation 整合: 各 group は bounded range なので従来の RangeReadEntry replay で検証可能
  (novalidate OFF でも健全)。

### [2026-06-12] エントリ12: TPC-H カバレッジ 2→19/22(SF=0.1)

**FER/FES 後の進捗**(matrix r3 → r4、計測は tpch_matrix.sh = per-query 完全ログ方式):
- r3(FER/FES+WEEDOUT 等追加前のビルド): 18/22 OK。stateful で TIMEOUT だった q5(4.2s)
  q7(1.7s) q8(1.9s) q10(1.7s) q17(0.96s) q18(3.8s) q20(2.5s) q21(4.6s) が全て数秒で完走。
- **q9 の根因**: lineitem probe の 2 つの bound keypart が異なる source step(partsupp.ps_partkey
  + supplier.s_suppkey)に跨り、staging のペアリングが破綻 → runtime miss。
  **修正 = multi-source remap**: bound source のうち最新 step を iterating source とし、他表
  field は Item_field::item_equal_all_join_nests(multiple equality)経由で iterating source の
  等価列へ付け替え(例: s_suppkey→ps_suppkey)。不能なら明示 reject。→ **r4: 19/22 OK**(q9 2.3s)。
- QEP node 追加: WEEDOUT / NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL / ZERO_ROWS(q4/q5 等の
  semijoin 戦略)。root は unit->root_access_path() を優先。
- 残り 3 本(q11/q15/q22)= **optimize 時に評価される非相関スカラサブクエリ**が外側 plan 確定前に
  handler を叩き「missing JOIN root_access_path」になる構造問題。
  **対処 = per-unit staging**: statement root が未構築なら table の所属 unit の root を解決して
  その subtree だけ staging(tx に staged-roots set を導入、query_id でリセット)。statement root
  が後で構築されたら従来通り一度だけ statement staging(resolved flag は維持)。
- 注意した点: 一時的に「直前クエリのエラー形状を引き継ぐ」ように見える断続事象を観測(再現せず)。
  計測ハーネスを per-query stderr 完全保存(bench/bin/tpch_matrix.sh)に変更して追跡可能化。

### [2026-06-12] エントリ13: **TPC-H 22/22 達成**(prefetch ON+novalidate, SF=0.1)

matrix r5(per-unit staging)で 21/22、r6(q15 temp-driven probe fallback)で **22/22 OK**。
commit 8759228。各クエリ数秒以内(最遅 q21/q5 ~4.6s、suite 合計 ~45s)。
stateful ベースライン(13/22 OK・9 本 >120s TIMEOUT)に対する質的飛躍:
| 例 | stateful | prefetch ON |
|---|---|---|
| q3 | 36.5s | 2.2s |
| q4 | 80.9s | 2.1s |
| q5/q7-q10/q17/q18/q20/q21 | >120s TIMEOUT | 1.5-4.6s |
| q13 | 94.5s | 0.4s |

次: (1) InnoDB 参照(port 3308)に同一 SF ロード→ 22 本 md5 全照合(正しさ確定)
(2) TPC-C 3 モード + TATP 2 モード回帰(共有経路改修のため必須)
(3) その後 perf 磨き込み(M1 filter pushdown / SF=1 へのスケール)

### [2026-06-12] エントリ14: 正しさ確定 + OLTP 回帰クリア + M1 実装

**md5 全照合**: InnoDB 参照インスタンス(port 3308, 同一 SF=0.1, benchbase ロード 12.7s)に対し
**22/22 md5 完全一致**(qualification 固定パラメータ、ソート後比較)。prefetch ON+novalidate の
結果は InnoDB と同一 = カバレッジ拡張は正確性を保っている。

**OLTP 回帰**(FER/FES+per-unit staging+cache 索引導入後):
| | before | after |
|---|---|---|
| TPC-C stateful | 140.3 | 143.8 |
| TPC-C explicit | 191.3 | **196.2**(err 0) |
| TPC-C autogen | 124.5 | 125.0 |
| TATP stateful | 1334.9 | 1330.0 |
| TATP autogen | 1590.7 | 1586.3 |
回帰なし(誤差範囲)。C1-C4 + SI 強プローブは explicit 実行後に再検証中。

**M1(filter pushdown to plan steps)実装**(ビルド前):
- proto PlanStep += `PushedPredicate filter`(semantics は stateless scan RPC と同一:
  不一致行 drop、parse 不能行は返して MySQL 再評価)。
- server: primary plan scan に PredicateEvaluator 適用。
- autogen: 非 for_each・primary scan step に cond_push 由来の table-local filter を付与。
  **gate = tx->ro_novalidate()**(validation replay は filtered key set を再現できないため、
  validation 有効時は従来通り未フィルタ転送)。autocommit 単文限定なので
  filtered エントリの statement 越え再利用も構造的に発生しない。

### [2026-06-12] エントリ15: M1 検証完了 + SF=1 開始

- **M1 後も 22/22 OK + md5 22/22 一致**(matrix r7)。q6 2.04→1.56s。commit 051e116。
- C1-C4 再検証(FER/FES 後の explicit 200.5 req/s 実行後)= 全 0、SI 強プローブ clean。
- benchbase submodule にローカルブランチ `helios/prefetch-maxopt`(34b8ac94 ベース)を作成し
  旧ブランチ検証済みの並列 TPC-H ローダ(1d637862)を cherry-pick(6f3f578e)→ jar 再ビルド。
- 運用注意: `scripts/stop_mysql.sh` は port 3308 の InnoDB 参照 mysqld も殺す(pkill being broad)。
  参照インスタンスはディスク永続なので再起動で復旧可。
- **SF=1 開始**: 並列ローダでロード → ON+novalidate で 22 本(timeout 300s)、RAM watchdog
  (avail<2GB で abort、/tmp/mem_sf1.log に RSS 記録)。

(以降追記)
