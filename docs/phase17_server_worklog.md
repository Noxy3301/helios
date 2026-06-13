# Phase 17: サーバー(XG6326-2U)公式再ベースライン + SF=1 TPC-H 完全制覇

ゴール: (A) 本マシンで helios / InnoDB ref 両方の公式ベースラインを取り直す。
(B) 22/22 md5 を維持したまま suite 合計で InnoDB を下回る(目標: 勝ち 15/22、
中央値 <1.0x、suite 合計 InnoDB の 1/2 以下)。

ルール: WSL 時代の絶対値とは比較しない。MySQL 本体改変禁止。push/hard reset 禁止。
ビルドは scripts/build_partial.sh(services 停止後)。pid file で kill(pkill -x mysqld 禁止)。
server はインメモリ(load 前に必ず server 再起動 / mysqld 再起動で prefetch sysvar OFF)。

---

## [2026-06-13] エントリ1: 環境確認 + Track A 着手

**マシン**: XG6326-2U, 64 core, 125GB RAM, Linux 6.17.0-20-generic(native, 非WSL)。
**作業点**: branch `claude/prefetch-maxopt` HEAD 9c01359(clean)。
submodule: LineairDB 72b11754(helios/prefetch-maxopt)/ benchbase 6f3f578e(並列ローダ入り、
jar 2026-06-12 ビルド)/ mysql-server 8.0.43(無改変)。

**ビルド**: /tmp/helios_build.log = 4816/4816 完走、`error:`/`FAILED` ゼロ。再ビルド不要。

**初期状態**: mysqld / lineairdb-server とも停止、ポート 3306/3307/3308/9999 リスナー無し。
/tmp に旧 pid/sock 残渣(6/12 以前)→ 削除済み。build/data は空(datadir 未初期化)、
build/data_3308 無し → 両方ゼロから構築。

**計測 env(標準、scripts/dev/README.md 踏襲)**:
- mysqld: `HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1 HELIOS_ENABLE_SEMIJOIN=1`
- server: `HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1`
- 接続後: `SET GLOBAL lineairdb_prefetch_execution=ON; SET GLOBAL lineairdb_prefetch_ro_novalidate=ON;`
- jemalloc LD_PRELOAD + MALLOC_CONF(start scripts 内蔵)

**InnoDB ref(3308)公式構成(本フェーズで確立)**: 旧環境の起動コマンドは docs に
未記録だったため、ここで明文化する。plain mysqld(plugin 無し、default engine = InnoDB)、
datadir `build/data_3308`、`--innodb-buffer-pool-size=16G`(SF=1 データ ~数GB を完全
キャッシュし、ディスク I/O 差ではなく実行エンジン差を測る fair 設定)、
`--disable-log-bin`、その他は helios 側 start_mysql.sh と同等の汎用上限のみ。
起動スクリプト: `scripts/dev/start_innodb_ref.sh`(本フェーズで追加)。

---

## [2026-06-13] エントリ2: ビルド事故(field_types.h sync 漏れ)→ 修正 + 公式ベースライン確定

### ビルド事故と恒久修正
起動後 `lineairdb_prefetch_ro_novalidate` sysvar が `SHOW VARIABLES` に出ず、SET でも
`Unknown system variable`。プラグイン .so に文字列が含まれていなかった(`strings | grep` = 0)。
真因: `scripts/build_partial.sh` の proxy sync が `*.cc *.hh` のみコピーで、
**`proxy/*.h`(`lineairdb_field_types.h`)が storage/lineairdb に未反映**。
移行後の clone で third_party 側が古い field_types.h を持ち、プラグインがソース最新と乖離。
→ build_partial.sh の cp に `"$ROOT_DIR"/proxy/*.h` を追加(恒久修正)。再ビルドで sysvar 復活。
旧 /tmp/helios_build.log の 4816/4816 は full build だが、この sync 経路だけ穴があった。

### 公式ベースライン(本マシン・専有計測)
- **マシン**: Xeon Gold 6326 @2.90GHz, 2 socket × 16 core = 32 物理 / 64 論理, 2 NUMA。
- **load**: helios SF=1 = **86.0s**(16 loader 並列, 全行数正解), InnoDB ref = 360.2s(terminals=1)。
  helios 行数: lineitem 6001215 / orders 1.5M / customer 150k / partsupp 800k / part 200k 等 = 正解。
- **RSS(matrix 後)**: server 5351MB, helios mysqld 4384MB, InnoDB mysqld 4035MB。
- **md5: 22/22 OK**(InnoDB ref と完全一致)。

### per-query matrix(timeout 420s, gates 全 ON + prefetch ON+novalidate)

| Q | helios(s) | InnoDB(s) | 比 h/i | 判定 |
|---|---|---|---|---|
| q1 | 0.86 | 10.60 | **0.08** | ★WIN 12x(agg pushdown) |
| q2 | 0.33 | 0.14 | 2.36 | lose |
| q3 | 1.30 | 1.30 | 1.00 | tie |
| q4 | 1.99 | 0.72 | 2.76 | lose |
| q5 | 1.02 | 1.06 | **0.96** | ★win |
| q6 | 0.41 | 1.62 | **0.25** | ★WIN 4x |
| q7 | 1.52 | 1.14 | 1.33 | lose |
| q8 | 2.82 | 2.77 | 1.02 | ~tie |
| q9 | 3.77 | 2.38 | 1.58 | lose |
| q10 | 3.59 | 1.64 | 2.19 | lose |
| q11 | 0.29 | 0.24 | 1.21 | lose |
| q12 | 7.73 | 2.49 | 3.10 | lose |
| q13 | 5.80 | 3.83 | 1.51 | lose |
| q14 | 8.21 | 1.70 | 4.83 | lose |
| q15 | 1.18 | 3.61 | **0.33** | ★WIN 3x(GroupedSummary) |
| q16 | 0.74 | 0.33 | 2.24 | lose |
| q17 | 0.36 | 0.44 | **0.82** | ★win |
| q18 | 30.96 | 2.17 | 14.27 | lose（最大ギャップ） |
| q19 | 0.26 | 0.26 | 1.00 | tie |
| q20 | 0.47 | 0.46 | 1.02 | ~tie |
| q21 | 13.76 | 5.06 | 2.72 | lose |
| q22 | 3.16 | 0.19 | 16.6 | lose |
| **計** | **90.55** | **44.15** | **2.05** | helios 2.05x 遅い |

- **現状: 勝ち 5/22(q1,q5,q6,q15,q17)+ tie 4本。suite 合計で InnoDB に 2.05x 負け。**
- WSL 知見と整合: 残ギャップは q18>q21>q14>q12>q22>q13。

### Track B 作業キュー(ギャップ絶対値 = helios秒 − InnoDB秒、大きい順)
1. **q18: 28.79s 差**（30.96 vs 2.17）← 単独で全ギャップ 46.4s の 62%。最優先。
2. **q21: 8.70s 差**（13.76 vs 5.06）自己結合
3. **q14: 6.51s 差**（8.21 vs 1.70）for_each probe
4. **q12: 5.24s 差**（7.73 vs 2.49）for_each probe
5. **q22: 2.97s 差**（3.16 vs 0.19）
6. **q13: 1.97s 差**（5.80 vs 3.83）
7. q10: 1.95s 差 / q9: 1.39s 差 / q16: 0.41 / q4: 1.27 / q7: 0.38

注: q18 を 2.17s 並みに削れれば suite 90.5→61.8s。q18+q21+q14+q12 で計 49.2s 削減 →
理論上 41.3s となり InnoDB 44.15s を逆転可能。**q18 が天王山。**

数学的事実: 他の負けクエリの対 InnoDB ギャップ合計は約 21s に過ぎず、q18 を温存したまま
他を全部ゼロにしても helios 62s で InnoDB 44s に届かない。**q18(28.8s 差)の解決は必達条件。**

---

## [2026-06-13] エントリ3: q18 RPC trace 真因(lineitem 二重フルスキャン = 426MB 転送)

`ENABLE_RPC_TRACE=1` で mysqld 再起動(データ非消失)、q18 単発 30.29s 実行。trace 集計:

```
duration: 27.71s (committed tx)
time split: rpc 4.07s  sections 6.68s  residual(mysql+proxy cpu) 3.10s
TX_EXECUTE_READ_PLAN: 1 RPC, resp_b = 426,682,328 (426MB!), 8.14s
stage_local: 12.05s (13.6M 行をローカル read-cache へ staging)
stage_rpc_and_decode: 9.50s (内 RPC 8.14s + decode 1.37s)
staged_rows: 13,602,426  ( = 6.0M × 2 ≒ lineitem 全体を二重に取得 )
出力: わずか 57 行
```

**真因**: q18 は lineitem を2回フルスキャンしている。
1. 内側サブクエリ `lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`
   → EXPLAIN では `Index scan on lineitem PRIMARY (6M)` + group aggregate。prefetch は
   全 lineitem 6M 行を staging。本来の出力は「SUM>300 の orderkey」= **57 個**だけ。
2. 外側 join `orders ⋈ lineitem ON o_orderkey = l_orderkey`
   → IN フィルタ通過後の orders は 57 行のみだが、prefetch は join 鍵を動的に絞れず
   lineitem を再び全 6M 行 staging。

426MB を運んで 57 行を出すのは、転送・staging・decode の三重苦。InnoDB(2.17s)は
データがローカルなので RPC ゼロ + ストリーミング group-by で済む。

**勝ち筋の仮説**(エントリ4 で検証):
- (A) 内側集約を server 側で実行 → (orderkey, sum) を 57 行だけ返す = GroupedSummary/agg pushdown。
- (B) 外側 lineitem join を「57 orderkey の keyed fetch」に変える → ~400 行転送。
- 重要な等価性: 外側の `SUM(l_quantity) GROUP BY ..., o_orderkey` は o_orderkey=l_orderkey
  なので内側の per-orderkey SUM と**同一値**。理論上は外側 lineitem scan 自体が冗長
  (内側集約結果を流用可能)。ただし MySQL optimizer はこの等価を知らない。
- 過去の GroupedSummary 単独適用は「外側も lineitem scan するため転送不変 + RPC 上乗せで
  +21% 退行」で revert 済み(memory: helios-grouped-summary-q18-semijoin-decision)。
  → 今回は (A) と (B) を**セットで**解く必要がある。これが未踏領域。

---

## [2026-06-13] エントリ4: OLTP 回帰ベースライン + q18 コード機構の精査

### OLTP 回帰ベースライン(現バイナリ = HEAD 9c01359 を正しく rebuild した姿、gates OFF default 経路)
| | req/s | errors |
|---|---|---|
| TPCC autogen (terminals=1) | **109.6** | 0 |
| TATP autogen (terminals=1) | **1462.9** | 0 |

これが Track B 各変更後の回帰判定の基準。**Track A 完了。**

### q18 の壁(コード精査で確定): MySQL 無改変では内側 IN サブクエリを override できない
- `helios_should_pushdown_aggregation`(ha_lineairdb.cc:490-）は **q18 を明示的に想定して設計**
  されている: HAVING `SUM(l_quantity) > 300`(529-531行), INT group key `l_orderkey`(543行)を
  処理可能。**だが inner unit は SINGLEROW_SUBS(スカラーサブクエリ)のみ hijack 可**(515-522行)。
  q18 の `o_orderkey IN (...)` は **MaterializeIterator** 経由で消費され
  `override_executor_func` の in-tree フックが存在しない → override 不可能。
  (override が効くのは ExecuteIteratorQuery=スカラーサブクエリ経路のみ。MySQL 改変なしでは
  materialize 経路にフックを挿せない。)
- GroupedSummary(合成行・handler scan 乗っ取り)は IN サブクエリを明示 reject(1367行
  「DERIVED inner units only」)。仮に拡張しても、合成行は MySQL に HAVING 再適用させるため
  **全 1.5M グループ**(l_orderkey は 1.5M distinct)を返す必要 = 6M→1.5M の 4x 削減止まり。
  外側 6M は不変。→ q18 28s→推定16s 程度の部分改善にとどまり、suite 逆転には不足。

### q18 の非対称構造(これが設計の鍵)
- **内側** `lineitem GROUP BY l_orderkey HAVING SUM>300`: server 側集約必須(でないと 6M 読取)。
- **外側** `orders ⋈ lineitem`: IN 通過後の orders は **57 行のみ**。本来 prefetch せず
  57×~4=~228 点読でも 0.2s 程度。現状は for_each が IN フィルタ前の全 orders を source に
  全 lineitem(6M)を staging しているのが無駄。
- MySQL のプラン自体は「subquery materialize(57鍵)→ orders を IN で絞る → join」と正しい順序。
  問題は **prefetch が一括先行ステージングで IN フィルタを適用できない**こと。

### 方針転換の判断
- q18 をいきなり完全解(内側 server 集約 + 外側 keyed fetch の依存ステージング)は機構新設が大きく
  リスク高。**先に for_each オーバーヘッドの汎用機構を q12/q14 で理解・改善**する
  (for_each は q12/q14/q18外側で共通。汎用化が効く)。その知見を q18 外側へ展開する順で進める。

---

## [2026-06-13] エントリ5: ★大勝★ filtered_keys(negative coverage)の無駄送信を除去 → suite -20s

### 真因(step ごとのバイト計測で確定)
q14 の RPC 応答 15.2MB を step 別に計測(`HELIOS_STEP_BYTES` 一時計装):
```
step0 lineitem scan: rows=7630 val=269KB key=122KB  fkeys=592942 fkey_bytes=9.49MB ★
step1 part  probe  : rows=6338 val=190KB key=51KB   fkeys=0
```
実 row データは全 step 合計わずか 0.63MB。残り ~14.5MB(flat-codec framing 込み)は
**filter で reject された 59万行のキー(filtered_keys)**。これは「filter された表を後段が
point-read した時に not-found を即答する negative coverage」用だが、**q14 の lineitem は
range scan のみで point-probe されない → 完全に無駄**。lineitem 600k を scan して 7630 を残し
592k を reject、その 592k 全キーを送って proxy は record_row_cache(not-found) を 59万回実行
していた(これが stage_local 324ms の正体でもある)。

メモリ「20x肥大はOCC range keys」の転送版。memory note は materialize(server RAM)を move 化で
緩和したが、**転送と proxy staging への影響は手付かずだった**。

### 修正(MySQL 無改変・proxy+server+proto)
- proto `PlanStep.suppress_filtered_keys=15` 追加。
- server: 主スキャン/2次スキャン/並列スキャンの3経路すべてで、フラグ時 `add_filtered_keys` を
  skip(並列経路は worker の push 自体を抑止しメモリも節約)。
- proxy `execute_read_plan`: **「filter 付き scan step かつ その table_name が他のどの step にも
  現れない」step にのみ flag を立てる**。table が1 step しか触れない = 後段 point-read が構造上
  存在しない、の安全条件。aggregate-stamp 済み step は base row を出さないので除外。
- 健全性: suppress しても最悪「予見できなかった probe が cache-miss fallback になる」だけで
  結果不変(filtered_keys は性能用の安全網であって正しさの根拠ではない)。

### 効果(SF=1 公式、md5 22/22 OK 維持)
| Q | 前 | 後 | InnoDB | 判定 |
|---|---|---|---|---|
| q4 | 1.99 | **0.70** | 0.72 | lose→**WIN** |
| q10 | 3.59 | **1.75** | 1.64 | lose→~tie |
| q12 | 7.73 | **0.66** | 2.49 | lose→**WIN 3.8x** |
| q14 | 8.21 | **0.84** | 1.70 | lose→**WIN 2x** |
| q19 | 0.26 | **0.14** | 0.26 | tie→**WIN** |
| q2/q9/q13/q16/q21 等 | | 微減 | | |

- **suite 合計 90.55 → 70.83s(-20s/-22%)**。対 InnoDB ギャップ 46.4s → 26.7s。
- **勝ち 5/22 → 9/22**(q1,q4,q5,q6,q12,q14,q15,q17,q19)。
- q14 SF=0.1: 0.736→0.109s(6.7x)、resp_b 15.2MB→0.97MB、stage_local 324ms→4.3ms。
- 残ギャップはほぼ q18 単独(30.94 vs 2.17 = 28.8s)に集約。次は q18 が事実上唯一の壁。

---

## [2026-06-13] エントリ6: q18 完全解の設計確定(agg-step を共通基盤に Phase A/B)

### 数学的必然
filtered_keys 後 helios 70.83 vs InnoDB 44.15(gap 26.7s)。**他の負けクエリのギャップ合計は
14.6s に過ぎず、q18(28.8s 差)を解かない限り suite 逆転は不可能**。q18 が唯一の道。
上位敗者 q21/q13/q9/q8 は再 trace で **residual(MySQL executor CPU)律速**(filtered_keys 的
転送の無駄なし)→ join+agg を server 押し込みしないと改善せず、q18 と同じ大型機構が必要。

### q18 構造(SF=0.1 trace: staged 1.36M 行, stage 974ms+RPC 788ms)
- 内側 `lineitem GROUP BY l_orderkey HAVING SUM(l_quantity)>300`: 600k scan→57 鍵。
- 外側 `customer⋈orders⋈lineitem`: IN 通過後 orders は 57 のみだが prefetch は一括先行
  ステージングで IN を適用できず、orders 全件(150k)を source に外側 lineitem 600k 全件 fetch。
- agg-pushdown の override は IN サブクエリ(MaterializeIterator)に挿せない(MySQL 無改変の壁)。

### 設計: agg step を共通基盤に
server の `emit_agg_groups` は HAVING を server 側適用でき(`agg_having_passes`)、
semijoin source は `extract_value_column(scan_values, source_column)` で群行の列を読める。
→ **inner 集約を専用 agg step として発行**し、その 57 (orderkey, sum) 群行を2用途に使う:
- **Phase A(外側縮約・GS不要・先行実装)**: orders scan に semijoin(source=agg step,
  source_column=0=orderkey, probe=o_orderkey)を付与 → orders 57 に縮約 → 外側 lineitem/
  customer の for_each は自動的に 57 件分だけ。IN は positive semijoin なので result-preserving。
  期待: 外側 6M 除去 → q18 ~16s。内側 6M は残る。
- **Phase B(内側除去・GS型・後続)**: 内側 lineitem scan を GS-skip、handler が agg step の
  群行を l_quantity=sum の合成 lineitem 行として供給 → MySQL が 57 鍵 materialize。
  期待: 内側 6M も除去 → q18 ~2s。**suite 逆転**(helios ~42 < InnoDB 44)。

### リスクと検証
- semijoin source = agg step(HAVING>300 群行)は IN 条件と厳密一致 → result-preserving。
  exact-decimal sum は agg pushdown 既存。anti-join でない(positive IN)ので whitelist OK。
- 最大リスクは autogen の q18 形状認識と step 順序(agg step を orders より前に)。md5 で gate。
- Phase A 単独でも -15s の bankable 改善。段階コミットで進める。

---

## [2026-06-13] エントリ7: q18 Phase A 実装・動作(外側縮約、md5 22/22)

### 実装(gate `HELIOS_Q18_SEMIJOIN`, default OFF)
- proxy: `helios_try_register_grouped_semijoin`(ha_lineairdb.cc)が IN サブクエリ
  `o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity)>300)`
  を厳密認識(単一 group col / 出力=group col / HAVING SUM / 内側 WHERE 無し / 両表 our engine)。
  既存 `helios_build_phase_b_spec` で agg spec(server HAVING 込み)を組み、tx に
  GroupedSemijoin{inner=lineitem, spec, outer=orders, probe_col=o_orderkey} 登録。
- autogen: 登録があれば agg step(lineitem 全 scan + spec)を**先頭に挿入**し source_step を
  +1 remap、orders の plain scan step に semijoin(source=agg step, source_col=0=l_orderkey,
  probe=o_orderkey)を付与。
- server: **semijoin(sj_reject)を for_each 経路だけでなく plain scan emit でも適用**するよう
  `fe_semijoins` 構築をブランチ前に移動(非 semijoin step では空=no-op、回帰無し)。
- バグ修正: 当初 plain scan で sj_reject 未適用 → orders 縮約されず(staged 不変)。移動で解決。

### 効果(md5 22/22 OK 維持、両 SF)
| | baseline | Phase A |
|---|---|---|
| q18 SF=0.1 | 2.79s | **1.59s** |
| q18 SF=1 | 30.94s | **19.0s** (-12s, rows=57 正しい) |
| staged_rows SF=0.1 | 1.36M | **600k**(外側 600k+150k 除去、内側 600k 残) |

- 外側 lineitem/customer/orders を IN 通過 57 件に縮約成功。**内側 lineitem 6M scan が残り 19s の主因**。
- 他 21 クエリ回帰なし(SF=0.1 matrix 全 OK)。server 変更は非 gate 経路で no-op。
- 次: Phase B(内側 raw scan を GS-skip し agg 群行を l_quantity=sum の合成 lineitem 行として供給)
  → q18 ~3s 見込み。GS を単一列 SUM 対応へ拡張する必要。

---

## [2026-06-13] エントリ8: ★★q18 完全制覇★★ Phase B(内側合成供給)実装 → q18 30.94→5.14s

### Phase B 実装(gate `HELIOS_Q18_GS`, 現 default ON)
- GsRegistration に `single_sum` モード追加(col_a を SUM 列に再利用、a*(1-b) 分解不要)。
- 認識(helios_try_register_grouped_semijoin)で、HAVING の SUM 引数が T の単一非NULL列かつ
  group 列と別、かつ **内側 read_set が {group,sum} のみ**を満たす時、内側 lineitem leaf に
  single_sum GS を登録。autogen が GS-skip で内側 raw scan を drop。
- gs_fill_buffers に single_sum 分岐(1 群行→1 合成行: l_quantity=server の exact sum)。
- **罠と解決**: 当初 prefetch cache miss で abort。真因 = q18 内側は GROUP BY l_orderkey が
  PK 先頭列のため **index scan(index_first→index_next)**経由で、GS serving が rnd_init のみ
  hook だった(q15 は table scan だったので効いた)。
  → gs_fill_buffers が secondary_index_results_/payloads_(index 経路バッファ)も populate、
  index_read_map に gs_skipped 分岐(full scan 開始時に GS 供給)を追加。解決。

### 安全強化(default-ON 化に伴い)
- outer 表が plain inner-join leaf(outer-join inner でない / sj-aj nest 外 / top-level qb)で
  あること、join 鍵(l_orderkey vs o_orderkey)が byte 互換・非NULL であることを認識で検証。
- single_sum は read_set が {group,sum} のみの時だけ(他列は "0" template になるため)。

### 効果(SF=1 公式、default-ON 標準 env、md5 22/22 OK)
| | baseline | Phase A | Phase A+B |
|---|---|---|---|
| q18 SF=0.1 | 2.79s | 1.59s | **0.36s** |
| q18 SF=1 | 30.94s | 19.0s | **5.14s**（rows=57, 2 RPC, staged 50, resp 3.8KB） |
| 内側 lineitem | staged 6M | staged 6M | **GS 合成 57**（index 供給） |
| 外側 lineitem/cust/orders | 全件 | 57縮約 | 57縮約 |

- **q18 30.94→5.14s(-25.8s, 6x)。対 InnoDB 14.3x→2.4x。**
- mysqld RSS 4384→2621MB(大量ステージ解消)。
- **suite 合計 ≈ 45.5s**（run間ノイズ 45.2-46.1）vs InnoDB 44.15 — ほぼ拮抗、わずかに上。
  決定的逆転には q21(13.2s)/q13(6.1s)/q22(3.3s) であと数秒。
- 機構: agg step(semijoin source)+ GS(内側合成供給)が agg spec を共有。MySQL 無改変。

---

## [2026-06-13] エントリ9: q18 二重集約の統合 → q18 5.14→2.65s(InnoDB 2.16s 並み)

### 真因(trace)
q18 5.14s = **2 RPC × ~2.3s = lineitem 6M を2回 server 集約**(agg step[semijoin source] +
GS serving RPC)。データ転送は 38KB と極小、residual も 5ms。純粋に server 集約スキャン2回が律速。

### 統合(MySQL 無改変)
主 prefetch の agg step が group 行(57)を tx に cache(`grouped_semijoin_groups_`、
inner_table_key で識別)。gs_fill_buffers は RPC の代わりにこの cache を読む(無ければ RPC に
fallback=q15 経路)。→ 6M 集約スキャンが1回に。

### 効果(md5 22/22 OK)
- **q18 SF=1: 5.14→2.65s**(rpc/tx 2.0→1.0)。対 InnoDB 2.16s = 1.2x まで肉薄。
- q18 単独 3 回: 2.74/2.93/2.64s 安定。

### Codex review(read-only)結論
- q18 の正確な形状では **sound**: HAVING フィルタ済 group key 集合 = IN 集合(positive membership)、
  単一合成行の l_quantity=S は再 SUM で S 再現、NULL group key はガード済。
- broader recognizer の silent-corruption edge(将来 generalize 時に要対処): IN が NOT/OR/CASE/
  select-list 下にある場合の positive 文脈証明、key の真の SQL 等価性(signedness/collation/
  decimal metadata)、sum store の rounding/truncation 警告での abort。現状 q18 形状には影響なし。

### 計測環境の注意(重要)
本マシンは他ワークツリー(/tmp/ordo-worktrees の 7+ codex セッション)と**共有**。helios は
server 並列(HELIOS_PARALLEL_SERVER)依存のため**コア競合で重 join(q8/q9/q13/q21)が大きく
悪化**(suite 45→53s)。InnoDB は単スレッドで競合に鈍感(41.4s で安定)。**公平比較は静穏窓で、
かつ helios/InnoDB を同条件・背中合わせで取る必要**。q18/filtered_keys の構造改善は競合非依存で堅牢。

---

## [2026-06-13] エントリ10: ★Phase17 総括★ 公式最終比較 + 残フロンティア

### 公式最終比較(静穏窓 load 1.79、helios/InnoDB 背中合わせ、md5 22/22 OK)
| Q | helios | InnoDB | 比 | | Q | helios | InnoDB | 比 |
|---|---|---|---|---|---|---|---|---|
| q1 | **0.83** | 10.37 | 0.08★ | | q12 | **0.62** | 2.41 | 0.26★ |
| q2 | 0.21 | 0.14 | 1.50 | | q13 | 5.82 | 3.84 | 1.52 |
| q3 | 1.24 | 1.19 | 1.04 | | q14 | **0.87** | 1.69 | 0.51★ |
| q4 | 0.69 | 0.60 | 1.15 | | q15 | **1.11** | 3.53 | 0.31★ |
| q5 | 1.00 | 0.94 | 1.06 | | q16 | 0.59 | 0.32 | 1.84 |
| q6 | **0.40** | 1.56 | 0.26★ | | q17 | 0.31 | 0.29 | 1.07 |
| q7 | 1.46 | 0.89 | 1.64 | | q18 | 2.60 | 2.16 | 1.20 |
| q8 | 2.97 | 2.51 | 1.18 | | q19 | **0.12** | 0.18 | 0.67★ |
| q9 | 3.30 | 1.52 | 2.17 | | q20 | 0.46 | 0.23 | 2.00 |
| q10 | 1.83 | 1.62 | 1.13 | | q21 | 12.98 | 4.99 | 2.60 |
| q11 | 0.28 | 0.13 | 2.15 | | q22 | 3.10 | 0.18 | 17.2 |
| | | | | | **計** | **42.79** | **41.29** | **1.036** |

### Phase17 全体の到達点
- **suite 90.55 → 42.79s(2.12x 改善)。対 InnoDB 2.05x → 1.036x(実質拮抗、+1.5s)。勝ち 5→6/22。**
- 二大成果(共に MySQL 無改変・md5 22/22・OLTP errors 0・Codex GO):
  1. **filtered_keys 抑止**(-20s): filter scan の reject 行キー送信が無駄。q14 8.2→0.84, q12 7.7→0.66。
  2. **q18 grouped-semijoin + GS 合成 + 集約統合**(-28.3s): 30.94→2.60s(14.3x→1.2x)。天王山攻略。
- mysqld RSS 4384→2621MB。

### 必達(suite < InnoDB)に届かなかった理由と残フロンティア
あと **-1.5s**。残ギャップの構造:
- **q21(12.98 vs 4.99, -8s)**: 最難。lineitem 3重自己結合 + EXISTS(l2) + NOT EXISTS(l3 anti-join)。
  staged 3.6M + MySQL CPU 6.8s。EXISTS/NOT EXISTS を per-orderkey 述語として server 集約 pushdown
  すれば staging 激減見込みだが、**q18 級の大型機構 + anti-join 正しさが繊細**。
- **q22(3.10 vs 0.18, -2.9s)**: NOT EXISTS(orders) が全 150k customer 駆動で orders 1.5M staging。
  真因 = customer filter(`SUBSTRING(c_phone,1,2) IN(...)` + `c_acctbal > (scalar avg subquery)`)が
  orders probe 前に適用不可(SUBSTRING/scalar-subquery は push 困難)。q18 と同型の over-fetch。
- **小 point-read 系(q2/q11/q20/q16/q7)**: 各 0.1-1.5s だが ratio 1.5-2.2x。disaggregation の
  per-statement RPC/staging 固定費を InnoDB のローカルアクセスが上回る**構造的劣位**(量が小さく
  pushdown で回収する余地が薄い)。
- **q13(5.82 vs 3.84)**: orders が `o_comment NOT LIKE` を MySQL 評価のため o_comment(49B)ship。
  LIKE は evaluator/serializer 対応済だが LEFT JOIN ON句 filter が build_single_table_filter 未抽出。
  ON句 filter の右表 push は正しさ繊細。期待 -0.8s。

### 計測上の重大注意
共有マシン(他 7+ codex worktree)競合で helios suite は 42.8(静穏)〜52.8s(競合)に振れる。
InnoDB は単スレッドで 41.3s 安定。**専有環境でないと「suite < InnoDB」の最終判定は不可能**。
本フェーズの構造改善(filtered_keys/q18)は競合非依存で堅牢に再現。次フェーズは専有環境 + q21/q22。

## [2026-06-13] エントリ11: TPC-C T1 退行疑い調査 → 退行なし(モード差+WSL/サーバ差)

ユーザ指摘「前は T1 で 200 req/s 出ていた、最近のコミットで 108 に退行?」を厳密検証。
**計測手順をスクリプト化**(`scripts/dev/tpcc_compare.sh`、commit 済): load 毎に server
クリーン再起動を強制(怠ると in-mem 残渣で CREATE 失敗 → 無出力の事故を実際に起こした)、
mysqld を固定 env で再起動、稼働バイナリがビルド後か検証、CSV に throughput/goodput/retry 記録。

### 同一マシン(XG6326-2U)実測(SF=1, T1, 各3回 mean)
| 版 | DSL `--prefetch` | autogen `--prefetch-stmt` |
|---|---|---|
| feat/prefetch-autogen (~/helios, LineairDB 4852e527) | 119.7 (※103 errors) | 87.1 |
| baseline 9c01359 (Phase17前) | **119.4** (116–124) | **103.9** (102–106) |
| HEAD Phase17 | **115.0** (105–124) | ~104 典型(稀に165–182スパイク) |

### 結論
- **Phase17 に退行なし**: baseline ≈ HEAD(DSL ~118, autogen ~104)。むしろ autogen は
  feat 87 → 現 104 と**改善**(phase15/16 の autogen 強化の成果)。HEAD は下振れせず稀に上振れ。
- **「108」= autogen モード**(このサーバで全版 ~87–104)。ユーザが回帰テストで見た値。
- **「180–200」はこのサーバのどの版でも再現しない**(DSL 最大 ~124)。メモリの WSL explicit=196.2
  に一致 → **WSL 時代の別ハード数値**。memory「WSL時代の絶対数値とは比較しない」の通り。
- **T1 は遅延律速で振れる**(HEAD autogen 100–182)。手順固定(tpcc_compare.sh)で再現性確保。
- 教訓再確認: ① load 前に server 再起動必須 ② stop スクリプトは全 mysqld/server を巻き込む
  (InnoDB ref 3308/3309 も。ただしディスク永続なので restart で復活) ③ pgrep レースで
  「already running」誤判定 → 旧 server 死亡待ちを挟む。

---

### first-match(scan_limit=1)レバー: 試作 → 不採用(antijoin の超読みで abort)
EXISTS/antijoin の inner probe は first-match(1行で十分)なので scan_limit=1 で staging 削減を狙い、
**試作実装**(collect_first_match_inners で NESTED_LOOP_JOIN(ANTI/SEMI)の bare-REF inner をマーク →
for_each step に scan_limit=1)。q21 の l2/l3 は `FILTER(l_suppkey<>...)→REF` で bare でないため
正しく除外された(ガード機能)。
- **実測**: q22 staged 1.5M→250k に激減(機構は動作)。だが **prefetch cache miss で abort(ERROR)**。
  真因: MySQL の nested-loop **antijoin はマッチ後も index_next を呼び**、limit-1 で truncated と
  マークされた範囲の先を読む → fail-closed abort。**first-match の素朴な仮定(マッチ後に読み止める)が
  MySQL の antijoin 実装では崩れる**。
- SEMI 限定にすれば q22(ANTI)は対象外で回帰しないが、q22 は改善しない。**不採用・revert**。
  正しく効かせるには「truncated 範囲を antijoin が読んでも、それが limit を意図した step なら
  END_OF_FILE を返して abort しない」consumer 側の対応が要る(=staging が membership を表す契約)。
  これは proxy consumer の改修で、次フェーズ候補。










---

## [2026-06-13] エントリ12: ★計測環境の重大発見★ C6 deep-sleep が OLTP T1 を 3.7x throttle

ユーザの「TPC-C 1T が遅すぎる(前は200出た)」疑念を追ったら、**真因は CPU の C6 C-state(復帰遅延170µs)だった**。コードでも退行でもない。

### 実測(HEAD, SF=1, T1, tpcc_compare.sh ×3, 同一マシン)
| モード | schedutil + C6有効(before) | **performance + C6無効(after)** | 倍率 |
|---|---|---|---|
| autogen `--prefetch-stmt` | ~104 | **390.7**(380–398, retry0) | 3.7x |
| DSL `--prefetch` | ~115 | **440.4**(432–446, retry0) | 3.8x |

### 機構
T1 は単一クライアントの遅延律速。TPC-C 1tx ≈ 20–30 RPC 往復で、各アイドルごとに CPU が C6 に落ち
復帰に 170µs。170µs × ~30 ≈ 5–6ms/tx の上乗せ → before 9ms/tx(108)が after 2.3ms/tx(440)。

### 含意(重要)
- **「200」はローカルで余裕で超える**(本来 ~390–440)。AWS c6i.24xlarge の 363 すら上回る
  (ベアメタルはコアが速く、C-state を外せば AWS 超え)。過去の「200」記憶は AWS or C6 throttle 前。
- これまでの **TPC-C/TPC-H 絶対値は全て C6 throttle 下**。相対比較(helios vs InnoDB)は同条件なので
  結論不変(Phase17 退行なしも不変)。だが**絶対値の再計測は C-state 無効が前提**。
- 設定(マシン全体・再起動で消える): `cpupower frequency-set -g performance` +
  `cpupower idle-set -D 50`(or sysfs state3/disable=1)。AWS は大型/metal のみ可、
  恒久は GRUB `intel_idle.max_cstate=1 processor.max_cstate=1`。Graviton 不可。
- 計測手順(tpcc_compare.sh 等)に C6 無効チェックを入れるべき。今後の SF=1 milestone も C-state 固定で。

---

## [2026-06-13] エントリ13: C6 無効での全面再計測(TPC-H/TATP/TPC-C × helios/InnoDB)

ユーザ依頼で performance governor + C6 無効を前提に一式再計測。`scripts/dev/cstate_guard.sh`
追加(governor!=performance or 深いC-state有効を warn)し sf1_milestone.sh / tpcc_compare.sh に組込。

### 結果(C6 off, SF=1, T1)
| ワークロード | helios (C6 on→off) | InnoDB (C6 on→off) | 備考 |
|---|---|---|---|
| **TPC-H suite** | 42.79 → **41.58s** | 41.29 → **42.98s** | md5 22/22 OK。ほぼ互角(ノイズ帯) |
| **TPC-C T1 autogen** | ~104 → **390** | — | helios 3.7x |
| **TPC-C T1 DSL** | ~115 → **440** | 56.9 → 70.9 | helios 3.8x / InnoDB ~1.25x |
| **TATP T1** | 1462.9 → **5379** | — → 682.8 | helios 3.7x |

### 結論(C6 効果の本質)
- **latency 律速(OLTP T1: TPC-C/TATP)は C6 off で一律 ~3.7x**。単一端末は RPC 往復ごとに
  CPU がアイドル→C6(170µs)で復帰遅延が積もるため。TPC-C 1tx≈20-30 RPC × 170µs ≈ 5-6ms/tx。
- **CPU 律速(TPC-H 並列 scan)は C6 ほぼ無関係**(コアが常時ビジー)。helios 42.79→41.58 と微差。
- **InnoDB OLTP は fsync 律速**なので C6 off でも微増(56.9→70.9)。helios(インメモリ・no fsync)が
  3.7x 跳ねたのと対照的 → OLTP の helios>>InnoDB は大部分が durability 差。
- **load も C6 off で高速化**: helios TPC-H load 86→30s、InnoDB TPC-H load 360→346s。
- TPC-H suite は C6 込みでも **helios ≈ InnoDB(互角)**。この run では helios が 1.4s 前へ出たが
  ノイズ帯内。**Phase17 退行なしの結論は不変**。
- 計測環境の確定事項: **絶対値計測は C-state 固定(performance + idle-set -D 50)必須**。相対比較は不変。
