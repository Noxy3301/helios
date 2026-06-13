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

---

## [2026-06-13] エントリ14: ★Compact 前チェックポイント / 引き継ぎ★

context compact を挟むため現状を集約。**working tree クリーン・全 commit 済み**(branch claude/prefetch-maxopt)。

### Phase17 成果サマリ(全て MySQL 無改変・md5 22/22 維持・OLTP errors 0・Codex GO)
1. **filtered_keys 抑止**(commit 39de1c1, -20s): filter scan の reject 行キー送信が無駄、単一 step 表で suppress。
2. **q18 天王山 攻略**(3108f4d/d58c976/1875383/4558f21, 30.9→2.6s, 14.3x→1.2x): agg-step semijoin(外側縮約)
   + GS 合成供給(内側 index-scan 経路も対応)+ 2回の集約を tx cache で1回に統合。**default ON**
   (`HELIOS_Q18_SEMIJOIN=0`/`HELIOS_Q18_GS=0` で OFF)。
3. **suite 90.55 → ~41.6s(2.1x改善)、対 InnoDB 2.05x→~1.0x(互角)**。勝ち 5→6/22。

### ★最重要の環境発見(エントリ11-13)★
- **CPU C6 deep-sleep(復帰170µs)が latency律速 OLTP T1 を 3.7x throttle**していた。
  performance governor + C6 無効で TPC-C T1 autogen 104→390 / DSL 115→440、TATP 1462→5379。
- **TPC-H は CPU 律速で C6 ほぼ無関係**(helios 42.79→41.58)。InnoDB OLTP は fsync 律速で C6 微増。
- 「前は200出た」= AWS(c6i.24xl 363)or C6 制御前の記憶。ローカル本来は OLTP T1 ~390-440。
- **Phase17 退行なし**(baseline 9c01359 = HEAD、feat/prefetch-autogen も実機計測)。
- **絶対値計測は C-state 固定が前提**: `sudo cpupower frequency-set -g performance; sudo cpupower idle-set -D 50`
  (再起動で消える)。恒久/AWS は GRUB `intel_idle.max_cstate=1 processor.max_cstate=1`。

### 現在の環境状態(compact 後に再開する人へ)
- CPU: **performance + C6 無効**(再起動で戻る。戻ったら上記コマンド再実行。`cstate_guard` が warn)。
- ポート: 9999=helios server / 3307=helios mysqld(**今 TATP ロード済み**、TPC-H 要なら reload)/
  3308=InnoDB TPC-H ref(lineitem 6M, 復元済み)。3309(SF=0.1 InnoDB ref)は停止中・要時 restart。
- 計測 env(標準): mysqld gates `HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1
  HELIOS_ENABLE_SEMIJOIN=1`、server `HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1`、
  接続後 prefetch sysvar 2つ ON。load 前は必ず server 再起動。

### 固定済み資産
- スクリプト: `scripts/dev/{sf1_milestone,oltp_regression,plan_stability_test,start_innodb_ref,
  tpcc_compare,cstate_guard}.sh`(後ろ3つは Phase17 新規、cstate_guard は milestone/tpcc_compare に組込済)。
- メモリ: helios-filtered-keys-suppression / helios-q18-grouped-semijoin / helios-cstate-oltp-throttle。

### 未解決・次手の候補(優先順)
1. **TPC-H suite を warm 状態で複数回測り helios vs InnoDB を確定**(現状互角、helios が前に出た run も
   あるがノイズ帯。warm + 多 run で勝ち/互角を pin)。
2. **q21(最大の残敗者, ~13s, 対 InnoDB 2.6x)**: lineitem 3重自己結合 + EXISTS(l2)/NOT EXISTS(l3)。
   inner が FILTER(REF) で first-match 不可。**filtered-existence の server 集約 pushdown**(q18級)が要る。
3. **q22(NOT EXISTS over-fetch)**: first-match(scan_limit=1)を試作したが MySQL antijoin が limit-1 を
   超読みして abort(revert 済, commit f985be3)。consumer 側「membership-staging 契約」が本筋。
4. q13(o_comment NOT LIKE が LEFT JOIN ON句で未 push)、小 point-read 群(disaggregation 固定費)。
- goal の必達(suite < InnoDB)は C-state 込みで**ほぼ達成(互角)**。decisive な勝ちには q21 が鍵。

---

## エントリ15: TPC-H SF=1 warm 全22クエリ helios vs InnoDB 確定計測(2026-06-13)

compact 後の最初の作業(次手候補①)。**C6 無効 + performance governor**下、helios と InnoDB を
それぞれ warm-up + 2run の最小値で比較。md5 22/22 OK。

### 計測条件
- helios: TPC-H SF=1 リロード(server 再起動で TATP 残渣クリア → gates 付き mysqld → 並列 load 31.4s,
  lineitem=6001215)。prefetch sysvar 2つ ON。port 3307。
- InnoDB: 3308(16G buffer pool, disk 永続, lineitem=6001215, 再ロード不要)。
- 各 warm-up 1回 + 計測 2回、per-query は 2run の min を採用(ノイズ低減)。
- helios suite: r1=42.59 / r2=41.76s。InnoDB: r1=41.78 / r2=41.33s。

### 結果(秒, 各クエリ 2run min)
```
query   helios   innodb   ratio  verdict      query   helios   innodb   ratio  verdict
q1        0.77    10.22   0.08x  WIN          q12       0.55     2.41   0.23x  WIN
q2        0.13     0.10   1.30x  lose         q13       5.80     3.96   1.46x  lose
q3        1.20     1.21   0.99x  ~tie         q14       0.74     1.68   0.44x  WIN
q4        0.59     0.61   0.97x  ~tie         q15       0.95     3.52   0.27x  WIN
q5        0.99     0.94   1.05x  lose         q16       0.53     0.31   1.71x  lose
q6        0.38     1.54   0.25x  WIN          q17       0.23     0.28   0.82x  WIN
q7        1.42     0.90   1.58x  lose         q18       2.56     2.14   1.20x  lose
q8        2.65     2.39   1.11x  lose         q19       0.11     0.15   0.73x  WIN
q9        3.30     1.60   2.06x  lose         q20       0.40     0.25   1.60x  lose
q10       1.69     1.60   1.06x  lose         q21      13.34     4.88   2.73x  lose
q11       0.21     0.14   1.50x  lose         q22       3.13     0.17  18.41x  lose
```
- **suite: helios 41.67s vs InnoDB 41.00s = 1.016x(互角・誤差帯)**。
- median per-query ratio = 1.083x。勝ち(<0.95x)=7 / tie=2 / 負け(>1.05x)=13。md5 22/22 OK。

### 読み取り(decisive win への梃子)
- **絶対損失は q21 が突出**: +8.46s(2.73x)。q21 を InnoDB 並み(4.88s)にできれば suite 33.2s →
  対 InnoDB **0.81x の明確な勝ち**になる。**q21 が単独の天王山**(他を全部足しても q21 1本に及ばない)。
- q22 は比率 18.41x と派手だが絶対は +2.96s。q9 +1.70s、q13 +1.84s が次点。
- 勝ち筋は agg pushdown 系(q1 0.08x, q12 0.23x, q6 0.25x, q15 0.27x, q14 0.44x)。
- 小クエリの負け(q2/q11/q16/q20 等)は disaggregation 固定費(RPC 往復)で 0.1-0.5s 帯、絶対影響小。
- **結論: 現状は「互角」を warm 多 run で pin 完了。decisive 勝ちの唯一の鍵は q21**。
  次手は候補②(q21 filtered-existence server 集約 pushdown)に集中するのが最も費用対効果が高い。

---

## エントリ16: q21/q22 詳細 root-cause(EXPLAIN ANALYZE + RPC trace)(2026-06-13)

候補①の地図(q21/q22 が最大の負け)を受け、両者の「なぜ InnoDB が速く helios が遅いか」を実測で確定。
**結論: q21/q22 の遅さは plan 差でも per-probe 遅延でもなく、prefetch が driving scan の前に
inner 作業集合を「全量・直列」で staging するコスト**。InnoDB は (a) scan と probe を pipeline し、
(b) 存在述語を covering index で行 fetch 無しに解くため、この量を一切 materialize しない。

### 計測根拠
**q21(13.34s vs InnoDB 4.88s, 2.73x)**
- EXPLAIN(TREE)は両者**構造完全一致**(join 順・EXISTS=semijoin・NOT EXISTS=antijoin・lineitem は
  全て PRIMARY index lookup by l_orderkey)。cost 値だけ COST_V2 で ~6x 高め。plan 問題ではない。
- EXPLAIN ANALYZE: InnoDB の `Table scan on orders` は first row 1.6ms(即パイプライン)。
  helios は **first row 6559ms**(driving scan が出力を始めるまで 6.5s の空白)。空白後の per-probe は
  l1 0.0028ms×729413 / supplier 0.0008ms×1.83M / l2 0.0027ms×75871 / l3 0.0021ms×73089 と
  **InnoDB と同等**(prefetch cache 供給が B-tree 並み)。残り ~6.8s が join 本体。
- RPC trace(`ENABLE_RPC_TRACE`): RPC 4本・resp 合計 **293.8MB**・転送 1.96s(1本が 293.77MB)。
  6.5s 空白の内訳 ≈ 転送 2s + **proxy 側 293.8MB の decode/materialize ~4.5s**。
  293.8MB ≈ lineitem 全6M行 projection(l_orderkey,l_suppkey,l_receiptdate,l_commitdate,l_linenumber)
  + orders。**EXISTS/NOT EXISTS の存在判定に lineitem 行を丸ごと運んでいる**。

**q22(3.13s vs InnoDB 0.17s, 18.4x)**
- EXPLAIN ANALYZE: InnoDB は `Covering index lookup on orders using o_custkey`(index だけで存在判定・
  行 fetch 無し)0.003ms×19000=55ms、全体 190ms。helios は非covering `Index lookup`、per-probe は
  0.0078ms×19000 と速いが、**antijoin 外側 customer scan の first row が ~2429ms**(~2.3s 空白)。
- RPC trace: RPC 2本・resp **100.3MB**・転送 0.78s ≈ orders 全体(1.5M)。
  **NOT EXISTS の存在判定のために orders を全量 staging**(InnoDB は covering index で 0 materialize)。

### 梃子(設計方向・未実装)
- **q22(高 payoff・低リスク)**: orders 全量 staging を、存在する distinct o_custkey の
  **membership set**(≈150k uint32 ≈ 600KB、100MB の ~166x 縮小)に置換し NOT EXISTS をローカル解決
  =「membership-staging 契約」(worklog 候補③)。リスク: OCC validation に staged set の range/key を含める、
  NULL/重複 custkey、GROUP BY 安定性。
- **q21(高 payoff・中リスク)**: EXISTS(l2)/NOT EXISTS(l3) を **l_orderkey keyed の server 側
  grouped existence summary**(q18 流)に push し、l2/l3 行 materialize を排除。l1 は実行に行が要るが
  o_orderstatus='F' の orders に紐づく分へ staging を絞れるか検討。リスク: l_suppkey<>l1.l_suppkey の
  self-exclusion、重複 orderkey、bit-exact。first-match 単独は antijoin が limit-1 超読みで不可(revert 済 f985be3)。
- **汎用**: staging を実行と **overlap(stream/pipeline)** できれば直列 barrier を崩せ、q21/q22 以外にも効く
  (固定費 disaggregation の本丸)。OCC・単一RPC staging モデルとの両立を要検討。Codex に inline 証拠で深掘り依頼中。

注: この計測のため mysqld は `ENABLE_RPC_TRACE=1` 付きで再起動済み(trace 微小オーバーヘッド)。
次の本計測前に trace 無しで再起動推奨。

---

## エントリ17: q21/q22 設計統合(自己調査 + Codex deep research)(2026-06-13)

### RPC 本数の疑義 → 解消(prefetch は正常)
trace の「q21=4本/q22=2本」は prefetch 失敗ではない。バルク転送は両者とも **1本の
`TX_EXECUTE_READ_PLAN`** に集約済み(q21=293.8MB/4表, q22=100.3MB/2表)。残りの小 RPC は
`TX_GET_TABLE_STATS`(=33, cost model の行数/NDV、HELIOS_OPT_STATS/COST_V2 由来、各 ~238B/数十µs)。
`rpc_trace.cc` の type_to_string が 32 までしか case を持たず 33 を `UNDEFINED` 表示していただけ
(**1行の cosmetic ラベル漏れ**)。q22 commit は `ro_novalidate_commit:1` で validate/commit RPC 無し。
→ 本数は無問題。コストは「staging の量 + 直列 barrier」。

### sections 実測(真の内訳)
```
q21: stage_rpc_and_decode=2.34s + stage_local=4.00s = 6.34s(first-row gap 6.56s ≒ loss の 77.5%)
     staged rows=3,641,158 = lineitem 2,901,744 + orders 729,413 + supplier 10,000 + nation 1
q22: stage_rpc_and_decode=0.93s + stage_local=1.42s = 2.35s(first-row gap 2.43s ≒ loss の 82%)
     staged rows=1,650,000 = orders 1,500,000(全件) + customer 150,000
```
- **`stage_local`(proxy 側 decode/materialize)が最大要素**(q21 4.0s)。65MB/s は raw memcpy には遅く、
  row decode + hash/cache build + alloc が主因(Codex 評: hash-build/codec が有力、OCC read-set は仮説、
  dedup O(N²)は低)。server scan は RPC 1.96s 側。

### Codex の精緻化(診断の限界)
- **「staging が全て」は強すぎ**。q21 は staging を完全に消しても実行 6.78s 残り、InnoDB 4.88s に
  **1.9s 届かない**(per-probe は既に良好: l2 0.0027ms×75871≈205ms, l3 0.0021ms×73089≈153ms)。
  staging は loss の 77.5% = 支配的だが 100% ではない。
- q22 は診断が強い(per-probe 差は loss の ~3%)。orders 全件 staging が過大。

### 設計(Codex + 自己検証)
**q22 membership-staging(高 payoff・低リスク, 推奨第一手)**
- server: `DISTINCT o_custkey FROM orders WHERE o_custkey IS NOT NULL` を sorted uint32[] で ship
  (≈150k×4B=600KB、現 100.3MB の **~167x 縮小**、転送 ~4.7ms)。proxy は hash-set で NOT EXISTS をローカル解決。
- bit-exact risk: NULL(=は UNKNOWN, NULL inner は非マッチ)/ MySQL が first-match 後も読む問題 →
  inner 列が上に proj  されない semijoin/antijoin node にのみ contract 付与 / OCC phantom 保護
  (set を read epoch に紐付け or range validate、ro_novalidate は read-only bench 限定)。

**q21 grouped-existence summary(複雑・要注意)**
- per `l_orderkey`: `distinct_supp_count` / `late_distinct_supp_count` / `late_only_suppkey`。
  proxy 評価: EXISTS l2 ⇔ distinct_supp_count≥2 / NOT EXISTS l3 ⇔
  NOT(late_distinct≥2 OR (late_distinct=1 AND late_only_suppkey≠l1.suppkey))。
  **boolean では不可**(l_suppkey<>l1.l_suppkey の self-exclusion + 同一supplier重複lineitem対応に
  distinct-supplier-count が必須)。
- **★自己検証で判明した落とし穴★**: staged lineitem 2,901,744 は **SharedScan で l1/l2/l3 が共有する1本の
  fetch**で、その実体は **l1 が必要とする per-order lineitem 行**(l1 は orderkey index lookup 4行/order ×
  729413 orders ≈ 2.9M、receiptdate>commitdate で 2.51行に絞る)。l2/l3 を summary 化しても
  **l1 が同じ 2.9M 行を実体で要求するので staging 量はほぼ減らない**(lineitem は staged rows の 80%)。
  → q21 の existence summary は**正しさは出せても速度 win は小さい見込み**。l2/l3 probe(計 ~0.35s)を
  消すだけ。q21 の真の梃子は (a) **staging を実行と overlap(streaming)**(下限 ~max(6.34,6.78)=6.8s、
  ただし InnoDB 4.88s には未達)か (b) **l1⋈supplier⋈nation+existence+COUNT を server 側 full 集約 pushdown**
  (q18 の全クエリ版、大機構)。q21 は構造的に難物。

### 効果/労力ランキング
1. **q22 membership-staging**(barrier ~2.4s の大半消去、~0.6MB ship、リスク=handler existence contract)。
2. q21: existence summary 単独は薄い → overlap or full-agg-pushdown が要。複雑。

### SOTA 対応(Codex survey, 既存実装の有無付き)
- decorrelation/unnesting(Neumann 2412.04294)/ Yannakakis semijoin(helios: semijoin/filtered_keys/q18 GS 実装済)/
  magic sets / groupjoin・JOIN-AGG(1906.05745, helios q18 GS が該当)/ predicate transfer・SIP(q21 orders-F→lineitem,
  q22 customer→orders)/ **Bloom は NOT EXISTS 単体不可**(false-positive で出すべき行を落とす、exact fallback 要)/
  late materialization・covering-index existence(InnoDB q22 がこれ)/ disaggregated pushdown(Aurora/PushdownDB 2002.05837)。

### Codex 注記(streaming overlap)
汎用に barrier を崩せるが volume を減らさねば decode CPU は残る。q22 の NOT EXISTS は partial set で「不在」を
証明できず、key-range partition ごとに "complete" marker が必要。OCC は streaming 中の snapshot 揺れで
bit-exact 破綻 → read epoch 固定 or chunk 単位 version/range validation。

---

# Phase 18: q22 membership-staging(antijoin 存在判定の集合化)

目的: q22 の `NOT EXISTS(orders WHERE o_custkey=c_custkey)` で orders 全件(値付き 1.5M 行 / 100.3MB)を
FES staging しているのを、**存在する DISTINCT o_custkey 集合だけ**(≈150k key ≈ 600KB, ~167x 縮小)に置換し、
antijoin 存在判定を proxy ローカルで解く。狙い: first-row gap ~2.4s(loss の 82%)をほぼ消す。
md5 22/22 維持・OLTP 退行なしを必須とし、回帰テストで締める。SOTA(Codex): late-materialization /
covering-index existence(InnoDB q22 がこれ)/ semijoin-antijoin existence aggregation の disaggregated 版。

## 現状の経路(Explore + trace で確定)
- proto `PlanStep`(lineairdb.proto:279-326): table/index/bindings/for_each/filter/projection/aggregate/
  semijoins(14)/suppress_filtered_keys(15)。`SemijoinFilter`(330-339)。`StepResult`(346-)に
  scan_keys/scan_values/secondary_keys/group_sizes。
- 出力経路: autogen `compile_ref_lookup`(autogen.cc:467-681)の FES 分岐(~649-661)が orders を
  o_custkey 二次索引で for_each scan(`is_scan=true, for_each=true, index_name=o_custkey`)。
  ※ semijoin reduction(1351-1470)は antijoin leaf を `helios_sj_safe_leaf`(955-973)で除外
  (「antijoin は不在行こそ残す対象」)。つまり q22 orders は semijoin でなく素の FES で値ごと staging。
- staging: transaction.cc execute_read_plan(242-575)、FES 分岐(437-462)が `LocalSecondaryScanEntry`
  (secondary_keys/primary_keys)を push。serving: ha_lineairdb.cc index_read_map(2171-)→
  get_matching_primary_keys→lookup_secondary_scan_cache。
- server: lineairdb_rpc.cc handleTxExecuteReadPlan(1364-1478+)、FES probe 実行(1480+)。
- 既存の keys-only/membership 専用モードは無し。projection(値trim)/semijoin(source_column 抽出)/
  q18 GroupedSummary(集約 step + 合成 group 供給)が近い scaffolding。

## 設計(membership-staging contract)
- **proto**: `PlanStep` に `bool existence_only = 16`(命名要検討)。true の時、server は当該 FES/scan を
  実行し **probe された inner の DISTINCT 二次キー集合(o_custkey)だけ** を返す(値・per-probe primary key 無し)。
- **autogen**: ref-lookup が NOT EXISTS/antijoin の inner(AccessPath が antijoin、上位に inner 列が
  projection されない)の時に existence_only=true を立てる。判定は `helios_sj_safe_leaf` の antijoin 検出
  ロジック(is_sj_or_aj_nest 等)を再利用。
- **server**: existence_only step は scan して DISTINCT secondary key 集合を StepResult.secondary_keys に
  集約(values/primary_keys 省略)。
- **proxy staging**: membership 集合(secondary key の set)を保持する軽量 cache に格納。
- **handler serving**: antijoin の index_read 存在 probe を membership set 照合に置換
  (in-set → 合成 found 1行で NOT EXISTS reject、absent → not-found で customer 通過)。inner 列は
  上に出ないので合成行の値は不問。
- **bit-exact risk**: (1) NULL o_custkey は集合に入れない(=は UNKNOWN, 非マッチ); (2) MySQL antijoin が
  first-match 後も読む問題 → existence_only は inner 列が上に出ない antijoin/semijoin node 限定で付与
  (revert 済 first-match と別機構: scan_limit でなく membership 即答); (3) OCC phantom 保護 →
  集合を read epoch に紐付け or range validate(ro_novalidate は read-only bench 限定で既に安全);
  (4) GROUP BY cntrycode/ORDER BY/decimal 集約は MySQL 側のまま、membership は存在判定のみ変更。
- **gate**: `HELIOS_Q22_MEMBERSHIP`(default 後で判断、まず opt-in で検証)。

## 進め方
1. autogen の antijoin 検出と FES 生成箇所を精読 → existence_only を立てる最小箇所を特定。
2. proto/proxy struct/serialization に existence_only 追加。
3. server に DISTINCT-key 集約モード。
4. proxy staging + handler serving。
5. build(services 停止 → build_partial.sh)→ md5 q22 一致確認 → q22 計測。
6. 回帰: md5 22/22 + suite warm + OLTP(TPC-C/TATP)→ 退行0 確認。
7. NDV pre-warm(measurement hygiene, 別タスク・低優先): 計測前に全表/索引 NDV を一括取得する hook。

---

## エントリ18: Phase18 実装完了 — q22 membership-staging(existence_only)(2026-06-13)

membership-staging を **existence_only** モードとして実装。設計通り「antijoin inner は存在判定のみ
→ server が probe ごと最初の1 match で打ち切る」。**md5 22/22 OK・suite 41.67→38.66s・対 InnoDB
1.016x→0.943x で helios が前に出た**。

### 実装(MySQL 無改変・gate `HELIOS_Q22_MEMBERSHIP`・default OFF)
- proto `PlanStep.existence_only = 16`(suppress_filtered_keys と同じ4点配線)。
- proxy `ReadPlanStep.existence_only` + serialization(lineairdb_proxy.cc)。
- autogen: `collect_existence_only_antijoin_inners(root)` で **antijoin の inner が「FILTER 無しの素の
  leaf」(= `qep_leaf_info()` が true)** の表だけを存在判定安全と判定し、for_each step に existence_only。
  **SOUND な residual 検出**: q22 orders は素の index lookup → 採用、q21 l3 は `Filter:`(相関 residual
  l_suppkey<>l1.l_suppkey AND dates)付き → `qep_leaf_info` が false で除外。これで q21 を壊さない。
- server(lineairdb_rpc.cc): FES の4 scan path(parallel/serial × primary/secondary)で
  `if (existence_only) break;` を 1 match push 後に挿入。scan_limit は触らない→`materialized_scan_truncated_`
  が立たず handler の index_next が EOF を綺麗に返す(revert 済 first-match=scan_limit truncation とは別機構)。
- serving/staging は無改変(per-probe group 構造そのまま、group 当たり行数が ≤1 になるだけ)。

### 計測(SF=1 warm, gate ON vs OFF vs InnoDB)
- **q22: 3.13s → 0.95s**(対 InnoDB 18.4x → 5.59x)。
- trace: orders staged `keys=1500000`→**`99996`**(orders を持つ distinct o_custkey, 15x減)、
  resp 100.3MB→**21.5MB**(残りは customer 150k 行が主)、staging 2.35s→**0.59s**
  (stage_rpc_and_decode 0.93→0.36 / stage_local 1.42→0.23)、staged_rows 1.65M→**250k**。
- 実行時 antijoin probe 19000 のうち order 有 12616(reject)/無 6384(NOT EXISTS 通過)= 結果 6384 行と一致。
- **suite: 41.67 → 38.66s。対 InnoDB 1.016x → 0.943x(helios 勝ち)**。他21クエリ退行なし(全て誤差内)。
- md5 22/22 OK(gate ON、q21 含め全一致)。

### 残課題・次手
- q22 残差 0.95s vs InnoDB 0.17s(5.6x): 残りは customer 150k 行 full-value staging(21.5MB の主)+
  customer を2回 scan(AVG subquery + outer)。customer projection 圧縮 or AVG 用 scan 共有が次の梃子。
- existence_only の更なる削減: 値を捨て distinct key 集合のみ(21.5→数MB)化は handler の value-less serving 要、別途。
- gate を default ON にするか: 22/22 維持・suite 改善・residual 保護済み。OLTP 回帰確認後に判断。
- **次**: OLTP 回帰(TPC-C/TATP)で退行0 を確認 → 必要なら default ON 検討。

### 回帰テスト結果(Phase18, 全クリア)
performance + C6 無効下(cstate_guard OK)。新バイナリ(existence_only 実装後)で計測。
- **md5 22/22 OK**(gate ON、q21 含め全一致)。
- **TPC-H suite: 41.67 → 38.66s**(q22 -2.18s 主因、他21クエリ退行なし)。対 InnoDB 0.943x。
- **TPC-C T1**(autogen, default=gate OFF): mean 374.6(367-382)req/s, retry 0。baseline ~390 と誤差内・退行なし。
- **TATP T1**(autogen, default): 5305.7 req/s, retry 0, errors 0。baseline ~5379 と誤差内・退行なし。
- 結論: **q22 membership-staging は suite を勝ち越しに乗せつつ md5/OLTP に退行なし**。gate default ON は妥当
  (residual 保護で q21 安全)。現状は opt-in のまま。
- 注: 回帰計測で server in-mem は TATP ロード状態(TPC-H 要時は reload)。3308=InnoDB TPC-H ref 維持。

---

# Phase 19: q22 仕上げ + NDV pre-warm + q22 残差 + q21

user 指示: ①NDV pre-warm(計測ノイズ除去)→ ②q22 gate default ON → ③q22 残差(customer staging)→
④q21、の順で進める。議事録を書きながら。

## ① NDV pre-warm 調査
**前提の再確認(コード読解)**: stats/NDV は `ctx->proxy`(接続ごと)が `fetch_table_stats`(TX_GET_TABLE_STATS)
で取得するが、**NDV 値とロード状態は GLOBAL な TABLE_SHARE に seed**(`share->index_ndv_`,
`share->index_ndv_loaded_`, `share->stats_base_records`)。info() の fetch は `!share->index_ndv_loaded_`
で gate されるため、**ある接続が最初に各表へ触れて share を埋めれば、以降は別接続でも再 fetch されない**
(= stats RPC は「表ごと・mysqld 生存中に一度きり」)。
→ 仮説: warmup run 1回で全表 share が埋まり、計測 run では stats RPC が消える。実測で確認する。

**実測結果**: 仮説どおり。warm mysqld(warmup 1回後)では q21/q22 とも **RPC=1本(TX_EXECUTE_READ_PLAN)・
stats RPC ゼロ**。`EXPLAIN SELECT * FROM <table>`(optimizer は走るが実行しない)で info() を発火させ
share を seed できることも確認(cold shares で全8表 EXPLAIN → 続く q9/q21/q22 すべて RPC=1本・stats 0)。
**コード変更不要**。

**成果物**: `scripts/dev/prewarm_stats.sh`(SHOW TABLES → 各表 `EXPLAIN SELECT *` で share seed、
行転送ゼロ)。`sf1_milestone.sh` に sysvar 有効化直後の prewarm 呼び出しを追加。
→ 計測前に1回流せば stats RPC が measured query に混入しない。従来の begin/end piggyback・on-demand は
そのまま温存(通常運用は無変更)。「stats はベンチ中ノイズ」問題は **share-gated(表ごと一度)+ prewarm** で解消。

## ② q22 membership-staging を default ON
`HELIOS_Q22_MEMBERSHIP` を q18 と同じ default-ON パターンに変更(`env==nullptr || env[0]!='0'`、
`=0` で OFF)。検証: **env 未設定の mysqld で q22 ~0.92s(最適化が効く)・md5 22/22 OK**。
residual 保護(qep_leaf_info ベース)で q21 等は不変。OLTP は gate と無関係(antijoin inner 非該当)。

## ③ q22 残差(customer staging)調査 — near disaggregation floor
- projection は **default-ON で既に効いている**: q22 staged は `HELIOS_PROJECTION=0` で 53.4MB、
  ON で **21.5MB(60%削減)**。customer は read_set {c_custkey,c_phone,c_acctbal} に trim 済
  (o_custkey binding は c_custkey=PK で from_key、unsafe 化せず)。
- 残 21.5MB = projection 済み作業集合(customer 150k 駆動行 + orders existence マーカー)。
  customer 150k は AVG subquery + outer の両 scan で全件必要(filter は scan 後評価)→ 削れない。
  customer は1回 staging し2回 scan(batch_cache_hit)なので AVG pushdown でも staging は不変。
- **value-less existence は不可**: プロトコル上 scan_value `""` = not-found。found を `""` で送ると
  handler が「存在せず」と誤判定し customer を誤って通過させる(md5 破壊)。found は実値が要る。
- 結論: q22 0.92s vs InnoDB 0.17s の残差は **disaggregation の床**(150k 駆動行の RPC 転送+decode ~0.59s)。
  これ以上は customer scan 自体を staging しない = **full query pushdown(server で filter+AVG+antijoin+
  GROUP+SUM を計算し結果行のみ返す)**が要る大機構。suite は既に勝ち越し(0.943x)なので polish には過剰。
  → #3 は「projection で既に床近く、cheap lever 無し」と結論。full pushdown は将来課題として保留。

## ④ q21 調査 — cheap lever 無し、大機構が必要(慎重結論)
q21(13.34s vs InnoDB 4.88s, +8.46s)の梃子を精査。**安価な最適化は存在しない**と結論。

### なぜ各 lever が塞がっているか
- **SAUDI-supplier の SIP/predicate-transfer(lineitem を l_suppkey∈SAUDI で枝刈り)= 不可**:
  semijoin reduction は **probe 表の KEY** で枝刈りするが、lineitem(l1)は **l_orderkey で probe**
  (orders 駆動)、l_suppkey は VALUE 列で probe key でない。
  【訂正(Phase18-20 review, Claude grounded)】「lineitem に l_suppkey 二次索引も無い」は**事実誤認**:
  `third_party/benchbase/.../tpch/postload-mysql.sql:24` に `CREATE INDEX l_sk ON lineitem(l_suppkey)` が
  **実在**。ただし計測 config `bench/config/tpch.xml` が `<afterload>` を wire しないため計測では作られない
  だけ。→ **afterload を有効化すれば l_suppkey 二次索引が実体化し、supplier-set を l_suppkey で probe する
  SIP が未検討 lever として開く**(要: InnoDB 側との公平性 + LineairDB 二次索引 scan コストの実測)。
  加えて supplier の選択性は nation 経由(`n_name='SAUDI ARABIA'`)で **supplier 単一表述語でない** →
  semijoin source 条件(選択的単一表述語)も不成立。よって既存 semijoin で lineitem は枝刈りされず 2.9M のまま。
- **l1 date filter(`l_receiptdate>l_commitdate`)の push = 不可**: lineitem は **SharedScan で l1/l2/l3 が
  1 fetch 共有**(commit 5fab65c)。l2(EXISTS)は date 無条件の全行が要るため、共有 fetch に l1 の date
  filter を push すると l2 が壊れる。un-share して l1 だけ filter すると fetch が 1→3 本に増え逆効果。
- **既に最適化済み**: lineitem は projection 済(read_set {l_orderkey,l_suppkey,l_receiptdate,l_commitdate}
  + binding 強制列、column-form なので projection 維持)、orders は o_orderstatus='F' で 729k に絞り込み済、
  supplier 10k。staging 293.8MB の主は lineitem 2.9M×~60B(DATE は ASCII 10B×2 が効く副因)。
- **existence_only(Phase18)も効かない**: l3(NOT EXISTS)は相関 residual で除外、l2(EXISTS)も
  SharedScan で l1 と fetch 共有のため、存在判定化しても l1 が同じ行を実体で要し staging 不変。

### 構造(再掲)
13.34s = staging 6.5s(stage_rpc_and_decode 2.34 + stage_local 4.0)+ 実行 6.8s。実行の per-probe は
InnoDB 同等。staging は helios だけが払う純オーバーヘッド。**完全 overlap しても下限 ~max(6.5,6.8)=6.8s**
で InnoDB 4.88s には未達 → overlap 単独でも勝てない。

### 残る選択肢(いずれも大機構・別途判断)
1. **streaming/overlap prefetch**: staging barrier を崩す汎用機構。q21 以外にも効くが、chunk 化・partial
   cache・OCC(snapshot 固定 or chunk validation)で重い。単独では InnoDB 未達。
2. **q21 full pushdown**: server で orders⋈l1⋈supplier⋈nation + EXISTS/NOT EXISTS + COUNT/GROUP BY を
   計算し結果行(411 group)のみ返す。q1 agg-pushdown の多表・相関版で最も効くが最も重い・bit-exact риск大。
3. **DATE packed codec**: row value の DATE を ASCII(10B)→packed(3-4B)。全クエリ横断の codec 変更で
   lineitem 等の転送を一律削減(q21 で ~35MB 減)。範囲広く慎重要。

### 結論
q21 は **本セッションでの安全な実装範囲を超える**(大機構 or 横断 codec)。suite は q22 で既に勝ち越し
(0.943x)のため、q21 大機構は費用対効果と 22/22 リスクを user 判断にゆだねる。#4 は「精査して
cheap lever 無しを確定・選択肢を提示」で一旦クローズ。

---

# Phase 20: TPC-C / TATP カリカリチューニング(OLTP)

ゴール(user /goal): TPC-C と TATP を徹底チューニング。prefetch が理論値(ideal)にどれだけ近いかを
詳細検証し、内部パス・アクセス方法の無駄を削って性能改善。手法は TPC-H と同じ
(per-tx RPC trace → 理論最小との差分 → 無駄特定 → 削減 → md5/回帰 → 議事録)。

既知の出発点(メモリ helios-tpcc-prefetch-coverage-gap): TPC-C prefetch は coverage ~10% と低く、
PK-MRR batch_read 経路が prefetch plan 未 stage(warehouse 点読 = abort の 88%)、plan は once-per-tx
staging で多 statement 非対応、note_oneshot_miss は無条件 abort。まず実測で裏取りする。

## ① ベースライン + RPC trace 採取(理論値との差分測定)

### TPC-C T1 RPC trace 解析(autogen vs DSL, C6-off)
per-tx RPC 構成と sections 内訳(理論値との差分):
```
mode     NewOrder      Delivery     仕組み
autogen  22.8 RPC/tx   51 RPC/tx    読み文ごとに1 EXECUTE_READ_PLAN(statement-scoped)
DSL      2.1 RPC/tx    2.0 RPC/tx   tx 全読みを1 plan + 1 commit(@_tx_plan, transaction-scoped)
```
- throughput: DSL 401.7 / autogen 359.2 / InnoDB ~390 → **DSL は既に InnoDB 超え**、autogen がやや下。
- sections(NewOrder, us/tx):
  - DSL: helios 寄与 ~115us(stage_rpc_and_decode 90 + txplan_parse 15 + stage_local 10)。残り ~3200us は
    **MySQL 35文実行 + benchbase JDBC 往復**(InnoDB でも同じ・helios では削れない)→ **DSL prefetch は実質理論値**。
  - autogen: helios 寄与 ~579us(stage_rpc_and_decode 528=21.6 RPC×24.5us + autogen_compile 41 + stage_local 9)。
    NewOrder 全体 3844us の ~15%。autogen は文単位なので跨いでバッチ不可(JDBC 用途では DSL 注入できない)。
- **結論(TPC-C)**: TPC-C T1 は **JDBC/MySQL 文実行律速**で storage engine は少数派。DSL は near-ideal で
  InnoDB 超え済み。autogen の梃子は per-statement RPC の固定費(~24.5us/RPC)削減 = 全モード横断で効く。
  ただし TPC-C 全体への寄与は限定的(~15%上限)。**storage-engine の伸びしろは単文中心の TATP の方が大きい見込み**。

### TATP T1 RPC trace 解析(autogen, C6-off, 4483 req/s)
per-tx(read 系は 2 RPC = 1 EXECUTE_READ_PLAN + 1 VALIDATE_AND_COMMIT):
```
type           n      rpc/tx stmt us/tx  helios(stage/compile/local)  種別
GetAccessData  11101  2.1    1.0  146    rpc25+comp2+loc1             read-only(1 key)
GetSubData     10677  2.0    1.0  138    rpc25+comp2+loc1             read-only(1 key)
UpdLocation    4405   2.0    2.0  221    rpc29+comp2+loc2             1 read + 1 update
GetNewDest     3087   2.0    1.0  180    rpc40+comp4+loc4             read-only(join)
InsertCallFwd  2528   5.5    1.5  995    write 系
```
- helios 寄与は read 系で ~28us(stage_rpc_and_decode 25 + compile 2 + local 1)。残り ~110us は
  MySQL parse/optimize/execute + JDBC 往復 + **VALIDATE_AND_COMMIT RPC**。
- **★lever 候補★**: read-only tx が **commit RPC(VALIDATE_AND_COMMIT)を打っている**。TPC-H read は
  `ro_novalidate_commit` で commit RPC 無しだったが OLTP では validate RPC が残る。**read-only かつ全読みを
  1 EXECUTE_READ_PLAN で読み切った tx は単発アトミック読み → その timestamp で直列化可能で validation 不要**。
  blanket ro_novalidate(無条件 validation 省略=並行下で危険)とは別の、**「single-shot read-only は
  commit RPC をローカルで完結」**という SOUND 最適化が可能か commit パスで確認する(read-only TATP ~70% に効く)。

## ② single-key read-only commit-skip(SOUND・実装)
**機構**: read-only かつ全 read-set が単一 point read(`write/range/delta 無し + base_row_read_set ≤1`)の tx は、
読んだ版を生成した write の直後に直列順序へ置けるため **並行下でも常に直列化可能** → OCC validation RPC を
ローカル完結で省略。blanket ro_novalidate(無条件省略=並行下で危険)とは別物の、証明可能な単項目 read-only ケース。
multi-key は server の plan 読みが stateless(snapshot 非アトミック)ゆえ cross-key 不整合の可能性があり除外、
range は phantom 懸念で除外。gate `HELIOS_RO_SINGLEKEY_COMMIT`(default ON, =0 で OFF)。
実装: `prefetch_validate_and_commit` に ro_novalidate fast path の直後で分岐追加。

**効果(TATP T1 autogen)**: throughput **4483 → 4819.9 req/s(+7.5%)**, retry 0, errors 0。
- GetSubData(1行 read-only): **2.0 → 1.0 RPC/tx**(skip 100%)= 単一行 read の理論値。
- GetAccessData 2.1→1.5(skip 82%)、GetNewDest 2.0→1.6(skip 42%, join 単一キー時)。
- 書き込み tx は skip 0%(正しく対象外)。md5 は TPC-H が multi-key+ro_novalidate で本 path 非該当=不変。

### single-key RO commit-skip クリーン A/B(TATP T1, 同一バイナリ, 15s, trace 無し)
- gate OFF(`HELIOS_RO_SINGLEKEY_COMMIT=0`): 5369/5385 → ~5377 req/s(メモリ baseline ~5379 と一致)
- gate ON(default): 5986/5935 → **~5960 req/s = +10.8%**。retry 0, errors 0。
- 回帰: TPC-C T1 376.6(退行なし)、TPC-H md5 22/22 OK(本 path 非該当)。

### InsertCallFwd の write-path fallback(特性記録・将来課題)
InsertCallFwd(TATP の ~7%, 995us/tx, 5.5 RPC/tx)は **75% が BEGIN/END:0.75 = prefetch attempt が
cache miss → stateful retry に落ちている**(multi-statement read+INSERT の coverage gap)。write_row 自体は
prefetch でバッファするが、tx 途中の未 stage read(insert 前の存在確認等)で cache miss → fallback。
UpdLocation(read+UPDATE)は begin/end 無しで prefetch 完走(2 RPC)なので差は INSERT 系の read 構造。
**TPC-C/TATP の write 経路は md5 オラクルが無く、修正は silent corruption リスクが高い**ため今回は見送り、
特性のみ記録。将来 lever: insert 前の存在確認 read を plan に stage して fallback を消す(要 write-path 検証基盤)。

## Phase 20 結論(OLTP near-ideal 検証 + 無駄削減)
**prefetch は read 系で実質理論値だと確認**:
- TPC-C DSL(tx-scoped @_tx_plan): NewOrder 2.1 RPC/tx(35文)/ Delivery 2.0 RPC/tx(70文)= near-ideal。
  throughput 401 > InnoDB ~390。helios 寄与 ~115us、残りは MySQL/JDBC 文実行(InnoDB と同じ・削れない)。
- TPC-C autogen(stmt-scoped, JDBC 用に必須): NewOrder 22.8 RPC/tx(文ごと1 plan)。TPC-C は JDBC 律速で
  storage engine は少数派(helios 寄与 ~15%)。文を跨ぐバッチは autogen では原理的に不可。
- TATP: read 系は single-key RO commit-skip で **1 RPC/tx(GetSubData)= 単一行 read の理論値**に到達。
**削った無駄**: read-only single-key tx の OCC validation RPC(SOUND, +10.8% TATP)。
**残る無駄(将来)**: InsertCallFwd 系 write tx の stateful fallback、multi-key read-only の commit-skip
(server 側 snapshot 読みが必要)、per-RPC 固定費 ~24.5us(ほぼ床)。

### helios vs InnoDB TATP T1(near-theoretical 検証, 確定)
| 構成 | req/s | 備考 |
|---|---|---|
| InnoDB durable(default, commit毎 fsync) | 750 | fsync 律速(本番現実の durable 比較) |
| helios in-mem(single-key RO skip 後) | 5960 | **durable InnoDB の 8x** |
| InnoDB 非durable(flush_log=2, doublewrite=0) | 9812 | ローカル・公平な床 |

**結論**: helios TATP read は **1 RPC = disaggregation の理論最小**(リモート行取得に最低 1 RPC は必須)。
非durable InnoDB との ~39% 差(168us vs 102us/tx)は「read ごと 1 RPC」の**不可避な disaggregation 税**で
無駄ではない。durable InnoDB には 8x 勝つ(インメモリ優位)。→ **OLTP read の prefetch は理論値に到達**。
InnoDB 3310(TATP, data_3310)は計測後 down。3308(TPC-H ref)は不変。

### 【訂正】InsertCallFwd「fallback」は load 混入の誤分析 — write 経路に fallback 無し
先の「InsertCallFwd 75% が begin/end で fallback」は **load フェーズの multi-row
`INSERT INTO call_forwarding VALUES(...),(...)` が分類器で InsertCallFwd に混入**したもの。load を除外
(execute のみ)した clean な集計では **全 TATP tx が begin/end 0% = prefetch 完走、fallback 皆無**:
```
type           n      rpc/tx us/tx begin% sk%
GetSubData     11487  1.0    115   0%     100%   ← read 理論床
GetAccessData  11184  1.0    102   0%     100%   ← read 理論床(先の82%も load 混入。clean は100%)
UpdLocation    4606   2.0    225   0%     -      ← read+update 最小(plan+commit)
GetNewDest     3248   1.6    168   0%     42%    ← join(multi-key 部分skip)
InsertCallFwd  648    3.0    362   0%     -      ← read+read+insert(plan×2+commit)最小
DeleteCallFwd  632    2.2    233   0%     -
UpdSubData     631    2.4    214   0%     -
```
→ OLTP write 経路の coverage gap は無く、write tx も prefetch で 2-3 RPC(必要最小)。**TATP は全 tx で
near-ideal**。read は single-key RO skip で 1 RPC(理論床)、write は plan+commit の最小構成。
**Phase 20 の安全な伸びしろは概ね掘り切った**(残るのは multi-key read-only の commit-skip)。
【訂正(Phase18-20 review 両者一致)】「実装不可」は不正確: multi-key read-only **自体は通常 validation で
完全サポート済み・正当**。局所証明できないのは **commit RPC の省略(skip)**だけで、現 stateless plan reads が
一貫 snapshot でないため。server 側に一貫 snapshot read API を足せば multi-key RO の skip も将来可能 →
「RPC 省略が局所証明不可」が正確で「実装不可」ではない。

---

## エントリ: Phase 18-20 deep review(Codex×3 + Claude×3, per-phase)+ 修正(2026-06-14)
user 依頼で直近 Phase を Codex と Claude 双方に厳密 adversarial review させた。Codex 3件は sandbox
(bwrap loopback)でファイル読込不可=inline 推論のみ、Claude 3件は実コード精読(grounded)。

### 確定した実バグと修正 🔴
**existence_only(q22)が default-ON × `ro_novalidate` OFF で確定 abort/livelock**(Phase18 Claude grounded 発見)。
existence_only は probe あたり1行の **incomplete secondary-range** を read-set に積むが、commit-time
`validate_secondary_key_list`(LineairDB commit.cpp:371-435)が全範囲再scan+完全一致を要求するため、
ro_novalidate OFF では確定不一致 → abort → 同一プランで livelock。md5 22/22 が通っていたのは計測 env が
ro_novalidate ON だったため(abort 方向で誤コミットではない)。
**修正(commit 後述)**: existence_only を `allow_filter_pushdown`(== `tx->ro_novalidate()`)でゲート。
filter/projection pushdown が同じ理由(reduced 結果は replay 不能)で既に ro_novalidate-gated なのと同一設計。
**検証**: prefetch ON + ro_novalidate OFF で q22 が **完走(7行・abort 無し)**、ro_novalidate ON で q22
~0.95s(existence_only 継続)+ **md5 22/22 OK**。

### 残す hardening 項目 🟡
- **Codex(Phase18)の residual gap(未検証)**: `qep_leaf_info(REF)=true` は FILTER ノード不在を見るだけで、
  REF に付随する residual(attached condition / ICP)が leaf 外で再評価されるケースを排除しきれない可能性
  (server first row を MySQL が reject → outer 誤残し = silent 誤り)。Claude grounded では TPC-H 実プランの
  residual は必ず FILTER 化(q21 で確認)= qep_leaf_info で除外され、md5 22/22 で実害なしを確認。任意クエリでの
  一般保証は未証明 → **hardening: anti-join inner に executor 側 residual / pushed_idx_cond が無いことの明示
  チェックを追加検討**(現状は ro_novalidate 限定 + FILTER-residual 前提で TPC-H 健全)。
- **q21 SIP**: l_suppkey 二次索引は postload DDL に実在(afterload 有効化で lever 化)。上の q21 訂正参照。
- **multi-key RO commit-skip**: server snapshot read API で将来可能。上の Phase20 訂正参照。

### review が SOUND 確認した主点(対応不要)
- single-key RO commit-skip: guard 述語が読み面を真に1キーに拘束(全 read 経路が base/range read-set に登録、
  すり抜け不可)、LineairDB read-only set は無ロック・無 install で他 tx 非依存。Claude grounded で反例構築不能。
- existence_only core: NestedLoop/BKA ANTI は inner 1行で打ち切り列を読まない(MySQL executor 意味論一致)、
  NOT IN の NULL 反転は ANTI 変換自体が拒否され除外、residual(FILTER)検出は健全。
- prewarm: EXPLAIN が cold path で info() 発火 → stats RPC を測定外へ。caveat=TDC 安定前提(FLUSH/DDL/ANALYZE で seed 失効)。
- value-less existence 不可は正(proto に per-row found 無し)。q22/q21 の floor/lever 結論は「現実装制約下で」の限定付きで妥当。

### hygiene
`.cache/clangd/index/*`(数千ファイル)が過去 `git add -A` で混入していた。`.gitignore` に `.cache/` 追加。

---

## エントリ: Phase 18-20 dual review iteration-2(post-fix 検証 + residual-gap 決着)(2026-06-14)
iteration-1 の fix(existence_only を ro_novalidate ゲート)を Codex + Claude で再レビュー。焦点は
①修正の正しさ・完全性、②iter-1 で Codex が挙げた residual-gap の到達可能性。

### ① 修正判定: 正しい・完全(Claude grounded で実証)
`allow_filter_pushdown == tx->ro_novalidate()` をコールチェーン全域で確認(`lineairdb_prefetch.cc:367`→
`autogen_read_plan_from_qep`→`compile_tree_leaves`、再代入なし)。ro_novalidate OFF で no-residual NOT EXISTS
が完走(abort 解消)、ro_novalidate ON で q22 マーク継続を実証。ゲート配置の副作用なし。

### ② residual-gap 判定: NOT-REACHABLE(構造証明で決着、Codex の REACHABLE は false positive)
Codex(iter-2, file-blind)は「相関残差 `i.b>o.x` が **ICP(pushed_idx_cond)に畳まれ bare REF のまま** →
qep_leaf_info=true → マーク → first-match 誤り」と **REACHABLE 判定**。しかし **data 非依存の構造証明**で否定:
- **helios の `index_flags` は `HA_READ_RANGE|HA_READ_NEXT|HA_READ_ORDER|HA_READ_PREV` のみで
  `HA_DO_INDEX_COND_PUSHDOWN` を広告しない**(`ha_lineairdb.hh:250-254`、proxy 全体に皆無)。MySQL は
  この flag を広告する index にしか ICP を使わない → **helios 表には ICP が一切適用されない** → Codex の
  ICP 畳み込みは helios では発生し得ない。
- **EXPLAIN FORMAT=TREE(data 非依存)** で Codex の厳密ケース(idx_ab(a,b), `i.b>o.x`)を実測 → inner は
  `-> Filter:(i.b > o.x)` → `-> Index lookup on i using idx_ab(a=o.a)` = **残差は独立 FILTER ノード**
  (REF に畳まれず)→ qep_leaf_info=false → existence_only 非マーク。
- Claude のソース根拠: REF/EQ_REF/MRR の AccessPath struct は条件フィールドを持たない(`access_path.h:870-876`)。
→ Codex は generic MySQL/InnoDB(ICP 対応)から推論した false positive。**helios は ICP 非対応なので residual は
常に FILTER 化 → existence_only は構造的に安全。hardening 不要。**

### 副次発見(tangential・pre-existing・review 対象外)
- 経験的 row 比較テストは **helios の ad-hoc multi-row INSERT + secondary index が行を取りこぼす**既存制約で
  汚染された(`INSERT ... VALUES(4 rows)` で 1 行しか入らず、index lookup も誤行を返した)。benchbase の bulk
  load 経路は正常(md5 22/22 が証拠)。ad-hoc DML 経路は別問題で本 review の射程外。
- **DROP TABLE が in-mem server の KV 行を purge しない**(同名 CREATE で旧行復活)。テスト hygiene 上の注意。
  ベンチは server 再起動 + fresh load なので影響なし。
→ いずれも residual-gap の結論(構造証明で NOT-REACHABLE)に影響しない。

### iteration-2 総括
**修正は正しく完全、residual gap は構造的に到達不能、追加修正不要。** Phase 18-20 の正しさは2イテレーションの
dual review を経て確定(single-key RO=SOUND、existence_only core=SOUND+ro_novalidate ゲート済、
prewarm/gate/結論=SOUND/訂正済)。最大の収穫は iter-1 Claude が見つけた ro_novalidate-abort 実バグの修正。

---

## エントリ: single-key RO commit-skip を revert(Serializable 方針, 2026-06-14)
user 方針確定: **Helios は Serializable を提供。validation-skip は「ワークロード全体が read-only」の例外
(ro_novalidate, 例: TPC-H)のみ許容。混合では Silo validation を必ず走らせる**(証明済みでも省かない=均一保証)。
→ Phase20 の single-key RO commit-skip(commit f56397b)を **revert**(`prefetch_validate_and_commit` の
分岐削除)。ro_novalidate に gate しても既存 ro_novalidate fast path と重複=死にコードのため完全削除。
混合 OLTP の単一キー read-only tx も通常の `tx_validate_and_commit` を通る。
- 効果: TATP T1 が +10.8%(5960)→ baseline(~5315, validation ON)に戻る。retry 0。md5 22/22 OK(TPC-H は
  ro_novalidate path で不変)。
- 記録: [[helios-serializable-no-si]]。**multi-key RO commit-skip(server snapshot=SI)も同方針で却下**(roadmap から除外)。
- 含意: OLTP read の「理論最小 1 RPC」は read-only 専用文脈(ro_novalidate)でのみ成立し、混合では
  read plan + validation の 2 RPC が正しい下限(Serializable の代償)。

---

## エントリ: ④ q21 SIP — focused 実測(branch claude/q21-secondary-index)(2026-06-14)
l_sk index を load 時実体化(lineitem CREATE TABLE に KEY l_sk/l_sk_pk 追加=benchbase jar DDL 更新)して q21 評価。
背景: helios の CREATE INDEX(DDL)は backfill 未実装(inplace_alter_table が metadata 登録のみ)→ load 時構築で実体化。

### 結果(SF=1 warm)
- q21: helios 13.24→3.85s(3.4x)・md5 一致 ✅。supplier 駆動(s_nationkey 400 → l1 using l_sk → orders PK)。lever 実在。
- 🔴 helios q15 が md5 不一致(誤): no-index 正(238879…/0.93s)、l_sk 有り誤(281ce0…/0.024s)。revenue0 内側集約単体は
  index 有でも正 → 誤るのは VIEW+外側+prefetch+l_sk の組合せ = prefetch×SI×view correctness バグ。
- ⚠️ InnoDB q15 3.52→21.88s(optimizer が l_sk_pk 誤用)。suite: helios 30.04 / InnoDB 55.87(歪み)。md5 ERR=1(q15)。
- load 31→105s(6M×2 index)。

### 結論
q21 の勝ちは本物だが l_sk を素で入れると q15 を壊す(helios)+ InnoDB q15 退行。full 実装は backfill だけでなく
prefetch×SI×view correctness(q15)修正を含む大きめの作業。user 判断待ち: (a)full投資 (b)index撤退しq21は①②へ (c)絞り込み継続。
branch throwaway 前提。現状 helios=l_sk 込みロード, InnoDB3308=l_sk/l_sk_pk 追加済。

---

# Phase 21: secondary-index ライフサイクルの foundational 実装(branch claude/q21-secondary-index)

目的: helios を **BenchBase 標準(InnoDB+MySQL 等価=afterload で標準索引一式)** に対応させる。q21 SIP の勝ち
(3.4x)はこの副産物。correctness 最優先。

## 順序(user 承認)
② CREATE INDEX backfill → afterload 有効化 + md5 → ④ prefetch×SI×view correctness(q15 級)→ ③ DROP purge。

## プロセス(user 指示)
- 各ステップ実装前に **既存システム(InnoDB / PostgreSQL / OSS / in-memory OCC 系)を Claude・Codex 並列調査**して
  設計の絵を描く。
- 各ステップを **Claude・Codex で厳密 dual review**、GO が出るまで反復してから次へ。

## Step ② 設計調査(進行中)
Claude(general-purpose, web+code)+ Codex(knowledge)を並列起動。論点: server 側 backfill vs proxy 駆動 /
exclusive-lock offline build(afterload は concurrent DML 無し)vs online build / ready-gating(完成まで index 不可視)/
Silo single-version OCC 下の一貫性 / RPC flow / correctness 不変条件(全 committed 行が1回・phantom/dup 無し・
secondary key 符号化が DML 経路と一致)/ test(CREATE INDEX 後の md5 vs InnoDB、count by col、load-built index 等価)。
→ 両調査を synthesis → 設計 dual review(GO まで)→ 実装 → 実装 dual review(GO まで)。

## Step ② 設計(GO 済み, docs/phase21_create_index_backfill_design.md)
A2 handler-driven backfill: `inplace_alter_table` が表を scan し各行 `build_secondary_key_from_row` +
secondary-index write を**再利用**(符号化 drift ゼロ)。exclusive-lock offline build(afterload は並行 DML 無し)。
FIX-1: KEY は `key_info_buffer`(0-based fieldnr)でなく **`altered_table->key_info` を index 名マッチ**で取得
(encoder の 1-based 規約と整合、silent index 破壊回避)。設計 v1→v2→v2.1 で Codex+Claude dual-review GO。

## Step ② 実装 first-cut(commit a5f0747): correct だが O(N²)
SF0.1 で q21 md5 = ground-truth `d36a1caf...` 完全一致(FIX-1 含め正しい index 生成を実証)。**ただし CREATE INDEX
14m57s/SF0.1**。真因 = 全 ~1.2M SI entry を**単一 OCC commit**で install、server `WriteSecondaryIndex`
(transaction_impl.cpp:359-386)が書込ごとに read_set_/write_set_ を線形走査 = O(N²)。

## Step ② chunk-COMMIT + read-chunking(本実装)
- **chunk-COMMIT**: SI 書込を ALTER の statement tx でなく **独立 tx 群**で chunk(2000 行)ごとに
  begin→buffer→end(commit)。各 tx の read_set_/write_set_ が chunk 内に限定 → 書込ごと走査 O(chunk)、
  全体 O(rows×chunk)。helper `ha_lineairdb::backfill_commit_chunk`。FENCE(=false)で per-chunk fence なし。
- **read-chunking**: 全行一括 fetch をやめ bounded PK-range scan(`get_matching_keys_and_values_in_range(cursor,
  "",50000)`、cursor = last+`'\0'` で厳密後続)→ SF≥1 で全行を一括 materialize しない。set_fields_from_lineairdb で
  行復元、PK は stored key kv.first を直接使用。
- **correctness**: 逐次・独立 tx で load-built と等価。非 UNIQUE は同一 SI キー跨りが merge-commute(commit 時 delta
  set-add)、UNIQUE 跨り重複は persistent leaf の IsInitialized() で abort。md5 で実証(下記)。
- **perf(SF0.1)**: 14m57s → **l_sk 10.8s + l_sk_pk(複合)9.5s ≈ 20s**。複合は chunk 縮小で 22.9→9.5s。
  q21 md5(backfilled)= `d36a1caf...` = ground-truth(回帰なし)。
- **perf(SF1, 6M 行)**: l_sk **3m31s** / l_sk_pk(複合)**1m40s** / 計 ~5min。q21 md5(backfilled)=
  ground-truth `49e5b76c...`(**SF1 でも回帰なし**)。O(N²) は解消(さもなくば数時間)。残差 = O(N×chunk) の write 走査
  + **非 UNIQUE の PK-list cross-chunk 再 seed コスト**(suppkey ごと list が成長し chunk 跨ぎで ReadUnvalidated +
  list コピー = ~O(list²))。後者ゆえ SF1 では low-distinct の l_sk(list 成長大)が複合 l_sk_pk より遅いという反転。
  (read-chunking 撤去前は l_sk 4m31s/l_sk_pk 2m40s/計 ~7min → 撤去で ~5min。)
- **dev は SF0.1(~20s)で回す方針**ゆえ backfill perf は dev には十分。SF1 は milestone のみ(one-time afterload、
  数分=許容)。さらなる高速化は LineairDB core(WriteSecondaryIndex を hash-set lookup 化 + 非 UNIQUE PK-list を
  append-only 化)= 将来の別 feature-branch 最適化。
- **知見**: range/prefix scan 応答は flat binary codec で 2GB protobuf 制限外(read の 2GB 懸念は非該当、read-chunk の
  効用は transient メモリ抑制)。chunk size 最適は index cardinality 依存(low-distinct は大・high-distinct は小)。
  さらなる高速化が要れば LineairDB core の WriteSecondaryIndex を hash-set lookup 化が本筋(feature branch)。
- **実装 dual-review = GO**: Claude(grounded, 実ファイル 59 tool-use 精読)= GO・correctness bug なし(C1 を
  AddSecondaryIndexValue の sorted PK-list 冪等 dedup で補強、md5 が捕まえない UNIQUE/NULL/複合列順/空表/境界跨り
  重複/中断も safe と実証)。Codex = CHANGES だが指摘の C1 UNIQUE+NULL / C4 失敗時 partial は chunk-COMMIT の回帰でなく
  既存性質/既知 hazard(measured 非発火、Step ③ で対応)、C2/C5 は grounded が非 bug 裁定。grounded の MED(read-chunking
  が server memory を bound せず O(N²/read_chunk) を足す)を受け **read-chunking を撤去・単一スキャンに**。
  詳細 docs/phase21_create_index_backfill_design.md。**Step ② = GO**(SF0.1/SF1 とも md5 = ground-truth)。

## afterload md5 検証(標準23索引一式, SF0.1, user 承認 A+B)
標準 SI 一式 = benchbase postload-mysql.sql の **23 索引**(UNIQUE 8 = 全て NOT NULL PK 列上、非UNIQUE 15)。
helios で全23を chunk-COMMIT backfill 構築(SF0.1, 計 1m22s)。

**Option A(helios 自己整合)**: 同一データで baseline(索引なし)vs full-index の全22 md5。
- **19/22 は prefetch ON でも完全一致**(= 索引が結果を変えない=正しい)。
- 差分は q1・q6(prefetch ON で Deadlock=prefetch abort)+ q15(unsupported QEP/view)。
- **prefetch OFF で q1/q6/q15 を再実行 → 3つとも baseline と完全一致**(q1=2903e9.. q6=17ded5.. q15=7fcde3..)。
  → **index 破損でなく純粋に prefetch×secondary-index 経路のバグ**(optimizer が SI plan を選ぶが prefetch 実行が SI
  range/view scan を stage 不可で abort)。**= Step ④ のスコープ確定(q1, q6, q15)**。
- 結論: **backfill は標準23索引を正しく構築**(prefetch OFF=22/22 相当・全クエリ正、prefetch ON=19/22 で残3は Step ④)。
- 検証手法の注記: helios は **FORCE INDEX を honor せず full table scan に倒す**(EXPLAIN type=ALL)ため Codex 提案の
  per-index forced (key,pk) scan は空振り。代わりに prefetch-OFF 全クエリ突合(optimizer が自然に使う索引を広く検証)+
  機構論証(build_secondary_key_from_row = write_row と byte 一致・全行・dual-review GO)で担保。query が使わない
  redundant UNIQUE(r_rk/p_pk/o_ok/ps_pk_sk 等、PK 列上)の完全性は機構論証に依存(残リスク低)。
- **発見**: helios の FK auto-index(l_partkey 等)は postload の同列 explicit index 作成で整理され、lineitem は postload
  の 8 本(l_ok/l_pk/l_sk/l_sd/l_cd/l_rd/l_pk_sk/l_sk_pk)+ PRIMARY に収束(非PRIMARY 計 23 = postload 全数)。

**Option B(helios vs InnoDB, 両系 SF0.1 + 全23索引)**: tpch_md5 helios(prefetch ON) vs InnoDB(native)。
- **OK=19, MISMATCH=0, ERR=3**(q1/q6/q15)。**誤結果ゼロ** — ERR は helios 側 prefetch×SI abort のみ
  (q1/q6=Deadlock 40001, q15=unsupported QEP 1235)。q21=OK(helios=InnoDB, supplier 駆動 l_sk)。
- → A と完全整合: backfill 索引は正しく InnoDB と一致、結果を変える failure は全て prefetch×SI(Step ④)。
- InnoDB(3308)は検証後 **SF1 + l_sk/l_sk_pk の標準 fixture に復元**(disk 永続)。

## afterload md5 ステップ 結論
- **backfill は標準23索引一式を正しく構築**(helios 自己整合 prefetch OFF=22/22 + helios vs InnoDB=19/22 誤り0)。
- **afterLoad config 恒久 wiring は Step ④ 完了まで保留**(今 wire すると通常 suite が prefetch ON で q1/q6/q15 を
  prefetch×SI で壊す + 全23索引 backfill で load 重化)。手動 postload で検証済み、config 化は ④ 後。
- **Step ④ スコープ確定: prefetch×secondary-index×view correctness = q1, q6, q15**(prefetch ON で SI range/view scan を
  prefetch 実行が stage 不可 → abort/unsupported。prefetch OFF normal 経路は正)。次ステップへ。

## Step ④ 根因 + 設計(grounded 調査 + Codex review + user 承認 = F')
**根因(grounded 81 tool-use 精読、初期仮説は誤りと訂正)**: SI staging 自体は実装済(compile_index_range_scan,
autogen:395-465 が secondary range を index_name 付きで stage、serve も lookup_secondary_scan_cache:1531 で存在)。
真因は **agg-pushdown override と optimizer leaf 選択の食い違い**:
- agg-pushdown override(JOIN::override_executor_func, ha:1742)は**常に primary 全スキャン**
  (`get_matching_keys_and_values_from_prefix("")` → primary range_scan_cache_)で読み server 側 filter+集約。
- SI(l_sd)在りだと optimizer は **secondary INDEX_RANGE_SCAN** を選び autogen は `index_name="l_sd"` の secondary step を
  stage。agg-stamp ループ(transaction.cc:310)は **primary scan step のみマッチ**(`index_name.empty()`)→ aggregate step
  生成されず primary range も staged されない → override の primary 全スキャンが miss → abort。
- q1/q6 は override が `my_error(ER_LOCK_DEADLOCK)` 直叩き(ha:1039)→ **1213 Deadlock**。q15 は derived-table 内
  materialize で同 divergence → abort_errno → **1235 unsupported**。EXPLAIN ANALYZE は agg-pushdown hijack 除外
  (ha:483)ゆえ working secondary 経路で成功 = 索引内容は正しい証左。
**設計 = F'(高速化維持, user 承認)**: agg-pushdown が有効な時、autogen staging を **optimizer の SI leaf でなく
primary 全スキャンに固定**(override の実動作に一致)。SI 無しの時と同じ proven 経路 = correct かつ agg-pushdown 勝ち
(q1 13→5s, q6 152x)維持。**dual-review**: Codex=SOUND(前提=WHERE 完全 push、既存 agg-pushdown が保証)、grounded の
事実も F' を支持。ガードレール: (1)agg-pushdown 実有効時のみ適用 (2)aggregate を当該 primary step に確実 stamp
(3)q15 derived inner unit も同様 (4)mismatch の runtime assert/log。Option F(override 無効化=最小だが q1/q6 perf 退行)・
S(secondary 上で集約=blast radius 大)は不採用。→ 実装へ。

## Step ④ 実装 — F'(1): main agg-pushdown 経路(q1/q6 修正済)
`execute_read_plan`(transaction.cc:308 直前)で、agg-pushdown 有効時(pushed_aggregate_ 非空)に db_table_key の
**secondary scan step を primary 全スキャンに rewrite**(index_name/key_prefix クリア, end=sentinel, limit/reverse リセット)。
直後の既存 stamp ループが primary step にマッチ → aggregate + WHERE filter(pushed_filter_)を stamp。override は元々
`agg_next_raw`=primary 全スキャンを consume するので staged と一致。stamped==0 を trace に記録(guardrail)。
**検証(SF0.1, prefetch ON, 全23索引)**: q1=2903e9.. / q6=17ded5.. = baseline 完全一致(従来 Deadlock)。**diff が
{q1,q6,q15} → {q15} に縮小 = 21/22**。range bound を落としても pushed_filter_(exact WHERE)が制限するので正(tx_set_pushed_aggregate
が exact serialization を要求、さもなくば Phase A fallback)。

## Step ④ q15 — 真因は inner-agg でなく「MATERIALIZE 内 SI scan」の coverage バグ(別系統)
当初 q15 を inner-aggregate 経路と仮定し F' を試作したが **revert**: trace で q15 の view(GROUP BY l_suppkey)は
**"not a scalar unit"** で inner-aggregate に登録されない(Phase16 は scalar uncorrelated subquery 限定)→ inner-agg F' は
無効。真因は **secondary-range scan の stage/consume coverage miss**(`Prefetch cache miss: secondary scan
table=lineitem`、transaction.cc:929)。切り分け(SF0.1, prefetch ON, 全索引):
- (a) view を **1回だけ** materialize(`SELECT * FROM revenue0`)→ **失敗**(cache miss)。∴ view 二重 materialize は無関係。
- (b) 直接の l_sd range scan(view/agg 無し `SELECT ... WHERE l_shipdate range`)→ **成功**。∴ 基本 SI range prefetch は OK。
- (c) grouped-agg over l_sd range を **standalone**(materialize せず)→ **成功**(F' が効く)。
→ 真因は **MATERIALIZE→AGGREGATE subtree 内の SI(l_sd)range scan**。collect_qep_leaves は MATERIALIZE に降り
(autogen:62-70)leaf を collect・compile_index_range_scan で staged するが、materialization 実行時の consume が staged
secondary range を miss する(bounds/coverage mismatch、materialize-aggregate 文脈固有)。**q1/q6(agg-pushdown)とは
別系統の deeper bug**。focused な「staged 範囲 vs materialize consume の secondary-range bounds」debug が要る。

## Step ④ q15 真因確定 + 修正(GS-skip guard)
focused trace(HELIOS_Q15_DEBUG 計装→除去済)で確定: q15 の view body は **GroupedSummary(GS, Phase16)に登録**され、
autogen 後段の **GS-skip pass(autogen:1291)が GS 登録表の scan step を drop**(handler が gs_fill_buffers で synthetic
group rows を供給する前提)。だが GS 供給は **full-scan 経路のみ**(rnd_init→gs_fill_buffers / index_read_map は key==nullptr
時のみ)。l_sd 索引があると optimizer は **keyed secondary index_read** を使い GS 経路を通らない → drop された step を
raw SI scan で consume し miss(`secondary_scan_cache_ 0 entries`)。トレース順: main compile が l_sd を PUSH(steps=1)→
GS-skip が drop(steps=0)→ consume miss。
**修正**: GS-skip drop 条件に **`index_name.empty()` を追加**(autogen:1311)= **primary full-scan step のみ GS-skip**、
secondary scan は staged 維持 → keyed index_read が hit、MySQL がローカル集約(GS 最適化なしだが correct)。q18 等の GS は
primary scan(index_name 空)なので不変。q1/q6 の F' と同クラス(prefetch 最適化が full-scan consume を仮定、SI で
index_read になり破綻)。

## Step ④ 完了(correctness)
- **TPC-H 全 suite が prefetch ON + 標準23索引で 22/22 一致**(helios 自己整合 baseline vs full-index, ALL 22 IDENTICAL)。
  q1/q6=F'(commit 4be3801)、q15=GS-skip guard。誤結果ゼロ。
- **⚠ 性能(latency)は未計測**: ここまで correctness(md5)のみ。Goal の「suite 再計測(q21 改善・q1/q6 非退行)」が未達。
  → SF1 + cstate guard + 標準索引で suite 性能を実測する(次)。
