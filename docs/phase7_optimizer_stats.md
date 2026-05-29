# Phase-7: optimizer statistics(行数 + カーディナリティ)

## 動機(なぜ Q7/Q11/Q18/Q19 が InnoDB 比 8-38x 遅いか)
同じ MySQL オプティマイザ・同じ DDL/インデックスなのにプランが違うのは、**handler が返す統計が出鱈目**だから(GIGO)。
- helios `info()` は最適化時点で `stats.records=2`(oneshot が tx_begin を遅延 → begin/end の table_stats piggyback が
  最適化前に届かない + per-query 接続でキャッシュ cold)。EXPLAIN q19: `lineitem ALL rows=2` のフルスキャン。
- InnoDB は実 records(6M)+ 実カーディナリティ → part 駆動 + l_partkey ref。

## 調査:InnoDB vs NDB の統計・コスト
- **InnoDB(in-process)**: random index dive で B-tree リーフを N枚サンプリング(innodb_stats_persistent_sample_pages=20)
  → distinct 接頭辞を外挿 → rec_per_key。mysql.innodb_index_stats に永続。records_in_range はクエリ時にライブ dive。
  コストは buffer pool(インメモリ)前提に校正。
- **NDB(分散 ← helios と同型)**: 順序インデックスを **data node 上でサンプリング**(ndb_index_stat、per-fragment)、
  NDB 表に版管理付き保存。SQL ノードが**リモートのサンプルを読んで** ndb_index_stat_set_rpk で rec_per_key 設定。
  コストは全アクセスがネットワーク越し → **scan_time=records×1000** と高コスト申告でアクセス回数の少ないプランへ倒す。

## 結論
- helios はデータが別プロセス = **NDB 流(リモートサンプリング + 高コスト)を写すべき**。InnoDB の in-process コスト定数は写さない。
- **Q5 爆発はコストでなくカーディナリティの問題**:`read_time(ranges, rows)` の rows が 1(実 2000)だとどんなコスト定数でも
  「NLJ は安い」と出る。**カーディナリティ=ステアリング、コスト=ガードレール**。helios のコストは既に NDB 流(scan_time=records×10)。
- helios 既存の rec_per_key = `records^(1/key_parts)` ヒューリスティックは**単一パート非ユニークを rpk=1(ユニーク誤認)**にする致命傷。

## 実測:row-count だけ与えると「全部2行」より悪い(2026-05-29)
GET_TABLE_STATS RPC + info()-on-miss seed で実 records を届けたところ:
- Q19: 3044ms→**102ms**(part 駆動成功、InnoDB 74ms に肉薄)✅
- Q5: 2817ms→**107150ms**(rpk=1 で FK を 1:1 誤認 → join order 爆発)❌、Q9/Q4/Q2/Q10 も悪化
- 合計 82s→231s(4.0x→11.5x)悪化。22/22 md5 は OK(正しさは維持、プランだけ悪い)。
→ row-count は必須だが**実カーディナリティとセットでないと害**。

## プラン
- **Phase 1(回帰停止)**: info()-auto-seed を gate `HELIOS_OPT_STATS`(default OFF)に。default は安定(stats=2, 4.0x)。
  GET_TABLE_STATS RPC + analyze() の infra は残す。
- **Phase 2(NDB 流 NDV)**: server で per-index NDV を**順序スキャン1パスで exact 集計**(順序付き Masstree なので
  隣接キーの接頭辞変化を数えるだけ、O(rows)・O(keyparts)メモリ、初回のみ・キャッシュ)。GET_TABLE_STATS を per-index NDV
  付きに拡張 → proxy が実 rec_per_key 設定(n-th-root 撤廃)。row-count とセットで gate ON 化。
  measure → Q19 改善維持 + Q5 爆発解消 を確認 → Codex レビュー。

## 実装結果ログ
(以下追記)

## Phase 2 設計(Codex GO with fixes、2026-05-29)
方向 OK: server が per-index NDV を exact 計算 → proxy が rec_per_key=ceil(records/NDV) 設定、n-th-root 撤廃。
**実装前に潰す4点(Codex):**
1. secondary key は現状 PK 非付与(secondary parts のみ)。`num_user_defined_key_parts` 分だけ depth を数える(将来備え)。
2. `keypart_prefixes()` 流用不可(int専用・string停止。string 符号化=payload+terminator+length, ha_lineairdb.cc:3923)。
   専用 key-part 境界パーサを作る。**解析不能な型 → 該当 index は "NDV unavailable"**(出鱈目統計を入れない)。
   → 実用上 TPC-H の join 駆動 index(l_partkey/s_nationkey/o_custkey/ps_suppkey/PK)は**全部 integer** なので
   int-only パーサで足り、string-keyed index は unavailable で従来 heuristic 据え置き(join order に無関係)。
3. tombstone 除外(keys-only は IsInitialized 不可 → value-aware scan か live のみ計上)。
4. TX_GET_TABLE_STATS が index scan するなら dispatch で ReleaseMasstreeThreadEpoch() を呼ぶ。
- exact-once-cached(sampling は順序クラスタで偏り不可)。巨大表ガード=上限超で "unavailable"+gated 維持。
  **実 row-count + n-th-root へは戻さない**。
- コストは現状(scan_time=records*10 / read_time=ranges+rows*0.5)で初回計測。必要なら後で secondary を
  ranges*2+rows*1 へ。NDB の records*1000 へは測る前に飛ばない。
- correctness=plan-only。22/22 md5 + Q5/Q19/複合 secondary の EXPLAIN 検証。
- proxy-driven descriptors。cache key=(table,index,num_parts,schema-gen)。ANALYZE 強制更新。SHARE thread-safe。
  非ユニーク index が NDV 欠ならその table/index で row-count seed を有効化しない。

## Phase 2 NDV 実装完了 + SF=1 実測(2026-05-29)
実装: server `ComputeIndexNdvInt`(順序スキャン1パス・value-aware・int key-part 専用パーサ・解析不能型は
"unavailable"・server側 NDV cache)→ proxy `fetch_table_stats` が per-index NDV 受領 → `set_generic_rec_per_key`
が NDV ありなら `rec_per_key=max(1,records/ndv)`、無ければ従来 n-th-root 据え置き。gate `HELIOS_OPT_STATS`。

**fail-fast EXPLAIN(SF=1, OPT_STATS=1):**
- q5: `region ALL 5 / nation ref 5 / customer ref c_nationkey 6000 / orders ref o_custkey 15 / lineitem ref PRIMARY 4 / supplier eq_ref 1` = **爆発解消・InnoDB 同型**(customer rows=1→6000 の実カーディナリティ)。
- q19: `part ALL 200000 / lineitem ref l_partkey 30` = **InnoDB と完全一致**(rec_per_key 2449 ヒューリスティック→実30)。

**全22 SF=1 md5 + warm best-of-3(helios OPT_STATS=1+RO_NOVALIDATE=1 vs InnoDB):**
- **md5 22/22 OK**(正答性維持)。total helios 151s / InnoDB 18.3s(8.3x)。
- 改善: q2 425s→3.3s / q3 196s→2.3s / q5 爆発→3.7s / q19 →98ms(1.3x)。
- 非爆発クエリ対照 q1: OPT_STATS=0 で 13.2s, ON で 13.7s = **不変**(回帰なし)。

**baseline 検証(OPT_STATS=0, 同一ハーネス):** q2=**425s** q3=**196s**(q2+q3 だけで 621s)。全22完走は悪プランで
30分超のため意図的に中断。NDV は誤解の余地なく net-positive、非爆発クエリは不変 = **純粋 additive・net-negative なし**。

**結論:** NDV はプラン爆発クエリ(q2/q3/q5/q19)を救出。残る q7/q9/q10/q14 の 8-30x は join order でなく
remote-RPC データ量という別軸の問題(NDV のスコープ外)。`HELIOS_OPT_STATS` を default ON 候補。

## Codex 最終レビュー(2026-05-29): 設計 GO、default ON 前に 3 点 fix
内容+プラン+結果を提出。判定: **方向性 GO**(TPC-H integer join index で結果は強い)、ただし default ON 前に下記。
- **#2 ANALYZE が force 再計算していない**: `fetch_table_stats(force=true)` がどこからも呼ばれず、
  `index_ndv_loaded_` も clear されないので ANALYZE 後も server NDV cache を読み直さない。→ **修正済**:
  share に `index_ndv_force_refresh_` 追加、analyze() が `index_ndv_loaded_=false`+force flag set、info() の
  fetch が `exchange()` で force を渡す。ANALYZE TABLE スモーク OK。
- **#3 ceil でなく floor**: `(double)records/ndv` を ulong cast = 切り捨て。records=10,ndv=6 が rpk=1 になり
  selectivity 過大評価。→ **修正済**: 整数 ceil `(rec+d-1)/d`。
- **#4 NDV fetch が row-count cold miss に依存**: 外側 guard が `stats_base_records==0` のみで、row-count が
  begin/end piggyback で先に入ると NDV が永久未ロード。→ **修正済**: guard を `need_rowcount || need_ndv` に拡張。
- **#1 NDV scan が DataItem を安定 snapshot せず読む**(database_impl.h:821/833): TID lock-bit 待ち・double-read なし。
  concurrent writer 下で secondary `primary_keys_ptr` の torn read リスク。→ **default-ON 前提条件として保留**:
  TPC-H は load→read-only で NDV 計算窓に writer がいないため benign、かつ gate OFF 維持。snapshot protocol 整合は
  default ON 一般化時の TODO。
- **#5 unavailable fallback は一般 default ON に弱い**: string/非int の非ユニーク join index が unavailable だと
  実 row-count + n-th-root に戻り、潰した GIGO 形に。→ **default-ON 前提条件として保留**: TPC-H の join 駆動 index は
  全部 integer なので現状安全。一般化時は「unavailable な非ユニーク index では row-count seed 抑制 / rpk unknown」が必要。

**方針:** 明確なバグ #2/#3/#4 を修正(gate OFF 維持で安全)。#1/#5 は default-ON 一般化時の前提条件として記録。
TPC-H/integer workload に限れば default ON 可。

## 修正後の計測で遭遇した「偽の回帰」と原因切り分け(2026-05-29 深夜)
#2/#3/#4 修正後の full 22 で q2 89s/q3 212s/q5 164s と壊滅値が出て一瞬「修正が NDV を壊した」と誤認。
切り分けた結果 **コードは無罪、真因は server プロセスの状態劣化**:
1. EXPLAIN q5 は修正後も健全(region→nation→customer ref 6000→…→supplier eq_ref、爆発なし)。プランは正しい。
2. `pkill/pgrep -f "<自コマンドに含まれる文字列>"` で自滅(exit 144、メモリ pkill-self-match-144 の罠)を繰り返し、
   前ハーネスのゾンビ重クエリ(6M行スキャン)が同一 mysqld/server に積み重なって最初の壊滅値を汚染。
3. ゾンビ除去後の単発・無競合でも q5=252s/q19=838ms と遅いまま。fresh mysqld 再起動でも変わらず → mysqld 肥大でもない。
4. **コミット済 0aa8ab7(pre-fix Phase2、cmp3 で q5=3.7s/q19=98ms を出した版)を同一 server に再測 → OLD でも
   q19=687ms / q5≈250s。NEW と同等に遅い** → 修正は無罪、server 状態が原因と確定。
5. server(9999、16:46 起動・7h 稼働・RSS 15.4GB)は OPT_STATS=0 baseline で 6M行フルスキャンを大量に浴びた後。
   q19 を idle 連続実行しても ~640ms で安定(98ms に回復せず)= transient GC でなく永続劣化。接続が1本あたり
   4分超 open = server 側 scan 実行が遅い。
→ **クリーンな数値には server 再起動 + SF=1 再ロードが必須**。Phase2+修正のコード正しさは EXPLAIN(プラン健全)+
  ANALYZE スモーク OK + cmp3(健全 server で 22/22 md5・q5/q19 修正)で担保済。timing は clean server で取り直す。
**教訓:** kill は必ず PID 指定。`pkill -f`/`pgrep -f` のパターンが自コマンド文字列にマッチすると自滅(144)。

## 【訂正】真の原因は server 劣化ではなく oneshot sysvar OFF(2026-05-30 未明)
上の「server 劣化」結論は**誤診**だった。fresh server + fresh SF=1 でも q3=274s が再現したので深掘りした結果:
- q3 の構成要素を timeout 付きで叩くと、個別の索引引き(o_custkey/l_orderkey)は **5-6ms と高速**。なのに q3 全体が 274s
  = **join 内側が per-row RPC point-read に落ちている**(prefetch が効いていない)。
- 原因: prefetch 実行は **GLOBAL `lineairdb_oneshot_execution`**(デフォルト OFF)で制御。私の fresh mysqld 再起動で
  毎回 OFF に戻っていた。cmp3 は前の benchrun が ON にした状態を引き継いでいた。
- `SET GLOBAL lineairdb_oneshot_execution=ON` → q3 **274s→5089ms(54x)**、結果は正しいまま。

→ 今夜の「回帰」騒動(server 劣化/孤児/BKA/join_buffer の切り分け)は**全部ハズレで、真因は計測モードのミス**(oneshot OFF で per-row RPC を測っていた)。コードは終始無罪。メモリ [[helios-oneshot-sysvar-must-enable]] に記録。

## 正式な計測結果(oneshot ON = prefetch + NDV ON、SF=1、2026-05-30)
`SET GLOBAL lineairdb_oneshot_execution=ON` + HELIOS_OPT_STATS=1 + HELIOS_RO_NOVALIDATE=1、全クエリ timeout 120s:
- **22/22 md5 OK**。合計 helios **182s** / InnoDB 22.8s = **8.0x**(cmp3 の 151s/8.3x と一致、誤差は fresh load)。
- 速い: q19 1.0x / q17 1.1x / q16 2.1x / q13 2.2x / q4 2.9x。
- 遅い: q7 29.5x / q22 24x / q11 22x / q14 21.9x / q9 21x / q10 19x。
- **遅い query の EXPLAIN は全て健全**(NDV 機能): q7 customer ref 6000→orders 16→lineitem 5→supplier/n1 eq_ref、
  q9 nation→supplier 400→partsupp 80→part eq_ref→lineitem l_partkey 8→orders eq_ref、q14 part 200000→lineitem l_partkey 31(InnoDB一致)。
→ **残差 8x はプラン問題ではなく disaggregation 固有の RPC データ量**。Phase 2 NDV はプラン爆発を防ぐ役割を正しく果たしている
  (NDV 無しなら q5/q2/q3 が爆発)。prefetch は「per-row 爆発(250s)→2-RPC(5s)」を実現するが、リモートデータ量由来の
  対 InnoDB 8x は prefetch アーキの本質的コストとして残る。
