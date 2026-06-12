# Phase 16 worklog — autogen 真因解明 (Track A) + TPC-H 遅query外科手術 (Track B)

> **ゴール (user, 2026-06-12)**: Track A = TPC-C autogen が stateful より遅い (125 < 140 req/s) 真因の
> 区間別計測による解明と修正 (目標 >140、explicit 191 に接近)。Track B = TPC-H を遅い順に外科手術
> (NDV 統計 + COST_V2 / projection pushdown / partial agg pushdown / LineairDB メタデータ削減、
> 目標: 対 InnoDB 中央値 5x 以内 + ピーク RSS 半減)。
> 前フェーズ: docs/phase15_prefetch_maxopt_worklog.md (全28エントリ)。
> ルール: 議事録必須 / push・hard reset 禁止 / build_partial.sh / 検証ガントレット
> (TPC-H matrix→md5 22/22, OLTP 3+2モード回帰+C1-C4) / server メモリ過剰使用禁止。

## 開始時点の状態 (2026-06-12)

- ブランチ: claude/prefetch-maxopt (cdc7d16)、working tree clean、push なし。
  submodule: LineairDB helios/prefetch-maxopt (3f5a6f7) / benchbase helios/prefetch-maxopt (6f3f578e)。
- 稼働中: lineairdb-server + mysqld 3307 (TPC-H SF=1 ロード済, prefetch ON + ro_novalidate ON)
  + InnoDB 参照系 mysqld 3308 (SF=1)。
- 基準値: TPC-C stateful 140 / autogen 125 / explicit 191-200 req/s。TATP autogen 1591。
  TPC-H SF=1 22/22 + md5 一致、対 InnoDB 5-100x (docs/phase15_innodb_vs_helios_sf1.csv)。

### [2026-06-12] エントリ1: Track A 区間別計測器(TxRpcTrace 拡張)

**目的**: autogen 125 < stateful 140 の真因切り分け。容疑者 = (a) 毎statement再コンパイル
CPU、(b) over-fetch、(c) stage したのに使われない二重払い、(d) range entry validation 肥大。

**設計**: 新規 env gate を作らず**既存 ENABLE_RPC_TRACE 基盤(per-tx JSONL)を拡張**。
RPC 種別ごとの回数/時間/バイトと statement 境界、cache hit/miss イベントは既に記録済み
だったので、不足分の「RPC 以外の区間時間」と「カウンタ」を追加:
- `TxRpcTrace::record_section(kind, us)` + `record_section_count(kind, n)` +
  RAII `SectionTimer`(trace 無効時は分岐1回のみ、clock 読まない)→ JSONL に `sections` 出力
- 計測区間: `autogen_compile`(QEP→plan)/ `txplan_parse`(explicit DSL)/
  `stage_rpc_decode`(RPC+flat decode; ネットワーク純分は TX_EXECUTE_READ_PLAN で別記録)/
  `stage_local`(キャッシュ staging)/ `lookup_range`・`lookup_secondary`(scan cache copy)
- カウンタ: `autogen_steps` / `staged_rows` / `commit_base_rows` / `commit_range_entries` /
  `commit_range_keys` / `commit_write_ops`
- `rpc_trace_` を mutable 化(const な lookup 関数が自己計時するため)
- 集計: 新規 `bench/bin/rpc_trace_agg.py`(JSONL→ tx数/duration percentiles/RPC種別表/
  sections 表/local view 表/residual(=MySQL executor+proxy CPU)/SQL正規化 statement 別時間)

**変更ファイル**: proxy/rpc_trace.{hh,cc}, proxy/lineairdb_transaction.{hh,cc},
proxy/lineairdb_prefetch.cc, bench/bin/rpc_trace_agg.py(新規)

**計測手順**(/tmp/track_a_trace.sh): 3モード(stateful / autogen --prefetch-stmt /
explicit --prefetch)それぞれ「server 再起動+TPC-C リロード(trace OFF)→ mysqld を
ENABLE_RPC_TRACE=1+モード別 path で再起動 → benchrun --no-setup --external-server
--time 60」。トレースのオーバーヘッドは3モード同条件なので分布比較は公平。

### [2026-06-12] エントリ2: 計測条件の重要発見 — phase15 ベースラインは terminals=1

最初の traced run を benchrun デフォルト(--terminals 64)で流したら stateful が
throughput 1709 / goodput 430 / **retry 246k**(NewOrder 167成功 vs 138k retry)という
別世界の数字になった。bench/results を遡って確認した結果:

- **phase15 の TPC-C 基準値(stateful 140 / autogen 125 / explicit 191)は全て
  `--terminals 1` の単スレッド・レイテンシ比較だった**(summary.csv の terminals 列=1)。
  Track A の「autogen はなぜ遅い」は per-tx レイテンシの内訳問題として扱うのが正しい。
- 64 terminals × 1 warehouse は Silo OCC の競合崩壊 regime(NewOrder/Payment が
  D_NEXT_O_ID 等のホット行 RMW で衝突)。これは別軸の事実として記録
  (高並行 TPC-C は phase15 でも未計測。warehouse 数を terminals に合わせて増やすのが
  TPC-C の正規のスケール法)。
- 対処: /tmp/track_a_trace.sh に `--terminals 1` を入れて3モード取り直し。

### [2026-06-12] エントリ3: Track A 真因確定 — 3モード trace 比較(terminals=1, 60s)

throughput(traced): stateful 132.3 / autogen 121.3 / explicit 185.1(トレード約5%減、
ギャップは未トレース時と同型)。集計(bench/bin/rpc_trace_agg.py):

| per-tx | stateful | autogen | explicit |
|---|---|---|---|
| mean duration | 7.3ms | 8.0ms | 5.0ms |
| RPC 回数 | 19.7 | **21.6** | **2.0** |
| RPC 時間 | 2.2ms | 2.7ms | 0.34ms |
| staged rows | — | **407** | 33.6 |
| commit base rows | — | **406** | 43.6 |
| commit range keys | — | **381** | 17.5 |
| validate req bytes | —(END req 50B) | **38.7KB** | 7.5KB |
| 受信 bytes | ~4.5KB | **35.5KB** | 9.2KB |
| residual(JDBC往復+CPU) | 5.1ms | 5.2ms | 4.7ms |

**真因(寄与順)**:
1. **RPC 回数が stateful より多い(21.6 vs 19.7)**。per-statement staging は「1 statement
   = 1 RPC」で、TPC-C は 1 statement ≈ 1-2 行なので per-row RPC と回数が変わらない。
   さらに **SELECT...FOR UPDATE → UPDATE 同一行ペアが二重 staging**(stock 33k+33k RPC、
   district/warehouse/customer も同型)— UPDATE 側の legacy-DML staging が直前 SELECT で
   キャッシュ済みの行を再 fetch していた。
2. **over-fetch**: Delivery の `SELECT NO_O_ID FROM new_order WHERE NO_D_ID=? AND
   NO_W_ID=? ORDER BY NO_O_ID ASC LIMIT 1` が **58.7KB/RPC**(district の全 new_order
   ~300行を staging、LIMIT 1 無視)。autogen compile は意図的に「canonical unbounded
   shape」で staging(lineairdb_autogen.cc:421 コメント、limit pushdown は v2 work)。
3. **validation 肥大**: range cache hit が「全 cached 行の base-row TID + range replay」を
   両方 append(LIMIT で 1 行しか消費しなくても ~300 行ぶん)→ validate req 38.7KB。
4. compile は **無実**(2us/回、220ms/60s 全体)。PS plan cache 仮説は棄却。
- 補助観測: autogen_compile 12.9/tx vs plan_request 20.65/tx の差 ≈ legacy-DML handler
  経路(compile timer 未通過)。use_point_read 26.4/tx で再読はキャッシュが効いている。
- **構造的天井の認識**: 全モード共通の residual ~5ms/tx はほぼ JDBC statement 往復
  (~34 stmt/tx)。per-statement staging の理論下限 ≈ stateful(first-touch 読みは両モード
  とも server 訪問が必須)。explicit 級(2 RPC/tx)に近づくには **同型 tx の read-set
  テンプレート学習による先読み**(Chardonnay 風、cross-statement)が必要 — miss は既存の
  abort→retry 安全網で正しさ担保。

### [2026-06-12] エントリ4: Track A 修正ラウンド1(F1+F3)

- **F1: カバー済み point step の staging スキップ**(lineairdb_transaction.cc
  execute_read_plan 冒頭): binding 無し・key 定数の scalar point step が write set /
  row_cache に既収載なら step を間引き、全 step カバー済みなら RPC 自体を省略。
  SELECT→UPDATE ペアの二重 fetch を根絶(~6-8 RPC/tx 削減見込み)。正しさ: cache hit
  消費時に TID が base_row_read へ載る(use 時検証)ので staging 再取得と等価。
- **F3-lite: range hit 時の base-row validation を消費行のみに**:
  - keys-only variant(get_matching_keys_in_range): per-row append を全廃。根拠 = key
    bytes は delete+insert なしに変わらず range replay が membership/order を保証、値の
    後続読みは read() cache-hit 経路が on-use で TID append(staging は validate_on_use=true
    で record_row_cache 済み)。
  - values variant: limit 窓内の行のみ TID append(窓外は値を観測していない)。
- 期待効果: RPC 21.6→~13/tx、validate req 38.7→~7KB。Delivery の 58.7KB staging 自体は
  F2(limit-aware staging + scan hint + truncation abort)の領域で次ラウンド判断。
- Codex レビュー(計測器 diff)反映済み: staged_rows の空 scan 過大計上修正(steps[i] で
  分岐)、stage_rpc_decode→stage_rpc_and_decode(overlapping 明示、集計側で除外+
  decode_only 導出)、trace OFF 時の無駄計算ガード。

### [2026-06-12] エントリ5: Track A 修正ラウンド2(F2: LIMIT-aware staging)+最終計測

**F2 設計(entry-driven)**: runtime の canonical unbounded 要求は維持したまま、
- コンパイラ(lineairdb_autogen.cc): root が `LIMIT_OFFSET(offset=0) → 単一 REF leaf 直結`
  (= FILTER/SORT 無し = WHERE が key prefix に完全吸収)のとき
  `range_scan_limit_for_order`(runtime と同一判定関数、ha_lineairdb.hh に公開)で
  step.scan_limit を staging。**ASC のみ**(DESC staged entry が tail consumer に
  渡る事故を構造的に排除)。
- cache(lineairdb_transaction.cc): unbounded 要求 ← limit-staged entry の opt-in fallback
  (完全同一 range・forward・**範囲内** pending own-write 無し)。truncated フラグ
  (rows.size()>=row_limit)を返し、forward 消費者(same_key/prefix_first/range_materialize)
  だけが opt-in。prefix_last/prev_key は渡さない(tail 消費は絶対に truncated を見ない)。
- 消費(ha_lineairdb.cc): truncated な materialization を limit 越えて index_next すると
  **EOF を偽らず loud abort**(gate にバグがあっても wrong result でなく可視エラー)。
- validation: staged entry の row_limit=N がそのまま range replay に乗る(server は
  limit-N scan を再実行して N key を比較 = 正しい phantom guard、DSL で実証済みの経路)。

**踏んだバグ**: 初版は fallback の pending-ops 拒否がテーブル粒度
(has_pending_ops_for_table)で、Delivery の district 1 の DELETE(バッファ済み)が
district 2-10 の scan fallback を拒否 → 393 エラー。**範囲交差チェック**
(has_pending_row_ops_in_range、merge と同一ソース・同一条件)に精緻化して解消。

**traced 比較(terminals=1)**: autogen 121.3 → F1+F3: 135.1 → +F2: **142.1**(エラー0)。
per-tx: staged rows 407→**38.3**(explicit 33.6)、commit base rows 406→**44.7**(43.6)、
commit range keys 381→**18.4**(17.5)、validate req 38.7KB→**7.6KB**・353→**187us**
(explicit と同値)、RPC 21.6→**14.9**、受信 35.5KB→10.1KB。

**最終 untraced(各リロード付き、60s, terminals=1)**:
| モード | before | after |
|---|---|---|
| TPC-C stateful | 140 | 135.2(run分散内) |
| **TPC-C autogen** | **125** | **149.5(+20%、stateful 超え)** |
| TPC-C explicit | 191 | 188.6(回帰なし) |
| TATP stateful | 1335 | 1287 |
| TATP autogen | 1591 | 1594(回帰なし) |
- **エラー/retry: autogen 0/0**、C1-C4(+C2b)= 全 0(post-autogen データ、prefetch OFF で検査)。
- commit 354dedc。Codex 敵対的レビュー依頼中(F1/F2/F3 個別 verdict)。
- **残差の構造**: autogen 149.5 vs explicit 188.6 の差 = first-touch per-statement staging
  RPC 13.9/tx vs 1/tx。次の梃子 = 同型 tx の read-set テンプレート学習(cross-statement
  先読み、miss は既存 abort 安全網)— エントリ3の構造的天井の項を参照。

### [2026-06-12] エントリ6: Track B1 NDV stats 移植(コミット済、ビルド/計測待ち)

- 移植列: proto d99ab02(3b0775c)→ Phase1 f4029cb(3cc1c9c)→ Phase2 0aa8ab7(c6e0e4c)
  → review fix 85af297(5ada3d3)。proto は NDV 拡張版 GetTableStats を正とした。
- LineairDB 側 `Database::ComputeIndexNdvInt` は 10c6e2c(NDV+無関係な tx-pin/sha256 の
  混載 snapshot)から **NDV 部分のみ手で抽出**して helios/prefetch-maxopt 作業樹に適用
  (database.h / database.cpp / database_impl.h、既存 Scan/Get API のみ使用・API 整合確認済)。
  gitlink はビルド検証後に submodule commit + 更新予定。
- gate: HELIOS_OPT_STATS=1(default OFF)。次: build_partial → SF=0.1/1 で q5 EXPLAIN/実測
  → COST_V2(phase15 移植済、default OFF)と併用評価・係数再較正。

### [2026-06-12] エントリ7: Track B1 NDV 効果実証 — q5 SF=1 335s → 23.1s(14.5x)

- Codex 敵対的レビュー(Track A)反映: **F1 GO / F2 NO-GO / F3 NO-GO** →
  F2 = SQL_CALC_FOUND_ROWS(count_all_rows)と scalar-subquery LIMIT
  (reject_multiple_rows)は LIMIT 後も行を読むので staging gate に除外条件追加。
  F3 = limited staged entry は「範囲内 pending own-write あり」では一切 serve しない
  (own delete で正解行が窓の外に出る反例)— 3 lookup 経路全部に
  has_pending_row_ops_in_range ガード。修正後 autogen traced 146.8、エラー0
  (commit 6e6c1cb)。
- LineairDB submodule に ComputeIndexNdvInt をコミット(535a077)、gitlink 更新(ce5a05c)。
- **SF=0.1**: EXPLAIN q5 が全段現実推定(customer rows=600=15k/25 厳密値、orders 15、
  lineitem 5)で region→nation→customer→orders→lineitem→supplier の健全プラン。1.87s。
- **SF=1**(HELIOS_OPT_STATS=1、prefetch+ro_novalidate ON、デフォルト cost):
  q5 **23.1s**(phase15 比較表 335.4s → **14.5x**)。プランは旧ブランチ実証形と同型
  (customer 6000/orders 16/lineitem 5)。COST_V2 無しで雪崩解消 = NDV(GIGO 解消)が
  主因という旧ブランチの結論を再確認。
- 実行中: 全22 matrix(回帰確認)→ md5 22/22 vs InnoDB(3308)。

### [2026-06-12] エントリ8: NDV 単独 / NDV+COST_V2 全22 matrix(SF=1)

| gate | 結果 | 主要値 |
|---|---|---|
| NDV のみ | 22/22 OK | q5 22.2s(335→)、広範改善(q3/4/7/9/17/21/22 で -15〜44%)、**q2 21x退行(2.2→47.4s)**、q18 +11% |
| NDV+COST_V2 | 20/22 OK | q2 2.0s(退行解消)、q17 **20.2s**(63.7→)、q10 18.0、q11 1.9、q15 33.9、q21 47.0s・**OOM 無し**(peak mysqld ~25GB 一時)| 
- q2 退行の真因: NDV で基数は正しくなったが legacy handler cost が「supplier 駆動の
  derived 実体化(partsupp 16万 probe)」を安く見積もる。COST_V2(RPC/転送比例)併用で
  part 駆動プランに復帰 = **NDV(基数)と COST_V2(アクセスコスト)は補完関係**、
  という想定どおりの結果。
- COST_V2 既知問題の再評価: q17 カバレッジ破壊は NDV 併用で**消えた**(プラン変化)、
  q21 44GB OOM も**消えた**。残るは **q18 / q20 の ERROR(prefetch cache miss)**。

**q18/q20 真因診断**(EXPLAIN + mysqld ログ):
- q20: dependent scalar subquery(select #4)が lineitem を l_partkey 2-part probe
  → miss "secondary scan lineitem"
- q18: `<in_optimizer>` の run-once materialized subquery(select #2)内の
  lineitem PRIMARY full index scan → miss "primary value scan lineitem"
- 共通根: **Item(Filter 条件)内に埋め込まれた subquery の AccessPath ツリーは
  collect_qep_leaves の child 走査に乗らない**(別ツリー)。default/NDV-only では
  semijoin 等で main ツリー内に展開されていたため露見しなかった。COST_V2 が
  in_optimizer/dependent 形を選ぶと未 stage アクセスが出る。
- 修正方針: autogen_read_plan_from_qep を「main 木 + statement の全 inner
  Query_expression の plan root」の葉を**同一 table_steps で一括コンパイル**に拡張。
  相関 probe(q20)は既存 FES binding(compile_ref_lookup の source step 解決)に
  そのまま乗る。run-once materialize(q18)は full scan staging で可。
  subquery 側の TABLE* は別インスタンスなので duplicate-leaf 検査とは衝突しない見込み。

### [2026-06-12] エントリ9: q18/q20 修正 — Item 埋め込み subquery の inner-unit staging

- **実装**(commit 61c08dc): autogen_read_plan_from_qep に include_inner_units を追加。
  statement の全 inner Query_expression を再帰列挙(unit root が null なら
  qb->join->root_access_path() にフォールバック — q18 の materialized IN はここ)し、
  **main 木と同一 table_steps/steps に追記コンパイル** → 相関 probe(q20 の
  lineitem 2-part probe)は既存 FES binding にそのまま乗る。inner unit の compile 失敗は
  **rollback して非致命**(stage されない subquery が実行されれば従来どおり miss abort
  = 退行ゼロ設計)。compile ループは compile_tree_leaves に分離(raise しない契約)。
- 結果(SF=1, NDV+COST_V2): q18 ERROR→**33.5s OK**、q20 ERROR→**24.6s OK**。
  q2/q4/q17/q21/q22 回帰なし。
- Codex NDV-port レビュー(NO-GO 2件)も同コミットで反映:
  (1) info() の row-count seeding を**非 gate に復元**(port が gate 内に巻き込み
  default 経路を退行させていた)+ gate ON 時は cold path で THD proxy を遅延生成。
  (2) ComputeIndexNdvInt の liveness 判定を **Silo stable-read プロトコル**
  (double-TID bracket + lock-bit spin)に変更 — 生読みは並行 writer と race
  (secondary PK vector の再割当で UB の可能性)。submodule 62eee2e + gitlink 8f34c74。
  ※ stable-read 修正は次回 server 再起動から有効(本日の matrix は read-only 単線で
  race window なし、結果有効)。

### [2026-06-12] エントリ10: Track B1 区切り計測(SF=1 フル)— 22/22 + md5 全一致

計測 env: HELIOS_OPT_STATS=1 + HELIOS_COST_V2=1 + prefetch + ro_novalidate(+MALLOC_CONF)。

| q | before(phase15) | B1後 | q | before | B1後 |
|---|---|---|---|---|---|
| q1 | 30.3 | 27.0 | q12 | 27.3 | 25.6 |
| q2 | 2.2 | 3.1 | q13 | 7.3 | **3.2** |
| q3 | 27.2 | 24.0 | q14 | 27.2 | 24.0 |
| q4 | 28.8 | **16.6** | q15 | 54.9 | **38.6** |
| q5 | **335.4** | **19.3** | q16 | 3.5 | 2.2 |
| q6 | 22.6 | 22.4 | q17 | 63.7 | **35.7** |
| q7 | 26.0 | 20.1 | q18 | 53.4 | **33.5** |
| q8 | 30.2 | 23.0 | q19 | 22.2 | 17.6 |
| q9 | 37.7 | 27.7 | q20 | 35.0 | **24.6** |
| q10 | 26.0 | **17.6** | q21 | 64.8 | **55.1** |
| q11 | 5.7 | 3.3 | q22 | 6.9 | **2.1** |

- **suite 合計 938.3s → 466.4s(2.0x)**、全22 OK、**md5 22/22 vs InnoDB 一致**。
  OOM なし。q5 は 17x。
- **結論: NDV(基数)× COST_V2(アクセスコスト)は併用が正解**。今後の標準計測 env に
  HELIOS_OPT_STATS=1 + HELIOS_COST_V2=1 を追加。
- user 指示(本日): 以降のイテレーションは **SF=0.1 基本**(転送バイト・staged 行数・RSS の
  量的指標を厳密に見る、プラン影響変更は両 SF で EXPLAIN 確認)、**SF=1 フルは
  トラック区切りのみ**(Track B 全完了時など)。
- OLTP 回帰(default gates、各リロード付き): TPC-C autogen **148.6 req/s**(149.5 と
  誤差圏)/ TATP autogen **1575.7**(1594 と誤差圏)、エラー/retry 全 0。**B1 クローズ**。
- 次: Track B2(projection pushdown + late materialization)を SF=0.1 イテレーションで。

### [2026-06-12] エントリ11: Track B2 projection pushdown 実装(SF=0.1 イテレーション)

**実装**(旧ブランチからの設計移植、semijoin 連携部は本ブランチに無いため除外):
- proto: ReadPlanStep に `RowProjection projection = 12`(num_columns + kept field_indexes)。
- proxy 計画(lineairdb_autogen.cc plan_projection_pushdown): physical table 単位で
  全 alias の read_set を UNION(自己結合は superset を共有)。除外 = gcol 表 /
  value-form binding(from_key=false)の source 表(v1 は remap せず full ship)/
  全列読み。**gate = ro_novalidate autocommit SELECT のみ**(tx がその statement で
  終わるため trimmed cache 行が後続 DML の行再構築に流れる罠を構造排除)+
  `HELIOS_PROJECTION=0` で無効化可(A/B + 緊急 off)。
- server(lineairdb_rpc.cc): `trim_row_value`(旧ブランチ移植 — [null_flags full]
  [kept cols] 再 emit、不整合は full ship の安全 fallback)を全6 emission 箇所
  (plain P/S scan, FER, FES, for_each point, scalar point)に `project_value` で適用。
  filter 評価は full row のまま(trim は評価後)。
- proxy decode(set_fields_from_lineairdb): tx の table_projection map で k 番目の
  parsed 列 → kept[k] へマップ(列数一致時のみ=自己修正)。**Step4a**(read_set 外の
  Field::store skip、SQLCOM_SELECT gate)も同時移植。LineairDBField に get_row_size()。
- SF=0.1 matrix: 22/22 OK。

**重大な副産物発見: stateful 経路の既存バグ**。md5 の参照系を「同一データの
prefetch OFF(stateful)」にしたところ q2/q9/q16 の ref が **rc=0 で 0 行**。
**B2 変更を stash した pre-B2 バイナリでも再現** = 既存バグ(B2 無関係)。
共通点 = part への文字列述語(LIKE 系)。ECP off でも再現。phase15 の stateful 重
query は SF=1 で >120s timeout のため一度も結果検証されていなかった。
→ バックログ化(stateful は計測対象外経路だが正しさバグとして要調査)。
md5 参照系は InnoDB(3308 に SF=0.1 ロード、SF=1 ref はマイルストーン時に再構築)へ変更。

### [2026-06-12] エントリ12: B2 計測(SF=0.1)— md5 22/22 + 転送 57% 削減

- **md5 22/22 vs InnoDB(SF=0.1)** — v1 と option-2 remap 後の両方で一致。
  (副次確認: stateful 0行バグの 3 query も target 側は正しかった)
- **A/B(HELIOS_PROJECTION=1 vs 0、全22、ENABLE_RPC_TRACE)**:
  - v1(remap 無し): 3019MB → 1786MB(-41%)、wall 41.0→36.9s(-10%)
  - 取り残し q5/q7/q8/q14/q21(2-13%しか減らず)の真因 = lineitem が value-form
    binding(l_partkey/l_suppkey 等の値列 probe)の source で unsafe 扱い
  - **option-2 remap 実装後: 3019MB → 1313MB(-57%)、wall 39.0→34.9s(-11%)**。
    q21 370→165MB(-56%)、q5 145→61MB、q14 113→46MB — 全 query 45-66% 削減
- remap = binding の source 列を kept に強制 + source_column を projected 位置
  (1-based)に書換え。server の extract_value_column は trimmed 行の位置で読む。
- SF=1 換算で suite あたり ~12GB の転送削減見込み + staged value 縮小により
  proxy 側 RSS も同率縮小(SF=1 はマイルストーン計測で確認)。
- Codex 敵対的レビュー実行中(binding/フィルタ × trimmed 行の交差攻撃ベクタ)。

### [2026-06-12] エントリ13: B2 hardening(Codex 2巡目)+ OLTP 回帰修正 → B2 クローズ

- Codex 1巡目 Finding#1(複数 staging episode の layout 上書き)→ **episode gate**:
  projection は statement-root episode のみ(unit episode は常に full ship、
  parsed-column-count で常に判別可能)。commit 0d7d91b 後に 354dedc 系として fc01f96。
- Codex 2巡目 P1(remap 済み binding が trim-fallback の full 行を誤読)→
  **read-plan の trim 失敗は fail-closed**(ok=false で loud abort。trim 失敗 = 行破損
  なので silent 続行自体が誤り)。その他の攻撃ベクタ(unsafe+force-kept 順序 /
  int_delta / midpoint / episode 再走 / for_each 行数)は全て GO。
- **OLTP 一律 -5% 回帰**の真因 = set_fields 毎行の tx lookup + projection map の
  文字列ハッシュ find。**per-statement メモ化**(query_id + process-wide projection
  epoch で失効 — unit serve が staging 前にメモを stamp する穴を epoch が塞ぐ)。
- 再検証: SF=0.1 md5 **22/22**、TPC-C autogen **162.2 req/s**(pre-B2 148.6 比 +9%!
  Step4a の Field::store skip が OLTP でも純益化)、TATP **1694**(+7.5%)。
- 教訓: 全 Deadlock 事故1件は stale lineairdb-server(pidfile 不整合で旧 image が
  残存)による運用問題 — サービス再起動で解消、コード起因ではない。
- **B2 クローズ**。残課題(優先度低): scalar-source for_each が probe しない既存制約、
  stateful 経路の q2/q9/q16 0行既存バグ(別調査)。

### [2026-06-13] エントリ14: B3 aggregation pushdown 移植(4+2コミット)

- 移植列: proto 03def12 → plumbing 0b33ba7(手書き適合: stamp は本ブランチの
  execute_read_plan の F1 後に配置)→ server 1e9c727(競合解決: row_passes 前置 +
  fail-closed projection と共存)→ executor a2ad6f1(+777行 auto-merge、適合2点:
  borrowed-scan 機構なし→ scanned_values_ serve、env→sysvar gate)
  → morsel 並列 28c92ec/267ee36(gate HELIOS_PARALLEL_SERVER、filter+limit 後適用
  fix も同梱)。
- **踏んだ正しさバグ**: 初版で q6 が **152x 過大**・q1 N/O グループ過大 = Phase B が
  **WHERE 未適用の全行を集約**。旧ブランチは rnd_init→set_pushed_filter→stamp の経路で
  filter が乗っていたが、本ブランチの compile 時 attach(cond_push 由来)は実際には
  空(cond_push 不発)だった。修正 = tx_set_pushed_aggregate で
  prepare_select_filter_for_tx に **WHERE 全直列化を要求**(不能なら Phase A に fallback
  = ローカル評価で常に正しい)+ execute_read_plan の stamp で filter を同梱。
- 検証(SF=0.1): q1/q6 InnoDB と byte 一致、**md5 22/22(HELIOS_AGG_PUSHDOWN=1)**、
  over-hijack なし(他20 query 不変)。q1 1.97→**1.53s**、q6 1.32→**1.08s**。
  morsel 並列は SF=0.1 では誤差圏(入力 600k 行で scan/RPC が支配)— SF=1 区切りで評価。
- 付随発見: cond_push が本ブランチで実質不発(pushed_filter_serialized_ 空)
  → ro_novalidate の compile 時 filter attach は無効化されていた(staged scan は全行
  ship し MySQL がローカル filter)。**WHERE の server filter 化は
  prepare_select_filter_for_tx 経路が実働**。scan filter pushdown の全面有効化は
  別項(q6 の非集約時転送をさらに削れる余地)。
- **B3 クローズ**(commit 471fcf7 まで)。

### [2026-06-13] エントリ15: B4 LineairDB DataItem スリム化(phase12 A1+A2 実施)

- **方式**: per-record の死荷重(2PL RW-lock 64B cache-line aligned + checkpoint 3
  fields ~56B)を **static inline ダミー**化(submodule b2cbe3a)。参照コード
  (TwoPhaseLocking = Masstree 構成では runtime 拒否済 / CheckpointManager =
  checkpointing=false で idle)は**無修正でコンパイル**され、per-record コストのみ
  消える。`LINEAIRDB_WITH_2PL_CHECKPOINT_METADATA` 定義で原状復帰可能。
  DataItem 192B → ~72B(**-120B/record**)。NWR pivot(16B)は silo_nwr 内 44 参照
  のため見送り(残課題)。
- **計測(SF=0.1、フレッシュロード後 server RSS)**: fat **956MB → slim 838MB**
  (-120MB/-13%)。SK entry も DataItem なので行数以上に効く。SF=1 では ~1.2GB+
  の削減見込み(区切り計測で確定)。
- 検証: md5 **22/22**、TPC-C autogen **164.1**(退行なし、むしろ +1%)、
  TATP **1729.8**(+2% — DataItem 縮小によるキャッシュ効率向上の線)。エラー全0。
- 計測の罠(再発): pgrep -f が bash wrapper に self-match して RSS 4MB を返す
  → `pgrep -x lineairdb-serve`(15字切詰め名)を使う。
- **B4 クローズ**。残: SF=1 区切りフル計測(B2+B3+B4 一括、InnoDB ref 再構築込み)。

### [2026-06-13] エントリ16: Phase B を真に発火させるまでの三段バグ(q1/q6)

SF=1 区切り計測で q1 22s / q6 19s(改善が小さい)に気づき追跡。**実は agg pushdown の
server 経路(Phase B)は一度も WHERE 付きで動いていなかった**(常に Phase A に fallback、
md5 は Phase A が正しいため検出されず):

1. **serialize_item が裸リテラル専用**: TPC-H の述語に含まれる `DATE ± INTERVAL` /
   `'0.06' - 0.01` は直列化失敗 → const-fold 追加(const_item は即時評価して
   リテラル化)。temporal は **必ず文字列で fold**(INT 形式 19980902 は ASCII 日付との
   辞書順比較を常真に退化させる — 全行 COUNT 6,001,215 で発覚)。
   `<cache>` ラッパは**未充填だと val_* が NULL** → example へ再帰(commit 0bb2035)。
2. **is_bool_func 判定の誤り**: 「functype != UNKNOWN_FUNC は構造直列化へ」とした
   が `DATE±INTERVAL` は DATEADD_FUNC → fold を素通りして失敗継続。比較/論理演算子の
   **明示ホワイトリスト**に変更(これで初めて Phase B が WHERE 付きで発火)。
3. **rnd_init / index init の filter 上書き**: cond_push 状態(実質常に空)を毎回
   tx に伝播するため、tx_set_pushed_aggregate が準備した WHERE filter を
   `clear_pushed_filter()` が**消していた** → server が無 filter 集約
   (q6 = 全 6M 行の SUM、q1 N/O 過大)。aggregate 保留中は clear をスキップ。
- 各段階を `SELECT COUNT(*) ... WHERE l_shipdate <= DATE-INTERVAL`(正解 5,916,591)
  の即値プローブで切り分け。**0.81s の「成功」が実は死にかけ server のエラー応答**
  (staging 1.3ms/246B は物理的に不可能)という罠も踏んだ — 異常に速い数字は疑え。
- **最終(SF=1, warm)**: q1 **0.57s**(phase15 30.3s 比 **53x**、InnoDB 8.48s 比 15x)、
  q6 **0.35s**(65x、InnoDB 1.03s 比 3x)、両方 byte 一致(commit f7e6f3c 系)。
  並列 scan+filter+agg(HELIOS_PARALLEL_SERVER_SCAN、T=8、l_orderkey 整数分割)が
  実効。途中観測した「server 消滅」は上記エラー応答期の残骸で、修正後の
  q1→q6→q1 連続実行では再現せず。

### [2026-06-13] エントリ17: Track B 完走 — SF=1 最終区切り計測

計測 env: HELIOS_OPT_STATS=1 + HELIOS_COST_V2=1 + HELIOS_AGG_PUSHDOWN=1 +
HELIOS_PARALLEL_SERVER=1 + HELIOS_PARALLEL_SERVER_SCAN=1(server)+ prefetch +
ro_novalidate + projection(default ON)。InnoDB ref は 3308 に SF=1 再構築。

| q | ph15 | 最終 | 倍率 | vs InnoDB | q | ph15 | 最終 | 倍率 | vs InnoDB |
|---|---|---|---|---|---|---|---|---|---|
| q1 | 30.3 | **0.8** | 39x | **0.1x(勝ち)** | q12 | 27.3 | 19.3 | 1.4x | 11.8x |
| q2 | 2.2 | 3.0 | 0.7x | 4.7x | q13 | 7.3 | 5.3 | 1.4x | 1.1x |
| q3 | 27.2 | 19.0 | 1.4x | 5.4x | q14 | 27.2 | 17.2 | 1.6x | — |
| q4 | 28.8 | 15.4 | 1.9x | 27.9x | q15 | 54.9 | 39.9 | 1.4x | 16.0x |
| q5 | 335.4 | 18.3 | 18.3x | 11.4x | q16 | 3.5 | 1.6 | 2.1x | 5.1x |
| q6 | 22.6 | **0.3** | 65x | **0.3x(勝ち)** | q17 | 63.7 | 34.2 | 1.9x | 85.7x |
| q7 | 26.0 | 19.0 | 1.4x | 120x | q18 | 53.4 | 30.0 | 1.8x | 18.7x |
| q8 | 30.2 | 21.1 | 1.4x | 14.1x | q19 | 22.2 | 15.9 | 1.4x | 80.9x |
| q9 | 37.7 | 24.8 | 1.5x | 3.0x | q20 | 35.0 | 24.8 | 1.4x | 45.6x |
| q10 | 26.0 | 16.4 | 1.6x | 11.4x | q21 | 64.8 | 40.9 | 1.6x | 22.2x |
| q11 | 5.7 | 3.9 | 1.4x | 7.2x | q22 | 6.9 | 3.8 | 1.8x | 10.3x |

- **suite 合計 938.2s → 375.0s(2.5x)**、**md5 22/22 vs InnoDB**、エラー0。
  対 InnoDB 中央値 **11.4x**(phase15 ~17x)。**q1/q6 は InnoDB に勝利**
  (agg pushdown + 並列 scan+filter+agg の全部入り)。
- メモリ: server RSS 4186MB(load 後; phase15 想定 ~6GB → **-30%**, B4 slim)、
  **mysqld RSS 17.1GB(matrix 後; phase15 41.5GB → -59%**, B2 projection -57% 転送)。
- データ: docs/phase16_final_sf1.csv(phase15/final/InnoDB 3列)。
- 残バックログ(優先順): 大表 scan ship 系(q3/q7/q8/q12 等)の残 ~19s は
  「scan+filter pushdown の非集約クエリへの全面適用 + 転送後の proxy decode CPU」
  が支配 — 次フェーズは join を含む partial agg / group-by pushdown 拡張か
  late materialization の徹底。NWR pivot 16B 削減、scalar-source for_each、
  stateful q2/q9/q16 0行バグ調査も残。

### [2026-06-13] エントリ18: scan filter pushdown 全面適用 + negative coverage(Track C1)

- **実装**(commit 系列): build_single_table_filter + nested-OR 必要条件抽出(旧ブランチ
  P1 predicate transfer)を移植し、**全 staged scan に table-local WHERE を同梱**
  (ro_novalidate gate)。const-fold(エントリ16)が前提条件だった。
- **被覆問題を2段で解決**: filter で落ちた行への point probe(EXISTS 変換 probe、
  temp-fallback 経由)が cache miss → ①「物理表が plan 内で唯一の leaf のときのみ
  filter 付与」(alias 交差 serve を構造排除)②それでも不可視 consumer が残るため
  **filtered_keys 返送 + negative row-cache 登録**(flat codec v2、StepResult.filtered_keys。
  alias の WHERE conjunct で落ちた行は MySQL も捨てるので not-found 供給は per-alias 健全)。
- **SF=1 最終**: **suite 938 → 167.2s(5.6x)**、md5 22/22(stale ref による全 MISMATCH
  事故1回 — ref 再ロードで解消、target hash は検証済み値と一致)、OLTP 回帰なし
  (TPC-C 160.9 / TATP 1728.9)。**mysqld RSS 41.5 → 8.1GB(-80%)**。
- **InnoDB に 6 本勝利**: q1(0.1x)q3(0.4x)q5(0.6x)q6(0.3x)q9(0.3x)q19(0.8x)。
  対 InnoDB 中央値 **2.7x**(phase15 ~17x)。q5 は 335s→0.9s(372x)。
- 残ギャップ上位: q7 12.2s(nation OR は necessary-condition で押せているが lineitem
  FER probe が支配)、q18 29.8s、q15 26.8s、q20 16.6s、q17 20.3s — semijoin/
  membership reduction(旧ブランチ実証済)と FER probe への filter 適用が次の梃子。

### [2026-06-13] エントリ19: semijoin reduction 移植 + stateful 0行バグ根治(Track C2)

**semijoin/membership reduction**(commit 系列、gate HELIOS_ENABLE_SEMIJOIN):
- compile 時に join ref の equality edge を収集 → union-find で等価クラス →
  FER/FES 高 fanout probe step に対し「同クラスの先行 step に選択的単一表述語があれば
  SemijoinFilter を付与」。server は membership set(source_filter 通過行の key 列)に
  無い probe 行を ship しない。FE point probe の棄却は not-found 供給(被覆維持)。
- P0 正当性 whitelist 同梱(inner equi-join のみ: outer-join inner / semi/anti nest /
  非 top-level / nullable・型不一致 key を拒否 — 旧ブランチで q21 0行・q22 過大の実績
  ある罠)。projection reconciliation(source_filter 持ち source は full ship、
  pre-filtered source は membership 列を force-keep + remap)。
- **F1 の潜在バグも同時修正**: covered-step drop が binding/semijoin の source_step
  index をずらす穴 → 参照済み step は drop 禁止 + 残 step の index remap。
- **SF=1: q7 12.2 → 1.3s**(semijoin 直撃)、q11 2.5→1.4s、**suite 152.2s(6.2x)**、
  対 InnoDB 中央値 **2.5x**、md5 22/22(ref 汚染事故2回 — SF=0.1 反復スクリプトが
  3308 を上書きしていた。要恒久対策: ref 専用ポートのデータを iteration script から守る)。

**stateful 0行バグ根治**(q2/q9/q16、pre-B2 から存在):
- 切り分け: 単一表 OK / `part ⋈ partsupp` で 0 行 → **prepare_select_filter_for_tx が
  WHERE 全体(cross-table 結合述語 p_partkey=ps_partkey 含む)を直列化**し、stateful
  scan が単一表の行に対して別表の列 ordinal で評価 → 全行 false が真因。
  FIELD=FIELD は昔から直列化可能だったため pre-B2 でも再現していた。
- 修正: prepare を build_single_table_filter(table-local 必要条件)ベースに変更。
  LIMIT 安全性は「WHERE がこの表のみ参照」のときに限定(部分 filter + server LIMIT は
  MySQL が必要とする行を切るため)。**2表スライス 42,656 = InnoDB 完全一致**。
- 副次修正: HELIOS_AGG_PUSHDOWN が stateful モードでも Phase B に入り
  「agg group row malformed」エラー(stateful scan は生 base 行)→ Phase B gate に
  srv_prefetch_execution を追加。
- 回帰: prefetch SF=1 suite 全 OK + md5 22/22(warm 108.9s)、OLTP TPC-C 163.0 /
  TATP 1729.9、エラー全0。

残バックログ(優先順): q18/q15/q17/q20 の残 16-29s = materialized subquery 内
single-table GROUP BY の server 集約拡張(override は top-level 限定のため未適用、
inner-unit Phase B 化が次の大物)。scalar-source for_each、NWR pivot 16B。

### [2026-06-13] エントリ20: SharedScan dedup — 同一 self-contained scan step の畳み込み(q15)

- **q9 stateful 再検証の顛末**: 前夜の background check が rc=1(`benchbase.part`
  不存在 = reload 競合)→ 早朝の再検は rc=124 timeout。真因は **mysqld が
  HELIOS_OPT_STATS/COST_V2 なしで再起動されていた**こと(default plan + stateful
  per-row RPC = 爆発)。標準 env で再起動後 **q9 stateful 10s MATCH**。回帰なし。
  教訓: stateful 計測も prefetch 同様 mysqld env 必須(plan が変わる)。
- **q15 真因**(RPC trace 実測, SF=0.1): view revenue0 の 2 回 materialize が
  byte-identical な lineitem full scan step を 2 本 stage →
  `plan_fetch:S:lineitem:keys=600572 ×2`、staged_rows=1,202,144。さらに
  lineitem が 2 step を持つため single-step ルールが filter pushdown を阻止
  (本来 ~66k 行で済む shipdate 3ヶ月窓が全行 ship ×2)。
- **修正**(lineairdb_autogen.cc): filter post-pass の直前に SharedScan dedup
  post-pass を追加。fold 条件 = is_scan / !for_each / scan_limit==0 /
  bindings・end_bindings 空 / aggregate・semijoin 未付与、同一
  (table, index, key_prefix, end_key_prefix, reverse, filter)。最初の step に
  畳み、binding/semijoin の source_step と table_steps を index remap
  (F1 covered-drop と同パターン)。folded alias リストを保持し、filter pass は
  「全 alias の build_single_table_filter が byte-identical」のときのみ付与
  (識別不能 clone = q15 view。異なる述語の self-join は不付与のまま)。
- **結果**(SF=0.1): q15 2.10s → **0.92s**、md5 **22/22 OK**(fold は q15 のみ
  発火、他クエリ無影響)。q18 1.65s 不変(full scan + for_each probe は別形状、
  fold 対象外 — こちらは inner-unit Phase B 案件)。
- 旧ブランチの SharedScan(5fab65c)は本ブランチ非祖先で未移植だったもの。

### [2026-06-13] エントリ21: inner-unit Phase B — materialized subquery の server 集約(q15/q18)

**機構**(エントリ19の「次の大物」を実装):
- **MySQL 側 hook**(submodule mysql-server, branch `helios/materialize-override-hook`,
  commit bb8795e048d): MaterializeQueryBlock 冒頭で inner JOIN の
  `override_executor_func` を確認し、設定済みなら iterator pipeline を回さず
  override に Query_result(temp table へ hash-dedup/write/spill 同等処理で格納)を
  渡す。ExecuteIteratorQuery の既存 bypass(sql_union.cc:1711)の materialize 版。
  override 未設定なら byte-identical(in-tree 利用者なし)。
- **shape gate 拡張**(ha_lineairdb.cc): (a) inner unit を許可 — 条件 =
  uncorrelated(`qe->uncacheable==0`)+ IN/ALL/ANY は **strategy ==
  SUBQ_MATERIALIZATION 必須**(EXISTS は per-outer-row 再実行で uncacheable では
  弾けない — decide_subquery_strategy が push_to_engines より先に確定するのを確認
  済)+ leaf が自 engine + ro_novalidate sysvar。(b) GROUP BY key に INT 許可
  (ORDER BY 無しのとき; emission 順は padded-binary で数値順でない)。
  (c) **HAVING 対応**: 「bare aggregate vs 定数」1比較のみ。q18 の inner HAVING は
  optimize 時点で in2exists 注入条件(`<cache>(o_orderkey)=<ref_null_helper>(...)`)
  が AND されている — `created_by_in2exists()` マーカーで注入分を strip
  (materialization 経路は注入条件を評価しないので正当)。
- **flow**: push_to_engine(optimize 時)で inner の Phase-B spec + 「WHERE の
  **EXACT** 直列化」(必要条件では集約は不正 — 新 out_exact フラグ)を tx に登録
  → autogen の **stamp pass**(fold/filter pass 後)が「step の filter ==
  登録 filter」のときのみ spec を staged scan step に押印し決定を tx に記録 →
  server が group rows を返却 → MaterializeIterator hook → override executor が
  rnd_init(staging 誘発)→ 押印確認 → Phase B consume(押印無しなら Phase A で
  生行を proxy 集約 — どちらも HAVING を評価)。
- **server-side HAVING**(AggregateSpec.having_op/agg_index/const): SUM/COUNT の
  group を emit 時に int128 exact 比較で落とす(q18: 150k groups → 5 行 ship)。
  AVG は div_precincrement のため proxy 側評価のまま。
- **隔離**(group rows は base 行ではない): range cache entry に `aggregate_rows`
  フラグ — 集約自身の consume(pushed_aggregate gate)以外の lookup は skip、
  row cache 登録・filtered_keys 負被覆も skip。binding 参照される step は押印
  しない(positional read が壊れる)。semijoin source からも除外。projection は
  押印 step を trim しない。
- **latent bug 修正**: top-level Phase B の WHERE fullness 検査が「非空」のみで
  部分直列化(necessary condition)を見逃していた → out_exact 必須化。

**計測**(SF=0.1): q15 **2.10 → 0.92(fold)→ 0.05s**(支配項消滅、InnoDB 0.21s に
勝利)、q18 1.65 → 1.7s(下記 plan 依存)、**md5 22/22 OK**。
- q18 の plan は stats 状態で2形態を往復: (A) orders driver + in_optimizer probe →
  inner scan 非参照 → **押印 OK**(server having で 5 行 ship)。(B) materialized
  subquery driver → orders FER probe が inner scan に **binding 参照** → 押印拒否
  (正当)→ Phase A fallback(正しいが 150k probe group が重い)。
- **残バックログ**: (B) plan で binding を group-row column へ remap すれば probe が
  HAVING 通過キー(SF=1 で ~57)だけになり大幅短縮(from_key slice binding の
  column-form 化が必要)。ado: COST_V2 が (B) を選びがちな probe cost 較正。

**運用ミス記録**: load 前の server 再起動を怠り q15/q18 検証がハング(DROP残渣)。
プロトコル再確認: **load 前は必ず server 再起動**。pkill -x mysqld は 3308 ref を
巻き込むため禁止(pid file 使用)。

### [2026-06-13] エントリ22: Codex P1×3 修正 + for_each fold(q17/q20/q21 直撃)

**Codex レビュー(NO-GO → 全 P1 修正)**:
- **P1-1 表キー衝突**: 同一表に異なる spec の inner unit が複数あると登録が上書き。
  → 登録を (spec,filter) 同一なら leaves 追記 / 異なれば **poison**(その表は一切
  押印せず全 unit が Phase A へ = fail-closed)。consume は executor のローカル
  spec と登録 spec の **byte 同一性必須**(begin_inner_agg_consume(expect_spec))。
- **P1-2 fold との合成**: 同一バイトの raw consumer scan が集約 step に fold され
  ると raw 供給が消える。→ 押印に **ownership guard**: folded alias 全員が登録
  unit の leaf であるときのみ押印。
- **P1-3 table 単位の可視窓**: consume 窓が開いている間、同表 lookup が両方向に
  混線し得る。→ `e.aggregate_rows != agg_consumer` で **双方向排他**(consumer は
  group entry のみ・他は raw のみ)。
- 押印記録も leaf 単位化(`inner_agg_stamped_leaves_`): 自分の unit が押印された
  ときだけ Phase B、他者押印は Phase A に綺麗に落ちる。

**for_each fold(旧ブランチ dedup_identical_fetch_steps の一般化移植)**:
- fold 対象を「bindings が deep-equal な for_each probe step」へ拡張。q21 の
  自己結合(l2/l3 が同一 probe-by-l_orderkey fetch)や **q17/q20 の相関 probe
  (main join と subquery が同一 l_partkey probe)** が1本化。
- **semijoin pass を全 alias 一致(unanimity)制に書換**: folded step への
  membership reduction は「全 alias が同一 (source,columns,filter) を選ぶ」とき
  のみ付与(1 alias でも非対応/不一致なら veto — 片側だけの reduction は他 alias
  の必要行を落とし得るため)。

**SF=1 計測**(matrix v3 / 全22 md5 OK=22):
- **q17 19.9→0.19s、q20 16.4→0.53s、q21 18.0→10.7s、q15 26.2→0.42s**
- suite **152.2 → 65.0s**(phase15 比 14.4x)。mysqld RSS 5.2GB / server 4.5GB。
- **InnoDB(3308, 同時再計測 75.4s)を suite 合計で逆転**。勝ち **10/22**
  (q1,2,5,6,8,9,11,15,17,20)、対 InnoDB 中央値 **~1.35x**(2.5x から改善)。
- 残ギャップ: q22 31x(3.15 vs 0.10s!)、q18 17x(16.2 vs 0.94s, cold plan で
  Phase A — binding の group-row remap が次の梃子)、q14 6.8x、q12 5.0x、q4 4.5x、
  q21 3.4x。

### [2026-06-13] エントリ23: server 並列化拡張(filtered scan + for_each probes)+ plan 不安定性の特定

**Track-B 並列化の拡張**(server/rpc, gate HELIOS_PARALLEL_SERVER_SCAN 共用):
- **parallel_primary_filter_scan**: 集約なし filtered primary scan も morsel 分割
  (q12/q14 の 6M 行 scan+filter が serial だった)。emission は morsel 連結 =
  global key 順で serial と byte 同一(validation/coverage 不変)。worker 失敗は
  全破棄 → serial 再走(fail-closed 維持)。
- **parallel for_each probes**: probe key を先に serial 解決(順序保持 dedup)→
  4096 probe 以上で worker 分割実行 → probe 順 emit(group bookkeeping 含め
  serial と同一列)。worker-local evaluator/projection、fe_semijoins は read-only
  共有。
- 効果(SF=1, warm): q12 7.6→6.5 / q14 7.8→6.2 / q22 3.15→2.4(matrix v4 で
  1.73)/ q21 10.7→9.05 / q4 1.88→0.75 / q3 1.30→0.81。md5 **22/22 OK**。

**plan 不安定性が最大の残差**(matrix v3 vs v4, 同一バイナリ):
- q17: 0.19 ↔ 20.3s、q20: 0.53 ↔ 16.9s、q18: 16.2 ↔ 21.0s — NDV stats の
  同期タイミングで join order が flip(probe-driver ↔ scan-driver)。良 plan 時は
  fold/併合が直撃して 100x 速いが、悪 plan は probe 爆発。suite 合計の振れ
  62〜99s は実質この3クエリ。
- **次の本命: plan 安定化**(NDV 取得を deterministic に / COST_V2 の probe cost
  較正で悪 plan を排除)+ q18 cold plan の group-row binding remap。

(以降追記)
