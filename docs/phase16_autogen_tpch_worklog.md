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

(以降、変更・計測ごとにエントリ追記)
