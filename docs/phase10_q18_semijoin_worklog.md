# Phase-10 q18 inner-aggregation → semijoin: 作業ログ（試行・Codexレビュー・採否）

**方針（user指示, 2026-06-01）**: 各ステップで必ず Codex レビュー/指摘をもらい、石橋を叩いて進める。
ノイズor悪化は不採用。**試したことは採否に関わらず全てここに残す**。

関連: [[helios-grouped-summary-q18-semijoin-decision]] / docs/phase10_grouped_summary_design.md §6 /
docs/phase10_grouped_summary_sota_survey.md

決定経緯（要約）: GroupedSummary の materialization intercept は MySQL コア改変が必要 → 「MySQL無改変」不変条件
([[helios-agg-pushdown-override]])と衝突するため却下。q18 のみ semijoin 経路（設計書代替案 a）で実装すると user が判断。
q15(CTE/view) はカバー外で保留。

---

## ステップ一覧と状態

| Step | 内容 | server改変 | 状態 |
|------|------|-----------|------|
| Plan | q18 semijoin 実装計画（Planエージェント） | — | 作成済 |
| Review-0 | 計画全体を Codex レビュー（GO/NO-GO） | — | 進行中 |
| M0 | 未コミット helper `execute_read_plan_raw_values` を commit | 無 | 未着手 |
| M1 | recognizer + aggregate step + proxy HAVING + OP_IN | 無 | 未着手 |
| M2(任意) | server側HAVING / aggregate-source semijoin | 有(feature branch) | 保留 |

---

## 試行ログ

### [Review-0] 計画全体の Codex レビュー（2026-06-01）
- 依頼内容: /tmp/codex_q18_plan_review.md（superset-reduction の健全性 / recognizer 誤hijack / OCC /
  OP_IN cardinality / より単純な機構の有無 / ノイズ・退行リスク）。
- 結果: **NO-GO（as written）**。Codex session 019e7f63、gpt-5.5 xhigh、read-only。全文: /home/noxy/.claude/projects/-home-noxy-helios/88e74208-cb37-45cc-a058-96961fe220af/tool-results/bg2lcfpjm.txt（+ task出力 b4alic7jq.output）。
  - **ブロッカー1（致命的・設計の誤解）**: Helios prefetch は「base-row **キャッシュ温め器**」でありクエリ書き換えではない。`S:lineitem` を aggregate step に差し替えても MySQL 実行エンジンには元の `o_orderkey IN (SELECT ... lineitem GROUP BY ... HAVING ...)` が残る。MySQL がその subquery の lineitem を scan する時、base-row cache が空(helper は意図的に非ingest)→ **per-row RPC 転落(50-100x, [[helios-oneshot-sysvar-must-enable]]と一致)**。よって lineitem base scan は捨てられない。
  - **ブロッカー2（退行）**: server `OP_IN` は行ごとに children 線形ループ(server/rpc/predicate_evaluator.cc:315)。1.5M orders × 57k keys = 数百億比較。hashed membership か既存 semijoin set 経路に変えない限りミルストン1は退行。
  - **副次指摘**: (a) superset 論法は「注入 key 集合 ⊇ 真の qualifying 集合」かつ IN が top-level AND/WHERE 文脈かつ両 key 非nullの時のみ正当。(b) grouped/HAVING IN-subquery は semijoin flatten されない(MySQL は flatten に no grouping/HAVING を要求, sql_resolver.cc)が、**IN2EXISTS 変換で HAVING に conjunct が足される**ので recognizer はそれに頑健である必要。(c) 既存 semijoin attach guard(ha_lineairdb.cc:3211 same-query-block)は意図的、緩めるな(anti/semi/outer/subquery 破損リスクのコメントあり)。
  - **revised GO の条件**: ① **base `S:lineitem` scan 無しで元の q18 inner subquery を満たす/迂回するプラグイン側手段**を用意する(=最大の未解決点)。② 57k-key OP_IN を hashed membership か semijoin set 評価に置換。③ recognizer を top-level-AND-only・非否定IN-only・IN2EXISTS追加HAVING に頑健化。④ RO_NOVALIDATE gate + 22/22 md5 + q17/q20/q2 非発火テスト。
- 採否: **不採用（NO-GO）**。計画ミルストン1は根本的に成立しない。条件①(lineitem base scan を消す plugin 側手段)を満たす新機構を次で検討。

### [Review-1] REVISED 機構: handler-level idempotent aggregate scan（2026-06-01, 進行中）
- 着想: Phase-8 の `agg_next_raw`(server集約→group行)を top-level override でなく**内側 IN-subquery の lineitem handler scan 自体**に適用。handler が rnd_next で「1 orderkey あたり pre-summed の行」(l_orderkey=key, l_quantity=group SUM)を record[0] に展開→MySQL の `SUM(l_quantity) GROUP BY l_orderkey` 再集約は**冪等**(SUMは分配的・1行/key)→HAVING も正しく適用。内側 scan が aggregate RPC で完結するので per-row RPC 罠を回避。MySQL 無改変。q18 は lineitem を内側subquery(独立grouped Query_block)と外側joinの2箇所で使うので、handler は自テーブルの query block shape で内外を判別。
- 依頼: /tmp/codex_q18_revised_review.md（冪等性の証明・内外scan判別の信頼性・rnd_init/rnd_next で agg scan を駆動する再入安全性・OCC・win/noise判定・より単純な代替）。
- 結果: **条件付きGO**。session bscjw5kxt。機構は意味論的に成立、ただし hard conditions 3点：
  - **C1**: 現行 cache aggregate 経路(`tx_set_pushed_aggregate`/`agg_next_raw`→base-row cache ingest, transaction.cc:605)は**禁止**。synthetic group 行が後続の外側 lineitem scan を汚染する。→ **`execute_read_plan_raw_values()`(transaction.cc:642、cache非ingest)を使え**。←前セッションの未コミット helper が条件①の答えだと Codex が裏付け。
  - **C2**: 内側 lineitem の base-row prefetch を**抑止**。auto-QEP uncovered-net(ha_lineairdb.cc:2940)が `S:lineitem` を出したままだと aggregate RPC が追加コスト=退行。内側 `TABLE*` だけ prefetch skip。
  - **C3**: recognizer は **materialized・非依存実行**を証明。IN2EXISTS 経路(item_subselect.cc:1921 HAVING conjunct追加/dependent化)で発火は危険・性能破滅。`table->pos_in_table_list->query_block`(:3628 で既出)で内外判別、`UNCACHEABLE_DEPENDENT`無/outer ref無/`GROUP BY l_orderkey`厳密/`SUM(l_quantity)>const`厳密、nullable/AVG/COUNT/DISTINCT/hidden HAVING agg/overflow を全 reject。
  - 確認: 冪等性は q18 の形で成立(composite_iterators.cc:304/395、l_orderkey/l_quantity NOT NULL)。rnd_init が即 oneshot 生成(:3730)するので recognizer/skip はその前に挟む。OCC は RO_NOVALIDATE のみ健全(transaction.cc:1704,2505)。win は **benchmark gate 必須**(server は依然6M scan+集約、residual floor: docs/phase10_pushdown_sweep.md:38)。
  - NO-GO 化条件: 現行 cache machinery を使う or 内側 S:lineitem prefetch を残す → NO-GO。
- 採否: **採用（条件付きGO）**。revised 機構で実装。helper は C1 を満たすので M1' の一部として commit。

---

## 実装計画 M1'（handler-local raw aggregate scan, plugin-only/MySQL無改変）
1. **recognizer** `helios_q18_inner_agg_table(TABLE*)`: pos_in_table_list->query_block が単一leaf grouped、GROUP BY=単一INT非null l_orderkey、having=SUM(col)>const、DISTINCT/LIMIT/ROLLUP/window無、UNCACHEABLE_DEPENDENT無/outer ref無(materialized非依存)、top多表blockでない。q17/q20/q2(相関スカラ)・外側lineitem(多表block)は非発火。
2. **prefetch skip**: auto_generate_plan_from_qep の uncovered-net(:2940)で当該内側 TABLE* に S: を出さない。
3. **handler-local agg scan**: 当該 TABLE* の rnd_init で AggregateSpec(group=[l_orderkey], aggs=[SUM(l_quantity)]) を `execute_read_plan_raw_values` で実行しバッファ、rnd_next で group 行を record[0] に展開(l_orderkey=key, l_quantity=SUM)、尽きたら EOF。cache machinery 不使用。
4. **gate**: HELIOS_RO_NOVALIDATE + recognizer。条件不一致は通常 scan へ fallback。
5. **verify**: 22/22 md5、q17/q20/q2 非発火ログ、q18 transfer(HELIOS_PLAN_SIZE)+latency、win を benchmark gate。
- 各増分で Codex review を挟む(user指示)。

### [M1'-inc1] recognizer `helios_q18_inner_agg_recipe` + debug probe（2026-06-01）
- 実装: proxy/ha_lineairdb.cc に recognizer 追加(auto_generate_plan_from_qep 直前)。挙動変更なし。rnd_init に HELIOS_FE_DEBUG ガードの `[GSPROBE] q18-inner-agg FIRE` ログのみ。
- 全 MySQL API は実ソースで事前検証(憶測なし): Query_block::is_dependent()/uncacheable/outer_query_block()/leaf_table_count/is_grouped/is_distinct/has_limit/olap/has_windows/group_list/having_cond/fields、master_query_expression()->item(Item_subselect)、substype()==IN_SUBS、Item_sum::sum_func()==SUM_FUNC/get_arg/argument_count、Item_func::GT_FUNC、Item::SUM_FUNC_ITEM。
- gate要点: 非依存(materialized,!is_dependent)・uncacheable==0でIN2EXISTS除外、INT非null単一group key、SUM単一・非DISTINCT、SELECT=group keyのみ、HAVING=SUM(col)>const単項(余分conjunct無)。
- ビルド: scripts/build_partial.sh（進行中）。
- 検証予定: q18 で inner lineitem のみ FIRE / q17・q20・q2・外側lineitem 非FIRE。→ 確認後 Codex review(inc1)。
- 環境: lineairdb server + mysqld(port3307, FE_DEBUG+RO_NOVALIDATE) 起動、SF=0.1 load 済(lineitem 600,572行)。oneshot ON。
- **発見1(probe配置)**: 初回 q18 実行で [GSPROBE] 非発火。原因=EXPLAIN FORMAT=TREE で内側 lineitem は **"Index scan on lineitem using PRIMARY"**(index scan)で `rnd_init` を通らない(外側 lineitem は "Table scan"=rnd)。→ probe を `index_init` にも追加して再検証。recognizer ロジック自体は未否定。
- **発見2(増分2の wiring 条件・重要)**: 内側プランは `Index scan PRIMARY`(l_orderkey 昇順) → **streaming `Group aggregate`** → `Materialize with deduplication` → `<auto_distinct_key>` lookup。含意: (a) server が返す group 行は **l_orderkey 昇順**でないと streaming Group aggregate が壊れる(順序保証が必須)。(b) Materialize dedup は 1行/orderkey なので no-op。(c) 増分2は index scan 経路(index_init/index_first/index_next or index_read_map)に hook する必要(rnd 経路でない)。
- 結果: index_init probe 版を再ビルド中。Codex inc1 code review も並行実行中。

### [Review-2] Codex inc1 recognizer code review（2026-06-01, session bokws0iy7）
- **HIGH（false-negative 真因, probe配置とは別の本質バグ）**:
  1. prepare 時 `split_sum_func2`(sql_resolver.cc:482)が HAVING の SUM を **aggregate ref でラップ** → `lhs->type()` は SUM_FUNC_ITEM でない。**`lhs->real_item()` でアンラップ必須**。
  2. `?` は `Item_param::used_tables()==INNER_TABLE_BIT` で **`const_item()`==false**。**`const_for_execution()`** を使う。
- **MEDIUM**: (a) `where_cond()==nullptr` ガード追加(aggregate scan は inner WHERE 非適用)。(b) hidden SUM が唯一の集約か明示、SUM 引数 nullable/非数値 reject。
- **回答要点**: 真因修正後は guards は q17/q20/q2/外側lineitem 除外に十分(22 内で false-positive 無し)。IN2EXISTS/materialization 決定は handler scan 前に確定(classic optimizer、hypergraph off 前提)→ flags 信頼可。GT_FUNC 限定は安全(false-negative 止まり)。redundancy: uncacheable!=0 が is_dependent を包含、group_list.elements==1 が is_grouped を概ね包含(無害, 保持)。
- **判定**: **probe が q18 inner のみ発火確認できるまで increment-2 をブロック**。
- 採用した修正(proxy/ha_lineairdb.cc recognizer): `lhs=hf->arguments()[0]->real_item()`、`rhs->const_for_execution()`、`where_cond()==nullptr` ガード、SUM result_type∈{DECIMAL,INT}、SUM arg 非nullable。table/column 名のハードコードは**不採用**(Codex 回答1で shape guards が22内で十分と確認済、汎用性優先)。再ビルド中。
- 結果（runtime 検証の連続調査, SF=0.1, build3〜9）:
  - **probe配置修正**: 内側 lineitem は `index_init`(index scan)経由。診断 `helios_q18_diag`/GSDIAG2/GSDIAG3 を追加。
  - **GSDIAG**: 内側 lineitem は早期ガード全通過(outer=1 dep=0 uncach=0 subq=1 substype=3=IN_SUBS simple=1 leaf=1 grouped=1 ngrp=1 where=0 having=1)。
  - **GSDIAG2**: group key OK(FIELD/INT/非null/本表)、SELECT OK(nvis=1)。**HAVING の htype=13=COND_ITEM**(MySQLは単一でもAND wrap)→FUNC_ITEMチェックで落ちていた。COND_AND unwrap を追加。
  - **GSDIAG3(致命的発見)**: HAVING は **COND_AND の2 conjunct**: conjunct[0]=GT_FUNC(`SUM>300`正しい)、**conjunct[1]=EQ_FUNC**。EXPLAIN では内側 Filter は `sum>300` のみなのに `having_cond()` には `SUM>300 AND <EQ>` が入る。この EQ は IN の等価条件(in2exists 注入の休眠条件と推測)。**もし内側スキャンが実フィルタとしてEQを適用するなら、私の機構(`GROUP BY...HAVING sum>const`のみ再現)は誤結果**。= Codex 第1レビューの警告「MySQLの書き換えでQuery_block形状が想定と違う」ケース。recognizer は multi-conjunct を reject するので**正しく非発火**(安全側)だが、その結果 **q18 はこのプラン形では発火しない**。
  - **EQ 引数特定(GSDIAG3)**: conjunct[1] EQ = `<subselect type=20 used_tables=0x2(外側)> = lineitem.l_orderkey(本表)`。= **in2exists 相関等価条件**。is_dependent()=0 なのに having_cond() は in2exists 拡張形を保持。内側 index_init は **1回のみ**(=materialization, in2exists per-row 反復でない)。
  - **核心の問題**: materialization 実行は `sum>300` のみ適用(EQは外側probe)だが、**resolved `having_cond()` は in2exists拡張形(EQ込み)を返す**。→ `having_cond()` だけでは materialization が実際に適用する条件を安全判定できない(Codex第1レビュー警告の現実化)。recognizer は multi-conjunct reject で**安全側非発火・誤結果なし**だが **q18 がこのプラン形で発火しない**。
  - 採否: 現状の recognizer は**正しく(安全側で)非発火**。

### [Review-3] Codex 判定依頼 = **ツール障害(ハング)**
- /tmp/codex_q18_inc1_review2.md で「materialized実条件の安全取得API/相関EQ休眠判定/q18 NO-GO か」を依頼。
- **Codex が "Reading additional input from stdin..." のまま69分ハング**(過去3回は4-8分で完了)。stdin待ちか内部スタック。有用出力ゼロ。PID kill で停止。ツール側障害、再発リスクあり。

### 実測で判明した核心(Codex 無しでも確定している事実)
1. q18 内側 subquery は **materialization** 実行(EXPLAIN: Materialize with dedup、内側 index_init は **1回**)。
2. materialized 内容 = **5 qualifying orderkeys**(`SELECT l_orderkey ... HAVING SUM(l_quantity)>300` standalone と一致、**外側非依存**)→ **EQ conjunct は休眠**(外側 probe で適用)。
3. しかし resolved `having_cond()` は in2exists 拡張形 `SUM>300 AND (<外側subselect ref>=l_orderkey)` を返す。**`having_cond()` だけでは materialization 実条件を安全判定できない**。
4. 識別の手がかり: 休眠 EQ は `used_tables()` に**外側ビット(0x2)+subselect**を含む。真の内側フィルタは used_tables ⊆ 本表(0x1)。→ 理論上は「外側参照 conjunct を除外し、残りが `SUM(col)>const` のみなら受理」が可能だが、**materialization が選ばれた保証(is_dependent()=0 だが EQ 混入で不十分)**と併せないと in2exists 実行時に誤結果リスク。= **高リスク、専門レビュー(Codex)必須の領域だが Codex がハング中**。

### 現状の到達点と判断ポイント
- recognizer は安全(誤結果なし)だが q18 はこのプラン形で**発火しない**。発火させるには「HAVING の外側参照 conjunct 除外 + 確実な materialization 判定」という高リスク recipe が必要で、Codex 検証が前提。
- win も不確実(Codex 既出: server は依然 6M scan、benchmark gate 必須。SF=0.1 では qualifying は 5 件のみ)。
- コード状態: 作業ツリーに recognizer + 診断(GSDIAG/2/3, debug-only・挙動不変)。未コミット。helper も未コミット(前セッション)。
- → user 判断「Codex 再試行を先に」。**ハング原因 = `> file` リダイレクトで stdin がbg bashの開いたパイプ→codex が stdin 待ち**。`< /dev/null` で解決。

### [Review-3b] Codex 決定的判定（session br20dllm2）= **条件付きGO（corrected recipe）**
機構は viable。修正点（実ソース根拠付き）:
- **`qb->having_cond()` は誤り**(prepared tree, in2exists EQ 混入, item_subselect.cc:1921/1977)。
- **正しい源 = `qb->join->having_cond`**(最適化後/実行用, sql_optimizer.h:511)。materialization finalizer(item_subselect.cc:422)が `created_by_in2exists()` 述語を strip 済(:406)。
- **ゲート = `Item_in_subselect::strategy == Subquery_strategy::SUBQ_MATERIALIZATION`**(item_subselect.h:428/412) + `qb->join->is_optimized()` + `join->where_cond==nullptr`、残り HAVING 厳密に `SUM(col)>const`。
- EQ 休眠は **used_tables 推論でなく finalizer が保証**(健全)。外側EQ は hash semijoin/temp probe 経路で処理。
- **採用**: recognizer を「qb->having_cond() multi-conjunct reject」→「strategy==MATERIALIZATION 証明 + qb->join->having_cond パース」に変更。全API実ソース検証済。

### [M1'-inc1 検証完了] corrected recipe 実機確認（SF=0.1, build10/11）
- **q18: FIRE=1**(`[GSPROBE] FIRE (index_init) table=lineitem grp_idx=0(l_orderkey) sum_idx=4(l_quantity)`)。
- **q17 / q20 / q2: FIRE=0**(相関スカラ subquery、正しく非発火)。
- 診断コード(GSDIAG/2/3)を全除去、FIRE-only probe のみ残置(GSDIAG残=0)。clean build OK、q18 FIRE=1 維持。
- **increment-1(recognizer)完了**。corrected recipe を Codex レビュー依頼中(session by828dl55、`< /dev/null` でハング回避)→ clean なら increment-2(handler-local agg scan 本体)へ。
- 残: 全22-suite での false-positive 最終確認は increment-2 の md5 で担保(挙動変更時に誤発火すれば md5 が壊れる)。

### [Review-4] Codex 最終 recipe レビュー（session by828dl55）= **承認（recognizer はバグなし、recipe 一致）**
- 回答1: `qb->join->having_cond` で正しい。`is_optimized()` 単体は finalization 証明にならない(set_optimized は strategy 確定前)が、**`strategy==SUBQ_MATERIALIZATION` ゲートが安全性を保証**(finalizer が strategy 設定と in2exists strip を同時実施, item_subselect.cc:432)。index_init 実行時点で ordering risk なし。recognizer を optimizer/setup コードに移すな。
- 回答2: strategy ゲートで十分(materialization eligibility が in2exists 前の相関を排除)。`qe->uncacheable==0` は任意の belt-and-suspenders。
- 回答3: multi-conjunct reject は安全(false-negative 止まり)。`created_by_in2exists()` reject は無害な追加防御。
- **採用した recognizer hardening**: `qe->uncacheable==0`、unwrap 後 `having->created_by_in2exists()` reject。→ 再ビルド・q18 FIRE 維持確認中。
- **increment-2 で対処すべき 3 ゲート(Codex P指摘)**:
  - **P1 carrier 域**: synthetic 行に group SUM を元列(l_quantity DECIMAL(15,2))へ書く時の overflow。q18 は SUM(l_quantity) max~350 で収まるが、**汎用テーブルでは危険**→ q18 narrowing か carrier 域証明。
  - **P2 scan順/index ガード**: `index_init` の `idx` で「group 列が active index の先頭 keypart(q18=PRIMARY)」を要求(昇順 group emit の前提)。probe は change_active_index 前なので `idx` 直接使用。
  - **P2 RO_NOVALIDATE 強制**: hijack(execute_read_plan_raw_values 突入)を必ず `tx_ro_novalidate()` でゲート。recipe comment だけでなく activation で強制。

## increment-2 設計メモ(handler-local idempotent aggregate scan 本体)
1. **prefetch skip**: auto_generate_plan_from_qep の uncovered-net で当該内側 TABLE* に S: を出さない(C2)。
2. **activation**: index_init で recognizer FIRE かつ idx==group列先頭index かつ tx_ro_novalidate() の時のみ hijack 有効化(state を handler member に)。
3. **agg fetch**: AggregateSpec(group=[l_orderkey], SUM(l_quantity)) を `execute_read_plan_raw_values` で実行しバッファ(base-cache 非ingest, C1)。group 行は **l_orderkey 昇順**で(streaming Group aggregate の前提)。
4. **emit**: index_first/index_next(と rnd 経路も?)で 1 group 行を record[0] に展開(l_orderkey=group key bytes, l_quantity=group SUM)、尽きたら EOF。
5. **carrier 域(P1)**: SUM を l_quantity DECIMAL(15,2) に格納可能か検証 or q18 narrowing。
6. **verify**: 22/22 md5(InnoDB 一致)、q18 transfer(HELIOS_PLAN_SIZE)+latency、win を benchmark gate。各増分で Codex review。

### [M1'-inc2 着手] handler-local idempotent aggregate scan 本体（2026-06-01, user「そのまま実装」）
- inc1 hardening 反映済(qe->uncacheable==0 / created_by_in2exists reject)、q18 FIRE=1 維持確認。
- 実装方針: 上記設計メモ 1-6。C1(cache非ingest=execute_read_plan_raw_values)/C2(prefetch skip)/3ゲート(carrier域/index順/RO_NOVALIDATE)遵守。
- 既存部品の調査から開始(AggregateSpec構築・group行フォーマット・execute_read_plan_raw_values呼出・ReadPlanStep表参照)。
- **データ形式確定(server/rpc/lineairdb_rpc.cc + proxy)**:
  - server group 行 = `emit_agg_groups`: `[null_flags(empty)][group cols: extract_value_column バイト][per-agg: value,count]`。各フィールドは agg_emit_field 框 `[byteSize:1B][len:LE][value]`。
  - **row value のカラムは ASCII テキスト**(l_orderkey="12345", sum="312.00")。`set_fields_from_lineairdb` は `field->store(text, len, &my_charset_bin, CHECK_FIELD_WARN)` で復元。
  - prefetch skip 地点 = uncovered-net(ha_lineairdb.cc:3106-3130, `Q18's IN(SELECT...lineitem...)` コメント)。当該内側 TABLE* を skip。
  - execute_read_plan_raw_values(steps,&out): steps[0].scan_values を返す(cache非ingest)。step は table_name=physical_table_key(t)/is_scan/end_key_prefix=16×0xff/aggregate_serialized。
### [Review-5] Codex increment-2 emit 設計（session bz0bfsbso）= **条件付きGO（fail-closed full-index-scan hijack）**
- **Emit A 採用**: synthetic full row value + `set_fields_from_lineairdb`(既存 null bitmap copy/read_set skip/Field::store 再利用)。先頭フィールド=raw null bitmap: `nf(null_bytes,'\xff')`、`mark_not_null(field[group/sum])`(nullable のみ null_offset/null_bit クリア)、以降 col 位置順(group=key ASCII, sum=sum ASCII, 他 NULL)。**read_set に group/sum 以外が無いこと**をガード。
- **順序**: integer(group_key) で数値昇順(ASCII 不可)。
- **carrier 域(fail-closed)**: sum_idx==l_quantity MYSQL_TYPE_NEWDECIMAL 非null scale2 precision15。sum_ascii の小数桁>scale or 整数桁>precision-scale なら **abort/error(silent fallback 禁止)**。INT carrier は range check。
- **entry points**: index_init=candidate(env+recognizer+RO_NOVALIDATE+idx先頭keypart==group列)。**index_first で activate+fetch**(full 昇順 scan 証明)。index_next で継続。**index_read_map(非null key)では hijack せず candidate clear**。index_end で reset。
- **interaction**: cache 迂回 OK(execute_read_plan_raw_values)。`last_fetched_primary_key_` に group key を入れるな。position()/rnd_pos は本scanで非関与(temp materialize)。OCC は RO_NOVALIDATE のみ健全。
- **採用**: 上記 safe recipe で実装。synthetic row の framing を set_write_buffer/LineairDBField と一致させる(確認中)。新 env gate `HELIOS_GS_Q18`(default OFF)。

### [M1'-inc2 実装] handler-local idempotent aggregate scan（2026-06-01, build中）
実装内容(proxy/ha_lineairdb.{cc,hh}):
- **メンバ**(hh): gs_candidate_/gs_active_/gs_group_idx_/gs_sum_idx_/gs_rows_(key_ascii,sum_ascii)/gs_pos_ + gs_fetch_and_buffer()/gs_emit()。
- **gate**: `helios_gs_q18_enabled()`(env HELIOS_GS_Q18, default OFF)。
- **carrier(fail-closed)**: gs_sum_fits_carrier(): DECIMAL は frac>scale or int>precision-scale で reject、INT は整数性+桁数。
- **gs_fetch_and_buffer**: AggregateSpec(group=[gs_group_idx], SUM(COLUMN_REF gs_sum_idx)) を execute_read_plan_raw_values(cache非ingest)で実行→[null_flags][group][sum][count]を4フィールドparse→carrier検証→**integer(group key)で数値昇順sort**→gs_rows_。anomaly は全て HA_ERR(fail-closed)。
- **gs_emit**: synthetic row value(null bitmap nf=0xff+mark_not_null group/sum、各col位置順: group=key, sum=sum, 他空)を LineairDBField で組み set_fields_from_lineairdb。last_fetched_primary_key_ は設定しない。
- **index_init**: candidate arm(gate+ro_novalidate+recognizer+idx先頭keypart==group列+read_set が group/sum のみ)。
- **index_first**: candidate→gs_fetch_and_buffer+gs_emit(activate)。**index_next**: gs_active→gs_emit。**index_read_map**: keyed(keypart_map!=0)で candidate clear。**index_end**: reset。
- **C2 prefetch skip**: uncovered-net で recognizer 発火の内側 TABLE* のみ S: を出さない(gate付き)。
### [M1'-inc2 検証] = **正しいが win 無し（q18 構造上 noise/退行）**
- **正しさ ✓**: gate ON の q18 結果が gate OFF(=InnoDB一致 baseline)と **md5 完全一致**(5169b322..., diff 空)。GSPROBE 発火=gs path 使用。冪等集約スキャン機構は正しく動作。
- **win ✗ (退行)**: SF=0.1, 20-run wall-clock: gate OFF **17.1s** vs gate ON **20.8s**(+21% 遅い)。
- **構造的根本原因(決定的)**: gate ON の [QEP] = step0 S:customer / step1 FES:orders / **step2 S:lineitem**(内側S:が外側とdedup統合, "dropped 1 redundant step")。
  - q18 は**外側でも lineitem を full scan**(最終 SUM(l_quantity) per order)→ lineitem 600k は外側のためにどのみち転送(内側と cache 共有)。
  - gate OFF: 共有 cache を内側も使い proxy で 600k 集約。gate ON: 内側を gs が **別 aggregate RPC**(server 600k 再scan→150k group行)で処理=**純上乗せ**、外側 600k 転送は不変。
  - **設計前提の誤り**: 「内側が6Mを別途ship」は誤り。内側は外側の lineitem scan を共有しており、減らせる別途転送が存在しない。**q18 では gs に win が構造上あり得ない**(転送不変+aggregate RPC上乗せ)。Codex が繰り返した「benchmark-gate, noise の可能性」が q18 の構造上不可避だった。
- **判定**: user 指示「ノイズまたは悪化する場合は採用不要」に該当。**q18 GroupedSummary は不採用**(gate `HELIOS_GS_Q18` default OFF なので本番影響ゼロ)。機構自体は正しく、「集約対象表が外側で scan されない」別クエリには有効だが q18 はそのケースでない(TPC-H に該当クエリは乏しい)。
- 残課題: コードの disposition(revert / gated-OFF dead code として保持 / SF=1 で念のため実測) は user 判断。22/22 md5(gate ON 全クエリ)と Codex review は採用する場合のみ要。

### [最終 disposition] = **revert して撤退**（2026-06-01, user判断）
- q18 は構造上 win 不可能(外側 lineitem scan と転送共有、gs は aggregate RPC 上乗せのみ)と判明。user 指示「ノイズ/悪化は採用不要」に該当。
- **revert 対象**(`git restore`): proxy/ha_lineairdb.{cc,hh}(recognizer/gs scan/index wiring/prefetch skip/helpers)、proxy/lineairdb_transaction.{cc,hh}(execute_read_plan_raw_values helper、前セッション由来だが q18 専用)。
- **保持**: 本議事録 + docs/phase10_grouped_summary_sota_survey.md(集約pushdown SOTA裏取り) + memory(receipt案/q18決定経緯)。試行・実装・計測・根本原因を完全保存。
- **教訓(将来用)**: GroupedSummary 機構(handler-local 冪等集約スキャン、recognizer は `qb->join->having_cond`+`SUBQ_MATERIALIZATION`、emit は synthetic row value+set_fields_from_lineairdb)は**正しく動作する**。ただし「集約対象表が外側クエリでも scan される」場合は転送が減らず無効。適用には「集約対象表が outer で非 scan」が必須条件。q18 はこれを満たさない。TPC-H で満たすクエリは乏しい(集約結果を IN/join で使う形は概ね outer でも同表参照)。
- Codex review 履歴(設計2回/recipe修正1回/コード2回/emit設計1回=計6セッション)は本機構の正当性検証として有効、結論は「正しいが q18 に win 無し」。
