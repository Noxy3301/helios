# LineairDB-PAX secondary executor — 実装ジャーナル (試行錯誤の記録)

目的: 「何を試し、何が起き、なぜその判断をしたか」の時系列記録。採用判断の
出典は lineage log (2026-07-02-olap-executor-lineage.md)、こちらは
**negative results と設計転換の経緯** (論文の design-space 議論・再現性の
一次ソース)。以後、マイルストーンごとに追記する。

## Phase B (2026-07-02): join 順序の試行

| 試行 | 観測 | 判断 |
|---|---|---|
| greedy join 順 (records 昇順で接続順に build) | q5 で lineairdb-server ごとクラッシュ。原因: `c_nationkey = s_nationkey` (M:N, 国=25種) を先に選び中間結果爆発 | 撤回。FROM 句順 (TPC-H はチェーン記述) + サーバ側 INNER build/probe 実サイズスワップ + 中間 64M 行上限 (fail→primary) |
| MySQL best_positions の写像 | secondary 経路では **best_positions が nil** (hypergraph optimizer が使われ旧構造は未構築) | 断念 → FROM 順継続。プラン情報は AccessPath 木にあると判明 (Phase E で回収) |

## Phase C (2026-07-02): サブクエリの下ろし方

| 試行 | 観測 | 判断 |
|---|---|---|
| 自前 decorrelation (Neumann-Kemper 変換を認識器に実装) の準備 | **secondary prepare 時に MySQL 自身が NOT EXISTS/相関スカラを derived+LEFT JOIN へ変換する** (HeatWave 向け transform、WL#13520 系) ことを q21 診断で発見 | 自前変換を書かず、変換済みプランを受ける「derived サブブロック再帰実行 + virtual table join」に一本化 — 実装量が激減し、意味論は同じ |
| sj nest キーだけで q21 | WHERE の `o_orderkey = l2.l_orderkey` が等値伝播で nest 内列経由になり join edge が消失 ("disconnected") | Item_equal のペア展開 + nest-inner 列を sj_outer 等価に書き戻す 2 規則を追加 |
| q13 の COUNT(o_orderkey) | LEFT ミス行が 1 と数えられ 1 行ズレ | COUNT(col) は必ず arg を IR に載せ、サーバが null-ref 行をスキップ |

## Phase D (2026-07-03): QEP の使い方 — 3 回の設計転換

| 試行 | 観測 | 判断 |
|---|---|---|
| **D2-a**: QEP のカーディナリティ見積もりだけ取り出し、join 順を見積もり昇順の貪欲で決め直す | q5 が 220ms→2107ms (10 倍悪化)。貪欲は表の小ささしか見ず M:N 辺 (nationkey) を先に選ぶ — Phase B の教訓の再演。**MySQL のプラン (順序) は正しく、負けたのは自前の順序決め** | 見積もり順ソートを derived 持ちクエリに限定 (中間判断) |
| **D2-b**: derived (見積もり 0 行) が発行順先頭に来る | semi filter の張り先が無くなり q17 効果ゼロ。レビュー (Claude F1) も独立に同指摘 | virtual 表は常に最後に発行 |
| **D3-a**: semi filter に「8 倍選択的」閾値 (乗算判定) | q17 不発の第 2 原因: derived 見積もり 0 に対し part(20)×8 > 0 が偽。さらに Codex が乗算の unsigned wrap (HA_POS_ERROR-1×8) を指摘 | sub-block への semi は無条件 (プルーニング利益 >> コスト) + サーバ側 4M キー動的ガード。判定は除算に |
| **D3-b**: 見積もり順ソート自体の再検討 | Codex#3: derived 持ちクエリでも real 表ソートは M:N 退行リスク。sub-block semi は「発行順が最後」なら FROM 順でも成立すると気付く | **ソート全撤回。最終形: join/発行順は FROM 順、QEP 見積もりは semi filter の選定のみに使う**。q17 2249ms→95ms (24 倍) を達成 |

**Phase D の教訓 (論文 discussion 用)**: プランの「材料 (見積もり)」だけ
借りて順序を自作すると、コストモデルが持つ結合選択率の知識 (M:N 回避) が
失われる。プランは構造ごと使うか、構造に触らない情報 (フィルタ配置) にだけ
使うかの二択が安全。

## Phase E (2026-07-03〜): プラン写像 + ルール積層への転換

ユーザー指摘 2 点による方向確定:
1. 「クエリ特徴への特化では?」— 正当。FROM 順前提・q13 専用認識・マジック
   ナンバー (8 倍則・4M) は TPC-H 駆動の暫定物
2. 「基本順序は QEP プラン通り、semi join 等の最適化は rule/module 化して
   適用可能なら適用 — row store 時代の pushdown と同じ構造に」— 採用

設計: AccessPath 木 (hash_join.join_predicate->expr に join 種別
INNER/SEMI/ANTI/LEFT・equijoin_conditions・非等値 join_conditions が全部
入っている) から join 構造を写像 → 自前 join tree 構築と sj nest 解析を
置き換え。semi filter 伝播は写像の上のルールとして残す。期待効果:
TPC-H 特化ヒューリスティックの大半 (FROM 順・Item_equal 展開・等値書き戻し
・nest mini-tree) がプラン写像に吸収されて消える。

### E1 実装記録 (2026-07-03)

| 試行 | 観測 | 判断 |
|---|---|---|
| AccessPath 写像の一発切替 | 初回ゲート 1/22。原因 3 連: (1) 旧経路の cross-join チェックが写像モードでも作動 (edges を作らないので常に空)、(2) req.Clear() が add_tables の後に走り登録消滅、(3) SF=0.01 では NLJ プランが出る (NESTED_LOOP_JOIN 未対応) | 順に修正。NLJ も JoinPredicate を持つので HASH_JOIN と同一写像 |
| 残 6 本 (WEEDOUT=type37, subquery materialization, keyless INNER) | q20/q21 は weedout 戦略、q4/q22 は leaf にない MATERIALIZE、q19 は OR 内の equi キーが equijoin_conditions に正規化されず keyless cross 化 → 64M cap | **写像失敗→構文ビルダーへリトライ** (最終 reject は loud のまま)。q19 は「keyless INNER over base tables」を写像側で reject し構文側の OR factor-out が受ける |
| 結果 | 22/22 MATCH。join 構造はプラン由来が第一経路、TPC-H 特化ヒューリスティックは第二経路に降格 | E2 で semi filter ルールを写像経路にも配線 (現在は構文経路のみ) |

### E1 SF=1 計測と「プラン写像の罠」(2026-07-03)

| 構成 | FORCED 合計 | 特記 |
|---|---|---|
| C3 (構文+FROM順) | 22.1s | |
| D3 (semi filter, 構文経路) | 21.0s | q17 95ms |
| **E1 (プラン写像第一)** | **27.0s** | **q5 231ms→4.7s、q9 305ms→1.0s に退行** |

**観測**: 写像は正しさ完璧 (22/22 md5) だが、q5/q9 で退行。原因は
**「MySQL のプランは NLJ+インデックス前提のコストで選ばれている」**こと。
NLJ なら安い順序 (例: M:N 辺を先に置いてもインデックスで引くだけ) を、
全 hash join の我々の実行に写すと中間結果が爆発する。オプティマイザと
実行エンジンが同じコスト前提を共有する DuckDB 型のバンドルでは起きない、
**「借り物オプティマイザ」構成に固有の問題**。HeatWave が
secondary_engine_modify_access_path_cost フック (プラン候補ごとに
secondary 向けコストを注入し、オプティマイザに hash 実行前提のプランを
選ばせる) を持つ理由の実証になった。

**次の選択肢**:
- E2: semi filter の写像経路への配線 (q17 回復、設計済み)
- E3: コストフック実装 — secondary 実行 (全 hash join+PAX scan) のコスト
  モデルをオプティマイザに返し、プラン自体を secondary 向けに最適化させる
  (HeatWave 方式の完成形)。q5/q9 退行の根本解
- 暫定: プラン写像の適用条件を絞る (退行検出時は構文経路優先) — 検証用

### E2+E3 初回計測 (2026-07-03)

| 試行 | 観測 | 状態 |
|---|---|---|
| E3 コストフック (NLJ/index reject + hash/scan 線形価格) | 22/22 MATCH 維持。q22 431→77ms。**しかし q5 3.96s / q9 0.96s は残存** — プラン順序が変わっていないように見える | 要診断: フックが実際に呼ばれているか (fprintf 確認)、reject 後に hypergraph が代替順序を探索できているか、hash 線形モデルでも M:N 先行が最安と評価されていないか |
| E2 semi rule on mapped plan | **q17 3.06s — 不発** (D3 構文経路では 95ms) | 要診断: 発火条件 `req.nodes(build).has_sub_block()` は build が MATERIALIZE 直下の場合のみ真。プランで FILTER が sub_block の上に挟まると不発 (有力)。build 側の中間ノードを透過して sub_block を探す必要 |
| 合計 | 26.6s (D3=21.0s が依然最良) | 正しさは全構成で維持。性能デバッグは E4 として継続 |

### E4 診断 (2026-07-03, SF=1 プランダンプ)

| 対象 | 観測 | 対策 |
|---|---|---|
| q17 semi 不発 | プランは (lineitem⋈derived)⋈part — E2 は発火するが semi source が lineitem(6M) で絞り込み効果ゼロ。D3 が速かったのは source=part(20行) だったから。derived の rows=0 見積もりがこの順序を作っている | sub_block の semi source を「join の直接相手」でなく「group キーと equi-join される最小の実表 scan」に。scan を先行発行 (records 昇順) して任意 scan を source にできるノード順を確保 |
| q5 3.96s | プランの中間 join 見積もりが rows=13.7e9 / 82.5e15 と大破綻。それでも hash 線形コスト (入力行数のみ) では M:N 順が最安評価 | HASH_JOIN コストに出力行数項 (+0.01*output_rows) を追加 — M:N 爆発プランが正しく高コスト化される |

### E4 結果 (2026-07-03)

| 修正 | 結果 |
|---|---|
| semi source 後処理 (scan 先行発行 + 「group キーと equi-join する最小実表 scan」選定) | **q17 3.06s→97ms** — 写像経路で D3 相当を回復。q22 119ms |
| hash join コストに出力行数項 | q5 4.0s 未回復。仮説: NLJ/index の全 reject で探索空間が痩せ、生き残る唯一の prefix が M:N 順 — reject でなく高価格化 (探索柔軟性を残す) が次の一手 |
| 合計 | 24.6s (D3 21.0s, 残差は q5 4.0s + q21 9.6s)。q21 は LEFT derived への実行時フィルタ注入 (構造課題) |

### E5: reject→高価格化 実験と「旧オプティマイザ」の発見 (2026-07-03)

| 試行 | 観測 | 判断 |
|---|---|---|
| コストフックの NLJ/index 系を reject(return true) から有限ペナルティ (100x+1) に変更 | ゲート 22/22 PASS、SF=1 合計 24.5s — **q5 4.15s 変化なし。仮説棄却** | 変更自体は維持 (原理的に安全な形) |
| q5/q21 の EXPLAIN 精査 | `orders⋈customer` 見積 13.7e9 行、`l1⋈l2 (orderkey)` 32.4e9 行 (真値 24M)、q21 全体 24.3e18 行 — **FK join の等値選択率が既定値に落ちている** | 探索でなく見積もりの問題と断定 |
| optimizer trace 取得 | **トレースは旧オプティマイザ形式** (`considered_execution_plans` / `condition_filtering_pct: 10` / weedout 戦略)。`SET optimizer_switch='hypergraph_optimizer=on'` → 「not supported in non-debug builds」 | **重大な訂正: このビルドは hypergraph 非対応。secondary 経路は常に旧 (System-R 型) オプティマイザで計画されていた** |

**訂正が波及する過去の記述 (論文用に重要)**:
1. Phase B の「hypergraph optimizer が使われ旧構造は未構築」→ 誤り。best_positions nil の理由は別 (要再調査だが、旧 optimizer が AccessPath 木も生成するため写像自体は有効)
2. E3 の `secondary_engine_modify_access_path_cost` フックは **hypergraph 専用 = このビルドでは一度も呼ばれていない死にコード**。E3/E5 の「効果」とされた q22 改善は実際には同時に入れた E2 (semi rule) のもの
3. 「プラン写像の罠 = NLJ 前提コストのプランを hash 実行に写すと爆発」という E1 の解釈 → 真因はより単純: **secondary TABLE インスタンスに rec_per_key が無く、全等値 join の選択率が盲目既定値 (0.1 / 1e-4) に落ち、FK join が M:N 爆発として見積もられる**。旧 optimizer は「爆発の小さい方」として本物の M:N 辺を先に選んでいた
4. 「HeatWave がコストフックを持つ理由の実証」→ このビルドでは検証不能 (フック不発)。主張は「借り物オプティマイザには secondary 向け統計の供給が必要」に差し替え — こちらの方が一般的で強い主張

### E6 バンドル (2026-07-03, 実装中)

| # | 変更 | 狙い |
|---|---|---|
| 1 | `info(HA_STATUS_CONST)` で primary TABLE (thd->open_tables から handler 一致で回収) の rec_per_key を secondary TABLE へ転写 | 等値選択率の根本修正 = q5/q21/q18/q20 のプラン正常化。「stats 同期」の完成 |
| 2 | syntax 経路の scan 発行順を records 昇順に (join tree は FROM 順のまま) | q21 (weedout→syntax fallback) で nation→supplier→l1→derived の semi 連鎖を形成 |
| 3 | 写像経路の semi 後処理を一般化: (a) join 型/側の安全性規則 (build 側=常に可、probe 側=INNER/SEMI のみ — LEFT/ANTI の probe 側注入は行落ち=Codex#1 と独立に同定した潜在バグ)、(b) 対象に実表 scan も追加 (scan 連鎖)、(c) source 候補に「join 相手サブツリーの実行済み root ノード」を追加 | q21 系 (derived が joined 中間のキー集合を受ける)、safety 修正 |
| 4 | Codex レビュー対応: FILTER 単一表 conjunct はポインタ一致時のみ skip (#2)、residual の表スコープ検査→写像 reject (#4)、NLJ は `join_type` を正、hash の `rewrite_semi_to_inner` は reject (#3) | 正しさ (wrong-results 級 2 件を先回りで封鎖) |

### E6 結果 (2026-07-03, SF=1 FORCED, 全 md5 MATCH)

| 指標 | E5 | E6 | 要因 |
|---|---|---|---|
| 合計 (min-of-3) | 24.5s | **19.6s (歴代最良、D3 21.0s 超え)** | scan semi 連鎖 |
| q2 | 273ms | **39ms** | region→nation→supplier 連鎖 |
| q7 | 829ms | **258ms** | nation 起点連鎖 |
| q9 | 980ms | **332ms** | 同上 |
| q5 | 4153ms | **2151ms** | scan 入力半減 (M:N 中間は残存) |
| q20 | 2063ms | **850ms** | 連鎖+derived semi |
| q22 | 117ms | **76ms** | |
| q21 | 9566ms | 9188ms (フラット) | weedout→syntax、vt semi 不発のまま |
| q18 | ~3.3s | フラット | derived agg (キーが agg 後に決まる形) |

**E6 の負の結果 2 件 (論文の重要観測)**:
1. rec_per_key 転写もヒストグラム (ANALYZE UPDATE HISTOGRAM + FLUSH TABLES) も**見積もりを 1 ミリも動かさなかった** (probe join 依然 900e9 行)。ソース読解で確定: 旧 optimizer の `Item_func_eq::get_filtering_effect` は field=field に対し (a) ヒストグラムは field=const 専用で不発、(b) フォールバックは `max(1/NDV, COND_FILTER_EQUALITY=0.1)` — **等値 join の選択率は 0.1 でハードフロア**。rec_per_key は REF アクセスのコストにしか効かず、index を持たない (scan+hash のみの) secondary エンジンには届かない
2. 教訓の一般化: 「借り物オプティマイザには統計を貸すだけでは足りず、**統計が効くアクセスパス (REF) ごと貸す**か、プラン品質は実行時ルール (sideways information passing) で取り返すかの二択」。HeatWave が cost フックだけでなく index metadata を secondary に保持する理由とも整合

### E7 (2026-07-03, 実装済み・検証中)

| # | 変更 | 狙い |
|---|---|---|
| 1 | **WEEDOUT→SEMI 写像**: WEEDOUT(FILTER*(INNER JOIN)) で dedup rowid 集合が join の片側と一致する形を hash SEMI (残差=中間 FILTER+非等値) に書き換え | q21/q20 の写像経路化 — semi ルール群 (scan 連鎖・derived 注入) がq21に届く |
| 2 | keyless INNER の緩和: 片側の見積もりが極小 (≤100 行) なら許可 (q21 の nation×weedout クロス)。q19 型 (両側巨大) は据え置き reject | q21 が keyless ガードで syntax に落ちるのを防ぐ |
| 3 | Claude レビュー F1-F5: 複数 derived の vt 番号不一致→emission 順序検査 (Critical, TPC-H 未発火の潜在 wrong-results)、`<=>` を等値キーとして写像しない、非 root の SORT/LIMIT/AGGREGATE を透過しない (root spine 検査)、HAVING conjunct はポインタ一致で skip (agg.having と二重化しない)、cost フックの負値ガード | 正しさ |
| 4 | 回帰テスト 2 本をゲートに追加 (2-derived 逆順 join / `<=>` NULL join) | レビュー提案 |

E7 の安全条件 (レビュー追認 2 点): (a) weedout→SEMI は dedup 表集合が join
片側と**完全一致**の場合のみ (subset は reject)。消される側への下流参照は
node_tabs 非包含で loud reject に落ちる。(b) keyless INNER の ≤100 行緩和は
見積もり由来 — 過大なら reject (syntax へ、安全側)、過小でも server の 64M
中間キャップが上限 (perf 事故止まり、wrong results にはならない)。

### E7 結果 (2026-07-03, SF=1)

ゲート 22/22 + 回帰 2 本 (two-derived MATCH / `<=>` loud reject) PASS、
SF=1 hot 5 本 md5 MATCH、合計 19.78s (E6 19.6s とフラット)。
**q21 は 9.6s のまま — weedout→SEMI 写像は着地したが q21 は依然 reject**。
プラン精査で真因特定: MySQL の等値伝播が LEFT join キーを
`derived.col = l2.l_orderkey` と **SEMI 化で消えた l2 の列**で表現しており、
サイド解決 (node_tabs 包含) が失敗して "plan join key sides" → syntax 落ち。
orders の join キー (`o_orderkey = l2.l_orderkey`) も同型。

### E7b: 強制済みキー等価による書き換え (2026-07-03)

サイド解決を一般化: **既発行の INNER/SEMI join キーペアを等価辺として蓄積**し、
キー端点の表が要求サイドに無い場合は等価クラス内の「サイドに居る」列へ置換。
健全性: INNER/SEMI のキーは生存行すべてで等値が強制済み (LEFT は null 拡張行、
ANTI は非一致行で不成立のため**辺に加えない**)。weedout→SEMI で消えた l2 への
参照が、SEMI 自身が強制した l1.ok=l2.ok を通じて l1 に書き戻る。
期待: q21 が写像経路に乗り、scan 連鎖 (nation→supplier→l1→l2) +
derived への 15 万キー注入で 9.6s → ~1s 圏。

| 試行 | 観測 | 判断 |
|---|---|---|
| E7b 初版 (サイド解決を常に等価クラス経由に) | ゲートで q10 MISMATCH — pick() が「元の端点が有効でも」クラスの別列を返し得るため join 出力の行順が変わり、q10 既知の ORDER BY 同値タイ+LIMIT 20 境界が発火 | 置換を**両向き失敗時のフォールバックに限定** (既写像クエリの挙動変更ゼロを保証)。ゲートに MISMATCH 時の diff+「same rows, order differs」判定を常設 |

### E7c 結果 (2026-07-03, SF=1 FORCED, ゲート 22/22 + 回帰 2 + hot md5 全 MATCH)

| 指標 | E6/E7 | E7c | 要因 |
|---|---|---|---|
| **合計 (min-of-3)** | 19.6s | **12.0s** | q21 の写像経路化 |
| q21 | 9,601ms | **1,457ms (6.6x)** | weedout→SEMI + E7b 書き換え + scan 連鎖 + derived 注入 |
| q20 | 835ms | 999ms | 同経路 (微増はノイズ域) |
| q5 | 2,099ms | 2,154ms | 未解決 (M:N 順序、REF 貸与が本命) |
| q18 | 2,922ms | 2,930ms | 未解決 (derived agg、キーが agg 後に決まる) |

**歴代**: C3 22.1s → D3 21.0s → E1 27.0s → E4 24.6s → E6 19.6s → **E7c 12.0s**
(InnoDB champion 42.2s の 3.5 倍、Phase D 目標 ~13s を達成)。
残る主要打者: q18 2.9s (サーバ集計の 1.5M group アロケーション疑い — perf へ) と
q5 2.2s (プラン順序)。

### E8: perf 駆動のサーバ側チューニング (2026-07-03)

perf 実測 (SF=1): q20 は `set_row_from_pax` (フィルタ評価用の全 16 列ロード) が
**37%**、q18 は string キー hash map (GroupState find + semi set probe +
hash/memcmp) が合計 **~31%**。

| 施策 | 結果 | 教訓 |
|---|---|---|
| フィルタが参照する列だけロード (`set_row_from_pax_cols` + collect_columns) | 全フィルタ付き scan が改善: q6 111→78ms, q14 165→111ms, q15 302→196ms, q12 132→96ms, q7 309→227ms | 幅広表の行全載せがフィルタ scan の支配項だった |
| semi/ext キー probe の一時 string 再利用 (per-row alloc 排除) | q20 999→912ms, q21 1457→1347ms | C++17 の unordered_set は heterogeneous lookup 不可 — assign 再利用で十分 |
| MergeGroups 内で毎ステップ reserve | **q18 2.93→4.95s に退行** — マージ 31 回それぞれが成長サイズへ rehash | reserve は「最初の move 後に合計サイズで 1 回」(E8b) — q18 2.93→**2.38s** |

**E8b 合計: 11.0s** (E7c 12.0s → -1.0s、全 md5 MATCH)。InnoDB champion 42.2s
の **3.8 倍**。残: q18 2.38s (hash map 本体 — compact GroupState + int64 キー
特化が次)、q5 2.24s (REF アクセス貸与)、q4 693ms、q13 498ms。

**TPC-C 回帰 (E8 サーバ変更後)**: 1/8/32/64t = 426.5/1546.3/3646.5/4892.0
req/s、Unexpected Errors 全点 0 — 導入前 (432.7/1543.9/3675.1/5098.1) と同等。
E8 の変更は TX_EXECUTE_QUERY_BLOCK 経路のみで OLTP 経路に触れない (構造どおり)。

### E9: secondary 統計によるプラン再ランク (2026-07-03, 検証中)

q5 の残り 2.2s は M:N 先行の join 順そのもの。REF アクセス貸与 (index_flags
開放 + REF/EQ_REF 葉の写像) は全 22 本のプラン形状を变える big-bang なので後回し
にし、**旧 optimizer の `compare_secondary_engine_cost` フック** (完全 join 順
ごとに呼ばれる — トレースの `secondary_engine_cost` で実証済み) で再ランク:
- 各 position の実効行数 = rows_fetched × filter_effect (optimizer 見積もり) に
  「prefix で束縛される各等値」の補正 (rec_per_key 選択率 / 0.1) を掛ける —
  0.1 フロアだけを実統計に差し替える外科的補正
- コスト = 全中間結果の体積和。等値の同定は keyuse_array (leading keypart のみ、
  (列, 束縛式) で重複排除)。rec_per_key は info() が primary から転写済みのもの
- **転写した rec_per_key が初めて load-bearing になる** — 「借り物オプティマイザ
  には統計を貸すだけでは不足」(E6) の续き: 統計が効く場所 (完全プラン比較) を
  フック側に自作した

### E9 結果と決定的バグの発見 (2026-07-03)

| 試行 | 観測 | 判断 |
|---|---|---|
| E9 (再ランクフック) | 合計 10.8s (q8 268→164 で E7c 退行回復、q20 757、q21 1193)。**q5 は 2.2s のまま** | q5 の trace で `secondary_engine_cost` が **1 回しか出ない** — 探索の境界枝刈り (0.1 フロアの prefix_cost ≥ best_read) が FK-first 順を完走前に刈り、フックに代替案が届かない。prune_level=0 でも境界枝刈りは残るため無効と判断 |
| ソース精読 | `Item_equal::get_filtering_effect` は**元々 rec_per_key を使う設計** (key_start + has_records_per_key → rpk/rows)。等値伝播で WHERE 等値は Item_equal 化される — つまり **rec_per_key 転写さえ効いていれば旧 optimizer は正しく見積もれるはず** | probe 再実行 → **依然 900e9 = 転写が一度も発火していない**ことが判明 |
| 転写バグの特定 | `ha_set_primary_handler` は **open_table の後**に呼ばれる (sql_base.cc:6712) — open 時の info(HA_STATUS_CONST) では primary handler が nullptr で copy が空振り | **HA_STATUS_CONST ゲートを外し、後続の (VARIABLE) 呼び出しで転写** (E9b) |

### E9b 結果 (2026-07-03, SF=1, 全 md5 MATCH): **q5 2154→216ms (10 倍)**

統計転写の発火で旧 optimizer が FK-first 順を選択。合計 **10.39s**。
ただしプラン変化の副作用: **q9 335→1431ms** — 新プランで lineitem の scan-semi
源候補 partsupp (raw 800k 行) が「6M/8=750k」の比率規則に僅差で弾かれ、6M 行
中間結果が発生 (partsupp 自身は part の green キーで 43k 行まで縮むのに、
判定が縮む前のサイズを見ていた)。q3/q7/q18/q20 も 100-300ms 級の微退行。

### E10: 有効カーディナリティの連鎖 (2026-07-03)

scan semi の割当を発行順に処理し、割当のたびに
`eff[t] = min(eff[t], eff[source] × rec_per_key(t, filtered_col))` を伝播。
比率判定と源ランキングは raw 行数でなく eff を見る (LIP のカスケードを見積もり
にも適用)。誤見積もり時の保険としてサーバ scan semi にも 4M キーガードを追加
(sub_block 側と同じ)。

結果: 合計 10.71s — **q9 は 1616ms で未回復** (q20 -104ms, q21 +188ms は
ノイズ域)。q9 の scan 連鎖は理屈上 ~400ms 圏になるはずで、悪化の主因は
join 順序側と再判断。

### E11: E9 再ランクフックの引退 (2026-07-03, 検証中)

**E9 の自己矛盾に気づく**: E9 の補正 (sel/0.1) は「0.1 フロア前提」の設計だが、
E9b で rec_per_key が探索自体に効くようになった後は**フロアの無い見積もりに
二重適用**され、順位付けを壊す (q9 悪化の有力原因)。フックをパススルーに戻し、
「統計同期だけで旧 optimizer のプランが直る」構成を検証。教訓:
**補正レイヤは補正対象のバグが直った瞬間に自分がバグになる** — 根本 (統計) を
直したら上物 (補正) は撤去する。

### E11 結果 (2026-07-03, ゲート 22/22 + 回帰 2 + hot md5 全 MATCH)

**合計 8.97s** (InnoDB champion 42.2s の **4.7 倍**)。読みが的中:
q9 1616→**269ms** (歴代最良、E9 の二重補正が真犯人)、q5 216→**142ms**
(素の統計ベースコストがさらに良い順序を選択)、q8 225ms。

**本日の推移**: E4 24.6s → E6 19.6s → E7c 12.0s → E8b 11.0s → **E11 8.97s**
(2.7 倍)。決定打は「統計同期の完成 (E9b の 1 行: 転写を CONST ゲートから外す)
+ 補正レイヤの撤去 (E11)」— 旧 optimizer は正しい統計さえ与えれば TPC-H の
join 順を自力で当てる (Item_equal の rec_per_key 経路)。
残: q18 2.41s (集計 hash map — compact GroupState/int64 キーが未着手の最終
レバー)、q21 1.35s、q4 697ms、q20 757ms。

### E12: compact GroupState (2026-07-03)

集計の GroupState を「フィールドごとに 1 vector (計 5 本)」から「aggregate
ごとに 1 スロット (AggSlot: count/acc/sval/has/dset を連続配置) の 1 vector」
に変更 — 群あたりのヒープ確保 7→3 ブロック + 局所性改善。
結果 (SF=1, 全 md5 MATCH): **q18 2413→1903ms (-21%)**、q13 441ms、q20 655ms。
**合計 8.04s = InnoDB champion 42.2s の 5.2 倍。本日: 24.6s → 8.04s (3.1 倍)**。
残: q18 1.9s (次は int64 キー特化 or radix 並列マージ)、q21 1.29s、q4 694ms。

### E13: perf/RSS 実測と実行時型判定キー (2026-07-03)

perf 再計測 (q18/q21/q20/q4): 4 本共通の支配項は**文字列キーの hash 処理** —
q21 は semi キー probe (unordered_set<string>::find 15.4% + hash 4.4% +
memcmp 5% + 一時 string 2.7%)、q4 は join hash 表 probe (16%+)、q18 は
group map find 10.6% + semi 5.6%。**RSS は健全**: baseline 10.4GB (SF=1 PAX
常駐) に対しクエリ中の増分 ≤30MB — late materialization (NodeResult=row-ref
のみ) で中間表現は爆発していない。

E13 = **実行時型判定の int64 キー特化** (server のみ、proto/proxy 変更なし):
「キー集合の全要素が int64 として完全パース (from_chars, 全長消費) できれば
int セット/int マップへ切替。1 つでも失敗 (DECIMAL/空/オーバーフロー) したら
従来のバイト比較経路」。canonical INT cell は値↔バイト表現が 1:1 なので
値等価 = バイト等価が保存される (意味論不変の証明つき置換)。対象:
(a) scan の semi/ext キー集合、(b) RunJoin の単一列キー hash 表。
SOTA 系譜: DuckDB/Umbra の typed hash keys の縮小版 —
本命の「型付きセル (SIMD の受け皿)」への中間段。

E13 結果: **合計 7.77s** (InnoDB の 5.4 倍)。q4 642→360ms、q13 441→290ms、
q21 1181ms、q9 222ms。q8/q10/q18 の +50-220ms は実行分散圏
(q18 は 1.9-2.4s で揺れる)。dual review: Codex は E8-E12 に
「correctness バグなし」(collect_columns 再帰網羅・AggSlot 残置なし・
info() 同一性・eff-chain 型安全の 4 点検証済み)。

### E14: NULL join 意味論の統一 (2026-07-03, review I1)

Claude review (E8-E12) の Important 1 件: RunJoin の byte キーが空セル (NULL)
同士をマッチさせ、semi filter (NULL を drop) と乖離 — semi の適用有無 (4M
ガード/eff 連鎖) で同一クエリの結果が変わり得た (TPC-H は全キー NOT NULL で
不可視)。build/probe 両側で空キー成分の行をスキップ = MySQL の null-rejecting
hash join と同一意味論に統一。回帰 regr-null-eq-join (NULL 入り =join,
期待 2) を常設 — MATCH。合計 7.74s 維持。Codex + Claude の dual review で
E8-E13 の指摘は全消化。

### E15: int64 グループキー特化 (2026-07-03)

E13 の perf 所見で残った最後の支配項 = 集計 GroupMap の
`unordered_map<string, GroupState>` (長さ接頭辞付きバイトキー)。q18 の
find+hash+memcmp が ~19%。E13 の join キー int64 化と同じ実行時型判定を
**グループキー**へ適用: 集計が (a) グループ列が 1 本、(b) prefix_len==0、
(c) 全キーセルが `from_chars` 完全長で int64 パース可 のとき
`unordered_map<int64_t, GroupState>` へ蓄積。1 つでも非 int (DECIMAL/空/
非正準文字列) があれば atomic parse_fail を立てて false を返し、集計全体を
文字列キー経路で再実行 (非 INT 列でしか起きないので追加スキャンは稀)。
canonical INT セルは値↔バイト 1:1 なので意味論不変 (E13 と同じ論拠)。

実装: `AccumulateRange` を `AccumulateRangeT<MapT>` にテンプレート化
(`if constexpr (IntKey)` でキー計算だけ分岐、集計本体は共有)、`MergeGroups`
も同テンプレート化、`GroupState.key_cols` は従来どおり文字列で埋めるので
HAVING/second-stage/出力は不変 — ただし後段全体 (HAVING フィルタ・q13 型
二段集計・出力 emit・ORDER/LIMIT) を `emit(auto& groups)` の generic lambda に
くくり出し両マップで呼べるように。並列ワーカーの locals は int パス時
IntGroupMap 型、共有 atomic の parse_fail で文字列経路再実行をトリガ。
構造エラー (fail()) は parse_fail と区別して伝播。

| 指標 | E14 | E15 | 要因 |
|---|---|---|---|
| **合計 (min-of-3)** | 7.74s | **6.88s** | int64 group map |
| q18 | 2054ms | **1459ms (-29%)** | o_orderkey/l_orderkey が INT |
| q13 | 307ms | **228ms (-26%)** | c_custkey が INT (二段集計) |
| q16 | 129ms | 129ms | p_brand/type/size は文字列 → 文字列経路 (設計どおり) |
| q20 | 633ms | 640ms | ノイズ域 |

ゲート 22/22 MATCH + 回帰 3 本 (two-derived MATCH / nullsafe-eq REJECT /
null-eq-join MATCH) + SF=1 hot-5 md5 全 MATCH。SF=1 の 22 本 md5 は E14 と
**全一致** (same=22 diff=0) — 置換の意味論不変を実証。InnoDB champion 42.2s の
**6.1 倍**、本日 24.6→6.88s (3.6 倍)。残: q21 1.17s、q20 640ms、q10 495ms。

### E16: semi キー集合を型付きで直接収集 (2026-07-03)

perf 再計測 (q21/q20/q10): semi-join のキー集合構築が支配項の一つ —
`collect_keys` が `unordered_set<std::string>` を組み (q21 5.35% + string
hashtable 1.51% + 一時 string 確保)、その後 `to_int_set` が全要素を
int64 へ**再パース** (q21 1.78% + from_chars 7.81%)。同じ二段が q10 でも
collect_keys 6.05% + to_int_set 2.41% を占めた。sideways information
passing (semi filter) を多用する q5/q7/q8 も同じ税を払っていた。

E16 = **型付き直接収集** `collect_keys_typed`: semi source 列を最初から
int64 セットへ 1 パスで積む。非 NULL・非空セルを 1 度だけ `parse_i64`
(full-length `from_chars`) し、最初に変換失敗したセル (DECIMAL/非正準) が
出た時だけ owned-string セットへ再走査フォールバック (非 INT キー列でのみ
発生・稀)。中間 string セット + `to_int_set` 再パスが丸ごと消える。canonical
INT セルは値↔バイト 1:1 (E13/E15 と同じ不変条件) なので probe 意味論は
byte-for-byte 不変。RunScan は `semi_set!=nullptr` 判定を
`semi_active`/`semi_int` フラグに分離 (int パスは string セットを持たない)。

| 指標 | E15 | E16 | 要因 |
|---|---|---|---|
| **合計 (min-of-3)** | 6.88s | **6.25s (-9.2%)** | semi キーの型付き直接収集 |
| q21 | 1168ms | **901ms (-23%)** | l_orderkey/l_suppkey 集合 |
| q10 | 495ms | **341ms (-31%)** | o_orderkey semi + LEFT group |
| q8 | 246ms | **133ms (-46%)** | 多段 semi 連鎖 |
| q7 | 323ms | **223ms (-31%)** | 多段 semi 連鎖 |
| q5 | 160ms | **106ms (-34%)** | 多段 semi 連鎖 |
| q20 | 640ms | 668ms | ノイズ域 (sub_block ext は未変更) |

ゲート 22/22 MATCH + 回帰 3 本 (two-derived MATCH / nullsafe-eq REJECT /
null-eq-join MATCH) + SF=1 hot-5 md5 全 MATCH。SF=1 の 22 本 md5 は E15 と
**全一致** — 意味論不変を実証。InnoDB champion 42.2s の **6.75 倍**。
**Codex review**: 4M サイズガードが旧 `semi_keys.size()`(distinct string)
→ 新 `semi_ikeys.size()`(distinct int64) になった点を指摘。実害なし —
(1) semi filter は LIP の冗長 pre-filter で、適用/スキップは純粋に性能選択
(後段 equijoin が同じ行を落とすので結果不変)、(2) canonical INT は
値↔バイト全単射で distinct int 数 = distinct string 数 (崩壊しない)、
(3) md5 全一致で実証。SF=1 では最大 semi source ≤1.5M で 4M 境界に届かない。
残: q18 1.5-1.7s (集計)、q20 640ms (sub_block ext の型付き化が残レバー)、
q21 901ms。

### E16b: 2 INT グループ列のパック型キー (2026-07-03)

q20 の call-graph (E16 上): derived サブブロックの
`GROUP BY l_partkey, l_suppkey` (両 INT) が**文字列 GroupMap** 経路 —
Hashtable ~12.75% + `_Hash_bytes` 1.83% + key_cols push_back 2.08% +
GroupState vector 1.95% + `_M_replace` 1.42% ≈ **21%**。E15 の int64 高速
経路は単一グループ列 (`n_grp==1`) 限定でここに効かなかった。

E16b = **2 INT 列を POD 128bit キー (`Int2Key{int64 a,b}` + カスタム
hash) にパック**する E15 の 2 列版。E15 の accumulate + 並列マージ
dispatch を汎用 lambda `run_typed` に括り出し (0=成功/1=parse fallback/
2=構造エラー)、単一 int (E15) と 2 int (E16b) の両 dispatch で共有。perf
で `_Hashtable<Int2Key>` が有効化を実証 (文字列 map シンボルは消失)。

**Codex review 対応 (correctness hardening)**: 高速経路のゲートを
`prefix_len==0` に加え **`cmp_kind()==0` (数値 result type)** へ厳格化。
STRING 列は非正準な数値表記 ("01" vs "1") を持ち得てバイトでは別グループ
だが int64 では衝突する — 数値列は canonical val_str なので parse 成功
= 値↔バイト 1:1。proxy は全 group column に cmp_kind を設定 (STRING→1/
数値→0) するので、STRING 列は確実に int 経路から除外される。単一 int
(E15) 経路にも同ゲートを遡及適用 (整合性 + 厳密化)。Codex 最終 review:
correctness バグなし・grp0/grp1 は n_grp≥1/≥2 で範囲保護・run_typed は
E15 と挙動一致を確認。

| 指標 | E16 | E16b | 要因 |
|---|---|---|---|
| q20 | 668ms | **569ms (-15%)** | (l_partkey,l_suppkey) パックキー |
| q13 | 228ms | 233ms | 単一 int 経路維持 (cmp_kind ゲート後も) |
| q18 | 1567ms | 1508ms | 単一 int 経路維持 |

SF=1 の 22 本 md5 は **E16 と全一致** (semantics 保存を実証) + 回帰 3 本
(two-derived MATCH / nullsafe-eq REJECT / null-eq-join MATCH) + FORCED-vs-
OFF (q20/q18/q13/q1) MATCH。**罠**: 検証中に primary (LineairDB 行エンジン)
が q5/q8 の重 join で稀に**ハング**し gate の `timeout 90` が空結果 → 偽
MISMATCH を出す (secondary 経路と直交・前段の foreground timeout 巻き添え
が誘発)。対策として semantics 検証は **md5-vs-E16 比較** (E16 は primary
検証済み) に切替 — flake-proof。残: q18 1.5s、q21 0.9s、q10 0.37s。


### E17: ゾーンマップ (strip min/max プルーニング) — 1-copy 設計と TPC-H の非クラスタ性 (2026-07-03)

**設計判断: sorted projection ではなく zone map (side info)。** 日付範囲述語を
速くする古典手法は (a) 該当列でソートした second copy か (b) strip ごとの min/max
補助情報。(a) は OLTP 書込パスが維持すべき **2 つ目の copy** になり 1-copy 原則に
反する (scatter のたびにソート維持 = 書込増幅)。(b) を採用: PaxGroup (8192 行) ごとに
列の min/max を持つだけ (2×int64/strip/列、lineitem SF=1 で ~733 strip × 数列 = 数十 KB)。

**遅延・write_counter キーのキャッシュ (OLTP 書込パス不可侵)。** min/max は **最初の
scan 時に遅延構築**し、プロセス wide な (PaxStore*, 列) キャッシュに格納。strip ごとに
**write_counter スナップショットをキー**にし、counter が変われば再構築。write_counter は
変更の開始と終了で bump され単調増加なので counter 不変 ⟺ strip のセル不変。stale/torn
zone が誤プルーニングを起こし得るのは counter が違うときだけで、その差分は同じ executor の
Prepare/Quiesced 検査を失敗させ結果を破棄する — つまり **プルーニングはクエリ結果を一切
変えない** (既存の静止検査が correctness backstop)。書込パス (ScatterRow) には一切触れない
= 1-copy 維持。scope は server/rpc のみ (新規 zone_map.{hh,cc} + RunScan)。

**byte-path 等価の compare_type ゲート。** byte 経路は compare_type で比較モードを選ぶ
(0/1 = strtoll/strtoull 数値、2 = strtod double、3 = 文字列)。数値 zone は byte 経路も
数値比較するときだけ使う (INT↔ct{0,1}、DECIMAL↔ct2、DATE↔ct3)。数値文字列を持つ VARCHAR
列 (ct3、文字列順 ≠ 数値順) は数値 zone で絶対にプルーニングしない — これが over-prune
防止の要 (回帰 regr-zone で検証: `v > '5'` は文字列比較のまま MATCH)。

**型付け規則 (E13/SIMD spike の classify を保守的に再利用)。** INT (full parse)、
DATE (YYYY-MM-DD→YYYYMMDD、int 順 = 固定長文字列順)、DECIMAL (一様 scale、scaled int)。
strip 内で 1 つでも型不一致/非数値セルがあれば当該 strip は非プルーニング。DECIMAL は
byte 経路の strtod double 意味論に合わせ、spike の dec_lower/dec_upper (正確な整数境界) を
**±1 拡幅** (widen) して保守側に倒す (double 丸めに対して over-prune を不可能に)。NULL/空
セルは min/max から除外 (NULL cmp X → byte 経路も除外なので安全)。

**プルーニング判定。** filter の top-level AND 各 conjunct (col CMP const / BETWEEN /
col CMP col) を strip の [min,max] 範囲に対し評価。**いずれか 1 つの conjunct が「この
strip に一致行なし」を証明したら strip 全体を skip** (AND なので 1 つ空なら全体空)。
col-vs-col (q12/q4 の l_receiptdate > l_commitdate 等) は同 kind/scale の両 zone が
あるとき max(lhs) ≤ min(rhs) 等でプルーニング。判定は request-local snapshot (不変) に
対し並列 scan worker が lock なしで読む。env `LDBC_ZONEMAP=0` で無効化可 (A/B 用)。

| 指標 | E16b | E17 |
|---|---|---|
| SF=1 FORCED 合計 (min-of-3) | 6.10s | **6.07s (flat)** |
| SF=1 全 22 本 md5 | — | **E16b と byte-identical (same=22 diff=0)** |

**決定的な負の結果: TPC-H は日付列でクラスタされていない。** 期待した q6/q1/q12/q14/
q4/q20 の日付範囲プルーニングは **1 strip も発火しなかった**。実測で確認: orderkey の
低位窓 (1..8192) と高位窓 (3M..3M+8192) の o_orderdate は**両方とも 1992-01-01 〜
1998-08-02 の全域**を張る (l_shipdate も 1992-01 〜 1998-11 で同様)。TPC-H の
o_orderdate は order ごとに一様乱数で orderkey と無相関なので、どの 8192 行 strip の
日付 zone も全 date domain を覆い、日付範囲述語はどの strip とも overlap する →
プルーニング 0。**クラスタされている列 (orderkey = ロード順) はどのクエリも範囲述語で
filter しない** (semi/equi-join キーとしてのみ使われ、これは semi filter 経路が担当) ため、
TPC-H では zone map が原理的に効かない。

**機構は正しく動く (クラスタ列で検証)。** l_orderkey (ロード順 = クラスタ) への範囲
述語でプルーニング発火を実証 — `l_orderkey < 60000` (0.25% 選択, クラスタ) は **9ms** で
~99% の strip を skip、対して同形の非クラスタ列 `l_quantity < 2` (~2% 選択) は **39ms** で
全 strip 走査 (無 filter の 45ms とほぼ同じ)。zone map は「filter 列が物理的にクラスタ
されていれば効く」— append-ordered な time-series や TPC-C の order 表では成立するが、
TPC-H の乱数日付では成立しない、という**データの性質の話であって機構の欠陥ではない**。

**Dual review (Codex + code-reviewer) 完了・全指摘対応済み。** 両者とも「TPC-H 到達
可能な wrong-result バグ無し・1-copy 制約遵守・スレッド安全・OOB 無し」で一致。対応
した指摘: (I1, High) INT 列 × CONST_DOUBLE は byte 経路が列を double 昇格させ 2^53
超で乖離、× CONST_STRING は byte 経路が文字列比較 — どちらも整数 zone で mirror 不可
なので **INT 列は CONST_INT/UINT のみプルーニング**に限定 (TPC-H の INT filter は全て
整数リテラル = CONST_INT で影響なし)。(S1) 極大 decimal 定数での DecLower/DecUpper
発散/overflow を `zone_dec_ok` (c×10^scale < 9e18) で防止。(overflow) ClassifyCell の
桁数上限をループ内に前倒し (19 桁目の乗算前に reject、int64 signed overflow 回避)。
(consistency) strip build に write_counter の二度読みを追加し torn build を非
プルーニング化 (Prepare/Quiesced の厳密な補完)、empty-numeric⇒NULL 不変条件と
PaxStore* 生存期間の依存をコメント明記。全指摘が「プルーニングを減らす方向 (より
保守的)」なので md5 は E16b と byte-identical のまま。

**設計系譜**: Small Materialized Aggregates (Moerkotte, VLDB1998)、Netezza zone maps、
MonetDB imprints (Sidirourgos & Kersten, SIGMOD2013)。新規性は 1-copy 制約下での
**遅延・write_counter キーの side-info** 化 (sorted copy を作らず OLTP 書込を一切増分
維持しない) と、静止検査を correctness backstop にした点。

### E18: fused shared scans — 依存解析の結果 SKIP (2026-07-03)

同一 request 内で同じ PAX 表 (同 table_name/PaxStore) を複数回 scan するケースを静的解析
(proxy build_block の table_idx 割当。self-join の alias は別 table_idx だが normalized_path が
同一なのでサーバでは同一 PaxStore に解決)。結論: **大表の独立複数 scan を 1 request 内に
持つクエリは存在しない**ため E18 は SKIP。

| クエリ | 同一表複数 scan | 依存 | 判定 |
|---|---|---|---|
| q21 | lineitem ×3 (l1 主 + l2 EXISTS→SEMI + l3 NOT EXISTS→ANTI) | l2/l3 の semi.source_node が両方 l1 → l1 と融合不可。{l2,l3} は相互独立だが両者とも l1 の orderkey semi filter で既に極小 | 融合利益ほぼ無 |
| q7/q8 | nation ×2 (派生ブロック内の n1,n2 自己結合) | 相互独立 (構造的には最も綺麗な候補) | nation = 25 行、利益無視可 |
| その他 | 各表 ≤1 scan/request (q2/q11/q17/q20/q22 等の重複は別 QbSubBlock = 別 request) | — | 対象外 |

大表の独立複数 scan が無い以上、共有 scan パスの利益は無い (q21 の l2/l3 は semi 後極小、
nation は自明) → **E18 は実装しない**。visibility/strip パスの共有は将来「大表を複数の
独立述語で同時集計する」ワークロードが現れたら再検討 (Crescando: Unterbrunner et al.
VLDB2009 / SharedDB: Giannikis et al. VLDB2012 の系譜)。

## メモリ圧縮フェーズ (2026-07-03〜): SF=5 分析からの改善

SF=5 分析 (`2026-07-03-sf5-analysis.md`) が RSS 38.5GB を arena 27.7GB (72%) +
primary index 7.0GB + secondary 3.8GB に分解し、arena のうち **13GB が utf8mb4 の
4× 文字列パッド**・**7GB が ASCII テキスト格納の数値**であることを示した。ここから
compression candidate を #1 文字列ストライド byte 幅化 → #2 型付き数値セル →
#3 低カーディナリティ辞書 の順で実装していく。

### M1: 文字列ストライドの byte 幅化 — utf8mb4 4× パッド除去 (2026-07-03)

**動機 (candidate #1).** PAX の文字列 cell は charset の**オクテット**長を予約していた:
utf8mb4 は 1 宣言文字あたり 4 バイト確保するので `l_comment VARCHAR(44)` は
純 ASCII データ (TPC-H/TPC-C 全て、実観測 max 43B) でも 176B の stride を張る = 丸損。

**変更 (1 箇所, proxy).** `compute_pax_field_widths` の文字列系型
(STRING/VARCHAR/VAR_STRING/ENUM/SET) の cell 幅を `f->field_length` (オクテット長)
から `f->char_length()` (宣言**文字**数 = field_length/mbmaxlen) に。latin1/binary
(mbmaxlen==1) では no-op。lineitem の文字列 stride は **334→91 B/row** (l_comment
178→46, l_shipinstruct 102→27, l_shipmode 42→12, returnflag/linestatus 6→3 ×2)、
行フットプリント 614→371 B。

**なぜ proxy 側か (engine 側でなく).** field_max_bytes は proxy が唯一計算し engine は
消費するだけ。char/charset 情報は proxy の `Field` にしかない。かつ field_max_bytes は
単一値で (a) stride (メモリ)・(b) ScatterRow の overflow 閾値・(c) cell()/GatherRow の
clamp を同時に駆動する — stride だけ engine 側で縮めて overflow 閾値をオクテット幅の
まま残すと `char_length < len ≤ octet` の行が ScatterRow の幅検査を通過するのに小さい
stride に収まらず**破損**する。従って cap は field_max_bytes 自体を縮めるしかなく、それは
`compute_pax_field_widths`。charset 非依存の cap を採用 (charset ベースのサイジングは
utf8mb4 宣言の TPC-H では無利益)。

**overflow フォールバックのトレードオフ (loud/slow, never wrong).** 実バイト数が
char_length() を超える行 (= 真のマルチバイト値) は既存の per-row heap フォールバック
(ScatterRow→false→BumpOverflow→heap Reset) に落ちる。overflow_count>0 は当該表の
strip-direct scan を無効化 → row-engine 経路 (正しいが遅い) に退化。query-block の
FORCED では `ER_SECONDARY_ENGINE` で loud reject (Codex 指摘、**silent wrong-result
ではない**)。TPC-H/TPC-C は純 ASCII なので overflow_count==0 を全 SF で実測確認。
**罠**: 単一の非 ASCII 行が表全体の strip scan を恒久無効化する poison-pill 特性
(overflow_count 単調・非リセット) — 純 ASCII では非発火だが、国際化データを載せる際は
要注意 (将来 #2 型付きセル/#3 辞書で緩和)。

**メモリ実測 (同一 jemalloc スタック、clean load、apples-to-apples).**
| SF | OLD (pre-M1) | M1 | 削減 |
|---|---|---|---|
| SF=1 lineairdb-server VmRSS | 9.79 GB | **7.33 GB** | **−25.1% (−2.46GB)** |
| SF=5 | 38.48 GB (分析ベースライン) | **25.38 GB** | **−34.0% (−13.10GB)** |

削減量が分析予測 (SF=5 で −13.09GB 文字列パッド、arena 27.7→14.6GB、RSS 38.5→25.4) と
一致。SF=5 の −34% は arena が RSS の 72% を占めるため大きく、SF=1 は arena 比率が低いので
−25%。SF=1 FORCED 合計は 6.33s(E17)/6.09s(E16b) → **5.96s** (狭い strip で転送減、微減)。

**dual review 対応 (Codex + code-reviewer).**
- **[code-reviewer, Important] silent wrong-result race → 修正済み.** strip-direct scan
  (query_block_executor `Quiesced` / lineairdb_rpc 集計経路) の quiesce 再検査が
  write_counter のみ見て overflow_count を見ない → overflow_count が scan 中に 0→1 遷移
  する狭い窓 (退避行が visible bit クリアで無音に欠落) で 1 行落とし得る。本変更**前**は
  文字列 overflow が実質デッドコード (field_length が val_str の物理上限) だったため到達
  不能 (table-full の 2^31 行のみ)、char_length() 化で非 ASCII 行が normal event として
  到達可能に。**修正**: 両 strip-direct 経路の quiesce に `overflow_count()>0 → return
  false` を追加。overflow_count は単調で Prepare 時点 0 保証なので再検査で >0 = scan 中
  遷移 → TID 検査経路へフォールバック。happens-before は既存の write_counter
  release/acquire が担う (RetireSlot の release bump は BumpOverflow の後に sequenced、
  snapshot が退避後カウンタを acquire-load した唯一のケース = write_counter 検査を通る
  ケースで overflow を必ず観測) ので **submodule の順序変更不要・server 側 2 ファイルのみ**。
  ASCII では overflow_count 恒久 0 なので非発火 → md5 不変 (再ゲートで確認)。ref-scan 経路
  (read.cpp) は heap 行を `!is_pax()` で検出し cancel、記録済み ref も per-row TID 再検査
  するため元々安全 (非変更)。
- **[Codex, High] 非 ASCII で query-block が loud fail.** 上記トレードオフの明示
  (never wrong, FORCED で reject)。設計上の loud degradation として受容・文書化。
- **[code-reviewer, Suggestion] wide-column admission.** 末尾 `w>kMaxCellBytes` ガードが
  char_length() 縮小で以前 reject された広い utf8mb4 列 (VARCHAR(513..2048)) を PAX に
  admit — 真のマルチバイト列だと arena 確保後に overflow で strip 無効化 = row-store より
  悪化し得る。純 ASCII では問題なし。将来 charset ベースの admit ゲートを検討。

**検証 (最終バイナリ: proxy plugin 1e346e0a + server 01197c67).** OLAP: SF=0.01 22/22
FORCED-vs-OFF ALL-MATCH + 回帰 3 本 (null-eq-join MATCH / string-stride round-trip
[full-width 10-char ASCII が新 stride 境界に収まる] / <=> loud-reject)、SF=1 22/22 md5 が
**E17 とバイト一致** (意味論保存)、overflow_count==0。TPC-C (書込パス接触必須):
1/8/32/64t = **420/1553/3663/5032 req/s、全点 Unexpected Errors=0**、ベースライン
(426/1546/3647/4892) 比 ±5% 内。

**設計系譜**: charset-aware storage sizing の古典。novelty は 1-copy PAX 制約下で
「per-row heap overflow を安全弁に strip 幅を宣言文字長へ攻める」点と、それが暴いた
quiesce プロトコルの overflow_count 再検査欠落の閉塞。次段: **candidate #2 型付き数値
セル** (INT/DATE/DECIMAL の ASCII テキスト格納を native 化、arena さらに −6.9GB +
ASCII パース税 [q1/q6 の 25-52%] 消滅、scatter 経路接触の本丸 = SIMD スパイクの結論
「型付けは効く」の受け皿)。
