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
