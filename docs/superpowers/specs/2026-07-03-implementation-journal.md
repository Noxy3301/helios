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
