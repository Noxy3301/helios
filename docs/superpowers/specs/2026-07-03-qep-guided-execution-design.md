# Phase D: QEP 誘導実行 — 設計

Date: 2026-07-03
Branch: helios/pax-qep-guided (from helios/pax-1copy @ b965a4b)
狙い: FORCED 22.1s の律速 A 群 (q17 2.5s / q18 3.2s / q20 1.5s / q21 9.6s =
16.7s) を、QEP のカーディナリティ情報を使った選択的実行で桁減らす。
目標: FORCED ~13s 圏。

## 観察 (D1 診断済み)

secondary 経路 (hypergraph optimizer) の QEP は q17 で:
```
Aggregate → HJ(part ⋈ ...) → [Filter part (rows=20)]
                            → HJ(lineitem ⋈ derived) → lineitem (rows=60175)
                                                     → derived (全 lineitem 集計)
```
- QEP 自体も全ドメイン derived 計画 → 構造の丸写しでは速くならない
- しかし **rows 見積もり (part=20)** が「どの表が選択的か」を教えてくれる
- stats は既存の GetTableStats/NDV/histogram 同期でオプティマイザに届いている

## 設計 (lineage: Neumann-Kemper の D=Π(相関列) 絞り込み / DuckDB delim join /
既存 Prefetch autogen の bindings 機構と同思想)

1. **QEP rows の取得** (proxy): OptimizeSecondaryEngine で
   `join->root_access_path()` を WalkAccessPaths し、TABLE_SCAN (+直上
   FILTER) の `num_output_rows` を表ごとに tabs[i].qep_rows へ。
2. **ノード発行順** (proxy): join_order を「接続制約下で qep_rows 昇順」に
   (現在は FROM 順)。選択的な表が先に実行される。
3. **半結合フィルタ伝播** (IR + server):
   - `QbScan.semi_filter { source_node, source_column(QbColumnRef),
     my_column }`: 実行済み source ノード出力のキー集合に載る行だけ通す
   - `QbSubBlock.semi_filter { source_node, source_column,
     target_table(子ブロック内表), target_column }`: 親がキー集合を集めて
     子 Executor に外部フィルタとして渡し、子の該当 scan に適用
   - proxy 規則: join edge で結ばれた 2 ノードのうち、先行ノードの
     qep_rows が自分より十分小さい (例: 100 分の 1) 場合に張る
4. **q17 での効果**: scan(part,20 行) → sub_block(derived) は 20 partkey
   のみ集計 (6M 行→数千行) → scan(lineitem) も同フィルタ。

## Dual review 運用

各マイルストーン (D2=rows 取得+順序, D3=semi_filter) ごとに
superpowers:code-reviewer と codex:codex-rescue の 2 系統レビュー→
指摘対応→md5 22 本ゲート→コミット。

## ゲート

SF=0.01 22 本 FORCED md5 → SF=1 FORCED 3 runs (目標 ~13s) + TPC-C 回帰なし。
