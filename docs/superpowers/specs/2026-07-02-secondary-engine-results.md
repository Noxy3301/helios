# LINEAIRDB_COLUMNAR secondary engine — Phase B 結果 (huge win 判定)

Date: 2026-07-02
Branch: helios/pax-1copy (B0=e5502c2, B1=1d1afbf, B2/B3 含む)
条件: SF=1, champion 条件 (HELIOS_PAX_STORAGE=1, prefetch ON, ro_novalidate ON,
subquery_to_derived=off, q18 パッチ gate ON), use_secondary_engine=ON (デフォルト),
3 runs 中央値, **22 クエリ md5 全一致**。

## 総合

| 構成 | TPC-H SF=1 合計 (22q) | 比 |
|---|---|---|
| LineairDB デフォルト | 69.7s | 1.0x |
| InnoDB (fixed champion) | 42.2s | 1.65x |
| LDB+q18 パッチ | 37.7s | 1.85x |
| PAX 1-copy + q18 パッチ (Phase A 前) | 36.1s | 1.93x |
| **PAX + secondary engine (Phase B)** | **16.2s** | **4.3x** |

**vs InnoDB: 2.6 倍。** row-store 時代の「PAX 純寄与 4%」から、実行エンジンごと
乗っ取る secondary engine で桁が動いた。オフロード 8 本が合計の 43% を占めていた
時間 (21.5s) を 2.0s に圧縮。

## オフロード 8 本 (すべて md5 一致)

| q | Phase A 前 | Phase B | 短縮 | vs InnoDB |
|---|---|---|---|---|
| q1 | 450ms | 259ms | 1.7x | 33x |
| q3 | 1.3s | 208ms | 6.3x | 17x |
| q5 | 1.0s | 232ms | 4.3x | 7x |
| q6 | 165ms | 95ms | 1.7x | 11x |
| q9 | 3.6s | 306ms | 11.6x | 25x |
| q10 | 1.8s | 320ms | 5.6x | 4.5x |
| q12 | 7.4s | 108ms | **69x** | 14x |
| q13 | 5.9s | 444ms | 13x | 11x |

## 実装 (何が効いたか)

1 RPC で**クエリブロック全体** (scan→hash join→group agg→sort/limit) をサーバ実行。
MySQL 側の join/agg・proxy staging・行転送が丸ごと消える — Amdahl の 7-8 割を
占めていた部分。

- IR: `TxExecuteQueryBlock` 演算子木 (Scan/Join(INNER/LEFT)/Aggregate+SecondStage)
- 実行: late materialization (タプル=表ごと PAX row-ref)、morsel 並列 scan
  (PAX group 単位)、chunk 並列 probe、パーティション並列 agg → merge
- INNER build/probe は実サイズでスワップ、中間 64M 行上限 (超過は primary へ fail over)
- 正しさ: HeatWave 流 allowlist (+md5 ゲート)。write-counter 静止チェック。
  reject/失敗は自動で primary 再実行 (q18 はパッチ済み primary が 2.0s で受ける)
- q13: derived 二段集計→SecondStage。q9: EXTRACT(YEAR)→prefix-4 グルーピング、
  表跨ぎ SUM 式→(table_idx<<16|column) タグ付き COLUMN_REF

## フォールバック 14 本の残り (今後の伸びしろ)

q21 3.9s (EXISTS/NOT EXISTS semijoin+残差述語)、q7 1.8s (クロステーブル OR)、
q18 2.0s (パッチ済み)、q15 1.5s (ビュー+MAX 相関)、q22 0.97s (相関 AVG)。
q21+q7+q15+q22 対応で理論 ~12s 圏。SIMD/型付きセルは未着手のまま
(現状でも文字列セル+interpreted で 2.6x — VLDB2018 の「vectorized-interpreted で
十分」を裏付ける結果)。

## OLTP への影響

secondary engine は read-only autocommit SELECT (かつコスト閾値超) のみに介入。
明示トランザクション・書き込みは構造上 primary 経路のまま (TPC-C 計測済みの
PAX 1-copy 性能 0.95-1.00x から不変)。
