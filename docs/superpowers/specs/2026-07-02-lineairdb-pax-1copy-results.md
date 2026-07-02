# LineairDB-PAX 1-copy — Win Condition 検証結果

Date: 2026-07-02
Branch: `feasibility/pax-1copy` (LineairDB submodule: `helios/pax-1copy`)
Machine: 64 vCPU Xeon Gold 6326 @2.90GHz, 125GB RAM, governor=performance, C6 off
設計: `2026-07-02-lineairdb-pax-1copy-design.md`

## 0. 結論

**Win condition は存在する。** row-store を全撤廃し PAX を唯一のストレージ (1-copy) にした構成で:

- **OLTP (TPC-C)**: row-store と同等 (0.95-1.00x)。並列スケーリング維持、エラー 0。
- **OLAP (TPC-H SF=1, 22 クエリ合計)**: **LineairDB デフォルトの 0.52 倍 (1.9x 高速)、InnoDB の 0.85 倍 (1.2x 高速)**。
- 正しさ: 全構成・全ゲートで 22/22 クエリ md5 byte-identical。

## 1. 実測値

### TPC-H SF=1 (warm, single-stream, min-of-3, 22 クエリ md5 全一致)

| 構成 | 合計 | 対 LDB デフォルト |
|---|---|---|
| LDB row-store (d053210 デフォルト) | 69.7s | 1.00x |
| InnoDB (固定 champion, 2026-06-13 同一箱計測) | 42.2s | 0.61x |
| LDB + q18 パッチ | 37.7s | 0.54x |
| **PAX + q18 パッチ (最終形)** | **36.1s** | **0.52x** |

- q18 パッチ同士のフェア比較でも **PAX 36.1s < LDB 37.7s** — レイアウト自体の勝ち。
- 主な個別勝ち: q1 **0.45x**, q6 **0.36x**, q18 **0.06x** (34.9→2.06s; パッチ+strip 集計。LDB+パッチの 2.70s より 24% 速い), q22 0.85x, q20 0.86x, q21 0.93x。
- 残る負け (小差): q7 1.10x (proxy 側 per-row probe staging が主因 — 2026-06-28 調査と同結論), q16 1.09x, q13/q14/q15 1.05-1.07x。

### TPC-C (SF=1, 60s, prefetch DSL, クリーン手順: 再起動→ロード→実行, エラー 0)

| terminals | LDB row-store | PAX | 比 |
|---|---|---|---|
| 1 | 428.5 req/s | 423.7 | 0.99x |
| 8 | 1556.7 | 1477.7 | 0.95x |
| 32 | 3658.5 | 3669.5 | 1.00x |

OCC retry も両者同水準 (t=32: 483K vs 484K)。point read の gather (~17 strips) と install の scatter は RPC 往復に埋もれる。
(InnoDB は歴史的に LDB が TPC-C で 2-5x 勝ち — handoff note 2026-07-01。)

## 2. 何を作ったか (マイルストーンと学び)

| M | 内容 | ゲート/実測 |
|---|---|---|
| M1 | DataBuffer タグ付きポインタで行バイトの正本を PaxGroup ストリップへ (1-copy)。copy-ctor=gather / operator= (install)=scatter。プロトコル・prefetch・pushdown 無変更 | md5 22/22。**学び: 全列 gather は SF=1 の DRAM 規模で崩壊 (q1 23s)** — per-row 17 miss |
| M2 | 集計フルスキャンの evaluate-on-strips (storage 順 group 走査、8 並列、write-counter 静止チェック、fallback 常備) | q1 0.44x, q6 0.34x。**ポインタ解決ゼロの連続 strip walk が勝ち分の本体** |
| M2.5-2.7 | 行返却スキャン: PaxRefScan (strip filter→生存行のみ projected gather→行単位 TID 再チェック)、int キー範囲 8 並列、probe/SI/point への sparse gather (0xFF プレースホルダ、RowProjection 付き step 限定) | q12/q19/q22 回復。**学び: シリアル ref-scan は既存 8 並列に負ける** (M2.5 の q12 悪化→並列化で回復) |
| q18 統合 | shelved パッチ (grouped-semijoin+GS) 移植 + PAX 集計への HAVING マーカー統合 + **ref-scan の hoisted semijoin 適用漏れ修正** | q18 34.9s→2.06s。**学び: 最適化経路を新設したら、既存経路が担っていた縮約 (semijoin) を漏らさず引き継ぐこと** — 漏れは md5 では捕まらず (正しさは MySQL 側 join が守る)、転送膨張として現れた |

## 3. 1-copy であることの構成

- 行バイトの正本は PaxGroup ストリップのみ。heap に行の連続バイトは存在しない (DataBuffer::value が PaxGroup* を指す)。
- 残存メタデータ: DataItem 48B/行 (TID ワード+strip 参照+SI posting list 参照) = バージョン語とインデックスの葉。Masstree/SI はインデックスであり行データを持たない。tx-local snapshot は Silo 読みプロトコル固有の一時コピー。
- overflow fallback (セル幅超過行のみ heap) は TPC-C/TPC-H で 0 件 (カウンタ監視、>0 で strip スキャン自体を無効化)。

## 4. 限界と次の一手 (正直な列挙)

1. **SIMD (AVX512) 未着手**: セルは val_str() 文字列表現 ([u16 len][bytes]) で SIMD 不向き。現在の勝ち分はメモリ階層 (帯域・ライン利用率・パース削減) 由来。型付き固定幅セル (スケール済み int64 等) への移行が次の伸びしろで、strip は固定 stride 配列なので受け皿はある。
2. **ref-scan は Masstree 順のポインタ解決混じり**: 純 columnar 化には key strip (PK エンコードキーのストリップ化) + zone map で storage 順走査に切り替える (設計 §3.1 に記載済み、未実装)。
3. **q7 (1.10x)**: proxy 側 per-row probe staging (string-key rowcache hash ~21% 等) が主因でストレージ層では救えない。汎用 staging cleanup は別軸。
4. **mixed HTAP 干渉は未計測**: TPC-C と TPC-H は別ベンチ。並行 writer 下の strip スキャンは write-counter 再チェック→fallback で正しさは守るが、リトライ率・干渉は未計測 (文献的にも single-copy の未解決点 — SOTA サーベイ参照)。
5. **WAL/recovery**: 全構成で無効 (既存ベンチと同条件)。有効化時は recovery replay の PAX 再構築が必要 (row-shaped WAL は互換)。
6. **メモリ**: パディング固定幅セルのため row-store 比で増える見込み (未計測)。圧縮は意図的にスコープ外 (mutable 領域を圧縮しないのは文献の一致した教訓)。
7. **InnoDB 値は 2026-06-13 の固定 champion** (同一箱、LDB ベースラインの再現一致で妥当性担保)。再計測はしていない。

## 5. 再現手順

```bash
git switch feasibility/pax-1copy   # LineairDB submodule: helios/pax-1copy
./scripts/build.sh                 # 初回 ~33 分

# OLAP (PAX+q18 最終形)
HELIOS_PAX_STORAGE=1 HELIOS_Q18_SEMIJOIN=1 HELIOS_Q18_GS=1 ./scripts/start_server.sh
HELIOS_Q18_SEMIJOIN=1 HELIOS_Q18_GS=1 ./scripts/start_mysql.sh --mysqld-port 3307 --server-host 127.0.0.1 --server-port 9999
./bench/bin/tpch_setup_sf1.sh --loader-threads 16
./bench/bin/tpch_wall.sh PAX-final --runs 3
python3 bench/bin/tpch_wall_report.py bench/results/tpch_wall/LDB-base-d053210.tsv bench/results/tpch_wall/PAX-final.tsv

# OLTP
python3 bench/bin/benchrun.py tpcc --sweep 1,8,32 --time 60 --prefetch --external-server
```

ゲート無し (`HELIOS_PAX_STORAGE` 未設定) で起動すれば完全に既存 row-store 動作 (byte-identical)。
