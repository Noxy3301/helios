# LineairDB-PAX 1-copy 設計 (v1)

Date: 2026-07-02
Base: `feat/q18-grouped-semijoin` @ d053210 (LineairDB submodule @ 0f42ae9)
Branch: `feasibility/pax-1copy` (helios) + `helios/pax-1copy` (LineairDB submodule)
Status: 実装前設計。9並列コードリーダー + 過去セッションノート発掘 + SOTA文献調査に基づく。

## 0. ゴール (win condition)

ユーザー要求: 「1-copyとして機能する PAX を row-store の代わりに使い、Prefetchモード・Pushdown・転送量最適化にそのまま乗り、OLTP/OLAP 両方で LineairDB デフォルトと InnoDB に勝つ」。

数値バー (このマシン、64 vCPU Xeon Gold 6326、実測済みベースライン):

| 軸 | 対象 | バー |
|---|---|---|
| OLAP | TPC-H SF=1 22クエリ合計 (warm, single-stream, min-of-3) | InnoDB 42.2s / LDB row-store 69.0s (うち q18 36.3s) より速い |
| OLTP | TPC-C (multi-terminal) | LDB row-store と同等以上 (InnoDB には LDB が既に 2-5x 勝ち) |
| 正しさ | TPC-H 22/22 md5 byte-identical、TPC-C 整合性、gate-OFF で既存と byte-identical | 必須 |

## 1. 前回 (feasibility/pax) の死因と、本設計の反転

前回は PAX_ONLY テーブル + 専用 RPC (PaxScan/PaxFetchRows/PaxBulkLoad) の**並行世界**を構築した。
結果: 単一表集計は 3-10x 勝ち、しかし (a) JOIN 内側 probe が同期 per-row RPC 化 (q9 で ~30万 RPC、3-47x 負け)、
(b) prefetch coverage 0%、(c) row-store 二重書き + RPC 増で TPC-C 0.83x。
教訓は「**転送・プラン資産から切れた瞬間に負ける**」。

本設計の反転: **proxy は PAX の存在を知らない**。ワイヤプロトコル・prefetch・pushdown・MRR・コミットプロトコルは
一切変えない。LineairDB 内部の「行バイトの置き場所」と、server 内スキャン実行の「行バイトの読み方」だけを変える。

裏付け (コード事実):
- LineairDB の行バイトへの読みアクセスは `StableReadValue` / `ReadDirect` の 2 choke point に集約されている
  (lineairdb-core reader 確認)。書き込みは Silo install の DataBuffer::Reset 1 箇所。
- server 側プラン実行 (handleTxExecuteReadPlan) のストレージアクセスは StatelessRead / StatelessRangeScan 系
  4 呼び出しに集約 (server-core reader 確認)。
- 継続ブランチ `feasibility/pax-prefetch-general` が「汎用スキャンパスの下に PAX を敷く」方向で
  TPC-H 22/22 md5 一致を維持できることを既に実証 (ただし dual-write / read-only 限定のまま未完)。

## 2. 性能仮説 (なぜ勝てるか)

実測プロファイル (Phase 0, SF=1) より:
- q1: server wall の 52-70% が行 decode (parse_row + dec_eval byte-walk)、さらに ~26% が
  Masstree walk + per-row std::string copy (stateless_ns)。→ 合計 ~80-90% が本設計の攻撃対象。
- q5 スキャン: カラム読み出し自体は 5.1ms、**行ワイヤフォーマット materialization が 74.5ms (94%)**
  (pax-prefetch-general の実測)。→ 「行を作らない」ことが本質。フィルタ/集計を strip 上で直接評価し、
  **生存行だけ** materialize する。集計ステップは group 行しか emit しないので materialization ≈ 0。
- q18 (36.3s, ベースライン合計の 53%): プラン認識の問題であり PAX と直交。shelved パッチ
  (GroupedSummary, 36s→2.65s, md5 22/22, レビュー済) が存在する。PAX はその server 側 GROUP BY 実行を
  さらに加速する。win condition 評価では「PAX 単体」「+q18 パッチ」を分けて計測する。
- OLTP: RPC 数はプロトコル不変により LDB と完全同一。差分は point read の gather (~µs/行) と
  write install の scatter のみで、RPC 往復 (数十µs) に埋もれる見込み。per-row heap malloc
  (DataBuffer) が消える分の相殺もある。

SOTA 裏付け: Umbra (single-copy in-place per-page PAX、storage latch とトランザクション CC の分離)、
HyPer DataBlocks / DB2 BLU (「mutable 領域は非ソート・軽圧縮なら delta-merge 不要」)、
Wildfire (single-copy PAX append、単一ノードなら成立)。圧縮は本フェーズのスコープ外
(圧縮を write path に入れないのが文献の一致した教訓)。

**新規性の認識** (2026-07-02 文献サーベイ結論): 「真の単一コピー PAX + 行単位 TID 保護 + in-place 更新 +
OLTP パリティ」は 2001-2026 の文献・実システムで未実証。最接近の Colibri (VLDB 2024) はホットデータ限定、
DuckDB は可視性を 2048 行ベクター単位にバッチ化、商用 HTAP は例外なく delta+main の 2 コピー。
本実験は未解決の設計空間に踏み込む — seqlock リトライ率など保護コストの実測が判定材料として必須。

## 3. アーキテクチャ

### 3.1 ストレージ (LineairDB 内部、`helios/pax-1copy` ブランチ)

```
Table
 └─ PaxStore (テーブルごと、スキーマ登録済みのときのみ)
     ├─ groups: chunked append-only array<PaxGroup*>  (atomic group_count)
     └─ PaxGroup (8192 行固定)
         ├─ strips[col]: 64B-aligned 固定幅セル配列   ← セル = [u16 len][max_len bytes (padded)]
         │   ※ NULL/空文字はどちらも len=0 セル (行再構築時に 0xFF field へ決定論的に戻す)。
         │     真の NULL 判定は null_field strip が担うため per-column null bitmap は不要。
         │   ※ strip 確保は lazy (グループへの初書き込み時) も検討 — eager 8192×全幅ゼロ埋めは
         │     remnant の実測でメモリ膨張源だった。
         ├─ null_field strip: 行の null-flags フィールド原文 (固定幅 = table null_bytes)
         ├─ visibility bitmap (atomic、delete で clear)
         ├─ seqlock: atomic<u64> version (write=+2, 奇数=in-progress)
         └─ zone map: min/max PK (storage-order scan の順序検証用、Phase 2)
            ※ zone map は sealed group (満杯) でのみ計算・信頼する。tail (active) group は
              zone map を使わず保守的経路 (文献推奨: アクティブ領域のゾーンマップは維持しない)。
            ※ seqlock 粒度は group=8192 行で開始。リトライ率が高ければ 1024-2048 行サブバッチへ
              細分化 (DuckDB のベクター粒度可視性 / HyPer の 1024 行バッチ可視性スキップに倣う)。
```

- **DataItem は温存** (48B ヘッダ、atomic TID)。`DataBuffer buffer` の役割を `PaxRef {u32 group, u32 slot}` が
  置き換え、行バイトの正本は strip セルになる (= 1-copy)。
- インデックス (Masstree / ConcurrentTable / SecondaryIndex=PackedPrimaryKeys) は**一切変更しない**。
  value は従来どおり DataItem*。→ Silo 検証・range replay・phantom 保護・Reaper GC が無傷で生きる。
  前回 occ-f4 が苦闘した per-slot TID / 5-tuple lock key / shadow TID は**全部不要**になる。
- **可変長セル**: 幅 = スキーマ最大幅 (proxy の field 定義から)。`val_str()` が最大幅を超えるセルは
  overflow フォールバック (その行だけ従来の DataBuffer に保持、PaxRef.slot に overflow bit)。
  正しさは常に保証、overflow はカウンタで fail-loud (TPC-C/TPC-H では 0 のはず)。
- **行の再構築 (gather)**: 行 = [null_field][col_0]...[col_n]、各セル [byteSize(1B)][len][bytes] は
  len から決定論的に再エンコード可能 (proxy codec 確認済み) → byte-identical 保証。
- **Insert**: tail group に slot を atomic 確保 → strips へ scatter → DataItem 発行 → 既存 index publish。
- **Update**: 既存 Silo install 経路 (TID ロック保持中) で strips を in-place 上書き + seqlock bump。
- **Delete**: 既存 tombstone (size=0) + visibility bit clear。slot 再利用なし (F3 踏襲、リークは Phase 3 GC)。
- **Reaper**: DataItem purge hook で visibility clear のみ追加。

### 3.2 スキーマ伝搬 (proxy → server → LineairDB)

- `DbCreateTable` proto に per-column 幅/nullable 記述子を additive 追加 (remnant の PaxColumnDesc を転用)。
- DatabaseManager → `Database::CreateTable(name, schema)` 拡張。スキーマ未登録テーブルは従来 row-store 動作
  (安全フォールバック; gate-OFF と同じ)。
- ゲート: server env `HELIOS_PAX_STORAGE=1` (static-init 一発読み、切替は再起動 = 既存ゲートと同じ流儀)。

### 3.3 読みパス

| パス | M1 (正しさの土台) | M2 (columnar fast path) |
|---|---|---|
| StatelessRead (point) | gather (全列 or projection) | 同左 |
| StatelessRangeScan (Masstree 順) | per-row gather | needed-columns gather |
| プラン scan step (filter/agg/semijoin/projection) | per-row gather → 既存 evaluator | **strip 直接評価 → 生存行のみ materialize** |
| aggregate step | 同上 | strip 直接 fold、group 行のみ emit |
| 並列スキャン (8-thread slicing) | 既存のまま | group 境界 slicing |

- M2 の strip 直接評価は **ro_novalidate 読み (autocommit SELECT = TPC-H 全部)** では per-row TID を
  読む必要がない。物理整合性は per-group seqlock (開始/終了で version 比較、変化したらその group を
  Masstree 経路で再実行)。
- 検証つき読み (prefetch 通常モード等、scan_tids が必要) でも strip 評価は使える: フィルタを strip 上で
  評価して生存行を決め、**生存行のみ** per-row の TID 読み→gather→TID 再読プロトコルで materialize する
  (フィルタ棄却行は返さず、範囲整合性は既存の key-set replay + filtered_keys 機構が担保)。迷ったら
  M1 経路 (Masstree 順 per-row) に落とす — 正しさ側のフォールバックは常に存在する。
- storage-order full scan (Phase 2 後半): zone map で group 間 PK 単調性を確認できた場合のみ
  Masstree を迂回して group 順走査 (バルクロードが PK 順なら成立)。崩れていれば Masstree 順に fallback。

### 3.4 書きパス

変更は LineairDB 内部の install だけ。proxy の write_buffer / TX_BATCH_WRITE / ValidateAndCommit
エンベロープ / SI 更新 / row_deltas は全部そのまま。

## 4. マイルストーン

| M | 内容 | ゲート |
|---|---|---|
| M0 | ブランチ + ビルド + ベースライン再計測 (LDB/InnoDB, TPC-C + TPC-H SF=1) | champion 表と整合すること |
| M1 | shred/gather storage swap (LineairDB) + スキーマ伝搬 + gate | TPC-H 22/22 md5 = row-store、TPC-C 動作、OLTP regression 実測 |
| M2 | evaluate-on-strips (server plan executor 統合) + needed-columns | TPC-H 22/22 md5 維持、q1/q6/q12/q18-subquery 系の実測改善 |
| M3 | OLTP 磨き (point-read projection, scatter コスト) + TPC-C 3-engine | TPC-C PAX ≥ LDB |
| M4 | TPC-H SF=1 3-engine 本計測、±q18 パッチ、win condition 判定 | 最終レポート |

各 M で `build_partial.sh` インクリメンタルビルド + docs/phase2 流の smoke で正しさゲートを先に通す。

## 5. リスクと対策

1. **OLTP regression (gather/scatter の cache miss 増)** — 最重要リスク。M1 直後に TPC-C を測る。
   緩和: point read への projection 適用、null_field/PK 列の gather 省略、セル幅の 8B 丸め。
2. **seqlock リトライ** — TPC-C と TPC-H は別ベンチなので実害は小さい。mixed HTAP 干渉は文献的にも
   未解決 (TiDB の 5-10% バーは物理分離の産物)。本フェーズの win condition 外として記録。
3. **materialization が依然支配** — M2 の「生存行のみ」原則で回避。q1 型 (全行生存・集計のみ) は
   emit ≈ 0、q6 型 (98% フィルタ死) は 2% のみ materialize。
4. **順序仮定** — storage-order scan は zone map 検証付き。検証失敗時は Masstree 経路 (正しさ不変)。
5. **WAL/durability** — ベンチ構成は logging 無効 (Phase 2 と同じ制約)。PAX の WAL 接続は Phase 3。
6. **メモリ** — SF=1 全データ+23index が row-store で 6.4GB。PAX パディングで ~1.5-2x 見込み、125GB 箱で余裕。

## 5.5 環境の既知制約 (critic 監査より)

- WAL/recovery は全デプロイ構成で env-gate OFF (database_manager.cc:21-22)。ベンチは揮発+毎回リロード前提。
  commit envelope は行値を row-shaped で運ぶため、将来 logging を有効化しても WAL は row-shaped のままで
  よい (recovery replay の PAX 再構築は Phase 3)。
- DDL は CREATE TABLE + ADD/DROP INDEX のみ実在。**DROP TABLE は no-op** (再 CREATE で旧行が復活する) —
  ベンチハーネスは必ずサーバ再起動 (ワイプ) を挟むこと。RENAME/TRUNCATE/copy-ALTER は不可。
- SI はサーバ側に行バイトを持たない (PK posting list のみ)、SI キーは proxy が行イメージから計算 →
  PAX スワップの影響ゼロ。remnant の PaxSecondaryIndex は削除対象。
- optimizer stats (NDV/histogram) は ordered PK index のキーだけを読む → Masstree 温存で無傷。
  stats には一切触らない (HELIOS_PAX_STATS_SCALE の偽装は wrong results を出した実績あり)。

## 6. 明示的な非目標

- 圧縮 (dict/RLE/bit-pack) — 文献の教訓どおり write path に入れない。Phase 3。
- slot 再利用 / live compaction — F19 踏襲 (mark-and-skip のみ)。
- mixed HTAP 干渉制御 — 記録はするが win condition 外。
- MySQL core 改造、シャーディング、max_thread 変更 — 従来からのハード制約。
