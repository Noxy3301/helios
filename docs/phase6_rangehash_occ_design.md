# Phase-6: Range-hash OCC validation (設計 + 承認/却下の記録)

User 指示(2026-05-29): 「何を検討して承認/却下したのかをちゃんとまとめたうえで、実際に
実装してどのくらい機能するか確認」。本書は range-hash OCC の検討経緯・Codex レビューでの
承認条件と却下事項・実装スコープを記録する。

---

## 動機(なぜやるか)

Phase-5 attribution で commit RPC が全クエリ一律 **7.5-8s**(Q1/Q21 SF=1, 6M lineitem)と
判明。内訳: proxy が ~6M (key,tid) read set を serialize → wire → server parse → O(rows)
revalidation。これは「intrinsic でない残コストの最大単一項目」。

physical OCC は既に:
- **node version**(per-leaf, O(leaves))で構造 phantom(insert/delete)検知
- **per-row TID**(O(rows))で value-update 検知(値変更は node version を bump しない)

→ per-row TID list(O(rows))を **per-range の SHA-256 hash(O(1)/range)** に圧縮できないか。

## 提案(承認された方向)

range read(full scan / FER / FES)について:
- **prefetch 時**: server が range scan しながら read footprint の rolling SHA-256 を計算、
  32-byte root を TxOccStore(tx_occ_key 配下)に保持
- **proxy は read set を受け取らない/持たない**。commit で tx_occ_key を echo するだけ
- **commit 時**: server が同じ range を `ValidateAndCommit` 内で re-scan、rolling SHA-256
  再計算、retained root と比較。一致=変更なし、不一致=abort
- **point read(R:/FE:)** は明示 (key,tid) のまま(数が少ない)

二層 validation:
- 構造 phantom(insert/delete)→ 既存 node-version(O(leaves))
- value update(tid 変化, 構造同じ)→ range hash(per-row TID list を置換)

## 期待効果
- commit wire(reads): O(rows) → O(1)/range
- proxy memory: 6M read-set materialize 不要
- server CPU: range re-scan(sequential leaf walk)+ hash vs 現状 O(rows) point lookup。
  comparable〜やや改善(sequential は cache-friendly)。SHA-NI で hash コスト最小化

---

## Codex レビュー結果(2026-05-29, high effort)

**判定: directionally good。ただし 2 つの correctness 条件を満たすまで "as stated" は不承認。**

### ✅ 承認された点
- 性能議論は妥当(commit が payload movement 主因なら大きく削れる)
- 汎用(TPC-H 特化でない。range read 一般に効く、point read 不変、query 形状分岐なし)
- flat 256-bit digest を先に(Merkle は localization/incremental 用、コア correctness には不要)

### ⚠️ 承認条件 1: hash は read footprint 全体を覆う(返却行だけでは穴)
含めるべき:
- visible rows(scan が返した行)
- **filter-reject 行**(これを漏らすと Step C で塞いだ穴が再発: prefetch で filter 落ち
  した行が value-update で条件を満たすようになる、node version は bump しない)
- **tombstone / 空 primary スロット**(scan が観測したもの)
- secondary scan: **secondary-index slot の TID + base-row primary TID 両方**
  (secondary slot 書換も表現必須。`(secondary_key, base_tid)` だけでは不足)
- range bound / direction / row_limit / 順序 を hash に含める

primary range: `(kind, table/index domain, key, tid, found)` を scan 順に flat hash。

### ⚠️ 承認条件 2: commit re-scan は ValidateAndCommit 内(ロック後・install 前)
- read-only tx は own-write がないので単純
- read-write tx は own-write 正規化が必要:
  - 自分がロックした行は `locked.before_lock` TID で hash(ロック後の odd/locked TID で
    なく)
  - 自分の pending new value は committed でないので除外
  - own insert は `reconcile_own_insert` 相当で phantom 扱いしない
  - **re-scan を ValidateAndCommit の外でやるのは read-write で誤り**

### 却下/保留事項
- **「read set を必ず渡す」版は却下**(hash 計算が増えるだけで wire/memory 削減ゼロ)。
  → server 再導出(range 再 scan)が必須。proxy は read set を送らない
- **Merkle tree は初手では不採用**(flat digest で足りる。Merkle は将来の部分検証用)
- **collision を「数学的に絶対不可能」とは主張しない**。2⁻²⁵⁶ の確率的等価。invariant に
  正直に明記する。user の「phantom 絶対あり得ない」ポリシーと厳密には矛盾 → **user が
  2⁻²⁵⁶ を許容と判断**(2026-05-29)

### Codex が挙げた実装リスク
- footprint を取り違える(特に filtered 行と secondary slot)
- re-scan を locked validation phase の外でやる
- own writes が false abort or committed 状態として誤計上
- 現 physical range_versions が start/end を保持してない可能性 → range descriptor 追加要
- TTL/pin expiry が validation 中に起きる(既存 lease/pin pattern を継続)

### Codex 推奨テスト
filtered 行が条件満たすようになる / 返却行の value-only update / tombstone reinsert /
secondary slot 書換 / read-range 後 own update / read-range 後 own insert /
LIMIT 周辺の concurrent insert/delete

---

## 実装スコープ(本 Phase で着手する範囲)

**案 A: read-only スコープ先行**(user 承認 2026-05-29)
- read-only tx のみ range-hash path。**write tx は既存 per-row TID path にフォールバック**
  (gate で分岐)。これで own-write 正規化(承認条件 2 の複雑部)を回避しつつ commit
  コスト削減を TPC-H で実証
- footprint 正確さ(承認条件 1)は read-only でも必須 — filtered/tombstone/secondary を
  含める
- collision: 256-bit flat SHA-256、invariant 明記

将来(別 Phase): own-write 正規化を入れて HTAP(read-write)対応 = 汎用版

## 実装手順(予定)
1. server: StatelessRangeScan / StatelessSecondaryRangeScan に rolling SHA-256 を追加
   (visible + filtered + tombstone + secondary slot を footprint に)
2. TxOccStore: per-range root hash + range descriptor を保持
3. TX_VALIDATE_AND_COMMIT: read-only tx で range 再 scan + hash 比較(ValidateAndCommit 内)
4. proxy: read-only tx では per-row read TID 記録を停止、tx_occ_key echo のみ
   (write tx 判定で従来 path にフォールバック)
5. gate: HELIOS_RANGEHASH_OCC=1(段階導入)
6. 検証: 22Q md5 == InnoDB + commit time 計測(before/after)+ Codex 推奨テスト

## 計測目標
- Q1/Q21 SF=1 commit RPC: 7.5-8s → 大幅減(目標 < 2s)
- 22/22 md5 維持
- read-only 限定なので write workload は不変(回帰なし)
