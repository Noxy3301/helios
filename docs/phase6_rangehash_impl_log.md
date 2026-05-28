# Phase-6 range-hash OCC 実装ログ(手順書 + 実測)

設計・承認条件: `docs/phase6_rangehash_occ_design.md`。
ceiling 測定(2026-05-29, Opus 4.8): per-row TID 記録 skip で Q1 41.3→24.9s(-40%),
Q21 50.2→36.9s(-26%)。結果データは sorted で InnoDB 一致(skip したのは OCC 検証のみ)。
→ 本実装で「この速度を correctness を保ったまま」達成するのが目標。

## 重要な前提(正直な現状)
Phase-2/3/4 で報告した Q21 39s / Q1 23s は **unsafe な Step C(per-row skip)込み**だった。
Step C revert 後の正しい OCC baseline は Q21 50s / Q1 41s。range-hash はこの speed を
安全に取り戻す。

## スコープ(本 Phase)
- **primary range の full-cover scan のみ**(Q1/Q21/Q18 の S:lineitem = commit コストの主因)
- **read-only tx 限定**(write tx は従来 per-row TID path に fallback)
- FER/FES/point read は従来通り(per-row 維持)
- 全て `HELIOS_RANGEHASH_OCC=1` gate。default OFF = 挙動不変

## footprint(Codex 承認条件 1: read footprint 全体を hash)
primary range の hash 対象 = scan が触る全エントリを **key 昇順**で:
- visible row: feed(key, packed_tid, found=1)
- filter-reject row: feed(key, packed_tid, found=1)  ← Step C で塞いだ穴の再発防止
- tombstone / 空スロット: feed(key, packed_tid=その時の tid, found=0)
prefetch と commit re-scan で **同一の feed 関数 + 同一 range = footprint 一致**を保証。

## 実装ステップ(チェックリスト)
- [ ] S1. sha256.hh(done, server/rpc/sha256.hh)
- [ ] S2. stateless.h: ExternalRangeHashEntry 型 + StatelessRangeScan に footprint hash out 追加
- [ ] S3. database_impl.h: feed_footprint 共有関数 + StatelessRangeScan 内で hash + ValidateAndCommit に range_hashes param + 検証ブロック(re-scan して hash 比較)
- [ ] S4. database.{h,cpp}: ValidateAndCommit / StatelessRangeScan wrapper signature 更新
- [ ] S5. tx_occ_store.hh: TxOccState に range_hashes(descriptor + root)
- [ ] S6. lineairdb_rpc.cc: prefetch で primary full range の hash を計算し TxOccState に格納; commit で range_hashes を ValidateAndCommit へ
- [ ] S7. proxy: read-only + gate 時、primary full-cover range の per-row read 記録を skip(range-hash が代替)。write tx 判定で fallback
- [ ] S8. 検証: gate OFF で 22/22 md5(回帰なし)→ gate ON で 22/22 md5 + commit/walltime 計測 + Codex 推奨テスト(値 update 検知が効くこと)

## 実測(各ステップ後に追記)
(以下、実装進行に伴い記録)

## 実装結果(2026-05-29)

### 実装完了分
- S1 sha256.hh → third_party/LineairDB/src/util/sha256.hh(header-only, public-domain)
- S3 database_impl.h: ComputePrimaryRangeFootprintHash(footprint = visible+filtered+tombstone を
  key 順に (key,packed_tid,found) で SHA-256。**epoch mgmt は caller 依存**=nested MakeMeOnline 禁止)
- S4 database.{h,cpp}: wrapper
- S5 tx_occ_store.hh: TxOccState::RangeHash(descriptor + root[32])
- S6 lineairdb_rpc.cc: prefetch で full-cover primary range の hash 計算→TxOccState 格納;
  commit で use_range_hash 時 re-derive+memcmp 比較→mismatch で abort
- S7 proxy: rangehash_eligible_(gate && SELECT)時、full-cover serve の per-row read 記録 skip +
  commit で use_range_hash=true
- proto: TxValidateAndCommit.Request.use_range_hash=10

### 計測(gate ON, Q1/Q21 = 主ターゲット)
```
       baseline(per-row)  range-hash    correctness
Q1     41.3s/commit 6.78  29.2s/1.76s   md5 == InnoDB ✅
Q21    50.2s/commit 8.40  34.7s/2.39s   md5 == InnoDB ✅
```
commit -72%, walltime -21〜29%、**md5 一致(OCC 正しさ維持)**。
value-update 検知は TID を hash に含むので構造的に保証(live 並行テストは pause hook 要、別途)。

### 既知の不具合(未解決, gate OFF が default なので本番影響なし)
22Q gate ON で **Q2, Q5 が regress**(Q2: prefetch で ONESHOT-MISS abort, Q5: 138s retry storm,
両 md5✗)。gate OFF では 22/22 ✅。
**切り分け**: mysqld gate OFF + server gate ON(= server は hash 計算するが proxy は skip しない)
→ Q2/Q5 **PASS**。よって犯人は **proxy 側の per-row read skip**(server 側 prefetch hashing は無罪)。
- 当初疑った prefetch の epoch nesting は除去済(MakeMeOnline/Offline を helper から削除)だが
  Q2/Q5 は直らず
- record_stateless_read は validation 専用(serving は別 local_read_set_)なので、skip が直接
  cache miss を起こす単純な機序が見えない → 想定外の相互作用。正確な root cause 未特定
- 仮説: full-cover entry から serve した範囲の per-row read を skip すると、同 tx 内の後続
  point-read/probe の何らかの前提(同一行の二度読み conflict check 等)が崩れる?要追加調査

### スコープ判断
- 全て HELIOS_RANGEHASH_OCC gate。**default OFF = 安定 22/22 に影響なし**
- Q1/Q21 で「correctness を保ったまま commit -72% / walltime -25%」を実証 = アプローチは有効
- Q2/Q5 の proxy-skip 機序解明が残課題。本番化には要解決

## 追記(2026-05-29): Q2/Q5 regression の切り分け完了 + SF=0.1 検証

### 切り分け結果
- **SF=0.1 で全 22Q: gate-ON == gate-OFF (md5 完全一致, FAIL 0/22)** → Phase-6 は SF=0.1 で正しい
- **regression は SF=1 固有**(Q2/Q5 は SF=1 gate-ON でのみ abort/retry)
- proxy gate OFF + server gate ON → Q2/Q5 PASS → 犯人は proxy 側 per-row read skip(確定)

### root cause(確定的方向)
- SF=1 の Q2: 相関サブクエリが partsupp を (ps_partkey,ps_suppkey) で point-read。多くは不在キー
  → read() の **negative caching**(find_negative_covering_range_scan)が不在証明して not-found を返す
- negcache は `kNegativeMembershipCap = 8192` を超える result_keys の range では**諦める**(線形membership走査が高コスト)
- **SF=1**: partsupp 覆い range の result_keys > 8192 → negcache 諦め → read() miss → abort-on-miss
- **SF=0.1**: result_keys < 8192 → negcache 機能 → OK
- gate-OFF SF=1 が通るのは、skip しない per-row read 記録が後続 point-read の serving を間接的に
  担っていた(正確な経路は未特定だが、skip がこの serving を奪い negcache へ fallback → cap)

### 結論
- **Phase-6 は SF=0.1 で correctness 完全(22/22)**+ SF=1 で Q1/Q21 commit -72%/walltime -25% 実証済
- 残課題: SF=1 で full-cover skip が negcache cap と衝突。本番化には「skip した range の後続
  point-read を full-cover cache から直接 serve する(negcache に頼らない)」修正が必要。
  SF=1 でのみ再現するので調査は SF=1 が要る
- 全て gate(HELIOS_RANGEHASH_OCC, default OFF)。本番安定性に影響なし

## 追記(2026-05-29): Q2 abort の真因確定 — **range-hash gate は無罪**(誤帰結の訂正)

### 制御実験(同一サーバ=同一データ、固定 canonical Q2、mysqld の gate だけ flip)
- 固定 Q2(p_size=15, p_type LIKE '%BRASS', r_name='EUROPE')を SF=1 ロード済データに直接実行
- **gate ON**: `[ONESHOT-MISS] read tbl=partsupp keylen=16 ... -> ABORT`(Deadlock)
- **gate OFF**(同一サーバ、mysqld のみ再起動、データ不変): **同じく ABORT**(partsupp point-read miss)
- InnoDB(3308)は同じ Q2 を**エラー無しで空結果**を返す → helios は本来通る query を誤って abort
- → **gate ON/OFF で挙動同一。以前の「gate ON で Q2/Q5 regress」は誤帰結**。原因は benchbase の
  **query param ランダム化 + 非 seed reload** で gate ON/OFF を別条件で比べていたアーティファクト

### 真因(コードで確定)
- FER/FES の for_each sub-scan ingest(lineairdb_transaction.cc:479-512)は各 sub-scan を
  **`push_local_range_scan` で range entry にのみ格納**し、**`record_local_read` を呼ばない**
  → FER prefetch 行は **local_read_set_ に一切入らない**(primary full scan path:443 は record_local_read を呼ぶので両方に入る、と対照的)
- Q2 の partsupp は FER(ps_partkey 前置 sub-scan、part ごと)で prefetch される
- optimizer/handler が partsupp に対し **full-PK point-read**(read(), 16B key)を発行すると、
  read() は local_read_set_ を miss → negcache → **physical OCC mode は logical result_keys 不在で
  不在証明不可(any_logical=false で常に nullptr)** → note_oneshot_miss → abort
- 行は実在し prefetch 済(range entry の中)だが read() が range entry を参照しないため miss 扱い
- 過去の「22/22」は param ランダム化が prefix-range access(range entry で serve 可)を引いた幸運。
  固定 param は point-read 経路を引いて確実に abort = **pre-existing な oneshot planner gap**

### 修正方向(Codex 相談中, /tmp/codex_q2_prompt.md)
- A) read() で miss 前に covering range entry を引いて PRESENT な行を serve(positive covering、OCC obligation 記録)
- B) FER ingest でも record_local_read を呼ぶ(Q21 19.5M 行の RAM 重複が懸念)
- C) ingest 時に key→(range idx,row idx) index を張り、値複製せず read() point-read を range entry 経由で解決

## 修正実装(2026-05-29): A 案(低メモリ positive/negative covering)

実際の abort は **2 種**あった(PCR debug で確定):
- (i) 実在する FER prefetch 行への full-PK point-read が local_read_set_ を miss
- (ii) **不在**の (part,supplier) ペア(Q2 相関 MIN が引く)の point-read が negcache に落ちるが
  physical mode で不在証明不可

### 変更(proxy/lineairdb_transaction.{cc,hh})
1. `lookup_positive_covering_range_row(table,key)` 新設: covering range entry の sorted `rows` を
   binary search、exact key 一致で {entry,row_idx} を返す。reverse_scan entry は skip(昇順前提)。
   row_tids.size()==rows.size() 要求。read() / batch_read() で local_read_set_ の後・negcache の前に呼ぶ。
   hit 時は値を serve + `record_stateless_read(key,true,row_tid)`(per-row TID で value-update 検知)。
2. `find_negative_covering_range_scan` を physical mode 拡張: any_logical==false の時、entry が
   **unfiltered かつ row_limit==0 かつ end_key 非空 かつ reverse でない**なら `rows` 全集合(binary search)
   で不在証明。range_versions(physical=node-version)で phantom 保護。旧 8192 membership cap も回避。
3. batch_read に positive + negative covering 両方を追加(Codex 指摘 a)。

### Codex レビュー(/tmp/codex_q2fix_review.md → /tmp/codex_fix_review.out)
**ブロッカー無し。Part1/Part2 とも OCC 健全。**
- Part2 phantom-soundness: StatelessRangeScan が visit_leaf で全 leaf version 捕捉、commit の
  ValidatePhantoms で再検証、tombstone も index_reads でカバー → insert-escape window 無し(コード追跡確認)
- Part1: existing-key point read は per-row TID 記録のみで十分(range activate 不要、false-abort を増やすだけ)
- FER sub-scan は forward(server StatelessRangeScan(...,false))→ binary search OK
- membership cap 除去は correctness-safe(完全 key set + O(log n))
- reverse_scan skip は正しいガード
- 残: slice_range_entry_fast も昇順前提で reverse ガード無し(本修正の範囲外、将来課題)

### 検証(SF=1, 固定 v_*.sql vs InnoDB md5)
- **gate OFF: 22/22 md5 ✅ bit-level 一致、ONESHOT-MISS 0**(reverse_scan ガード版・batch-negcache 版とも)
- **gate ON(range-hash 有効): 22/22 md5 ✅**(Q2 fix と range-hash の両立確認)
- Q2 は修正前 abort → 修正後 101 行 ✅。gate 無関係なので default(gate OFF)でこの correctness bug が解消。
  range-hash 機能(gate ON)も同時に correctness 維持。
