# Phase-7: read-only no-validation モード(gated)

## 背景
ユーザーの先生の指摘:「TPC-H = read-only。read-only なら**トランザクションを考える必要がなく、validation も TID 送信も要らない**」。
これを起点に「どこで判断すべきか(QP インスタンス/システム全体/クエリ)」を InnoDB 調査で決定。

### InnoDB の決定ロジック(trx0trx.cc:1327-1335, trx_start_low)
```
trx->read_only = (api_trx && !read_write)
              || thd_trx_is_read_only(thd)   // ② START/SET TRANSACTION READ ONLY
              || srv_read_only_mode;          // ③ system-wide
if (auto_commit && will_lock==0) read_only = true;  // ④ autocommit 単一・非ロック SELECT を自動判定
```
= **3レイヤ(system / session-tx / 自動判定)を階層使用、auto-detect が既定**。
**決定的差**: InnoDB の read-only 最適化は **write 機構だけ剥がす**(trx_id/undo/rseg/rw-list)。読みは **MVCC read view で常に一貫スナップショット**なので isolation は下がらない。
helios は **MVCC 無し** → validation skip は **isolation を実際に下げる**(read-committed 相当)。

## Codex 判定(2026-05-29)
**GO、ただし「明示オプトインの弱い読みモード」として。InnoDB 風の透過的自動最適化としては NO-GO。**
`START TRANSACTION READ ONLY` や素の autocommit SELECT で自動有効化してはいけない(ユーザー同意必須)。

### 文書化する保証
> `READ ONLY NO VALIDATE` は read-plan RPC 中に観測したコミット済み行バージョンの寄せ集めを返す。
> 各行値は copy 時点で安定だが、**全体はスナップショットでなく、並行 writer 下では直列化可能でない**。
> 許容異常(writer 並行時): 文内 non-repeatable read / read skew(共存しなかったバージョンの混在)/ phantom /
> secondary↔base 不一致。**並行 writer が無ければ(=TPC-H ベンチ)無条件に正しい。**

### Codex 指摘の実装要点
1. RCU セクションは memory 回収を防ぐだけで snapshot でない(論理バージョン非固定)。
2. **lifecycle**: commit を単純 skip すると read-plan が登録した tx pin + TxOccState が connection close/TTL まで残り
   RCU 回収を阻害。→ **no-validation read-plan は retained state を作らない**(server: tx_occ_key=0、
   TxOccStore.Insert skip、RegisterTxEpochPin skip、range hash skip)。prefetch 終端で self-release(別 RPC 不要)。
3. **proxy のキャッシュ判定が validation token を要求**(borrowed full-cover=row_tids==rows / positive covering=
   row_tids / range slicing=range_versions 非空 / negative covering=phantom token)。TID/range_versions を抑制するなら
   これらを「coverage-only」に緩める必要(さもないと cache miss→abort)。range-hash は別物(検証する最適化)なので
   no-validation 時は無効化。
4. **gate**: `rangehash_eligible_`(単なる SELECT)流用不可。専用 var + `thd_tx_is_read_only()`(汎用 plugin API)。
   安全 gate = 明示 ON + SQLCOM_SELECT + autocommit/非BEGIN/非TABLE_LOCK + 非ロック + 書き込み皆無(出たら abort)+
   プラン生成失敗時は OCC fallback。

## 実装方針(段階)
- **Stage 1(まず測る)**: server に no-retain フラグ(tx pin + TxOccStore.Insert を skip、self-release)。
  proxy に ro_novalidate モード(record_stateless_read / activate_range_validation を skip、commit 検証 RPC を no-op=
  2-RPC→1-RPC)。**当面 server は row_tids/range_versions を送り続ける** → proxy のキャッシュ判定はそのまま通る(predicate-relax 不要)。
  狙い: commit ~2.6s + record/activate ~9% + commit round-trip 1回の除去。
  gate: env `HELIOS_RO_NOVALIDATE`(default OFF)+ SQLCOM_SELECT + 書き込み皆無。
  （本番 surface は session/global var + thd_tx_is_read_only。ベンチ計測用に env gate。）
- **Stage 2(効果次第)**: server で TID/range_versions/index_reads/filtered/tombstone を suppress(payload/proto_copy/
  ingest 削減)+ proxy 判定を coverage-only 化。range-hash は no-validation 時 disable。

## 実装結果ログ
(以下、各 Stage の測定をここに追記)

### Stage 0(proxy-only 計測)— 2026-05-29 → 採用(gated 計測モード、default OFF)
実装: gate `HELIOS_RO_NOVALIDATE=1` + SQLCOM_SELECT で tx->ro_novalidate_。3ガード:
record_stateless_read / activate_range_validation を早期 return、commit は write 無しなら
tx_validate_and_commit を skip(2-RPC→1-RPC)。write 混入時は hard-abort(Codex Q3)。
server 無変更(TID/range_versions は送り続ける→proxy 判定温存、predicate-relax 不要)。
測定(SF=1, 22/22 md5 OK):
```
            #2(直前)         RO_NOVALIDATE
Q21         17428ms/4.11GB → 13575ms/2.66GB   (-22% time, -35% mem)
```
メモリは全クエリ減(per-row TID/range_validation 構造を作らない)。累積 Q21: 52s→13.6s(3.8x)/
mysqld 23GB→2.66GB(8.6x)。
Codex GO(default-OFF 計測モードとして commit 可。read 結果に skip 状態依存なし、leak は
connection-close/TTL で bounded・UAF/crash/誤結果なし)。
**本番 NO-GO until Stage 1**: server no-retain フラグ(tx_occ_key=0 / TxOccStore.Insert skip /
RegisterTxEpochPin skip)+ 本物の read-only 契約 gate(thd_tx_is_read_only / session-global、
非ロック、write surface 無し)。理由: Stage 0 は commit skip で TxOccState/pin が
connection-close/TTL まで残る(全クエリ Stage 0 の持続負荷で遅延回収/メモリ増。UAF は無い)。

### Stage 1(server no-retain + range-hash skip)— 2026-05-29 → 採用(production-correct)
proto: TxExecuteReadPlan.Request に bool read_only_no_validate=2。proxy: tx_execute_read_plan に
引数追加、tx が ro_novalidate_ を渡す。server: ro_novalidate 時に RegisterTxEpochPin + TxOccStore.Insert +
set_tx_occ_key を skip(tx_occ_key=0、cleanup=tls reset/physical-mode off は維持)→ **leak 解消**。
さらに Codex 指摘の dead work のうち最重量の **range-hash footprint SHA-256(6M行)を ro_novalidate で skip**
(digest は未検証なので無駄)。
測定(SF=1, 22/22 md5 OK):
```
            Stage0           Stage1(no-retain)   Stage1+rh-skip
Q21         13575ms/2.66GB → 12654ms/2.66GB    → 10934ms/2.68GB
```
累積 Q21: 52s→10.9s(4.8x)/ mysqld 23GB→2.68GB(8.6x)。
Codex GO(lifecycle/UAF 問題なし:pin/state 未登録で leak なし、node_ptr は proxy が deref しない、
phys_state の use-after-scope なし、tx_occ_key=0 で proxy commit skip 前なので無害)。
残 dead work: filtered_rows routing(physical 出力に絡む)→ Stage 2 で node_versions/TID suppress と併せて。
