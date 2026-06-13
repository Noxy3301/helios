# Phase 21 / Step ② — CREATE INDEX backfill 設計(dual-review 対象 v1)

disaggregated Helios(MySQL plugin "proxy" ↔ RPC ↔ in-memory "LineairDB", CC=Silo single-version OCC)で、
**既にデータの入った表**への `CREATE INDEX` / `ALTER TABLE ADD INDEX` が新規 secondary index を既存行から
**正しく**充填する設計。correctness 最優先。実装前の設計のみ。Claude(grounded)+ Codex の並列調査を統合。

## 1. 問題(コード裏取り済み)
現 `CREATE INDEX` は index を空のまま登録 → optimizer が空 index を使い**誤/空結果**:
- `ha_lineairdb.cc:3862 inplace_alter_table` は `db_create_secondary_index` を呼ぶだけ。
- `table.h:21 CreateSecondaryIndex` は空 struct 生成のみ(既存行走査・充填なし)。
- 空 index への scan(`stateless/read.cpp:87 StatelessSecondaryRangeScan`)は 0 件 → 誤/空。
- 正解 DML 経路: `write_row`(`ha:2006`)が `build_secondary_key_from_row`(`lineairdb_keyenc.cc:213`)で
  secondary key を作り `write_secondary_index` 発行。**この符号化が唯一の真実**(DATE=ASCII, 整数順序保存,
  NULL, 複合連結, prefix)。backfill はこれと byte 一致が必須。

## 2. 実システム調査(citations)
- **InnoDB online DDL ADD INDEX**(WL#5526): prepare(短 X lock)→ build(clustered scan + sort-merge, DML は
  online log に捕捉)→ commit(log replay + 短 X swap)。「committed snapshot を走査 + 並行 DML を別 log + atomic publish」。
- **PostgreSQL CREATE INDEX**: 通常は writes を lock して 1-scan build(並行変更が無いので一発正しい)。
  CONCURRENTLY は invalid 登録 → 2-scan + snapshot 待ち、失敗で invalid index 残存。**「完成まで不可視」= ready-gating**。
- **in-memory/OCC 系**: Hekaton は memory-optimized 表の index を**全て offline build**(online 無し)。
  MyRocks は「**index drop → load → load 後 add index**」推奨 = afterload と同型。
- 共通不変条件: 完成 index は**その時点の committed 行と厳密一致**(全 committed 行が 1 回, phantom/dup 無し)。

## 3. 設計決定(correctness 最優先)
### 並行性: **exclusive-lock offline build**(決定)
`check_if_supported_inplace_alter` が既に `HA_ALTER_INPLACE_EXCLUSIVE_LOCK`(`ha:3859`)を返し、MySQL レイヤが
DDL 中の当該表 DML を止める。afterload は bulk load 後・並行 DML ゼロ。→ build 中の committed 行集合は不変、
**1-scan 充填が自明に正しい**(Silo 単一バージョンでも online log/2-scan 不要)。Hekaton 前例と一致。

### どこで/どう充填: **A2(handler-driven, canonical encoder 再利用)を first cut に採用**(決定)
| | A1 server-side scan | **A2 handler-driven(採用)** |
|---|---|---|
| 符号化 | server が `serialize_key_from_field` を**再実装** → DATE/整数/NULL/複合で 1 byte ずれたら index 破壊(最大リスク) | `inplace_alter_table` が表を scan し各行 `table->record[0]` から **`build_secondary_key_from_row` をそのまま呼ぶ** → 符号化が DML 経路と**自動 byte 一致**(drift ゼロ) |
| 書込 | server 内 直接 Put | 既存 `write_secondary_index`/buffer 経路を再利用 |
| 速度 | 速(転送0) | 遅(全行 scan + index write RPC, batched) — だが **afterload は一度きり・計測対象外**なので許容 |
| 実装 | server/LineairDB に scan+encode+publish 新規 | handler 内で scan+既存 write 経路 |
| correctness | 二重実装 drift リスク高 | **canonical 経路再利用でリスク最小** |

**根拠**: correctness 最優先 + afterload は非計測の一度きり前処理 → 速度より「符号化 drift ゼロ」を取る。
A1(server-side, 高速)は将来最適化(符号化を proxy/server 共有の単一ソースに切り出した後)に回す。

### フロー(A2)
`inplace_alter_table`(各 add index について):
1. `db_create_secondary_index`(空登録, 既存)。
2. **当該表を全件 scan**(handler の read 経路 rnd_init/rnd_next で `table->record[0]` に各行を読む。DDL 文脈なので
   prefetch でなく素の scan)。
3. 各行で **`build_secondary_key_from_row(record, new_key_info)`** → `write_secondary_index`(UNIQUE は即重複検出)。
   batch flush。
4. **全行完了まで同期**(backfill が server 上で終わるまで return しない)。成功 → return false / 失敗 → error を返し
   MySQL に DDL 失敗させる(index は DD に登録されず**不可視**)。
→ **可視性 gate = inplace_alter の同期完了**(MySQL は false 受領で初めて index を DD commit → query で使用)。
排他 lock 下で並行 query 無し → 途中の partial index は誰にも見えない。

### ready-gating(防御的・推奨だが first cut は任意)
single-node では「同期完了」で十分。disaggregated 多ノード/中断耐性のため server 側 `ready_` flag(create=not-ready,
backfill 完了で atomic に ready)を**後続で**足すと、空/partial index を構造的に読ませない。first cut は同期 gate のみで可。

## 4. correctness 不変条件(厳密)
1. **完全性**: 全 committed 行 r に対し index は `(build_secondary_key_from_row(r), r.PK)` を**ちょうど1個**含む。
2. **健全性**: index の全 entry は live 行に対応(phantom/tombstone 無し)。
3. **符号化一致**: backfill の key bytes = `build_secondary_key_from_row` 出力と**byte 一致**(A2 は同関数再利用で自動充足)。
4. **UNIQUE 整合**: UNIQUE は各 key に PK 高々1, 違反で DDL 失敗(不可視)。
5. **load-built 等価**: 「CREATE TABLE 宣言込み load」した index と「load 後 CREATE INDEX backfill」した index が**完全一致**。
6. **可視性**: 上記充足まで index は optimizer 不可視(同期 gate / DDL 未成功)。

## 5. リスクと緩和
- **符号化 drift**: A2 採用で**回避**(canonical encoder 再利用)。A1 に最適化する時のみ再浮上 → 符号化単一ソース化が前提。
- **build 時間/RPC**: SF1 lineitem 600万 × 索引で長時間 blocking。index ごと分割 + batch write + DDL 用 timeout 延長。
- **DROP/再 CREATE 相互作用**(Step ③ と関連): DROP が no-op で stale entry が残ると二重充填。**CREATE 時に
  「既存 index があれば clear してから backfill」or DROP purge を併設**して clean slate を保証(本 step で最低限の
  "既存なら空に" を入れる)。
- **prefetch×SI×view correctness(q15, Step ④)**: backfill が正しくても残る別バグ。検証は **prefetch ON/OFF 両方**で。
- **MySQL 無改変**(memory helios-mysql-no-modification): handler/proxy/server/LineairDB 内に収まり抵触なし。

## 6. テスト計画
1. **load-built vs backfilled 全件等価(主軸)**: 同データで (a) 宣言込み load の index と (b) load 後 backfill の index を
   `StatelessSecondaryRangeScan` で全件ダンプ → `(sk, sorted PK list)` 完全一致(不変条件 5 を直接検証)。
2. **TPC-H md5**: afterload 索引一式 → q1-22 が InnoDB と md5 一致。q21/q15 重点。**q15 は prefetch ON/OFF 両方**
   (backfill バグと prefetch×SI バグの切り分け)。
3. **count by indexed col**: index 経由(FORCE INDEX)と full scan で件数一致。
4. **UNIQUE**: 重複ありで CREATE UNIQUE INDEX → DDL 失敗 & 不可視。重複無し(c_ck 等)は成功。
5. **可視性 gate**: 失敗 DDL 後に optimizer が index を使わない/誤結果を返さない。
6. **edge**: NULL 列, 複合(l_sk_pk), DATE 列(o_orderdate/l_shipdate=ASCII), 空表。
7. **再 CREATE**: CREATE→(DROP)→再 CREATE で stale/二重 entry 無し。

## 7. 結論(RECOMMENDATION)
**A2 handler-driven backfill(`inplace_alter_table` で表を scan し `build_secondary_key_from_row` +
`write_secondary_index` を再利用)+ exclusive-lock offline(既存)+ 同期完了 gate**。correctness 最優先 =
符号化 drift ゼロ・canonical 経路再利用。afterload は一度きり非計測ゆえ速度より正しさを取る。検証主軸は
**load-built index との全件等価**。server-side(A1, 高速)は符号化単一ソース化後の将来最適化。
ready flag は防御的後続。DROP purge(Step ③)と「CREATE は clean slate」の保証を併せて要設計。

参照: proxy/ha_lineairdb.cc(:3862 inplace_alter, :2006 write_row, :3859 EXCLUSIVE_LOCK),
lineairdb_keyenc.cc:213(canonical encoder), proxy/lineairdb_proxy.cc:794(tx_write_secondary_index),
server/rpc/lineairdb_rpc.cc:2864(handleDbCreateSecondaryIndex), third_party/LineairDB/src/index/secondary_index.h,
src/table/table.h:21, benchbase tpch/postload-mysql.sql, docs/phase17_server_worklog.md:1095-.

---

# v2 改訂(dual-review 反映 — Codex + Claude grounded)

両 review = CHANGES-NEEDED。方向性(A2 handler-driven / exclusive-lock offline / 同期 gate)は GO 支持。
以下の必須修正を反映して v2 とする。

## 🔴 [CRITICAL] FIX-1: KEY の出所を `altered_table->key_info[]` にする(fieldnr 規約)
- `build_secondary_key_from_row`(keyenc.cc:226)は `table->field[key_part.fieldnr - 1]`(**1-based** = runtime
  `TABLE::key_info` 規約)。
- `inplace_alter_table` の `ha_alter_info->key_info_buffer[idx]`(ha:3875)は **0-based fieldnr**(MySQL 本体は
  `field[fieldnr]` を -1 なしで引く: sql_table.cc:12521、規約差は handler.h:3434-3442 に明記)。
- そのまま canonical encoder に渡すと **全 key part が1列左を読む** → crash せず UNIQUE 違反も出さず**静かに index 破壊**。
- **修正**: backfill は `key_info_buffer` でなく **`altered_table->key_info[]` の対応 KEY**(1-based, encoder と整合)を
  使う。純 ADD INDEX で列増減が無いので `altered_table` と `table` の field 配置は同一 → record[0] はそのまま使える。
- **受け入れ条件**: テスト 6.1(load-built vs backfilled の全件 `(sk, sorted PK)` 等価)に **列順違い composite
  (`l_pk_sk`/`l_sk_pk`, `ps_pk_sk`/`ps_sk_pk`)を必須**化(md5 だけでは列取り違えを取りこぼす)。

## [必須] FIX-2: backfill を行 chunk 分割 commit
- SF1 lineitem 600万行 ×(特に UNIQUE)を**単一 tx** backfill すると、既知の **protobuf 2GB framing
  (helios-prefetch-flat-codec-2gb)/ OCC read-set 肥大(helios-occ-rangekeys-bloat)**に抵触し得る。
- **修正**: 行 chunk(例 数万〜数十万行)ごとに write をためて commit、を繰り返す。
- **不変条件 追加(完全性の前提)**: chunk 分割しても **EXCLUSIVE lock 解除前(=inplace_alter が success を返す前)に
  全 chunk 完了**すること。途中 commit 済み + 未 commit の中間状態は lock 下で不可視。

## [追記] FIX-3: handler 自己 scan の `inited` 状態管理
- `inplace_alter_table` は original handler(`table->file`, open 済み・EXCLUSIVE lock 下)で呼ばれ、commit phase で
  同 handler を再利用する。backfill 用の scan(rnd_init/rnd_next or 低レベル batched scan)が handler の `inited`
  状態を汚さないこと(scan 後に確実に reset、or 専用経路)。DDL は prefetch 無効(prefetch.cc:46)なので
  素の `fetch_next_batch`(prefix("") 全件)が無改変で使える。

## [追記] FIX-4: commit-abort 耐性 と「定義先行」hazard
- ALTER の statement tx は DDL 末尾の `trans_commit_stmt`→`lineairdb_commit`(ha:3506)→flush で commit(明示 flush 不要)。
  OCC abort 時は `lineairdb_commit` が `HA_ERR_LOCK_DEADLOCK`(ha:3522)→ ALTER 全体 rollback。afterload は並行 DML
  ゼロなので実 abort 確率は低い。
- **hazard**: `db_create_secondary_index`(定義)は **非トランザクショナル**で backfill より先(ha:3882)。backfill が
  abort/crash すると「定義あり・entry なし」の空 index が server に残る。single-node では可視性 gate(DD 未 commit)で
  optimizer は使わないが、multi-node や restart で空定義を拾う hazard。→ first cut は single-node 前提で許容、
  ready flag(server 側)を防御として後続。失敗時は可能なら index 定義も clear(FIX-5 の primitive 流用)。

## [前提繰り上げ] FIX-5: clean-slate(③ DROP purge を ② の co-requisite に)
- whole-index clear / DROP purge primitive は**現存しない**(table.h:21 CreateSecondaryIndex は既存なら false 返すのみ、
  secondary_index.h は per-key Delete/Purge のみ、DROP INDEX は ha:3873 で無視)。→ **再 CREATE で二重充填 / stale**。
- **first 計測は fresh load(server 再起動→ロード)→ CREATE INDEX 1回**なので再 CREATE せず回避可(blocker でない)。
- ただし robustness のため **whole-index clear(or DROP purge)RPC を Step ② に取り込む**(Step ③ 前倒し)。最低限
  「CREATE 時に同名 index があれば server 側で空にしてから backfill」。これが入るまでは「dirty server で再 CREATE しない」
  を運用制約として明記。

## v2 フロー(確定)
`inplace_alter_table`(各 add index):
1. （clean-slate primitive があれば）同名既存 index を server で空に。無ければ fresh load 前提。
2. `db_create_secondary_index`(空定義、既存)。
3. **`altered_table->key_info[]` から対応 KEY を取得**(FIX-1)。
4. 表を **素の batched scan**(prefetch 無効、`inited` 非汚染、FIX-3)で全件、`table->record[0]` に各行復元。
5. 各行 `build_secondary_key_from_row(record, key)` → `write_secondary_index`。**chunk ごとに commit**(FIX-2)。
   UNIQUE は重複検出で即失敗。
6. **全 chunk 完了まで同期**(lock 解除前)。成功 → return false(MySQL が DD commit=可視化)/ 失敗 → error(DD 未 commit
   =不可視、可能なら空定義も clear)。

## v2 テスト(更新)
- 6.1 を**列順違い composite 必須**で受け入れ条件化(FIX-1 検証)。
- chunk 分割 backfill が SF1 で完走(memory/2GB 非抵触)。
- 他は v1 の §6 を踏襲(load-built 等価主軸、TPC-H md5 prefetch ON/OFF、UNIQUE 失敗不可視、DATE 列、空表)。

## v2 結論
A2 handler-driven + exclusive-lock offline + 同期 gate を維持しつつ、**FIX-1(altered_table->key_info, CRITICAL)・
FIX-2(chunk 分割)・FIX-3(inited)・FIX-4(abort/定義先行)・FIX-5(clean-slate を ② co-req)** を反映。build 自体は
「scan + canonical 経路で各行のキーを入れる」のままシンプル。heavy machinery(generation/online/ready 必須)は afterload
単一ノードでは不要。再 review にかけて GO を確認する。

---

# v2.1(re-review GO 反映 + multi-node DDL-sync 接続)

両 re-review: Codex=GO / Claude(grounded)=CHANGES-NEEDED(軽微1点)。以下を反映で **GO**。

## must-fix(GO 条件): FIX-1 の KEY 特定を「index 名マッチ」に明記
v2 手順3「`altered_table->key_info[]` の対応 KEY を取得」の **対応の取り方**が未記述だった。
- `ha_alter_info->index_add_buffer[i]`(ha:3874)は **`key_info_buffer` への offset であって `altered_table->key_info[]`
  の添字ではない**(sql_table.cc:11918-11919 のコメント)。`altered_table->key_info[index_add_buffer[i]]` と直接
  添字すると別 index を指し得る(既存 index 混在 ALTER で顕在化)→ FIX-1 が潰した「静かな index 破壊」を別経路で再導入。
- **正**: `key_info_buffer[index_add_buffer[i]].name` で `altered_table->key_info[0..s->keys]` を **name 走査して
  マッチ**(InnoDB も name マッチ: handler0alter.cc:2688)。マッチした KEY は 1-based fieldnr(runtime 規約、
  encoder と整合: table.cc:686 `fieldnr=field_index()+1`)。
- 受け入れテスト 6.1 に「**複数 index 同時 ADD で 1 つが既存 index と隣接**」を1本追加(name マッチ欠落も捕捉)。

## 実装 must-watch(GO 後)
1. **FIX-2 chunk 境界は「tx end」でなく `flush_write_buffer`(buffered SI writes を RPC 送出)**。OCC tx の
   commit/begin は ALTER 末尾の1回(`lineairdb_commit` ha:3506 が参照する `ctx->tx` ライフサイクルとの干渉回避)。
   2GB framing 回避(chunk ごと送出)と tx 干渉回避を両立。UNIQUE は write_row 非 prefetch 経路(ha:2036, 即時
   `write_secondary_index`)で chunk 内重複検出 → 失敗で error 返却(DDL 失敗・不可視)。
2. abort 時に MySQL-DD 未コミットで rollback / engine 側 index 定義が restart・fresh-load を跨いで残らない。
3. chunk 境界で scan cursor / handler `inited` が行 skip/重複を起こさない。EXCLUSIVE lock を**最後の chunk まで**保持。
4. doc 参照訂正: prefetch DDL gate は `prefetch.cc:46` でなく **`proxy/ha_lineairdb.cc:482`**(sql_command!=SELECT→false)。

## multi-node DDL-sync(本来の前提・本設計の visibility の最終形)
Helios の本題は **複数 MySQL クエリ層 + 単一 fast storage で水平 scale**(~/helios CLAUDE.md: Strict
Serializability/1SR 維持・read-intensive scale-out)。よって single-node 前提は first-cut の簡略化で、本来は **multi-node
DDL 同期**が correctness の肝。既存方針(~/helios architecture.md:89「**Stats/DDL 同期を BEGIN/END レスポンス
相乗り**」、stats は 2026-04-01 実装済・DDL は予定 / troubleshooting.md:27)+ user 提案の強化版:
> **ストレージが DDL+version を保持。MySQL は全 RPC に自分の DDL 版を乗せる。版が古ければストレージが reject →
> 現行 DDL を返す → MySQL が再適用してから retry。giant lock 不要・双方ステートレス風。**

→ **本 backfill 設計で「後回し」とした server-side ready flag は、この DDL-version-sync に包含される**:
index が ready になった時点でストレージの **DDL version が bump**、stale 版の他 MySQL ノードは reject されて resync し
新 index(ready 済)を拾う。= per-index ready flag でなく **統一 DDL 版機構**で multi-node 可視性を担保。
**Step ② first-cut は single-node の同期 gate で実装**し、**multi-node 可視性はこの DDL-version-sync(別 step / 将来)
に乗せる**。backfill のサーバ状態(index 定義 + ready)は DDL version に紐付ける設計とする。

## v2.1 結論
**GO**(上記 name-マッチ明記で Claude の唯一の must-fix 解消、Codex は既に GO)。Step ② = A2 handler-driven +
exclusive-lock offline + chunk-flush + 同期 gate、single-node first-cut。multi-node 可視性は DDL-version-sync へ。

---

# Step② 実装 first-cut の SF0.1 検証結果(2026-06-14)

実装(ha_lineairdb::inplace_alter_table の A2 handler-driven backfill, FIX-1 name-match 反映)を build → SF0.1 で検証。
- **correctness ✅**: lineitem に l_sk/l_sk_pk が backfill で生成され、q21 が supplier 駆動(l_sk)plan に切替。
  **q21 md5(backfilled)= ground-truth(index 無し)= `d36a1caf7da30bf792c4cbb7e9682823` 完全一致**。
  → FIX-1(name-match で altered_table->key_info の 1-based KEY を使用)が効き、silent index 破壊なし。backfill は
  正しい index を生成する、と実証。
- **🔴 perf 致命的**: CREATE INDEX が **14m57s(SF0.1, ~600k 行 × 2 索引)**。user/sys≈0=全 server/RPC 待ち。
  原因: 末尾の**単一 OCC commit で ~1.2M SI entry を一括 install**(known O(N²) commit dedup,
  memory helios-oneshot-dedup-on2-perf)。
- **結論**: FIX-2 の chunk-flush(1 tx 維持・commit は末尾1回)は **perf に不十分**。**chunk-COMMIT
  (chunk ごとに独立 tx で begin→write→commit)が必須**。これは「ALTER の statement tx を途中で触らない」という
  Claude の懸念と衝突するので、**backfill を ALTER の statement tx でなく自前の独立 tx 群で回す**設計に改める必要。
  あわせて read 側(fetch_next_batch の全行一括)も chunk 化(SF1 で 2GB framing 回避)。
- 次手: ① backfill を chunk-COMMIT 化(独立 tx)+ read chunk 化 → SF0.1 で perf 再計測(目標: 秒オーダー)→
  SF1 で md5 + perf → ② 実装 dual-review(GO まで)。correctness は first-cut で確認済みなので回帰させないこと。

---

# v2.2(chunk-COMMIT 設計 — O(N²) を grounding して独立 tx 群に分割)

## O(N²) の正確な所在(コード裏取り)
15min の真因は **commit の dedup でなく、書込ごとの per-write 線形走査**。`third_party/LineairDB/src/
transaction_impl.cpp::WriteSecondaryIndex`(:320-423)は **1 write ごとに**:
- `read_set_` を線形走査(:359-367, RMW 判定)
- `write_set_` を線形走査(:370-386, UNIQUE 制約 + 同一キー merge)

さらに非 UNIQUE は新規キーごとに `read_set_` に seed entry を積む(:388-409 `ReadUnvalidated`)。
→ 1 tx に N 件貯めると read_set_/write_set_ が N まで成長し、各 write が O(N) 走査 = **O(N²)**。
SF0.1 は index ごと ~600k 行 → 600k² ×2 索引 ≈ 7.2e11 比較 ≈ 15min。server-side(handleTxBatchWrite が
flush RPC 受信時に WriteSecondaryIndex を呼ぶ)で発生し、proxy は待つだけ(user/sys≈0 と整合)。

## 設計: 独立 tx 群で chunk-COMMIT
ALTER の statement tx(`ctx->tx` = `get_transaction(thd)`)は **read 専用**(全件 scan は ctx->tx の read_set を
作るが read validation は O(N) で支配項でない)。SI **書込は ctx->tx に貯めず、`kBackfillChunkRows` 行ごとに
独立した別 tx で begin→buffer→end(commit)** する。
- 各 chunk tx は `new LineairDBTransaction(thd, proxy, hton, FENCE)`(`#define FENCE false` = isFence=false,
  ctx->tx と同条件・per-chunk db_fence なし)→ `set_prefetch_mode(false)` → `begin_transaction()`(server で
  新 tx_id・`trans_register_ha` は同一 hton で冪等=無害)→ `choose_table` →
  `buffer_write_secondary_index`(WRITE_BATCH_SIZE で自動 flush)→ `end_transaction()`(flush+`db_end_transaction`
  +`delete this`)。
- 効果: 各 chunk tx の read_set_/write_set_ は chunk 内に限定 → write ごと走査 O(chunk)、全体 **O(rows×chunk)**。
  chunk=C, rows=N で N/C 倍高速化(C=5000, N=600k → 120x、15min→~7.5s 見込み)。

## correctness 論証(逐次・独立 tx で load-built と等価)
EXCLUSIVE lock 下・単一スレッド・**逐次** commit(`end_transaction` は同期 RPC、戻り時に Precommit/install 完了):
1. **非 UNIQUE merge-commute**: 同一 SI キー K が chunk A(行 r1)と chunk B(行 r2)に跨る場合、A commit で leaf={r1}。
   B は `GetOrInsertForWrite(K)` で既存 leaf を得(非 UNIQUE は initialized でも abort せず)、`ReadUnvalidated` で
   現値 {r1} を seed → delta Add r2 を merge-install(:397 "set add commutes")→ leaf={r1,r2}。逐次なので順序非依存で
   union 正しい。同一 chunk 内反復キーは write_set_ 走査(:370-386)が merge。
2. **UNIQUE 跨り検出**: A commit で K の leaf が **initialized**。B の `WriteSecondaryIndex(K)` は
   `index_leaf->IsInitialized() && IsUnique()` で **Abort**(:352)→ chunk tx commit=false → DDL を error 失敗
   (index は DD 未 commit=不可視)。同一 chunk 内の重複は write_set_ 走査(:374)が Abort。→ 全件 UNIQUE 整合維持。
   (本計測の l_sk/l_sk_pk は非 UNIQUE。)
3. **ctx->tx(read)と chunk tx(write)の非干渉**: 別 tx_id・別キー空間(base PK vs SI)。chunk tx の Precommit は
   自身の read_set(SI seed)のみ検証、ctx->tx の base-row read は SI 書込で不変 → 相互 abort なし。共有 proxy/socket は
   単一スレッド逐次 RPC なので衝突なし。chunk commit が proxy の current_trace を null 化するが trace は非 correctness。
4. → **不変条件 1-6(v1 §4)を維持**。md5 と「load-built vs backfilled 全件等価(列順違い composite 含む)」で実証必須。

## read 側 chunk 化(SF1)
`fetch_next_batch`(ha:2503)は `get_matching_keys_and_values_from_prefix("")` で **全行を 1 RPC** に載せる。SF1
lineitem 6M 行 ×~150B ≈ 900MB を 1 protobuf に詰めると 2GB framing(memory helios-prefetch-flat-codec-2gb)/メモリ
肥大の懸念。**backfill 専用の bounded PK-range scan ループ**(`get_matching_keys_and_values_in_range(cursor, "",
LIMIT)` で cursor を進める)で read も chunk 化する。SF0.1(~600k 行 ~90MB)は 1 RPC で問題なく、write-fix の検証は
先に SF0.1 で行い、read-chunk は SF1 robustness として続けて入れる。**hot path の汎用 `fetch_next_batch` は変更しない**
(全 full-scan に波及するため)。

## chunk サイズ
初期 `kBackfillChunkRows = 5000`(O(N×C) と commit RPC 回数 N/C のトレードオフ中庸、SF0.1 で ~120 commits/index)。
SF0.1 計測後にチューニング。

## v2.2 結論
write を独立 tx 群に分割(chunk-COMMIT)、read を bounded-range で chunk 化。correctness は逐次・独立 tx の
merge-commute / persistent-leaf-UNIQUE で load-built と等価を維持。SF0.1 で perf(秒オーダー)+ md5 不変を確認 →
SF1 → 実装 dual-review。

## v2.2 SF0.1 検証結果(chunk=5000, 2026-06-14)
chunk-COMMIT 実装(ha_lineairdb::backfill_commit_chunk + inplace_alter_table 書換)を build → SF0.1 で検証。
- **correctness ✅**: q21 md5(backfilled)= `d36a1caf7da30bf792c4cbb7e9682823` = ground-truth。**回帰なし**
  (chunk-COMMIT が単一 commit と同じ index を生成、name-match FIX-1 含め維持)。複合 l_sk_pk も生成。
- **perf**: l_sk(単一列)**8.2s** / l_sk_pk(複合2列)**22.9s** / 計 ~31s。**14m57s → ~29x 高速化**。
  user/sys≈0(server CPU = WriteSecondaryIndex の chunk 内 per-write 線形走査)。
- **観察**: 複合 l_sk_pk が l_sk の ~3x。理由 = chunk 内の **distinct key 数**が支配項。l_sk は suppkey 単独で
  SF0.1 では distinct ~1000 → chunk(5000)内で多数 merge され write_set_ は ~1000 止まり。l_sk_pk は
  suppkey+partkey でほぼ全行 distinct → write_set_ が chunk まで成長 = O(chunk²)。→ **distinct が高い索引ほど
  小さい chunk が有利**。SF1 は N が 10x なので O(N×C) が ~10x(l_sk_pk ~230s)に伸びる見込み → chunk 縮小 +
  read-chunking が SF1 で必須。
- 次手: chunk を 2000 に縮小 + read 側 chunk 化(bounded PK-range scan)→ SF0.1 再検証(md5 不変・perf 改善)→
  SF1 で md5 + perf。

## v2.2 SF0.1 再検証(read-chunk + write chunk=2000, 2026-06-14)
read 側を bounded PK-range scan(`get_matching_keys_and_values_in_range(cursor,"",50000)`、cursor=last+`'\0'`
で厳密後続)に置換 + write chunk を 2000 に縮小して再 build → SF0.1。
- **correctness ✅**: q21 md5(backfilled)= `d36a1caf...` = ground-truth。**read-chunk 経路でも回帰なし**
  (set_fields_from_lineairdb は active-index 非依存・DDL は projection 無効で full-row 復元、PK は stored key
  kv.first を直接使用)。
- **perf**: l_sk **10.8s** / l_sk_pk(複合)**9.5s** / 計 ~20s。
  - 複合 22.9→**9.5s**(2.4x): high-distinct は chunk² が支配項なので chunk 縮小が効く。
  - l_sk 8.2→**10.8s**(微増): distinct suppkey ~1000 が write_set 上限なので chunk 縮小で scan は減らず、commit 数
    増(120→300)+ read RPC 分割(1→12)の overhead だけ乗る。→ **chunk サイズは index cardinality で最適が逆**
    (low-distinct は大 chunk、high-distinct は小 chunk)。2000 は両者の中庸で SF0.1 合計最小。
- **codec 知見**: range/prefix scan の応答は flat binary codec(send_protobuf_recv_binary/parse_binary_kv_response)
  で **protobuf 2GB 制限の対象外**だった(設計が懸念した「2GB framing」は read には非該当)。read-chunk の効用は
  transient メモリ抑制(SF1 で全行 ~1GB を一括 materialize しない)。
- 観察: commit 1回 ~10ms(epoch group-commit, isFence=false でも)。SF1 は N が ~10x なので commit 数も ~10x、
  commit latency が無視できなくなる可能性 → SF1 実測で確認。さらなる高速化が要るなら LineairDB core の
  WriteSecondaryIndex を hash-set lookup 化(per-write O(1))が本筋(feature branch・別途 dual-review)。

## v2.2 実装 dual-review(2026-06-14)
Claude(grounded, 実ファイル 59 tool-use 精読)+ Codex(inline)を並列。
- **Claude grounded = GO**(correctness bug なし)。8 facts + C1-C5 すべて file:line 付きで CONFIRMED。特に
  **C1 を補強**: `AddSecondaryIndexValue`(data_item.hpp:176-184)は PK list を **sorted 保持・既存 PK は早期 return
  =冪等** → 再読/chunk 跨りで PK 二重化は構造上不可能(cursor 論証の上にさらに backstop)。C2 cursor は `begin`
  inclusive + `\0` 最小バイト=厳密後続で正(prefix-free 不要)。C3 SI 構造は primary index と disjoint で OCC 干渉なし。
  C4 失敗→DDL 失敗→不可視。C5 benign。md5 が捕まえない全ケース(UNIQUE/NULL/複合列順/空表/境界跨り重複/中断)も safe。
- **Codex = CHANGES-NEEDED** だが裁定の結果:C1 UNIQUE+NULL / C4 失敗時 partial SI は **chunk-COMMIT の回帰でなく
  既存 LineairDB SI 性質 / 既知 FIX-4・FIX-5 hazard**。measured(非 UNIQUE・fresh-load 単一 CREATE)では非発火、
  grounded reviewer も benign と確認。Step ③(DROP purge / clean-slate)で対応。C2/C5 は grounded が非 bug 裁定。
  RAII leak は throw 時のみ(現状 throw 経路なし、既存 get_transaction も raw new)= LOW。
- **MED(perf, grounded 指摘)→ read-chunking を撤去**: bounded read は (1) 応答 codec が flat binary で 2GB 制限外、
  (2) server の unbounded-end Scan(transaction_impl.cpp:565)が chunk ごとに残り全 key を収集し row_limit は応答だけ
  cap = **server memory を bound せず O(rows²/read_chunk) を足すだけ**、の二点で前提が崩れていた。→ **単一順序スキャン
  (O(rows) 1回)に戻した**。write chunk-COMMIT(correctness の要・両者 GO)は不変、correctness surface は縮小、
  cursor 論点は消滅。server 側スキャンの真の bounding は LineairDB core(:565 の early-stop を unbounded-end でも
  row_limit 尊重)= 将来の最適化。
- → write chunk-COMMIT は **GO**。read 単一スキャンへ戻して再 build → SF0.1/SF1 md5 + perf 再確認(下記)。

## v2.2 最終(read-chunking 撤去後, 2026-06-14)
single-scan read + write chunk-COMMIT(chunk=2000)で再 build → 検証。
- **correctness ✅**: SF0.1 q21 md5 = `d36a1caf...` / SF1 q21 md5 = `49e5b76c...`、いずれも backfilled == ground-truth
  (no-index)。read 撤去で回帰なし。
- **perf**: SF0.1 l_sk 10.6s / l_sk_pk 9.0s(計 ~20s)。SF1 l_sk 3m31s / l_sk_pk 1m40s(計 ~5min、read-chunk 撤去で
  ~7min から短縮)。14m57s(first-cut 単一 commit)比 SF0.1 ~45x。O(N²) 解消。
- **Step ② 結論 = GO**。correctness 最優先を満たし、dual-review GO(grounded)、md5 両 SF 一致。残課題は (i) UNIQUE+NULL
  の SQL 準拠(現状 LineairDB SI が NULL を unique 衝突扱い → TPC-H 標準索引は非 UNIQUE なので非該当、将来 UNIQUE 対応時に
  encode-with-PK 等)、(ii) 失敗時/再 CREATE の clean-slate = Step ③ DROP purge、(iii) perf の core hash-set 化。
  いずれも本 step の measured スコープ外で文書化済。
