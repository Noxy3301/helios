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
