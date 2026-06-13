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
