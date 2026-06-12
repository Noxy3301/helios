# Phase-12 メモリコンパクション調査: どこを削れるか

**目的(user, 2026-06-01)**: Helios は生 TPC-H の ~4x にメモリ肥大する(srv ≈ 4.0×SF + 2.1GB, [[helios-sf-sweep-scaling]])。
SF が大きくなると全 in-memory では対応できなくなるので、**できるだけコンパクトにしたい。どこを削れるか**を調査。
「今はかなり可読性に振っているイメージ」「MVCC になっているの? TID と MVCC は別物では?」という user の問い に厳密に答える。

## 0. 前提の訂正: これは MVCC ではない(user の指摘が正しい)

私(Claude)が phase11 で「MVCC versioning」と書いたのは**誤り**。コードで確認した結論:

- **LineairDB の実 CC = Silo single-version OCC**(`server/storage/database_manager.cc:24` で `concurrency_control_protocol = Silo` 固定)。
- **version chain は存在しない**。1 レコード = 1 物理バージョンの in-place overwrite(`silo_nwr.hpp:285` `*snapshot.index_cache = snapshot.data_item_copy;`)。`DataItem` に prev/next ポインタ無し(`data_item.hpp:40-52`)。
- **TID は version chain ナビゲーション用ではない**。`TransactionId = {epoch:32bit, tid:32bit}` で、`tid` の LSB が排他ロックビット(Silo 風: `data_item.hpp:211` `if (tid.tid & 1llu)`)。= ロック状態 + epoch タグ。
- → **TID ≠ MVCC**。user の理解どおり別物。Silo は「単一バージョン + commit 時 validation で直列化可能性を検証」する OCC。多バージョンを物理保持する MVCC ではない。

## 1. per-record 固定メタの実態(最大の死荷重)

`third_party/LineairDB/src/types/data_item.hpp:40-52` の `DataItem`(全レコードが1つ持つ):

| フィールド | 推定 B | 用途 | **実設定(Silo + checkpoint OFF)での要否** |
|-----------|-------|------|------|
| `transaction_id` (atomic) | 8 | Silo の lock bit + epoch | **必須** |
| `initialized` (+pad) | 8 | live/delete flag | 必須(TID 埋込で 8→0 可) |
| `buffer` (DataBuffer: value*/size/capacity) | 24 | 現在値へのポインタ | **必須** |
| `primary_keys_ptr` (shared_ptr) | 16 | secondary→PK 逆引き | secondary 使う表のみ必要 |
| `checkpoint_primary_keys` (vector) | 24 | checkpoint 時の PK 保存 | **死荷重**(checkpointing=false) |
| `checkpoint_primary_keys_captured` (+pad) | 8 | 上の flag | **死荷重** |
| `checkpoint_buffer` (DataBuffer) | 24 | stable version(recovery/checkpoint) | **死荷重**(checkpointing=false) |
| `pivot_object` (atomic<NWRPivotObject>) | 16 | NWR validation メタ | **死荷重**(Silo≠SiloNWR) |
| `readers_writers_lock` (ReadersWritersLockBO) | 64 | **2PL 専用** RW-lock(cache-line aligned) | **死荷重**(Silo≠TwoPhaseLocking) |
| **合計** | **~192** | | うち **死荷重 ~128B/record** |

**核心**: 実運用は `Silo` + `checkpointing=false` + `recovery/logging=OFF(default)` なのに、
**2PL 用 RW-lock(64B) + NWR 用 pivot(16B) + checkpoint 用(48B) = ~128B/record が一切使われず常駐**している。
これが「複数 CC プロトコルを1つの構造体で汎用サポート」した設計の代償＝user の言う「可読性に振っている」の正体。

- インパクト概算: lineitem SF=1 = 6M 行 → 128B×6M = **~0.77GB**。SF=5 = 30M 行 → **~3.8GB**。全テーブルでは更に大。
- 削減後: per-record 192B → ~62B(transaction_id 8 + buffer 24 + primary_keys_ptr 16 + initialized 等)。**~67% のメタ削減**。

根拠ファイル: `data_item.hpp:40-52`, `silo_nwr.hpp:84/209-217/285`, `two_phase_locking.hpp:150`, `pivot_object.hpp`, `lock/impl/readers_writers_lock.hpp`, `server/storage/database_manager.cc:20-24`。

## 2. 行値(value)のエンコード: 全列 ASCII 文字列化 + per-field framing

`proxy/ha_lineairdb.cc:6246-6265` の `set_write_buffer`:
```cpp
for (Field **field = table->field; *field; field++) {
  (*field)->val_str(&attribute, &attribute);   // ← 全型を ASCII 文字列化
  ldbField.set_lineairdb_field(attribute.c_ptr(), attribute.length());
}
```
- **DATE = "1998-12-01"(10B 文字列)** ← packed なら 3B。lineitem は DATE 3列で +21B/行 ([[helios-date-row-ascii]])。
- INT/DECIMAL も数字列(int "12345"=5B+, decimal "12345.67"=8B+)。本来 4B/7B packed。
- **per-field framing**(`lineairdb_field.hh:17-27`, `lineairdb_field.cc:42-72`): 各列 `[byteSize:1B][valueLength:1-4B][value]`。固定長列にも length prefix が付く。lineitem 1行で framing だけ ~68B。
- lineitem 1行: 値本体 ~120-130B + framing ~68B = **~200B**(+RPC/protobuf envelope で実測 470-670B/row に膨らむ)。

肥大主因 top3(行値側): (1) DATE/数値の ASCII 化 25-35%、(2) per-field framing 20-30%、(3) 固定長列への可変長 framing 10-15%。

根拠: `ha_lineairdb.cc:6246-6265`, `lineairdb_field.{hh,cc}`, `server/rpc/lineairdb_rpc.cc:171-202`, `proto/lineairdb.proto`。

## 3. Secondary index: lineitem に 8本も実体化されている

benchbase postload(`third_party/benchbase/.../tpch/postload-mysql.sql`)が張る index:
- **lineitem: PK + secondary 8本** → `l_ok, l_pk, l_sk, l_sd, l_cd, l_rd, l_pk_sk, l_sk_pk`
- partsupp 4本, orders 2本, customer/supplier 各1本... 全テーブルで **30本超**。

各 secondary は LineairDB server 側で**独立した Masstree 木として実体化**(`proxy/ha_lineairdb.cc:4895-4902` → RPC → `table.h:28-31`)。
lineitem だけで PK+8SK = **9本の Masstree**、各 6M(SF1)/30M(SF5) キー保持。Masstree leaf は 64B 単位 alloc + ikey/ksuf 二重持ち(`masstree_struct.hh:248-300`)。

TPC-H で実際に join/filter に使うのは `l_ok`(Q3/4/...), `l_pk`/`l_sk`(join), `l_sd`(Q6/14 range) 程度。
**`l_cd`(commitdate 単独), `l_rd`(receiptdate 単独), 複合 `l_pk_sk`/`l_sk_pk` の一部は実質未使用** → 削減候補。

## 4. 削減候補(優先順位つき)

| 層 | 候補 | 概算削減 | 難易度 | リスク/トレードオフ | 変更箇所 |
|----|------|---------|--------|------|---------|
| **A1** | per-record の 2PL RW-lock(64B) を Silo 時 exclude | **~64B/row** | 中 | core struct 変更、2PL 研究を使う時 #ifdef 要 | LineairDB `data_item.hpp`(**submodule branch 必須** [[helios-lineairdb-submodule-branch-rule]]) |
| **A2** | checkpoint_buffer + checkpoint_pks(48B) を checkpointing=false 時 exclude | ~48B/row | 中 | 同上、recovery 経路の分岐 | 同上 |
| **A3** | NWR pivot_object(16B) を Silo 時 exclude | ~16B/row | 低 | NWR 研究時 #ifdef | 同上 |
| **B1** | DATE を packed 3B binary 化 | ~21B/row | 中 | デバッグ可読性↓ | proxy `ha_lineairdb.cc:6259` 型別 codec |
| **B2** | INT/DECIMAL を binary(varint/fixed) 化 | ~18-25B/row | 中 | 可読性↓ | 同上 + `lineairdb_field_types.h` |
| **B3** | 固定長列の length prefix 除去 | ~8-12B/row | 低 | parser 複雑化 | `lineairdb_field.{hh,cc}` |
| **C1** | 未使用 secondary index 削減(l_cd/l_rd/複合の一部) | index 5-15% | 低 | クエリ plan 退行が無いか要検証 | benchbase postload or proxy index gate |
| **C2** | Masstree key prefix compression | index 20-30% | 高 | Masstree core 改修 | submodule |
| **D** | OCC range-key digest 化(既知の最大点) | suite peak 40-50% | 非常に高 | collision/phantom リスク(Codex NO-GO・user 判断待ち [[helios-occ-rangekeys-bloat]]) | proxy/server |

### 推奨ロードマップ(リスク/効果順)
1. **C1(未使用 SK 削減)**: 即効・低リスク。proxy か postload で TPC-H 不要 index を外す。md5/plan 退行検証のみ。
2. **A1/A2/A3(per-record 死荷重メタ)**: 最大の data 本体削減(~128B/row, SF5 で ~3.8GB)。LineairDB を branch して **CC プロトコル/recovery を compile-time/config で条件化**。Silo+no-checkpoint プロファイルで RW-lock/pivot/checkpoint を struct から除外。**core 変更ゆえ慎重に(Codex レビュー必須)**。
3. **B1/B2/B3(行値 binary codec)**: ~40-50B/row。型別 codec で可読性とトレードオフ。debug flag で ASCII 形式も残せる設計に。
4. **D(OCC digest)/C2(prefix compress)**: 大きいが高リスク・要 user 判断。

### 注意
- A 系は [[helios-lineairdb-submodule-branch-rule]] 適用(third_party/LineairDB は branch を切る、research/helios 本筋に乗せない)。
- 全変更で **22-suite md5 一致**(対 InnoDB)を維持。read-only path だけでなく DML 行バッファ経路も影響([[helios-setfields-dml-readset-trap]])。
- B 系は proxy 行エンコード変更 → server parse と必ず両側同時に。
- 計測は MALLOC_CONF + 全 gate ON の proper config([[helios-sf-sweep-scaling]])、srv_post の SF=1/3 比較で効果検証。

## 5. InnoDB との比較(なぜ向こうは小さいか・再掲)
- InnoDB lineitem 実測 **190B/row**(packed binary, clustered B-tree, disk-based で RAM は buffer pool のみ)。
- Helios ~470-670B/row。差は **(本体)ASCII+framing+死荷重メタ × (常駐)全 in-memory**。
- 上記 A+B+C を全部やれば、値 ~130B + メタ ~62B + index 圧縮で **InnoDB の 1.5-2x 程度(~300-400B/row)** までは現実的に縮められる見込み(in-memory + secondary 多数ぶんは残る)。
