# Phase-7: row-serve fast path(InnoDB いいとこどり)設計

調査: 2026-05-29。InnoDB serve 機構を 4 領域に分け、claude エージェント×4 + Codex×4 の計8並列で
実 InnoDB ソース(third_party/mysql-server/storage/innobase/)を grounding して精査。本書はその統合。

目的: TPC-H Q21 SF1 の per-row marshalling(rnd_next 15.7s/600万 + index_read 7s/707万 +
set_fields 5.9s/1315万 ≒ **23s**、InnoDB 同条件 2.6s)を、InnoDB の技法移植で削る。

## InnoDB が速い 4 機構(実コード)
1. **prebuilt template (`mysql_row_templ_t`)** — 列ごとに事前計算した記述子
   {mysql_col_offset, mysql_col_len, null_byte_offset/bit, type, mysql_type, length_bytes, charset}。
   hot path `row_sel_store_mysql_rec`(row0sel.cc:2892-3048)は **typed memcpy + 整数 byte-shuffle** だけで、
   `Field::store()` の charset 検証/文字列 parse を通らない。テンプレ構築は scan 開始時1回
   (ha_innodb.cc:8252-8388)。
2. **fetch cache が MySQL record 形式** — まとめ取り後 cache に **record bytes** で保持し、
   pop は `memcpy` 一発(row0sel.cc:3556-3624, 5592-5698)。1 row 1 handler call は同じだが、
   cached call は B-tree/MVCC/decode を全部 skip。
3. **点引き = AHI + page directory 二分探索** — 繰返し同型 probe を hash で O(1)(btr0sea.cc)、
   page 内は directory slot の二分探索(page0cur.cc:328-585)。`rec_cache.offsets` で offset 再計算も償却。
4. **compact record = 連続バッファ + offset アクセス** — per-cell allocation ゼロ、列値は `rec+offs` の
   pointer+len 参照(rem0wrec.h:67-111, rec.h:1082-1260)。scan は隣接 record へ進む tight loop。

## helios の現状ギャップ(なぜ遅い/重い)
- **値がテキスト**: `set_write_buffer` が `Field::val_str()` で「表示用文字列」を格納(ha_lineairdb.cc:4877,
  lineairdb_field.hh:16-25、memory `helios-date-row-ascii` と整合)。read 側 `set_fields_from_lineairdb`
  (ha_lineairdb.cc:4905)が serve 毎に **列ごと `Field::store(slice,&my_charset_bin)`**(:4954,:4970)で
  テキスト→record binary を**再パース**。`Field_long::store`→`strntoull10rnd`、`Field_varstring::store`→
  charset 検証(sql/field.cc)。これが set_fields の主因。
- **slice 毎に deep copy**: `slice_range_entry`(lineairdb_transaction.cc:1865)が probe 毎に entry 全体を
  `cached=src` deep copy + result_keys 線形フィルタ。707万 probe で index_read 7s の主因。
  ※ `slice_range_entry_fast`(:1837)は既に lower_bound 二分探索化済(AHI/page-dir 相当は実装済)、
  `range_scan_index_`(:403)も AHI 相当の O(1) start-key 引きは実装済。
- **per-cell alloc**: `LocalRangeScanEntry::rows` = `vector<pair<string,string>>`(行毎 2-3 malloc)。
  Q21 mysqld +13GB の主因。

## ★ 並列調査が捕まえた「漏れ」(claude が弱く Codex が明示)
**テキスト→typed binary 化(本命の最大効果)は server 側も壊す**:
- server の predicate pushdown が row value を**文字列 slice として parse**(server/rpc/predicate_evaluator.cc:20-27,124-160)
- value-column binding が decimal 文字列を `strtoll()` で key 化(server/rpc/lineairdb_rpc.cc:226-231)
→ binary-only value にすると filter/binding が壊れる。**proxy 単独では完結せず server まで波及**。
かつ record image は MySQL version/charset/pointer size 依存 → KV 永続化には format version/migration 必須。
（claude 側はこれを proxy 内の話と見ていた。両並列でこの blast radius を確定できたのが収穫）

## 戦略: 2 tier(proxy-only 先行 → 必要なら format v2)
全調査が「**事前計算テンプレート + record-image cache + slice view + arena**」に収束。リスク順に段階化:

### Step 1 (M, proxy-only, 低リスク, 最初の本命) — record-image cache + HeliosRowTemplate
- テーブル open 時に列記述子 `HeliosRowTemplate`{field_index, record offset(field->offset(record[0])),
  pack_length, null byte/bit, mysql_type, length_bytes, charset, projection mapping} を1回構築
  (InnoDB `build_template_field` の写し)。
- 行を**一度だけ** MySQL record image へ decode して arena に保持(`set_fields_from_lineairdb` を再利用可)。
  serve(rnd_next/rnd_pos/index_read)は `memcpy(buf, image, reclen)` + BLOB pointer fixup に置換。
- 効果: 同一行の再 serve(FER re-probe、Q21 は touch ~2x)で `Field::store` を回避。set_fields とその
  内側 re-parse を再 serve 分だけ削減。**初回 decode は残る**(テキストのため)。
- BLOB/GEOMETRY 含むテーブルは従来 path に fallback(InnoDB も `templ_contains_blob` で prefetch 切る)。
- メモリ: raw serialized と image の**二重保持を避ける**(image 化後 raw を捨てる or windowed)。
  memory `helios-prefetch-mem-copies` と整合。

### Step 2 (M) — slice を view/span 化(deep copy 撤廃)
- `lookup_local_range_scan`/`slice_range_entry` を「entry + [lo,hi) span」返しにし、`result_keys` の
  線形コピーは OCC validation 時に遅延 materialize。index_read 7s と memory を直撃。
- memory `helios-occ-rangekeys-bloat`(exact copy→move 済)と同系統。デグレ確認(md5/22-suite)厳守。

### Step 3 (L) — CachedRowArena(連続バッファ + row directory)
- `vector<pair<string,string>>` を arena + `RowDir{key_off,len,val_off,len,col_offsets}` に置換。
  per-row alloc を N→O(1)、slice はビュー。Q21 mysqld +13GB を直撃。改修面積大(rows を触る 30+箇所)。

### Step 4 (L, 要判断) — value format v2(typed/binary)
- write path を `val_str` から typed binary(`field->pack` 等)へ。**初回 serve からも `Field::store` 除去**
  = set_fields 5.9s の大半を消す本丸。ただし **server predicate_evaluator + value binding + wire codec +
  migration** に波及(上記「漏れ」)。"LineairDB を lightweight に" の観点では typed binary は wire/材料化が
  小さくなる利点もあるが、blast radius が大きい。**Step1-3 の実測後に費用対効果で判断**。

## 実装順(決定)
**Step 1 → 計測(Q21 set_fields/rnd_next 再測)→ Step 2 → 計測 → Step 3** を proxy-only で進め、
各段で md5(gate ON/OFF 22/22)とメモリ(HELIOS_MEMPROF / memprofile_per_query.sh)を確認しながら
1ステップ1コミット。Step 4 は Step1-3 の結果を見て user 判断。

## 不変条件(全 Step 共通・壊さない)
- OCC: positive hit は per-row TID 記録、negative hit は range validation activate(Q2 fix と整合)。
- projection: read_set/projection を cache key に含める(image の有効範囲が変わる、現 kept guard 維持)。
- own-write invalidation: local write/delete 時に image/cache を落とす(drop_local_secondary_scans 等のタイミング)。
- BLOB: image に pointer を焼かない(blobroot へ payload copy)。
- メモリ二重保持回避。phantom-free OCC は不可侵。

---
## 実装結果ログ

### Step 1 (record-image cache, index/re-probe path) — 2026-05-29 → **REVERTED(負の結果)**
実装: `fetch_and_set_current_result` の inline path に PK→record[0] image cache(BLOB除外, rnd_init/index_init/write で clear)。
測定(Q21 SF1, gate OFF, 22/22 md5 ✅ は維持):
```
              baseline   Step1
idx_read      6968ms  →  8952ms   (悪化)
set_fields    5855ms  →  6390ms   (13.1M 回 = ほぼ不変)
total wall    52s     →  62s      (悪化)
```
**set_fields の呼び出し回数が 13.1M でほぼ不変 = cache がほぼヒットしない。**
Q21 の FER probe(idx_read 7M)は「同じ行の re-probe」ではなく **distinct な行**だった(仮説外れ)。
image cache は find/insert と 7M エントリ分のメモリを足すだけで利得ゼロ → revert。
**教訓**: record-image cache は re-read 反復が多い workload 向け。TPC-H の oneshot 全行 prefetch では
各行 ~1回 serve なので効かない。serve を速くするには「初回 decode を安くする(Step 4 binary/template)」か
「per-probe の copy を消す(Step 2 slice-view)」が本筋。→ Step 2 へ前倒し。
