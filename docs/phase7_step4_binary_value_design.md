# Phase-7 Step4: typed-binary value format (set_fields 5.9s 削減)

目的: Q21 SF=1 の set_fields=5.9s(13.1M calls の Field::store text→binary 再パース)を削る。
現状 wall 32.6s のうち ~18%。set_fields は rnd_next(11.4s)+ idx_read(7.0s)にネスト。

## 現状フォーマット(text、proxy↔server 共有)
`lineairdb_field.hh`: 行 = [null_flags field] + 各列 `[byteSize:1B][valueLength:byteSize B][value]`。
value = **MySQL 表示用テキスト**(set_write_buffer の `Field::val_str`)。

### 値を解釈する全タッチポイント(= blast radius)
- **proxy write**: `set_write_buffer`(ha_lineairdb.cc:4868) `field->val_str` → text。
- **proxy read**: `set_fields_from_lineairdb`(:4908) 列毎 `Field::store(text,&my_charset_bin,CHECK_FIELD_WARN)`。←削りたい本体
- **server filter**: `predicate_evaluator::extract_value`(predicate_evaluator.cc:124-164) 列 string_view に
  `strtoll/strtoull/strtod`(compare_type ヒント)。
- **server binding**: `encode_column_as_int_key`(lineairdb_rpc.cc:227-231) value 列に `strtoll` で join key 生成。
- **wire codec**: flat_read_plan_codec(値 bytes をそのまま運ぶ。framing 非依存)。
- **storage**: KV server は値を不透明 bytes として永続(in-memory)。再起動でリロード必須。

text の利点: サーバはスキーマ不要(strtoll は任意 ASCII で動く)。binary にすると列の型を知らないと解釈不可。

## 提案: 型タグ付きバイナリ(server をスキーマ非依存に保つ)
各列を `[byteSize][valueLength][type_tag:1B][typed_bytes]` に拡張(type_tag を valueLength 内 or 別ヘッダ)。
- type_tag: INT64 / UINT64 / DOUBLE / DECIMAL / STRING / DATE... (MySQL field type から導出、書込時に proxy が付与)
- typed_bytes: INT は固定長 little-endian、STRING はそのまま、DECIMAL は ? (要検討: text 維持か packed)
- proxy read: tag→typed memcpy で field の record スロットへ(Field::store 回避)。InnoDB row_sel_store_mysql_rec 相当。
- server filter: tag を見て typed 比較(strtoll 不要)。tag==STRING のみ文字列比較。
- server binding: tag==INT なら strtoll 不要で直接読む。

### Migration / gate
- `HELIOS_BINARY_VALUE`(default OFF で導入)。ON の時のみ proxy が typed-binary 書込。
- server は **dual-read**: 先頭に format-version マーカ(or tag の有無)で text/binary を判別し両対応。
- in-memory なのでリロードで全置換可(永続移行ツール不要)。混在は「リロードまで」だけ許容するか、
  リロード必須にして混在を禁止するか要判断。
- 全 22 SF=1 md5(ON/OFF)で gate 両系統を担保。

## Codex への質問
1. この型タグ付き設計で「server スキーマ非依存」を本当に保てるか? DECIMAL/DATE/TIME/ENUM/SET の扱いで
   text を捨てられない型はあるか(比較セマンティクスが MySQL collation/decimal-scale 依存の場合)?
2. set_fields を Field::store 回避で typed memcpy にする時、record format(null bit, unsigned flag,
   varchar length bytes, DECIMAL packed, DATE packed)を InnoDB build_template 相当でどこまで再現要か。
   一番安全な最小サブセット(例: 整数列だけ binary 化、他は text 維持の hybrid)は?
3. CHECK_FIELD_WARN→CHECK_FIELD_IGNORE だけで set_fields がどれだけ安くなるか(format 変更なしの先行最適化)。
4. dual-read の format 判別を安全にする方法(version byte を行頭に置く vs tag 領域)。誤判定で
   text を binary 解釈する事故をどう排除するか。
5. 最狭で効果が出る scope は? (全列 binary は過大。整数列のみ? filter/bind されない列のみ?)
6. OCC/phantom-free への影響(値 format は OCC の TID/digest に無関係のはずだが確認)。

---
## 実装結果

### Step4a (read_set skip, format 変更なし) — 2026-05-29 → **採用**
Codex 設計レビュー(2026-05-29)の結論「全列 binary は過大、最狭で効果が出るのは read_set 外スキップ」を採用。
set_fields_from_lineairdb の kept パス/フル行パス両方で `bitmap_is_set(table->read_set, fi)` が
偽の列は Field::store を skip(フル行は columnIndex を進めてから skip し位置整合を維持)。

測定(Q21 SF=1, TIMEPROF):
```
              Step2c    Step4a
set_fields    5941ms  → 3356ms   (-44%)
rnd_next      11424ms → 9052ms
idx_read      7006ms  → 6191ms
wall          ~30.9s  → ~28.7s
```
全22 SF=1 md5 = 22/22 OK(回帰なし)。proxy のみ・server 無関係・migration 不要・OCC 無関係。

残 set_fields 3.4s = make_mysql_table_row(全列の framing parse)+ read 列の store。さらに削るには
binary format 本体(下記 #1-2, INT32 限定 + server predicate/binding 対応 + row-head magic + dual-read)
が必要。効果は残 3.4s の一部に留まり blast radius 大のため、費用対効果で後日判断(保留)。

### Step4a 修正 — 2026-05-29(Codex 安全性レビュー指摘)
Codex が Step4a に DML バグを発見: set_fields_from_lineairdb は SELECT の行提供だけでなく
update_row/delete_row が読む行バッファ生成にも使われ、read_set 外スキップだと old 行/secondary key
再構築が stale 列で壊れる(engine は HA_PARTIAL_COLUMN_READ 未宣言なので MySQL は secondary-key 列を
read_set から外せる)。22/22 md5 が見逃したのは TPC-H が全 SELECT だから。
修正: skip を `thd->lex->sql_command == SQLCOM_SELECT` に限定。非 SELECT は全列 materialize(従来同一)。
- DML 中立の論証: 非 SELECT で select_serve=false → skip 無効 → pre-Step4a とバイト同一挙動。
- 注: helios は ad-hoc autocommit DML(mysql クライアントの単発 UPDATE/DELETE)が pre-Step4a でも
  Deadlock(40001)する既存制限あり(pre-Step4a 5535a14 で再現確認)。TPC-C は benchbase 経由で別経路。
  本件はスコープ外。Step4a 修正は SELECT(q1/q6/q9/q21 md5 一致)+ DML 中立で担保。
