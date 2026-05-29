# Phase-7: optimizer statistics(行数 + カーディナリティ)

## 動機(なぜ Q7/Q11/Q18/Q19 が InnoDB 比 8-38x 遅いか)
同じ MySQL オプティマイザ・同じ DDL/インデックスなのにプランが違うのは、**handler が返す統計が出鱈目**だから(GIGO)。
- helios `info()` は最適化時点で `stats.records=2`(oneshot が tx_begin を遅延 → begin/end の table_stats piggyback が
  最適化前に届かない + per-query 接続でキャッシュ cold)。EXPLAIN q19: `lineitem ALL rows=2` のフルスキャン。
- InnoDB は実 records(6M)+ 実カーディナリティ → part 駆動 + l_partkey ref。

## 調査:InnoDB vs NDB の統計・コスト
- **InnoDB(in-process)**: random index dive で B-tree リーフを N枚サンプリング(innodb_stats_persistent_sample_pages=20)
  → distinct 接頭辞を外挿 → rec_per_key。mysql.innodb_index_stats に永続。records_in_range はクエリ時にライブ dive。
  コストは buffer pool(インメモリ)前提に校正。
- **NDB(分散 ← helios と同型)**: 順序インデックスを **data node 上でサンプリング**(ndb_index_stat、per-fragment)、
  NDB 表に版管理付き保存。SQL ノードが**リモートのサンプルを読んで** ndb_index_stat_set_rpk で rec_per_key 設定。
  コストは全アクセスがネットワーク越し → **scan_time=records×1000** と高コスト申告でアクセス回数の少ないプランへ倒す。

## 結論
- helios はデータが別プロセス = **NDB 流(リモートサンプリング + 高コスト)を写すべき**。InnoDB の in-process コスト定数は写さない。
- **Q5 爆発はコストでなくカーディナリティの問題**:`read_time(ranges, rows)` の rows が 1(実 2000)だとどんなコスト定数でも
  「NLJ は安い」と出る。**カーディナリティ=ステアリング、コスト=ガードレール**。helios のコストは既に NDB 流(scan_time=records×10)。
- helios 既存の rec_per_key = `records^(1/key_parts)` ヒューリスティックは**単一パート非ユニークを rpk=1(ユニーク誤認)**にする致命傷。

## 実測:row-count だけ与えると「全部2行」より悪い(2026-05-29)
GET_TABLE_STATS RPC + info()-on-miss seed で実 records を届けたところ:
- Q19: 3044ms→**102ms**(part 駆動成功、InnoDB 74ms に肉薄)✅
- Q5: 2817ms→**107150ms**(rpk=1 で FK を 1:1 誤認 → join order 爆発)❌、Q9/Q4/Q2/Q10 も悪化
- 合計 82s→231s(4.0x→11.5x)悪化。22/22 md5 は OK(正しさは維持、プランだけ悪い)。
→ row-count は必須だが**実カーディナリティとセットでないと害**。

## プラン
- **Phase 1(回帰停止)**: info()-auto-seed を gate `HELIOS_OPT_STATS`(default OFF)に。default は安定(stats=2, 4.0x)。
  GET_TABLE_STATS RPC + analyze() の infra は残す。
- **Phase 2(NDB 流 NDV)**: server で per-index NDV を**順序スキャン1パスで exact 集計**(順序付き Masstree なので
  隣接キーの接頭辞変化を数えるだけ、O(rows)・O(keyparts)メモリ、初回のみ・キャッシュ)。GET_TABLE_STATS を per-index NDV
  付きに拡張 → proxy が実 rec_per_key 設定(n-th-root 撤廃)。row-count とセットで gate ON 化。
  measure → Q19 改善維持 + Q5 爆発解消 を確認 → Codex レビュー。

## 実装結果ログ
(以下追記)

## Phase 2 設計(Codex GO with fixes、2026-05-29)
方向 OK: server が per-index NDV を exact 計算 → proxy が rec_per_key=ceil(records/NDV) 設定、n-th-root 撤廃。
**実装前に潰す4点(Codex):**
1. secondary key は現状 PK 非付与(secondary parts のみ)。`num_user_defined_key_parts` 分だけ depth を数える(将来備え)。
2. `keypart_prefixes()` 流用不可(int専用・string停止。string 符号化=payload+terminator+length, ha_lineairdb.cc:3923)。
   専用 key-part 境界パーサを作る。**解析不能な型 → 該当 index は "NDV unavailable"**(出鱈目統計を入れない)。
   → 実用上 TPC-H の join 駆動 index(l_partkey/s_nationkey/o_custkey/ps_suppkey/PK)は**全部 integer** なので
   int-only パーサで足り、string-keyed index は unavailable で従来 heuristic 据え置き(join order に無関係)。
3. tombstone 除外(keys-only は IsInitialized 不可 → value-aware scan か live のみ計上)。
4. TX_GET_TABLE_STATS が index scan するなら dispatch で ReleaseMasstreeThreadEpoch() を呼ぶ。
- exact-once-cached(sampling は順序クラスタで偏り不可)。巨大表ガード=上限超で "unavailable"+gated 維持。
  **実 row-count + n-th-root へは戻さない**。
- コストは現状(scan_time=records*10 / read_time=ranges+rows*0.5)で初回計測。必要なら後で secondary を
  ranges*2+rows*1 へ。NDB の records*1000 へは測る前に飛ばない。
- correctness=plan-only。22/22 md5 + Q5/Q19/複合 secondary の EXPLAIN 検証。
- proxy-driven descriptors。cache key=(table,index,num_parts,schema-gen)。ANALYZE 強制更新。SHARE thread-safe。
  非ユニーク index が NDV 欠ならその table/index で row-count seed を有効化しない。
