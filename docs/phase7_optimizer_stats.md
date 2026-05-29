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
