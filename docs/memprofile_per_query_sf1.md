# Per-query メモリ + 時間プロファイル (SF=1, helios vs InnoDB)

計測: 2026-05-29。`bench/bin/memprofile_per_query.sh`(各クエリで helios mysqld をクリーン再起動し
ingest キャッシュ baseline をリセット、lineairdb-server は生かしたまま)。両プロセス + InnoDB の RSS を
10Hz で時系列サンプリング(`/tmp/memprof/ts_<q>_{helios,innodb}.csv`)。固定 `v_*.sql`、md5 は全 22 ✅。
gate(HELIOS_RANGEHASH_OCC)は memory には無関係(prefetch/ingest は gate 非依存)。

## 固定ベースライン(クエリ非依存)
| プロセス | idle RSS | 中身 |
|---|---|---|
| lineairdb-server | **4.89 GB** | インメモリ KV に常駐する SF1 全テーブル(これがデータ本体の固定費) |
| helios mysqld | 0.39 GB | プラグイン + MySQL のみ(再起動直後、ingest キャッシュ空) |
| InnoDB mysqld | ~3.3 GB | buffer pool に温まったデータ(16G 上限だが SF1 分のみ常駐) |

## 全 22 クエリ(時間 ms / メモリ GB)

`my`=helios mysqld, `sv`=lineairdb-server, `Δ`=クエリ中の増分(=ワーキングセット),
`tot`=helios 両プロセス同時ピーク(my_peak+sv_peak)。mem×=tot/InnoDB_peak。

| Q | helios ms | InnoDB ms | 時間比 | my peak/Δ | sv peak/Δ | **helios tot** | InnoDB peak | mem比 |
|---|---:|---:|---:|---|---|---:|---:|---:|
| q1 | 34253 | 4991 | 6.9x | 10.19 / 9.81 | 9.13 / 4.24 | **19.32** | 3.30 | 5.9x |
| q2 | 171 | 59 | 2.9x | 0.42 / 0.04 | 4.94 / 0.05 | 5.36 | 3.30 | 1.6x |
| q3 | 5344 | 1659 | 3.2x | 1.84 / 1.46 | 6.09 / 1.20 | 7.93 | 3.30 | 2.4x |
| q4 | 1787 | 307 | 5.8x | 0.92 / 0.53 | 5.56 / 0.67 | 6.48 | 3.30 | 2.0x |
| q5 | 9870 | 591 | 16.7x | 2.85 / 2.47 | 6.51 / 1.63 | 9.36 | 3.30 | 2.8x |
| q6 | 8355 | 757 | 11.0x | 2.26 / 1.87 | 7.95 / 3.07 | 10.21 | 3.30 | 3.1x |
| q7 | 22270 | 492 | 45.3x | 5.62 / 5.23 | 8.60 / 3.71 | 14.22 | 3.30 | 4.3x |
| q8 | 710 | 1484 | **0.5x** | 0.55 / 0.17 | 4.99 / 0.10 | 5.54 | 3.30 | 1.7x |
| q9 | 4009 | 2194 | 1.8x | 1.18 / 0.79 | 5.38 / 0.50 | 6.56 | 3.31 | 2.0x |
| q10 | 2839 | 892 | 3.2x | 0.97 / 0.58 | 5.60 / 0.71 | 6.57 | 3.33 | 2.0x |
| q11 | 2971 | 446 | 6.7x | 1.20 / 0.81 | 5.59 / 0.70 | 6.79 | 3.34 | 2.0x |
| q12 | 8139 | 1163 | 7.0x | 2.10 / 1.71 | 8.06 / 3.17 | 10.16 | 3.33 | 3.1x |
| q13 | 8270 | 1710 | 4.8x | 2.16 / 1.78 | 5.73 / 0.84 | 7.89 | 3.34 | 2.4x |
| q14 | 8433 | 802 | 10.5x | 2.15 / 1.76 | 8.08 / 3.19 | 10.23 | 3.33 | 3.1x |
| q15 | 37151 | 1846 | 20.1x | 11.71 / 11.32 | 9.13 / 4.24 | **20.84** | 3.34 | 6.2x |
| q16 | 847 | 239 | 3.5x | 0.68 / 0.30 | 5.06 / 0.17 | 5.74 | 3.33 | 1.7x |
| q17 | 230 | 132 | 1.7x | 0.42 / 0.03 | 4.96 / 0.07 | 5.38 | 3.33 | 1.6x |
| q18 | 37574 | 1033 | 36.4x | 11.07 / 10.69 | 9.87 / 4.98 | **20.94** | 3.33 | 6.3x |
| q19 | 9106 | 126 | 72.3x | 2.27 / 1.88 | 7.91 / 3.02 | 10.18 | 3.33 | 3.1x |
| q20 | 1370 | 244 | 5.6x | 0.78 / 0.39 | 5.08 / 0.19 | 5.86 | 3.33 | 1.8x |
| q21 | 50506 | 2601 | 19.4x | 13.31 / 12.92 | 9.77 / 4.88 | **23.08** | 3.33 | 6.9x |
| q22 | 4114 | 99 | 41.6x | 1.36 / 0.97 | 5.42 / 0.53 | 6.78 | 3.33 | 2.0x |

## 「誰がどこで」使っているか

ピーク総メモリ(例 q21 23GB)の内訳:
```
q21 23.08GB = sv_idle 4.89 (常駐データ) + sv_Δ 4.88 (server scan материialize)
            + my_idle 0.39 (mysqld base) + my_Δ 12.92 (proxy ingest copy)
```

1. **proxy ingest copy(mysqld Δ)= 最大の犯人**。重いクエリで最大 12.9GB(q21)。
   prefetch した行を proxy 側 local cache(local_read_set_ / range entry)へコピーする分。
   full-projection で lineitem 6M 行を抱える q1/q15/q18/q21 で突出。result が 101 行でも
   中間 prefetch が巨大(q21)。→ [[helios-prefetch-mem-copies]] の多重コピーがここに出ている。

2. **server scan materialize(server Δ)= 次点**。どの lineitem full scan でも +3〜5GB 一定。
   server がスキャン結果を **送信前に全部 materialize**(ReadPlanResult を丸ごと構築)してから
   RPC で返すため。フィルタ集約系(q6/q12/q14/q19)は mysqld ingest は小さいが server Δ は +3GB。

3. **server 常駐 4.89GB**= データ本体。クエリ非依存の固定費。query transient(最大 +18GB)に比べれば軽い。

## InnoDB との対比
- InnoDB は全クエリで RSS ほぼ一定 3.3GB(in-process・buffer pool からゼロコピー行アクセス、中間コピー無し)。
- helios は heavy query で 6〜7x のメモリ。**差分はほぼ全部「データを2回コピーする」アーキ由来**
  (server で materialize → wire → proxy で ingest)。
- 時間も InnoDB が速い(唯一の勝ちは q8 0.5x)。memory 比と時間比は概ね相関(コピー量 ∝ 遅さ)。

## 軽量化の方向(優先度順)
- **(A) server を lightweight に(ユーザ要望の本丸)**: scan の **streaming 送信**。今は全行 materialize→
  一括返却。チャンク stream で server 側ピークを ~5GB → ほぼ baseline に落とせる(Phase-4 D-2/D-3、保留中)。
- **(B) proxy ingest のコピー削減**: zero-copy ingest(wire バッファを直接 cache 行に move、Phase-4 B2 保留)。
  projection pushdown は実装済で full-projection クエリには効かない(全列必要だから)。
- **(C) 常駐 4.89GB**: 圧縮ストレージはスコープ外。relative に軽いので優先度低。

時系列 CSV は `/tmp/memprof/ts_<q>_helios.csv`(t_ms,mysqld_kb,server_kb)と
`ts_<q>_innodb.csv` に保存済(後から波形確認可)。
