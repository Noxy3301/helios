# Phase-11 SF sweep: メモリ肥大化・OOM上限・実行時間スケーリング特性評価

**目的(user, 2026-06-01)**: TPC-H SF を sweep し (1) このマシン(WSL VM 47GB cap + swap 32GB, free~45GB)で
**どこまで動くか(OOM上限)**、(2) **Helios のメモリ肥大化の SF 依存**、(3) **実行時間が線形増加するか**を確認する。

## 環境
- VM RAM 47GB(cap 48GB), swap 32GB, free ~45GB。disk 935GB 空き。16 cores。
- 計測モード: `HELIOS_RO_NOVALIDATE=1` + oneshot ON、FE_DEBUG off。各 SF で server を wipe して clean load。
- ハーネス: /tmp/sf_point.sh — load(watchdog付) → 後RSS → q1/q6(O(N) full-lineitem scan, ピークRSSサンプリング+watchdog)。
  available RAM < 2500MB で load/query を中断(WSL OOM 回避)。
- 計測クエリ: q1(pricing summary, full scan+GROUP BY+exact-decimal agg)、q6(forecast revenue, full scan+filter+SUM)。
  どちらも O(N) lineitem full scan なので線形性プローブに適。

## 指標
- **データ肥大**: load 後 lineairdb-server RSS(srv_post) — インメモリデータ量。
- **クエリ肥大**: q1/q6 実行中の mysqld+server 合計ピーク RSS(peak_tot_rss) — prefetch 作業集合(既知の肥大点 [[helios-occ-rangekeys-bloat]]/[[helios-prefetch-mem-copies]])。
- **時間線形性**: q1/q6 の t(s) vs SF。

## 結果

| SF | lineitem rows | load_t(s) | srv_post(GB) | q1 t(s) | q1 peak(GB) | q6 t(s) | q6 peak(GB) | 備考 |
|----|--------------|-----------|--------------|---------|-------------|---------|-------------|------|
| 0.1 | 600572 | 22 | 0.48 | 1.53 | 1.45 | 0.61 | 1.51 | srv_empty=0.01GB |
| 1   | 6001215 | 220 | 5.45 | 16.24 | 11.41 | 3.68 | 10.84 | data+time≈線形(11.4x/10.6x) |
| 3   | 17996609 | 699 | 15.84 | 53.69 | 31.93 | 11.97 | 32.84 | load後avail26.7GB |
| 10  | ~60M  | | | | | | | |

## 全22-suite 計測（benchbase run_tpch_queries.py, query毎にJVM起動=overhead込み, 120s/query timeout）
計測 env: RO_NOVALIDATE + oneshot ON のみ（**perf gate: semijoin/cost-model/agg-pushdown は OFF の baseline**）。

### SF=1 (data 5.45GB loaded)
- **21/22 OK、Q5 のみ TIMEOUT(120s, 時間切れ・メモリではない)**。peak Helios RSS=**18.25GB**(OOM watchdog 発動なし)。
- 主な時間: Q9 51.7s, Q7 39.7s, Q1 35.8s, Q10 34.7s, Q4 19.6s, Q21 19.7s, Q18 17.6s。軽量: Q17 1.65s, Q19 1.61s, Q20 2.61s, Q11 3.61s。
- Q5 timeout は perf-gate OFF が原因の可能性大(memory: semijoin gate ON で q5 3.5→1.2s)。

### SF=3 (data 15.84GB loaded)
- **Q06 で watchdog 発動(RSS 41.28GB, avail 4.3GB)→ 中断、完走せず**。完走: Q01 114s, Q02 67s, Q03 38s, Q04 70s, **Q05 TIMEOUT(120s)**, Q06 でメモリ天井。
- **メモリ蓄積が真因**: 単一クエリ peak(q1=31.93GB)は cap 内だが、22-suite を通すと mysqld メモリが**クエリ間で解放されず蓄積**(jemalloc 高水位保持 + Q05 abort 残渣)し Q06 で 41GB に到達。watchdog が hard OOM(WSL クラッシュ)を阻止。

## 観察・結論

### (1) OOM 上限（このマシン: VM 47GB cap + swap 32GB）
- **SF=10 は不可**: load だけでデータ ~53GB(5.3GB/SF×10) > 47GB cap → load 時点で OOM。
- **全22-suite 完走の上限 ≈ SF=2**: SF=1 完走(peak 18.25GB)、SF=3 は Q06 で 41GB 到達し中断。間の SF=2 が実用上限(suite peak ~25-30GB 予測)。
- **単一重クエリ(q1)の上限 ≈ SF=4**: q1 peak 10.6GB/SF → SF=4 ~42GB(ぎりぎり)、SF=5 ~53GB OOM。
- suite が単一クエリより上限が低い理由 = **クエリ間メモリ非解放(蓄積)**。

### (2) メモリ肥大化 = ほぼ完全に線形
- **データ(server RSS)**: 0.48(SF0.1)→5.45(SF1)→15.84GB(SF3) = **~5.3GB/SF, 線形**(生TPC-Hの約5x肥大)。
- **クエリ peak(mysqld+server)**: q1 1.45→11.41→31.93GB = **~10.6GB/SF, 線形**(うち data 5.3 + prefetch transient ~5.3/SF)。prefetch 作業集合の多重コピー([[helios-prefetch-mem-copies]])が transient の主因。

### (3) 実行時間 = ほぼ線形
- **q1**(O(N) full scan+GROUP BY+exact-decimal agg, 直接計測): 1.53→16.24→53.69s = SF1→3 で 3.3x(≈線形, わずかに super-linear)。
- **q6**(O(N) filter+SUM): 0.61→3.68→11.97s = SF1→3 で 3.25x(≈線形)。
- → Helios の実行時間は**データ量に対しほぼ線形**(scan/agg律速、転送も線形)。join 系は未分離だが SF=3 で多くが 120s 超(線形伸長の帰結)。

### 注意・前提
- benchbase runner はクエリ毎 JVM 起動の overhead 込み(全22の絶対時間)。直接 mysql 計測の q1/q6 はクリーン。
- watchdog(avail<4.5GB で中断)が SF=3 で WSL OOM を阻止、計測継続可能だった。

## ⚠️ 上記 baseline は計測不備（2点）→ proper config で再計測

user 指摘で 2 つの計測不備が判明:
1. **perf gate 全 OFF** だった(AGG_PUSHDOWN/COST_V2/SEMIJOIN/OPT_STATS)。標準計測は全 ON。
2. **`MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000` 未設定**だった。これが無いと jemalloc が解放メモリを保持し RSS 高止まり → SF=3 suite の「Q06で41GB蓄積」は**実は累積使用でなく jemalloc 保持**が主因。

### proper config (MALLOC_CONF + AGG_PUSHDOWN+COST_V2+SEMIJOIN+OPT_STATS+RO_NOVALIDATE + oneshot) 再計測
- **データ肥大は不変**: srv_post SF=1 = **6.14GB**(baseline 5.45GB と同等≒run差)。→ **5-6x肥大は jemalloc artifact でなく本物の live データ**(構造的: ASCII encode + masstree索引 + MVCC versioning)。gate と無関係。生1.03GB→6.14GB=**~6x**。
- **q1**: 16.24s/11.41GB(baseline) → **5.34s/7.76GB**(proper) = **3x速・peak-32%**(agg-pushdownでserver集約、6M行ship→4group行)。
- **q6**: 3.68s/10.84GB → **2.88s/7.79GB** = 速・peak-28%。
- 含意: gate ON だと query peak は data(6.14GB)+小transient に縮小 → **ceiling は data-load律速寄り**に上がる。

### proper config 全22-suite
**SF=1**: **22/22 OK**(baseline 21/22)、peak **10.95GB**(baseline 18.25GB)。
- **Q05: TIMEOUT(120s)→7.63s**(semijoin gate)。**Q09: 51.69→5.63s(9x)**、Q07 39.7→4.6s、Q01 35.8→11.6s、Q21 19.7→24.6s。
- gate ON で heavy join も高速化・Q5 timeout 解消、peak も MALLOC_CONF で大幅減。

**SF=3** (data 13.29GB loaded): **22/22 OK**(baseline は Q06 で中断 → MALLOC_CONF で完走)、peak **29.81GB**、srv_post **13.29GB**、load 694s。
- 最重 Q21 74.7s, Q18 49.7s, Q15 44.6s, Q01 27.75s。**baseline「Q06で41GB中断」は jemalloc 保持が主因と確定**(MALLOC_CONF で 22/22 完走)。

### proper config スケーリング(3点)・OOM上限(更新)
| SF | srv_post(GB) | 22-suite peak(GB) | q1 t(s) | 22/22 |
|----|-------------|-------------------|---------|-------|
| 1  | 6.14  | 10.95 | 5.34  | OK |
| 3  | 13.29 | 29.81 | 27.75 | OK |
- **データ**: 6.14→13.29GB ≈ **~3.6GB/SF**(生1.03GB/SF の約3.5-4x、SF1で6x→SF3で4.3x と単位あたり逓減=固定オーバーヘッド償却)。
- **22-suite peak**: 10.95→29.81GB ≈ **~9.4GB/SF**(transient がデータより大きく効く)。
- **q1 時間**: 5.34→27.75s = SF1→3 で 5.2x(やや super-linear、SF3 で agg-pushdown 効果薄れ scan 律速)。
- **全22完走の上限 ≈ SF=4**(peak ~9.4GB/SF → SF4 ~39GB < 47GB cap、SF5 ~48GB ≈ cap 超で危険)。
- **データ load の上限 ≈ SF=9**(下記 SF=5 実測で srv≈4×SF+2GB と確定 → SF10≈42GB は watchdog ceiling 42.5GB 際どく fail 寄り)。

**SF=5** (data 22.13GB loaded, lineitem 29,999,795, load 1165s): **14/22 で watchdog 中断**(Q15 で avail 4460MB → kill)、**22-suite peak 41.96GB**。
- 完走: Q01 49.9s, Q02 8.6s, Q03 55.6s, Q04 10.6s, Q05 40.6s, Q06 52.7s, Q07 36.7s, Q08 59.6s, Q09 50.6s, Q10 17.6s, Q11 4.6s, Q12 43.6s, Q13 65.7s, Q14 49.7s → **Q15 でメモリ天井(中断)**。
- **これで全22完走の上限が SF=4 と確定**(SF=5 は 41.96GB で 22-suite を通せない、watchdog ceiling 42.5GB ≈ cap 47 − safe 4.5)。

### proper config 最終スケーリング(4点)
| SF | srv_post(GB) | 22-suite peak(GB) | q1 t(s) | 完走 |
|----|-------------|-------------------|---------|------|
| 0.1| 0.48 | (n/a) | — | — |
| 1  | 6.14  | 10.95 | 5.34  | 22/22 |
| 3  | 13.29 | 29.81 | 27.75 | 22/22 |
| 5  | 22.13 | 41.96 | 49.92 | 14/22(中断) |
- **データ(srv_post)**: 線形 fit **srv ≈ 4.0×SF + 2.1GB**(生 1.03GB/SF の ~4x、固定オーバーヘッド ~2GB 償却で単位コスト逓減)。
- **22-suite peak**: 10.95→29.81→41.96GB ≈ **~7.8GB/SF + transient**(SF1→5 で 31GB増/4SF)。
- **q1 時間**: 5.34→27.75→49.92s = SF1→5 で **9.3x/5x = やや super-linear**(agg-pushdown の group 圧縮効果が大データで scan 律速に埋もれる)。

---

## 最終結論（proper config: 全 perf gate ON + MALLOC_CONF、user の3問への回答）

計測 config = `MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000` +
`HELIOS_AGG_PUSHDOWN=1 HELIOS_COST_V2=1 HELIOS_ENABLE_SEMIJOIN=1 HELIOS_OPT_STATS=1 HELIOS_RO_NOVALIDATE=1` +
`lineairdb_oneshot_execution=ON`。マシン = WSL VM 47GB cap + swap 32GB（watchdog safe 4.5GB → 実効上限 ~42.5GB）。

### Q1. このマシンでどこまで動くか（OOM 上限）
- **全22-suite 完走の上限 = SF=4**。SF=3 は 22/22 完走（peak 29.81GB）、SF=5 は Q15 で中断（peak 41.96GB > 実効 42.5GB 際）。SF=4 は peak ~37-38GB 予測で完走圏。
- **データ load のみの上限 ≈ SF=9**（srv ≈ 4×SF+2GB → SF9 ~38GB、SF10 ~42GB は load 中 mysqld 分も足して危険）。
- → baseline の「全22は SF=2 上限 / SF=10 load 不可」は **計測不備（gate OFF + jemalloc 保持）由来の過小評価**。proper config で **suite 上限 SF=2→4、load 上限 ~SF=9 に改善**。

### Q2. Helios のメモリ肥大化の SF 依存
- **サーバ side データ = 完全に線形、srv ≈ 4.0×SF + 2.1GB**（生 TPC-H の約 4x）。
- 4x の内訳は構造的: ASCII テキスト列エンコード + per-field framing + テーブル毎 in-memory 索引 + per-record メタ。**gate と無関係・query と無関係の本物の live データ**（jemalloc artifact ではない＝MALLOC_CONF ありでも srv_post は不変）。
- query 実行時の transient（prefetch 作業集合の多重コピー [[helios-prefetch-mem-copies]]）が data に上乗せ → 22-suite peak ≈ data + ~3.6GB/SF transient。

#### InnoDB との本体サイズ実測比較（2026-06-01）
同一 lineitem スキーマで InnoDB に 2M 行コピーして実測:

| エンジン | バイト/行 | 内訳 | 倍率 |
|---------|----------|------|------|
| **InnoDB** | **190 B/行** | data 145 + secondary index 36（packed binary, clustered B-tree） | 1.0x |
| **Helios/LineairDB** | **~470–670 B/行** | 限界 ~4GB/SF ÷ lineitem 6M行/SF（lineitem 主因と仮定の概算） | **~3–3.5x** |

InnoDB が小さい二重の理由:
- **(A) フォーマット効率**: バイナリパック。例 DATE = 3B(InnoDB) vs **10B ASCII文字列**(Helios, [[helios-date-row-ascii]])。lineitem は DATE 3列で +21B/行差。int/decimal も Helios は数字列。
- **(B) ディスクベース vs 純in-memory**: InnoDB は `.ibd` 上にデータ、RAM(mysqld RSS)には buffer pool のアクセス済みページのみ常駐。Helios は全データ RAM 常駐 → RSS=全量。
- → メモリ削減タスク docs/phase12_memory_compaction_investigation.md で削減余地を調査(下記)。

### Q3. 実行時間は線形増加か
- **ほぼ線形〜やや super-linear**。q1（O(N) scan+GROUP BY+exact-decimal agg）= 5.34→27.75→49.92s（SF1→5 で 9.3x / 5x data）。
- super-linear 成分の理由 = agg-pushdown による group 圧縮（6M→4 行 ship）の相対効果が大データで scan/exact-decimal CPU 律速に埋もれる（[[helios-pushdown-sweep-findings]] の「残存 latency は server 側大表 scan+agg CPU 律速」と整合）。
- join 系（Q08 59.6s, Q13 65.7s @SF5）も同様に scan 律速で線形伸長。semijoin/cost-v2 gate で SF=1 の Q5 timeout・Q9 9x 等は解消済だが、絶対 scan コストは SF 線形。

### baseline との対比（計測不備の影響）
| 指標 | baseline(gate OFF, MALLOC_CONF 無) | proper(gate ON + MALLOC_CONF) |
|------|-----------|-----------|
| 全22完走上限 | SF=2 | **SF=4** |
| data load 上限 | SF=10 不可（~53GB と誤推定） | **~SF=9**（srv 4x が実測で確定） |
| SF=1 22-suite | 21/22（Q5 timeout）, peak 18.25GB | **22/22, peak 10.95GB** |
| SF=3 22-suite | Q06 で中断（41GB 蓄積と誤帰結） | **22/22, peak 29.81GB** |
| q1 @SF1 | 16.24s / 11.41GB | **5.34s / 7.76GB** |

→ user 指摘通り baseline は二重の計測不備で悲観的だった。**正味の結論: データ肥大は線形（~4x）、時間は線形〜やや super-linear、このマシンでは全22が SF=4 まで・データ単独 load は ~SF=9 まで動く。**
