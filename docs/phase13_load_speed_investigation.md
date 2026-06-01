# Phase-13 Load 速度調査: 何が遅いか・どう速くするか

**目的(user, 2026-06-01)**: load が遅い。SF=3 で「死ぬほど遅い」と実験段階に入れない。
SF=1 で原因を特定し、改善方法を詳細に調査する。

## 0. 実測 baseline(proper config, このマシン 16 core / WSL)

| SF | load 時間 | lineitem 行数 | 1行あたり |
|----|----------|--------------|----------|
| SF1 | **227s** | 6M | ~38μs |
| SF3 | 692s | 18M | ~38μs |
| SF5 | 1161s | 30M | ~39μs |

load 時間は **完全に行数線形**(~38μs/行)。deadlock retry は成功 run には無し(過去 log の error 149 は別の失敗 run)。
**postload の secondary index 8本(l_ok/l_sk/...)は、この harness(tpch.xml に afterload script 無し)では実行されない**。
load 中に更新される index は **PK + FK 自動 index `(l_partkey,l_suppkey)` の 2本のみ**(`SHOW INDEX FROM lineitem` で確認)。
→ load 時間 = 純粋な data 挿入。index build フェーズは含まれない。

## 1. 律速の正体: lineitem 単一スレッド + マシン 94% 遊休

**(a) lineitem は単一スレッドで全行挿入**(`TPCHLoader.java:337-358`):
benchbase は **1テーブル=1 LoaderThread**(8テーブル→8スレッド)。lineitem は `LineItemGenerator(scaleFactor, part=1, partCount=1)` で
**分割されず 1 スレッドが 6M 行を順次 INSERT**。他7テーブル(orders 1.5M, partsupp 0.8M ...)は早期に終わり、
**load wall-time ≈ lineitem 1スレッドの時間**。検算: 6M × 38μs ≈ 228s ≈ 実測 227s。一致。

**(b) マシンは load 中ほぼ遊休**(SF1 load 中の CPU 実測、`/tmp/load_probe_cpu.log`):
- 序盤 ~20s(複数テーブル並列): sys ~2.4 core
- 長い尾部(lineitem 単独、支配的): **server 0.55 core + mysqld 0.26 core = 実計算 0.81 core**、システム全体 1.2 core
- load 全体の実 CPU = **srv 137 + mysqld 64 = 201 core-sec / wall 227s ×16core = 3632 core-sec → 利用率 5.5%(94.5% 遊休)**

**(c) ~90s は非オーバーラップの同期 stall**:
server busy 137s だが wall 227s。完全 pipeline なら max(137,64)=137s で済むはず。差 ~90s = mysqld↔server の
**同期 RPC 往復のシリアル化**(flush 毎に mysqld が server 応答をブロック待ち、互いに idle)。
epoch group-commit 待ちでは **ない**: `#define FENCE false`(`ha_lineairdb.cc:138`)で per-commit の `Fence()`(epoch Sync 待ち)は呼ばれない。

## 2. 1行あたり ~38μs の内訳(コード経路)

書き込みは per-row RPC ではなく **buffer→batch flush**(`lineairdb_transaction.cc:1381` buffer_write、`WRITE_BATCH_SIZE` 毎に flush)。
benchbase は JDBC `rewriteBatchedStatements` + batchsize=1024 → 1 文 = 1024 行 multi-INSERT = 1 autocommit txn。

旧 `WRITE_BATCH_SIZE=100` だと 1024 行文あたり **flush RPC 10回(100行毎)+ commit RPC 1回 = ~11 往復**。各往復で mysqld がブロック。
server 側(`lineairdb_rpc.cc:1053` handleTxBatchWrite)は **1 RPC=N行を逐次処理**:
- Masstree `GetOrInsert`(`transaction_impl.cpp:298`)= 木探索 + `DataItem` alloc(~192B、うち [[helios-lineairdb-cc-and-record-layout]] の ~128B 死荷重)
- commit 時に **再度 GetOrInsert で write target を解決**(二重 Masstree 探索)+ OCC lock/validate/install
- value の memcpy(ASCII 化済み row、[[helios-date-row-ascii]])

## 3. 改善策(優先度順)

### ✅ B: WRITE_BATCH_SIZE 100→1024(実装・実測済、低リスク)
`proxy/lineairdb_transaction.hh:486`。往復 RPC を 11→2 に削減。
**実測: SF1 load 227s → 191s(-16%)**、実 CPU 201→163 core-sec(冗長 RPC encode/decode 38 core-sec 削減)。
**正しさ安全**: 読み取り経路は read 前に `flush_write_buffer_for_table()`(`lineairdb_transaction.cc:669/685/741`)を呼ぶため、
read-your-own-writes は buffer サイズ非依存で保証。read-only 22-suite は write 経路を通らず md5 不変。
※ 尾部 CPU は依然 ~1.2 core で、単一スレッド律速は残る(B 単独では頭打ち)。

### ✅★ A: 全テーブル load の並列化(本命、実装・実測済 = 最大 7.8x)
15 コアが遊んでいるのが最大の損失。TPC-H の各 generator(`LineItemGenerator`/`OrderGenerator`/… `(sf, part, partCount)`)は **既に sharding 対応**
(iterator が全 RNG ストリームを `advanceRows(startIndex)` でシーク = TPC-H dbgen 式の chunked 並列生成 →
N 分割は 1 ストリームと **数学的に同一**のデータ・欠落/重複ゼロ)。
`third_party/benchbase` submodule(branch `helios/parallel-lineitem-load`)の `TPCHLoader.createLoaderThreads()` を
**汎用 sharding helper `addTable()`** に書き換え、大テーブル(part/customer/orders/partsupp/lineitem)を各 N シャードの並列 LoaderThread に
(tiny な region/nation/supplier は単一)。FK 親子順序の **latch DAG は維持(= "クリティカルセクション")**、各 shard は親 latch を await。
シャード数は `-Dtpch.load.shards=N`(default 8、**再ビルド不要**)。Maven 再ビルド → `bench/benchbase-mysql/benchbase.jar` 差し替え。

**pool deadlock 注意**: 総スレッド = `3 + 5*N`。loader pool(config `<loaderThreads>`)を総数以上にしないと、
latch 待ちで全 pool slot が埋まり、その latch を release すべき shard が queue で待つ → デッドロック。
`bench/config/tpch.xml` に `<loaderThreads>128</loaderThreads>`(shards≤25 を担保)を追加済。

**実測(SF1, 16 core, B=1024 と併用)**:
| 構成 | load | 倍率 | 律速 |
|------|------|------|------|
| baseline(1スレッド/表) | 227s | 1.0x | lineitem 単一(15 core 遊休) |
| lineitem のみ 8 並列 | 58s | 3.9x | orders serial prefix |
| **全表 8 並列** | **36s** | **6.3x** | lineitem 8 core(8 core 余り) |
| **全表 16 並列** | **29s** | **7.8x** | **CPU 16/16 飽和** |
- **全 8 テーブルの行数が正解と完全一致**(lineitem 6,001,215 / orders 1,500,000 / customer 150,000 / part 200,000 / partsupp 800,000 / supplier 10,000 / nation 25 / region 5)、**deadlock 0 件**。
- CPU: prefix で最大 15.9 core、shards=16 の lineitem フェーズで **15.9/16 core 飽和**(srv 7.5 / mysqld 4.4)。N≈core 数が sweet spot。
- メモリ副作用: srv_post 4.43(baseline)→4.63(8)→4.85GB(16)。並列度↑で同時バッファ/jemalloc arena が増え +0.4GB。高 SF で OOM 余裕が要る時は shards を下げる。

**upstream 確認(user 指摘)**: cmu-db/benchbase main も TPC-H loader は **`LineItemGenerator(sf,1,1)` の単一スレッド/表のまま**で、
intra-table 並列化の merged PR/Issue は無し(関連は前身 OLTPBench の peloton#117 のみ)。
ただし framework 自体は複数 LoaderThread を許容(**TPC-C は warehouse 単位で shard 済**)なので本改修は idiomatic。
→ Helios 側 fork の独自改善。残る律速は server per-row CPU = 下記 C。

### C: server 側 per-row コスト削減(A で並列化した後の真の天井向け)
A で並列化すると server CPU(137 core-sec)が次の天井になる。削減候補:
- **二重 Masstree 探索の解消**: Write 時の `GetOrInsert` で得た index leaf を commit まで持ち越し、再探索を省く(`transaction_impl.cpp` / `database_impl.h` commit path)。
- **DataItem 死荷重 ~128B**([[helios-lineairdb-cc-and-record-layout]] A1/A2/A3): alloc/memset コストとメモリ両方に効く。LineairDB submodule branch 必須([[helios-lineairdb-submodule-branch-rule]])。
- row value の binary codec([[helios-date-row-ascii]]): encode/decode と転送量削減。

### D: RPC 非同期化/pipeline(invasive)
flush RPC を fire-and-forget + 後追い ack にして mysqld を待たせない。同期往復 stall(~90s 分)を直接削る。設計大きめ。

## 4. 結論
- **何が遅かったか**: benchbase が各テーブル(特に lineitem 6M)を単一スレッドで挿入し、16 コア中 ~1 コアしか使わなかった(94% 遊休)。同期 RPC 往復で更に待ち。CPU バウンドですらなかった。
- **✅ 実装・実測済(SF1 227s → 29s = 7.8x、全行数正解一致・deadlock 0)**:
  - **A: 全テーブル N シャード並列**(`TPCHLoader.addTable()`、`-Dtpch.load.shards=N`、N≈core 数)。遊休コアを埋める本命。単独で 227→36s(8並列)/29s(16並列)。
  - **B: WRITE_BATCH_SIZE=1024**(`proxy/lineairdb_transaction.hh`)。往復 RPC 11→2、1 行・低リスク。A と直交し併用。
- **残る天井 = server per-row CPU**: shards=16 で 16 コア飽和したので、これ以上は **C(二重 Masstree 探索の解消 / DataItem 死荷重削減 / row binary codec)** = 1 行あたりの server 計算量そのものを削る軸([[helios-lineairdb-cc-and-record-layout]] と同根)。
- **SF3 換算**: 旧 692s → 推定 ~90s(7.8x)で実験サイクルが回る。

計測ハーネス: `/tmp/load_probe.sh`(stop→restart→SF1 load、per-proc CPU sampler 付)。baseline log `/tmp/load_probe_*_baseline.log`。
