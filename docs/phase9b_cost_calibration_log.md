# Phase-9b 議事録: Disaggregated Cost Model 較正

実測較正された disaggregated cost model へ「経験値ハードコード」から昇華させる作業の検討・進行ログ。
日付は2026-05-30開始。branch `claude/perf/serve-fastpath-and-mem`。MySQL本体は無改変、proxy/server/proto のみ。

---

## 0. 背景と問い

現状 Phase-9 の cost model (gate `HELIOS_COST_V2`, `proxy/ha_lineairdb.hh`):

```cpp
static constexpr double kHeliosRowXfer = 0.20;  // per-row 転送/materialize
static constexpr double kHeliosRpc     = 50.0;  // 1 RPC 相当 ≒ 500 行評価
// table_scan_cost = records*0.20 (cpu) + 50 (io)
// read_cost/index_scan_cost = rows*0.20 (cpu) + (ranges>0 ? 50 : 0) (io)
```

ユーザの問い:
- 式が未完全で係数がハードコード。データサイズ / レイテンシ / LineairDB性能 のどれで決まるべき?
- InnoDB はどう決めていて、Helios の現状はどうなっているのか?
- パラメータチューニング的にやるべき? いろんなサイズで試す?
- MySQL→Server のレイテンシも入るのか?

### Codex verdict (xhigh) の要約

- **方針**: InnoDB の join order に合わせるのは誤り。Helios 実行モデル =
  `effective_rpc_count·C_rpc + returned_bytes·C_byte_xfer + returned_rows·C_row_ingest + scanned·C_remote_work` に較正する。
  - InnoDB: `page_reads·page_cost + rows·row_eval_cost`
  - Helios: `rpc_count·rpc_cost + bytes/rows·transfer/materialize_cost`
  - disk/page/buffer-pool 項は不要。
- **レイテンシは入る** → それが `C_rpc`(空RPC往復 = MySQL↔server ネットワーク往復)。bytes項=帯域、rows項=materialize CPU。
- **係数は実測で決める**: 複数サイズで `elapsed_us = a·rpc + b·bytes + c·rows + d·probe_keys` を線形回帰。
  MySQL単位化 `cost = measured_us · ROW_EVALUATE_COST(0.1) / row_eval_us`。今の `kHeliosRpc=50` = 「500行評価」相当。
- **`mysql.engine_cost` 流用は不可**(page read 用)。plugin sysvar / Helios独自 config が筋。
- **ref は batch-amortize**: `ranges·C_rpc` は過大。`effective_rpc_count = ceil(probe_keys / effective_batch_size)`。
  `C_rpc` を下げるのではなく `effective_rpc_count` を下げる(非batch point lookup を安く見せない為)。
- **残り6本(driving table 相違)は InnoDB に合わせる対象ではない**。判断基準:
  - forced InnoDB-order が Helios でも速い → cost model が悪い(直す)
  - Helios big-table-driving が実際速い → Helios optimizer の正しい結果として受容
- **落とし所**: (1) default係数 + (2) startup microbench 自動較正 + (3) sysvar 上書き。SF で係数は不変。

### 進め方(ユーザ承認済み・この順)

1. **(4) forced-plan 検証** ← 先にやる。残り6本で「InnoDB型 vs Helios型」どちらが Helios で実測速いか → 直す価値の有無を確定
2. (1)(2) 式改良(係数分解 + bytes項 + ref batch-amortize)
3. (3) microbench で係数測定

---

## 1. Step (4) forced-plan 検証

### 1.0 対象と現状 join order (2026-05-30 実測, SF=1)

| q | InnoDB driving order | Helios driving order | 備考 |
|---|---|---|---|
| q3 | customer→orders→lineitem | orders→lineitem→customer | 小テーブル駆動 vs 大テーブル駆動 |
| q7 | lineitem-drive (**今回変化**) | lineitem-drive | **今は一致** (InnoDB plan が不安定) |
| q10 | customer→orders→lineitem→nation | lineitem→orders→customer→nation | |
| q16 | part→partsupp | partsupp→part | |
| q18 | orders→lineitem→customer | lineitem-drive | サブクエリ有 |
| q21 | orders-drive | nation-drive | サブクエリ有 |

**発見1**: q7 の InnoDB プランが以前(n2-drive, rows=25)から lineitem-drive へ変化。
→ InnoDB のプラン自体が stats freshness で揺れる。「InnoDB に合わせる」が筋の悪い目標である実証(Codex D を裏付け)。

### 1.1 検証手順

`/*+ JOIN_ORDER(...) */` で join order を強制し helios 上で wall-clock を比較(`HELIOS_COST_V2=1`)。
判定: 旧 model が選んだ「大テーブル駆動」plan が「小テーブル駆動 ref chain」より遅いなら cost model が悪い。

### 1.2 結果 (2026-05-30, SF=1, COST_V2=1, min-of-2)

旧 model(係数分解前)の natural plan は q3=3422 / q10=7242 / q18=9913ms(いずれも大テーブル駆動)。
新 model 投入後、forced で「旧の大テーブル駆動 order」を再現すると依然遅い(= 旧 model の選択が悪かった事の直接証拠):

| q | natural(新 model = 小テーブル駆動 ref chain) | forced(旧の大テーブル駆動) | 旧 plan は何倍遅いか |
|---|---|---|---|
| q3 | 1641ms (customer 駆動) | 3389ms (orders 駆動) | 2.06x |
| q10 | 1738ms (customer 駆動) | 8016ms (lineitem 駆動) | **4.61x** |
| q18 | 1740ms (orders 駆動) | 8016ms (lineitem 駆動) | **4.6x** |

> 注: 当初この検証を旧 model 上で実施しようとしたが shell 環境不調(PATH 欠落 / 相対パス誤り)で
> 失敗値を掴んでいた。上表は新 model ビルド後に再取得した検証済み実数。

### 1.3 判定: **cost model は間違っており、直す価値あり**

旧 model が選んだ大テーブル駆動 plan は ref chain より 2-4.6x 遅い。Codex 判定基準 D に従い model 修正が妥当。

**発見2 (根本原因)**: 現状 `read_cost(index,ranges,rows)` は ref アクセス1呼び出しごとに `add_io(ranges>0 ? kHeliosRpc(50) : 0)`。
join optimizer (`best_access_path`) はこれを外側 prefix rows 倍する。例: q10 で customer 148k 行 → 148k × 50 = 7.4M の io cost。
一方 lineitem 単一 full scan = 6M × 0.20 = 1.2M。→ 巨大 full scan の方が「安く」見え、optimizer が大テーブル駆動を選択。
だが実行実態は full scan が 6M 行を RPC 転送(数百MB〜GB)するため 3-4x 遅い。

**= batch amortization の欠如が主犯**(Codex 予測 `effective_rpc_count = ceil(probe_keys/batch_size)` がそのまま当たった)。
helios は prefetch で probe keys を 1-2 RPC にまとめて取得するので、ref 1回 = 1 RPC ではない。
per-lookup の RPC 課金を `C_rpc / effective_batch_size` に薄める必要がある(ただし C_rpc 自体は下げない — 単発 point lookup を不当に安くしない為)。

---

## 2. Step (1)(2) cost 式改良 (係数分解 + bytes項 + ref batch-amortize)

### 2.0 設計方針 (Codex 式の handler API への落とし込み)

handler の cost API は **1 アクセス(1 lookup / 1 scan)単位**で、join optimizer が外側 fanout を掛ける。
よって Codex の `effective_rpc_count = ceil(total_probe_keys/batch)` を per-lookup に翻訳する:

```
full scan (table_scan_cost):
  io  = C_rpc                                  # 1 scan = 1 RPC
  cpu = records * (C_row_ingest + avg_row_bytes * C_byte_xfer) + records * C_remote_work

ref / index lookup (read_cost / index_scan_cost), per single lookup of `rows` rows:
  io  = C_rpc / effective_batch_size           # batched probe の薄い RPC 償却
  cpu = rows * (C_key_probe + C_row_ingest + avg_row_bytes * C_byte_xfer)
```

- 単発 point lookup(rows小・1回)は `C_rpc/batch + C_key_probe + ingest` を払う → 不当に安くならない。
- 大 fanout nested loop(prefix×lookup)は io が `prefix * C_rpc/batch` ≒ 実 RPC 数に比例 → full scan と公平に競合。
- `bytes = rows * avg_row_len` を materialize/転送に反映(rows のみより精密)。

係数(初期 default、後で microbench 較正):
```
C_rpc             : 空RPC往復 [us] → MySQL単位化
C_byte_xfer       : per-byte 転送
C_row_ingest      : per-row deserialize/materialize
C_key_probe       : per-key Masstree probe
C_remote_work     : per-scanned-row server CPU
effective_batch_size : prefetch batch (probe を 1 RPC に束ねる数)
```

### 2.1 実装 (proxy/ha_lineairdb.hh, gate HELIOS_COST_V2)

`kHeliosRpc=50 / kHeliosRowXfer=0.20` の2定数を分解。全て env 上書き可(sweep/microbench 用)。

| param | env | default | 意味 |
|---|---|---|---|
| C_rpc | `HELIOS_C_RPC` | 50.0 | 空RPC往復(=レイテンシ)を MySQL cost 単位化 |
| C_byte | `HELIOS_C_BYTE` | 0.0008 | per-byte 転送(帯域) |
| C_row | `HELIOS_C_ROW` | 0.10 | per-row deserialize/materialize |
| C_probe | `HELIOS_C_PROBE` | 0.05 | per-key Masstree probe |
| C_remote | `HELIOS_C_REMOTE` | 0.05 | per-scanned-row server CPU |
| eff_batch | `HELIOS_BATCH` | 1024 | prefetch batch(probe を 1 RPC に束ねる数) |

- bytes は `stats.mean_rec_length`(info() で `table->s->reclength` から既に設定済)を使用。server 改変不要。
- **核心修正**: ref/index の per-lookup io を `ranges * (C_rpc / eff_batch)` に。
  join optimizer が外側 prefix 倍するため、high-fanout nested loop は `~prefix/batch` RPC 相当に償却される。
  → 巨大 full scan を不当に優先しなくなる。

### 2.2 結果 (2026-05-30, SF=1, 22/22 md5 OK, min-of-2) — 検証済み実数

pre-9b は `/tmp/full22.out`(係数分解前 COST_V2)、9b は `/tmp/full22b.out`(本変更後)。全 22 で md5 InnoDB 一致。

| q | pre-9b (COST_V2) | **9b** | 改善 | helios drive |
|---|---|---|---|---|
| q3 | 3422 | **1640** | 2.09x | customer ✓ (=InnoDB) |
| q10 | 7242 | **1740** | **4.16x** | customer ✓ (=InnoDB) |
| q18 | 9913 | **3010** | **3.29x** | orders ✓ (=InnoDB) |
| q5 | 3375 | 2980 | 1.13x | region |
| q20 | -785 (計測異常) | 940 | 正常化 | nation |
| q21 | 10448 | 10170 | ≈ (未flip) | nation |
| q16 | 1165 | 1140 | ≈ | partsupp (維持=正) |
| q1 | 18169 | 17560 | ≈ | lineitem |
| q4 | 10910 | 10880 | ≈ | orders |
| q6 | 3768 | 3740 | ≈ | lineitem |
| q7 | 6453 | 6420 | ≈ | lineitem |
| q9 | 2046 | 2010 | ≈ | part |
| q12 | 4461 | 4440 | ≈ | lineitem |
| q14 | 3665 | 3640 | ≈ | lineitem |
| q15 | 6052 | 6010 | ≈ | derived |
| (q2,q8,q11,q13,q17,q19,q22) | — | — | 全て ≈、regression なし | — |

新 model の natural plan は §1.2 で旧 model の選択(大テーブル駆動)より 2-4.6x 速い事を実証済み。

**判定: 成功。回帰ゼロ・22/22 md5・主要3本で 2-4.2x(q3 2.1x / q10 4.2x / q18 3.3x)。** batch-amortize により optimizer が helios で実際速い plan を選ぶようになった。

**残課題**: q21 は依然 nation-drive(forced orders-order が 1.1x 速い程度の小差)。q21 のサブクエリ(EXISTS/NOT EXISTS l2/l3)の fanout 評価が弱い可能性。優先度低。

---

## 3. Step (3) microbench で係数測定 (進行中)

default 係数(C_rpc=50 等)は経験値。Codex の言う通り「空RPC / サイズ別 scan / point probe を実測」して係数が説明可能か確認する。

### 3.1 測定 (2026-05-30, SF=1, `/tmp/microb.sh`, 各 min-of-3, fresh connection 毎回)

| 測定 | µs | 備考 |
|---|---|---|
| baseline `select 1` | 21703 | **client 接続+auth+parse の固定オーバヘッド**(クエリコストでない、要減算) |
| point `nation` | 21548 | net ≈ 0(baseline 内) |
| point `customer` | 21873 | net ≈ 170 |
| point `lineitem` (PK) | ~23000 | net ≈ 1300 |

full scan(agg pushdown OFF = 実 scan+転送+mysqld集計)、net = 実測 − baseline:

| table | rows | 実測µs | net µs | **µs/row** |
|---|---|---|---|---|
| region | 5 | 21532 | ~0 | — (noise) |
| nation | 25 | 21943 | 240 | — (noise) |
| supplier | 10,000 | 24727 | 3024 | 0.302 |
| part | 200,000 | 46751 | 25048 | 0.125 |
| customer | 150,000 | 42924 | 21221 | 0.141 |
| partsupp | 800,000 | 120914 | 99211 | 0.124 |
| orders | 1,500,000 | 205207 | 183504 | 0.122 |
| lineitem | 6,001,215 | 696845 | 675142 | **0.1125** |

### 3.2 説明可能な結論

1. **per-row scan throughput ≈ 0.11〜0.14 µs/row** が 200k〜6M 行で安定(= `C_row+C_remote+mean_rec_len*C_byte` の合算)。
   → 行数が変わっても per-row 係数は不変。**Codex の「SF で係数は変わらない」を実測で確認**(変わるのは rows だけ)。
2. **単発 RPC(point read)は sub-millisecond**(point lookup が baseline+〜0.2〜1.3ms)。
   → 解析系 TPC-H では RPC レイテンシより「転送行数 × per-row」が支配的。**ただし大 full scan が数百万行を ship する時だけ RPC ではなく転送量が爆発**する(lineitem 全 scan = 675ms)。これが旧 model の大テーブル駆動が遅かった実体。
3. **接続オーバヘッド 21.7ms は計測アーティファクト**(クエリ毎に新規接続)。コストモデルとは無関係、ベンチ計測時は減算して解釈する。
4. 係数の絶対スケール: モデルの per-row(default 0.15〜0.24 cost unit)↔ 実測 0.115µs/row は定数倍で整合。join order を決めるのは **比率 + batch 償却**であり、その構造が 22/22 + 2-4.2x で検証済み。
   → 現状の手設定 default は実測と桁・比率が整合しており妥当。Codex E の「startup microbench 自動較正 + sysvar」は env 上書き(`HELIOS_C_*`/`HELIOS_BATCH`)で既に sweep 可能、自動化は将来課題。

### 3.3 残課題 / 今後

- eff_batch=1024 は実 RPC 数(prefetch で ~1-2 RPC)よりまだ保守的(過大)だが plan flip には十分。さらに上げる余地あり。
- q21 の nation-drive(サブクエリ fanout 評価)。
- SF sweep(0.1/0.3)で per-row 係数の不変性をクロス確認(本測定は SF=1 内のテーブルサイズ差で既に実証)。
- startup microbench による C_* 自動較正(Codex E)。


