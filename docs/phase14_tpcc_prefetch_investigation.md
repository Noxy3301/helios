# Phase-14 TPC-C Prefetch 調査: 高速化度合い・over-fetch・RW 健全性・fallback 根本原因

**目的(user, 2026-06-03)**: TPC-C の prefetch(oneshot execution)が現状どれだけ速くなっているか、
不要データを拾っていないか(over-fetch)、read-only でない write 経路で壊れていないか。
さらに「prefetch 時は fallback させたくない思想。fallback しているならその原因を調査」。

計測 env: 1 warehouse / 1 terminal / SERIALIZABLE。`SET GLOBAL lineairdb_oneshot_execution=ON/OFF`。
proxy(mysqld)のみ再起動すれば server のインメモリデータは保持される(TIMEPROF 等の env 有効化に再ロード不要)。

## 0. 結論サマリ

| 観点 | 結果 |
|------|------|
| **高速化** | **ほぼ無し**。ON 141 req/s ≈ OFF 136 req/s、avg latency 7.1 vs 7.3ms。prefetch が効くのは全 tx の **~10%** だけ |
| **over-fetch** | **無し**。prefetch が成立した tx は ingest **≤1 行**(avg 0.12, max 1)= 完全に tight な点読 |
| **RW 健全性** | **壊れていない**。TPC-C 整合条件 4 つ(C1-C4)が ON 実行後も全て violation 0。abort は正しく巻き戻り/再適用 |
| **fallback/abort の原因** | **2 層の欠陥**(下記 §3)。warehouse 点読が prefetch plan に載らず無条件 abort(全 abort の 88%) |

## 1. 高速化の実測(prefetch OFF vs ON)

| | throughput | **goodput** | avg latency | 95th |
|--|--|--|--|--|
| OFF(oneshot=OFF) | 135.98 | 137.15 | 7343μs | 22890μs |
| ON(oneshot=ON) | 141.12 | **73.80** | 7077μs | 19462μs |

throughput ほぼ同じなのに **goodput が半減**。goodput=成功(retry されなかった)分。
→ ON では大量の tx が abort→retry しており、prefetch の利得が出ていない。

## 2. over-fetch / RPC 数(HELIOS_TIMEPROF, 15s mix, oneshot ON)

3719 の oneshot tx のうち:
- **385 tx**: `rpc_exec=1, ingest=1 行, committed=1` — prefetch 成立。**1 RPC・1 行で over-fetch ゼロ**。
- **3334 tx**: `rpc_exec=0, commit=0, ingest=0, aborted=1` — prefetch を**実行すらせず abort**(= ONESHOT-MISS)。

→ over-fetch は皆無。問題は「不要に拾う」ではなく「**カバーできず abort する**」。

## 3. fallback/abort の根本原因(本題)

`note_oneshot_miss`(`lineairdb_transaction.cc:1853`)は **無条件 abort**(`HELIOS_ALLOW_ONESHOT_FALLBACK` を見ない)。
実証: fallback フラグ ON でも ONESHOT-MISS は 0 にならず goodput も回復しない(74.3 のまま)。

ONESHOT-MISS 3334 件の内訳(operation × table):
| 未カバーアクセス | 件数 | 出どころ |
|---|---|---|
| **`batch_read` warehouse**(w_id=1 点読) | **2949 (88%)** | NewOrder/Payment 全件 |
| `pk_value_scan` new_order(MIN(no_o_id) 走査) | 270 | Delivery |
| `secondary_scan:idx_customer_name` customer | 115 | Payment/OrderStatus by-name |

### 欠陥①(主因, 88%): PK-MRR `batch_read` 経路が prefetch plan を stage しない
- warehouse は `SELECT W_TAX FROM warehouse WHERE W_ID=?`(単一行 PK)→ MySQL が const/EQ_REF と判断し
  **PK-MRR 経路**(`multi_range_read_init` `ha_lineairdb.cc:5017`、`multi_range_read_info_const:4977` が
  PK lookup で `HA_MRR_USE_DEFAULT_IMPL` をクリア)→ `tx->batch_read()` `:5070` に流れる。
- だが `multi_range_read_init` は **`maybe_auto_stage_oneshot_plan()` を呼ばない**
  (`index_read_map:1506` / `rnd_init:3730` は呼ぶ。MRR 経路だけ欠落)。
- → plan 未 stage → `execute_pending_oneshot_plan()`(`batch_read:203`)が no-op → warehouse key が
  local read set に無い → `batch_read:259` `note_oneshot_miss` → **無条件 abort**。
- **修正の当たり所**: `multi_range_read_init` 先頭(`tx->batch_read` 直前 ~`:5060`)で
  `maybe_auto_stage_oneshot_plan(ha_thd(), tx)` を呼ぶ(`index_read_map:1506` と同形)。

### 欠陥②(構造的, 補強): plan は「tx に 1 回」しか stage されない(statement 毎でない)
- `oneshot_plan_resolved_` は初回 stage で true になり **statement 境界でリセットされない**
  (`maybe_auto_stage_oneshot_plan:3315` が resolved 時 early-return)。`LineairDBTransaction` は
  tx 内で使い回し(`get_transaction:4516`)。
- TPC-C tx は **多 statement**(warehouse→district→item×N→stock×N→insert…)。oneshot prefetch は
  元々 **TPC-H = 1 statement/tx** 用設計で、TPC-C = N statement/tx には構造的に不適合。
- → 欠陥①を直しても、1 つの staged statement だけでは tx 内の後続 statement のアクセスをカバーできない。
  ~10% しか commit しないのはこのため(touch するもの全てを単一 staged statement で賄える tx だけ成功)。
- 2-RPC を保ったまま TPC-C tx 全体をカバーするには、(a)statement 毎 stage(=多 RPC 化で 2-RPC 契約崩れ)か
  (b)tx 全体を事前に知って一括 prefetch(命令的 JDBC では困難)。**設計判断が要る所**。

### 残り 2 パターン(副次)
- new_order の MIN(no_o_id) 走査(Delivery)= `pk_value_scan` 未カバー。
- customer の last-name 二次索引引き(Payment/OrderStatus by-name)= `secondary_scan` 未カバー。
  どちらも fallback サイトはあるが no-fallback 思想では abort 対象。

## 4. RW 健全性(整合条件)

TPC-C consistency(spec 3.3.2)を OFF+ON 実行後に検証 — **全 violation 0**:
- C1: per-warehouse `W_YTD == Σ D_YTD`
- C2: per-district `D_NEXT_O_ID-1 == max(O_ID) == max(NO_O_ID)`
- C3: per-district `max(NO_O_ID)-min(NO_O_ID)+1 == count(new_order)`
- C4: per-district `Σ O_OL_CNT == count(order_line)`

→ prefetch ON の write 経路でも**データ破損なし**。abort は安全に巻き戻る。
read 最適化(projection/ro_novalidate/read_skip)は `SQLCOM_SELECT` gate で write 経路から隔離済
(`ha_lineairdb.cc:3336/3509/499`)、write buffered 時 hard-abort ガードあり(`lineairdb_transaction.cc:2494`)。

## 5. 次アクション候補
- **欠陥①の修正**(MRR 経路に stage 追加)= warehouse 88% を救う最小・高効果パッチ。要 build+再計測。
- 欠陥②(statement 毎 stage / tx 一括 prefetch)= 設計判断。TPC-C で prefetch を本当に効かせるなら必須。
- 修正後は本 doc の OFF/ON/goodput/ONESHOT-MISS 内訳で before/after を取る。

計測ハーネス: `/tmp/tpcc_load.sh`(load) `/tmp/tpcc_speedup.sh`(OFF/ON) `/tmp/tpcc_timeprof.sh`(RPC/ingest/abort)
`/tmp/tpcc_consistency.sh`(整合条件) `/tmp/tpcc_fallback.sh`(fallback 無効の実証)。
