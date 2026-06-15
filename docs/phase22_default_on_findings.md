# Phase 22 — HELIOS_COST_V2 default-ON: validation findings

_Final goal-listed milestone: 「HELIOS_COST_V2 default-ON」 — 再較正した
disaggregated cost model をデフォルトで有効化し、標準 TPC-H secondary-index 一式が
デフォルト設定のまま (out of the box) net-positive になるようにする。ただし OLTP (TPC-C) を退行させないこと。本ドキュメントは、
TPC-C 退行の原因を cost-model バンドル全体のせいにしていた以前のドラフトを置き換える。
以下の isolation 実験が真因を訂正する。_

## 1. Root-cause correction: AGG_PUSHDOWN is the OLTP-catastrophic feature, not COST_V2

以前のパスでは、~32× の TPC-C 退行を cost-model バンドル全体に帰着させていた
(COST_V2 が `order_line` のアクセスを `range` から `ALL` へ反転させるため)。それは誤りだった。
single-variable isolation run (TPC-C SF1, 60 s, terminals=1, prefetch ON, governor/C6
pinned, config ごとに mysqld restart, server data は保持) でバンドルを分解する:

| config | `order_line` access | throughput |
|---|---|---|
| stock (COST_V2=0) | range | **1266 req/s** |
| COST_V2 + OPT_STATS | ALL | **1267 req/s** (parity) |
| COST_V2 + OPT_STATS + SEMIJOIN | ALL | **1255 req/s** (parity) |
| COST_V2 + OPT_STATS + **AGG** | ALL | **16.6 req/s** (catastrophic) |
| full bundle (+AGG+SEMIJOIN) | ALL | **8.0 req/s** (catastrophic) |
| COST_V2 + RANGE_HIST (separate) | — | 1302 vs stock 1315 (parity) |

`order_line=ALL` プラン自体は TPC-C throughput では無害である。catastrophe が現れるのは
`HELIOS_AGG_PUSHDOWN` を追加したとき **だけ** である: それは ~427k-row の `order_line` scan に対する
server-side `COUNT(DISTINCT)` を **トランザクションごとに** 実行する。COST_V2, OPT_STATS, SEMIJOIN,
RANGE_HIST はそれぞれ TPC-C parity である。

**Conclusion (OLTP safety):** `COST_V2 / OPT_STATS / SEMIJOIN / RANGE_HIST` はすべて
OLTP-safe であり、default-ON *candidates* である。`HELIOS_AGG_PUSHDOWN` は **opt-in**
(default-OFF) のままとする。§4 では次に、TPC-H benefit/regression の evidence で candidates を絞り込む。

## 2. Design review (Codex, ②設計 dual-review) → GO-WITH-CHANGES

Codex は 5 点を挙げた; disposition:

1. **Narrow vs broad bundle.** Codex: マイルストーンを `COST_V2 + OPT_STATS` に留めよ;
   `RANGE_HIST` と `SEMIJOIN` はそれぞれ独立した default-on 判断として扱い、各々が固有の
   TPC-H benefit/regression evidence を必要とする (それらは単に cost inputs を良くするだけでなく、
   plan flips / execution rewrites へと blast radius を広げる)。**Disposition:** 方法論として受諾 —
   §4 の fullidx/noidx sweep が各 gate の marginal contribution と regression
   envelope を独立に計測するので、default-on set は盲目的にバンドルするのではなくデータで選ばれる。
2. **RANGE_HIST floor-only ≠ plan-safe.** 推定値を引き上げることは、それでもプランを悪い方向に
   反転させうる (有用な index を回避 → table scan / より悪い join order)。結果は byte-identical
   (plan-only) のままだが、perf regression はありうる。**Disposition:** §4 で計測; §3 の md5 が
   correctness を守る。
3. **SEMIJOIN correctness class.** TPC-C parity は、inner-equi-only guards が
   outer joins / NULL keys / non-equi / duplicate-sensitive plans に対して安全であることを証明しない。**Disposition:**
   22-query md5 vs InnoDB (§3/§4) が TPC-H の LEFT JOIN (q13) と NOT EXISTS
   (q21/q22) を走らせる; byte-identical の結果がこのベンチマークにおける empirical correctness gate である。
4. **Env-parse idiom.** `(env==nullptr)||(env[0]!='0')` は 空/`false`/`off` を ON として扱い、
   gate 間で不整合だった (AGG gate は presence-based だったので、`=0` が ENABLE していた)。
   **Disposition: fixed** — §3 を参照。
5. **AGG_PUSHDOWN opt-in.** 同意。Follow-up idea (cost/shape-gated AGG) を記しておいた。

## 3. Implementation

Default-on flips (proxy only; MySQL core untouched):
- `helios_cost_v2_on()` (`proxy/ha_lineairdb.hh`) → default-ON
- `HELIOS_OPT_STATS` gate (`proxy/ha_lineairdb.cc`) → default-ON
- `HELIOS_ENABLE_SEMIJOIN` gate (`proxy/lineairdb_autogen.cc`) → default-ON (earns it: §4 −10.8s)
- `HELIOS_RANGE_HIST` gate (`proxy/ha_lineairdb.cc`) → **kept OPT-IN** (floor-only, fail-closed;
  §4 はデフォルトの AGG-off regime ではこれが NEUTRAL であることを示す; その benefit は AGG-coupled)
- `HELIOS_AGG_PUSHDOWN` → default-OFF のまま変更なし、ただし修正後の parser 経由

**Unified gate parser** (`proxy/helios_gate.hh`, Codex #4): 一箇所であらゆる gate を parse する。
Disable tokens `{0,false,off,no}` と enable tokens `{1,true,on,yes}` (case-insensitive);
unset/empty/unknown は gate の default にフォールバックする。4 つの default-on features には
`gate_default_on(name)`; AGG には `gate_default_off(name)` — これは、`HELIOS_AGG_PUSHDOWN=0`
が feature を有効化していた (presence-based gate) という以前の bug も修正する。

## 4. Results — TPC-H SF1 fullidx-vs-noidx sweep, AGG OFF (the default deployment)

`scripts/dev/phase22_defaulton_eval.sh`: 1 回の TPC-H SF1 load、mysqld restart 経由の
config sweep (server data は保持)、config ごとに 1 回の計測 matrix pass (TMO=150 s/query)、
prefetch ON。Net-positive ⇔ fullidx(cfg) < noidx(cfg)。OK-sum = OK クエリのみの合計。

| config (AGG off) | fullidx OK-sum | noidx OK-sum | md5 |
|---|---|---|---|
| stockcost (no cost model) | 150.12 s (22/22) | 235.16 s (20/22) | — |
| **base (COST_V2+OPT_STATS)** | **114.00 s (22/22)** | **185.16 s (20/22)** | **22/22** |
| base +RANGE_HIST | 114.47 s (22/22) | — | — |
| base +SEMIJOIN | **103.19 s (22/22)** | — | — |
| full (base+HIST+SJ) | 103.62 s (22/22) | 190.42 s (20/22) | **22/22** |

Reads:
- **Net-positive holds by default.** fullidx (103–114 s, 全 22 完了) ≪ noidx
  (185–235 s, さらに noidx は 2 クエリが各 >150 s で TIMES OUT する — つまり noidx は
  その OK-sum が示すよりも悪い)。Indexes は COST_V2+OPT_STATS だけでも net-positive である。
- **COST_V2+OPT_STATS:** −36 s (stockcost 150.12 → base 114.00)。Core fix。
- **SEMIJOIN:** −10.8 s (base 114.00 → +sj 103.19)、md5-clean → **earns default-ON.**
- **RANGE_HIST:** +0.47 s (base→+hist) および +0.43 s (+sj→full) — この AGG-off regime では
  頑健に **NEUTRAL** (どちらの基準から測っても一貫; commit 済みの q3/q10 benefit, suite 31.75→28.42 s, は AGG-ON だった)。
  Codex #2 (floor-only の bump でも、未計測の workloads ではプランを反転させうる) と合わせると、
  default-ON には値しない → **kept opt-in**、analytical deployments では AGG と共に走らせる。
- **Correctness:** base と full の両方が md5 22/22 vs InnoDB — SEMIJOIN は TPC-H の
  LEFT JOIN (q13) と NOT EXISTS (q21/q22) を byte-for-byte で保持する。

**Absolute scale:** デフォルト deployment (AGG off) は ~103 s で、goal の headline
**31.75 s — これは AGG を必要とする** (q1/q6/group-by の server-side aggregation) の ~3× である。
AGG は OLTP-catastrophic (§1) なので、31.75 s という数字は明示的な analytical
opt-in (`HELIOS_AGG_PUSHDOWN=1`, 理想的には + `HELIOS_RANGE_HIST=1`) としてのみ到達可能である。
これが中心的な feasibility finding である: 単一の global default は OLTP-safe であると同時に
analytical headline に当てることはできない; cost model + semijoin は safe-by-default かつ net-positive であり、
最後の ~3× (aggregation pushdown) は query-shape gating ("AUTO") が入るまでは opt-in である。

Note: helios "fullidx" は、その DDL がサポートする `postload-mysql.sql` indexes の subset のみ
materialise する (sec_idx=6 of 23; これまでの phase22 fullidx run すべてと同じ load path)。よって
この比較は goal の 31.75 s / 39 s baselines と整合的である。

## 5. Verdict

**Ship default-ON: `COST_V2 + OPT_STATS + SEMIJOIN`** (各々が TPC-H で net-positive と計測され、
md5-clean; §1 ですべて TPC-C-parity)。**Keep opt-in: `RANGE_HIST`** (デフォルトでは neutral、
benefit は AGG-coupled) **and `AGG_PUSHDOWN`** (OLTP-catastrophic)。Default-on semantics は
unified な `helios::gate_default_on/off` parser 経由; いずれも `=0` で disable できる。shipping binary 上の
TPC-C parity: `scripts/dev/phase22_tpcc_parity.sh` (default vs stock) を参照。
