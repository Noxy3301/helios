# Phase-14 TPC-C Prefetch 研究ノート(作業ログ)

> 後で Notion に移行予定。時系列の作業ログ。確定した結論は
> `docs/phase14_tpcc_prefetch_investigation.md` 側に集約。
> branch: `claude/investigate/tpcc-prefetch`

**ゴール(user, 2026-06-03)**: TPC-C のクエリが飛んできたら autogen で解析し、**1 回の RPC で必要な
ものを全部持ってきて**上手にハンドルして 2-RPC で解決する。理論的には可能なはず。Codex と相談しつつ
諸問題を解決し、(a) 変な abort をしていないか (b) データが壊れていないか (c) 性能が劣化していないか
を担保する。

---

## 計測環境メモ
- 1 warehouse / 1 terminal / SERIALIZABLE、`SET GLOBAL lineairdb_oneshot_execution=ON/OFF`。
- **proxy(mysqld)のみ再起動すれば server のインメモリデータは保持**(env 有効化に再ロード不要)。
- env: `HELIOS_TIMEPROF=1`(per-tx の rpc_exec/ingest/commit/aborted)、
  `HELIOS_ALLOW_ONESHOT_FALLBACK=1`(silent fallback 許可 — ただし batch_read miss には効かない)、
  `HELIOS_ONESHOT_PLAN`(?)= benchbase 側 explicit plan の有効化(要調査)。
- ハーネス: `/tmp/tpcc_{load,speedup,timeprof,consistency,fallback}.sh`。

## 既知の事実(調査フェーズ, 完了済 → investigation.md 参照)
- 高速化ほぼ無し(ON 141 ≈ OFF 136 req/s, goodput 137→74 半減)、prefetch 成立は ~10%。
- over-fetch 無し(成立 tx の ingest ≤1 行)。RW 整合 C1-C4 全 pass(データ破損なし)。
- abort の主因 = ONESHOT-MISS 3334:`batch_read warehouse` 2949(88%)+ `pk_value_scan new_order` 270
  + `secondary_scan idx_customer_name` 115。
- 欠陥①: PK-MRR `batch_read`(`multi_range_read_init` ha_lineairdb.cc:5017)が
  `maybe_auto_stage_oneshot_plan()` を呼ばない → warehouse 点読が plan 未 stage → 無条件 abort。
- 欠陥②: plan は tx に 1 回しか stage されない(`oneshot_plan_resolved_` が statement 境界で未リセット)。
  TPC-C は多 statement/tx なので構造的に不適合。

---

## 作業ログ

### [2026-06-03] エントリ0: 着手・タスク化
- タスク #1 benchbase patch 調査 / #2 欠陥① / #3 欠陥② / #4 検証 を作成。
- **user の重要指摘**: 「2-RPC 保持で TPC-C 全体カバー」は **benchbase にパッチを当てて SQL を先に生成
  していた気がする」→ これは explicit-plan(`HELIOS_ONESHOT_PLAN`)経路では? 私の計測は auto-gen
  (env 無し)だったので別経路の可能性。**最優先で確認**(タスク #1)。

### [2026-06-03] エントリ1: benchbase explicit-plan パッチの確認(task #1)
- **判明**: 全 TPC-C procedure に `setOrdoOneshotPlan()`(`HELIOS_ONESHOT_PLAN=1` で有効)が実装済 =
  user 記憶の「SQL/plan を先に生成する benchbase パッチ」。procedure が tx の全アクセスを列挙して
  1 プラン供給(例 NewOrder = customer+warehouse+district+(item+stock)×lines)。`FORCE INDEX (PRIMARY)` も付与。
  → これが「tx 全体事前把握一括 prefetch」の実装。auto-gen(env 無し)とは別経路。
- **実測比較(30s mix, oneshot ON)**:
  | 経路 | throughput | goodput | prefetch成立(committed/total) | 残 ONESHOT-MISS |
  |--|--|--|--|--|
  | auto-gen(env無) | 141 | **74** | 385/3719 (**10%**) | 3334 |
  | **explicit(HELIOS_ONESHOT_PLAN=1)** | **202** | **189** | 5863/7444 (**79%**) | 1551 |
  → explicit で goodput 2.5x・coverage 10%→79%。**「事前生成すれば理論通り回る」を実証**。
- **ただし explicit でも 21% abort 残**。残差の内訳:
  - 777 `secondary_scan:o_w_id oorder` = OrderStatus 最新注文(`ORDER BY o_id DESC LIMIT 1`)
  - 774 `pk_value_scan new_order` = Delivery 最古 new_order(`MIN(no_o_id)`)
  - explicit plan は両方を **scan step で表現している**(Delivery `new_order limit=1`、OrderStatus
    `oorder o_w_id limit=1 reverse=1`)が、**proxy 側がその prefetch 済 scan step を後続クエリの
    scan にマッチさせられず miss→abort**。
- 注: TIMEPROF の ingest count は「ingest 操作回数≒prefetch RPC 数」で行数ではない(over-fetch の
  行数指標としては別途要確認。両経路とも committed tx は rpc_exec=1=1 RPC)。

### 問題の3分類(確定)
- **(A) 点読カバー**: warehouse 等の点読が auto-gen で stage されない。欠陥①(MRR `multi_range_read_init`
  が `maybe_auto_stage_oneshot_plan` 未呼出)+ 欠陥②(plan が tx に1回・statement 毎でない)。
  **explicit は解決済 → auto-gen を parity に上げるのが目標**。
- **(B) 極値 scan の covering**: prefetch 済の `limit=1`/`reverse=1` scan step が実クエリ
  (`pk_value_scan`/`secondary_scan`)の cache hit にならず miss。**proxy 側、auto-gen/explicit 両方に影響**。
- task #1 完了。次: Codex 相談 → 欠陥① 実装 → 計測 → ② → B。

### [2026-06-03] エントリ2: Codex 設計相談(全文 /tmp/codex_tpcc_out.txt)
Codex の優先順と要点:
1. **欠陥① MRR staging(最初・低リスク)**: `multi_range_read_init` の `get_transaction(ha_thd())` 直後に
   `maybe_auto_stage_oneshot_plan(ha_thd(), tx)` を挿入(`multi_range_read_info_const` は costing 経路で
   状態変更不可)。double-stage は `oneshot_plan_resolved`(ha_lineairdb.cc:3315)で防止、`batch_read` が
   pending plan を発火(lineairdb_transaction.cc:196-203)。非点読 MRR は手前(:5039-5048)で default に
   逃げるので誤発火なし。TPC-H 低リスク。
2. **クラスB 二次索引マッチ(次・低リスク, production 価値大)**: prefetch は range entry に reverse+limit を
   保存(lineairdb_transaction.cc:605-623)するが、runtime `get_matching_primary_keys_in_range` が常に
   `(reverse=false,limit=0)` で引く(:1203-1204、API に limit/reverse 無し :hh78-79)→ explicit の
   `oorder o_w_id limit=1 reverse=1` が一致せず miss。**修正**: 二次 scan API に row_limit/reverse_scan を
   追加、handler から渡し、`secondary_entry_matches`(:2297-2307)で direction も照合。default `(0,false)` で
   TPC-H 不変。→ explicit 残 21% の oorder miss を解消。
3. **クラスB 主キー極値**: prefetch `limit=1` は runtime 無制限 `pk_value_scan(row_limit=0)` を被覆不可(正)。
   極値クエリ側を `(limit=1,reverse)` 同一 identity にする。generic cursor scan には limit を押し付けない。
4. **欠陥② statement 毎 re-stage(最後・最高リスク)**: plan state のみリセット(`oneshot_plan_resolved_=false`,
   `pending_oneshot_plan_steps_.clear()`, **query_id で新 statement 検知**、SQL text 不可)。local read/write/OCC
   は **リセットしない**(別フィールド)。**ハザード**: (a) reset は explicit-plan staging の前(get_transaction 内
   `execute_oneshot_plan_if_present` :4528 の前)で。(b) 「no plan→oneshot off」経路は prior local state が
   あると危険→abort に。(c) **`tx_occ_key_` がスカラで毎 prefetch 上書き(:439-441/:2512-2516)→ tx あたり
   複数 prefetch RPC は OCC token 保持不可**(要 multi-token or server 側 merge)。(d) projection metadata が
   per-tx/table → per-statement projection が先行行の decode を壊す→ multi-statement では projection 無効化 or
   per-cached-value 化。

**戦略的含意**: whole-tx **2-RPC は explicit plan(1 prefetch=1 token)でこそ成立**。auto-gen は ② を入れても
複数 prefetch RPC=OCC 多token化(大改修)が要り whole-tx 2-RPC には届かない。当面の高価値 = **B(explicit の
production 残 21% を潰す)** と **①(土台)**。②(auto-gen whole-tx)は OCC 設計判断を要する別フェーズ。

→ 実装順: **① → 計測 → B二次 → 計測 → B主キー極値 → 計測**、② は OCC 設計を詰めてから。

### [2026-06-03] エントリ3: 欠陥① 実装・計測 — 「abort storm → graceful degrade」と判明
- 実装: `multi_range_read_init`(ha_lineairdb.cc:5060 直後)に `maybe_auto_stage_oneshot_plan(ha_thd(),tx)`
  追加。ビルド OK(.so リンク)。
- **計測(auto-gen, 30s)**: throughput 141→**127**、**goodput 74→123**(≈throughput)、**abort 3334→663**、
  **warehouse miss 完全消滅**(残 = new_order 492 + customer by-name 171)、整合 C1-C4 全 0。
- **但し prefetch 成立は逆に消失**: committed=1 の oneshot tx が 385→**4**。残りは TIMEPROF を出さない=
  通常経路。FE_DEBUG トレース(8s): **auto-gen「no plan→oneshot off」1177 回 vs staged 194 回(全 1-step)**。
- **解釈(重要)**: `auto_generate_plan_from_qep` は **TPC-H の JOIN QEP 用設計で、TPC-C の単表 point-read/
  単純 scan をモデル化できず no-plan を返し oneshot を無効化**(→通常 per-row 経路で正しく commit)。
  - ①前: warehouse が MRR で **stage されず cache miss→loud abort**(3334=abort storm、retry 浪費)。
  - ①後: warehouse が **stage 試行→no-plan→oneshot off→通常経路で commit**(abort 消滅・goodput 回復・
    整合 OK)。**= prefetch ではなく graceful degrade への変換**。
  - → **① は良い修正(無駄 abort を消し正しく degrade)。だが TPC-C の真の prefetch にはならない**。
    残 663 abort は「oneshot に留まったまま miss した極値/by-name」(new_order MIN・customer by-name)。
- **結論の更新**: TPC-C を実際に prefetch(2-RPC)するなら **explicit-plan(HELIOS_ONESHOT_PLAN, 79%)が本命**。
  auto-gen で TPC-C を prefetch するには「単表アクセスのモデル化 + ②per-statement stage + OCC 多token」= 大工事。
  当面: **① keep(abort storm 解消)→ Class B(explicit の残 21% 極値を解消、production 価値大)** が高 ROI。
- 次: ① が explicit-plan / TPC-H 22-suite を壊さないか回帰確認 → Class B。

### [2026-06-03] エントリ4: ① 回帰確認(explicit-plan 経路)
- explicit-plan + ①: throughput 199 / goodput 186 / committed 5774 / aborted 1594(new_order 803 +
  oorder 765)/ 整合 0。**①前(202/189)と同等 = 回帰なし**。maybe_auto_stage は explicit staging 後
  `oneshot_plan_resolved` 済なので early-return で inert。TPC-H 22-suite も同理由で inert(query 冒頭で
  index_read/rnd_init が既 stage)→ 大きな回帰リスク無し(最終 verify で 22-suite 実走確認予定)。
- **① 確定(task #2 完了)**: abort storm 解消・goodput 回復・両経路で整合維持・回帰なし。

### 戦略の再整理(当初「①②両方」→ 調査で判明した実像)
- TPC-C を真に prefetch(2-RPC)する本命は **explicit-plan(benchbase HELIOS_ONESHOT_PLAN, 既実装, 79%)**。
- **auto-gen で TPC-C を prefetch**するには3点の大工事が必要:
  1. `auto_generate_plan_from_qep` が**単表 point-read/単純 scan をモデル化**(現状 no-plan→oneshot off)。
  2. **②per-statement re-stage**(`oneshot_plan_resolved_` を query_id 境界でリセット)。
  3. **OCC 多token**(`tx_occ_key_` がスカラ、複数 prefetch RPC/tx で last しか検証されない=並行下で不健全)。
  → ②③は OCC 設計判断を要する。auto-gen whole-tx 2-RPC は大規模 follow-up。
- **当面の高 ROI = Class B**(極値 covering)。explicit(production)経路の残 21%(new_order MIN・oorder 最新)
  を解消し 79%→ほぼ全へ。auto-gen の残 663 abort の一部(new_order)にも効く。
  - B 二次索引: `get_matching_primary_keys_in_range` に row_limit/reverse を通す(default 0,false で TPC-H 不変)。
  - B 主キー極値: 極値クエリ側を `(limit=1)` identity に。リスク中(generic cursor に limit 押し付けない)。

### 追記(2026-06-03): read-set 表現の整理 + 簡素化方針 → `phase14_readset_representation_design.md`

User 提起。(1) `filtered_rows` の O(rows) server 保持(20x 肥大)と (2) validation 表現が
3 種に増えた件(cherry-pick で簡素化したい)が **同じ一手**に収束: `filtered_rows` を畳めば
表現が 1 つ減り肥大も消える(simplify == optimize)。Codex 相談済(/tmp/codex_readset_repr.md)。
詳細は専用 doc。要点のみ:

- 3 表現 = node_version(構造 O(leaves))/ RangeHash(値 digest O(1), read-only full-cover 限定)/
  filtered_rows(値 enumerate O(rows), 残り全部)。#2/#3 は「値」軸の重複。
- 「棄却行(filter-rejected rows)」= scan が触ったが WHERE で落とした行。OCC 上は「読んだ」ので
  TID 検証対象として retain している。だが落とした行の値変化は **membership を跨ぐ時だけ**意味を持つ。
- 目標形 = 直交 2 軸 {node_version(構造), 値の membership 再検証 1 本}。TPC-C=E 固定 /
  TPC-H validation=#2 集約・#3 撤去(K-hybrid は後回し)。
- 初手(要実測): full-cover primary filtered scan が #2+#3 両方持つか計測 → 冗長なら #3 drop。

(以降追記)

