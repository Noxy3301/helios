# Phase 22 M2b — full calibration design (completing the missing NNLS protocol)

_②設計 (dual-review対象). ①調査(agent, grounded)で確定したDONE/MISSING matrixを起点に、
methodology(docs/phase22_m2_methodology.md)の未達5要素を完成させる。_

## 0. ①調査で確定した現状 (DONE / PARTIAL / MISSING)
- **DONE:** (b) access-class micro-bench, (c) true-cardinality probe表(m2b_probe_gen.py),
  (i) kC_materialise物理再導出。
- **PARTIAL:** (a) C_rpc-as-unit — 実際はS_scan(table_scan CPU=0.15cu)をanchorに置換。
- **MISSING:** (d) joint NNLS fit, (e) 識別性ゲート(cond#/VIF/Belsley), (f) engine_cost
  eq_ref fit, (g) multi-SF(0.1/1/3) scale不変, (h) robustness(LOO-CV + 2-10x摂動)。
- 現定数(ha_lineairdb.hh, 全env-tunable, static-once): C_rpc=50, C_byte=0.0008, C_row=0.10,
  C_probe=0.05, C_remote=0.05, B_eff=1024, **C_materialise=8.0**。M2bで定数は1つも変わってない。

## 1. 中心的発見と本設計の目的
(i)の結論: **kC_materialise=8.0は物理~0.2-0.33cuの~20-40x ＝ steering値であり、物理から
再導出されない**(物理値0.27にするとq7=12.4s/q8=8.3s/q3=5.5sで suite退行)。すなわち
**「物理cost定数の較正だけではplan品質は改善しない；真のレバレッジはcardinality」**
(Leis VLDBJ-18; 本セッションのD1=複合キー末尾range cardinality修正が実証済)。

本設計の目的は **未達の affirmative protocol(joint NNLS + 識別性ゲート + multi-SF +
robustness)を完成させ、cost定数の物理的較正可能性を厳密に検証する**こと。

**PRE-REGISTERED HYPOTHESIS (結果の先読みを避ける; m2b_design.md:37-41を踏襲):**
- **H (model-validated):** joint-NNLSのphysical C_materialiseが現行8.0の[0.5x, 2x]帯内に
  収束し、かつSF0.1/1/3でscale不変 ⇒ 物理較正で十分=cost modelはVALIDATED。
- **¬H (steering-required, negative result):** physical C_matが8.0から1桁以上乖離(既存
  §7.1 hold-outでは0.27=~30x乖離が観測済=supporting evidenceだが、full protocolは未走)
  ⇒ 8.0はsteering値(cardinality補償)、物理較正は不十分、cardinalityが次のlever(D1で着手済)。
- 物理定数(C_rpc/C_byte/S_scan)のscale不変性(=hardware定数か)は (g) で別途判定する独立の問い。
- **どちらに転んでも論文の芯**: Hなら「物理cost modelが勝ちを再現」、¬Hなら「物理較正の
  限界=cardinality lever」を、識別性ゲート付きで厳密に立証する。先に答えを書かない。

## 2. 設計哲学 — cost-driven自動 (user directive)
**フラグは実行モード(prefetch / batched)の根本選択にのみ使う。** 最適化(cost model /
pushdown / semijoin / cardinality)の適用可否を「このworkloadだから許可」と人間がフラグで
切り替えるのは反パターン。最適化は **cost / cardinality / 不変定数で自動判断**されるべき。
M2bの較正済み定数はその基盤: 定数が正しければoptimizerが自動でscan/ref/pushdownを選ぶ。
- D2の閾値`1000`(num_output_rows, ha_lineairdb.cc:1782)を cost-derived crossover に置換する
  のが理想だが、**②review指摘で本設計からはDESCOPE**: fitted access定数(C_rpc/C_byte/C_row/
  C_mat/B_eff)だけでは不足——D2が選ぶ代替の *native集約コスト* (server側 ROW_EVALUATE級の
  per-row CPU over bounded N rows)は fitted定数に含まれず、crossover不等式 pushdown_cost(R)
  vs native_cost(R) を解くには non-access項の追加fitが要る。現状は1000をenv-tunable
  (HELIOS_AGG_MIN_SCAN_ROWS)既定のまま残し、cost-derived化はM2b完了後のfollow-upとする。
- HELIOS_COST_V2/OPT_STATS/SEMIJOIN/AGG/RANGE_HISTフラグは将来的にmeasurement/debugの
  kill switchへ降格(production defaultは常時有効・cost自動判断)。本設計はその根拠(定数の
  識別可能性 + cost-derived閾値)を提供する。

## 3. 未達5要素の実装計画
### (d) Joint NNLS fit
- `scipy.optimize.nnls` を stacked design matrix に対して。現 m2b_fit.py の per-class
  OLS(`np.linalg.lstsq`)を joint NNLS に置換。**新規 m2b_nnls.py は m2b_fit.py の parser
  (parse()/med_by(), trace JSONLから(plan,n,us,resp_b)抽出, :25-62)を import 再利用**(②review #1)。
- 非負拘束(cost定数は物理的に≥0)。C_rpcをunitにpin(係数1に固定)し残りを比で。
- **secondary_flag は construction上 non-identifiable と事前宣言(②review #3, 無限loop回避)**:
  本エンジンは covering実行が無い(HA_KEYREAD_ONLY無し, ha_lineairdb.hh:251 / m2b_findings:30)
  ため covering vs non-covering は byte差のみ=secondary_flag列はbytes列と完全共線。よって
  4列`[n_rpc,bytes,n_rows,secondary_flag]`は **3列`[n_rpc,bytes,n_rows]`に縮約**してfitし、
  secondary_flag非識別を *finding* として記録する(=fit failureでない; (e)の再設計loopに入らない)。
### (e) 識別性ゲート
- `numpy.linalg.cond`(column-standardized matrix) + 列ごと VIF + Belsley condition index。
- **受理 iff cond < 30 AND max VIF < 10**。非識別/共線なら micro-query再設計(probe表の
  bytes/rows/rpcを脱相関させる: 行数固定でbytesだけ振る等)してre-fit。loop。
### (f) engine_cost eq_ref fit (②review #2訂正: vacuousでない=eq_refの唯一lever)
- **訂正**: 前案は「page_read_cost非override→eq_refはHelios cost非通過→vacuous」としたが
  推論が逆。eq_refは `sql_planner.cc:432-433` の `prev_record_reads * page_read_cost(1.0)`
  で価格付けされ、`page_read_cost`はMySQL handler virtualで `mysql.engine_cost`
  (io_block_read_cost/memory_block_read_cost)が `Cost_model_table::page_read_cost`
  (opt_costmodel.cc:79-93)をfeedする。**Heliosのoverride(read_cost/index_scan_cost/
  table_scan_cost)はこのsiteにtouchしない**ので、engine_costこそeq_refに届く唯一のlever
  (親design cost_model_design.md:128 M1 / :144 M4a/M4bと整合)。vacuousではない。
- 実装: eq_ref micro-bench(点読N回latency)で engine_cost(io/memory_block_read_cost)をfit
  (methodology step7)。非covering-secondary refは find_cost_for_ref:163で handler
  `page_read_cost(keyno,num_rows)`(=override可能site, 別軸)。
- **omission解決(②review #5): q7のaccess classを実測確定**。q7はeq_ref想定だが
  m2b_findings:170でC_mat sweepに反応(12.37→1.53) ⇒ q7のfact-table accessが実は
  non-covering rangeの可能性。EXPLAINで chosen plan を確認し、eq_ref(engine_cost lever)か
  non-covering range(read_cost/C_mat)かを確定してから engine_cost storyを組む。
### (g) multi-SF scale不変
- SF0.1 / SF1 / SF3 を **server reload(stop_server厳禁) + probe-gen + NNLS** で各々re-fit。
- **scope(②review #4): re-fitするのは micro-probe(cal_n/cal_w)のみ**(各SFで cal_* を
  probe_gen→fit、安価=cal_*再load)。**22-suite hold-out(§7.1)はSF1で1回のみ**(per-SFで
  22-suite再走=~30分backfill×3は不要)。問うのはmicro-probe由来の物理定数のSF間安定性だけ。
- 物理定数(C_rpc/C_byte/S_scan)のSF間安定性(scale不変=hardware)を実測。steering定数が
  SF依存(=非物理)であることを対照。
### (h) robustness
- leave-one-query-out CV(各regressor除外でfit→除外queryのplan予測)。
- 2-10x rec_per_key/prefix_rowcount摂動で plan-flip しないか(safety margin)。

## 4. Harness (再利用 vs 新規)
**再利用:** m2b_probe_gen.py(probe表), m2b_taxonomy.sh(restart-only bench driver),
m2b_accept.sh(restart-per-value sweep), m1_sweep.sh(plan-shape+timing), cstate_guard.sh。
**新規:**
- `m2b_nnls.py` — joint scipy.nnls + 識別性ゲート(cond/VIF/Belsley)。
- `m2b_engine_cost.sh` — eq_ref到達性実証 + (非vacuousなら)eq_ref micro-bench。
- `m2b_multisf.sh` — SF0.1/1/3 server-reload re-fit driver + scale不変チェック。
- `m2b_robust.sh` — LOO-CV + 摂動 plan-flip。

## 5. 運用制約 (絶対・①調査再確認)
- build=`scripts/build_partial.sh`。build前は **mysqldのみpid kill**(`kill -9 $(cat /tmp/mysql.pid)`)。
- **`stop_server.sh`絶対禁止**(in-memory SF1消失→~30分backfill)。serverはmysqld再起動を跨いで維持。
- InnoDB ref `:3308`は触らない。
- 定数はenv-only + static-once読込 → **mysqld再起動sweepで足りる(rebuild不要)**。`SET GLOBAL`不可。
- 計測env: mysqld `HELIOS_OPT_STATS=1 HELIOS_COST_V2=1 HELIOS_AGG_PUSHDOWN=1
  HELIOS_ENABLE_SEMIJOIN=1`; server `HELIOS_PARALLEL_SERVER=1 HELIOS_PARALLEL_SERVER_SCAN=1`;
  起動後 `SET GLOBAL lineairdb_prefetch_execution=ON; lineairdb_prefetch_ro_novalidate=ON;
  optimizer_switch='mrr_cost_based=on,batched_key_access=off'`; cstate_guard必須。dev=SF0.1, 検証=SF1。
- 不変条件: MySQL本体無改変・1SR・tx跨ぎキャッシュ禁止・branch内commit(claude/phase22-cost-model)。

## 6. 受理基準 (④検証)
- (d) NNLS係数が非負で収束、C_rpc-pin下で比が物理traceと整合。
- (e) cond<30 ∧ VIF<10 を満たす micro-query設計に到達(or 非識別を明示)。
- (f) q7のaccess classをEXPLAINで確定 → eq_refなら engine_cost fit(io/memory_block_read_cost);
  non-covering rangeなら read_cost/C_mat軸として扱う(②review: "vacuous"撤回済)。
- (g) 物理定数がSF0.1/1/3で安定(scale不変)、steering定数は非物理として対照。
- (h) LOO-CV + 2-10x摂動で結論(物理較正 insufficient / cardinality lever)が頑健。
- 既存perf(SF1 fullidx<noidx)とcorrectness(md5 22/22)を維持(定数変更があれば再測)。
