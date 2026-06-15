# Phase 22 — D1/D2: OLTP退行はcardinality + pushdownの実装欠陥である

_議事録 / 論文関連の記録。これは以前の「AGGが唯一のOLTP元凶」という枠組みを上書きする。disaggregated cost modelはworkload非互換ではない。2つの具体的なIMPLEMENTATION欠陥(名前は教科書的な手法と一致するが、実装は一致していない)がOLTP退行を引き起こした。両者ともfile:lineまで根本原因を特定し、production engine群 + 文献が正しくどう実装しているかのSOTAサーベイと突き合わせた。_

## 0. 診断の経緯(3度訂正した — 誠実さのために記録)
1. 最初は~32×のTPC-C退行をCOST_V2バンドル全体のせいにした。
2. 次に(terminals=8の分離、contentionで歪んだ可能性あり)AGG_PUSHDOWNがSOLEな元凶であり、COST_V2+OPT_STATS+SEMIJOINはparityだと結論づけた。
3. **クリーンなterminals=1のgate分離(下記)がそれを覆した: OLTPのkillerはcardinality欠陥(D1)経由のOPT_STATSである。COST_V2単独はNOT a regressionである。** AGGは*別個の*欠陥(D2)であり、opt-inである。

## 1. 決定的実験 — クリーンなgate分離(TPC-C SF1, terminals=1, 30 s/cfg, AGG off)
config毎にmysqld restart、server dataは保持、governor/C6をpin、**並行agentなし**(以前のrunはCPU-contendedだった — stock自体が3.4×低く読まれていた):

| config | order_line range est. | throughput |
|---|---|---|
| stock (all gates off) | 273 | 394 req/s |
| **COST_V2 only** | **298** (正確) | **1747 req/s** ⚠ 未検証、§5参照 |
| **COST_V2 + OPT_STATS** | **17802** (66×過大) | **10.3 req/s** (38×崩壊) |
| + SEMIJOIN (出荷時のdefault) | 17802 | 10.8 req/s |
| + RANGE_HIST | 17802 | 15.8 (ほぼ動かない) |

読み方: OPT_STATSがorder_line range estimateを298→17802へ反転させ、throughputを1747→10へ反転させる。estimateがmechanismであり、17802が文字通りのbugである。SEMIJOINはneutral。RANGE_HISTは助けにならない。(COST_V2-onlyの1747は疑わしく高く、検証中である — §5。)

## 2. D1 — composite-key非先頭rangeのcardinality(OLTPのkiller、default-onを阻む)
`proxy/ha_lineairdb.cc:3807-3818`、`records_in_range`の`eq_parts>0`ブランチ:
```cpp
estimate = key->rec_per_key[eq_parts-1];        // (w_id,d_id) prefix = records/NDV ≈ 30000+
if (eq_parts < key_parts_used)
    estimate = std::max(1, estimate / 2);        // ← the bug: /2 IGNORES the o_id range
```
order_line PK=(ol_w_id,ol_d_id,ol_o_id,ol_number)。クエリ `w_id=? AND d_id=? AND o_id IN [x-20,x)`。`rec_per_key[1]=records/NDV(w,d)≈30000`、半分にして~17802 — これは(warehouse,district)のorder集合全体を返してしまい、20-orderのo_idウィンドウ(~273行)で決して絞り込まない。
**なぜOPT_STATS下でのみか:** OPT_STATSなしでは`rec_per_key`がn乗根ヒューリスティック(`records^(2/4)≈550`, /2≈275)へfallbackし、それが偶然~accurateである。OPT_STATSのNDV-realな`rec_per_key[1]≈30000`はexact-but-wrong-for-a-rangeである。**なぜRANGE_HISTでは直せないか:** equi-depth histogramは`eq_parts==0`(純粋な先頭range)ブランチ(`:3842`)にgateされており、LEADING列(ol_w_id, NDV=1)上に構築されている — 識別性のある列はkey-part 3であり、決して到達されない。

## 3. D2 — aggregate pushdownがfull scanを強制する(別個。AGGはopt-in)
`helios_override_executor`はoptimizerが選んだaccess pathに関わらず`ha_rnd_init(true)`(`:884/918/1134`)を駆動する。`lineairdb_push_to_engine(:1741)`は選ばれた`AccessPath* root_path`を受け取りながら破棄する。`helios_offloadable_shape(:480)`はクエリの*shape*のみでgateし、access-path/row-countでは決してgateしない。TPC-C Deliveryの`SUM(ol_amount) WHERE w=? AND d=? AND o=?`(PK-bounded, ~10行)は、~300kのfull scanとして実行される(filterはrange boundではなくpost-scanのrow-skipとして適用される)。AGGはStockLevelでは発火しない(rejected: `:325`の`COUNT(DISTINCT)`、`:527`の2-table join)。

両欠陥は同じ病である: **optimizerのbounded access pathが破棄され、distribution-blindなwhole-table operationに置き換えられる** — D1はcost-estimation時に、D2はexecution時に。

## 4. SOTAサーベイ — 正しい実装(5-agent sweep: arxiv + OSS + 自前docs)
- **平均化されたper-key統計(rec_per_key / SQL Server density vector / Oracle column-group stats)は、trailing-column rangeを推定することがSTRUCTURALLYに不可能である** — 満場一致で確認された(PostgreSQL docs, Oracle, SQL Server)。leading-column histogramはD1を直せない。
- **正しいパターン = INDEX DIVE**(InnoDB `records_in_range`/`btr_estimate_n_rows_in_range`): MySQLはtrailing range列を`min_key`/`max_key`の両方へエンコードする。engineは両方のcomposite-key endpointをseekし、その間のentryを数える。Range-、prefix-、correlation-exactである。Heliosは既に`min_key`/`max_key`を受け取っており、trailing bytesを破棄している。より安価なplan-time類似物: **per-eq-prefix conditional histogram / `[min,max,count]` extent**、1回のordered scanから構築、per-statement RPCゼロ(PostgreSQL `selfuncs.c`のintra-bucket interpolation; Poosala-Ioannidis MHIST VLDB'97; multidim caseにはSTHoles SIGMOD'01 — ここではoverkill)。
- **D2の正しいパターン = 選ばれたaccess pathに乗る。** MariaDB `group_by_handler`(MDEV-6080)はoptimization*の後*に作られ「最適化されたWHEREとsuggested indexesの恩恵を受ける」。postgres_fdwは`remote_conds`を再発行してremoteが自身のindexを使うようにし + remote-vs-local aggregationをcost-compareする。Trino `applyAggregation`は既にpushされたpredicateの上に合成し、有益でないときは辞退する(`Optional.empty()`)。PushdownDB/S3-Selectの失敗モード(index無し → full-object scan)はまさにD2である。fixはboundをscanに手渡すことである。サーベイしたどのシステムも`override_executor_func`スタイルのhijackを使っていない — D2のアーキテクチャは前例がない。fixはcontractから合成される。

## 5. RESOLVED — 「COST_V2-only = 1747 req/s」はARTIFACTだった。COST_V2はTPC-Cを壊す
goodput + Unexpected Errorsで計測(throughputはerroredなtxnを数え、それらは即座に終わる → MORE errorsがHIGHER「throughput」として読まれる):

| config | goodput | Unexpected Errors |
|---|---|---|
| stock | ~390 | 0 |
| COST_V2 only (stale stats, --no-setup) | 163 / 195 | 46050 / 69542 |
| COST_V2 only (FULL setup + fresh ANALYZE) | 152 | 25716 |

COST_V2の実goodputは~150-195 — **stockの半分以下**である。fresh `ANALYZE`はそれを直さないので、stale-stats harnessのartifactではなく、real COST_V2 behaviourである。errorsはCOST_V2-specificなautogenの`ER_NOT_SUPPORTED_YET`(`ERROR 1235`)「unsupported QEP」rejection(`lineairdb_autogen.cc:103`)である: COST_V2はTPC-C statementのplanを`--prefetch-stmt` autogen pathがコンパイルできないQEP shapeへ移し、**fallbackする代わりにerrorする**。

**METHODOLOGY LESSONS (paper-relevant):**
1. **Throughputはpartial failure下で嘘をつくmetricである** — erroredなtxn(即座に終わる)を数えるので、more errorsがhigher「throughput」として読まれる。goodput + errorsを使え。
2. **TPC-CはWRITE workloadである — config間でloaded dataを決して再利用するな。** NewOrder(INSERT)、Delivery(DELETE)、Payment(UPDATE)は初期stateを変異させ(order_lineは増え、stockは減る)、in-memoryのLineairDBは前runのwrites + OCC metadataを抱える。各configはMUST fresh `lineairdb-server` + fresh mysqld + fresh load(`stop_server` → `start_server` → `start_mysql` → full create+load+ANALYZE)であり、`--no-setup`の再利用ではない。
このセッションでthroughput-onlyでAND/ORで`--no-setup` data再利用で計測された全TPC-C数値 — gate分離、parity runs、AGGをsole culpritとしてpinした元の「isolation」parity表 — は両軸でinvalidであり、cleanにやり直さねばならない。

## 6. Fix plan (Codex ②設計 GO-WITH-CHANGES; Option A first)
**D1 first (default-onを阻む、OLTP-dominant):** `:3816`の`/2`をtrailing-range narrowingに置換する — trailing key-partの整数をmin_key/max_keyから公式の`key_restore` path経由でdecodeし(signedness/key-format safe; raw bytesはそうではない)、`range_vals × rec_per_key[eq_parts]`を`rec_per_key[eq_parts-1]`でcapして推定、floor 1。HELIOS_OPT_STATS下でgateし、non-int/nullable/descending/missing-endpointの各caseでは`/2`へfail-close。gate-isolation rerun(17802→~200を期待、throughput復活)+ TPC-H md5 22/22 + composite-index ranges(q2/q9/q21)で検証。
**D2 second (AGG opt-in):** `lineairdb_push_to_engine`で`root_path`を消費する。optimizerがbounded index/range/ref pathを選んだときはoverrideをskipする(または`[min_key,max_key]`上で`ha_index_init` + `index_read_map`を駆動する)。

## 7. 改訂されたblocker chain — cost modelはdefault-onからgate-flip一発の距離ではない
TPC-C execution failure(§5)はdependency chainにおいてD1 cardinality bugのBEFOREに位置する:

1. **Autogen graceful fallback (NEW — 最初のblocker)。** COST_V2のplan変化は`--prefetch-stmt` autogen compilerのカバレッジを追い越す。unsupported QEPは`ERROR 1235`ではなくnon-prefetch executionへfallbackせねばならない。それまではCOST_V2はTPC-C上で正直に*計測*すらできない(errorsがdominateし、throughputが嘘をつく)。Plan B(prefetch無しのcost_v2)はこれがSOLE cause(errors→0, goodput復活)なのか、それともcost_v2のplan CHOICEも壊れているのかを切り分ける。
   **RESULT (Plan B, clean, T1, config毎fresh load): cost_v2 no-prefetch = goodput 354.5, errors 0; stock no-prefetch = goodput 372.6, errors 0。autogen coverage gapがSOLE causeである — cost_v2のplan executionは健全(stock-parity goodput, zero errors)。Plan Aはautogen graceful fallbackへ正しくscopeされている。cost_v2のplan CHOICEには別個のTPC-C欠陥はない(opt_stats-off時)。「4.4× win」は純粋なerror-inflationだった。**
2. **D1 cardinality** (§2): OPT_STATS path上のcomposite-key trailing-range estimate。
3. その後にようやく「cost model default-ON」がmeasurable / shippableになる。

**DECISION (記録): Plan Aを追求する — autogen graceful fallbackを適切に直す — 安易なgate-offではない。** 理由: optimizerがprefetch compilerのコンパイルできないplanを選んだときに`ERROR 1235`でfail-closeするのは*correctness*欠陥である。disaggregated optimizerのplan spaceは常にあらゆるprefetch compilerのカバレッジを超えるので、slower-but-correctなpathへfail OPENすることはmandatoryである。これがpublishableな貢献であり、数値を良く見せるためにpatchしたものではなく、A*-standardで作られている。

ここから先、ALLの再計測は**goodput + Unexpected Errors**(およびper-statement read/validate footprint用のRPC_TRACE)を使う — throughput単独は決して使わない(§5 methodology lesson)。

## 8. Plan A fix SHIPPED (A2a) — read_costでのclustered-PK materialise skip
「autogen graceful fallback」という枠組みはWRONGだった(per-row executionへfallbackすることはprefetchの目的そのものである2-RPC保証を打ち消す — RTTが爆発する)。real fixはcost_v2をautogen-compatible(2-RPC)なplanへsteerする。

**Root cause (optimizer trace + reject probe, cost_v2 ON):** 全12842件のTPC-C rejectは`UPDATE warehouse`(6564) + `UPDATE district`(6278)だった — tiny table(1-2 / 10行)上のPK-keyed UPDATEが`type=ALL`としてplanされた。`read_cost()`はclustered-PRIMARY-KEYのsingle-row fetch — これはindex→PKのdouble-hopがNO — に対してすらper-row PK-materialise RPC(`+ceil(rows/B)*C_rpc`)をchargeし、PK accessを58.6でpricingしてfull scanの53.1のABOVEにした → `type=ALL` → prefetchの「legacy DML full/reverse table scan」reject → TPC-C goodput collapse。

**Fix:** `helios_charge_materialise(uint index)`(ha_lineairdb.cc)は`index == table->s->primary_key`に対してfalseを返す — clustered rowは既に`helios_ref_cost`のtransfer termでcoverされているので、per-row PK-materialise RPCはdouble-countingである。read_cost overrideが`index`を渡す(ha_lineairdb.hh)。

**ROUTE SEPARATIONによりTPC-H-orthogonal**(Codexのopen Q3を解決): non-covering SECONDARY ref(index != primary)は依然chargeする。eq_ref NLJは`sql_planner.cc:433`でengine_cost経由でpricingされる(read_cost / このgateを決して通らない)ので、high-fanout NLJは影響を受けない。

**RESULT (clean, A2a binary):** UPDATE warehouse/district → `type=range key=PRIMARY rows=1`(以前はALL); cost_v2+prefetch TPC-C **reject 12842→0, Unexpected Errors→0, throughput 383**(real, = stock ~390)。**TPC-H no-regression CONFIRMED (A2a binary, AGG off):** md5 base & full **22/22**(MISMATCH=0, ERR=0); fullidx full 94.3s / stockcost 135.0s(ratio 0.70 == pre-A2a 103.6/150.1 = 0.69 → suite-NEUTRAL; absolute dropはcost_v2-OFFの`stockcost`にも当たるので、A2a effectではなく共有boxのcross-run env noiseである)。PK-range queriesはむしろ補正された(double-countされていない)pricingから恩恵を受ける。

**Process note:** Codex ②/④ reviewは両試行とも環境的にblockされた(`bwrap` loopback denied — このセッションはCodex sandboxがdown)。grounded Claudeがroute separation経由でそのopen Q3(eq_ref-NLJ re-regression)を解決した。**④ grounded review = GO:** domain-match exact(read_costはkey numberを受け取る、== s->primary_key domain; MAX_KEY no-PK caseはskip); route separationはdiffが主張するよりさらにSTRONGER — proxyは`primary_key_is_clustered()==false`を返すので、PK *ref* accessもpage_read_cost経由でread_costをbypassし、A2aを`type=range key=PRIMARY`のrange/ROR pathにのみeffectiveなまま残す; large-PK-range under-pricingは存在しない(PK-vs-scan gapは定数~C_rpc, R-independentであり、TPC-Hにはlarge non-covering PRIMARY rangeが無い)。2件のLOW note(commentは緩く「engine_cost」と言う; reject-logがstatic-cachedされていない) — non-blocking, commit時に整理する。

**Updated blocker chain:** (1) ✅ eq_ref/clustered-PK pricing (A2a, this section) → (2) D1 cardinality (§2, composite-key trailing range) → (3) cost-model default-on measurable。AGG_PUSHDOWNはopt-inのまま(別個, OLTP-unsafe)。

## 9. D1 fix SHIPPED — records_in_rangeでのtrailing-range narrowing
eq-prefix + trailing-rangeの`estimate/2`(ha_lineairdb.cc:3816)をInnoDB-index-dive-styleなestimateに置換した: trailing key-partの整数をmin_key/max_keyから`key_restore`経由でdecodeし(signedness/key-format safe, scratch `record[1]` + move_field_offsetへ)、`range_vals * rec_per_key[eq_parts]`を`rec_per_key[eq_parts-1]`でcapして推定、floor 1。HELIOS_OPT_STATS下でgate(+ HELIOS_TRAIL_RANGE kill switch)、non-int / nullable / descending / missing-endpoint / decode missの各caseで`/2`へfail-close。新規`helios_decode_keypart_int` helper。Codex ②設計 GO-WITH-CHANGESを適用(floor 1, lower clampなし, safe key_restore decode, guards)。

**RESULT (cost_v2+opt_stats, A2a+D1 binary):** order_line range estimate **17802 → 220**(~actual 273); stockは`eq_ref`でjoinする; cost_v2+opt_stats+prefetch TPC-C **goodput 394 / errors 0 / reject 0**(= stock ~390)。**TPC-H md5/suite no-regression CONFIRMED:** md5 base/full **22/22**(MISMATCH=0); fullidx full 94.9s(A2aの94.3sと同等 → suite-neutral)。

**STATUS:** A2a + D1により、default-deployment cost model(COST_V2 + OPT_STATS + SEMIJOIN, AGG opt-in)は今やTPC-C-clean(reject 0, errors 0, goodput = stock)AND TPC-H net-positive + md5である — cost-model default-onへの2つのOLTP blockerはクリアされた。

## 10. D2 fix SHIPPED — AGG pushdownがaccess pathを尊重する(AGGをOLTP-safe化)
「fallbackで逃げる」のではなくcost_v2をautogen-compatibleなplanへsteerしたA2a/D1と異なり、D2はAGG pushdown(opt-in)自体を直す。AGG overrideは`helios_override_executor`が`ha_rnd_init`(full table scan)をoptimizerの選んだaccess pathに関わらず駆動していた — TPC-C Delivery `SUM(ol_amount) WHERE w=? AND d=? AND o=?`(PK-bounded ~10行)が~300k full scanになり、AGG-onでTPC-C崩壊(16.6 req/s)。

**Fix:** `lineairdb_push_to_engine`(ha_lineairdb.cc:1742)で破棄されていた`root_path`を使い、`WalkAccessPaths`で最初のbase-table leafを取り、leafがbounded(REF/REF_OR_NULL/EQ_REF/INDEX_RANGE_SCAN)かつsmall(`num_output_rows() < kMinScanRows=1000`)ならoverrideをinstallしない(MySQLの通常bounded pipeline + native aggregationに委譲)。TABLE_SCAN / full INDEX_SCAN(TPC-H q1/q6/q18, large estimate)は依然fire。④review hardeningでleafがblock自身のtableか明示確認(fail-closed)。

**RESULT (full bundle + AGG on):** AGG-on TPC-C **goodput 16.6→391, errors 0**(Delivery 4940 skip, install 0); AGG-on TPC-H **md5 22/22**, suite 40s(install 9 = q1/q6/q18依然fire, skip 0 = 誤爆なし)。**AGGがOLTP-safe化した。** ④ grounded review = GO(WalkAccessPaths API-correct, wrong-leafなし, GS/grouped-semijoinとstate orthogonal, 結果不変 = optimize-time install/skip判断のみ)。Codexは環境block継続(`bwrap` loopback)。

**最終blocker chain:** (1)✅ A2a eq_ref pricing → (2)✅ D1 cardinality → (3)✅ D2 AGG access-path = **cost model も AGG も両方OLTP-safe**。reject debug logはrevert済(役目終了)。残: AGG自体をdefault-onにするかは閾値1000の頑健性を別workloadで確認後に判断(現状opt-in維持)。
