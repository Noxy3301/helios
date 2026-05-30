# Phase-9: optimizer cost model + semijoin reduction (heavy-join attack)

計測: SF=1、helios(proxy+RPC server)vs InnoDB(buffer pool 16G)。両エンジン同一クエリ。
正答性は全て md5(helios == InnoDB)で確認。cost は join 順だけ変え結果は不変。

## 背景
Phase-8(aggregation pushdown)後、対 InnoDB 残差は **多テーブル join**(q7/q9/q10/q12/q14/q21、
helios 総時間の ~73%)。EXPLAIN 比較で2系統の原因を特定:
1. **join 順退行**: helios は「小テーブル起点で大テーブルを index-nested-loop」、InnoDB は
   「大テーブルを selective filter で先に絞る」。q9 で helios=nation駆動 vs InnoDB=part駆動(green先行)。
2. **実行モデル**: 同プラン(q7)でも prefetch が中間 join 集合を全 materialize/ship(orders 150万・
   lineitem 180万 = 最終 26 行なのに 350万行)。

## 真因 = cost model(統計ではない)
当初「NDV 統計が危うい」を疑ったが、実機 STATSDBG で **NDV/rec_per_key は正しく optimizer に届いて
いた**(part rpk=1, partsupp ps_suppkey 80, lineitem l_partkey 25, orders 16, nation 5 — InnoDB近似)。
SHOW INDEX cardinality=2 は SHOW 実行時の stats 未取得アーティファクトで、optimizer 値とは無関係。

真因は `proxy/ha_lineairdb.hh` の **`scan_time()=records*10`**。full scan を InnoDB の 25-100倍に高評価し、
「大テーブル先行 filter」を全部潰していた(コメントに "discourages full scan" と意図的)。一方
`read_time()=ranges + rows*0.5` は index ref を安く見せ、深い nested-loop を許容。

## Cost model 修正(commit: perf(cost-model)、gate HELIOS_COST_V2)
MySQL 8.0 の `scan_time`/`read_time`(deprecated)でなく **`Cost_estimate` API**
(`table_scan_cost`/`read_cost`/`index_scan_cost`)を override。helios の実コスト(I/O ページでなく
**転送行数 × per-row serialize/RPC/ingest + RPC 往復**)に比例:
```
table_scan_cost = records * kHeliosRowXfer  + 1 RPC
read_cost       = rows    * kHeliosRowXfer  + (ranges>0 ? 1 RPC : 0)
index_scan_cost = 同上
kHeliosRowXfer = 0.20, kHeliosRpc = 50.0
```
これで full scan は records 比例(大テーブルは高い=先行 filter に倒れる、小テーブルは安い=index系を壊さない)、
ref は matched rows 比例(選択的 join は安い)。

**段階的に判明した教訓**:
- 第1版(scan_time=page-count): 重 join は速くなるが q6/q3/q15 が **2x 悪化**(full scan を安くしすぎ、
  index で済むクエリが full scan に倒れた)。Codex 確認: `table_scan_cost=data_file_length/IO_SIZE` は
  SF1 大表で records の 1/100 に圧縮 → range/ref より安く見える。
- 第2版(Cost_estimate API + records 比例): 重 join 速いまま **q6/q3/q15 の回帰消滅**。
- `page_read_cost`/`worst_seek_times` も足したが(classic optimizer の secondary ref が直接使う経路)、
  **A/B で q10/q3 を逆に悪化させ q7 も直らなかったので撤回**。3関数 override が最良点。

### 結果(SF=1, all 22 md5 == InnoDB)
```
Q     baseline  optA(COST_V2)  InnoDB   駆動表一致
q9    26798ms   2864ms         4842     ✓ part (InnoDB超え)
q10   17233ms   1874ms         1847     ✓ orders (並)
q14   20328ms   3723ms         1303     ✓
q12   13530ms   3769ms         1510     ✓
q7    20600ms   7949ms          916     ✗ (q7のみ lineitem駆動、但し2.5x速)
q21   28653ms   ~10000ms       3169     —
q6     3646ms   3651ms          959     ✓ 回帰なし
q3     4290ms   4139ms          712     ✓ 回帰なし
q15    5815ms   6040ms         1102     ✓ 回帰なし
```
重 join 群が 2.5〜9.4x 高速化、軽量/index クエリは回帰なし。q7(6テーブル join)のみ駆動表が
InnoDB と異なる(n2=nation25行起点を選べない)が時間は改善済み。**default 化はまだ gate のまま**
(SF違い・microbench で係数固定してから、と Codex)。

## Semijoin reduction(commit: feat(semijoin)、gate HELIOS_ENABLE_SEMIJOIN)
cost model で join 順は直るが、prefetch の中間 ship 量は別問題。**semijoin/Bloom reduction**:
high-fanout probe step の join 鍵が、earlier の選択的 filter step と同じ equality class にあるとき、
server が filter 済み source 行から membership set を作り、**join 相手の無い probe 行を ship 前に落とす**。
join 自体は compute 側に残す(Exadata bloom-offload / Trino dynamic-filtering と同型、full join
pushdown ではない=disaggregation 維持)。

実装: `PlanStep.semijoins{source_step, source_column, probe_column, source_filter}`。server は
source_filter を通過した source 行からのみ鍵を集める(point-read source は全行を executor に ship しつつ
membership は green サブセットに縮約)。proxy plan-gen が join equality graph(union-find over Field*)で
「selective source → 同 class の later high-fanout probe」を検出して張る。

**効果**: q9 lineitem ship 6M→319k(19x削減)、md5 不変。ただし時間改善は小(probe 自体は partsupp
80万起点で走るため per-probe コストが残る — Codex 指摘どおり、より効くのは partsupp step への適用)。
gate OFF で 22-suite は InnoDB と byte 一致。

**既知の不具合(gate ON のみ)**: q22 の anti-join/相関サブクエリに semijoin が誤適用され numcust が
~3x(本来除外すべき行を残す)。offload whitelist の絞り込みが必要。default OFF なので通常影響なし。

## まとめ / 残課題
- cost model は「disaggregated storage engine の optimizer cost を page I/O でなく RPC/転送に較正」=
  研究貢献になり得る(Codex)。q9 が InnoDB を上回るのが象徴的。
- 残: (a) cost model の係数を SF違い・microbench で固定し default 化、(b) q7 6-way の駆動表、
  (c) semijoin を partsupp step へ(probe 数削減)、(d) q22 semijoin whitelist 修正、
  (e) semijoin の OCC footprint(read-only scope では range_versions で成立)。
