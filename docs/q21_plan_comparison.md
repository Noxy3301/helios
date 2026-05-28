# Q21 プラン比較: 現状 vs 「OR-union 1スキャン」モデル

Codex review + 実コード解析を踏まえた、Q21 (TPC-H supplier-who-kept-orders-waiting) の
現状動作と user 提案 (OR union) との対比メモ。

---

## 1. Q21 SQL (TPC-H spec、SF=1 で実測対象)

```sql
SELECT s_name, COUNT(*) AS numwait
FROM supplier, lineitem l1, orders, nation
WHERE s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (SELECT * FROM lineitem l2
              WHERE l2.l_orderkey = l1.l_orderkey
                AND l2.l_suppkey <> l1.l_suppkey)
  AND NOT EXISTS (SELECT * FROM lineitem l3
                  WHERE l3.l_orderkey = l1.l_orderkey
                    AND l3.l_suppkey <> l1.l_suppkey
                    AND l3.l_receiptdate > l3.l_commitdate)
  AND s_nationkey = n_nationkey
  AND n_name = 'SAUDI ARABIA'
GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100;
```

- 3 つの `lineitem` self-reference: `l1`, `l2`, `l3`
- MySQL optimizer は l1 を outer scan、l2/l3 を correlated subquery (l_orderkey 一致)
  に変換

---

## 2. 現状の自動生成プラン (HELIOS_FE_DEBUG=1 実測)

```
[QEP] root_type=31 collect_ok=1 leaves=6
[QEP] step0 S:./benchbase/lineitem    idx=                                  ← l1
[QEP] step1 FE:./benchbase/supplier   idx= B0.CI col=3 off=0 len=0           ← supplier per l1
[QEP] step2 FE:./benchbase/orders     idx= B0.K  col=0 off=0 len=8           ← orders per l1
[QEP] step3 FER:./benchbase/lineitem  idx= B0.K  col=0 off=0 len=8           ← l2 per l1
[QEP] step4 FER:./benchbase/lineitem  idx= B0.K  col=0 off=0 len=8           ← l3 per l1
[QEP] step5 FE:./benchbase/nation     idx= B1.CI col=4 off=0 len=0           ← nation per supplier
[QEP] projection tbl=./benchbase/orders keep=2/9
[QEP] projection tbl=./benchbase/nation keep=2/4
```

**プラン構造**:
- `S:` = 全件 scan (unfiltered, full range, `name_refs(lineitem)=3` のため filter pushdown 無効)
- `FE:` = source row 毎の PK point read (1 行ずつ)
- `FER:` = source row 毎の PK-prefix range scan (=l_orderkey で複数行ヒット)
- `B0.K` / `B0.CI` = 「step0 の row N の "PK の指定範囲" / "value column N" を probe key にする」

### LineairDB 側の実際の実行 (`server/rpc/lineairdb_rpc.cc::handleTxExecuteReadPlanStreamed`)

step ごとに分岐:
- **`!for_each && is_scan`** → `db->StatelessRangeScan(table, start, end)` を 1 回
- **`for_each && !is_scan`** → source step の各 row N について `build_plan_key(N, bindings)`
  → `db->StatelessRead(table, key)` を N 回
- **`for_each && is_scan`** → source step の各 row N について
  `db->StatelessRangeScan(table, key, key+1)` を N 回 (FER) /
  `db->StatelessSecondaryRangeScan(...)` を N 回 (FES)

SF=1 lineitem ≈ 6,001,215 行で、Q21 の実行内訳:

| step | 種類 | server-side 呼び出し回数 | 返却 row 数 (約) | 計算式 |
|---|---|---|---|---|
| 0 | S:lineitem | StatelessRangeScan ×1 | 6.0M | 全件 |
| 1 | FE:supplier | StatelessRead ×6.0M | 6.0M | per l1 |
| 2 | FE:orders | StatelessRangeScan ×6.0M | ~6.0M | per l1, ~1 row hit |
| 3 | FER:lineitem (l2) | StatelessRangeScan ×6.0M | ~24M | per l1, ~4 row/orderkey |
| 4 | FER:lineitem (l3) | StatelessRangeScan ×6.0M | ~24M | per l1, ~4 row/orderkey |
| 5 | FE:nation | StatelessRead ×~10K | ~10K | per supplier |

合計: **54M lineitem rows**, **24M+ server-side sub-scan**, wire data ~8.5GB,
proxy ingest メモリ 24GB (実測)、latency 108s。

### proxy 側の cache 形態

`proxy/lineairdb_transaction.cc:LocalRangeScanEntry`:

```cpp
struct LocalRangeScanEntry {
  std::string table_name;
  std::string start_key, end_key;
  std::vector<std::string> scan_keys;     // ←PK昇順ソート済み
  std::vector<std::string> scan_values;   // ←対応する value(parallel)
  std::vector<RangeValidationEntry> range_versions;  // OCC用
  ...
};
```

`range_scan_index_ = unordered_map<start_key, vector<entry_idx>>` で **start_key 単位で
entry を引く**。FER は各 probe ごとに別 entry (start_key=encode(l_orderkey)) なので
exact-start hash で O(1)。エントリ内の vector を順次返すだけ → MySQL の rnd_next も
index_next もここで完結。

**重要な制限** (line 1698-1742 `slice_range_entry`):
```cpp
LocalRangeScanEntry cached = src;   // ← 全 src を deep copy
for (...) { 線形で rows をフィルタ }
```
**slice は O(N) + 1回の deep copy**。N=6M の step0 を毎 probe で slice したら破綻。

---

## 3. User 提案: 「Planner 解析で OR-union → 1 テーブル 1 scan」

頭の中のイメージを具体化すると:

```
inner subquery l2 が必要なのは
   "{ l_orderkey: l_orderkey ∈ outer の l_orderkey set }"
inner subquery l3 が必要なのは
   "{ l_orderkey: l_orderkey ∈ outer の l_orderkey set } AND l_receiptdate > l_commitdate"

→ l1 (= outer) と合わせた「lineitem に必要な行集合」は
     (l_receiptdate > l_commitdate) OR (l_orderkey ∈ outer の l_orderkey set)
```

仮想 DSL イメージ:

```
S:./benchbase/lineitem WHERE
     (l_receiptdate > l_commitdate)                      -- l1 が必要
  OR (l_orderkey IN derived(step_l1, col=l_orderkey))    -- l2/l3 が必要
```

= 「lineitem は 1 回だけ scan、その中で **3 つの ref を満たす union 行集合だけ
返す**」。proxy 側は 1 つの cache entry に全部入る。inner subquery の probe は
そこから出す。

### この方式の難点(現実装に当てはめると)

1. **derived(step_l1, ...) の bind タイミング**: step_l1 が「lineitem の `l_receiptdate >
   l_commitdate` フィルタ通過行」だとすると、その l_orderkey 集合は step_l1 を
   走らせ終えないと確定しない。つまり **2 段階の RPC** (step0 → step_inner) になり、
   現状の "1 RPC で全部" が壊れる。

2. **OR の片方が広すぎる場合**: l1 自体が「lineitem 全体から filter で 70% 残す」
   ような場合、`l_orderkey IN (l1 の l_orderkey set)` は **lineitem の大半をカバー** →
   OR union ≈ ほぼ全件。ならば最初から **filter 無しで全件 scan** した方が条件評価コスト
   ゼロで速い。

3. **server-side IN-set 評価コスト**: `l_orderkey IN (~6M values)` を server 側で評価
   するには hashset(~200MB)を作って lineitem 全行に対して probe = 1.2G hash 操作。
   現状の FER は per-row sub-scan で 6M × log(n) = 120M Masstree 操作なので、IN-set の
   方が **遅い可能性が高い**。

### Q21 で OR-union 思想を素直に当てはめると現状とほぼ同じ

- l1 の filter = `l_receiptdate > l_commitdate` → lineitem ~75% match
- 既に `name_refs(lineitem)=3` で **filter pushdown を自動 OFF** にしてる(line 1798-1802)
  ので step0 は **filter なしで全件**。
- → OR-union の結論 = filter 無し全件 scan = **step0 そのもの**

つまり Q21 の場合、user 提案の OR-union 形態は **既にコード内で半分実現**してる。
残ってる無駄は **step3/step4 が同じデータをもう一度取り直してる** という後段の問題。

---

## 4. 現実的な最適化案 (proposal)

### Phase-1: planner で redundant step を「drop」する

**条件** (全部満たす step S_inner を drop):
- `S_inner.for_each && S_inner.is_scan` (FER のみ; FES は別扱い)
- `S_inner.index_name.empty()` (primary)
- 同じ `physical_table_key` に対し別 step S_outer が存在し、S_outer は:
  - `is_scan && !for_each && index_name.empty()`
  - `bindings.empty() && end_bindings.empty()`
  - `key_prefix.empty() && end_key_prefix == 0xff×16`
  - `scan_limit == 0`
  - `filter_serialized.empty()`
- どの後続 step も S_inner を binding source に使ってない

Q21 で step3, step4 が drop される。プラン: **6 → 4 steps**。

### Phase-1 の前提: slice path の効率化

drop すると、MySQL 側の inner probe は `lookup_local_range_scan` の full-cover
fallback (line 1818-1835) で step0 に到達する。しかし現状 `slice_range_entry` は
**O(N) deep-copy** なので、6M probe × 6M deep-copy = 破滅。

なので **Phase-1 と一緒に slice 高速化**が必要:

```cpp
// 提案: 高速 slice (zero-deep-copy)
static LocalRangeScanEntry slice_range_entry_fast(
    const LocalRangeScanEntry& src,
    const std::string& start_key, const std::string& end_key) {
  LocalRangeScanEntry out;
  out.table_name = src.table_name;
  out.start_key = start_key;
  out.end_key = end_key.empty() ? src.end_key : end_key;
  out.reverse_scan = src.reverse_scan;
  out.row_limit = src.row_limit;
  // scan_keys は sorted → 二分探索で window 確定
  auto lo = std::lower_bound(src.scan_keys.begin(), src.scan_keys.end(), start_key);
  auto hi = std::lower_bound(src.scan_keys.begin(), src.scan_keys.end(), out.end_key);
  size_t i0 = lo - src.scan_keys.begin();
  size_t i1 = hi - src.scan_keys.begin();
  out.scan_keys.assign(src.scan_keys.begin()+i0, src.scan_keys.begin()+i1);
  out.scan_values.assign(src.scan_values.begin()+i0, src.scan_values.begin()+i1);
  // range_versions: src のものを共有 OR 空(step0 の version は既に
  // range_validation_set_ に activate 済み)
  out.range_versions = src.range_versions;   // shallow copy of pointers OK?
  // rows: 同様に二分 + 範囲 assign
  ...
  return out;
}
```

これで 1 probe ≈ O(log N) + O(slice_size ~4) = ほぼ定数。6M probe で 120M 操作。

### 期待効果 (Q21 SF=1)

| 指標 | 現状 (gate=ON) | Phase-1 (drop + fast slice) | 削減 |
|---|---|---|---|
| Plan steps | 6 | 4 | -33% |
| Server sub-scans (lineitem inner) | 12M | 0 | -100% |
| Wire data | ~8.5 GB | ~1.0 GB | -88% |
| Proxy peak RSS | 24.4 GB | ~5-8 GB (推定) | -67% |
| Latency | 108 s | 推定 10-20 s | -80% |

### Correctness (Codex 確認済み)

step0 (unfiltered full primary range) の `range_versions` は **lineitem 全 leaf を
カバー**。任意の leaf への insert/update は step0 が必ず観測 → OCC で commit 時に
弾かれる。step3/step4 の per-probe NodeVersionEntries は step0 の真部分集合で、
追加保護を提供しない (Codex 引用: "If step0 is a true unfiltered full primary range,
an insert into a step3 primary leaf is covered by step0's range validation")。

filter 付き S: の場合は別物として扱う必要あり (現 `range_entry_matches` で既に
`if (!e.filter_serialized.empty() && !exact_range) return false;` で拒否)。

### Out of scope (今回は手を出さない)

- **FES (secondary inner)**: Q9, Q12, Q13 等の `lineitem WHERE l_partkey=X`
  パターン。secondary index cache が別途必要 (Codex 確認)。
- **真の OR-union DSL**: predicate を server に union-pushdown する方式。実装が
  大規模 + 多くの場合「filter 無し全件 = OR-union 結果」になるので利益薄。
- **slice_range_entry の汎用書き換え**: 既存パスを壊さないように、新規 fast path
  を追加して dedup-drop 経由のみ通す方が安全。

---

## 5. 結論: どちらの方式を採るか

User 提案 (OR-union) は **思想として正しい** が、Q21 においては:
- l1 の filter が ~75% match で広い
- name_refs=3 ですでに filter pushdown OFF
- → OR-union の素直な実装結果 = **filter 無し全件 scan** = 現 step0 と同等

つまり Q21 では **OR-union と「step0 を残して step3/step4 を drop」は等価**。
実装の素直さは drop の方が上 (planner 後段の局所改造のみ、proto/DSL 変更なし)。

→ **Phase-1 patch = drop + fast slice** を提案。OR-union のフル実装は将来の汎用
最適化に残す (利益薄い案件)。

## Appendix: Q9 / Q18 のような FES self-join

```sql
-- Q9 (簡略): nation, supplier, partsupp, lineitem (l_partkey via secondary), part, orders
-- lineitem は part 経由で l_partkey で probe → FES
```

Phase-1 では FES self-join は drop 対象外 (secondary cache が別系統)。Phase-2 で
secondary index の full prefetch + dedup を検討。
