# Phase-7 per-query attribution: time + memory breakdown (prefetch path, SF=1)

計測条件: `lineairdb_oneshot_execution=ON`(prefetch/2-RPC)+ `HELIOS_OPT_STATS=1`(NDV)
+ `HELIOS_RO_NOVALIDATE=1`、SF=1、全クエリ timeout 付き。InnoDB は 3308(buffer-pool 16G)。

- 時間内訳: proxy `HELIOS_TIMEPROF`(`rpc_exec`=prefetch RPC=server scan+serialize+network、
  `ingest`=proxy 側 deserialize+行 materialize、`commit`=validate RPC)+ `HELIOS_HANDLER_TIMEPROF`(`set_fields` 等)。
- メモリ: `memprofile_per_query.sh`(mysqld と server の RSS を 10Hz サンプリング、idle→peak の delta)。
  mysqld 再起動を per-query 実施しクリーン baseline。server は再起動しない(idle が常駐データ baseline)。

## 全22 時間内訳(prefetch path)
```
Q     total    rpc_exec   ingest  commit  set_fields
q1   16602ms   4522.7    156.1    0.0    1911.10
q2    4016ms   2911.7    650.2    0.0      41.42
q3    4154ms   2944.5    386.1    0.0      57.70
q4    1006ms    778.6     35.6    0.0      21.52
q5    4459ms   3161.0    508.2    0.0      84.47
q6    3594ms   3188.0      3.0    0.0      24.09
q7   19163ms  15456.2   2329.2    0.0      72.41
q8    5883ms   3725.5    674.8    0.0     200.28
q9   24667ms  18651.7   3145.8    0.0     285.37
q10  17800ms  13173.4   1902.6    0.0     255.73
q11   2440ms   2047.5    221.7    0.0       0.00
q12  12728ms   6503.9   1023.9    0.0     181.62
q13   4619ms   2441.6    438.7    0.0     154.92
q14  20527ms  12400.2   2157.7    0.0    1365.78
q15   7994ms   3937.1    166.9    0.0    2256.34
q16    455ms    252.5     24.2    0.0      10.82
q17    154ms     68.8      1.2    0.0      16.64
q18  10806ms   5844.2    511.3    0.0     863.36
q19    128ms    108.5      2.0    0.0       2.93
q20   1032ms    746.4     54.8    0.0      38.17
q21  29420ms  19215.4   4143.2    0.0     796.44
q22   3581ms   3037.8    248.3    0.0      34.18
```
- commit=0.0 全クエリ = RO_NOVALIDATE の 1-RPC 化が機能(validation/commit RPC を撤廃)。
- 重クエリは **rpc_exec が 60-81%** を占める。ingest が次点。set_fields は大量行 materialize の q1/q14/q15/q18 で顕著。

## メモリ(代表サブセット、mysqld Δ / server Δ = per-query 作業集合)
```
Q    hel_ms  inn_ms  ratio  mysqld_ΔGB  server_ΔGB  innodb_peakGB  rows
q1   17743   6296    2.8     +4.03       +3.66       3.37(flat)     5
q7   20275    753   26.9     +4.96       +4.35       3.37(flat)     5
q9   28003   2622   10.7     +5.92       +3.80       3.37(flat)   176
q14  20774   1041   20.0     +4.81       +3.24       3.37(flat)     2
q21  30049   3190    9.4     +8.29       +6.79       3.37(flat)   101
q19    158    171    0.9     +0.03       +0.06       3.37(flat)     2
```
- 重クエリは **2〜176 行しか返さない**のに mysqld と server をそれぞれ +3〜8GB(合計 8〜15GB)膨張させる。
- InnoDB は同じ join を buffer pool でストリーム処理し **3.37GB 横ばい**(per-query 膨張ほぼ無し)。

## 統合結論
時間とメモリは**同一現象の表裏**: prefetch が「集約・フィルタ前の作業集合(数百万行)を server で
スキャン+シリアライズし、proxy に転送して materialize」している。
- **時間**: その RPC(`rpc_exec`)が支配的。
- **メモリ**: その作業集合が server 側(retained)+ proxy 側(ingest)で二重に常駐。
- 対して InnoDB は同じ join をインプロセス buffer pool でストリームし、中間結果を RAM に溜めない。

prefetch は「per-row RPC 爆発(q3 が 274s)→ 2-RPC(5s)」を達成した上での話で、**残る対 InnoDB 8x と
8-15GB は『作業集合まるごと materialize』のコスト**。Phase 2 NDV はプラン爆発を防ぐ役割を正しく果たしており、
ここは別レバー。

## 最も効くレバー(次の打ち手候補)
rpc_exec とメモリの両方を同時に削るのは **server 側 pushdown** = prefetch が「必要な列・行・集約済み中間結果」だけを
返すようにする:
1. **projection pushdown 拡張**(既存 [[helios-projection-pushdown]] を全クエリに効かせ、不要列を ship しない)。
2. **filter pushdown**(WHERE 述語を server scan に押し込み、reject 行を ship しない/materialize しない)。
3. **partial aggregation pushdown**(GROUP BY/集約を server で部分実行 → q1/q7/q9/q14 のように少数行しか返らない
   クエリは中間数百万行を ship せず済む。最大のメモリ・時間削減)。
4. 補助: **RPC 圧縮**(既存 `HELIOS_RPC_COMPRESS`)で network 分のみ削減(scan/serialize/materialize は残る)。

文献的にはこれは「prefetch(pull)」に「computation pushdown(push)」を足す方向で、
survey が対抗パラダイムとして挙げた FlexPushdownDB 系と合流する。prefetch を default にしつつ
集約/フィルタを pushdown するハイブリッドが、disaggregated TPC-H の自然な次の一手。
