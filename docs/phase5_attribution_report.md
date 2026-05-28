# Phase-5 Attribution Report (2026-05-29)

Handler-entry timing(`HELIOS_HANDLER_TIMEPROF=1`)で、これまで "handler_residual"
として未分解だった時間を MySQL handler 入口別に分解した。TPC-H SF=1、physical OCC、
Step C revert 済(per-row TID 記録 reinstated)。

**注意**: handler timer 自体が clock_gettime×2 + get_transaction を per-call 入れるため、
6M 回呼ばれる rnd_next / 10M 回の set_fields では計測オーバーヘッドが ~1-2s 乗る。
絶対値はやや膨張、相対分解は有効。

## 計測結果(handler timer ON、3 代表クエリ)

```
                  Q1       Q18      Q21
walltime          40.3s    42.5s    49.5s
─ rnd_init        12.2s    15.8s    19.0s  ← prefetch (rpc_exec + ingest) を内包
   ├ rpc_exec      6.6s     6.9s    10.1s    (server scan+encode+wire+proxy decode)
   └ ingest        5.6s     8.9s     8.9s    (ReadPlanResult → proxy cache, std::string×N)
─ rnd_next        14.2s    12.1s    18.2s  ← MySQL が cache 行を 1 行ずつ iterate
   (calls)        5.9M     6.0M     6.0M
   └ set_fields    2.5s     1.4s     5.2s    (helios row 形式デコード, nested)
─ idx_read         0s       4.8s     3.9s  ← join probe(cache hit, O(1))
─ idx_next         0s       1.2s     0s
─ commit           7.5s     8.0s     7.5s  ← OCC: per-row TID 検証(Step C revert で重い)
─ ext/start/store  ~0       ~0       ~0
```

## 結論: handler_residual の正体

「Q21 の 18s residual」= **rnd_next 18.2s(6M 行 iterate)+ idx_read 3.9s**。
内訳:
- **set_fields(helios row デコード)** 5.2s … helios のコード、最適化余地あり
- **rnd_next plumbing(vector index + executor 返却)** ~13s … 大半は MySQL executor が
  6M 行を読む intrinsic コスト。call count は query が全行必要とするため削減不可。
  (計測 timer overhead ~1-2s 込み)

## 本質: helios は同じ行を「4 回」触る

```
①server scan + encode + wire (rpc_exec)        6.6-10.1s
②proxy decode + materialize  (ingest)          5.6-8.9s
③MySQL rnd_next iterate + set_fields decode     12-18s
④commit: per-row TID 再検証   (commit)          7.5-8.0s
```

InnoDB は同じ 6M 行を **1 回**(buffer pool page を in-process で executor に zero-copy
で渡す)。helios は disaggregation により ①転送+物質化 → ③再 iterate+再デコード →
④再検証 で **行を 4 回通過**。これが 5-13x の geomean gap の構造的要因。

## helios 側で最適化可能な chunk(優先度)

| chunk | 時間 | 最適化方向 | 性質 |
|---|---|---|---|
| ingest | 5.6-8.9s | zero-copy decode (Step D-2/D-3) | 大型 infra refactor |
| commit | 7.5-8.0s | OCC validation set の転送/検証圧縮 | O(rows) は本質、encoding 改善のみ |
| set_fields | 2.5-5.2s | row デコードの per-field store 削減 | 中規模 |
| rpc_exec | 6.6-10.1s | server proto bypass (Step D-2) | 大型 |

**intrinsic(削減不可)**:
- rnd_next の call count(6M)= query が全行を要求するため
- idx_read の call count(4M)= NLJ の join probe 数

## perf flamegraph

(注: perf は環境に未インストール/権限不足の可能性。handler timer で十分な分解が得られた
ため、本レポートは timer ベース。perf 取得は Phase-6 で環境整備後に補完予定。)

## Phase-6 への示唆

1. **commit が全クエリ一律 7.5-8s** は注目。physical OCC + per-row TID 記録の検証コスト。
   read-only でも 6M TID を validate。**OCC validation の転送・検証を O(rows) から
   O(leaves) に寄せられないか**(Masstree node version だけで済ませ、TID 検証を減らす)
   が最大の単一 win 候補。ただし Step C で「value-update 検知に per-row TID 必須」と
   判明済 → HTAP 安全性とのトレードオフ再検討が要る。
2. **ingest + rpc_exec(transport/materialization)= 12-19s** は Step D-2/D-3 の対象だが
   大型 refactor。
3. **rnd_next(行 iteration)~13s は intrinsic** — helios では削れない(MySQL executor の
   仕事 + 行が全部要る)。InnoDB との差は「in-process zero-copy」対「cross-process
   materialized」の構造差で、アーキテクチャの床。

→ 単一 query を InnoDB 並みにするのは構造上困難。**commit の OCC 圧縮**が唯一
"big lever かつ中規模" の候補だが Step C の教訓(per-row TID は correctness 必須)と衝突。
