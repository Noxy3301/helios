# Phase 16 worklog — autogen 真因解明 (Track A) + TPC-H 遅query外科手術 (Track B)

> **ゴール (user, 2026-06-12)**: Track A = TPC-C autogen が stateful より遅い (125 < 140 req/s) 真因の
> 区間別計測による解明と修正 (目標 >140、explicit 191 に接近)。Track B = TPC-H を遅い順に外科手術
> (NDV 統計 + COST_V2 / projection pushdown / partial agg pushdown / LineairDB メタデータ削減、
> 目標: 対 InnoDB 中央値 5x 以内 + ピーク RSS 半減)。
> 前フェーズ: docs/phase15_prefetch_maxopt_worklog.md (全28エントリ)。
> ルール: 議事録必須 / push・hard reset 禁止 / build_partial.sh / 検証ガントレット
> (TPC-H matrix→md5 22/22, OLTP 3+2モード回帰+C1-C4) / server メモリ過剰使用禁止。

## 開始時点の状態 (2026-06-12)

- ブランチ: claude/prefetch-maxopt (cdc7d16)、working tree clean、push なし。
  submodule: LineairDB helios/prefetch-maxopt (3f5a6f7) / benchbase helios/prefetch-maxopt (6f3f578e)。
- 稼働中: lineairdb-server + mysqld 3307 (TPC-H SF=1 ロード済, prefetch ON + ro_novalidate ON)
  + InnoDB 参照系 mysqld 3308 (SF=1)。
- 基準値: TPC-C stateful 140 / autogen 125 / explicit 191-200 req/s。TATP autogen 1591。
  TPC-H SF=1 22/22 + md5 一致、対 InnoDB 5-100x (docs/phase15_innodb_vs_helios_sf1.csv)。

### [2026-06-12] エントリ1: Track A 区間別計測器(TxRpcTrace 拡張)

**目的**: autogen 125 < stateful 140 の真因切り分け。容疑者 = (a) 毎statement再コンパイル
CPU、(b) over-fetch、(c) stage したのに使われない二重払い、(d) range entry validation 肥大。

**設計**: 新規 env gate を作らず**既存 ENABLE_RPC_TRACE 基盤(per-tx JSONL)を拡張**。
RPC 種別ごとの回数/時間/バイトと statement 境界、cache hit/miss イベントは既に記録済み
だったので、不足分の「RPC 以外の区間時間」と「カウンタ」を追加:
- `TxRpcTrace::record_section(kind, us)` + `record_section_count(kind, n)` +
  RAII `SectionTimer`(trace 無効時は分岐1回のみ、clock 読まない)→ JSONL に `sections` 出力
- 計測区間: `autogen_compile`(QEP→plan)/ `txplan_parse`(explicit DSL)/
  `stage_rpc_decode`(RPC+flat decode; ネットワーク純分は TX_EXECUTE_READ_PLAN で別記録)/
  `stage_local`(キャッシュ staging)/ `lookup_range`・`lookup_secondary`(scan cache copy)
- カウンタ: `autogen_steps` / `staged_rows` / `commit_base_rows` / `commit_range_entries` /
  `commit_range_keys` / `commit_write_ops`
- `rpc_trace_` を mutable 化(const な lookup 関数が自己計時するため)
- 集計: 新規 `bench/bin/rpc_trace_agg.py`(JSONL→ tx数/duration percentiles/RPC種別表/
  sections 表/local view 表/residual(=MySQL executor+proxy CPU)/SQL正規化 statement 別時間)

**変更ファイル**: proxy/rpc_trace.{hh,cc}, proxy/lineairdb_transaction.{hh,cc},
proxy/lineairdb_prefetch.cc, bench/bin/rpc_trace_agg.py(新規)

**計測手順**(/tmp/track_a_trace.sh): 3モード(stateful / autogen --prefetch-stmt /
explicit --prefetch)それぞれ「server 再起動+TPC-C リロード(trace OFF)→ mysqld を
ENABLE_RPC_TRACE=1+モード別 path で再起動 → benchrun --no-setup --external-server
--time 60」。トレースのオーバーヘッドは3モード同条件なので分布比較は公平。

### [2026-06-12] エントリ2: 計測条件の重要発見 — phase15 ベースラインは terminals=1

最初の traced run を benchrun デフォルト(--terminals 64)で流したら stateful が
throughput 1709 / goodput 430 / **retry 246k**(NewOrder 167成功 vs 138k retry)という
別世界の数字になった。bench/results を遡って確認した結果:

- **phase15 の TPC-C 基準値(stateful 140 / autogen 125 / explicit 191)は全て
  `--terminals 1` の単スレッド・レイテンシ比較だった**(summary.csv の terminals 列=1)。
  Track A の「autogen はなぜ遅い」は per-tx レイテンシの内訳問題として扱うのが正しい。
- 64 terminals × 1 warehouse は Silo OCC の競合崩壊 regime(NewOrder/Payment が
  D_NEXT_O_ID 等のホット行 RMW で衝突)。これは別軸の事実として記録
  (高並行 TPC-C は phase15 でも未計測。warehouse 数を terminals に合わせて増やすのが
  TPC-C の正規のスケール法)。
- 対処: /tmp/track_a_trace.sh に `--terminals 1` を入れて3モード取り直し。

### [2026-06-12] エントリ3: Track A 真因確定 — 3モード trace 比較(terminals=1, 60s)

throughput(traced): stateful 132.3 / autogen 121.3 / explicit 185.1(トレード約5%減、
ギャップは未トレース時と同型)。集計(bench/bin/rpc_trace_agg.py):

| per-tx | stateful | autogen | explicit |
|---|---|---|---|
| mean duration | 7.3ms | 8.0ms | 5.0ms |
| RPC 回数 | 19.7 | **21.6** | **2.0** |
| RPC 時間 | 2.2ms | 2.7ms | 0.34ms |
| staged rows | — | **407** | 33.6 |
| commit base rows | — | **406** | 43.6 |
| commit range keys | — | **381** | 17.5 |
| validate req bytes | —(END req 50B) | **38.7KB** | 7.5KB |
| 受信 bytes | ~4.5KB | **35.5KB** | 9.2KB |
| residual(JDBC往復+CPU) | 5.1ms | 5.2ms | 4.7ms |

**真因(寄与順)**:
1. **RPC 回数が stateful より多い(21.6 vs 19.7)**。per-statement staging は「1 statement
   = 1 RPC」で、TPC-C は 1 statement ≈ 1-2 行なので per-row RPC と回数が変わらない。
   さらに **SELECT...FOR UPDATE → UPDATE 同一行ペアが二重 staging**(stock 33k+33k RPC、
   district/warehouse/customer も同型)— UPDATE 側の legacy-DML staging が直前 SELECT で
   キャッシュ済みの行を再 fetch していた。
2. **over-fetch**: Delivery の `SELECT NO_O_ID FROM new_order WHERE NO_D_ID=? AND
   NO_W_ID=? ORDER BY NO_O_ID ASC LIMIT 1` が **58.7KB/RPC**(district の全 new_order
   ~300行を staging、LIMIT 1 無視)。autogen compile は意図的に「canonical unbounded
   shape」で staging(lineairdb_autogen.cc:421 コメント、limit pushdown は v2 work)。
3. **validation 肥大**: range cache hit が「全 cached 行の base-row TID + range replay」を
   両方 append(LIMIT で 1 行しか消費しなくても ~300 行ぶん)→ validate req 38.7KB。
4. compile は **無実**(2us/回、220ms/60s 全体)。PS plan cache 仮説は棄却。
- 補助観測: autogen_compile 12.9/tx vs plan_request 20.65/tx の差 ≈ legacy-DML handler
  経路(compile timer 未通過)。use_point_read 26.4/tx で再読はキャッシュが効いている。
- **構造的天井の認識**: 全モード共通の residual ~5ms/tx はほぼ JDBC statement 往復
  (~34 stmt/tx)。per-statement staging の理論下限 ≈ stateful(first-touch 読みは両モード
  とも server 訪問が必須)。explicit 級(2 RPC/tx)に近づくには **同型 tx の read-set
  テンプレート学習による先読み**(Chardonnay 風、cross-statement)が必要 — miss は既存の
  abort→retry 安全網で正しさ担保。

### [2026-06-12] エントリ4: Track A 修正ラウンド1(F1+F3)

- **F1: カバー済み point step の staging スキップ**(lineairdb_transaction.cc
  execute_read_plan 冒頭): binding 無し・key 定数の scalar point step が write set /
  row_cache に既収載なら step を間引き、全 step カバー済みなら RPC 自体を省略。
  SELECT→UPDATE ペアの二重 fetch を根絶(~6-8 RPC/tx 削減見込み)。正しさ: cache hit
  消費時に TID が base_row_read へ載る(use 時検証)ので staging 再取得と等価。
- **F3-lite: range hit 時の base-row validation を消費行のみに**:
  - keys-only variant(get_matching_keys_in_range): per-row append を全廃。根拠 = key
    bytes は delete+insert なしに変わらず range replay が membership/order を保証、値の
    後続読みは read() cache-hit 経路が on-use で TID append(staging は validate_on_use=true
    で record_row_cache 済み)。
  - values variant: limit 窓内の行のみ TID append(窓外は値を観測していない)。
- 期待効果: RPC 21.6→~13/tx、validate req 38.7→~7KB。Delivery の 58.7KB staging 自体は
  F2(limit-aware staging + scan hint + truncation abort)の領域で次ラウンド判断。
- Codex レビュー(計測器 diff)反映済み: staged_rows の空 scan 過大計上修正(steps[i] で
  分岐)、stage_rpc_decode→stage_rpc_and_decode(overlapping 明示、集計側で除外+
  decode_only 導出)、trace OFF 時の無駄計算ガード。

(以降、変更・計測ごとにエントリ追記)
