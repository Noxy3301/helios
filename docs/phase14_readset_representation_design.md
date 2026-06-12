# Phase-14: read-set validation 表現の整理(enumerate / digest / re-scan)+ 簡素化方針

User 提起(2026-06-03)。2 つの動機が **同じ一手**に収束する:
1. `filtered_rows` が O(rows) の (key,tid) を commit まで server 保持 = 実測 ~20x mem 肥大
   (SF1 TPC-H で filtered 5.9M、`helios-occ-rangekeys-bloat`)。
2. range read の validation 表現が **3 種類**に増えていて、cherry-pick で main に載せる時に
   簡素化したい(「複雑にすると何もわからなくなる」)。

→ **`filtered_rows` を畳むと、表現が 1 つ減り、かつメモリ肥大も消える。simplify == optimize。**

## 現状(feature branch のみ。origin/main には未到達 — 確認済 a595b71/cadf9be NOT on main)

range read の OCC footprint を検証する表現が今 3 つ、しかも**重なっている**:

| # | 表現 | コスト | 守るもの | scope(gate) |
|---|---|---|---|---|
| 1 | **node_version**(`range_reads`/`index_reads`) | O(leaves) ~24B/range | 構造 phantom(insert/delete で leaf version bump) | 常時・ワイヤ送出 |
| 2 | **RangeHash**(SHA-256 32B/range) | O(1)/range | value-update + 構造。commit で re-scan→memcmp | `start_key==""`(full-cover primary)∧`!for_each`∧secondary でない∧read-only(`use_range_hash`) |
| 3 | **filtered_rows**(enumerate {table,key,tid}) | **O(rows)** | filter 棄却行の value-update | 上記 2 に当てはまらない全部(bounded / secondary / for_each / filtered / **write txn**) |

問題は「3 つある」こと自体より **重複**: full-cover primary filtered scan は #2 と #3 を**両方** retain しうる
(commit は filtered_rows を先に merge し、`use_range_hash` なら hash も照合。tx_occ_store.hh:66 + lineairdb_rpc.cc:2151)。
※ ただしコメント lineairdb_rpc.cc:1687 は「full-cover は hash・per-row skip / bounded は per-row 保持」とも読め、
**この重複の有無は最初の実測対象**(full-cover primary filtered scan の filtered_rows 件数を数える)。

## 直交 2 軸へ畳む(簡素化の目標形)

3 表現は本来「構造」と「値」の 2 軸で、#2/#3 は同じ「値」軸の重複実装:

- **構造軸 = node_version**(O(leaves))。insert/delete を検知。安く正しく直交。**残す。**
- **値軸 = 1 つの membership 再検証**に統一。#2(digest)と #3(enumerate)を畳む。

→ 表現は **{node_version, value 再検証}** の 2 つ、各々ジョブが 1 つで重複なし。これが cherry-pick 形。

## R(re-scan)の正しい姿 — Codex 訂正 + membership 反論

Codex(/tmp/codex_readset_repr.md 相談)の核心訂正: **「predicate + node_version の re-scan」だけでは
enumerate と byte 等価にならない**。slip 例: 棄却行 k(TID t0)を別 tx が同 leaf 内の非 index 値更新で
t1 に。k は依然 predicate 不成立。構造不変 → node_version pass、re-scan も棄却のまま → t0 を持たない限り
比較対象が無くすり抜ける。E なら `exact_read_tid_moved` で abort。

**ただし byte 等価は要件ではない(本 doc の主張)**: この slip(棄却→棄却のまま値変化)は
クエリ結果集合を変えない → serializable 的に **abort 不要**(E が過剰 abort しているだけ)。
本当に守るべきは **membership**:
- 棄却→受理(値変化で predicate を満たすように)= 構造不変の value-update、node_version では拾えない**真の危険**。
  → commit で re-scan + predicate 再適用 → 「今 pass する集合 P_now」を作り、**元の pass 集合 P_orig
  (= 既に `reads` にある受理行キー、O(passed))と比較**。差があれば abort。これで検知できる。
- 受理行の値変化 = `reads` の TID 検証で既に担保(filtered_rows ではない)。

→ **棄却行 O(rows)(5.9M の肥大本体)は捨てられる。** 必要なのは O(passed)(既存)+ predicate(O(1))+
commit の membership 再スキャン(CPU O(scanned)=SF 比例、hash でないので collision 無し)。
これが #3 を畳んだ後の「値軸」の実体で、#2(digest)はその確率版(collision 有)。

## ワークロード別の落とし所(Codex ランキング + 上記)

| ワークロード/形状 | 推奨 | 理由 |
|---|---|---|
| TPC-C write txn(小 range・点読) | **E(enumerate)** | range 小 → O(rows) も小。re-scan は commit に 2nd index walk を足し p95/p99 を悪化。Silo の lock→validate→install 相に enumerate が既に整合(database_impl.h:1100) |
| TPC-H read-only(no-validate ON) | **retention しない** | commit RPC 自体無し(lineairdb_rpc.cc:1873 / proxy 2490)→ 非問題 |
| TPC-H validation時 full-cover primary | **#2 digest に委ね #3 を落とす** | deterministic。冗長 filtered_rows 除去がゼロ新機構の初手 |
| TPC-H bounded/secondary/for_each filtered | **hybrid: 小=E / 大=membership(or H)** | 閾値 K = per-scan mem budget / bytes_per_filtered_row(jemalloc 実測、~100-150B/row 想定で K≈8k-40k) |

注: 「単一表現に強制統一」と「SF1→SF10・TPC-C↔TPC-H 最適」は引っ張り合う。TPC-C は range が
小さく enumerate と re-scan が実質収束するので、**cherry-pick 簡素版では K-hybrid を入れず
「TPC-C=E 固定 / TPC-H validation=#2 に集約・#3 撤去」まで**に留めるのが、複雑さを増やさず
最大のメモリ勝ちを取る現実解(K-hybrid は後で必要になってから)。

## 実験計画(E vs H/membership を実測で決める)

configs:
- TPC-C write: 現状 E vs 強制 compact(eligible scan のみ)
- TPC-H SF1/SF10, `read_only_no_validate=ON`
- TPC-H SF1/SF10, `read_only_no_validate=OFF`, 現状 E
- 同上 validation, 大 filtered scan を H/membership

metrics: server peak RSS + jemalloc allocated/resident / TxOccStore entry・filtered 件数・retained bytes /
commit RPC p50/p95/p99(内訳: filtered merge・re-scan/hash・ValidateAndCommit)/ TPC-C tx/s+p99 /
TPC-H query latency・goodput / abort 率・理由 / prefetch・commit wire bytes。

falsifiers:
- **E for TPC-H**: SF10 か中程度並行で RSS が線形に swap/OOM へ → E 棄却。
- **H/membership**: SF10 で commit re-scan が wall time を支配 or 並行 writer 下で false-abort 過多 → 棄却。
- **R(predicate-only, per-row TID 主張)**: exact 等価は既に反証済(上 slip)。membership 版に再定義して使う。
- **E for TPC-C**: retained mem か exact-read 検証が実 p99 ボトルネックに出た時のみ棄却(出ない見込み)。

## 初手(ゼロ新機構・要実測)

full-cover primary filtered scan が validation 時に #2 と #3 を**両方**持っているか計測 →
持っているなら #3(filtered_rows)を drop して #2 に委ねるだけで、md5/22-suite 不変のままメモリ勝ち。
これが「表現を 1 つ減らす」最初の安全な一歩。次に bounded/secondary を membership 版へ。

---

## レビュー後の訂正方針(2026-06-03、学術レビュー 9/10 + pin Codex)

詳細は `phase14_readset_minutes.md`。Claude 5 + Codex 5(L1 残) + pin Codex の収束結論で、
**当初の「R(棄却行を捨てる membership 再評価)が exact で原則的」という見立てを訂正する**。

### 結論: naive R(棄却 TID を捨てる)は罠
- **直列化クラスは conflict でなく view/semantic-serializability**(満場一致)。棄却行の rw-antidependency
  辺を捨てる行為が CSR→VSR の格下げそのもの。
- **3 体が UNSOUND**(別々の実在ホール、各々 strengthening で解消):
  - read-write の rw-antidep(read-only 限定で解消)
  - non-linearizable 再 scan の torn read(commit 再評価を**単一 linearizable snapshot**化で解消)
  - **ABA/tombstone-reuse**(slot 再利用は leaf node-version 不 bump、database_impl.h:471 → tombstone
    TID 保持 or write-delta で解消。**read-only + 純粋述語の封筒内でも残る穴**)
- **R が sound な封筒(全条件 AND)**: read-only ∧ pure/deterministic/row-local 述語 ∧ 真の述語再評価が
  単一 linearizable snapshot ∧ ABA 別途封じ ∧ no LIMIT/ORDER BY/cardinality 集約/secondary/join。
- **ship 済 engine は R 未実装**(route_filtered_row で棄却 TID 保持 + digest が全行 TID 畳む)= **Silo
  等価で sound**。**20x 肥大は soundness の代価**。
- **novelty は INCREMENTAL**(predicate/precision locking + Silo node-set の既知再構成。貢献は
  disaggregation の read-set elision のみ。新しい isolation 定理ではない)。

### 20x 肥大を消す sound な道は 2 つ(表現選択を更新)
| | 内容 | メモリ | 正しさ | 弱点 |
|---|---|---|---|---|
| **(A) digest 圧縮(本命)** | 棄却 TID も hash(= 既存 RangeHash #2) | O(1) | Silo 等価 | SHA-256 collision **2^-256**(確率的・TID の exact とは種類が違う) |
| **(B) 封筒限定 exact R** | 上記封筒を gate 強制 + 真の述語再評価(単一 snapshot) + ABA 保持 | O(1)+α | exact | 封筒が狭い・ABA 対応で retention 部分復活・実装重い |

→ 当初表の **R を「本命の値軸」とした評価を撤回**。**read-only TPC-H 大 filtered scan = (A) digest が
現実解**(既存・Silo 等価)、(B) は封筒厳守可能な限定箇所のみ。**TPC-C(write)= E 据え置き**(R は元々
unsound・小範囲で肥大も無し)。

### TxOccState / pin
- pin は現 raw-pointer 設計で memory safety 必須(Codex 確定)。**logical token 化(key で re-descend、
  生 node_ptr を RPC 跨がせない)で pin・TxOccState を撤去できる** = 「Masstree は機構だけ・lifecycle は
  LineairDB が持つ」方向。digest(A)はワイヤ O(1) なので logical 化と両立する。
