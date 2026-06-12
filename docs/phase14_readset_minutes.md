# 議事録: read-set validation 表現と Silo アーキテクチャ再考(2026-06-03)

> User との対話ログ(意思決定の経緯)。確定結論は `phase14_readset_representation_design.md`、
> 機構の起源は git(Phase-6 commits a595b71/cadf9be)。Codex 相談 = /tmp/codex_readset_repr.md,
> /tmp/codex_silo_arch.md(後者は本議事録時点で実行中)。

## 発端

TPC-C prefetch を auto-gen で 1-RPC 化する話の中で「OCC token(`tx_occ_key`)が CC のブロッカー」
という Codex の懸念を User が疑問視 → 検証したら User が正しく、token は狭い最適化の handle に
すぎないと判明。そこから「そもそもこの機構は何で、どうあるべきか」を first principles で掘った。

## Q&A の流れ(確定した理解)

### Q1. `tx_occ_key` の初出は? 俺が書いた?
- **A: User 自身の Phase-6 コミット**(a595b71 server / cadf9be proxy, 2026-05-29, author Noxy3301)。
  origin/main には未到達(feature branch のみ)。設計記録 docs/phase6_rangehash_occ_design.md。
- Codex はこのセッションで「scalar token の multi-RPC 上書き」を一般ブロッカーと**過大評価**。
  実際は point read では token=0/無関係(doc 明記)。

### Q2. 名前が悪い(`tx_occ_key`)。read_set_hash では?
- 実体は `uint64_t` のハンドル(server の単調カウンタ `next_tx_occ_key`)で、`GlobalTxOccStore`
  の lookup key。**hash ではなく、server 退避した read-set を引く番号**。
- → `read_set_hash` は誤読を招く(proxy は hash すら持たない)。より正確には
  `retained_read_set_handle` 等。ただし後述の通り「そもそも server 保持を無くす」方向に進むので
  rename 自体が moot になりうる。

### Q3. 「retained」「用途」が分からない
- コインロッカー比喩。OCC commit は「読んだ行が変わってないか」を全部再検証する必要があり、
  6M 行 scan だと read-set が 6M。ナイーブだと server→proxy→server と 6M を往復(Phase-5 で
  commit 一律 7.5-8s の主因)。retention = server が読んだ read-set を手元に置き、proxy には
  ロッカー番号(tx_occ_key)だけ渡す → ワイヤを流れるのは番号 1 個。**転送削減の最適化**。

### Q4. server が 6M を commit まで握る? メモリ大変では? 範囲表現で再検索した方が?
- 半分当たり。実装は 3 表現: node_version(構造 O(leaves), ワイヤ)/ RangeHash(値 digest
  O(1), read-only full-cover 限定)/ filtered_rows(値 enumerate O(rows), 残り全部=肥大本体)。
- 「範囲で表現して commit で再検索」= User の re-scan 案。digest の collision NO-GO を exact
  再評価で回避するルート。**実測 20x 肥大(filtered 5.9M @ SF1)= 既知の主犯**に直撃。

### Q5. 棄却行(filtered_rows)とは? なぜ棄却したのに持つ?
- 「棄却」= クエリ**結果**から落とした、だが predicate 評価のため**値は読んだ**行 → OCC 的に
  read-set メンバー。例 `WHERE o_total>1000` で 500 の行は「結果に入れない」判断のため読んだ。
- 持つ理由: その行が後で 2000 に変われば結果に入るべき → 検知しないと非直列化。
- だが**個々の棄却行 TID 変化は membership を跨ぐ時だけ意味を持つ** → commit で再 scan+再 filter
  すれば membership 変化は捕まる → 全棄却行 TID を抱える必要なし(= 捨てられる)。

### Q6. pushdown で取った範囲を「PK で直接拾ったのと等価」にすれば? → phantom?
- 正しく phantom。read-set の単位は「マッチ行」でなく「舐めた範囲」。点に畳むと (a) 新行 INSERT
  (phantom)と (b) 既存棄却行の境界跨ぎ UPDATE を両方取りこぼす。
- Helios は: 構造変化(insert/delete)= node_version(leaf version bump)、in-place 値更新
  (構造不変・TID だけ bump)= 値軸、で直交に守る。**範囲(node_version)は捨てられないが、
  値軸の "全棄却行 enumerate" だけは捨てられる**。

### Q7. R(re-scan)が分からない、もう少し
- prefetch では「範囲+述語」と「受理キー A_orig(既に reads にある)」だけ保存、棄却行はゼロ。
  commit で範囲 re-scan→述語再適用→A_now を作り A_orig と比較。境界跨ぎ/phantom/受理行削除=
  membership 不一致で検知、受理行の値変化= reads の TID 検証で検知。**exact・衝突無・O(1)**。
  代償 = 述語を server に運ぶ配線 + commit の re-scan CPU。

### Q8. TxOccState が美しくない。prefetch の美点は proxy が read/write-set を持つこと。
###     TID と re-scan を別軸で見てるから混乱してる気がする。Silo を読み直して。
- **一次資料で確定**:
  - LineairDB Silo CC(silo_nwr.hpp)の検証集合は 2 つ = `read_set_`(per-record TID)+ deferred
    phantom(node-version)。**Silo は commit で re-scan しない**(保存値を照合するだけ)。
  - → **User の指摘が正しい**: 真の軸は {値=read_set_, 構造=node-version} の 2 つ。
    E/H/R は**値軸の 3 実装**であって re-scan は軸ではない。
  - server 保持が紛れた理由(技術的強制、美意識でない): (1) node-version が masstree 生ポインタ
    を指し RCU が 2-RPC 間に free → epoch pin が必要。ただし**現状実装は node-version を proxy に
    ship+echo 済**で、TxOccState は「epoch pin」と「filtered_rows」に縮小。(2) pushdown で棄却行
    が proxy に届かない → filtered_rows を server 保持。**これが唯一残る "美しくない" server 保持**。
  - あるべき形(設計ノート range_digest_validation_design.md 既出の出口): 軸1 を proxy 保持の
    per-range digest に畳む → filtered_rows も result_keys も廃止 → **TxOccState から軸1 が消える**。
    残る epoch pin も node-version を logical 形にすれば消せる(別作業)。
    → **simplify(表現減)== optimize(肥大消)== proxy-holds-set(Silo 美意識)が揃う**。

### Q9. 「128bit digest は 64bit TID より強い」は違う。TID は atomic 採番だから衝突概念と別物。
- **User 正しい。訂正。** TID は atomic 採番で書込みごとに決定的に変わる → observed vs current の
  比較は**厳密・非確率的**(64bit は wraparound 回避の幅で、衝突耐性の話ではない)。digest equality
  は **2^-128 で取りこぼす確率的検査**。よって H は **Silo に無い確率的健全性仮定を持ち込む**。
  「digest > TID」は種類の違うものの比較で誤り。
- 含意: exact を保ったまま filtered_rows を消すなら **H(digest)でなく R(exact 再導出)が原則的に
  正しい**。R は集合比較+per-record TID で hash 無し・確率無し。
  → Codex に再確認依頼中(/tmp/codex_silo_arch.md): この訂正の是非、R の健全性の穴(述語決定性/
  re-scan snapshot 一貫性/phantom 取りこぼし)、TxOccState 全廃(epoch pin の logical 化)可否、
  commit CPU は E も R/H も O(rows)(救うのは状態/転送/メモリ)の確認、ワークロード別終着形。

## 暫定の方向性(Codex 確認待ち)

1. **値軸を proxy 保持に戻し TxOccState.filtered_rows を撤去**。表現としては:
   - exact を優先 → **R(membership 再導出)**。digest H は確率的仮定を入れるので原則回避。
   - TPC-C(小 range)は E 据え置きが妥当(re-scan の commit latency が割に合わない見込み)。
2. **初手(ゼロ新機構・要実測)**: full-cover primary filtered scan が RangeHash と filtered_rows
   を冗長に両方持つか計測 → 冗長なら filtered_rows drop で md5 不変のままメモリ勝ち。
3. **最終形**: Silo 2 軸 on proxy(値=R or 小さい digest を proxy 保持 / 構造=logical node-version)、
   TxOccState 全廃、epoch pin も logical 化で除去。cherry-pick で main にはこの簡素形を載せる。

## Codex verdict(2026-06-03, /tmp/codex_silo_arch_out.txt)

**User の訂正を支持。** 加えてコード自身が裏付け: 現 digest は SHA-256 で、`sha256.hh:1` が
「NOT a hard mathematical guarantee」と明記。TID 比較は exact(silo_nwr.hpp:91/137/321 で観測
TransactionId を commit で equality 比較)。digest は確率的 = **種類が違う**。

- **ファイル訂正(我々の誤り①)**: この fork の Silo exact 検証ループは `read_set_` ではなく
  **`validation_set_`**。`ReadDirect` は full read_set_ snapshot なしで validation_set_ に登録。
- **Q1 H の確率性**: 2^-128/2^-256 は checksum/content-addressing/破損検知には実用的だが、
  **serializable 検証は通常 exact 規定**。proof-level の正しさを求めるなら **E か R**。H を残すなら
  「probabilistic serializability / read-mostly 方針」と**明記**せよ(Silo-equivalent と呼ぶな)。
- **Q2 R は exact 化可能、ただし eligibility 条件付き**:
  - 述語は prefetch で実際に使った **serialized `PushedPredicate`** を使う(SQL/session 再解釈は不可)。
    server evaluator は row bytes+FilterExpr 上で決定的(predicate_evaluator.cc:258)。MySQL 側
    serializer が**非決定/session 依存式を R 前に弾く**必要。proto に焼いた定数は OK。
  - **phase**: read-only は validation が serialization point で可。**read-write は re-scan を
    `ValidateAndCommit` 内(write lock 後・install 前)+ own-write normalization** が必須
    (phase6 doc:64 が outside-phase re-scan は RW で誤りと既述)。
  - torn-row は既存の even-TID double-check 規律(database_impl.h:447/568)で無し。ABA は既存
    Silo/TID-wrap と同クラスのみ。
  - R は「全 scan 行 abort」の classic Silo とは**非等価**: 棄却→棄却は abort 不要 = exact predicate
    validation。SQL 述語読みには健全で E より非保守的(= membership 反論が正式に裏取りされた)。
- **Q3 TxOccState 全廃は可能、だが今の raw physical token では不可**: 現状 owner_ptr/node_ptr/version
  を ship し ValidateAndCommit が dereference(database_impl.h:1315)→ pin は memory safety で必須
  (lineairdb_rpc.cc:1937)。pointer-free にするには commit token を **logical**(range 記述子+述語+
  accepted keys/TID、or fence/boundary key 付き安定 leaf 記述子+version)に。「key で現 leaf を引いて
  version 比較」だけでは**不足**(split/merge が必ず mismatch→abort になる証明が要る)。代償 = 追加
  tree descent or commit re-scan。
- **Q4 commit CPU**: 確認。E=O(rows) random point lookup / R/H=O(rows) sequential re-scan。
  **R/H が救うのは state/wire/random-lookup コストで、O(rows) commit work は消えない**。
  → TPC-C は **E 据え置き**。R/H は大 filtered scan 限定、R が exact target、H は確率許容時のみ。
- **Q5 終着形**: TPC-C/write=E + physical node-version phantom(range hash 無し、R 使うなら
  ValidateAndCommit 内)。TPC-H filtered=**R exact 再導出が原則的 target**(proxy が range 記述子+
  serialized 述語+accepted key/TID obligation、server が membership 再 scan、受理行 TID は exact)。
  read-only bench mode のみ H/SHA-256 を「probabilistic と明記の上」staged 最適化として許容。

**我々の誤り②(初手の安全性)**: 「full-cover で filtered_rows を drop」は **`use_range_hash` で
グローバルに skip してはいけない**。`TxOccState.filtered_rows` は **range identity を持たない**ため、
bounded/secondary/for_each の行に対して unsafe。**「その step を range hash が provably cover する時だけ」
step 単位で drop** すること。

**我々の誤り③(R の前提)**: 「accepted keys は既に reads にある」は通常 validating path では真だが、
**現状の read-only range-hash full-cover scan は per-row TID 記録を意図的に skip している**
(lineairdb_transaction.cc:817)。H を R に置換するなら、この例外を**外すか R で exact に cover** する必要。

## 確定した方向性

- **値軸の原則 target = R(exact 再導出)**。H(digest)は「probabilistic と明記」した read-only 限定
  staged 最適化に格下げ。E は TPC-C/write・小範囲で据え置き。
- **TxOccState 全廃は最終目標だが、まず raw physical token を logical token 化が前提**(pin 除去の道)。
- 段階パス(各段 md5 22-suite + 並行テストで gate):
  1. **stale コメント修正**(proto/TxOccState コメントが「range を server 保持」と書くが実際は
     range_versions を ship・filtered/hash/pin を保持)。
  2. **ゼロ新機構の初手**: filtered_rows drop は「その step を retained range hash が provably cover」
     な時のみ(use_range_hash でグローバル skip しない)。range identity を付ける所から。
  3. primary full-cover filtered scan に **exact R path** を gate 付きで追加。
  4. read-write の R は own-write normalization テスト通過後に `ValidateAndCommit` 内へ。
  5. 並行テスト: 棄却→受理 flip / 受理行値更新 / tombstone reinsert / LIMIT 境界前 insert /
     自 tx の範囲内 insert・delete / secondary-slot rewrite。

## 追加で確定した事実(2026-06-03、コード一次確認)

### #1 Silo の検証セットはポインタベース(key でない)
`silo_nwr.hpp:42` `struct ValidationItem { const DataItem* item_p_cache; TransactionId transaction_id; }`。
commit の `AntiDependencyValidation`(:322)は **item_p_cache を直接 deref して現 TID を load・比較**、
key を使わない。→ 「leaf ptr, tid」は Silo 本来の設計(read-set = record-pointer+observed-TID)。
「(key,tid)」は disaggregation 用の論理表現(ネット越しにポインタを送れないから key で引き直す)。
native=ポインタ / cross-RPC=key の違い。

### #3 threading: pool でなく「1 接続 = 専用 1 thread」、pin 必要性に疑義
- `tcp_server.cc:98` で接続ごとに専用 `std::thread`(recycle pool ではない)。`handle_client`
  (`lineairdb_server.cc:22`)が while ループで同じ thread が prefetch→commit を処理。
- `release_masstree_thread_epoch`(rcu_stop)8 箇所は `handle_rpc`(非ストリーム)側。
  streamed prefetch handler(1868)は pin 登録(1936)はするが release lambda を通らない。
- **未決点**: 専用 thread が prefetch↔commit 間で本当に offline(gc_epoch_ 手放し)になるのか。
  ならないなら thread 自身の gc_epoch_ が leaf を生かす → **pin 不要かも**。User の「offline に
  なる余地がない(pool 不使用)」は概ね当たり。pin の真の利点は TTL/接続 crash cleanup であって
  liveness 自体ではない可能性。→ Codex 集中レビュー(bpel00h43)で a/b/c 判定中:
  (a) pin 必須 / (b) 「thread を online 維持 + commit で慎重に再 stamp」で代替可 / (c) logical
  node-version 化(key で re-descend、生 node_ptr を送らない)で完全除去可。

## 学術レビュー(R = 最適 predicate 再検証 は Silo の直列化性質と等価か)— 進行中

User 指示: 「これが Silo の Validation protocol で成し得たい性質と厳密に等価なら major
contribution レベル。Claude と Codex のサブエージェントを 5 つずつ立てて収束を見たい」。

検証命題(/tmp/r_review_core.md): R = commit 時に (a)受理行を exact TID 検証 + (b)node-set 検証 +
(c)同 predicate を同 range で再評価し membership A_now==A を確認、棄却行は保持も TID 検証もしない。
**CLAIM: R は sound(非直列化を commit しない)。棄却行の per-row TID 検証は SQL 述語読みでは冗長
(余分な abort を生むだけ)**。7 問(soundness 反例 / Silo との関係 / 直列化クラス conflict vs view /
phantom・境界の完全性 / 述語決定性 / secondary・LIMIT・集約への拡張 / 新規性 prior-art)。

立てたレビュア(各全問回答+担当レンズ深掘り+構造化 VERDICT):
- Claude: L1 反例ハント a65b3c2 / L2 isolation theory ae022dd / L3 phantom 完全性 a34999c /
  L4 述語決定性+拡張 a448606 / L5 新規性 a7818cf
- Codex: L1 bli8vuy2e / L2 beohq4p2y / L3 bd2w2p0ys / L4 bmew1xifn / L5 blj00k8t9
収束(10 個が同じ SOUND/UNSOUND・novelty 判定に至るか)と、edge case の和集合を synthesis 予定。

## 学術レビュー収束 — Claude 5/5 + pin Codex 確定(2026-06-03、Codex R-5 体は待ち)

### Claude 5 体の VERDICT
| | SOUNDNESS | CLASS | NOVELTY | top risk |
|---|---|---|---|---|
| L1 反例 | **UNSOUND**(read-write 反例) | sub-VIEW | KNOWN | rejected 行の rw-antidep(T_A が書く時) |
| L2 isolation | SOUND_W_COND | VIEW | INCREMENTAL | (c) は真の述語再評価必須(keyspace 比較では不可) |
| L3 phantom | SOUND_W_COND | VIEW | INCREMENTAL | **ABA/tombstone-reuse**(前提下でも穴) |
| L4 determinism | SOUND_W_COND | CONFLICT(predicate model) | INCREMENTAL | collation/comparator 安定性 |
| L5 novelty | SOUND_W_COND | VIEW | INCREMENTAL | engine は R 未実装・(c) fidelity |

### 収束(矛盾なく一致)
- **全員「R は無条件には sound でない」。** L1 の UNSOUND は read-write を含む一般評価、L2-L5 の
  SOUND_W_COND の条件群は **L1 の反例ケースを丁度除外**(read-only + row-local + ABA 無 + LIMIT/集約/SI/join 無)。
- **R が sound な封筒(全条件の積):** (1)**read-only**(rejected が出力に寄与しない)/(2)P が
  deterministic・pure・**row-local**(NOW/RAND/session/相関サブクエリ/UDF 不可)/(3)**(c) は真の述語
  再評価**(keyspace=result_keys 比較では in-place rejected→accepted を取りこぼす)/(4)単一表・GROUP BY
  /cardinality 集約・ORDER BY+LIMIT・secondary index 無し/(5)read と commit で comparator/collation 同一。
- **封筒内でも残る 2 穴**: (A) **ABA/tombstone-reuse** — slot 再利用は leaf node-version を bump
  しない(database_impl.h:471)ので、同じ述語答え+同じ TID に戻る再利用を (a)(b)(c) 全部が盲。
  engine は tombstone TID を明示保持して塞ぐ(R はそれを捨てる)。(B) (c) を keyspace 比較で実装すると
  case iii 漏れ。→ R は「server で述語を当て直す再 scan」必須。
- **直列化クラス**: 標準 tuple model では **VIEW**(read-only 封筒内)、read-write/条件外では view 未満。
  L4 の CONFLICT は predicate-lock model での言い換え(同じ保証の別モデル記述)。
- **novelty**: ほぼ満場一致 **INCREMENTAL/KNOWN**。CC プリミティブ = optimistic predicate/precision
  locking(Eswaran 1976 / Jordan 1981)+ Silo node-set + Hekaton commit 再検証。新規は **disaggregation
  の read-set 圧縮/elision(systems)だけ**。新しい isolation 定理ではない。
- **決定的な満場一致の発見**: **ship 済 engine は R を実装していない。** route_filtered_row で rejected
  (key,tid) を保持 + footprint digest が全行 TID(rejected 含む)を畳む → **Silo 等価で sound**。
  **20x 肥大は soundness の代償**。「rejected を捨てる」緩和は未採用で、それが危険部分。

### 設計への含意(方針の更新)
- **naive R(rejected を捨てる)は罠**: read-write で unsound、read-only でも ABA + (c)-fidelity +
  predicate-determinism + no-LIMIT/集約/SI の狭い封筒。前回「R(exact)が H より原則的」は **過度に楽観**だった。
- **20x 肥大を消す sound な手段は 2 つ**:
  - **(A) digest(rejected TID も畳む RangeHash)= 圧縮**。ship 済・Silo 等価・O(1) wire/state。代償は
    SHA-256 collision 2^-256(前回 Codex が指摘した確率的仮定。TID の exact と種類が違う)。
  - **(B) 封筒限定の exact R**。read-only TPC-H・row-local 述語・no LIMIT/集約/SI・**true 述語再評価**
    + **ABA を tombstone TID 保持で別途封じる**。exact だが封筒が狭く ABA 対応で部分的に retention 復活。
- **実務の落とし所**: TPC-C(write)= **E 据え置き**(小範囲・R は元々 unsound)。TPC-H(read-only 大 filtered
  scan、肥大が痛い)= **(A) digest が現実解**(既存・sound modulo 2^-256)、(B) は封筒厳守できる限定箇所のみ。

### #3 pin 必要性(Codex 確定)
- streamed prefetch handler は **応答送出前に明示的に rcu_stop**(lineairdb_rpc.cc:1975, gc_epoch_=0)→
  専用 thread は commit 待ちの間 **RCU-offline**。→ **User の「offline にならない」は現コードでは外れ**
  (明示 stop する)。ただし「online 維持できる」着想は妥当。
- verdict: (a) 現 raw-pointer 設計では **pin は memory safety に必須**。(b) thread を online 維持で代替可
  だが reclamation stall は同じ + TTL sweep できず fd-affinity 不変条件が要る(劣る)。(c) **logical token
  化(key で re-descend、生 node_ptr を RPC 跨ぎで送らない)で pin 完全除去**。既存 logical 経路が前例。
- → User の「Masstree は機構だけ・lifecycle は LineairDB」= (c)。pin/TxOccState 除去の正道。

### Codex R 側 VERDICT(L1 のみ実行残・9/10 確定)
| | SOUNDNESS | CLASS | NOVELTY | 指摘 |
|---|---|---|---|---|
| Codex L2 | SOUND_W_COND | semantic-view | INCREMENTAL | rejected を irrelevant 扱いするのは plan/UDF/subquery/order/agg/session 次第で破れる |
| Codex L3 | **UNSOUND** | OTHER(非 conflict) | INCREMENTAL | **commit 再 scan が non-linearizable**(torn read)+ in-place 無 bump。**単一 linearizable snapshot にすれば反例消滅** |
| Codex L4 | SOUND_W_COND | semantic-view(非 conflict) | INCREMENTAL | pushed per-table 必要条件 filter / 非決定式を exact 述語答えと誤認 |
| Codex L5 | **UNSOUND** | OTHER(非 conflict) | INCREMENTAL | **predicate ABA histories**。final-answer equality では ABA を見逃す。単一 certification snapshot or write-delta 検証が要る |

### 最終収束(9/10 + pin、満場一致レベル)
- **soundness: 3 UNSOUND(Claude L1 / Codex L3 / Codex L5)+ 6 SOUND_W_COND。矛盾なし** — 3 つの UNSOUND は
  各々別の穴を突き、いずれも「特定の strengthening で sound 化」と明言:
  - L1 = **read-write の rw-antidep** → read-only 限定で解消
  - Codex L3 = **non-linearizable 再 scan(torn read)** → commit 再評価を**単一 linearizable snapshot**化で解消
  - Codex L5 = **ABA** → 単一 certification snapshot or write-delta 検証(= tombstone/ABA 保持)で解消
  → naive R は unsound、下記条件を**全部**足して初めて sound、という同一境界を別表現しているだけ。
- **R が sound な条件(全 9 体の積、確定版):**
  1. **read-only**(rejected が txn の write/出力に寄与しない)
  2. P が **pure・deterministic・row-local**(NOW/RAND/session/相関サブクエリ/UDF/collation 不一致は不可)
  3. commit 再評価は **真の述語再評価 かつ 単一 linearizable snapshot read**(keyspace 比較や torn 多段 re-scan は不可)
  4. **ABA/tombstone-reuse を別途封じる**(tombstone TID 保持 or write-delta)
  5. **no LIMIT/ORDER BY・no cardinality 集約・no secondary index・single-table(no join/semijoin)**
- **CLASS: 非 conflict-serializable。** view/semantic-serializability(論理述語読み上)が上限。満場一致。
- **NOVELTY: 満場一致 INCREMENTAL。** 理論は predicate/precision locking + Kung-Robinson OCC + Silo
  node-set + SSI の既知再構成。貢献は disaggregation の read-set elision(systems)のみ。
- **ship 済 engine は R 未実装(rejected TID 保持)= Silo 等価で sound。20x 肥大は soundness の代価。**(全員独立確認)

## アクション

- [x] 学術レビュー **10/10** + pin → 最終収束確定。Codex L1 = SOUND_W_COND / semantic-view /
      INCREMENTAL(conf 86)。最終カウント: **3 UNSOUND(Claude L1, Codex L3, Codex L5)/ 7 SOUND_W_COND**、
      全て同一境界の別表現。CLASS 満場一致「非 conflict、view/semantic が上限」。NOVELTY 満場一致 INCREMENTAL。
      prior art 確定リスト: predicate locking(Eswaran 1976)/ precision locking(Jordan 1981)/
      Kung-Robinson OCC(1981)/ Silo node-set(Tu 2013)/ SSI(Cahill-Fekete 2008)/
      **Fast Serializable MVCC(Neumann-Mühlbauer-Kemper SIGMOD 2015, intensional-predicate vs
      extensional-write validation = 最近接)**。貢献は disaggregation read-set elision のみ。
- [x] 設計方針更新を design doc に反映(phase14_readset_representation_design.md 末尾「レビュー後の訂正方針」)。
- [ ] (実装着手時)封筒限定 exact R は「read-only + linearizable snapshot 再評価 + ABA 保持 + 純粋述語 +
      no LIMIT/集約/SI/join」を gate で強制。それ以外は digest 圧縮 or E。
- [x] Claude 5/5 + pin Codex / Codex の Silo アーキ確認(b03ijugul)。

## MAGI 検証(digest 方向の独立 3 体査読、2026-06-03)

User 指示「digest で問題ないか Codex 3 体独立に(MAGI 風)」。命題 = digest(全 footprint を
32B root に圧縮、commit で key 再scan して照合)が (1)CSR を保つ (2)構造=range記述子+root で
node-version 不要 (3)pin/TxOccState 撤去可 (4)衝突は 2^-256 を明示前提化で可、の 4 claim。

| | Melchior(理論) | Balthasar(運用) | Casper(敵対) |
|---|---|---|---|
| C1 CSR保持 | AGREE_W_CAV | AGREE_W_CAV | AGREE_W_CAV |
| C2 構造 | AGREE_W_CAV | AGREE_W_CAV | AGREE_W_CAV |
| C3 pin撤去 | AGREE_W_CAV | AGREE_W_CAV | AGREE_W_CAV |
| C4 衝突 | AGREE_W_CAV | AGREE_W_CAV | AGREE_W_CAV |
| OVERALL | **SOUND_W_COND** | **SOUND_W_COND** | **SOUND_W_COND** |
| conf | 86 | 86 | 86 |

**3/3 満場一致で条件付可決。DISAGREE/NOT_SOUND ゼロ。敵対役 Casper も陥落せず。** digest 方向は
通る(4 claim いずれも本質的に正しい)。衝突数学も Melchior が確認: per-check 2^-256、q 回で
q/2^256(union bound、無視可)、chosen-collision は 2^128-work の別脅威モデル。

### 実装契約(caveat 和集合 — digest を正しくやるための条件)
1. **prefetch digest は「元 scan が観測した (key,TID,found) そのもの」から計算**せよ。別途 post-scan
   re-walk で作ると実際に読んだ値と違う footprint を検証しうる(Melchior の biggest hole。現リポジトリは
   second scan で root を作る=要修正点)。
2. **commit 再scan は linearizable**(単一 snapshot)。
3. **カバレッジ不変条件**: per-row TID を skip したら必ず対応 digest が捕捉+検証されたことを保証、
   skip したのに digest 無し/空なら **abort**(Casper の biggest hole=安全インターロック)。
4. **point read + 非 hash range は exact TID 維持**(digest は read-set の一部であって全部でない)。
5. **secondary index** は richer descriptor(secondary slot + base-row TID の digest)or 既存 exact 維持。
6. **read-write は locked-phase own-write 正規化が必須**、現状の安全 scope は **read-only**。
7. **衝突**: 2^-256/check を明示前提として文書化 + hygiene(256bit/domain sep/count bind/暗号hash)。
   敵対的 chosen-collision(2^128-work)は別脅威モデルとして別記。
8. **pin/TxOccState 撤去は実リファクタ**(生 node_ptr 検証を実際に消す + digest token を protocol で運ぶ)。
   現状はまだ commit で物理 node_ptr range_reads を検証し RangeHash を TxOccState に格納している(Balthasar)。

→ **結論: digest = CSR 維持・メモリ O(1)・pin 撤去を一手で達成する正道。GO。上記 8 条件が実装契約。**

## main は filtered scan で VSR 落ちしている(2026-06-04、コード確証)

User 仮説「pushdown × serializable の併用例が少なく、必要情報が落ちて VSR 化していそう。main で確認」
→ **当たり。** main(production)は predicate pushdown 導入時から filtered scan が sub-CSR(VSR)。

### main が実際にやっていること(origin/main 直接確認)
- predicate を **server 側で適用し棄却行を完全 skip**(`server/rpc/lineairdb_rpc.cc:1139-1140`
  `if (!evaluator.evaluate(*filter_expr)) return false; // skip row`)。key/value/TID を一切残さない。
- `result_keys` = **マッチ行のみ**(`proxy:1124`)。マッチ行は `row_tids` で TID 検証。
- **`RangeValidationEntry` proto(proto/lineairdb.proto:73-85)に filter フィールド無し** → commit で
  **predicate を当て直せない**。範囲検証は node-version(構造 phantom)+ result_keys。
- **`route_filtered_row`/`filtered_keys` は main に存在しない**(棄却行 TID 運搬機構ゼロ)。

### 具体的な穴: in-place rejected→accepted を検出できない
範囲内の既存行が in-place 更新で述語を満たすようになる(棄却→受理)場合、4 経路すべて盲:
- node-version: in-place 値更新で bump しない(finish(0))→ 見えない
- row_tids: 棄却で ship されてない → 無い
- result_keys: 元々マッチでない → 無い
- commit の predicate 再評価: proto に filter 無し → しない
→ **結果に入るべき行を取りこぼす = phantom 系の直列化違反。** rejected→rejected の値変化も同様に盲。

### 判定
- **main は filtered scan で CSR を割っている(sub-CSR / VSR)。** Silo が abort するスケジュールを通す。
- **read-only**: たまたま view-serializable に留まる(flip が読後なら自分を writer の前に順序付け可)→
  TPC-H read-only 結果は実害が出にくい。
- **read-write(HTAP/混在)**: 棄却行の rw 辺 + 自分の write で閉路 → **真に非直列化**。実エクスポージャ。

### 全部が繋がる(重要な気づき)
- **main** = predicate pushdown あり・棄却行検証なし → 安いが unsound(read-write)
- **feature branch Phase-6**(`a595b71` physical OCC + TxOccStore + range-hash footprint + route_filtered_row)
  = 棄却行検証を**追加** → CSR 回復、ただし 20x 肥大
- → **20x 肥大は main がサボっていた soundness の代金。digest はそれを O(1) で取り戻す手段。**
- **projection pushdown は無罪**(検証はレコード TID=行全体の版、列間引きで版情報は失われない。digest も
  (key,TID,found) を hash し値バイトは hash しない)。**問題は predicate pushdown だけ。**

### 注
LineairDB submodule core(main pin 版)は gitlink で直接 grep 不可だが、proto に filter 無し・棄却行 skip・
node-version は構造のみ・route_filtered_row 不在、で「commit が棄却行の値変化を検出する経路が無い」は確定的。

### 最小再現テスト(predicate write-skew) `/tmp/rejected_writeskew_v2.sh`
2 txn が各々「k=1 の行は無い」(両既存行 k=0=棄却)と読み、各々別行に k=1 を立てる。conflict graph に
閉路 → 非直列化。CSR なら 1 つ abort(COUNT(k=1)=1)、棄却行を捨てると両 commit(=2, anomaly)。
- **oneshot ON では ad-hoc RW txn 自体が deadlock**(既知制約。prefetch 経路は対話的 RW を捌けない)→
  oneshot OFF の per-op 経路で実施。
- **feature branch 実測: T2 が COMMIT で abort(ERROR 1180)、COUNT(k=1)=1 → 書き込みスキューを正しく阻止。**
  棄却行検証が end-to-end で効いている実証(= #4「破損なし」検証も兼ねる)。
- **main なら =2(両 commit=anomaly)になるはず**(棄却行検証不在)。実機確認には main ビルドが要る(未実施)。

### TPC-C の validation 数と pushdown 削減(構造的見立て)
**TPC-C で predicate pushdown による棄却行 drop はほぼ起きない。** 理由: pushdown が棄却行を落とすのは
「**非索引列述語を server 側で適用する RANGE scan**」のときだけ。TPC-C の読みは大半が **point read**
(fetch 済=filter で落ちても read-set に入る)か **索引レンジ**(order_line by o_id 等、非索引述語なし)。
StockLevel の `s_quantity < ?` も stock を s_i_id=PK で point 取得した上での filter なので range-reject でない。
→ **TPC-C の validation item は point read 主体で全部検証される。VSR gap は実質 TPC-H(大 filtered scan)の問題。**
(精密計測は HELIOS_TIMEPROF + filtered_validate trace で short TPC-C を回せば取れる。期待 filtered≈0。)

### ★ 実機検証(main ビルド)で前言を訂正(2026-06-04)
User「experimental branch でたまたま動いてるだけかもしれないので main で実機確認したい」→ main を
in-place overlay + build_partial でビルドし、別 instance で write-skew テストを実行。

**結果: main でも k1=1(T2 が abort)= feature と同じ。anomaly は出なかった。** さらに control
(rejected→rejected の値変化)でも main の per-op で T1 が abort → **per-op 経路は棄却行も含む full
read-set を保持し CSR で正しい(むしろ過剰 abort)**。

**コードで理由判明**: `ha_lineairdb.cc:1462` `if (!tx->is_oneshot_mode()) return;` →
**predicate pushdown は oneshot 専用に gate**。per-op(oneshot OFF)は述語を push せず server は全行返却
→ MySQL 後段 filter → proxy read-set に全行(棄却含む)→ 全検証 → **穴なし**。

**従って前言「main は filtered read-write で VSR」は経路を限定せず言いすぎ。正しくは:**
- **per-op 経路 = 穴なし(両ブランチ)。** ad-hoc/対話 SQL はこの経路。
- **oneshot 経路のみ main に穴**(述語 push で棄却 drop、route_filtered_row 無し)。ただし **oneshot は
  ad-hoc RW で deadlock し対話的に踏めない**。踏むには benchbase 風の read-write explicit-plan +
  filtered range scan が要る(TPC-C は point-read 主体でほぼ該当せず)。
- **実害エクスポージャは小さい**: oneshot × read-write × 非索引述語の range scan、という稀な交差点のみ。
  read-only TPC-H(oneshot)は view-serializable で無害、TPC-C(point-read 主体)はほぼ無傷。

**教訓**: コード解析(proto に filter 無し・棄却 skip)から「main は VSR」と confident に言ったが、
**経路 gate(oneshot 限定 pushdown)を見落としていた。実機が解析を正した。** User の live 検証要求が正しかった。
(in-code コメント `lineairdb_transaction.cc:535-543` "Logical range key-list validation alone misses
such value-only changes (P1 closed)" は oneshot 経路の話で、解析自体は正しいが**適用範囲が oneshot 限定**。)
- [ ] 段階1: stale コメント修正(低リスク・先行可)。
- [ ] 段階2: filtered_rows に range identity を付与 → provably-covered step だけ drop(要実測)。
- [ ] full-cover redundancy + per-step cover 可否の実測ハーネス。
- [ ] (後段)R path 実装、read-only→read-write の順。lineairdb_transaction.cc:817 の per-row TID
      skip 例外の扱いを R 設計に織り込む。
