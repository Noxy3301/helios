# Phase-7 alloc-churn 削減(perf 起点)

perf(Q21 SF=1, mysqld)で alloc/memcpy が self 48.7% = 最大ボトルネックと判明。
hot: make_mysql_table_row 7.4% / lookup_local_read_set 6.98% / record_stateless_read 5.61% /
activate_range_validation 3.80%。いずれも per-row(数百万回)の std::string キー構築 + unordered_map 操作。
SOTA 調査(web)+ Codex で施策をランク付け。ankerl::unordered_dense が MySQL に vendored 済(C++17 で
透過 lookup 可)。順に: #1 scratch-key + const* / #5 row.reserve / #2 ankerl maps。
(#6 borrow→index_read は Q21 の FE 点読みが range-cover されない純点読みのため Q21 効果ゼロ → skip。)

## #1 reusable scratch-key + const* 返し — 2026-05-29 → 採用(小勝ち)
lookup_local_read_set: make_local_read_key の per-call string 確保を member scratch(lrk_scratch_)再利用に、
返り値を std::optional<LocalRowEntry>(値コピー) → const LocalRowEntry*(コピー無し)へ。drop_local_read も
scratch。record_stateless_read の dedup find も scratch(srr_scratch_)、新規キーのみ map へ copy。
呼び出し側は if(auto e=...)/e->/*e が optional/pointer 共通で無改修。
測定(SF=1, 22/22 md5 OK): Q21 20367ms→19940ms(~-2%)/ mysqld 4.19GB 同。alloc は jemalloc 小確保が
速く(~50ns×6M≈0.3s)効果は限定。lookup の self の大半は hash+探索+値コピーで、本丸は #2 ankerl。
Codex GO(pointer lifetime/ scratch aliasing / dedup 意味論 / THD-scoped thread-safety 確認、findings なし)。

## #5 make_mysql_table_row: variant→inline LE decode — 2026-05-29 → 採用(小勝ち)
convert_bytes_to_numeric は std::variant をバイト毎に std::visit(Q21 SF=1 で ~192M 回)。
make_mysql_table_row は常に const std::byte* なので variant 無しの decode_le_bytes に置換(他 caller 用の
元関数は残す)。row vector は member+clear で容量保持済のため reserve は不要(Codex 指摘)。
測定(22/22 md5 OK): q1 20060→19299, q7 15792→14956, q18 18116→17961(各~0.7s)、Q21 はノイズ範囲。
Codex GO(decode 等価、4byte 高位ビットの旧 int-promotion UB をむしろ修正)。

## #2 ankerl::unordered_dense maps — 2026-05-29 → 採用(本丸・大勝ち)
std::unordered_map(per-node alloc + std::hash)を ankerl::unordered_dense(MySQL vendored 4.4.0、
wyhash + open addressing + node alloc 無し)に置換。extra::unordered_dense(INTERFACE target)を
proxy/CMakeLists.txt の plugin link に追加。
- local_read_set_ → segmented_map(INSERT で reference 安定 → #1 の const* を保つ)
- stateless_read_index_ → flat map(値 size_t、ポインタ非返却で flat 安全)
全 call site は find/erase/operator[]/empty のみ(std 互換)。
測定(SF=1, 22/22 md5 OK): Q21 20476ms→17428ms(-15%)/ mysqld 4.19→4.11GB。広範改善:
q4 -45%, q13 -26%, q5/q9/q18/q22 等も低下(per-row map 操作が全クエリで軽量化)。
Codex GO(reference 安定性 / flat 安全 / operator[]=try_emplace 互換 / NUL 全長 hash / INTERFACE link 健全 /
iteration order 非依存、bug なし)。

## alloc-churn まとめ
#6 は Q21 の FE 点読みが range-cover 外で効果ゼロ → skip。#1(scratch+const*)小、#5(inline decode)小、
#2(ankerl)大。累積 Q21 SF=1: 52s→17.4s(3.0x)/ mysqld 23GB→4.11GB(5.6x)。alloc/memcpy 48.7% を直撃。
