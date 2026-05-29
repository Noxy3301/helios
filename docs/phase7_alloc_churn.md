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
