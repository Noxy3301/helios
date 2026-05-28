# Phase-1A: Drop redundant inner FER + fast slice fallback

**Status**: design + implementation procedure, awaiting verification.
**Owner**: helios.
**Related**: docs/q21_plan_comparison.md (background analysis), Codex review (2026-05-28).

## Problem statement

TPC-H Q21 SF=1 emits 6 plan steps; among them step3 + step4 are FER (per-source-row PK-prefix
range scans) on `lineitem` whose data is ALREADY covered by step0 (a full unfiltered S:
scan of `lineitem`). Server-side: 12M sub-scans, ~48M lineitem rows redundantly shipped.
Result: 8.5GB wire, 24GB proxy RSS, 108s latency (vs InnoDB 3s / 3.3GB).

Root cause: `auto_generate_plan_from_qep`'s `tbl_step` dedup map is keyed by `TABLE *`
(MySQL's per-alias handle), not `physical_table_key(...)`. Self-joins (`lineitem l1, l2, l3`)
hand out distinct `TABLE *` per alias → 3 separate plan steps for the same physical table.

## Goal

Drop a FER step `S_inner` when there's an already-existing `S_outer` for the same physical
table that's a TRUE full-cover unfiltered S: AND no later step's binding sources from
`S_inner`. The MySQL inner probe (`index_read_map` on the inner alias) falls through to
`lookup_local_range_scan`'s full-cover bucket and serves from `S_outer`'s data.

## Codex review summary (2026-05-28)

> A first. It is the right Q21 fix: same end state as B for this case, much lower engineering
> risk, and it directly removes the pathological duplicated lineitem transfer.
> No hidden OCC pitfall if step0 is truly an unfiltered full physical-table key range and
> validation treats that range as covering all inserts/deletes/updates in the table range.
> For fast slice, recompute only the slice-dependent fields. `range_versions` should usually
> stay the original full-cover range validation, not be narrowed away.

## Design

### Step B1: `slice_range_entry_fast` — new shallow + binary-search slice path

In `proxy/lineairdb_transaction.cc`. Static fn next to `slice_range_entry`.

**Inputs**: src `LocalRangeScanEntry` (a full-cover entry, sorted scan_keys), `start_key`,
`end_key`.

**Algorithm**:
1. `out.table_name = src.table_name`, `out.start_key = start_key`,
   `out.end_key = end_key.empty() ? src.end_key : end_key`, `out.reverse_scan = src.reverse_scan`,
   `out.row_limit = src.row_limit`.
2. Binary search: `lo = std::lower_bound(src.scan_keys.begin(), src.scan_keys.end(), start_key)`,
   `hi = std::lower_bound(src.scan_keys.begin(), src.scan_keys.end(), out.end_key)`.
3. `out.scan_keys.assign(lo, hi)`, `out.scan_values.assign(src.scan_values.begin()+i0,
   ...begin()+i1)`. Same for `scan_tids` / `secondary_keys` if populated.
4. Mirror for `rows` (parallel vector of `pair<key,value>`). Binary search on
   `rows[i].first` (also sorted).
5. **Critical** (Codex 4): `out.range_versions = src.range_versions;` — copy WHOLE (do NOT
   slice `result_keys`). This preserves the full-cover OCC validation as proven by Codex.
6. `out.filtered_keys` / `filtered_tids`: ALSO copy WHOLE — they're the pre-filter
   rejected set covering the full range, not a slice-membership artefact.
7. `out.index_reads` = copy WHOLE.

**Why not also slice range_versions/filtered_keys**: they participate in commit-time OCC
revalidation against the wider scanned range; narrowing them silently weakens OCC.

### Step B2: route inner probes through fast slice via full-cover bucket

In `lookup_local_range_scan` (`proxy/lineairdb_transaction.cc:1778`):

Currently the full-cover bucket (line 1818-1835) calls `slice_range_entry(...)` which is
the O(N) + deep-copy version. Add a guard:

```cpp
if (range_entry_matches(cand, table_name, start_key, end_key, ...)) {
  // Fast path: cand is keyed by empty start_key, has unfiltered ordered rows.
  // The slicing here serves probes inside the full table — for Q21-scale (6M
  // probes hitting a 6M-row entry) the O(N)+deep-copy is catastrophic. Use
  // the binary-search shallow-slice path instead.
  if (cand.start_key.empty() && cand.filter_serialized.empty() && cand.row_limit == 0)
    return slice_range_entry_fast(cand, start_key, end_key);
  return slice_range_entry(cand, start_key, end_key);
}
```

This restricts the fast path to **truly unfiltered full-cover entries**, leaving filtered /
limited / keypart-prefix entries on the existing path.

### Step B3: planner dedup pass

In `proxy/ha_lineairdb.cc:auto_generate_plan_from_qep`, AFTER the main + subquery
`compile_leaf` loops AND the `net_full_scans` uncovered-net loop (around line 2000), BEFORE
projection planning (line 2106):

```cpp
// Phase-1A: identify true full-cover S: steps per physical table.
auto is_full_cover_step = [](const ReadPlanStep& s) {
  // 16-byte 0xFF sentinel is what the compiler emits for full-table S: (line 1791).
  static const std::string kFullEnd(16, '\xff');
  return s.is_scan && !s.for_each &&
         s.index_name.empty() &&
         s.bindings.empty() && s.end_bindings.empty() &&
         s.key_prefix.empty() &&
         s.end_key_prefix == kFullEnd &&
         s.scan_limit == 0 &&
         s.filter_serialized.empty();
};
std::unordered_map<std::string, size_t> full_coverer;  // table_name -> idx
for (size_t i = 0; i < steps.size(); ++i)
  if (is_full_cover_step(steps[i]))
    full_coverer.try_emplace(steps[i].table_name, i);

// Identify steps that ARE binding sources for later steps.
std::unordered_set<size_t> bound_source_steps;
for (const auto &s : steps) {
  for (const auto &b : s.bindings) bound_source_steps.insert(b.source_step);
  for (const auto &b : s.end_bindings) bound_source_steps.insert(b.source_step);
}

// Mark FER inner steps droppable.
std::vector<bool> drop(steps.size(), false);
size_t n_drop = 0;
for (size_t i = 0; i < steps.size(); ++i) {
  const auto &s = steps[i];
  // Only primary FER (not FE point reads — they're cheap and need own validation).
  // Not FES — secondary cache is a separate lookup path.
  if (!(s.for_each && s.is_scan && s.index_name.empty())) continue;
  auto it = full_coverer.find(s.table_name);
  if (it == full_coverer.end() || it->second == i) continue;
  if (bound_source_steps.count(i)) continue;  // a later step binds from us
  drop[i] = true;
  ++n_drop;
}
if (n_drop == 0) goto skip_dedup;

// Compact + reindex bindings. drop[i] in increasing i, so new index = old - count_drops_before(i).
std::vector<size_t> new_idx(steps.size());
size_t out_i = 0;
for (size_t i = 0; i < steps.size(); ++i) {
  if (drop[i]) { new_idx[i] = SIZE_MAX; continue; }
  new_idx[i] = out_i++;
}
std::vector<ReadPlanStep> kept;
kept.reserve(out_i);
for (size_t i = 0; i < steps.size(); ++i) {
  if (drop[i]) continue;
  ReadPlanStep s = std::move(steps[i]);
  for (auto &b : s.bindings)     b.source_step = new_idx[b.source_step];
  for (auto &b : s.end_bindings) b.source_step = new_idx[b.source_step];
  kept.push_back(std::move(s));
}
steps = std::move(kept);

if (std::getenv("HELIOS_FE_DEBUG"))
  std::fprintf(stderr, "[QEP] phase1a-dedup dropped %zu redundant FER step(s)\n", n_drop);
skip_dedup:;
```

NOTE: dropped steps have indices ≤ their dependents; bindings to dropped indices are filtered
out (we already excluded that case). The `new_idx[old]` lookup for surviving bindings yields
their new positions correctly.

### Step B4 (deferred): FES secondary self-joins

Q9 / Q12-style: same physical table referenced via secondary index from inner. Drop is unsafe
without secondary cache prefetch on S_outer. Skip for Phase-1A; tag for Phase-2.

## Procedure (reproducible run)

```bash
cd /home/noxy/helios
# Pre-flight: confirm InnoDB + helios mysqld are both up.
build/runtime_output_directory/mysqladmin ping -u root --socket=/tmp/mysql.sock
build/runtime_output_directory/mysqladmin ping -u root --socket=/tmp/mysql_innodb.sock

# B1/B2: edit proxy/lineairdb_transaction.cc (slice_range_entry_fast + lookup integration).
# B3:    edit proxy/ha_lineairdb.cc (auto_generate_plan_from_qep post-pass).

# Build (partial, proxy only)
./scripts/build_partial.sh   # or full ./scripts/build.sh

# Restart only mysqld (server data preserved per memory).
kill $(cat /tmp/mysql.pid); sleep 4
# (re-launch as in scripts/start_mysql.sh; uses LD_PRELOAD=jemalloc and HELIOS_PIN_TTL_MS=1800000)

# Verify Q21 plan shrinks to 4 steps.
# HELIOS_FE_DEBUG=1 must be in mysqld env. Look for "phase1a-dedup dropped 2".
tail -F lineairdb_logs/mysqld_3307_*.log | grep -E 'phase1a|QEP step|QEP root'

# Functional check (md5 vs InnoDB)
for q in q6 q1 q3 q9 q18 q21; do
  HELIOS=$(./build/runtime_output_directory/mysql -u root --socket=/tmp/mysql.sock benchbase < /tmp/v_$q.sql | md5sum)
  INNO=$(./build/runtime_output_directory/mysql -u root --socket=/tmp/mysql_innodb.sock benchbase < /tmp/v_$q.sql | md5sum)
  echo "$q helios=$HELIOS innodb=$INNO"
done

# Memory + latency probe (per-q peak RSS via /proc).
bash /tmp/measqry.sh  # OR re-run /tmp/meas_innodb.sh-equivalent for helios mysqld.
```

## Acceptance criteria

1. Q21 [QEP] dump shows 4 steps (was 6); `phase1a-dedup dropped 2` line present.
2. `md5(helios) == md5(InnoDB)` for all 6 Q's we have baseline for; same for the 22-suite when fully harvested.
3. Q21 latency ≤ 30s SF=1 (current 108s); RSS ≤ 12GB (current 24GB).
4. No regression on Q1/Q3/Q6/Q9/Q18 (latency within 20% of pre-patch).
5. Codex final review GO.

## Risks + mitigations

| Risk | Mitigation |
|---|---|
| Inner probe doesn't reach full-cover bucket | `range_entry_matches` guard at line 1683 already requires non-filtered for non-exact reuse; we only fast-path unfiltered+full-end entries |
| Bindings re-index off-by-one | Use `new_idx` lookup table; assert no surviving binding maps to dropped index (we filtered those upstream) |
| Sliced range_versions narrowed away | Step B1 explicitly copies WHOLE — Codex point (4) |
| FES inner needs cache that doesn't exist | Phase-1A excludes FES (only `index_name.empty()` qualifies for drop) |
| filter_serialized non-empty edge case | Only TRUE full-cover entries are fast-pathed (`filter_serialized.empty()` guard in B2) |

## Out of scope

- FES self-join optimization → Phase-2.
- Real OR-union pushdown DSL → Phase-N (low value vs effort per Codex).
- Cache structure change (hash → tree) → no benefit per measurement-supported analysis.

