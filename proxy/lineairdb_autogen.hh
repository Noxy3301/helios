#ifndef LINEAIRDB_AUTOGEN_HH
#define LINEAIRDB_AUTOGEN_HH

#include <vector>

class THD;
struct TABLE;

#include "lineairdb_index_search.hh"
#include "lineairdb_proxy.hh"

struct AccessPath;

// Auto-generate a statement-scoped prefetch read plan from the given QEP root
// (statement root, or a subquery unit's root when the statement plan is not
// finalized yet). Returns true with steps on full success. On ANY unsupported
// QEP shape it raises my_error(...) and returns false (NO fallback, NO
// best-effort coverage). Caller must fail the statement when false.
bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out);

// Auto-generate one statement-scoped prefetch step from the handler access
// selected for legacy single-table UPDATE/DELETE.
bool autogen_read_plan_from_index_search(
    THD *thd, TABLE *table, uint index, const IndexSearchPlan &search,
    std::vector<LineairDBProxy::ReadPlanStep> *out);

#endif
