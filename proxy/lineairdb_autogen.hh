#ifndef LINEAIRDB_AUTOGEN_HH
#define LINEAIRDB_AUTOGEN_HH

#include <vector>

class THD;
struct TABLE;

#include "lineairdb_index_search.hh"
#include "lineairdb_proxy.hh"

struct AccessPath;

/**
 * @brief Build a statement-scoped prefetch read plan from a QEP root.
 *
 * @details `root` may be the whole statement root, or a subquery unit root
 * when MySQL evaluates the subquery before the statement plan is finalized.
 * If `allow_filter_pushdown` is true, eligible scan steps carry the
 * table-local cond_push() filter.
 *
 * @note Unsupported QEP shapes raise my_error() and return false. Callers must
 * fail the statement; this path intentionally has no best-effort fallback.
 */
bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out);

/**
 * @brief Build one statement-scoped prefetch step from handler index access.
 *
 * @details Used for legacy single-table UPDATE/DELETE, where the handler
 * supplies the selected index/range instead of a normal QEP root.
 */
bool autogen_read_plan_from_index_search(
    THD *thd, TABLE *table, uint index, const IndexSearchPlan &search,
    std::vector<LineairDBProxy::ReadPlanStep> *out);

#endif
