#ifndef LINEAIRDB_AUTOGEN_HH
#define LINEAIRDB_AUTOGEN_HH

#include <cstdint>
#include <string>
#include <unordered_map>
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
 * If `include_inner_units` is true, Item-held subquery plans are compiled into
 * the same step list so correlated probes can bind to earlier outer steps.
 *
 * @note Unsupported QEP shapes raise my_error() and return false. Callers must
 * fail the statement. Inner-unit failures are ignored only when
 * `include_inner_units` is true; the outer plan remains unchanged.
 */
bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out,
    bool include_inner_units = false);

/**
 * @brief Build one statement-scoped prefetch step from handler index access.
 *
 * @details Used for legacy single-table UPDATE/DELETE, where the handler
 * supplies the selected index/range instead of a normal QEP root.
 */
bool autogen_read_plan_from_index_search(
    THD *thd, TABLE *table, uint index, const IndexSearchPlan &search,
    std::vector<LineairDBProxy::ReadPlanStep> *out);

/**
 * @brief Plan column projection for staged read-plan rows.
 *
 * @details For each physical table, union read_set across aliases and annotate
 * eligible steps with the kept column ordinals. Column-form value bindings
 * force their source column to stay in the projection and are remapped to the
 * trimmed layout. Byte-slice bindings and generated-column tables ship full
 * rows.
 */
void plan_projection_pushdown(
    THD *thd, std::vector<LineairDBProxy::ReadPlanStep> *steps,
    std::unordered_map<std::string, std::vector<uint32_t>> *kept_out);

#endif
