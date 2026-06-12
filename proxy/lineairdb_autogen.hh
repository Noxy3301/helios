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
class LineairDBTransaction;

// Auto-generate a statement-scoped prefetch read plan from the given QEP root
// (statement root, or a subquery unit's root when the statement plan is not
// finalized yet). Returns true with steps on full success. On ANY unsupported
// QEP shape it raises my_error(...) and returns false (NO fallback, NO
// best-effort coverage). Caller must fail the statement when false.
//
// include_inner_units: also compile the plan trees of the statement's inner
// query expressions (dependent scalar subqueries / in_optimizer materialized
// IN), which hang off Item conditions and are invisible to the main-tree
// walk. Their leaves share the main tree's step table so correlated probes
// bind as for_each. Inner-unit compile failures are NON-fatal (that unit is
// skipped; executing it later misses and aborts, i.e. today's behavior).
// tx (optional): consulted for inner-unit aggregate registrations — a staged
// scan step whose table was registered (and whose attached filter equals the
// registered one) is stamped with the AggregateSpec so the server returns
// group rows; the stamp decision is recorded back on the tx.
bool autogen_read_plan_from_qep(
    THD *thd, AccessPath *root, bool allow_filter_pushdown,
    std::vector<LineairDBProxy::ReadPlanStep> *out,
    bool include_inner_units = false, LineairDBTransaction *tx = nullptr);

// Auto-generate one statement-scoped prefetch step from the handler access
// selected for legacy single-table UPDATE/DELETE.
bool autogen_read_plan_from_index_search(
    THD *thd, TABLE *table, uint index, const IndexSearchPlan &search,
    std::vector<LineairDBProxy::ReadPlanStep> *out);

// Projection pushdown planning (ro_novalidate SELECT only): per physical
// table, union the read_set across aliases; annotate eligible scan steps with
// the kept column set so the server trims shipped VALUES. kept_out gets one
// entry per projected table for the tx-side decoder map. Tables serving
// value-form bindings (the server extracts columns/bytes from their shipped
// rows positionally) and generated-column tables ship full.
void plan_projection_pushdown(
    THD *thd, std::vector<LineairDBProxy::ReadPlanStep> *steps,
    std::unordered_map<std::string, std::vector<uint32_t>> *kept_out);

#endif
