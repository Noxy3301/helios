#ifndef LINEAIRDB_AUTOGEN_HH
#define LINEAIRDB_AUTOGEN_HH

#include <vector>

class THD;

#include "lineairdb_proxy.hh"

// Auto-generate a statement-scoped prefetch read plan from the current
// statement's MySQL QEP (JOIN::root_access_path()). Returns true with steps on
// full success. On ANY unsupported QEP shape it raises my_error(...) and
// returns false (NO fallback, NO best-effort coverage). Caller must fail the
// statement when false.
bool autogen_read_plan_from_qep(
    THD *thd, std::vector<LineairDBProxy::ReadPlanStep> *out);

#endif
