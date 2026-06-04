#ifndef LINEAIRDB_PREFETCH_HH
#define LINEAIRDB_PREFETCH_HH

class THD;
class LineairDBTransaction;

bool thd_can_use_prefetch(THD *thd);
bool thd_has_prefetch_plan(THD *thd);
void execute_prefetch_plan_if_present(THD *thd,
                                      LineairDBTransaction *tx);

#endif // LINEAIRDB_PREFETCH_HH
