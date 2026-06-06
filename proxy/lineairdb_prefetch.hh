#ifndef LINEAIRDB_PREFETCH_HH
#define LINEAIRDB_PREFETCH_HH

class THD;
class LineairDBTransaction;

bool thd_can_use_prefetch(THD *thd);
bool thd_has_tx_plan(THD *thd);
void maybe_prefetch_for_transaction(THD *thd,
                                      LineairDBTransaction *tx);

#endif // LINEAIRDB_PREFETCH_HH
