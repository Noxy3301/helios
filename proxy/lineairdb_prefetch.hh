#ifndef LINEAIRDB_PREFETCH_HH
#define LINEAIRDB_PREFETCH_HH

class THD;
class LineairDBTransaction;
struct IndexSearchPlan;
struct TABLE;

bool thd_can_use_prefetch(THD *thd);
bool thd_has_tx_plan(THD *thd);

/**
 * @brief Transaction-scoped prefetch: run an injected @_tx_plan (DSL) once
 *        at transaction begin, if one is present.
 *
 * Marks the transaction (tx_plan_used) so the statement-scoped autogen path
 * stays out of the way; the two prefetch paths are mutually exclusive per
 * transaction. No-op when no @_tx_plan is set.
 *
 * @param thd Current session.
 * @param tx  Transaction to prefetch into.
 */
void maybe_prefetch_for_transaction(THD *thd,
                                      LineairDBTransaction *tx);

/**
 * @brief Statement-scoped prefetch (default path): auto-generate the prefetch
 *        read plan from the QEP, run it in one RPC, and load the result into the
 *        transaction's local view, at most once per statement.
 *
 * "maybe" because it is a conditional no-op: it returns 0 without prefetching
 * when the transaction is not in prefetch mode, a tx-scoped @_tx_plan is
 * already active, or the statement was already prefetched (keyed by
 * thd->query_id). An ineligible or unsupported statement instead fails with an
 * HA_ERR_* code (see @return). Call from rnd_init() / index_read_map() after the
 * optimizer has built the plan.
 *
 * @param thd Current session; its query_id keys the per-statement guard.
 * @param tx  Transaction to prefetch into.
 * @return 0 on success or skip (including an empty plan). HA_ERR_UNSUPPORTED
 *         when the statement's shape cannot be prefetched (no fallback; a
 *         my_error is raised first, by the eligibility check or by autogen).
 *         HA_ERR_LOCK_DEADLOCK when the prefetch ran but the transaction aborted
 *         (e.g. a read miss), with no my_error. Propagate any non-zero return to
 *         fail the statement.
 */
int maybe_prefetch_for_statement(THD *thd, LineairDBTransaction *tx,
                                 TABLE *table);

/**
 * @brief True when this statement must defer autogen until index_read_map()
 *        exposes the legacy single-table DML handler access.
 *        The handler entry points index_read_map(), rnd_init(), and
 *        multi_range_read_init() consult this to route legacy single-table
 *        UPDATE/DELETE to the deferred path.
 */
bool prefetch_needs_legacy_dml_handler(THD *thd,
                                      LineairDBTransaction *tx);

/**
 * @brief Compile and stage a legacy single-table UPDATE/DELETE from its first
 *        handler index access, at most once per statement.
 */
int maybe_prefetch_for_legacy_dml_handler(
    THD *thd, LineairDBTransaction *tx, TABLE *table, uint index,
    const IndexSearchPlan &search);

/**
 * @brief Fail, loudly, a read surface or statement that prefetch mode cannot
 *        serve (no fallback).
 *
 * Emits ER_NOT_SUPPORTED_YET, marks the transaction aborted and the statement
 * for rollback. Use instead of letting a read fall through to a silent prefetch
 * miss, which surfaces as a retry-looking HA_ERR_LOCK_DEADLOCK.
 *
 * @param thd    Current session.
 * @param tx     Transaction to abort; may be null.
 * @param reason Short human-readable cause, included in the error message.
 * @return HA_ERR_UNSUPPORTED, so the caller can return it directly.
 */
int prefetch_reject_unsupported(THD *thd, LineairDBTransaction *tx,
                                const char *reason);

#endif // LINEAIRDB_PREFETCH_HH
