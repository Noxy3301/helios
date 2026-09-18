#ifndef LINEAIRDB_PREFETCH_HH
#define LINEAIRDB_PREFETCH_HH

#include <cstdint>
#include <string>

class THD;
class LineairDBTransaction;
struct IndexSearchPlan;
struct TABLE;

// True for statements whose reads a plan can be generated for; every other
// statement reads row by row.
bool thd_can_use_prefetch(THD *thd);

/**
 * @brief Transaction-scoped prefetch: run an injected @_tx_plan (DSL) once
 *        at transaction begin, if one is present.
 *
 * Marks the transaction (tx_plan_used) so the statement-scoped autogen path
 * stays out of the way; a transaction takes its plan from one source, the
 * DSL or the QEP. No-op when no @_tx_plan is set.
 *
 * @param thd Current session.
 * @param tx  Transaction to prefetch into.
 */
void maybe_prefetch_for_transaction(THD *thd,
                                      LineairDBTransaction *tx);

/**
 * @brief Statement-scoped prefetch: auto-generate the read plan from the QEP,
 *        run it in one RPC, and load the result into the transaction's local
 *        view, at most once per statement.
 *
 * "maybe" because it is a conditional no-op: it returns 0 without staging
 * anything when the read path is row, a tx-scoped @_tx_plan is already active,
 * the statement was already staged (keyed by thd->query_id), or its shape has
 * no plan. Unstaged reads go to the storage server one request at a time. Call
 * from rnd_init() / index_read_map() after the optimizer has built the plan.
 *
 * @param thd Current session; its query_id keys the per-statement guard.
 * @param tx  Transaction to stage into.
 * @return 0 on success or skip. HA_ERR_LOCK_DEADLOCK when the staging RPC
 *         aborted the transaction, HA_ERR_NO_CONNECTION when it lost the
 *         connection. Propagate any non-zero return to fail the statement.
 */
int maybe_prefetch_for_statement(THD *thd, LineairDBTransaction *tx,
                                 TABLE *table);

/**
 * @brief True when this statement must defer plan generation until
 *        index_read_map() exposes the legacy single-table DML handler access.
 *        The handler entry points index_read_map(), rnd_init(), and
 *        multi_range_read_init() consult this to route legacy single-table
 *        UPDATE/DELETE to the deferred path.
 */
bool prefetch_needs_legacy_dml_handler(THD *thd,
                                      LineairDBTransaction *tx);

/**
 * @brief Compile and stage a legacy single-table UPDATE/DELETE from its first
 *        handler index access, at most once per statement. A shape one staged
 *        range cannot cover is left to the row path.
 */
int maybe_prefetch_for_legacy_dml_handler(
    THD *thd, LineairDBTransaction *tx, TABLE *table, uint index,
    const IndexSearchPlan &search);

/**
 * @brief Stage the reverse tail window for a key-less primary index_last seek,
 *        at most once per statement and table.
 *
 * The handler consumes the window through the ordinary staged-scan lookup,
 * which registers the reverse+limit range for commit-time replay. `table_key`
 * must be the key the handler passed to choose_table() so the staged entry and
 * the lookup agree.
 *
 * @return 0 on success or skip (row path / tx-scoped plan active). Non-zero
 *         HA_ERR_* when the staging RPC aborted; propagate it.
 */
int maybe_prefetch_for_index_tail(THD *thd, LineairDBTransaction *tx,
                                  const std::string &table_key,
                                  uint64_t window_rows);

/**
 * @brief Fail, loudly, a statement this engine cannot execute at all.
 *
 * Emits ER_NOT_SUPPORTED_YET, marks the transaction aborted and the statement
 * for rollback.
 *
 * @param thd    Current session.
 * @param tx     Transaction to abort; may be null.
 * @param reason Short human-readable cause, included in the error message.
 * @return HA_ERR_UNSUPPORTED, so the caller can return it directly.
 */
int reject_unsupported_statement(THD *thd, LineairDBTransaction *tx,
                                 const char *reason);

/**
 * @brief Map a completed staging attempt to a handler error code.
 *
 * A lost connection maps to HA_ERR_NO_CONNECTION, any other abort to the
 * retryable HA_ERR_LOCK_DEADLOCK with the transaction marked for rollback.
 * This guards on is_aborted() (returning 0 when the transaction is live or
 * null), unlike abort_errno, whose callers may pass a live or null transaction
 * and still need a deadlock.
 *
 * @param thd Current session.
 * @param tx  Transaction whose outcome is inspected; may be null.
 * @return 0 when live or null, else the handler error for the abort.
 */
int prefetch_abort_errno(THD *thd, LineairDBTransaction *tx);

#endif // LINEAIRDB_PREFETCH_HH
