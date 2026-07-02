#ifndef HA_LINEAIRDB_COLUMNAR_HH
#define HA_LINEAIRDB_COLUMNAR_HH

/**
 * LINEAIRDB_COLUMNAR — secondary engine over the same LineairDB server.
 *
 * OLTP stays on the primary engine (ha_lineairdb, tuple-at-a-time). Read-only
 * autocommit SELECTs whose optimizer cost crosses
 * secondary_engine_cost_threshold are offloaded here: the whole query block
 * is executed server-side over the (single-copy) PAX strips and result rows
 * are streamed straight into the client protocol, bypassing MySQL's
 * executor (JOIN::override_executor_func, the HeatWave mechanism).
 *
 * Unlike HeatWave there is no second data copy: SECONDARY_LOAD only
 * registers the table — primary and secondary read the same LineairDB
 * storage, so freshness lag is zero by construction.
 *
 * Phase A scope: single-table aggregation query blocks (SUM / COUNT(*),
 * optional server-evaluable WHERE, optional GROUP BY over binary-safe
 * columns; no HAVING/ORDER BY/LIMIT/DISTINCT). Anything else is rejected in
 * the optimize hook, which re-prepares the statement on the primary engine.
 */

#include "my_base.h"
#include "sql/handler.h"
#include "thr_lock.h"

class THD;
struct TABLE;
struct TABLE_SHARE;

namespace dd {
class Table;
}

namespace lineairdb_columnar {

class ha_lineairdb_columnar : public handler {
 public:
  ha_lineairdb_columnar(handlerton *hton, TABLE_SHARE *table_share_arg);

  int create(const char *, TABLE *, HA_CREATE_INFO *, dd::Table *) override {
    return HA_ERR_WRONG_COMMAND;
  }
  int open(const char *name, int mode, unsigned int test_if_locked,
           const dd::Table *table_def) override;
  int close() override { return 0; }
  int rnd_init(bool) override { return 0; }
  int rnd_next(unsigned char *) override { return HA_ERR_END_OF_FILE; }
  int rnd_pos(unsigned char *, unsigned char *) override {
    return HA_ERR_WRONG_COMMAND;
  }
  int info(unsigned int) override;
  ha_rows records_in_range(unsigned int index, key_range *min_key,
                           key_range *max_key) override;
  void position(const unsigned char *) override {}
  unsigned long index_flags(unsigned int idx, unsigned int part,
                            bool all_parts) const override;
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             thr_lock_type lock_type) override;
  Table_flags table_flags() const override { return HA_NO_INDEX_ACCESS; }
  const char *table_type() const override { return "LINEAIRDB_COLUMNAR"; }
  int load_table(const TABLE &table) override;
  int unload_table(const char *db_name, const char *table_name,
                   bool error_if_not_loaded) override;

 private:
  THR_LOCK_DATA m_lock;
};

}  // namespace lineairdb_columnar

#endif  // HA_LINEAIRDB_COLUMNAR_HH
