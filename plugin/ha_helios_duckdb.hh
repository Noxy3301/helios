#ifndef HA_HELIOS_DUCKDB_HH
#define HA_HELIOS_DUCKDB_HH

#include "my_base.h"
#include "sql/handler.h"
#include "thr_lock.h"

class THD;
struct TABLE;
struct TABLE_SHARE;

namespace dd {
class Table;
}

/**
 * @brief MySQL secondary engine handler for HELIOS_DUCKDB.
 *
 * This is the table-facing handler MySQL opens after a table is assigned to
 * HELIOS_DUCKDB and marked with SECONDARY_LOAD. Loading only records that
 * the table may be opened through this plugin; rows stay in Helios/PAX
 * storage and are shared with the primary Helios handler.
 *
 * Row-access methods are intentionally inert. Supported SELECT blocks run
 * through JOIN::override_executor_func after optimize_secondary_engine accepts
 * their shape; unsupported blocks reject from the secondary path and can retry
 * on the primary engine when use_secondary_engine=ON.
 */
class ha_helios_duckdb : public handler {
 public:
  ha_helios_duckdb(handlerton *hton, TABLE_SHARE *table_share_arg);

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
  int info(unsigned int flags) override;
  ha_rows records_in_range(unsigned int index, key_range *min_key,
                           key_range *max_key) override;
  void position(const unsigned char *) override {}
  unsigned long index_flags(unsigned int index, unsigned int part,
                            bool all_parts) const override;
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             thr_lock_type lock_type) override;
  int external_lock(THD *thd, int lock_type) override;
  Table_flags table_flags() const override { return HA_NO_INDEX_ACCESS; }
  const char *table_type() const override { return "HELIOS_DUCKDB"; }
  int load_table(const TABLE &table) override;
  int unload_table(const char *db_name, const char *table_name,
                   bool error_if_not_loaded) override;

 private:
  THR_LOCK_DATA lock_data_;
};

#endif  // HA_HELIOS_DUCKDB_HH
