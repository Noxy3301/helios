#pragma once

// Declares the LineairDB engine-pushdown hook installed during plugin
// initialization.

class THD;
class JOIN;
struct AccessPath;

/**
 * @brief Install the aggregate executor override for supported query shapes.
 *
 * Unsupported queries leave `override_executor_func` unset and continue through
 * MySQL's normal iterator executor.
 */
int lineairdb_push_to_engine(THD *thd, AccessPath *root_path, JOIN *join);
