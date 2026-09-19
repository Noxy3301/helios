// index_search.hh
// Helios Storage Engine: A structure to hold the search plan

#ifndef HELIOS_INDEX_SEARCH_HH
#define HELIOS_INDEX_SEARCH_HH

#include <string>
#include <optional>
#include "my_base.h"

/**
 * @brief Types of search operations
 */
enum class IndexSearchOp
{
    kIndexFirst,         // key == nullptr
    kUniquePoint,        // (PK or UNIQUE) && full key && !nullable-unique
    kSameKey,            // find_flag == EXACT but not unique point
    kPrefixFirst,        // HA_READ_PREFIX: return first match only
    kRangeScan,          // range search (KEY_OR_NEXT / AFTER_KEY, etc.)
    kPrevKey,            // HA_READ_KEY_OR_PREV / HA_READ_BEFORE_KEY
    kPrefixLast,         // HA_READ_PREFIX_LAST / LAST_OR_PREV, etc.
};

/**
 * @brief Structure to hold search plan
 */
struct IndexSearchPlan
{
    IndexSearchOp op = IndexSearchOp::kRangeScan;

    // basic information
    bool is_primary = false;
    uint used_key_parts = 0;
    bool all_parts_specified = false;
    bool is_unique_index = false;    // HA_NOSAME
    bool has_nullable_parts = false; // HA_NULL_PART_KEY
    enum ha_rkey_function find_flag = HA_READ_KEY_EXACT;

    // boundary information (packed)
    std::string packed_start_key;
    std::string packed_end_key;

    // same group boundary (for index_next_same)
    std::string packed_same_key_prefix;
    std::string packed_same_key_end;

    void reset()
    {
        op = IndexSearchOp::kRangeScan;
        is_primary = false;
        used_key_parts = 0;
        all_parts_specified = false;
        is_unique_index = false;
        has_nullable_parts = false;
        find_flag = HA_READ_KEY_EXACT;
        packed_start_key.clear();
        packed_end_key.clear();
        packed_same_key_prefix.clear();
        packed_same_key_end.clear();
    }
};

#endif // HELIOS_INDEX_SEARCH_HH
