#ifndef HELIOS_FIELD_TYPES_H
#define HELIOS_FIELD_TYPES_H

#include <cstdint>
#include <vector>

#include "field_types.h"

struct TABLE;

/**
 * @brief PAX typed-cell kind constants.
 *
 * @details The values of helios::storage::pax::FieldType (server/storage
 * include/lineairdb/pax.h); the plugin carries no storage header.
 */
namespace pax_kind {
constexpr uint32_t UNTYPED = 0;                 // verbatim byte layout
constexpr uint32_t INT32 = 1;                   // 4-byte LE signed int
constexpr uint32_t INT64 = 2;                   // 8-byte LE signed int
constexpr uint32_t DATE = 3;                    // 4-byte LE YYYYMMDD
constexpr uint32_t DEC64 = 4;                   // 8-byte LE scaled int (DEC64)
}  // namespace pax_kind

/**
 * @brief Computes PAX cell widths, kinds, and scales for the encoded row
 * fields of `table`.
 *
 * @details Helios rows store each MySQL field as the string payload
 * produced by Field::val_str(). The returned vector contains one maximum
 * payload width per encoded row field: entry 0 is the row null-flags field,
 * and the remaining entries follow TABLE::field order. For an UNTYPED field
 * the width is a safe upper bound on the val_str bytes; for a typed field
 * (INT family, DATE) the width is the fixed binary payload width (4/8) and
 * the kind tells the engine to parse val_str once at scatter and reformat it
 * at gather. A table with any field wider than the PAX cell cap returns an
 * empty vector, and CREATE TABLE refuses the table.
 *
 * The computation is a pure function of TABLE metadata: any two calls under
 * an unchanged schema yield identical widths/kinds/scales. CREATE TABLE
 * installs these values server-side; a caller that needs to describe the
 * stored PAX cells recomputes them rather than reading them back from the
 * server.
 *
 * @param table Table whose encoded row fields are described.
 * @param kinds When non-null, receives one pax_kind value per returned width.
 * @param scales When non-null, receives one decimal scale per returned width
 * (nonzero only for DEC64).
 * @param wide_field When non-null, receives the index of the column whose
 * payload exceeds the PAX cell limit, set only when the result is empty.
 * @return Per-field maximum payload widths, or an empty vector when a column
 * is too wide for a PAX cell.
 */
std::vector<uint32_t> compute_pax_field_widths(
    TABLE *table, std::vector<uint32_t> *kinds = nullptr,
    std::vector<int32_t> *scales = nullptr, uint *wide_field = nullptr);

/**
 * @brief Helios-internal classification of MySQL field types.
 */
enum class HeliosFieldType {
  // Numeric types
  HELIOS_INT,

  // String types
  HELIOS_STRING,

  // Date/Time types
  HELIOS_DATETIME,

  // Other/Unsupported types
  HELIOS_OTHER
};

/**
 * @brief Maps a MySQL field type to its Helios field type.
 *
 * @param mysql_type MySQL's enum_field_types.
 * @return Corresponding Helios field type.
 */
inline HeliosFieldType
convert_mysql_type_to_helios(enum_field_types mysql_type) {
  switch (mysql_type) {
  // Numeric types
  case MYSQL_TYPE_TINY:       // TINYINT
  case MYSQL_TYPE_SHORT:      // SMALLINT
  case MYSQL_TYPE_LONG:       // INT
  case MYSQL_TYPE_LONGLONG:   // BIGINT
  case MYSQL_TYPE_INT24:      // MEDIUMINT
  case MYSQL_TYPE_FLOAT:      // FLOAT
  case MYSQL_TYPE_DOUBLE:     // DOUBLE
  case MYSQL_TYPE_DECIMAL:    // DECIMAL (old)
  case MYSQL_TYPE_NEWDECIMAL: // DECIMAL (new)
  case MYSQL_TYPE_YEAR:       // YEAR
    return HeliosFieldType::HELIOS_INT;

  // String types
  case MYSQL_TYPE_VARCHAR:     // VARCHAR
  case MYSQL_TYPE_STRING:      // CHAR
  case MYSQL_TYPE_VAR_STRING:  // VAR_STRING
  case MYSQL_TYPE_BLOB:        // BLOB, TEXT
  case MYSQL_TYPE_TINY_BLOB:   // TINYBLOB, TINYTEXT
  case MYSQL_TYPE_MEDIUM_BLOB: // MEDIUMBLOB, MEDIUMTEXT
  case MYSQL_TYPE_LONG_BLOB:   // LONGBLOB, LONGTEXT
  case MYSQL_TYPE_ENUM:        // ENUM
  case MYSQL_TYPE_SET:         // SET
    return HeliosFieldType::HELIOS_STRING;

  // Date/Time types
  case MYSQL_TYPE_TIMESTAMP:  // TIMESTAMP
  case MYSQL_TYPE_TIMESTAMP2: // TIMESTAMP (internal)
  case MYSQL_TYPE_DATETIME:   // DATETIME
  case MYSQL_TYPE_DATETIME2:  // DATETIME (internal)
  case MYSQL_TYPE_DATE:       // DATE
  case MYSQL_TYPE_TIME:       // TIME
  case MYSQL_TYPE_TIME2:      // TIME (internal)
  case MYSQL_TYPE_NEWDATE:    // NEWDATE (internal)
    return HeliosFieldType::HELIOS_DATETIME;

  // Other/Unsupported types
  case MYSQL_TYPE_NULL:        // NULL
  case MYSQL_TYPE_BIT:         // BIT
  case MYSQL_TYPE_JSON:        // JSON
  case MYSQL_TYPE_GEOMETRY:    // GEOMETRY
  case MYSQL_TYPE_TYPED_ARRAY: // TYPED_ARRAY (replication)
  case MYSQL_TYPE_BOOL:        // BOOL (placeholder)
  case MYSQL_TYPE_INVALID:     // INVALID
  default:
    return HeliosFieldType::HELIOS_OTHER;
  }
}

/**
 * @brief Returns the display name of a Helios field type.
 *
 * @param type Helios field type.
 * @return Type name as a string constant.
 */
inline const char *helios_field_type_name(HeliosFieldType type) {
  switch (type) {
  case HeliosFieldType::HELIOS_INT:
    return "INT";
  case HeliosFieldType::HELIOS_STRING:
    return "STRING";
  case HeliosFieldType::HELIOS_DATETIME:
    return "DATETIME";
  case HeliosFieldType::HELIOS_OTHER:
    return "OTHER";
  default:
    return "UNKNOWN";
  }
}

#endif  // HELIOS_FIELD_TYPES_H
