// lineairdb_keyenc.hh
// LineairDB Storage Engine: shared constants for index key / range encoding.
//
// These key-type tags and null markers are used both by the index key
// encoders (ha_lineairdb::key_part_type_tag / append_key_part_encoding, defined
// in lineairdb_keyenc.cc) and by the prefetch plan key encoders in
// ha_lineairdb.cc. constexpr => internal linkage, safe to include in multiple
// translation units.

#ifndef LINEAIRDB_KEYENC_HH
#define LINEAIRDB_KEYENC_HH

constexpr unsigned char kKeyMarkerNotNull = 0x00;
constexpr unsigned char kKeyMarkerNull = 0x01;

constexpr unsigned char kKeyTypeInt = 0x10;
constexpr unsigned char kKeyTypeString = 0x20;
constexpr unsigned char kKeyTypeDatetime = 0x30;
constexpr unsigned char kKeyTypeOther = 0xF0;

#endif // LINEAIRDB_KEYENC_HH
