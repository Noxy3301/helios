#include "row_codec.hh"

#include <cstdlib>

std::string next_lexicographic_key(std::string key) {
    for (size_t i = key.size(); i-- > 0;) {
        auto byte = static_cast<unsigned char>(key[i]);
        if (byte != 0xFF) {
            key[i] = static_cast<char>(byte + 1);
            key.resize(i + 1);
            return key;
        }
    }
    return {};
}

bool trim_row_value(const std::string& full,
                    const google::protobuf::RepeatedField<uint32_t>& kept,
                    uint32_t num_columns, std::string& out) {
    out.clear();
    const char* end = full.data() + full.size();
    auto read_field = [&](const char*& q, const char*& fstart,
                          size_t& flen) -> bool {
        fstart = q;
        if (q >= end) return false;
        uint8_t bs = static_cast<uint8_t>(*q);
        if (bs == 0xFF) { flen = 1; q += 1; return true; }
        if (q + 1 + bs > end) return false;
        size_t len = 0;
        for (uint8_t i = 0; i < bs; i++)
            len |= static_cast<size_t>(static_cast<uint8_t>(q[1 + i])) << (8 * i);
        if (q + 1 + bs + len > end) return false;
        flen = 1 + bs + len;
        q += flen;
        return true;
    };
    const char* q = full.data();
    const char* fs;
    size_t fl;
    if (!read_field(q, fs, fl)) return false;  // field 0 = null_flags
    out.append(fs, fl);
    int ki = 0;
    for (uint32_t c = 0; c < num_columns; c++) {  // column c is field index c+1
        const char* cs;
        size_t cl;
        if (!read_field(q, cs, cl)) return false;
        if (ki < kept.size() &&
            kept.Get(ki) == static_cast<uint32_t>(c)) {
            out.append(cs, cl);
            ki++;
        }
    }
    return ki == kept.size();  // every requested column was present
}

std::string_view extract_value_column(const std::string& row,
                                      int column_index) {
    size_t offset = 0;
    int field_index = 0;
    const int target_field = column_index + 1; // field 0 is null flags.

    while (offset < row.size()) {
        const auto byte_size = static_cast<unsigned char>(row[offset]);
        ++offset;
        if (byte_size == 0xFF) {
            if (field_index == target_field) return {};
            ++field_index;
            continue;
        }
        if (offset + byte_size > row.size()) return {};

        size_t value_length = 0;
        for (unsigned int i = 0; i < byte_size; ++i) {
            value_length |= static_cast<size_t>(
                static_cast<unsigned char>(row[offset + i])) << (8 * i);
        }
        offset += byte_size;
        if (offset + value_length > row.size()) return {};

        if (field_index == target_field) {
            return std::string_view(row.data() + offset, value_length);
        }
        offset += value_length;
        ++field_index;
    }
    return {};
}

std::string encode_int_key_part(int64_t value) {
    const auto encoded = static_cast<uint32_t>(static_cast<int32_t>(value)) ^
                         0x80000000U;
    std::string out;
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x10));
    out.push_back(static_cast<char>(0x00));
    out.push_back(static_cast<char>(0x04));
    out.push_back(static_cast<char>((encoded >> 24) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 16) & 0xFF));
    out.push_back(static_cast<char>((encoded >> 8) & 0xFF));
    out.push_back(static_cast<char>(encoded & 0xFF));
    return out;
}

bool decode_leading_int_key(std::string_view key, int64_t& out) {
    if (key.size() < 8) return false;
    if (static_cast<uint8_t>(key[0]) != 0x00 ||
        static_cast<uint8_t>(key[1]) != 0x10 ||
        static_cast<uint8_t>(key[2]) != 0x00 ||
        static_cast<uint8_t>(key[3]) != 0x04) {
        return false;
    }

    const uint32_t encoded =
        (static_cast<uint32_t>(static_cast<uint8_t>(key[4])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(key[5])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(key[6])) << 8) |
        static_cast<uint32_t>(static_cast<uint8_t>(key[7]));
    out = static_cast<int32_t>(encoded ^ 0x80000000U);
    return true;
}

std::string encode_column_as_int_key(std::string_view column,
                                     int64_t int_delta) {
    std::string tmp(column);
    int64_t value = std::strtoll(tmp.c_str(), nullptr, 10);
    return encode_int_key_part(value + int_delta);
}
