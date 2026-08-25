#pragma once

struct CHARSET_INFO;

namespace mysql_charset_runtime {

struct Collations {
    CHARSET_INFO* utf8mb4_0900_ai_ci;
    CHARSET_INFO* utf8mb4_0900_bin;
};

// Initializes the MySQL runtime and returns ready-to-use compiled collations.
const Collations& initialize(const char* program_name);

}  // namespace mysql_charset_runtime
