#include "mysql_charset_runtime.hh"

#include <stdexcept>
#include <string>

#include "m_ctype.h"
#include "my_sys.h"

namespace mysql_charset_runtime {
namespace {

CHARSET_INFO* load_collation(const char* name) {
    MY_CHARSET_LOADER loader;
    CHARSET_INFO* charset = my_collation_get_by_name(&loader, name, MYF(0));
    if (charset == nullptr) {
        throw std::runtime_error(std::string("MySQL collation not found: ") +
                                 name);
    }
    if ((charset->state & MY_CS_READY) == 0) {
        throw std::runtime_error(std::string("MySQL collation not ready: ") +
                                 name);
    }
    return charset;
}

}  // namespace

const Collations& initialize(const char* program_name) {
    static const Collations collations = [program_name] {
        my_progname = program_name;
        if (my_init()) {
            throw std::runtime_error("MySQL runtime initialization failed");
        }

        return Collations{
            load_collation("utf8mb4_0900_ai_ci"),
            load_collation("utf8mb4_0900_bin"),
        };
    }();
    return collations;
}

}  // namespace mysql_charset_runtime
