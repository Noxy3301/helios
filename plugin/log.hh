#ifndef HELIOS_LOG_HH
#define HELIOS_LOG_HH

#define LOG_COMPONENT_TAG "helios"

#include <mysql/components/services/log_builtins.h>
#include <mysqld_error.h>

extern SERVICE_TYPE(log_builtins) * log_bi;
extern SERVICE_TYPE(log_builtins_string) * log_bs;

#define LOG_ERROR(...) \
  LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG, __VA_ARGS__)
#define LOG_WARNING(...) \
  LogPluginErrMsg(WARNING_LEVEL, ER_LOG_PRINTF_MSG, __VA_ARGS__)
#define LOG_INFO(...) \
  LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, __VA_ARGS__)

#endif  // HELIOS_LOG_HH
