cmake_minimum_required(VERSION 3.16)
project(helios)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)

# DuckDB library path (third_party submodule). The shared library is a release
# build produced inside the submodule; scripts/build.sh builds it when absent.
set(DUCKDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/duckdb")
set(DUCKDB_LIB_DIR "${DUCKDB_ROOT}/build/release/src")

# MySQL charset runtime. libstrings contains the collation descriptors and
# handlers; libmysys contains the loader, registry, and runtime
# initialization. The final libstrings entry resolves libmysys
# back-references with static-link semantics.
set(MYSQL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/mysql-server")
set(MYSQL_BUILD_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../build")
set(MYSQL_ARCHIVE_DIR "${MYSQL_BUILD_ROOT}/archive_output_directory")
set(MYSQL_CHARSET_LIBRARIES
    ${MYSQL_ARCHIVE_DIR}/libstrings.a
    ${MYSQL_ARCHIVE_DIR}/libmysys.a
    ${MYSQL_ARCHIVE_DIR}/libstrings.a)
set(MYSQL_INCLUDE_DIRS
    ${MYSQL_ROOT}
    ${MYSQL_ROOT}/include
    ${MYSQL_BUILD_ROOT}/include)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../proto)
include_directories(${DUCKDB_ROOT}/src/include)

# Generate protobuf files
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../proto")
set(PROTO_FILE "${PROTO_DIR}/helios.proto")
set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/helios.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/helios.pb.h")

# Custom command to generate protobuf files
add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR} --proto_path=${PROTO_DIR} ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating protobuf files"
)

# The storage engine; it exports its include and src directories.
add_subdirectory(storage)

# Source files
set(SOURCES
    # Core
    main.cc
    storage_server.cc
    storage_server.hh
    server_config.cc
    server_config.hh
    
    # Network layer
    network/tcp_server.cc
    network/tcp_server.hh
    network/message_handler.cc
    network/message_handler.hh
    
    # RPC layer
    rpc/helios_rpc.cc
    rpc/helios_rpc.hh
    rpc/database_operations.cc
    rpc/flat_plan_encode.cc
    rpc/flat_plan_encode.hh
    rpc/parallel_scan.cc
    rpc/parallel_scan.hh
    rpc/read_plan_executor.cc
    rpc/row_codec.cc
    rpc/row_codec.hh
    rpc/transaction_operations.cc
    rpc/table_stats.cc
    mysql_charset_runtime.cc
    mysql_charset_runtime.hh
    rpc/duckdb_ast_builder.cc
    rpc/duckdb_ast_builder.hh
    rpc/duckdb_bridge_dispatch.cc
    rpc/duckdb_bridge_executor.cc
    rpc/duckdb_bridge_executor.hh
    
    # Storage layer
    database_manager.cc
    database_manager.hh
)

# The generated sources belong to exactly one target: listing them in two
# executables lets a parallel build run protoc twice over the same files.
add_library(helios_proto STATIC ${PROTO_SRCS})
target_include_directories(helios_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(helios_proto PUBLIC ${Protobuf_LIBRARIES})

# Create executable
add_executable(helios-storage ${SOURCES})

# Link libraries
target_link_libraries(helios-storage
    helios_storage
    helios_proto
    pthread
    ${DUCKDB_LIB_DIR}/libduckdb.so
    ${MYSQL_CHARSET_LIBRARIES}
)

# Additional include directories for generated files
target_include_directories(helios-storage PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${MYSQL_INCLUDE_DIRS})

# Embed an rpath to the DuckDB shared library. start_server.sh runs the binary
# directly, without LD_LIBRARY_PATH.
set_target_properties(helios-storage PROPERTIES
    BUILD_RPATH "${DUCKDB_LIB_DIR}"
    INSTALL_RPATH "${DUCKDB_LIB_DIR}")

# -Wswitch reports an envelope arm that the dispatch switch has no case for.
target_compile_options(helios-storage PRIVATE -O3 -Wno-error -Wno-unused-parameter -Wswitch)

# spdlog drops anything below this at compile time, so debug lines stay in the
# binary and log_level decides at run time.
target_compile_definitions(helios-storage PRIVATE
    SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG)

