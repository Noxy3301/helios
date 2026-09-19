# Copyright (c) 2006, 2021, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
#
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation.  The
# authors of MySQL hereby grant you an additional permission to link the program
# and your derivative works with the separately licensed software that they have
# included with MySQL.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License, version 2.0,
# for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51 Franklin
# St, Fifth Floor, Boston, MA 02110-1301  USA

set(CMAKE_CXX_STANDARD 17)

# Use MySQL-defined cmake configuration for protobuf
INCLUDE(${CMAKE_SOURCE_DIR}/cmake/protobuf.cmake)

# Generate protobuf library
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
MYSQL_PROTOBUF_GENERATE_CPP_LIBRARY(
  helios_proto
  ${PROTO_DIR}/helios.proto)

# Add include path for generated protobuf header files
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(HELIOS_SOURCES ha_helios.cc ha_helios.hh
                      ha_helios_duckdb.cc
                      ha_helios_duckdb.hh
                      duckdb_request_builder.cc
                      duckdb_request_builder.hh
                      ddl.cc
                      dml.cc
                      index_scan.cc
                      table_scan.cc
                      multi_range_read.cc
                      optimizer_stats.cc
                      helios_field.cc helios_field.hh
                      log.hh
                      transaction.cc transaction.hh
                      helios_proxy.cc helios_proxy.hh
                      key_pack.cc key_pack.hh
                      row_codec.cc
                      index_search.cc index_search.hh
                      prefetch.cc prefetch.hh
                      read_plan_compiler.cc read_plan_compiler.hh
                      rpc_trace.cc rpc_trace.hh)
add_definitions(-DMYSQL_SERVER)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error -Wextra -mcx16 -fPIC")

mysql_add_plugin(helios_storage_engine ${HELIOS_SOURCES} STORAGE_ENGINE
                 MODULE_ONLY LINK_LIBRARIES helios_proto ext::libprotobuf extra::rapidjson)
