#include "lineairdb_rpc.hh"

#include <cstdint>
#include <vector>

#include "../../common/log.h"
#include "lineairdb.pb.h"

// Database-wide RPC handlers for fencing and DDL.

void LineairDBRpc::handleDbFence(const std::string& message,
                                 std::string& result) {
    LOG_DEBUG("Handling DbFence");

    LineairDB::Protocol::DbFence::Request request;
    LineairDB::Protocol::DbFence::Response response;

    request.ParseFromString(message);

    db_manager_->get_database()->Fence();
    LOG_DEBUG("Database fence completed");

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateTable(const std::string& message,
                                       std::string& result) {
    LOG_DEBUG("Handling DbCreateTable");

    LineairDB::Protocol::DbCreateTable::Request request;
    LineairDB::Protocol::DbCreateTable::Response response;

    request.ParseFromString(message);

    const bool success =
        db_manager_->get_database()->CreateTable(request.table_name());
    response.set_success(success);
    LOG_DEBUG("CreateTable '%s': %s", request.table_name().c_str(),
              success ? "success" : "already exists");

    // Non-empty widths mean the proxy wants this table to try PAX storage.
    if (request.pax_field_max_bytes_size() > 0) {
        std::vector<uint32_t> widths;
        widths.reserve(request.pax_field_max_bytes_size());
        for (const uint32_t width : request.pax_field_max_bytes()) {
            widths.push_back(width);
        }

        const bool installed = db_manager_->get_database()->InstallPaxSchema(
            request.table_name(), widths);
        LOG_INFO("PAX schema for '%s': %zu fields, %s",
                 request.table_name().c_str(), widths.size(),
                 installed ? "installed" : "skipped");
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbCreateSecondaryIndex(const std::string& message,
                                                std::string& result) {
    LOG_DEBUG("Handling DbCreateSecondaryIndex");

    LineairDB::Protocol::DbCreateSecondaryIndex::Request request;
    LineairDB::Protocol::DbCreateSecondaryIndex::Response response;

    request.ParseFromString(message);

    const bool success = db_manager_->get_database()->CreateSecondaryIndex(
        request.table_name(), request.index_name(), request.index_type());
    response.set_success(success);

    result = response.SerializeAsString();
}
