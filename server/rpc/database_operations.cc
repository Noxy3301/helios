#include "lineairdb_rpc.hh"

#include <cstdint>
#include <cstdlib>
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

        // Typed cells: gate on HELIOS_PAX_TYPED (default on). When off, or when
        // the proxy sent no/mismatched kinds, install an UNTYPED schema
        // (byte-identical to the ASCII layout).
        static const bool pax_typed_enabled = []() {
            const char* v = std::getenv("HELIOS_PAX_TYPED");
            return !(v != nullptr && v[0] == '0' && v[1] == '\0');
        }();
        std::vector<uint8_t> kinds;
        std::vector<int8_t> scales;
        if (pax_typed_enabled &&
            request.pax_field_kind_size() ==
                request.pax_field_max_bytes_size()) {
            kinds.assign(request.pax_field_kind().begin(),
                         request.pax_field_kind().end());
            if (request.pax_field_scale_size() ==
                request.pax_field_max_bytes_size()) {
                scales.reserve(request.pax_field_scale_size());
                for (const int32_t scale : request.pax_field_scale()) {
                    scales.push_back(static_cast<int8_t>(scale));
                }
            }
        }

        const bool installed = db_manager_->get_database()->InstallPaxSchema(
            request.table_name(), widths, kinds, scales);
        LOG_INFO("PAX schema for '%s': %zu fields, typed=%s, %s",
                 request.table_name().c_str(), widths.size(),
                 kinds.empty() ? "no" : "yes",
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
