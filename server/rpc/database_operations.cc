#include "helios_rpc.hh"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <random>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>
#include "helios.pb.h"
#include "helios/pax.h"
#include "helios/transaction.h"

// Database-wide RPC handlers for DDL.

namespace {

// Holds one next-unallocated id per table, keyed by table name. MySQL table
// names are paths and always begin with "./", so no user table can land here.
constexpr char kWatermarkTable[] = "__helios_hidden_keys";

}  // namespace

uint64_t storage_boot_token() {
    // Startup nanoseconds with the low bits replaced by entropy: restarts are
    // milliseconds apart, so the token strictly increases across runs. A clock
    // stepped back onto a bucket a past run used could repeat a token.
    constexpr int kEntropyBits  = 16;  // ~65 us, far below a restart
    constexpr uint64_t kLowMask = (1ull << kEntropyBits) - 1;

    static const uint64_t token = []() -> uint64_t {
        const uint64_t started = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
        std::random_device source;
        const uint64_t value =
            (started & ~kLowMask) | (static_cast<uint64_t>(source()) & kLowMask);
        return value == 0 ? 1 : value;  // zero means "no token" to the plugin
    }();
    return token;
}

namespace {

// Watermark rows are one row-format field of 8 bytes after the null flags.
const std::vector<uint32_t> kWatermarkFields = {1, 8};

std::string pack_watermark(uint64_t next) {
    std::string row("\x01\x01\x00\x01\x08", 5);  // null flags: one byte, no nulls; then the counter
    row.append(reinterpret_cast<const char*>(&next), sizeof(next));
    return row;
}

bool unpack_watermark(const std::string& row, uint64_t* next) {
    if (row.size() != 13 ||
        row.compare(0, 5, std::string("\x01\x01\x00\x01\x08", 5)) != 0) {
        return false;
    }
    std::memcpy(next, row.data() + 5, sizeof(*next));
    return true;
}

using DurabilityRpc = Helios::Protocol::DbSetCommitDurability;

const char* durability_name(helios::storage::CommitDurability mode) {
    switch (mode) {
        case helios::storage::CommitDurability::kAsync:
            return "ASYNC";
        case helios::storage::CommitDurability::kSync:
            return "SYNC";
    }
    return "UNKNOWN";
}

DurabilityRpc::Mode to_wire_mode(helios::storage::CommitDurability mode) {
    switch (mode) {
        case helios::storage::CommitDurability::kAsync:
            return DurabilityRpc::ASYNC;
        case helios::storage::CommitDurability::kSync:
            return DurabilityRpc::SYNC;
    }
    return DurabilityRpc::MODE_UNSPECIFIED;
}

}  // namespace

void HiddenKeyAllocator::ForgetTable(const std::string& table_name) {
    std::lock_guard<std::mutex> serialized(mutex_);
    refused_.erase(table_name);
    announced_.erase(table_name);
}

bool HiddenKeyAllocator::Allocate(helios::storage::Database& database,
                                  const std::string& table_name, uint32_t count,
                                  uint64_t* first_id, std::string* error,
                                  bool* permanent) {
    *permanent = false;
    if (count == 0 || count > kMaxCount) {
        *error     = "requested hidden key count is out of range";
        *permanent = true;
        return false;
    }

    std::lock_guard<std::mutex> serialized(mutex_);
    const auto refused = refused_.find(table_name);
    if (refused != refused_.end()) {
        *error     = refused->second;
        *permanent = true;
        return false;
    }

    if (!watermark_table_ready_) {
        // Recovery restores the table and its schema when they existed; this
        // covers a fresh store.
        database.CreateTable(kWatermarkTable);
        if (!database.InstallPaxSchema(kWatermarkTable, kWatermarkFields)) {
            *error     = "the hidden key table has no schema";
            *permanent = true;
            return false;
        }
        watermark_table_ready_ = true;
    }

    const auto stored = database.Read(kWatermarkTable, table_name);
    uint64_t next = 0;
    if (stored.found && !unpack_watermark(stored.value, &next)) {
        *error = "the hidden key watermark of " + table_name + " is not a counter";
        refused_[table_name] = *error;
        *permanent           = true;
        return false;
    }
    if (next > std::numeric_limits<uint64_t>::max() - count) {
        *error = "the hidden key space of " + table_name + " is exhausted";
        refused_[table_name] = *error;
        *permanent           = true;
        return false;
    }

    // Committed Async under the mutex: the boot token, not durability, ties
    // the range to this run.
    helios::storage::silo::Transaction tx(database);
    tx.Read(kWatermarkTable, table_name, helios::storage::Tidword(stored.tid));
    std::string reason;
    const std::string row = pack_watermark(next + count);
    if (!tx.Write(kWatermarkTable, table_name, row,
                  stored.found ? helios::storage::RowOp::kUpdate
                               : helios::storage::RowOp::kInsert,
                  reason) ||
        !tx.Commit(helios::storage::CommitDurability::kAsync, reason)) {
        *error = "could not reserve hidden keys for " + table_name + ": " + reason;
        return false;
    }

    if (announced_.insert(table_name).second) {
        SPDLOG_INFO("Hidden keys for '{}' resume at {}", table_name.c_str(),
                 static_cast<unsigned long long>(next));
    }
    *first_id = next;
    return true;
}

void HeliosRpc::handleDbAllocateHiddenKeys(const std::string& message,
                                              std::string& result) {
    Helios::Protocol::DbAllocateHiddenKeys::Request request;
    Helios::Protocol::DbAllocateHiddenKeys::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_permanent(true);
        response.set_error("malformed request");
        SPDLOG_ERROR("AllocateHiddenKeys: malformed request");
        result = response.SerializeAsString();
        return;
    }

    uint64_t first_id = 0;
    std::string error;
    bool permanent = false;
    const bool ok  = hidden_keys_->Allocate(*db_manager_->get_database(),
                                            request.table_name(),
                                            request.count(), &first_id, &error,
                                            &permanent);
    response.set_ok(ok);
    response.set_boot_token(storage_boot_token());
    if (ok) {
        response.set_first_id(first_id);
    } else {
        response.set_permanent(permanent);
        response.set_error(error);
        SPDLOG_ERROR("AllocateHiddenKeys for '{}': {}",
                  request.table_name().c_str(), error.c_str());
    }

    result = response.SerializeAsString();
}

void HeliosRpc::handleDbSetCommitDurability(const std::string& message,
                                               std::string& result) {
    Helios::Protocol::DbSetCommitDurability::Request request;
    Helios::Protocol::DbSetCommitDurability::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_mode(to_wire_mode(db_manager_->commit_durability()));
        response.set_error("malformed request");
        SPDLOG_ERROR("SetCommitDurability: malformed request");
        result = response.SerializeAsString();
        return;
    }

    helios::storage::CommitDurability mode;
    switch (request.mode()) {
        case DurabilityRpc::ASYNC:
            mode = helios::storage::CommitDurability::kAsync;
            break;
        case DurabilityRpc::SYNC:
            mode = helios::storage::CommitDurability::kSync;
            break;
        default:
            response.set_ok(false);
            response.set_mode(to_wire_mode(db_manager_->commit_durability()));
            response.set_error("commit durability mode cannot be requested");
            SPDLOG_ERROR("SetCommitDurability: unrequestable mode");
            result = response.SerializeAsString();
            return;
    }

    db_manager_->set_commit_durability(mode);
    response.set_ok(true);
    response.set_mode(to_wire_mode(mode));

    SPDLOG_INFO("Commit durability switched to {}", durability_name(mode));

    result = response.SerializeAsString();
}

void HeliosRpc::handleDbCreateTable(const std::string& message,
                                       std::string& result) {
    SPDLOG_DEBUG("Handling DbCreateTable");

    Helios::Protocol::DbCreateTable::Request request;
    Helios::Protocol::DbCreateTable::Response response;

    request.ParseFromString(message);

    // A table another query node created is the same table; its schema
    // install answers whether the definition matches the one in place.
    auto db = db_manager_->get_database();
    const bool created =
        db->CreateTable(request.table_name()) || db->HasTable(request.table_name());
    hidden_keys_->ForgetTable(request.table_name());

    bool installed = false;
    if (created) {
        std::vector<uint32_t> widths;
        widths.reserve(request.pax_field_max_bytes_size());
        for (const uint32_t width : request.pax_field_max_bytes()) {
            widths.push_back(width);
        }

        // Every schema is typed: the request carries one kind per width.
        std::vector<helios::storage::pax::FieldType> types;
        std::vector<int8_t> scales;
        {
            types.reserve(request.pax_field_kind_size());
            for (const uint32_t kind : request.pax_field_kind()) {
                types.push_back(
                    static_cast<helios::storage::pax::FieldType>(kind));
            }
            if (request.pax_field_scale_size() ==
                request.pax_field_max_bytes_size()) {
                scales.reserve(request.pax_field_scale_size());
                for (const int32_t scale : request.pax_field_scale()) {
                    scales.push_back(static_cast<int8_t>(scale));
                }
            }
        }

        installed = db->InstallPaxSchema(request.table_name(), widths, types,
                                         scales);
        SPDLOG_INFO("PAX schema for '{}': {} fields, typed={}, {}",
                 request.table_name().c_str(), widths.size(),
                 types.empty() ? "no" : "yes",
                 installed ? "installed" : "refused");
    }
    response.set_success(created && installed);
    SPDLOG_DEBUG("CreateTable '{}': {}", request.table_name().c_str(),
              response.success() ? "success" : "refused");

    result = response.SerializeAsString();
}

void HeliosRpc::handleDbCreateSecondaryIndex(const std::string& message,
                                                std::string& result) {
    SPDLOG_DEBUG("Handling DbCreateSecondaryIndex");

    Helios::Protocol::DbCreateSecondaryIndex::Request request;
    Helios::Protocol::DbCreateSecondaryIndex::Response response;

    request.ParseFromString(message);

    const bool success = db_manager_->get_database()->CreateSecondaryIndex(
        request.table_name(), request.index_name(),
        static_cast<helios::storage::IndexConstraint>(request.index_type()));
    response.set_success(success);

    result = response.SerializeAsString();
}
