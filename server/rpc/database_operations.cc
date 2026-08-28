#include "lineairdb_rpc.hh"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
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

namespace {

// Holds one next-unallocated id per table, keyed by table name. MySQL table
// names are paths and always begin with "./", so no user table can land here.
constexpr char kWatermarkTable[] = "__helios_hidden_keys";

// A concurrent writer can invalidate the publishing commit; retry before
// turning that into a statement error.
constexpr int kReserveAttempts = 8;

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

// One reservation attempt, shared with the pool thread that runs it. Held by
// shared_ptr so the transaction task can outlive the caller's frame.
struct Reservation {
    std::string table_name;
    uint32_t count = 0;

    std::mutex mutex;
    std::condition_variable decided;
    bool done      = false;
    bool committed = false;
    bool permanent = false;
    // A refusal that belongs to the table, not to this attempt
    bool refusal_is_permanent_for_table = false;
    uint64_t first_id = 0;
    std::string error;
};

// The reservation is one small OCC transaction on an idle pool; a wait this
// long means something is wrong, and holding the allocator mutex for it would
// stall every other table too.
constexpr auto kReservationDeadline = std::chrono::seconds(60);

void run_reservation(LineairDB::Transaction& tx, Reservation& reservation) {
    if (!tx.SetTable(kWatermarkTable)) {
        reservation.error     = "the hidden key table is missing";
        reservation.permanent = true;
        tx.Abort();
        return;
    }

    // The grant comes from what this transaction read, never from a value
    // captured earlier. The allocator mutex and max_thread=1 serialise
    // reservations; once the row exists the concurrency control backs that up.
    const auto stored = tx.Read<uint64_t>(reservation.table_name);
    const uint64_t next = stored.has_value() ? stored.value() : 0;

    if (next > std::numeric_limits<uint64_t>::max() - reservation.count) {
        reservation.error = "the hidden key space of " + reservation.table_name +
                            " is exhausted";
        reservation.permanent                      = true;
        reservation.refusal_is_permanent_for_table = true;
        tx.Abort();
        return;
    }

    reservation.first_id = next;
    tx.Write<uint64_t>(reservation.table_name, next + reservation.count);
}

using DurabilityRpc = LineairDB::Protocol::DbSetCommitDurability;

// Bound on the durable barrier; the wait holds the switch mutex and a closed
// socket does not cancel it
constexpr auto kBarrierTimeout = std::chrono::hours(1);

const char* durability_name(LineairDB::Config::CommitDurability mode) {
    switch (mode) {
        case LineairDB::Config::CommitDurability::Volatile:
            return "VOLATILE";
        case LineairDB::Config::CommitDurability::Async:
            return "ASYNC";
        case LineairDB::Config::CommitDurability::Sync:
            return "SYNC";
    }
    return "UNKNOWN";
}

DurabilityRpc::Mode to_wire_mode(LineairDB::Config::CommitDurability mode) {
    switch (mode) {
        case LineairDB::Config::CommitDurability::Async:
            return DurabilityRpc::ASYNC;
        case LineairDB::Config::CommitDurability::Sync:
            return DurabilityRpc::SYNC;
        case LineairDB::Config::CommitDurability::Volatile:
            return DurabilityRpc::VOLATILE;
    }
    return DurabilityRpc::MODE_UNSPECIFIED;
}

}  // namespace

void HiddenKeyAllocator::ForgetTable(const std::string& table_name) {
    std::lock_guard<std::mutex> serialized(mutex_);
    refused_.erase(table_name);
    announced_.erase(table_name);
}

bool HiddenKeyAllocator::Allocate(LineairDB::Database& database,
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
        // Recovery restores it when it already existed; this covers a fresh one
        database.CreateTable(kWatermarkTable);
        watermark_table_ready_ = true;
    }

    for (int attempt = 0; attempt < kReserveAttempts; ++attempt) {
        auto reservation        = std::make_shared<Reservation>();
        reservation->table_name = table_name;
        reservation->count      = count;

        auto* db = &database;
        database.ExecuteTransaction(
            [reservation](LineairDB::Transaction& tx) {
                run_reservation(tx, *reservation);
            },
            // Runs on the pool thread with nothing of this transaction in
            // flight, the only safe point to hand masstree's RCU epoch back;
            // nothing else un-enrols this thread, so leaves would never reclaim
            [db](LineairDB::TxStatus) { db->ReleaseMasstreeThreadEpoch(); },
            // Precommit is the decision to act on. Its log record rides the
            // epoch this commits in, which closes no later than the epoch of
            // the first row written under the range.
            [reservation](LineairDB::TxStatus status) {
                {
                    std::lock_guard<std::mutex> lock(reservation->mutex);
                    reservation->committed =
                        status == LineairDB::TxStatus::Committed;
                    reservation->done = true;
                }
                reservation->decided.notify_one();
            });

        bool decided = false;
        {
            std::unique_lock<std::mutex> lock(reservation->mutex);
            decided = reservation->decided.wait_for(
                lock, kReservationDeadline,
                [&reservation]() { return reservation->done; });
        }
        if (!decided) {
            // The attempt may still commit; harmless, since the next one reads
            // the watermark it leaves behind rather than anything cached here.
            *error = "the storage server did not answer a hidden key "
                     "reservation for " + table_name;
            return false;
        }

        if (reservation->permanent) {
            if (reservation->refusal_is_permanent_for_table) {
                refused_[table_name] = reservation->error;
            }
            *error     = reservation->error;
            *permanent = true;
            return false;
        }
        if (!reservation->committed) continue;

        if (announced_.insert(table_name).second) {
            LOG_INFO("Hidden keys for '%s' resume at %llu", table_name.c_str(),
                     static_cast<unsigned long long>(reservation->first_id));
        }
        *first_id = reservation->first_id;
        return true;
    }

    *error = "could not reserve hidden keys for " + table_name;
    return false;
}

void LineairDBRpc::handleDbAllocateHiddenKeys(const std::string& message,
                                              std::string& result) {
    LineairDB::Protocol::DbAllocateHiddenKeys::Request request;
    LineairDB::Protocol::DbAllocateHiddenKeys::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_permanent(true);
        response.set_error("malformed request");
        LOG_ERROR("AllocateHiddenKeys: malformed request");
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
        LOG_ERROR("AllocateHiddenKeys for '%s': %s",
                  request.table_name().c_str(), error.c_str());
    }

    result = response.SerializeAsString();
}

void LineairDBRpc::handleDbSetCommitDurability(const std::string& message,
                                               std::string& result) {
    LineairDB::Protocol::DbSetCommitDurability::Request request;
    LineairDB::Protocol::DbSetCommitDurability::Response response;

    if (!request.ParseFromString(message)) {
        response.set_ok(false);
        response.set_mode(
            to_wire_mode(db_manager_->get_database()->GetCommitDurability()));
        response.set_error("malformed request");
        LOG_ERROR("SetCommitDurability: malformed request");
        result = response.SerializeAsString();
        return;
    }

    LineairDB::Config::CommitDurability mode;
    switch (request.mode()) {
        case DurabilityRpc::ASYNC:
            mode = LineairDB::Config::CommitDurability::Async;
            break;
        case DurabilityRpc::SYNC:
            mode = LineairDB::Config::CommitDurability::Sync;
            break;
        default:
            response.set_ok(false);
            response.set_mode(to_wire_mode(
                db_manager_->get_database()->GetCommitDurability()));
            // VOLATILE lands here too: it is reportable, not requestable
            response.set_error("commit durability mode cannot be requested");
            LOG_ERROR("SetCommitDurability: unrequestable mode");
            result = response.SerializeAsString();
            return;
    }

    const bool ok =
        db_manager_->get_database()->SetCommitDurability(mode, kBarrierTimeout);

    const auto effective = db_manager_->get_database()->GetCommitDurability();
    response.set_ok(ok);
    response.set_mode(to_wire_mode(effective));
    if (!ok) {
        if (effective == LineairDB::Config::CommitDurability::Volatile) {
            response.set_error(
                "the database is volatile; commit durability is fixed at "
                "startup");
        } else {
            response.set_error(
                std::string("commit durability switch not confirmed; policy "
                            "in force: ") +
                durability_name(effective));
        }
    }

    LOG_INFO("SetCommitDurability requested=%s result=%s effective=%s",
             durability_name(mode), ok ? "ok" : "failed",
             durability_name(effective));

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
    hidden_keys_->ForgetTable(request.table_name());
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
