#include "storage_server.hh"

#include "common/rpc_flat.h"
#include "server_config.hh"
#include <spdlog/spdlog.h>
#include "helios.pb.h"
#include "rpc/helios_rpc.hh"

#include <google/protobuf/io/coded_stream.h>

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <iostream>
#include <iterator>
#include <mutex>
#include <string_view>
#include <thread>
#include <vector>

using helios::storage::EpochNumber;
using Helios::Protocol::Request;

// A commit reply held until its epoch is durable.
struct DeferredAck {
    EpochNumber epoch;
    std::shared_ptr<Connection> conn;
    uint64_t id;
    std::string reply;
    std::string payload;
};

struct AckQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<DeferredAck> acks;
};

// A request that runs on the worker pool rather than on its connection's
// thread.
struct Job {
    std::shared_ptr<Connection> conn;
    uint64_t id;
    std::string request;
};

struct JobQueue {
    std::mutex mutex;
    std::condition_variable cv;
    std::deque<Job> jobs;
};

namespace {

// Moves the acks swapped out of the queue to the end of `held`.
void hold(std::vector<DeferredAck>& incoming, std::vector<DeferredAck>& held) {
    held.insert(held.end(), std::make_move_iterator(incoming.begin()),
                std::make_move_iterator(incoming.end()));
    incoming.clear();
}

// The ack thread: queues each durable commit reply and wakes its connection.
void deliver_acks(std::shared_ptr<AckQueue> queue,
                  std::shared_ptr<helios::storage::Database> db) {
    EpochNumber durable = 0;
    // The queue is only swapped with `incoming` under its mutex; everything
    // else works on vectors this thread owns.
    std::vector<DeferredAck> incoming;
    std::vector<DeferredAck> held;
    std::vector<DeferredAck> ready;
    for (;;) {
        {
            std::unique_lock<std::mutex> lock(queue->mutex);
            queue->cv.wait(lock, [&] {
                return !held.empty() || !queue->acks.empty();
            });
            incoming.swap(queue->acks);
        }
        hold(incoming, held);
        // Wake at the next durable epoch even when the oldest pending one is
        // later, so an ack queued behind a later one is not held for it.
        EpochNumber target = durable + 1;
        for (const auto& ack : held) {
            target = std::min(target, ack.epoch);
        }
        durable = db->await_durable(target);

        // Acks queued during the wait may already be durable.
        {
            std::lock_guard<std::mutex> lock(queue->mutex);
            incoming.swap(queue->acks);
        }
        hold(incoming, held);
        auto later = std::partition(
            held.begin(), held.end(),
            [&](const DeferredAck& ack) { return ack.epoch > durable; });
        ready.assign(std::make_move_iterator(later),
                     std::make_move_iterator(held.end()));
        held.erase(later, held.end());
        // The connection's own thread sends, so a slow peer holds up only
        // the acks on its connection.
        for (auto& ack : ready) {
            if (ack.conn->append(ack.id, ack.reply, ack.payload)) {
                ack.conn->wake();
            }
        }
        ready.clear();
    }
}

// A worker: runs queued requests on a HeliosRpc of its own.
void run_jobs(std::shared_ptr<JobQueue> queue,
              std::shared_ptr<DatabaseManager> db_manager,
              std::shared_ptr<TableRowCounts> row_counts,
              std::shared_ptr<HiddenKeyAllocator> hidden_keys) {
    HeliosRpc rpc(db_manager, row_counts, hidden_keys);
    for (;;) {
        Job job;
        {
            std::unique_lock<std::mutex> lock(queue->mutex);
            queue->cv.wait(lock, [&] { return !queue->jobs.empty(); });
            job = std::move(queue->jobs.front());
            queue->jobs.pop_front();
        }
        std::string reply, payload;
        EpochNumber await_epoch;  // 0: no arm run here commits
        if (!rpc.handle_rpc(job.request, {}, reply, payload, await_epoch)) {
            job.conn->shutdown();  // the client must not wait for this reply
            continue;
        }
        if (job.conn->append(job.id, reply, payload)) job.conn->wake();
    }
}

}  // namespace

StorageServer::StorageServer() : TcpServer(config().server_port) {}

void StorageServer::init() {
    // Initialize components in dependency order
    if (!db_manager_) {
        db_manager_ = std::make_shared<DatabaseManager>();
    }
    // Commits only move the counts, so a restart starts them from the rows
    // recovery restored; otherwise the optimizer sees near-empty tables.
    const auto& recovered = db_manager_->get_database()->recovered_row_counts();
    row_counts_->seed(recovered);
    SPDLOG_INFO("Row counts seeded for {} tables", recovered.size());

    acks_ = std::make_shared<AckQueue>();
    jobs_ = std::make_shared<JobQueue>();
    std::thread(deliver_acks, acks_, db_manager_->get_database()).detach();
    const unsigned workers = std::max(2u, hardware_threads / 4);
    for (unsigned i = 0; i < workers; ++i) {
        std::thread(run_jobs, jobs_, db_manager_, row_counts_, hidden_keys_)
            .detach();
    }

    SPDLOG_INFO("Storage server initialized on port {}, boot token {}",
             static_cast<unsigned>(config().server_port), hidden_keys_->boot_token);
}

void StorageServer::handle_client(int client_socket) {
    SPDLOG_INFO("Handling client connection fd={}", client_socket);
    auto rpc_handler =
        std::make_shared<HeliosRpc>(db_manager_, row_counts_, hidden_keys_);
    auto conn = std::make_shared<Connection>(client_socket);
    FrameReader reader(*conn);

    uint64_t id;
    std::string_view request, request_payload;
    while (reader.next(id, request, request_payload)) {
        // Stats, DuckDB, CREATE TABLE, CREATE INDEX and durability-mode
        // requests run on the worker pool; an envelope sets one field, so its
        // first tag names the arm. A one-byte envelope is flat and runs here.
        bool pooled = false;
        if (request.size() != 1) {
            google::protobuf::io::CodedInputStream in(
                reinterpret_cast<const uint8_t*>(request.data()),
                static_cast<int>(request.size()));
            switch (in.ReadTag() >> 3) {
                case Request::kGetTableStatsFieldNumber:
                case Request::kTxExecuteDuckdbQueryFieldNumber:
                case Request::kDbCreateTableFieldNumber:
                case Request::kDbCreateSecondaryIndexFieldNumber:
                case Request::kDbSetCommitDurabilityFieldNumber:
                    pooled = true;
                    break;
                default:
                    break;
            }
        }

        if (pooled) {
            std::lock_guard<std::mutex> lock(jobs_->mutex);
            jobs_->jobs.push_back({conn, id, std::string(request)});
            jobs_->cv.notify_one();
        } else {
            // A reply already built must not wait behind a longer request.
            if (request.size() != 1 ||
                static_cast<helios::rpc::FlatOp>(request[0]) ==
                    helios::rpc::FlatOp::kCommit) {
                conn->flush();
            }
            std::string reply, reply_payload;
            EpochNumber await_epoch;
            if (!rpc_handler->handle_rpc(request, request_payload, reply,
                                         reply_payload, await_epoch)) {
                break;  // Nothing to answer with
            }
            if (await_epoch != 0) {
                std::lock_guard<std::mutex> lock(acks_->mutex);
                acks_->acks.push_back({await_epoch, conn, id, std::move(reply),
                                       std::move(reply_payload)});
                // The ack thread sleeps on the queue only while it is empty.
                if (acks_->acks.size() == 1) acks_->cv.notify_one();
            } else {
                conn->append(id, reply, reply_payload);
            }
        }
        // One send carries every reply queued before the next read waits.
        if (!reader.buffered_frame()) conn->flush();
    }
    // Replies still owed to this client are dropped.
    conn->shutdown();
}
