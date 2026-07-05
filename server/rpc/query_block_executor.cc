#include "query_block_executor.hh"

#include <exception>

namespace query_block {
namespace {

void fail(LineairDB::Protocol::TxExecuteQueryBlock::Response* response,
          const char* message) {
    if (response == nullptr) return;
    response->set_ok(false);
    response->set_error(message);
}

}  // namespace

void ExecuteQueryBlock(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteQueryBlock::Request& request,
    LineairDB::Protocol::TxExecuteQueryBlock::Response* response) {
    (void)db;
    (void)request;

    try {
        fail(response, "query-block execution is not implemented");
    } catch (const std::exception& e) {
        fail(response, e.what());
    } catch (...) {
        fail(response, "query-block execution failed");
    }
}

}  // namespace query_block
