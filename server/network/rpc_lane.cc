#include "rpc_lane.hh"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "lineairdb.pb.h"

namespace {

// TX_VALIDATE_AND_COMMIT admission caps (frozen; do not retune ad hoc).
constexpr size_t kCommitMaxPayloadBytes = 32 * 1024;  // frame cap, checked before parse
constexpr int kCommitMaxRowDeltas = 128;              // row_deltas_size() cap

// Work-size bins for commit admission: entries and frame KiB each round up
// to the smallest bin that holds them; a value past the last bin rejects.
constexpr uint32_t kCommitEntriesBins[] = {128, 256, 384, 768};
constexpr size_t kCommitEntriesBinsCount =
    sizeof(kCommitEntriesBins) / sizeof(kCommitEntriesBins[0]);
constexpr uint32_t kCommitFrameKibBins[] = {1, 2, 4, 8, 16, 32};
constexpr size_t kCommitFrameKibBinsCount =
    sizeof(kCommitFrameKibBins) / sizeof(kCommitFrameKibBins[0]);

// Admitted (entries_bin, frame_kib_bin) pairs, exhaustive, no interpolation.
// Any other pair -> kSlow.
constexpr std::pair<uint32_t, uint32_t> kCommitAdmitPairs[] = {
    {128, 1}, {128, 2}, {128, 4}, {128, 8}, {128, 16},
    {256, 16},
    {384, 16}, {384, 32},
    {768, 32},
};
constexpr size_t kCommitAdmitPairsCount =
    sizeof(kCommitAdmitPairs) / sizeof(kCommitAdmitPairs[0]);

// TX_EXECUTE_READ_PLAN admission caps (frozen; do not retune ad hoc).
constexpr int kReadPlanMaxSteps = 48;             // steps_size() cap
constexpr uint64_t kReadPlanMaxScanLimit = 1024;  // per-step scan_limit cap when nonzero

// Rounds `value` up to the smallest entry in `bins`; a zero value, or a
// value past the last bin, returns 0 (caller rejects).
uint32_t ceil_to_bin(uint64_t value, const uint32_t *bins, size_t count) {
  if (value == 0) return 0;  // bin 0 is not in the admitted pairs
  for (size_t i = 0; i < count; i++) {
    if (value <= bins[i]) return bins[i];
  }
  return 0;
}

// Payload bytes rounded up to whole KiB.
uint64_t kib_ceil(size_t bytes) { return (bytes + 1023) / 1024; }

bool commit_admitted(uint32_t entries_bin, uint32_t frame_kib_bin) {
  for (size_t i = 0; i < kCommitAdmitPairsCount; i++) {
    if (kCommitAdmitPairs[i].first == entries_bin && kCommitAdmitPairs[i].second == frame_kib_bin) {
      return true;
    }
  }
  return false;
}

RpcLane classify_commit(std::string_view payload) {
  // Size check first: an over-cap commit must not pay the parse on the
  // reactor thread -- it is slow regardless of content.
  if (payload.size() > kCommitMaxPayloadBytes) return RpcLane::kSlow;
  LineairDB::Protocol::TxValidateAndCommit::Request req;
  if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    return RpcLane::kMalformed;
  }
  // Range reads and fenced commits are unconditionally slow: worst-case
  // re-scan cost cannot be bounded from the request; slow regardless of
  // the bins below.
  if (req.fence() || req.range_reads_size() != 0) return RpcLane::kSlow;
  if (req.row_deltas_size() > kCommitMaxRowDeltas) return RpcLane::kSlow;

  uint64_t entries = static_cast<uint64_t>(req.reads_size()) +
                      static_cast<uint64_t>(req.writes_size()) +
                      static_cast<uint64_t>(req.deletes_size()) +
                      static_cast<uint64_t>(req.secondary_index_ops_size()) +
                      static_cast<uint64_t>(req.range_reads_size()) +
                      static_cast<uint64_t>(req.row_deltas_size());
  uint32_t entries_bin = ceil_to_bin(entries, kCommitEntriesBins, kCommitEntriesBinsCount);
  uint32_t frame_kib_bin =
      ceil_to_bin(kib_ceil(payload.size()), kCommitFrameKibBins, kCommitFrameKibBinsCount);
  if (entries_bin == 0 || frame_kib_bin == 0) return RpcLane::kSlow;
  return commit_admitted(entries_bin, frame_kib_bin) ? RpcLane::kFast : RpcLane::kSlow;
}

RpcLane classify_read_plan(std::string_view payload) {
  LineairDB::Protocol::TxExecuteReadPlan::Request req;
  if (!req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    return RpcLane::kMalformed;
  }
  if (req.steps_size() > kReadPlanMaxSteps) return RpcLane::kSlow;
  for (const auto &step : req.steps()) {
    if (step.scan_limit() != 0 && step.scan_limit() > kReadPlanMaxScanLimit) return RpcLane::kSlow;
  }
  return RpcLane::kFast;
}

}  // namespace

RpcLane classify_rpc(MessageType type, std::string_view payload) {
  switch (type) {
    // kFast candidates, subject to the caps above.
    case MessageType::TX_VALIDATE_AND_COMMIT:
      return classify_commit(payload);
    case MessageType::TX_EXECUTE_READ_PLAN:
      return classify_read_plan(payload);

    // kSlow: heavy or unbounded single-shot operations; served by the
    // helper pool, with DDL/fence falling back to migration under overload.
    case MessageType::DB_FENCE:
    case MessageType::DB_CREATE_TABLE:
    case MessageType::DB_CREATE_SECONDARY_INDEX:
    case MessageType::TX_GET_TABLE_STATS:
    case MessageType::TX_EXECUTE_SQL_DUCKDB:
    case MessageType::TX_STATELESS_READ:
    case MessageType::TX_STATELESS_BATCH_READ:
      return RpcLane::kSlow;

    // kConv: every remaining defined opcode references cross-RPC transaction
    // state; unknown opcodes never reach here (rejected at header decode).
    default:
      return RpcLane::kConv;
  }
}
