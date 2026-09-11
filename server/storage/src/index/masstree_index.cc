/**
 * @file server/storage/src/index/masstree_index.cc
 * The masstree-beta instantiation: the only translation unit that includes
 * masstree headers, and the RCU handshake its readers require.
 */

#include "index/masstree_index.h"

#include <atomic>
#include <cstring>
#include <mutex>
#include <utility>

#include "util/debug_sync.h"

// Masstree headers. They come after the header guard so no other storage
// source sees them; masstree's include path is PRIVATE to storage and is
// unreachable from the public headers and the tests. config.h defines the
// macros compiler.hh reads, so this block is not sortable.
// clang-format off
#include "config.h"
#include "compiler.hh"
#include "kvthread.hh"
#include "masstree.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_tcursor.hh"
#include "string.hh"
// clang-format on

// Globals masstree-beta requires: kvthread.hh declares them extern, so
// exactly one translation unit must define them, with the types declared
// there. MasstreeAdvanceEpoch moves globalepoch and recomputes active_epoch,
// once per storage epoch from the epoch hook; between those ticks nodeversion
// bits alone serialize tree access. `recovering` stays false: this wrapper
// has no masstree-level recovery.
relaxed_atomic<mrcu_epoch_type> globalepoch{1};
relaxed_atomic<mrcu_epoch_type> active_epoch{1};
volatile bool recovering = false;

namespace helios::storage {
namespace index {

namespace {

class key_unparse_unsigned {
 public:
  static int unparse_key(Masstree::key<std::uint64_t> key, char *buf,
                         int buflen) {
    return snprintf(buf, buflen, "%" PRIu64, key.ikey());
  }
};

struct table_params : public Masstree::nodeparams<15, 15> {
  using value_type = DataItem *;
  using value_print_type = Masstree::value_print<value_type>;
  using threadinfo_type = threadinfo;
  using key_unparse_type = key_unparse_unsigned;
  static constexpr ssize_t print_max_indent_depth = 12;
};

using table_type = Masstree::basic_table<table_params>;
using unlocked_cursor_type = Masstree::unlocked_tcursor<table_params>;
using cursor_type = Masstree::tcursor<table_params>;

thread_local threadinfo *tls_ti = nullptr;
// True while this thread has called rcu_start without a matching rcu_stop.
// Re-stamping gc_epoch_ on every op would advance the thread past the epoch
// that protects a raw DataItem * an earlier Get returned, so the thread stays
// at the epoch of its first access until it releases at a safe boundary.
thread_local bool tls_enrolled = false;
std::atomic<int> next_thread_id{0};
std::mutex thread_init_mutex;

// threadinfo::make prepends to masstree's allthreads list without
// synchronization, so the first use on each thread is serialized here.
// Registration does not enrol the thread in the RCU epoch (that is
// ensure_thread_active's job), which lets a thread between operations leave
// gc_epoch_ at 0 and not pin min_active_epoch.
inline void ensure_thread_init() {
  if (__builtin_expect(tls_ti == nullptr, 0)) {
    std::lock_guard<std::mutex> lg(thread_init_mutex);
    if (tls_ti == nullptr) {
      tls_ti = threadinfo::make(
          threadinfo::TI_PROCESS,
          next_thread_id.fetch_add(1, std::memory_order_relaxed));
    }
  }
}

// Registers the thread and enrols it at the current masstree epoch, unless
// it is enrolled already: a caller may still hold a raw DataItem * an earlier
// Get returned, and re-enrolling mid-transaction would let RCU reclaim it.
// The enrolment ends at MasstreeReleaseThreadEpoch.
inline void ensure_thread_active() {
  ensure_thread_init();
  if (!tls_enrolled) {
    tls_ti->rcu_start();
    tls_enrolled = true;
  }
}

/**
 * @brief Deletes one DataItem once RCU says no reader can reach it.
 *
 * @details Allocated through threadinfo::allocate so it lives in masstree's
 * accounting pool, and frees itself after deleting the item, the same shape
 * as masstree's own gc_layer_rcu_callback.
 */
struct RcuFreeCallback : public threadinfo::mrcu_callback {
  DataItem *item;
  explicit RcuFreeCallback(DataItem *it) : item(it) {}
  void operator()(threadinfo &ti) override {
    delete item;
    ti.deallocate(this, sizeof(RcuFreeCallback), memtag_masstree_gc);
  }
};

inline void RcuFree(DataItem *item) {
  if (item == nullptr) return;
  void *mem = tls_ti->allocate(sizeof(RcuFreeCallback), memtag_masstree_gc);
  auto *operation = new (mem) RcuFreeCallback(item);
  tls_ti->rcu_register(operation);
}

// Three-way comparison of a range bound with a scanned key, memcmp style.
inline int KeyCmp(const char *bound, size_t bound_len, Masstree::Str key) {
  const size_t key_len = static_cast<size_t>(key.len);
  const int cmp = std::memcmp(bound, key.s, std::min(bound_len, key_len));
  if (cmp != 0) return cmp;
  if (bound_len == key_len) return 0;
  return bound_len > key_len ? 1 : -1;
}

/**
 * @brief Drives masstree's forward and reverse scan into the callback shape
 *        used here, where returning `true` cancels the scan.
 */
struct ScanAdapter {
  const char *begin_ptr;
  size_t begin_len;
  bool has_begin;
  const char *end_ptr;
  size_t end_len;
  bool has_end;
  std::function<bool(std::string_view)> operation;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS &, const K &, threadinfo &) {}

  // Returns true to keep scanning, false to stop (masstree convention).
  bool visit_value(Masstree::Str key, DataItem * /*val*/, threadinfo &) {
    if (has_end && KeyCmp(end_ptr, end_len, key) <= 0) return false;
    if (has_begin && KeyCmp(begin_ptr, begin_len, key) > 0) return false;
    ++count;
    if (operation(std::string_view(key.s, key.len)))
      return false;  // caller cancel
    return true;
  }
};

// Same walk as ScanAdapter, with the DataItem handed to the callback.
struct ScanValueAdapter {
  const char *begin_ptr;
  size_t begin_len;
  bool has_begin;
  const char *end_ptr;
  size_t end_len;
  bool has_end;
  std::function<bool(std::string_view, DataItem &)> operation;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS &, const K &, threadinfo &) {}

  bool visit_value(Masstree::Str key, DataItem *val, threadinfo &) {
    if (has_end && KeyCmp(end_ptr, end_len, key) <= 0) return false;
    if (has_begin && KeyCmp(begin_ptr, begin_len, key) > 0) return false;
    ++count;
    if (operation(std::string_view(key.s, key.len), *val)) return false;
    return true;
  }
};

}  // namespace

struct MasstreeIndex::Impl {
  table_type table_;

  Impl() {
    // Enrol briefly so table_.initialize can allocate the root node through
    // the threadinfo's RCU-aware allocator, then leave the epoch so the
    // construction thread does not pin min_active_epoch at the initial
    // value forever. If the same thread later does masstree ops, each op
    // path goes through ensure_thread_active and re-enrols.
    //
    // A caller already inside an RCU critical section keeps its enrolment:
    // rcu_stop() would clear gc_epoch_ while raw DataItem* pointers it still
    // holds could be scheduled for RCU free by another thread's physical
    // delete. The existing rcu_start covers the initialize.
    ensure_thread_init();
    const bool was_enrolled = tls_enrolled;
    if (!was_enrolled) {
      tls_ti->rcu_start();
    }
    table_.initialize(*tls_ti);
    if (!was_enrolled) {
      tls_ti->rcu_stop();
    }
  }

  ~Impl() {
    // table_.destroy() is not called: it only registers an RCU callback, and
    // nothing drains this thread's limbo afterwards. The live tree and the
    // DataItem pointers it holds are left to the process exit.
  }

  DataItem *Get(std::string_view key) {
    ensure_thread_active();
    unlocked_cursor_type lp(table_, key.data(), key.size());
    if (lp.find_unlocked(*tls_ti)) return lp.value();
    return nullptr;
  }

  // Absent rows created after SetPaxTable are initialized in PAX mode so their
  // first committed payload scatters into the table's strips.
  pax::PaxTable *pax_table_ = nullptr;
  DataItem *NewAbsentItem() {
    auto *item = new DataItem();
    if (pax_table_ != nullptr) item->buffer.InitPaxAbsent(pax_table_);
    return item;
  }

  // Upsert with a freshly-allocated DataItem.
  void Put(std::string_view key, DataItem &&value) {
    ensure_thread_active();
    auto *fresh = new DataItem(std::move(value));
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    // On an overwrite the previous DataItem is leaked: a concurrent reader
    // may still hold the raw pointer Get() returned.
    lp.value() = fresh;  // FIXME: retire the old item through RCU
    fence();
    // 1 == structural insert (bumps the leaf's vinsert counter), 0 == in-place
    // overwrite. Claiming an insert on overwrite would falsely trigger phantom
    // retries on concurrent scanners watching this leaf.
    lp.finish(found ? 0 : 1, *tls_ti);
  }

  // Structural removal of a committed delete. Invoked by the deferred reaper
  // after it has CAS-locked the DataItem and verified the tombstone TID.
  // Returns true if the key was physically removed; false if the slot is
  // already gone or a racing replacement won the position.
  bool Purge(std::string_view key, DataItem *expected,
             TransactionId retired_tid) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    const bool found = lp.find_locked(*tls_ti);
    if (!found) {
      lp.finish(0, *tls_ti);
      return false;
    }
    DataItem *current = lp.value();
    if (expected != nullptr && current != expected) {
      // Another writer replaced the slot between the commit-time decision
      // and this erase. Leave the new occupant alone.
      lp.finish(0, *tls_ti);
      return false;
    }
    // A reader that resolved this entry parks on its lock bit until the
    // retired TID is published below.
    HELIOS_DEBUG_SYNC("reaper.purge_locked_window");
    // finish(-1) calls finish_remove which removes the permutation slot and
    // RCU-frees the leaf if it becomes empty. The DataItem* itself rides on
    // a separate RCU callback below.
    lp.finish(-1, *tls_ti);
    if (!retired_tid.IsEmpty()) {
      current->transaction_id.store(retired_tid);
    }
    RcuFree(current);
    return true;
  }

  // Idempotent absent insert: never removes, just ensures a slot exists.
  void PutAbsent(std::string_view key) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (!found) {
      lp.value() = NewAbsentItem();
    }
    fence();
    lp.finish(found ? 0 : 1, *tls_ti);
  }

  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view)> op) {
    ensure_thread_active();
    ScanAdapter adapter{nullptr,
                        0,
                        false,
                        end.has_value() ? end->data() : nullptr,
                        end.has_value() ? end->size() : 0,
                        end.has_value(),
                        std::move(op),
                        0};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, adapter, *tls_ti);
    return adapter.count;
  }

  size_t Scan(std::string_view begin, std::string_view end,
              std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    ScanValueAdapter adapter{nullptr,    0,    false,        end.data(),
                             end.size(), true, std::move(op)};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, adapter, *tls_ti);
    return adapter.count;
  }

  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view)> op) {
    ensure_thread_active();
    // Reverse scan walks downward from `end - 1`, stopping once key < begin.
    // Range is [begin, end) just like forward Scan.
    ScanAdapter adapter{begin.data(), begin.size(), true, nullptr, 0,
                        false,        std::move(op)};
    if (end.has_value()) {
      Masstree::Str firstkey(end->data(), end->size());
      table_.rscan(firstkey, /*emit_firstkey=*/false, adapter, *tls_ti);
    } else {
      // rscan from infinity: use an empty firstkey with emit=true to
      // walk the tail backwards.
      table_.rscan(Masstree::Str(), /*emit_firstkey=*/true, adapter, *tls_ti);
    }
    return adapter.count;
  }

  size_t ScanReverse(std::string_view begin, std::string_view end,
                     std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    ScanValueAdapter adapter{begin.data(), begin.size(), true, nullptr, 0,
                             false,        std::move(op)};
    Masstree::Str firstkey(end.data(), end.size());
    table_.rscan(firstkey, /*emit_firstkey=*/false, adapter, *tls_ti);
    return adapter.count;
  }

  void ForEach(std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    ScanValueAdapter adapter{nullptr, 0,     false,        nullptr,
                             0,       false, std::move(op)};
    table_.scan(Masstree::Str(), /*emit_firstkey=*/true, adapter, *tls_ti);
  }
};

MasstreeIndex::MasstreeIndex() : impl_(std::make_unique<Impl>()) {}

MasstreeIndex::~MasstreeIndex() = default;

void MasstreeIndex::SetPaxTable(pax::PaxTable *store) {
  impl_->pax_table_ = store;
}

DataItem *MasstreeIndex::Get(std::string_view key) { return impl_->Get(key); }

void MasstreeIndex::Put(std::string_view key, DataItem &&value) {
  impl_->Put(key, std::move(value));
}

void MasstreeIndex::PutAbsent(std::string_view key) { impl_->PutAbsent(key); }

size_t MasstreeIndex::Scan(std::string_view begin,
                           std::optional<std::string_view> end,
                           std::function<bool(std::string_view)> operation) {
  return impl_->Scan(begin, end, std::move(operation));
}

size_t MasstreeIndex::Scan(
    std::string_view begin, std::string_view end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return impl_->Scan(begin, end, std::move(operation));
}

size_t MasstreeIndex::ScanReverse(
    std::string_view begin, std::optional<std::string_view> end,
    std::function<bool(std::string_view)> operation) {
  return impl_->ScanReverse(begin, end, std::move(operation));
}

size_t MasstreeIndex::ScanReverse(
    std::string_view begin, std::string_view end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return impl_->ScanReverse(begin, end, std::move(operation));
}

void MasstreeIndex::ForEach(
    std::function<bool(std::string_view, DataItem &)> operation) {
  impl_->ForEach(std::move(operation));
}

bool MasstreeIndex::Purge(std::string_view key, DataItem *expected,
                          TransactionId retired_tid) {
  return impl_->Purge(key, expected, retired_tid);
}

void MasstreeAdvanceEpoch() {
  // Bump masstree's global epoch and recompute active_epoch. The mutex
  // serialises with ensure_thread_init's threadinfo::make, which prepends to
  // the unsynchronised allthreads list that min_active_epoch walks.
  std::lock_guard<std::mutex> lg(thread_init_mutex);
  globalepoch.store(globalepoch.load() + 1);
  active_epoch.store(threadinfo::min_active_epoch());
}

void MasstreeReleaseThreadEpoch() {
  // One rcu_stop pass: it frees what active_epoch already covers and leaves
  // the rest of this thread's limbo for its next release.
  if (tls_ti == nullptr) return;
  tls_ti->rcu_stop();
  tls_enrolled = false;
}

}  // namespace index
}  // namespace helios::storage
