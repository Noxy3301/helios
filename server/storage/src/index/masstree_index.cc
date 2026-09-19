/**
 * @file server/storage/src/index/masstree_index.cc
 * Masstree operations and RCU lifetime management.
 */

#include "index/masstree_index.h"

#include <array>
#include <atomic>
#include <mutex>
#include <utility>

#include "util/debug_sync.h"

// Keep this order: compiler.hh needs the macros from config.h.
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

// Globals required by Masstree. Storage recovery does not use recovering.
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
// Keep the first access epoch pinned until the caller releases its pointers.
thread_local bool tls_enrolled = false;
std::atomic<int> next_thread_id{0};
std::mutex thread_init_mutex;

// Serialize registration: Masstree's thread list has no internal lock.
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

// Keep the original epoch while the caller still uses its DataItems.
inline void ensure_thread_active() {
  ensure_thread_init();
  if (!tls_enrolled) {
    tls_ti->rcu_start();
    tls_enrolled = true;
  }
}

// Delete the item after the RCU grace period, then free this callback.
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

// Pass matching entries to the callback and count visits.
struct Scanner {
  std::optional<std::string_view> begin;
  std::optional<std::string_view> end;
  std::function<bool(std::string_view, DataItem &)> operation;
  size_t count = 0;

  template <typename SS, typename K>
  void visit_leaf(const SS &, const K &, threadinfo &) {}

  // Masstree returns true to continue; our callback returns true to stop.
  bool visit_value(Masstree::Str key, DataItem *item, threadinfo &) {
    const std::string_view scanned_key(key.s, key.len);
    if (end && scanned_key >= *end) return false;
    if (begin && scanned_key < *begin) return false;
    ++count;
    return !operation(scanned_key, *item);
  }
};

}  // namespace

struct MasstreeIndex::Impl {
  table_type table_;

  Impl() {
    // Initialize inside an RCU epoch; preserve any epoch the caller already holds.
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
    // Tree and live items remain until process exit.
    // destroy() only queues cleanup; this destructor does not run it.
  }

  DataItem *Get(std::string_view key) {
    ensure_thread_active();
    unlocked_cursor_type lp(table_, key.data(), key.size());
    if (lp.find_unlocked(*tls_ti)) return lp.value();
    return nullptr;
  }

  // Insert or replace the entry with an owned DataItem.
  void Put(std::string_view key, DataItem &&value) {
    ensure_thread_active();
    auto *item = new DataItem(std::move(value));
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    // Keep the old item alive for readers that still hold its pointer.
    lp.value() = item;  // FIXME: retire the old item through RCU
    fence();
    // Report 1 for a new key, 0 for replacement, to keep leaf versions correct.
    lp.finish(found ? 0 : 1, *tls_ti);
  }

  // The reaper has locked the item and checked its delete TID.
  bool Purge(std::string_view key, DataItem &expected,
             Tidword retired_tid) {
    ensure_thread_active();
    cursor_type lp(table_, key.data(), key.size());
    const bool found = lp.find_locked(*tls_ti);
    if (!found) {
      lp.finish(0, *tls_ti);
      return false;
    }
    DataItem *current = lp.value();
    if (current != &expected) {
      // Do not remove a replacement stored under the same key.
      lp.finish(0, *tls_ti);
      return false;
    }
    // Readers wait on the lock bit until we publish retired_tid below.
    HELIOS_DEBUG_SYNC("reaper.purge_locked_window");
    // Change the old item's TID word after unlinking it; RCU keeps it alive for
    // readers that still hold its pointer.
    lp.finish(-1, *tls_ti);
    current->transaction_id.store(retired_tid);
    RcuFree(current);
    return true;
  }

  DataItem *GetOrInsert(std::string_view key) {
    // Existing entries need only the optimistic lookup.
    if (auto *item = Get(key)) return item;

    // Recheck under the leaf lock before publishing a new absent entry.
    cursor_type lp(table_, key.data(), key.size());
    bool found = lp.find_insert(*tls_ti);
    if (!found) lp.value() = new DataItem();
    DataItem *item = lp.value();
    fence();
    lp.finish(found ? 0 : 1, *tls_ti);
    return item;
  }

  size_t Scan(std::string_view begin, std::optional<std::string_view> end,
              std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    // Start at begin; the scanner stops before end.
    Scanner scanner{std::nullopt, end, std::move(op)};
    Masstree::Str firstkey(begin.data(), begin.size());
    table_.scan(firstkey, /*emit_firstkey=*/true, scanner, *tls_ti);
    return scanner.count;
  }

  size_t ScanReverse(std::string_view begin,
                     std::optional<std::string_view> end,
                     std::function<bool(std::string_view, DataItem &)> op) {
    ensure_thread_active();
    // Start below end and stop when a key is smaller than begin.
    Scanner scanner{begin, std::nullopt, std::move(op)};
    if (end) {
      Masstree::Str firstkey(end->data(), end->size());
      table_.rscan(firstkey, /*emit_firstkey=*/false, scanner, *tls_ti);
    } else {
      // Start at the largest supported key, including that key itself.
      std::array<char, MASSTREE_MAXKEYLEN> last_key;
      last_key.fill(static_cast<char>(0xff));
      table_.rscan(Masstree::Str(last_key.data(), last_key.size()),
                   /*emit_firstkey=*/true, scanner, *tls_ti);
    }
    return scanner.count;
  }
};

MasstreeIndex::MasstreeIndex() : impl_(std::make_unique<Impl>()) {}

MasstreeIndex::~MasstreeIndex() = default;

DataItem *MasstreeIndex::Get(std::string_view key) { return impl_->Get(key); }

void MasstreeIndex::Put(std::string_view key, DataItem &&value) {
  impl_->Put(key, std::move(value));
}

DataItem *MasstreeIndex::GetOrInsert(std::string_view key) {
  return impl_->GetOrInsert(key);
}

size_t MasstreeIndex::Scan(
    std::string_view begin, std::optional<std::string_view> end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return impl_->Scan(begin, end, std::move(operation));
}

size_t MasstreeIndex::ScanReverse(
    std::string_view begin, std::optional<std::string_view> end,
    std::function<bool(std::string_view, DataItem &)> operation) {
  return impl_->ScanReverse(begin, end, std::move(operation));
}

void MasstreeIndex::ForEach(
    std::function<bool(std::string_view, DataItem &)> operation) {
  impl_->Scan(std::string_view(), std::nullopt, std::move(operation));
}

bool MasstreeIndex::Purge(std::string_view key, DataItem &expected,
                          Tidword retired_tid) {
  return impl_->Purge(key, expected, retired_tid);
}

void advance_epoch() {
  // Hold the thread-list lock while reading active epochs.
  std::lock_guard<std::mutex> lg(thread_init_mutex);
  globalepoch.store(globalepoch.load() + 1);
  active_epoch.store(threadinfo::min_active_epoch());
}

void release_thread_epoch() {
  // Release the epoch and reclaim items whose RCU grace period has passed.
  if (tls_ti == nullptr) return;
  tls_ti->rcu_stop();
  tls_enrolled = false;
}

}  // namespace index
}  // namespace helios::storage
