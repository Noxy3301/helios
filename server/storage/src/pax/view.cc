/**
 * @file server/storage/src/pax/view.cc
 * PAX schema installation, and the consistent columnar read view, which an
 * epoch fence makes consistent.
 */

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>

#include "database_impl.h"
#include "pax/table.h"
#include "pax/version_store.h"
#include "util/debug_sync.h"

namespace helios::storage {

// How far the global epoch must move past the cut before every install that
// could have missed the capture flag has drained.
constexpr EpochNumber kInstallDrainEpochs = 2;

pax::PaxTable *Database::Impl::GetPaxTable(const std::string_view table_name) {
  Table *table = GetTable(table_name);
  return table == nullptr ? nullptr : table->GetPaxTable();
}

Database::PaxReadView Database::Impl::AcquirePaxView(
    uint32_t fence_timeout_ms) {
  Database::PaxReadView view;
  auto token = pax::VersionStore::Global().BeginCapture();
  if (!token.valid) {
    view.error =
        "columnar read view rejected: the capture failed for the active "
        "generation";
    return view;
  }
  // Fence order (do not reorder): arm the capture flag (seq_cst
  // increment in BeginCapture), then load the cut epoch. An install
  // whose flag check missed the capture belongs to a commit at or below the
  // cut, and a thread online in epoch e keeps the global epoch at or below
  // e + 1; once the global epoch has moved kInstallDrainEpochs on, those
  // installs have drained. Refuse when the fence target would reach or cross
  // the high-water mark, compared without addition to stay exact at
  // the numeric limit.
  const EpochNumber cut_epoch = epoch_framework_.GetGlobalEpoch();
  if (cut_epoch >= epoch::Framework::kEpochHighWater - kInstallDrainEpochs) {
    pax::VersionStore::Global().EndCapture(token);
    view.error =
        "columnar read view rejected: epoch space is near its wrap "
        "high-water mark, restart the server";
    return view;
  }
  if (!epoch_framework_.WaitEpoch(
          cut_epoch + kInstallDrainEpochs,
          std::chrono::milliseconds(fence_timeout_ms))) {
    pax::VersionStore::Global().EndCapture(token);
    view.error =
        "columnar read view fence timed out; a long-running transaction is "
        "holding the epoch";
    return view;
  }
  // Test hook: a wait point after the fence and before the handle is marked
  // valid.
  HELIOS_DEBUG_SYNC("pax_read_view.after_fence");
  // A capture failure landing during acquisition must fail it here; callers
  // treat a valid view as a serviceable read view.
  if (pax::VersionStore::Global().CaptureFailed()) {
    pax::VersionStore::Global().EndCapture(token);
    view.error = "columnar read view invalidated during acquisition";
    return view;
  }
  view.valid = true;
  view.cut_epoch = cut_epoch;
  view.token = token.id;
  return view;
}

void Database::Impl::ReleasePaxView(const Database::PaxReadView &view) {
  if (!view.valid) return;
  pax::VersionStore::ReadViewToken token;
  token.id = view.token;
  token.valid = true;
  pax::VersionStore::Global().EndCapture(token);
}

bool Database::Impl::PaxViewValid(const Database::PaxReadView &view) const {
  if (!view.valid) return false;
  if (epoch_framework_.GetGlobalEpoch() - view.cut_epoch >=
      kPaxReadViewEpochLifetime) {
    return false;  // expired: comparisons could leave the wrap-free window
  }
  return !pax::VersionStore::Global().CaptureFailed();
}

bool Database::Impl::InstallPaxSchema(
    const std::string_view table_name,
    const std::vector<uint32_t> &field_max_bytes,
    const std::vector<pax::FieldType> &field_type,
    const std::vector<int8_t> &field_scale) {
  if (field_max_bytes.empty()) return false;
  // A definition change, like CreateSecondaryIndex: every request holds this
  // lock shared, and the absent rows it creates read the store pointer.
  std::unique_lock<std::shared_mutex> lk(schema_mutex_);
  Table *table = GetTable(table_name);
  if (table == nullptr) return false;
  pax::TableSchema schema;
  schema.table_name = std::string(table_name);
  schema.field_max_bytes = field_max_bytes;
  // Typed cells only when the types vector matches the field count; otherwise
  // every field stays UNTYPED (byte-identical to the untyped layout). When
  // the types match, a scale vector of any other length is replaced with
  // zeros.
  if (field_type.size() == field_max_bytes.size()) {
    schema.field_type = field_type;
    if (field_scale.size() == field_max_bytes.size())
      schema.field_scale = field_scale;
    else
      schema.field_scale.assign(field_max_bytes.size(), 0);
  }
  return table->InstallPaxSchema(std::move(schema));
}

}  // namespace helios::storage
