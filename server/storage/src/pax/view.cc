/**
 * @file server/storage/src/pax/view.cc
 * PAX schema installation, and the consistent columnar read view, which an
 * epoch fence makes consistent.
 */

#include <chrono>
#include <mutex>
#include <string>
#include <utility>

#include "lineairdb/database.h"

#include "pax/catalog.h"
#include "pax/epoch_image_buffer.h"
#include "pax/table.h"
#include "util/spdlog.h"

namespace helios::storage {

// Wait for global epoch `E >= se + 2` to drain installs that preserved no
// image.
constexpr EpochNumber kInstallDrainEpochs = 2;

pax::PaxTable *Database::GetPaxTable(const std::string_view table_name) {
  Table *table = GetTable(table_name);
  return table == nullptr ? nullptr : table->GetPaxTable();
}

Database::PaxReadView Database::OpenPaxView(uint32_t fence_timeout_ms) {
  Database::PaxReadView view;
  auto &image_buffer = pax::EpochImageBuffer::Global();
  // Open registers the view and samples `E` as snapshot `se` under the
  // registry lock. An install that did not see this registration belongs to a
  // commit at or below `se`, and every such commit has left before the wait
  // ends. Its worker epoch `e_w` keeps `E < e_w + 2` until it leaves, so
  // waiting for `E >= se + 2` drains those installs. Check the high-water
  // bound before adding the wait interval, so the calculation cannot wrap.
  const EpochNumber snapshot_epoch = image_buffer.Open(epoch_framework_);
  if (snapshot_epoch >= epoch::Framework::kEpochHighWater - kInstallDrainEpochs) {
    image_buffer.Close(snapshot_epoch);
    view.error =
        "columnar read view rejected: epoch space is near its wrap "
        "high-water mark, restart the server";
    return view;
  }
  if (!epoch_framework_.WaitEpoch(
          snapshot_epoch + kInstallDrainEpochs,
          std::chrono::milliseconds(fence_timeout_ms))) {
    image_buffer.Close(snapshot_epoch);
    view.error =
        "columnar read view fence timed out; a long-running transaction is "
        "holding the epoch";
    return view;
  }
  view.valid = true;
  view.snapshot_epoch = snapshot_epoch;
  return view;
}

void Database::ClosePaxView(const PaxReadView &view) {
  if (!view.valid) return;
  pax::EpochImageBuffer::Global().Close(view.snapshot_epoch);
}

bool Database::PaxViewValid(const PaxReadView &view) const {
  if (!view.valid) return false;
  if (epoch_framework_.GetGlobalEpoch() - view.snapshot_epoch >=
      kPaxReadViewEpochLifetime) {
    return false;  // expired: comparisons could leave the wrap-free window
  }
  return true;
}

bool Database::InstallPaxSchema(const std::string_view table_name,
                                const std::vector<uint32_t> &field_max_bytes,
                                const std::vector<pax::FieldType> &field_type,
                                const std::vector<int8_t> &field_scale) {
  if (field_max_bytes.empty()) return false;
  // The catalog is rewritten from a snapshot of every installed schema, so
  // two installs must not interleave.
  std::lock_guard<std::mutex> lk(ddl_mutex_);
  Table *table = GetTable(table_name);
  if (table == nullptr) return false;
  pax::TableSchema schema;

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

  // Reattaching with the recovered definition is allowed; changing it is not.
  if (const auto *store = table->GetPaxTable()) {
    const auto &current = store->schema();
    return current.field_max_bytes == schema.field_max_bytes &&
           current.field_type == schema.field_type &&
           current.field_scale == schema.field_scale;
  }

  // Rewrite the catalog from every installed definition, then make this
  // schema writable.
  pax::CatalogEntries entries;
  table_dictionary_.ForEachTable([&entries](Table &entry) {
    if (const auto *store = entry.GetPaxTable()) {
      entries.emplace(entry.Name(), store->schema());
    }
  });
  entries.emplace(std::string(table_name), schema);
  if (!pax::StoreCatalog(config_.work_dir, entries)) return false;
  return table->InstallPaxSchema(std::move(schema));
}

}  // namespace helios::storage
