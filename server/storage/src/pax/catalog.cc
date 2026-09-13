/**
 * @file server/storage/src/pax/catalog.cc
 * Atomic publication and loading of the PAX schema catalog.
 */

#include "pax/catalog.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <msgpack.hpp>
#include <stdexcept>
#include <vector>

#include "util/spdlog.h"

namespace helios::storage::pax {
namespace {

constexpr uint32_t kCatalogVersion = 1;
constexpr const char *kCatalogFile = "pax_schema.catalog";
constexpr const char *kWorkingFile = "pax_schema.working";
using File = std::unique_ptr<FILE, int (*)(FILE *)>;

// Field types are stored as their numeric tags; the map key is the table name.
struct Entry {
  std::vector<uint32_t> widths;
  std::vector<uint8_t> types;
  std::vector<int8_t> scales;
  MSGPACK_DEFINE(widths, types, scales);
};

struct PackedCatalog {
  uint32_t version = kCatalogVersion;
  std::map<std::string, Entry> entries;
  MSGPACK_DEFINE(version, entries);
};

bool Sync(int fd) {
  while (::fsync(fd) != 0) {
    if (errno != EINTR) return false;
  }
  return true;
}

}  // namespace

bool StoreCatalog(const std::string &work_dir, const CatalogEntries &entries) {
  PackedCatalog catalog;
  for (const auto &[name, schema] : entries) {
    if (schema.field_max_bytes.empty()) return false;
    Entry entry{schema.field_max_bytes, {}, schema.field_scale};
    for (const auto type : schema.field_type) {
      const auto tag = static_cast<uint8_t>(type);
      if (tag > static_cast<uint8_t>(FieldType::kDecimal64)) {
        SPDLOG_ERROR("Cannot save unknown PAX field type {} for table {}", tag,
                     name);
        return false;
      }
      entry.types.push_back(tag);
    }
    catalog.entries.emplace(name, std::move(entry));
  }
  msgpack::sbuffer bytes;
  msgpack::pack(bytes, catalog);

  // Publish a complete file so a crash cannot expose a partially written
  // schema.
  const auto directory = std::filesystem::path(work_dir);
  const auto working = (directory / kWorkingFile).string();
  const auto published = (directory / kCatalogFile).string();
  File file(std::fopen(working.c_str(), "wb"), &std::fclose);
  if (!file) {
    SPDLOG_ERROR("Cannot open PAX catalog {}: errno {}", working, errno);
    return false;
  }
  const bool written =
      std::fwrite(bytes.data(), 1, bytes.size(), file.get()) == bytes.size() &&
      std::fflush(file.get()) == 0 && Sync(::fileno(file.get()));
  const int write_error = errno;
  file.reset();
  if (!written || ::rename(working.c_str(), published.c_str()) != 0) {
    const int error = written ? errno : write_error;
    ::unlink(working.c_str());
    SPDLOG_ERROR("Cannot publish PAX catalog {}: errno {}", published, error);
    return false;
  }

  // The new filename must be durable before writes can use the schema.
  const int directory_fd =
      ::open(work_dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (directory_fd < 0) {
    SPDLOG_ERROR("Cannot open PAX catalog directory {}: errno {}",
                 work_dir, errno);
    return false;
  }
  const bool synced = Sync(directory_fd);
  const int sync_error = errno;
  ::close(directory_fd);
  if (!synced) {
    SPDLOG_ERROR("Cannot sync PAX catalog directory {}: errno {}",
                 work_dir, sync_error);
  }
  return synced;
}

Catalog LoadCatalog(const std::string &work_dir) {
  Catalog result;
  const auto path = (std::filesystem::path(work_dir) / kCatalogFile).string();
  File file(std::fopen(path.c_str(), "rb"), &std::fclose);
  if (!file) {
    if (errno == ENOENT) return result;
    result.status = Catalog::Status::kUnusable;
    result.detail = "cannot open " + path + ": errno " + std::to_string(errno);
    return result;
  }

  result.status = Catalog::Status::kUnusable;
  struct stat info {};
  if (::fstat(::fileno(file.get()), &info) != 0 || info.st_size <= 0) {
    result.detail = "cannot read the PAX catalog size";
    return result;
  }

  try {
    // Decode the whole catalog; never recover with only some table definitions.
    std::vector<char> bytes(static_cast<size_t>(info.st_size));
    if (std::fread(bytes.data(), 1, bytes.size(), file.get()) != bytes.size()) {
      result.detail = "cannot read the complete PAX catalog";
      return result;
    }
    size_t consumed = 0;
    const auto object = msgpack::unpack(bytes.data(), bytes.size(), consumed);
    if (object.get().type != msgpack::type::ARRAY ||
        object.get().via.array.size != 2) {
      result.detail = "invalid PAX catalog header";
      return result;
    }
    PackedCatalog catalog;
    object.get().convert(catalog);
    if (consumed != bytes.size() || catalog.version != kCatalogVersion) {
      result.detail = "unsupported PAX catalog format";
      return result;
    }
    for (auto &[name, entry] : catalog.entries) {
      if (entry.widths.empty())
        throw std::runtime_error("PAX schema has no fields");
      TableSchema schema;

      schema.field_max_bytes = std::move(entry.widths);
      schema.field_scale = std::move(entry.scales);
      for (const auto type : entry.types) {
        if (type > static_cast<uint8_t>(FieldType::kDecimal64)) {
          throw std::runtime_error("unknown PAX field type");
        }
        schema.field_type.push_back(static_cast<FieldType>(type));
      }
      result.entries.emplace(name, std::move(schema));
    }
  } catch (const std::exception &error) {
    result.entries.clear();
    result.detail = error.what();
    return result;
  }
  result.status = Catalog::Status::kOk;
  return result;
}

}  // namespace helios::storage::pax
