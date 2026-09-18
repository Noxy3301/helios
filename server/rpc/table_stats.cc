#include "helios_rpc.hh"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lineairdb.pb.h"

// Table statistics handler: row counts plus the process-wide NDV and
// range-histogram caches consumed by the proxy cost model.

namespace {

// A key part is [0x00 not-null][type][2-byte big-endian length][payload].
// Fills the offset past each of the leading num_parts parts; false leaves the key out.
bool key_part_ends(std::string_view key, uint32_t num_parts, size_t* ends,
                   bool allow_datetime) {
    size_t off = 0;
    for (uint32_t p = 0; p < num_parts; ++p) {
        if (off + 4 > key.size()) return false;
        const auto marker = static_cast<unsigned char>(key[off]);
        const auto type = static_cast<unsigned char>(key[off + 1]);
        if (marker != 0x00 || !(type == 0x10 || (allow_datetime && type == 0x30))) {
            return false;
        }
        const size_t len =
            (static_cast<size_t>(static_cast<unsigned char>(key[off + 2])) << 8) |
            static_cast<unsigned char>(key[off + 3]);
        off += 4 + len;
        if (off > key.size()) return false;
        ends[p] = off;
    }
    return true;
}

bool int_key_parts(std::string_view key, uint32_t n, size_t* ends) {
    return key_part_ends(key, n, ends, false);
}

bool hist_key_parts(std::string_view key, uint32_t n, size_t* ends) {
    return key_part_ends(key, n, ends, true);
}

}  // namespace

std::mutex HeliosRpc::ndv_cache_mu_;
std::unordered_map<std::string, std::pair<bool, std::vector<uint64_t>>>
    HeliosRpc::ndv_cache_;
std::unordered_map<std::string, HeliosRpc::HistEntry>
    HeliosRpc::hist_cache_;

void HeliosRpc::handleTxGetTableStats(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::GetTableStats::Request request;
    request.ParseFromString(message);
    LineairDB::Protocol::GetTableStats::Response response;
    // Every connection asks for stats when it opens a table, so this is where
    // it learns which run of this server it is talking to.
    response.set_boot_token(storage_boot_token());

    for (const auto& [name, count] : row_counts_->snapshot()) {
        auto* ts = response.add_table_stats();
        ts->set_table_name(name);
        ts->set_row_count(count);
    }

    if (!request.ndv_table().empty()) {
        auto db = db_manager_->get_database();
        for (const auto& desc : request.ndv_indexes()) {
            auto* out = response.add_index_ndv();
            out->set_index_name(desc.index_name());

            std::string cache_key = request.ndv_table();
            cache_key.push_back('\0');
            cache_key.append(desc.index_name());
            cache_key.push_back('\0');
            cache_key.append(std::to_string(desc.num_key_parts()));

            bool cached = false;
            bool available = false;
            std::vector<uint64_t> ndv;
            {
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                if (request.ndv_force_recompute()) ndv_cache_.erase(cache_key);
                auto it = ndv_cache_.find(cache_key);
                if (it != ndv_cache_.end()) {
                    cached = true;
                    available = it->second.first;
                    ndv = it->second.second;
                }
            }

            if (!cached) {
                available = db->IndexNdv(request.ndv_table(),
                                               desc.index_name(),
                                               desc.num_key_parts(),
                                               int_key_parts, ndv);
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                ndv_cache_[cache_key] = {available, ndv};
            }

            out->set_available(available);
            if (available) {
                for (uint64_t value : ndv) out->add_ndv(value);
            }

            constexpr uint32_t kHistogramBuckets = 64;
            HistEntry hist;
            bool hist_cached = false;
            {
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                if (request.ndv_force_recompute()) hist_cache_.erase(cache_key);
                auto it = hist_cache_.find(cache_key);
                if (it != hist_cache_.end()) {
                    hist_cached = true;
                    hist = it->second;
                }
            }

            if (!hist_cached) {
                hist.available = db->IndexHistogram(request.ndv_table(),
                                                          desc.index_name(),
                                                          kHistogramBuckets,
                                                          hist_key_parts,
                                                          hist.bounds,
                                                          hist.cum);
                if (!hist.available) {
                    hist.bounds.clear();
                    hist.cum.clear();
                }
                std::lock_guard<std::mutex> lock(ndv_cache_mu_);
                hist_cache_[cache_key] = hist;
            }

            out->set_hist_available(hist.available);
            if (hist.available) {
                for (const auto& bound : hist.bounds)
                    out->add_hist_bounds(bound);
                for (uint64_t value : hist.cum)
                    out->add_hist_cum(value);
            }
        }
    }

    result = response.SerializeAsString();
}
