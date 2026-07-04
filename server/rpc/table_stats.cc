#include "lineairdb_rpc.hh"

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lineairdb.pb.h"

// Table statistics handler: row counts plus the process-wide NDV and
// range-histogram caches consumed by the proxy cost model.

std::mutex LineairDBRpc::ndv_cache_mu_;
std::unordered_map<std::string, std::pair<bool, std::vector<uint64_t>>>
    LineairDBRpc::ndv_cache_;
std::unordered_map<std::string, LineairDBRpc::HistEntry>
    LineairDBRpc::hist_cache_;

void LineairDBRpc::handleTxGetTableStats(const std::string& message,
                                         std::string& result) {
    LineairDB::Protocol::GetTableStats::Request request;
    request.ParseFromString(message);
    LineairDB::Protocol::GetTableStats::Response response;

    if (row_counts_) {
        for (const auto& [name, count] : row_counts_->snapshot()) {
            auto* ts = response.add_table_stats();
            ts->set_table_name(name);
            ts->set_row_count(count);
        }
    }

    if (!request.ndv_table().empty() && db_manager_) {
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
                available = db && db->ComputeIndexNdvInt(
                                      request.ndv_table(), desc.index_name(),
                                      desc.num_key_parts(), ndv);
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
                hist.available = db && db->ComputeIndexHistogram(
                                            request.ndv_table(),
                                            desc.index_name(),
                                            kHistogramBuckets,
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
