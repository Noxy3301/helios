#include "query_block_executor.hh"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstdint>
#include <exception>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <lineairdb/database.h>
#include <lineairdb/pax_store.h>

#include "decimal_arithmetic.hh"
#include "predicate_evaluator.hh"
#include "row_codec.hh"

namespace query_block {
namespace {

namespace pb = LineairDB::Protocol;
using LineairDB::Pax::PaxGroup;
using LineairDB::Pax::PaxStore;

constexpr uint64_t kNullRowRef = std::numeric_limits<uint64_t>::max();
// Very large runtime filters are not selective enough to justify probing.
constexpr size_t kMaxRuntimeFilterKeys = size_t{4} << 20;
constexpr uint32_t kNoExternalFilterTable =
    std::numeric_limits<uint32_t>::max();
constexpr uint32_t kTaggedColumnShift = 16;
constexpr uint32_t kTaggedColumnMask = (uint32_t{1} << kTaggedColumnShift) - 1;

bool parse_int64_cell(std::string_view value, int64_t* parsed) {
    if (parsed == nullptr || value.empty()) return false;
    const char* first = value.data();
    const char* last = value.data() + value.size();
    const auto result = std::from_chars(first, last, *parsed);
    return result.ec == std::errc() && result.ptr == last;
}

bool build_int64_key_set(const std::unordered_set<std::string>& source,
                         std::unordered_set<int64_t>* target) {
    if (target == nullptr) return false;
    target->clear();
    target->reserve(source.size());
    for (const std::string& key : source) {
        int64_t parsed = 0;
        if (!parse_int64_cell(key, &parsed)) {
            target->clear();
            return false;
        }
        target->insert(parsed);
    }
    return true;
}

void set_failure(pb::TxExecuteQueryBlock::Response* response,
                 const std::string& message) {
    if (response == nullptr) return;
    response->set_ok(false);
    response->set_error(message);
    response->clear_rows();
}

__int128 decimal_power_of_ten(int exponent) {
    __int128 value = 1;
    while (exponent-- > 0) value *= 10;
    return value;
}

// MySQL AVG uses the argument scale plus div_precision_increment, which is 4 in
// the default configuration used by this build.
DecimalValue divide_decimal_value(const DecimalValue& sum, uint64_t count,
                                  int output_scale) {
    DecimalValue result;
    result.is_null = true;
    if (sum.is_null || count == 0) return result;

    __int128 numerator = sum.mantissa;
    const bool negative = numerator < 0;
    if (negative) numerator = -numerator;

    // Keep one extra decimal digit for round-half-up after the divide.
    const int extra_scale = output_scale - sum.scale + 1;
    if (extra_scale > 0) {
        numerator *= decimal_power_of_ten(extra_scale);
    } else if (extra_scale < 0) {
        numerator /= decimal_power_of_ten(-extra_scale);
    }

    __int128 quotient = numerator / static_cast<__int128>(count);
    quotient = (quotient + 5) / 10;
    result.mantissa = negative ? -quotient : quotient;
    result.scale = output_scale;
    result.is_null = false;
    return result;
}

DecimalValue divide_decimal_values(const DecimalValue& lhs,
                                   const DecimalValue& rhs,
                                   int output_scale) {
    DecimalValue result;
    result.is_null = true;
    if (lhs.is_null || rhs.is_null || rhs.mantissa == 0) return result;

    __int128 numerator = lhs.mantissa;
    __int128 denominator = rhs.mantissa;
    bool negative = false;
    if (numerator < 0) {
        numerator = -numerator;
        negative = !negative;
    }
    if (denominator < 0) {
        denominator = -denominator;
        negative = !negative;
    }

    // Scale the numerator to keep one extra decimal digit for half-up rounding.
    const int scale_shift = output_scale + rhs.scale - lhs.scale + 1;
    if (scale_shift > 0) {
        numerator *= decimal_power_of_ten(scale_shift);
    } else if (scale_shift < 0) {
        numerator /= decimal_power_of_ten(-scale_shift);
    }

    __int128 quotient = numerator / denominator;
    quotient = (quotient + 5) / 10;
    result.mantissa = negative ? -quotient : quotient;
    result.scale = output_scale;
    result.is_null = false;
    return result;
}

void round_decimal_value(DecimalValue* value, int output_scale) {
    if (value == nullptr || value->is_null || value->scale <= output_scale) {
        return;
    }

    const int drop = value->scale - output_scale;
    const __int128 divisor = decimal_power_of_ten(drop);
    __int128 mantissa = value->mantissa;
    const bool negative = mantissa < 0;
    if (negative) mantissa = -mantissa;
    mantissa = (mantissa + divisor / 2) / divisor;
    value->mantissa = negative ? -mantissa : mantissa;
    value->scale = output_scale;
}

void append_result_field(std::string& row, std::string_view payload,
                         bool is_null) {
    if (is_null) {
        row.push_back(static_cast<char>(0xFF));
        return;
    }

    const size_t length = payload.size();
    size_t byte_size = 1;
    for (size_t value = length >> 8; value != 0; value >>= 8) {
        ++byte_size;
    }
    row.push_back(static_cast<char>(byte_size));
    for (size_t idx = 0; idx < byte_size; ++idx) {
        row.push_back(static_cast<char>((length >> (8 * idx)) & 0xFF));
    }
    row.append(payload.data(), payload.size());
}

// Materialized operator output. Each logical tuple is represented as one row
// reference per participating table. Real tables use PAX row references; virtual
// tables produced by sub-blocks use result row numbers.
struct NodeResult {
    std::vector<uint32_t> tables;
    std::vector<std::vector<uint64_t>> refs;

    size_t rows() const { return refs.empty() ? 0 : refs[0].size(); }

    int table_pos(uint32_t table_idx) const {
        for (size_t idx = 0; idx < tables.size(); ++idx) {
            if (tables[idx] == table_idx) return static_cast<int>(idx);
        }
        return -1;
    }
};

struct DecodedColumnRef {
    uint32_t table_idx = 0;
    uint32_t column = 0;
};

class Executor {
 public:
    Executor(LineairDB::Database* db,
             const pb::TxExecuteQueryBlock::Request& request)
        : db_(db), request_(request) {}

    Executor(LineairDB::Database* db,
             const pb::TxExecuteQueryBlock::Request& request,
             const std::unordered_set<std::string>* external_keys,
             uint32_t external_filter_table, uint32_t external_filter_column)
        : db_(db),
          request_(request),
          external_keys_(external_keys),
          external_filter_table_(external_filter_table),
          external_filter_column_(external_filter_column) {}

    bool Run(pb::TxExecuteQueryBlock::Response* response) {
        if (!PrepareTables()) return false;
        if (!ValidateNodeOrder()) return false;
        if (!RunNodes(response)) return false;
        if (!TablesAreStillQuiet()) return fail("concurrent modification");
        return true;
    }

    const std::string& error() const { return error_; }

 private:
    bool fail(std::string message) {
        if (error_.empty()) error_ = std::move(message);
        return false;
    }

    bool PrepareTables() {
        if (db_ == nullptr) return fail("database is unavailable");

        stores_.assign(request_.tables_size(), nullptr);
        write_counter_snapshots_.assign(request_.tables_size(), {});
        slot_snapshots_.assign(request_.tables_size(), 0);
        overflow_snapshots_.assign(request_.tables_size(), 0);
        for (int table_idx = 0; table_idx < request_.tables_size();
             ++table_idx) {
            PaxStore* store =
                db_->GetPaxStore(request_.tables(table_idx).table_name());
            if (store == nullptr) return fail("table has no PAX store");
            const uint64_t overflow_snapshot = store->overflow_count();
            if (overflow_snapshot > 0) {
                return fail("table has heap fallback rows");
            }

            stores_[table_idx] = store;
            slot_snapshots_[table_idx] = store->slots_allocated();
            overflow_snapshots_[table_idx] = overflow_snapshot;

            // Capture each allocated group's writer counter before operators
            // read strip cells. A changed or odd counter rejects the request.
            std::vector<uint64_t>& snapshot =
                write_counter_snapshots_[table_idx];
            snapshot.resize(store->group_count());
            for (size_t group_idx = 0; group_idx < snapshot.size();
                 ++group_idx) {
                PaxGroup* group = store->group(group_idx);
                snapshot[group_idx] =
                    group == nullptr
                        ? 0
                        : group->write_counter.load(std::memory_order_acquire);
                if ((snapshot[group_idx] & 1u) != 0) {
                    return fail("table has in-progress PAX writes");
                }
            }
        }
        return true;
    }

    bool ValidateNodeOrder() {
        if (request_.nodes_size() == 0) return fail("empty query block");

        for (int node_idx = 0; node_idx < request_.nodes_size(); ++node_idx) {
            const pb::QueryBlockNode& node = request_.nodes(node_idx);
            if (node.has_scan()) {
                if (node.scan().table_idx() >=
                    static_cast<uint32_t>(request_.tables_size())) {
                    return fail("scan table out of range");
                }
                if (node.scan().has_semi() &&
                    node.scan().semi().source_node() >=
                        static_cast<uint32_t>(node_idx)) {
                    return fail("scan semi-filter source order");
                }
                continue;
            }
            if (node.has_join()) {
                if (node.join().build() >= static_cast<uint32_t>(node_idx) ||
                    node.join().probe() >= static_cast<uint32_t>(node_idx)) {
                    return fail("join child order");
                }
                continue;
            }
            if (node.has_filter()) {
                if (node.filter().input() >=
                    static_cast<uint32_t>(node_idx)) {
                    return fail("tuple filter child order");
                }
                continue;
            }
            if (node.has_sub_block()) {
                if (node.sub_block().has_semi()) {
                    if (node.sub_block().semi().source_node() >=
                        static_cast<uint32_t>(node_idx)) {
                        return fail("sub-block semi-filter source order");
                    }
                    if (node.sub_block().target_table() >=
                        static_cast<uint32_t>(
                            node.sub_block().block().tables_size())) {
                        return fail("sub-block semi-filter target table");
                    }
                }
                continue;
            }
            if (node.has_aggregate()) {
                if (node.aggregate().input() >=
                    static_cast<uint32_t>(node_idx)) {
                    return fail("aggregate child order");
                }
                continue;
            }
            return fail("unknown query-block node");
        }
        return true;
    }

    struct VirtualTable {
        std::vector<std::vector<std::string>> values;
        std::vector<std::vector<bool>> nulls;
    };

    bool IsVirtualTable(uint32_t table_idx) const {
        return table_idx >= static_cast<uint32_t>(request_.tables_size());
    }

    const VirtualTable* FindVirtualTable(uint32_t table_idx) const {
        if (!IsVirtualTable(table_idx)) return nullptr;
        const uint32_t virtual_idx =
            table_idx - static_cast<uint32_t>(request_.tables_size());
        if (virtual_idx >= virtual_tables_.size()) return nullptr;
        return &virtual_tables_[virtual_idx];
    }

    std::string_view ValueOf(uint32_t table_idx, uint64_t ref,
                             uint32_t column) const {
        if (ref == kNullRowRef) return {};
        if (!IsVirtualTable(table_idx)) {
            return extract_value_column(ToRowRef(table_idx, ref),
                                        static_cast<int>(column));
        }

        const VirtualTable* table = FindVirtualTable(table_idx);
        if (table == nullptr || ref >= table->values.size() ||
            column >= table->values[ref].size()) {
            return {};
        }
        return table->values[ref][column];
    }

    bool NullOf(uint32_t table_idx, uint64_t ref, uint32_t column) const {
        if (ref == kNullRowRef) return true;
        if (!IsVirtualTable(table_idx)) {
            return ValueOf(table_idx, ref, column).empty();
        }

        const VirtualTable* table = FindVirtualTable(table_idx);
        if (table == nullptr || ref >= table->nulls.size() ||
            column >= table->nulls[ref].size()) {
            return true;
        }
        return table->nulls[ref][column];
    }

    PaxRowRef ToRowRef(uint32_t table_idx, uint64_t ref) const {
        if (ref == kNullRowRef) return PaxRowRef{};
        if (table_idx >= stores_.size()) return PaxRowRef{};
        PaxStore* store = stores_[table_idx];
        return PaxRowRef{
            store->group(ref / PaxGroup::kRows),
            static_cast<uint32_t>(ref % PaxGroup::kRows),
        };
    }

    PaxRowRef RowRefForTable(const NodeResult& input, size_t row_idx,
                             uint32_t table_idx) const {
        const int position = input.table_pos(table_idx);
        if (position < 0) return PaxRowRef{};
        return ToRowRef(table_idx, input.refs[position][row_idx]);
    }

    static DecodedColumnRef DecodeAggregateColumnRef(
        uint32_t default_table_idx, uint32_t encoded_column) {
        if ((encoded_column >> kTaggedColumnShift) == 0) {
            return DecodedColumnRef{default_table_idx, encoded_column};
        }
        return DecodedColumnRef{
            encoded_column >> kTaggedColumnShift,
            encoded_column & kTaggedColumnMask,
        };
    }

    bool ValidateAggregateExpressionTables(
        const pb::FilterExpr& expression, const NodeResult& input,
        uint32_t default_table_idx) {
        if (expression.op() == pb::FilterExpr::COLUMN_REF) {
            const DecodedColumnRef column = DecodeAggregateColumnRef(
                default_table_idx, expression.column_index());
            if (input.table_pos(column.table_idx) < 0) {
                return fail("aggregate expression table is not in input");
            }
        }
        for (const pb::FilterExpr& child : expression.children()) {
            if (!ValidateAggregateExpressionTables(child, input,
                                                   default_table_idx)) {
                return false;
            }
        }
        return true;
    }

    std::string_view GroupColumnValue(
        const pb::QueryBlockColumnRef& column, uint64_t ref) const {
        std::string_view value =
            ValueOf(column.table_idx(), ref, column.column());
        if (column.prefix_len() > 0 && value.size() > column.prefix_len()) {
            value = value.substr(0, column.prefix_len());
        }
        return value;
    }

    bool ReadAggregateColumn(const pb::FilterExpr& expression,
                             const NodeResult& input, size_t row_idx,
                             uint32_t default_table_idx,
                             std::string_view* value) const {
        if (expression.op() != pb::FilterExpr::COLUMN_REF) return false;
        const DecodedColumnRef column = DecodeAggregateColumnRef(
            default_table_idx, expression.column_index());
        const int position = input.table_pos(column.table_idx);
        if (position < 0) return false;
        const uint64_t ref = input.refs[position][row_idx];
        if (NullOf(column.table_idx, ref, column.column)) return false;
        *value = ValueOf(column.table_idx, ref, column.column);
        return true;
    }

    DecimalValue EvaluateAggregateExpression(
        const pb::FilterExpr& expression, const NodeResult& input,
        size_t row_idx, uint32_t default_table_idx) const {
        using FE = pb::FilterExpr;
        switch (expression.op()) {
            case FE::COLUMN_REF: {
                std::string_view value;
                if (!ReadAggregateColumn(expression, input, row_idx,
                                         default_table_idx, &value)) {
                    DecimalValue result;
                    result.is_null = true;
                    return result;
                }
                return parse_decimal_value(value);
            }
            case FE::CONST_INT: {
                DecimalValue result;
                result.mantissa = expression.int_val();
                return result;
            }
            case FE::CONST_UINT: {
                DecimalValue result;
                result.mantissa =
                    static_cast<__int128>(expression.uint_val());
                return result;
            }
            case FE::OP_ADD:
            case FE::OP_SUB: {
                if (expression.children_size() != 2) {
                    DecimalValue result;
                    result.is_null = true;
                    return result;
                }
                DecimalValue lhs = EvaluateAggregateExpression(
                    expression.children(0), input, row_idx,
                    default_table_idx);
                DecimalValue rhs = EvaluateAggregateExpression(
                    expression.children(1), input, row_idx,
                    default_table_idx);
                if (expression.op() == FE::OP_SUB) {
                    rhs.mantissa = -rhs.mantissa;
                }
                add_decimal_value(lhs, rhs);
                return lhs;
            }
            case FE::OP_MUL: {
                if (expression.children_size() != 2) {
                    DecimalValue result;
                    result.is_null = true;
                    return result;
                }
                DecimalValue lhs = EvaluateAggregateExpression(
                    expression.children(0), input, row_idx,
                    default_table_idx);
                DecimalValue rhs = EvaluateAggregateExpression(
                    expression.children(1), input, row_idx,
                    default_table_idx);
                DecimalValue result;
                result.mantissa = lhs.mantissa * rhs.mantissa;
                result.scale = lhs.scale + rhs.scale;
                result.is_null = lhs.is_null || rhs.is_null;
                return result;
            }
            case FE::OP_NEG: {
                if (expression.children_size() != 1) {
                    DecimalValue result;
                    result.is_null = true;
                    return result;
                }
                DecimalValue result = EvaluateAggregateExpression(
                    expression.children(0), input, row_idx,
                    default_table_idx);
                result.mantissa = -result.mantissa;
                return result;
            }
            default: {
                DecimalValue result;
                result.is_null = true;
                return result;
            }
        }
    }

    bool CollectSemiFilterKeys(
        const pb::QueryBlockSemiFilter& semi,
        std::unordered_set<std::string>* keys) {
        if (keys == nullptr) return false;
        if (semi.source_node() >= results_.size()) {
            return fail("semi-filter source node out of range");
        }

        const NodeResult& source = results_[semi.source_node()];
        const pb::QueryBlockColumnRef& column = semi.source_column();
        const int position = source.table_pos(column.table_idx());
        if (position < 0) return fail("semi-filter source table");

        keys->clear();
        keys->reserve(source.rows());
        for (size_t row_idx = 0; row_idx < source.rows(); row_idx++) {
            const uint64_t ref = source.refs[position][row_idx];
            if (NullOf(column.table_idx(), ref, column.column())) continue;
            std::string_view value = GroupColumnValue(column, ref);
            keys->insert(std::string(value));
        }
        return true;
    }

    // Collects the semi-filter source's key set directly in its typed form:
    // an int64 set when every source cell parses full-length (*is_int true),
    // else an owned-string set. This is a single pass over the source; the
    // first non-convertible cell (DECIMAL, empty/NULL, non-canonical) reverts
    // to the string collection, which is byte-for-byte identical to the probe
    // path. On success exactly one of *int_keys (when *is_int) or *string_keys
    // is populated. Canonical INT cells are value<->byte 1:1, so the int set
    // probes identically to the string set.
    bool CollectSemiFilterKeysTyped(
        const pb::QueryBlockSemiFilter& semi,
        std::unordered_set<int64_t>* int_keys,
        std::unordered_set<std::string>* string_keys, bool* is_int) {
        if (int_keys == nullptr || string_keys == nullptr ||
            is_int == nullptr) {
            return false;
        }
        if (semi.source_node() >= results_.size()) {
            return fail("semi-filter source node out of range");
        }
        const NodeResult& source = results_[semi.source_node()];
        const pb::QueryBlockColumnRef& column = semi.source_column();
        const int position = source.table_pos(column.table_idx());
        if (position < 0) return fail("semi-filter source table");

        int_keys->clear();
        int_keys->reserve(source.rows());
        *is_int = true;
        for (size_t row_idx = 0; row_idx < source.rows(); row_idx++) {
            const uint64_t ref = source.refs[position][row_idx];
            if (NullOf(column.table_idx(), ref, column.column())) continue;
            const std::string_view value = GroupColumnValue(column, ref);
            int64_t parsed = 0;
            if (!parse_int64_cell(value, &parsed)) {
                // Non-INT key column: abandon the int set and re-collect the
                // source as owned strings (the byte-comparison probe path).
                *is_int = false;
                int_keys->clear();
                return CollectSemiFilterKeys(semi, string_keys);
            }
            int_keys->insert(parsed);
        }
        return true;
    }

    static bool CellMatchesKeySet(
        const PaxGroup* group, uint32_t slot, uint32_t column,
        const std::unordered_set<std::string>* keys,
        const std::unordered_set<int64_t>* int_keys, std::string* probe) {
        if (keys == nullptr && int_keys == nullptr) return true;
        if (group == nullptr) return false;
        const std::string_view value = group->cell(column + 1, slot);
        if (value.empty()) return false;
        if (int_keys != nullptr) {
            int64_t parsed = 0;
            return parse_int64_cell(value, &parsed) &&
                   int_keys->find(parsed) != int_keys->end();
        }
        // Reuse the caller's buffer: C++17 unordered_set has no heterogeneous
        // lookup, so the string key is rebuilt without a per-row allocation.
        probe->assign(value.data(), value.size());
        return keys->find(*probe) != keys->end();
    }

    bool RunScan(const pb::QueryBlockScan& scan, NodeResult* output) {
        if (scan.table_idx() >= static_cast<uint32_t>(request_.tables_size())) {
            return fail("scan table out of range");
        }

        PaxStore* store = stores_[scan.table_idx()];
        const size_t group_count =
            write_counter_snapshots_[scan.table_idx()].size();
        output->tables = {scan.table_idx()};
        output->refs.assign(1, {});
        if (group_count == 0) return true;

        // The plan-side semi keys are collected straight into their typed
        // form: an int64 set when every source cell parses (semi_int), else an
        // owned-string set. A single pass replaces the earlier build-string-
        // then-reparse two-pass conversion.
        std::unordered_set<std::string> semi_keys_storage;
        std::unordered_set<int64_t> semi_int_keys_storage;
        const std::unordered_set<std::string>* semi_keys = nullptr;
        const std::unordered_set<int64_t>* semi_int_keys = nullptr;
        if (scan.has_semi()) {
            bool semi_is_int = false;
            if (!CollectSemiFilterKeysTyped(scan.semi(), &semi_int_keys_storage,
                                            &semi_keys_storage, &semi_is_int)) {
                return false;
            }
            // A huge key set costs more to probe than it prunes; the counts
            // match across representations (canonical INT is value<->byte 1:1).
            const size_t key_count = semi_is_int ? semi_int_keys_storage.size()
                                                 : semi_keys_storage.size();
            if (key_count <= kMaxRuntimeFilterKeys) {
                if (semi_is_int) {
                    semi_int_keys = &semi_int_keys_storage;
                } else {
                    semi_keys = &semi_keys_storage;
                }
            }
        }

        const std::unordered_set<std::string>* external_keys = nullptr;
        if (external_keys_ != nullptr &&
            external_filter_table_ == scan.table_idx() &&
            external_keys_->size() <= kMaxRuntimeFilterKeys) {
            external_keys = external_keys_;
        }
        std::unordered_set<int64_t> external_int_keys_storage;
        const std::unordered_set<int64_t>* external_int_keys = nullptr;
        if (external_keys != nullptr &&
            build_int64_key_set(*external_keys, &external_int_keys_storage)) {
            external_int_keys = &external_int_keys_storage;
        }

        const bool has_filter = scan.has_filter() && scan.filter().has_expr();
        // Fetch only the cells the filter references; a wide table's full-row
        // load dominates otherwise cheap filtered scans.
        std::vector<uint32_t> filter_cols;
        if (has_filter) {
            PredicateEvaluator::collect_columns(scan.filter().expr(),
                                                &filter_cols);
        }
        const unsigned worker_count = static_cast<unsigned>(
            std::min<size_t>(WorkerCount(), group_count));
        std::vector<std::vector<uint64_t>> local_refs(worker_count);
        std::vector<char> worker_failed(worker_count, 0);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);

        for (unsigned worker_idx = 0; worker_idx < worker_count; ++worker_idx) {
            workers.emplace_back([&, worker_idx] {
                PredicateEvaluator evaluator;
                std::vector<uint64_t>& refs = local_refs[worker_idx];
                std::string probe;  // reused key buffer: no per-row alloc
                for (size_t group_idx = worker_idx; group_idx < group_count;
                     group_idx += worker_count) {
                    PaxGroup* group = store->group(group_idx);
                    if (group == nullptr) continue;

                    // Turn one 64-slot visibility word into row references for
                    // the live slots that survive the optional predicate.
                    for (uint32_t base = 0; base < PaxGroup::kRows;
                         base += 64) {
                        uint64_t visible_bits = 0;
                        for (uint32_t bit = 0; bit < 64; ++bit) {
                            if (group->IsVisible(base + bit)) {
                                visible_bits |= uint64_t{1} << bit;
                            }
                        }

                        while (visible_bits != 0) {
                            const uint32_t bit = static_cast<uint32_t>(
                                __builtin_ctzll(visible_bits));
                            visible_bits &= visible_bits - 1;
                            const uint32_t slot = base + bit;
                            if (!CellMatchesKeySet(
                                    group, slot, scan.semi().my_column(),
                                    semi_keys, semi_int_keys, &probe)) {
                                continue;
                            }
                            if (!CellMatchesKeySet(
                                    group, slot, external_filter_column_,
                                    external_keys, external_int_keys, &probe)) {
                                continue;
                            }
                            if (has_filter) {
                                if (!evaluator.set_row_from_pax_cols(
                                        *group, slot,
                                        scan.filter().num_columns(),
                                        filter_cols)) {
                                    worker_failed[worker_idx] = 1;
                                    return;
                                }
                                if (!evaluator.evaluate(scan.filter().expr())) {
                                    continue;
                                }
                            }
                            refs.push_back(group_idx * PaxGroup::kRows + slot);
                        }
                    }
                }
            });
        }

        for (std::thread& worker : workers) worker.join();
        for (char failed : worker_failed) {
            if (failed) return fail("scan filter cannot read PAX columns");
        }

        size_t total_refs = 0;
        for (const std::vector<uint64_t>& refs : local_refs) {
            total_refs += refs.size();
        }
        output->refs[0].reserve(total_refs);
        for (const std::vector<uint64_t>& refs : local_refs) {
            output->refs[0].insert(output->refs[0].end(), refs.begin(),
                                   refs.end());
        }
        return true;
    }

    bool DecodeSubBlockRow(const std::string& row,
                           std::vector<std::string>* values,
                           std::vector<bool>* nulls) {
        if (values == nullptr || nulls == nullptr) {
            return fail("sub-block row output missing");
        }

        values->clear();
        nulls->clear();
        size_t offset = 0;
        bool first_field = true;
        while (offset < row.size()) {
            const auto byte_size =
                static_cast<unsigned char>(row[offset]);
            ++offset;

            if (byte_size == 0xFF) {
                if (!first_field) {
                    values->emplace_back();
                    nulls->push_back(true);
                }
                first_field = false;
                continue;
            }

            if (offset + byte_size > row.size()) {
                return fail("sub-block row is malformed");
            }
            size_t value_length = 0;
            for (unsigned int byte_idx = 0; byte_idx < byte_size;
                 ++byte_idx) {
                value_length |=
                    static_cast<size_t>(
                        static_cast<unsigned char>(row[offset + byte_idx]))
                    << (8 * byte_idx);
            }
            offset += byte_size;
            if (offset + value_length > row.size()) {
                return fail("sub-block row is malformed");
            }

            if (!first_field) {
                values->emplace_back(row.data() + offset, value_length);
                nulls->push_back(false);
            }
            first_field = false;
            offset += value_length;
        }
        return true;
    }

    bool RunSubBlock(const pb::QueryBlockSubBlock& sub_block,
                     NodeResult* output) {
        pb::TxExecuteQueryBlock::Response response;
        if (sub_block.has_semi()) {
            std::unordered_set<std::string> keys;
            if (!CollectSemiFilterKeys(sub_block.semi(), &keys)) return false;

            Executor child(db_, sub_block.block(), &keys,
                           sub_block.target_table(),
                           sub_block.target_column());
            if (!child.Run(&response)) {
                return fail(child.error().empty() ? "sub-block failed"
                                                  : child.error());
            }
            response.set_ok(true);
        } else {
            ExecuteQueryBlock(db_, sub_block.block(), &response);
            if (!response.ok()) {
                return fail(response.error().empty() ? "sub-block failed"
                                                     : response.error());
            }
        }

        VirtualTable table;
        table.values.reserve(response.rows_size());
        table.nulls.reserve(response.rows_size());
        for (const std::string& row : response.rows()) {
            std::vector<std::string> values;
            std::vector<bool> nulls;
            if (!DecodeSubBlockRow(row, &values, &nulls)) return false;
            table.values.push_back(std::move(values));
            table.nulls.push_back(std::move(nulls));
        }

        const uint32_t table_idx = static_cast<uint32_t>(
            request_.tables_size() + virtual_tables_.size());
        virtual_tables_.push_back(std::move(table));

        output->tables = {table_idx};
        output->refs.assign(1, {});
        const size_t row_count = virtual_tables_.back().values.size();
        output->refs[0].reserve(row_count);
        for (size_t row_idx = 0; row_idx < row_count; ++row_idx) {
            output->refs[0].push_back(row_idx);
        }
        return true;
    }

    struct TupleFilterColumn {
        int ref_position = -1;
        uint32_t table_idx = 0;
        uint32_t column = 0;
    };

    bool ResolveTupleFilterColumns(
        const pb::QueryBlockTupleFilter& filter, const NodeResult& input,
        std::vector<TupleFilterColumn>* columns) {
        if (!filter.has_predicate() || !filter.predicate().has_expr()) {
            return fail("tuple filter predicate missing");
        }
        if (filter.predicate().num_columns() !=
            static_cast<uint32_t>(filter.columns_size())) {
            return fail("tuple filter column count");
        }

        columns->clear();
        columns->reserve(filter.columns_size());
        for (const pb::QueryBlockColumnRef& column : filter.columns()) {
            const int position = input.table_pos(column.table_idx());
            if (position < 0) return fail("tuple filter table");
            columns->push_back(
                TupleFilterColumn{position, column.table_idx(),
                                  column.column()});
        }
        return true;
    }

    bool BuildTupleFilterRow(
        const NodeResult& input,
        const std::vector<TupleFilterColumn>& columns, size_t row_idx,
        std::vector<std::string_view>* cells,
        std::vector<bool>* nulls) const {
        cells->resize(columns.size());
        nulls->resize(columns.size());
        for (size_t column_idx = 0; column_idx < columns.size();
             ++column_idx) {
            const TupleFilterColumn& column = columns[column_idx];
            const uint64_t ref = input.refs[column.ref_position][row_idx];
            if (ref == kNullRowRef) {
                (*cells)[column_idx] = {};
                (*nulls)[column_idx] = true;
                continue;
            }

            (*cells)[column_idx] =
                ValueOf(column.table_idx, ref, column.column);
            (*nulls)[column_idx] =
                NullOf(column.table_idx, ref, column.column);
        }
        return true;
    }

    bool RunTupleFilter(const pb::QueryBlockTupleFilterNode& filter,
                        NodeResult* output) {
        if (filter.input() >= results_.size()) {
            return fail("tuple filter child out of range");
        }
        const NodeResult& input = results_[filter.input()];

        std::vector<TupleFilterColumn> columns;
        if (!ResolveTupleFilterColumns(filter.filter(), input, &columns)) {
            return false;
        }

        output->tables = input.tables;
        output->refs.assign(input.refs.size(), {});
        const size_t input_rows = input.rows();
        const unsigned worker_count = static_cast<unsigned>(std::min<size_t>(
            WorkerCount(), std::max<size_t>(input_rows / 16384, 1)));

        struct WorkerOutput {
            std::vector<std::vector<uint64_t>> refs;
        };
        std::vector<WorkerOutput> local_outputs(worker_count);
        std::vector<char> worker_failed(worker_count, 0);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);

        for (unsigned worker_idx = 0; worker_idx < worker_count; ++worker_idx) {
            workers.emplace_back([&, worker_idx] {
                PredicateEvaluator evaluator;
                std::vector<std::string_view> cells;
                std::vector<bool> nulls;
                WorkerOutput& local = local_outputs[worker_idx];
                local.refs.assign(input.refs.size(), {});
                const size_t begin = input_rows * worker_idx / worker_count;
                const size_t end =
                    input_rows * (worker_idx + 1) / worker_count;
                for (size_t row_idx = begin; row_idx < end; ++row_idx) {
                    if (!BuildTupleFilterRow(input, columns, row_idx, &cells,
                                             &nulls)) {
                        worker_failed[worker_idx] = 1;
                        return;
                    }
                    evaluator.set_row_from_views(cells, nulls);
                    if (!evaluator.evaluate(filter.filter().predicate()
                                                .expr())) {
                        continue;
                    }
                    for (size_t column_idx = 0; column_idx < input.refs.size();
                         ++column_idx) {
                        local.refs[column_idx].push_back(
                            input.refs[column_idx][row_idx]);
                    }
                }
            });
        }

        for (std::thread& worker : workers) worker.join();
        for (char failed : worker_failed) {
            if (failed) return fail("tuple filter cannot read PAX columns");
        }

        size_t total_rows = 0;
        for (const WorkerOutput& local : local_outputs) {
            total_rows += local.refs.empty() ? 0 : local.refs[0].size();
        }
        for (size_t column_idx = 0; column_idx < output->refs.size();
             ++column_idx) {
            output->refs[column_idx].reserve(total_rows);
            for (const WorkerOutput& local : local_outputs) {
                if (local.refs.empty()) continue;
                output->refs[column_idx].insert(
                    output->refs[column_idx].end(),
                    local.refs[column_idx].begin(),
                    local.refs[column_idx].end());
            }
        }
        return true;
    }

    static void AppendJoinKeyPart(std::string* key, std::string_view value) {
        const uint32_t length = static_cast<uint32_t>(value.size());
        key->append(reinterpret_cast<const char*>(&length), sizeof(length));
        key->append(value.data(), value.size());
    }

    struct JoinKeyColumn {
        int ref_position = -1;
        uint32_t table_idx = 0;
        uint32_t column = 0;
    };

    bool ResolveJoinKeys(
        const NodeResult& input,
        const google::protobuf::RepeatedPtrField<pb::QueryBlockColumnRef>& keys,
        std::vector<JoinKeyColumn>* resolved) const {
        resolved->clear();
        resolved->reserve(keys.size());
        for (const pb::QueryBlockColumnRef& key : keys) {
            const int position = input.table_pos(key.table_idx());
            if (position < 0) return false;
            resolved->push_back(
                JoinKeyColumn{position, key.table_idx(), key.column()});
        }
        return true;
    }

    bool BuildJoinKey(const NodeResult& input,
                      const std::vector<JoinKeyColumn>& key_columns,
                      size_t row_idx, std::string* key) const {
        key->clear();
        for (const JoinKeyColumn& column : key_columns) {
            const uint64_t ref = input.refs[column.ref_position][row_idx];
            // SQL equijoins are null-rejecting. Real PAX cells encode NULL as
            // an empty cell, so empty key parts never match, mirroring the
            // scan semi-filter semantics.
            if (NullOf(column.table_idx, ref, column.column)) return false;
            AppendJoinKeyPart(key,
                              ValueOf(column.table_idx, ref, column.column));
        }
        return true;
    }

    bool RunJoin(const pb::QueryBlockJoin& join, NodeResult* output) {
        const bool is_inner = join.type() == pb::QueryBlockJoin::INNER;
        const bool is_left = join.type() == pb::QueryBlockJoin::LEFT;
        const bool is_semi = join.type() == pb::QueryBlockJoin::SEMI;
        const bool is_anti = join.type() == pb::QueryBlockJoin::ANTI;
        if (!is_inner && !is_left && !is_semi && !is_anti) {
            return fail("unsupported query-block join type");
        }
        if (join.build_keys_size() != join.probe_keys_size()) {
            return fail("join key arity");
        }
        // Keyless INNER and LEFT joins are cross products. The proxy only emits
        // them for small INNER inputs or one-row virtual LEFT inputs.
        if (join.build_keys_size() == 0 && !is_inner && !is_left) {
            return fail("join key arity");
        }
        if (join.build() >= results_.size() || join.probe() >= results_.size()) {
            return fail("join child out of range");
        }

        // INNER joins are symmetric; hash the smaller child at runtime. LEFT,
        // SEMI, and ANTI keep the request's build/probe sides because their
        // output semantics are defined by the probe side.
        const bool swap =
            is_inner &&
            results_[join.build()].rows() > results_[join.probe()].rows();
        const NodeResult& build = results_[swap ? join.probe() : join.build()];
        const NodeResult& probe = results_[swap ? join.build() : join.probe()];
        const auto& build_keys = swap ? join.probe_keys() : join.build_keys();
        const auto& probe_keys = swap ? join.build_keys() : join.probe_keys();

        std::vector<JoinKeyColumn> build_key_columns;
        std::vector<JoinKeyColumn> probe_key_columns;
        if (!ResolveJoinKeys(build, build_keys, &build_key_columns)) {
            return fail("build key table is not in join input");
        }
        if (!ResolveJoinKeys(probe, probe_keys, &probe_key_columns)) {
            return fail("probe key table is not in join input");
        }

        struct ResidualColumn {
            bool from_build = false;
            int ref_position = -1;
            uint32_t table_idx = 0;
            uint32_t column = 0;
        };

        std::vector<ResidualColumn> residual_columns;
        const bool has_residual =
            join.has_residual() && join.residual().has_predicate() &&
            join.residual().predicate().has_expr();
        if (has_residual) {
            if (join.residual().predicate().num_columns() !=
                static_cast<uint32_t>(join.residual().columns_size())) {
                return fail("join residual column count");
            }
            residual_columns.reserve(join.residual().columns_size());
            for (const pb::QueryBlockColumnRef& column :
                 join.residual().columns()) {
                int position = probe.table_pos(column.table_idx());
                if (position >= 0) {
                    residual_columns.push_back(
                        ResidualColumn{false, position, column.table_idx(),
                                       column.column()});
                    continue;
                }
                position = build.table_pos(column.table_idx());
                if (position < 0) return fail("join residual table");
                residual_columns.push_back(
                    ResidualColumn{true, position, column.table_idx(),
                                   column.column()});
            }
        }

        // Build a hash table from the chosen build child. A single-column
        // join keys the table by native int64 when every non-null build key
        // parses full-length; one non-integer key reverts to the composite
        // byte key. Canonical INT cells are value<->byte 1:1, so match
        // semantics are unchanged (the proxy restricts these keys to
        // INT/DECIMAL result types stored as canonical text).
        std::unordered_map<std::string, std::vector<size_t>> hash_table;
        std::unordered_map<int64_t, std::vector<size_t>> int_hash_table;
        bool int_join = build_key_columns.size() == 1;
        if (int_join) {
            const JoinKeyColumn& build_key = build_key_columns[0];
            int_hash_table.reserve(build.rows());
            for (size_t row_idx = 0; row_idx < build.rows(); ++row_idx) {
                const uint64_t ref =
                    build.refs[build_key.ref_position][row_idx];
                // Null-rejecting: skip null keys exactly as BuildJoinKey does.
                if (NullOf(build_key.table_idx, ref, build_key.column)) {
                    continue;
                }
                int64_t parsed = 0;
                if (!parse_int64_cell(
                        ValueOf(build_key.table_idx, ref, build_key.column),
                        &parsed)) {
                    int_join = false;
                    int_hash_table.clear();
                    break;
                }
                int_hash_table[parsed].push_back(row_idx);
            }
        }
        if (!int_join) {
            hash_table.reserve(build.rows());
            std::string key;
            for (size_t row_idx = 0; row_idx < build.rows(); ++row_idx) {
                if (!BuildJoinKey(build, build_key_columns, row_idx, &key)) {
                    continue;
                }
                hash_table[key].push_back(row_idx);
            }
        }

        // Keep only row references in the joined tuple; later operators read
        // column values directly from each table's PAX strips.
        const bool keep_build = is_inner || is_left;
        output->tables = probe.tables;
        if (keep_build) {
            output->tables.insert(output->tables.end(),
                                  build.tables.begin(), build.tables.end());
        }
        output->refs.assign(output->tables.size(), {});

        struct WorkerOutput {
            std::vector<std::vector<uint64_t>> refs;
        };

        // Probe in independent row chunks and merge per-worker ref columns at
        // the end to avoid synchronized appends on every match.
        const size_t probe_rows = probe.rows();
        const unsigned worker_count = static_cast<unsigned>(std::min<size_t>(
            WorkerCount(), std::max<size_t>(probe_rows / 16384, 1)));
        std::vector<WorkerOutput> local_outputs(worker_count);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);

        for (unsigned worker_idx = 0; worker_idx < worker_count; ++worker_idx) {
            workers.emplace_back([&, worker_idx] {
                WorkerOutput& local = local_outputs[worker_idx];
                local.refs.assign(output->tables.size(), {});
                std::string probe_key;
                PredicateEvaluator residual_evaluator;
                std::vector<std::string_view> residual_cells(
                    residual_columns.size());
                std::vector<bool> residual_nulls(residual_columns.size());
                auto residual_ok = [&](size_t probe_idx,
                                       size_t build_idx) -> bool {
                    if (!has_residual) return true;
                    for (size_t column_idx = 0;
                         column_idx < residual_columns.size();
                         ++column_idx) {
                        const ResidualColumn& column =
                            residual_columns[column_idx];
                        const NodeResult& source =
                            column.from_build ? build : probe;
                        const size_t source_row =
                            column.from_build ? build_idx : probe_idx;
                        const uint64_t ref =
                            source.refs[column.ref_position][source_row];
                        if (ref == kNullRowRef) {
                            residual_cells[column_idx] = {};
                            residual_nulls[column_idx] = true;
                            continue;
                        }

                        residual_cells[column_idx] =
                            ValueOf(column.table_idx, ref, column.column);
                        residual_nulls[column_idx] =
                            NullOf(column.table_idx, ref, column.column);
                    }
                    residual_evaluator.set_row_from_views(residual_cells,
                                                          residual_nulls);
                    return residual_evaluator.evaluate(
                        join.residual().predicate().expr());
                };
                const size_t begin = probe_rows * worker_idx / worker_count;
                const size_t end =
                    probe_rows * (worker_idx + 1) / worker_count;
                for (size_t probe_idx = begin; probe_idx < end; ++probe_idx) {
                    // Probe the matching table; int64 lookup when the build
                    // side switched to native keys, else the byte key. A null
                    // or non-integer probe cell misses, mirroring the byte
                    // path for canonical INT columns.
                    const std::vector<size_t>* match_rows = nullptr;
                    if (int_join) {
                        const JoinKeyColumn& probe_key_column =
                            probe_key_columns[0];
                        const uint64_t ref =
                            probe.refs[probe_key_column.ref_position][probe_idx];
                        int64_t parsed = 0;
                        if (!NullOf(probe_key_column.table_idx, ref,
                                    probe_key_column.column) &&
                            parse_int64_cell(
                                ValueOf(probe_key_column.table_idx, ref,
                                        probe_key_column.column),
                                &parsed)) {
                            const auto it = int_hash_table.find(parsed);
                            if (it != int_hash_table.end()) {
                                match_rows = &it->second;
                            }
                        }
                    } else if (BuildJoinKey(probe, probe_key_columns, probe_idx,
                                            &probe_key)) {
                        const auto it = hash_table.find(probe_key);
                        if (it != hash_table.end()) match_rows = &it->second;
                    }
                    bool matched =
                        match_rows != nullptr && !match_rows->empty();
                    if (matched && has_residual) {
                        matched = false;
                        for (const size_t build_idx : *match_rows) {
                            if (residual_ok(probe_idx, build_idx)) {
                                matched = true;
                                break;
                            }
                        }
                    }

                    if (match_rows == nullptr) {
                        if (!is_left && !is_anti) continue;
                        for (size_t column_idx = 0;
                             column_idx < probe.tables.size(); ++column_idx) {
                            local.refs[column_idx].push_back(
                                probe.refs[column_idx][probe_idx]);
                        }
                        if (is_anti) continue;
                        for (size_t column_idx = 0;
                             column_idx < build.tables.size(); ++column_idx) {
                            local.refs[probe.tables.size() + column_idx]
                                .push_back(kNullRowRef);
                        }
                        continue;
                    }

                    if (is_semi || is_anti) {
                        if (matched == is_semi) {
                            for (size_t column_idx = 0;
                                 column_idx < probe.tables.size();
                                 ++column_idx) {
                                local.refs[column_idx].push_back(
                                    probe.refs[column_idx][probe_idx]);
                            }
                        }
                        continue;
                    }

                    if (!matched) {
                        if (!is_left) continue;
                        for (size_t column_idx = 0;
                             column_idx < probe.tables.size(); ++column_idx) {
                            local.refs[column_idx].push_back(
                                probe.refs[column_idx][probe_idx]);
                        }
                        for (size_t column_idx = 0;
                             column_idx < build.tables.size(); ++column_idx) {
                            local.refs[probe.tables.size() + column_idx]
                                .push_back(kNullRowRef);
                        }
                        continue;
                    }

                    for (const size_t build_idx : *match_rows) {
                        if (has_residual &&
                            !residual_ok(probe_idx, build_idx)) {
                            continue;
                        }
                        for (size_t column_idx = 0;
                             column_idx < probe.tables.size(); ++column_idx) {
                            local.refs[column_idx].push_back(
                                probe.refs[column_idx][probe_idx]);
                        }
                        for (size_t column_idx = 0;
                             column_idx < build.tables.size(); ++column_idx) {
                            local.refs[probe.tables.size() + column_idx]
                                .push_back(build.refs[column_idx][build_idx]);
                        }
                    }
                }
            });
        }

        for (std::thread& worker : workers) worker.join();

        size_t total_rows = 0;
        for (const WorkerOutput& local : local_outputs) {
            total_rows += local.refs.empty() ? 0 : local.refs[0].size();
        }
        // Fan-out safety valve: reject the offload before materializing an
        // unbounded intermediate and let the primary path run the query.
        if (total_rows > (size_t{64} << 20)) {
            return fail("join intermediate too large");
        }

        for (size_t column_idx = 0; column_idx < output->refs.size();
             ++column_idx) {
            output->refs[column_idx].reserve(total_rows);
            for (const WorkerOutput& local : local_outputs) {
                if (local.refs.empty()) continue;
                output->refs[column_idx].insert(
                    output->refs[column_idx].end(),
                    local.refs[column_idx].begin(),
                    local.refs[column_idx].end());
            }
        }
        return true;
    }

    // One contiguous slot per aggregate holds every per-aggregate
    // accumulator field, improving locality on group find/accumulate.
    struct AggSlot {
        uint64_t count = 0;
        DecimalValue decimal{};    // SUM/AVG accumulator or MIN/MAX (numeric)
        std::string string_value;  // MIN/MAX (binary string)
        bool has_value = false;    // MIN/MAX has seen a value
        std::set<std::string> distinct_values;  // COUNT(DISTINCT) value set
    };
    struct GroupState {
        std::vector<std::string> keys;
        std::vector<AggSlot> aggregates;
        // The typed group-key fast paths defer key formatting to emit: instead
        // of building `keys` at accumulate time they stash the representative
        // row ref per group column here and leave key_done false, so a group
        // discarded by HAVING before any key is read never pays the ASCII
        // format. EnsureGroupKeys reproduces the exact canonical bytes from the
        // stored cell (not a to_string(int64) round-trip), keeping non-canonical
        // spellings byte-identical. The string path fills `keys` eagerly, so
        // key_done stays true there and the deferral is a no-op.
        uint64_t key_ref[2] = {kNullRowRef, kNullRowRef};
        bool key_done = true;
    };

    struct OutputRow {
        std::vector<std::string> values;
        std::vector<bool> nulls;
    };

    using GroupMap = std::unordered_map<std::string, GroupState>;
    // Single INT group column keyed by native int64 (value<->byte 1:1 for
    // canonical numeric cells), the group analogue of the join fast path.
    using IntGroupMap = std::unordered_map<int64_t, GroupState>;
    // Two INT group columns packed into a POD 128-bit key.
    struct Int2Key {
        int64_t a = 0;
        int64_t b = 0;
        bool operator==(const Int2Key& other) const {
            return a == other.a && b == other.b;
        }
    };
    struct Int2Hash {
        size_t operator()(const Int2Key& key) const {
            size_t hash = std::hash<int64_t>()(key.a);
            hash ^= std::hash<int64_t>()(key.b) + 0x9e3779b97f4a7c15ULL +
                    (hash << 6) + (hash >> 2);
            return hash;
        }
    };
    using Int2GroupMap = std::unordered_map<Int2Key, GroupState, Int2Hash>;

    static uint32_t FilterTableForAggregate(
        const pb::QueryBlockAggFunc& function) {
        if (!function.has_filter()) return function.arg_table();
        if (function.kind() == pb::QueryBlockAggFunc::COUNT &&
            !function.has_arg()) {
            return function.arg_table();
        }
        return function.filter_table();
    }

    bool BuildPredicateRowForTable(uint32_t table_idx, uint64_t ref,
                                   uint32_t column_count,
                                   std::vector<std::string_view>* cells,
                                   std::vector<bool>* nulls) const {
        if (cells == nullptr || nulls == nullptr) return false;
        if (ref == kNullRowRef) return false;
        if (!IsVirtualTable(table_idx)) {
            const PaxRowRef row = ToRowRef(table_idx, ref);
            if (row.group == nullptr ||
                row.group->schema().field_count() <
                    static_cast<size_t>(column_count) + 1) {
                return false;
            }
        } else {
            const VirtualTable* table = FindVirtualTable(table_idx);
            if (table == nullptr || ref >= table->values.size()) {
                return false;
            }
        }

        cells->resize(column_count);
        nulls->resize(column_count);
        for (uint32_t column_idx = 0; column_idx < column_count;
             ++column_idx) {
            (*cells)[column_idx] = ValueOf(table_idx, ref, column_idx);
            (*nulls)[column_idx] = NullOf(table_idx, ref, column_idx);
        }
        return true;
    }

    // Accumulates rows [begin,end) into `groups`, generic over the map key.
    // The string-keyed GroupMap builds length-prefixed byte keys; the int64
    // IntGroupMap and packed Int2GroupMap parse the group cells to int64 and
    // key on the value. In an int path the first cell that does not parse
    // full-length sets *parse_fail and returns false, so the caller discards
    // the partial map and re-runs the aggregation on the string path. Callers
    // only take an int path for numeric group columns with prefix_len==0, and
    // canonical numeric cells are value<->byte 1:1, so results are unchanged.
    template <typename MapT>
    bool AccumulateRangeT(const pb::QueryBlockAggregate& aggregate,
                          const NodeResult& input, size_t begin, size_t end,
                          MapT* groups, std::atomic<bool>* parse_fail) {
        constexpr bool IntKey =
            std::is_same_v<typename MapT::key_type, int64_t>;
        constexpr bool Int2 =
            std::is_same_v<typename MapT::key_type, Int2Key>;
        const int group_count = aggregate.group_columns_size();
        const int aggregate_count = aggregate.aggs_size();

        std::vector<int> group_positions(group_count, -1);
        for (int group_idx = 0; group_idx < group_count; ++group_idx) {
            group_positions[group_idx] =
                input.table_pos(aggregate.group_columns(group_idx).table_idx());
            if (group_positions[group_idx] < 0) {
                return fail("group table is not in aggregate input");
            }
        }

        std::vector<int> aggregate_positions(aggregate_count, -1);
        std::vector<int> filter_positions(aggregate_count, -1);
        for (int aggregate_idx = 0; aggregate_idx < aggregate_count;
             ++aggregate_idx) {
            const pb::QueryBlockAggFunc& function =
                aggregate.aggs(aggregate_idx);
            if (function.has_arg()) {
                aggregate_positions[aggregate_idx] =
                    input.table_pos(function.arg_table());
                if (aggregate_positions[aggregate_idx] < 0) {
                    return fail("aggregate table is not in aggregate input");
                }
            }
            if (function.has_filter()) {
                filter_positions[aggregate_idx] =
                    input.table_pos(FilterTableForAggregate(function));
                if (filter_positions[aggregate_idx] < 0) {
                    return fail(
                        "aggregate filter table is not in aggregate input");
                }
            }
            if (function.has_arg() &&
                !ValidateAggregateExpressionTables(
                    function.arg(), input, function.arg_table())) {
                return false;
            }
        }

        PredicateEvaluator evaluator;
        [[maybe_unused]] std::string key_buffer;
        [[maybe_unused]] std::vector<std::string_view> group_values(
            group_count);
        std::vector<std::string_view> filter_values;
        std::vector<bool> filter_nulls;
        for (size_t row_idx = begin; row_idx < end; ++row_idx) {
            GroupState* state = nullptr;
            if constexpr (IntKey) {
                // Single group column, prefix_len==0 (enforced by caller).
                const pb::QueryBlockColumnRef& column =
                    aggregate.group_columns(0);
                const uint64_t ref = input.refs[group_positions[0]][row_idx];
                const std::string_view value = GroupColumnValue(column, ref);
                int64_t parsed = 0;
                if (!parse_int64_cell(value, &parsed)) {
                    // Non-INT or NULL key: abandon the int path so the caller
                    // re-runs this aggregation over string keys.
                    parse_fail->store(true, std::memory_order_relaxed);
                    return false;
                }
                auto group_it = groups->find(parsed);
                if (group_it == groups->end()) {
                    GroupState new_state;
                    // Defer the canonical-ASCII format to emit: stash the row
                    // that created the group so EnsureGroupKeys can rebuild the
                    // exact stored bytes only if a key read actually reaches it.
                    new_state.key_ref[0] = ref;
                    new_state.key_done = false;
                    new_state.aggregates.resize(aggregate_count);
                    state = &groups->emplace(parsed, std::move(new_state))
                                 .first->second;
                } else {
                    state = &group_it->second;
                }
            } else if constexpr (Int2) {
                // Two group columns, both prefix_len==0 (enforced by caller).
                const pb::QueryBlockColumnRef& column0 =
                    aggregate.group_columns(0);
                const pb::QueryBlockColumnRef& column1 =
                    aggregate.group_columns(1);
                const uint64_t ref0 = input.refs[group_positions[0]][row_idx];
                const uint64_t ref1 = input.refs[group_positions[1]][row_idx];
                const std::string_view value0 =
                    GroupColumnValue(column0, ref0);
                const std::string_view value1 =
                    GroupColumnValue(column1, ref1);
                Int2Key key;
                if (!parse_int64_cell(value0, &key.a) ||
                    !parse_int64_cell(value1, &key.b)) {
                    parse_fail->store(true, std::memory_order_relaxed);
                    return false;
                }
                auto group_it = groups->find(key);
                if (group_it == groups->end()) {
                    GroupState new_state;
                    // Defer both cells' canonical-ASCII format to emit.
                    new_state.key_ref[0] = ref0;
                    new_state.key_ref[1] = ref1;
                    new_state.key_done = false;
                    new_state.aggregates.resize(aggregate_count);
                    state = &groups->emplace(key, std::move(new_state))
                                 .first->second;
                } else {
                    state = &group_it->second;
                }
            } else {
                key_buffer.clear();
                for (int group_idx = 0; group_idx < group_count; ++group_idx) {
                    const pb::QueryBlockColumnRef& column =
                        aggregate.group_columns(group_idx);
                    const uint64_t ref =
                        input.refs[group_positions[group_idx]][row_idx];
                    group_values[group_idx] = GroupColumnValue(column, ref);
                    const uint32_t length =
                        static_cast<uint32_t>(group_values[group_idx].size());
                    key_buffer.append(reinterpret_cast<const char*>(&length),
                                      sizeof(length));
                    key_buffer.append(group_values[group_idx].data(),
                                      group_values[group_idx].size());
                }

                auto group_it = groups->find(key_buffer);
                if (group_it == groups->end()) {
                    GroupState new_state;
                    new_state.keys.resize(group_count);
                    for (int group_idx = 0; group_idx < group_count;
                         ++group_idx) {
                        new_state.keys[group_idx] =
                            std::string(group_values[group_idx]);
                    }
                    new_state.aggregates.resize(aggregate_count);
                    state = &groups->emplace(std::move(key_buffer),
                                             std::move(new_state))
                                 .first->second;
                    key_buffer.clear();
                } else {
                    state = &group_it->second;
                }
            }

            for (int aggregate_idx = 0; aggregate_idx < aggregate_count;
                 ++aggregate_idx) {
                const pb::QueryBlockAggFunc& function =
                    aggregate.aggs(aggregate_idx);
                const int input_pos = aggregate_positions[aggregate_idx];
                const uint64_t ref =
                    input_pos >= 0 ? input.refs[input_pos][row_idx] : 0;
                const bool has_arg_row =
                    input_pos < 0 || ref != kNullRowRef;

                if (function.has_filter() && function.filter().has_expr()) {
                    const int filter_pos = filter_positions[aggregate_idx];
                    const uint64_t filter_ref =
                        input.refs[filter_pos][row_idx];
                    if (filter_ref == kNullRowRef) continue;
                    const uint32_t filter_table =
                        FilterTableForAggregate(function);
                    if (!BuildPredicateRowForTable(
                            filter_table, filter_ref,
                            function.filter().num_columns(), &filter_values,
                            &filter_nulls)) {
                        return fail(
                            "aggregate filter cannot read table columns");
                    }
                    evaluator.set_row_from_views(filter_values, filter_nulls);
                    if (!evaluator.evaluate(function.filter().expr())) {
                        continue;
                    }
                }

                switch (function.kind()) {
                    case pb::QueryBlockAggFunc::COUNT:
                        if (function.distinct()) {
                            if (!function.has_arg() || !has_arg_row) break;
                            std::string_view value;
                            if (ReadAggregateColumn(
                                    function.arg(), input, row_idx,
                                    function.arg_table(), &value)) {
                                state->aggregates[aggregate_idx]
                                    .distinct_values.emplace(value);
                            }
                            break;
                        }
                        if (function.has_arg() && !has_arg_row) break;
                        state->aggregates[aggregate_idx].count += 1;
                        break;
                    case pb::QueryBlockAggFunc::SUM:
                    case pb::QueryBlockAggFunc::AVG: {
                        DecimalValue value = EvaluateAggregateExpression(
                            function.arg(), input, row_idx,
                            function.arg_table());
                        if (value.is_null) break;
                        add_decimal_value(
                            state->aggregates[aggregate_idx].decimal, value);
                        state->aggregates[aggregate_idx].count += 1;
                        break;
                    }
                    case pb::QueryBlockAggFunc::MIN:
                    case pb::QueryBlockAggFunc::MAX: {
                        const bool wants_max =
                            function.kind() == pb::QueryBlockAggFunc::MAX;
                        if (function.cmp_kind() == 1) {
                            std::string_view value;
                            if (!ReadAggregateColumn(function.arg(), input,
                                                     row_idx,
                                                     function.arg_table(),
                                                     &value)) {
                                break;
                            }
                            AggSlot& slot = state->aggregates[aggregate_idx];
                            if (!slot.has_value ||
                                (wants_max
                                     ? value > std::string_view(
                                                   slot.string_value)
                                     : value < std::string_view(
                                                   slot.string_value))) {
                                slot.string_value = std::string(value);
                            }
                        } else {
                            DecimalValue value = EvaluateAggregateExpression(
                                function.arg(), input, row_idx,
                                function.arg_table());
                            if (value.is_null) break;
                            AggSlot& slot = state->aggregates[aggregate_idx];
                            if (!slot.has_value ||
                                (wants_max
                                     ? compare_decimal_values(
                                           value, slot.decimal) > 0
                                     : compare_decimal_values(
                                           value, slot.decimal) < 0)) {
                                slot.decimal = value;
                            }
                        }
                        state->aggregates[aggregate_idx].has_value = true;
                        break;
                    }
                    default:
                        return fail("unsupported aggregate function");
                }
            }
        }
        return true;
    }

    template <typename MapT>
    static void MergeGroups(MapT* destination, MapT* source,
                            const pb::QueryBlockAggregate& aggregate) {
        if (destination->empty()) {
            *destination = std::move(*source);
            return;
        }

        for (auto& entry : *source) {
            auto destination_it = destination->find(entry.first);
            if (destination_it == destination->end()) {
                destination->emplace(entry.first, std::move(entry.second));
                continue;
            }

            GroupState& destination_state = destination_it->second;
            GroupState& source_state = entry.second;
            for (int aggregate_idx = 0;
                 aggregate_idx < aggregate.aggs_size(); ++aggregate_idx) {
                const pb::QueryBlockAggFunc& function =
                    aggregate.aggs(aggregate_idx);
                AggSlot& destination_slot =
                    destination_state.aggregates[aggregate_idx];
                AggSlot& source_slot =
                    source_state.aggregates[aggregate_idx];
                switch (function.kind()) {
                    case pb::QueryBlockAggFunc::COUNT:
                        if (function.distinct()) {
                            destination_slot.distinct_values.merge(
                                source_slot.distinct_values);
                            break;
                        }
                        destination_slot.count += source_slot.count;
                        break;
                    case pb::QueryBlockAggFunc::SUM:
                    case pb::QueryBlockAggFunc::AVG:
                        if (source_slot.count > 0) {
                            add_decimal_value(destination_slot.decimal,
                                              source_slot.decimal);
                            destination_slot.count += source_slot.count;
                        }
                        break;
                    case pb::QueryBlockAggFunc::MIN:
                    case pb::QueryBlockAggFunc::MAX: {
                        if (!source_slot.has_value) break;
                        const bool wants_max =
                            function.kind() == pb::QueryBlockAggFunc::MAX;
                        if (function.cmp_kind() == 1) {
                            if (!destination_slot.has_value ||
                                (wants_max
                                     ? source_slot.string_value >
                                           destination_slot.string_value
                                     : source_slot.string_value <
                                           destination_slot.string_value)) {
                                destination_slot.string_value =
                                    std::move(source_slot.string_value);
                            }
                        } else if (
                            !destination_slot.has_value ||
                            (wants_max
                                 ? compare_decimal_values(
                                       source_slot.decimal,
                                       destination_slot.decimal) > 0
                                 : compare_decimal_values(
                                       source_slot.decimal,
                                       destination_slot.decimal) < 0)) {
                            destination_slot.decimal = source_slot.decimal;
                        }
                        destination_slot.has_value = true;
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    }

    // Lazily materialize a deferred group's key cells from its representative
    // row refs. A no-op once key_done is true (the string path fills `keys`
    // eagerly), and on the typed fast paths for groups whose keys are never
    // read. GroupColumnValue on the stored ref returns the exact canonical
    // bytes, so the deferred format is byte-identical to eager formatting.
    // Reentrant (no shared buffer) so per-group emit steps may call it on
    // disjoint groups concurrently.
    void EnsureGroupKeys(const pb::QueryBlockAggregate& aggregate,
                         GroupState* state) const {
        if (state->key_done) return;
        const int group_count = aggregate.group_columns_size();
        state->keys.resize(group_count);
        for (int group_idx = 0; group_idx < group_count; ++group_idx) {
            const pb::QueryBlockColumnRef& column =
                aggregate.group_columns(group_idx);
            state->keys[group_idx] = std::string(
                GroupColumnValue(column, state->key_ref[group_idx]));
        }
        state->key_done = true;
    }

    std::string AggregateValue(const pb::QueryBlockAggFunc& function,
                               const GroupState& state, int aggregate_idx,
                               bool* is_null) const {
        *is_null = false;
        const AggSlot& slot = state.aggregates[aggregate_idx];
        switch (function.kind()) {
            case pb::QueryBlockAggFunc::COUNT:
                return std::to_string(function.distinct()
                                          ? slot.distinct_values.size()
                                          : slot.count);
            case pb::QueryBlockAggFunc::SUM:
                if (slot.count == 0) {
                    if (function.zero_if_empty()) {
                        DecimalValue zero;
                        zero.scale = static_cast<int>(function.arg_scale());
                        zero.is_null = false;
                        return format_decimal_value(zero);
                    }
                    *is_null = true;
                    return {};
                }
                return format_decimal_value(slot.decimal);
            case pb::QueryBlockAggFunc::AVG: {
                if (slot.count == 0) {
                    *is_null = true;
                    return {};
                }
                const int output_scale =
                    static_cast<int>(function.arg_scale()) + 4;
                return format_decimal_value(
                    divide_decimal_value(slot.decimal, slot.count,
                                         output_scale));
            }
            case pb::QueryBlockAggFunc::MIN:
            case pb::QueryBlockAggFunc::MAX:
                if (!slot.has_value) {
                    *is_null = true;
                    return {};
                }
                return function.cmp_kind() == 1
                           ? slot.string_value
                           : format_decimal_value(slot.decimal);
            default:
                *is_null = true;
                return {};
        }
    }

    bool EvaluateOutputExpression(const pb::FilterExpr& expression,
                                  const pb::QueryBlockAggregate& aggregate,
                                  const GroupState& state,
                                  DecimalValue* result) {
        if (result == nullptr) return fail("missing output expression result");

        using FE = pb::FilterExpr;
        switch (expression.op()) {
            case FE::COLUMN_REF: {
                const uint32_t ordinal = expression.column_index();
                const uint32_t group_count =
                    static_cast<uint32_t>(aggregate.group_columns_size());
                if (ordinal < group_count) {
                    *result = parse_decimal_value(state.keys[ordinal]);
                    return true;
                }

                const uint32_t aggregate_idx = ordinal - group_count;
                if (aggregate_idx >=
                    static_cast<uint32_t>(aggregate.aggs_size())) {
                    return fail("output expression ordinal out of range");
                }
                bool is_null = false;
                const std::string value = AggregateValue(
                    aggregate.aggs(aggregate_idx), state,
                    static_cast<int>(aggregate_idx), &is_null);
                if (is_null) {
                    *result = DecimalValue{};
                    result->is_null = true;
                    return true;
                }
                *result = parse_decimal_value(value);
                return true;
            }
            case FE::CONST_INT:
                result->mantissa = expression.int_val();
                result->scale = 0;
                result->is_null = false;
                return true;
            case FE::CONST_UINT:
                result->mantissa =
                    static_cast<__int128>(expression.uint_val());
                result->scale = 0;
                result->is_null = false;
                return true;
            case FE::CONST_STRING:
                *result = parse_decimal_value(expression.string_val());
                return true;
            case FE::CONST_NULL:
                *result = DecimalValue{};
                result->is_null = true;
                return true;
            case FE::OP_ADD:
            case FE::OP_SUB: {
                if (expression.children_size() != 2) {
                    return fail("output expression arity mismatch");
                }
                DecimalValue lhs;
                DecimalValue rhs;
                if (!EvaluateOutputExpression(expression.children(0),
                                              aggregate, state, &lhs) ||
                    !EvaluateOutputExpression(expression.children(1),
                                              aggregate, state, &rhs)) {
                    return false;
                }
                if (expression.op() == FE::OP_SUB) {
                    rhs.mantissa = -rhs.mantissa;
                }
                add_decimal_value(lhs, rhs);
                *result = lhs;
                return true;
            }
            case FE::OP_MUL: {
                if (expression.children_size() != 2) {
                    return fail("output expression arity mismatch");
                }
                DecimalValue lhs;
                DecimalValue rhs;
                if (!EvaluateOutputExpression(expression.children(0),
                                              aggregate, state, &lhs) ||
                    !EvaluateOutputExpression(expression.children(1),
                                              aggregate, state, &rhs)) {
                    return false;
                }
                result->mantissa = lhs.mantissa * rhs.mantissa;
                result->scale = lhs.scale + rhs.scale;
                result->is_null = lhs.is_null || rhs.is_null;
                return true;
            }
            case FE::OP_DIV: {
                if (expression.children_size() != 2) {
                    return fail("output expression arity mismatch");
                }
                DecimalValue lhs;
                DecimalValue rhs;
                if (!EvaluateOutputExpression(expression.children(0),
                                              aggregate, state, &lhs) ||
                    !EvaluateOutputExpression(expression.children(1),
                                              aggregate, state, &rhs)) {
                    return false;
                }
                // Match MySQL DECIMAL division: left operand scale plus the
                // default div_precision_increment, rounded half up.
                *result = divide_decimal_values(lhs, rhs, lhs.scale + 4);
                return true;
            }
            case FE::OP_NEG: {
                if (expression.children_size() != 1) {
                    return fail("output expression arity mismatch");
                }
                if (!EvaluateOutputExpression(expression.children(0),
                                              aggregate, state, result)) {
                    return false;
                }
                result->mantissa = -result->mantissa;
                return true;
            }
            default:
                return fail("unsupported output expression");
        }
    }

    bool AggregateRowValue(const pb::QueryBlockAggregate& aggregate,
                           int group_count, const GroupState& state,
                           uint32_t ordinal, std::string* value,
                           bool* is_null) {
        value->clear();
        *is_null = false;
        if (ordinal < static_cast<uint32_t>(group_count)) {
            value->assign(state.keys[ordinal]);
            return true;
        }

        const int aggregate_idx = static_cast<int>(ordinal) - group_count;
        if (aggregate_idx < 0 || aggregate_idx >= aggregate.aggs_size()) {
            return fail("regroup value ordinal out of range");
        }
        value->assign(AggregateValue(aggregate.aggs(aggregate_idx), state,
                                     aggregate_idx, is_null));
        return true;
    }

    static void AppendRegroupKeyPart(std::string* key, std::string_view value,
                                     bool is_null) {
        key->push_back(is_null ? '\1' : '\0');
        if (is_null) return;
        AppendJoinKeyPart(key, value);
    }

    struct RegroupState {
        std::vector<std::string> keys;
        std::vector<bool> nulls;
        uint64_t count = 0;
    };

    template <typename MapT>
    bool BuildRegroupedRows(const pb::QueryBlockAggregate& aggregate,
                            int group_count, MapT& groups,
                            std::vector<OutputRow>* output_rows) {
        const auto& regroup = aggregate.regroup();
        if (!regroup.count_star()) return fail("regroup must count");

        std::unordered_map<std::string, RegroupState> regrouped;
        std::string key;
        std::vector<std::string> values;
        std::vector<bool> nulls;
        for (auto& entry : groups) {
            GroupState& state = entry.second;
            key.clear();
            values.clear();
            nulls.clear();
            values.reserve(regroup.group_value_ordinals_size());
            nulls.reserve(regroup.group_value_ordinals_size());

            for (const uint32_t ordinal : regroup.group_value_ordinals()) {
                // Only a group ordinal reads the deferred key cells; an
                // aggregate ordinal (q13 groups by the per-customer count)
                // never materializes them.
                if (ordinal < static_cast<uint32_t>(group_count)) {
                    EnsureGroupKeys(aggregate, &state);
                }
                std::string value;
                bool is_null = false;
                if (!AggregateRowValue(aggregate, group_count, state, ordinal,
                                       &value, &is_null)) {
                    return false;
                }
                AppendRegroupKeyPart(&key, value, is_null);
                values.push_back(std::move(value));
                nulls.push_back(is_null);
            }

            auto [it, inserted] = regrouped.emplace(key, RegroupState{});
            RegroupState& regroup_state = it->second;
            if (inserted) {
                regroup_state.keys = values;
                regroup_state.nulls = nulls;
            }
            regroup_state.count += 1;
        }

        output_rows->clear();
        output_rows->reserve(regrouped.size());
        for (const auto& entry : regrouped) {
            const RegroupState& state = entry.second;
            OutputRow row;
            row.values.reserve(request_.output_size());
            row.nulls.reserve(request_.output_size());
            for (const pb::QueryBlockOutputExpr& expression :
                 request_.output()) {
                switch (expression.source()) {
                    case pb::QueryBlockOutputExpr::GROUP:
                        if (expression.ordinal() >= state.keys.size()) {
                            return fail("regroup output ordinal out of range");
                        }
                        row.values.push_back(
                            state.keys[expression.ordinal()]);
                        row.nulls.push_back(
                            state.nulls[expression.ordinal()]);
                        break;
                    case pb::QueryBlockOutputExpr::AGG:
                        if (expression.ordinal() != 0) {
                            return fail("regroup aggregate ordinal out of range");
                        }
                        row.values.push_back(std::to_string(state.count));
                        row.nulls.push_back(false);
                        break;
                    default:
                        return fail("unsupported regroup output source");
                }
            }
            output_rows->push_back(std::move(row));
        }
        return true;
    }

    bool SortOutputRows(std::vector<OutputRow>* output_rows) {
        for (const pb::QueryBlockSortKey& key : request_.order_by()) {
            if (key.output_ordinal() >=
                static_cast<uint32_t>(request_.output_size())) {
                return fail("order-by output ordinal out of range");
            }
        }

        std::sort(output_rows->begin(), output_rows->end(),
                  [&](const OutputRow& lhs, const OutputRow& rhs) {
                      for (const pb::QueryBlockSortKey& key :
                           request_.order_by()) {
                          const uint32_t ordinal = key.output_ordinal();
                          const bool lhs_null = lhs.nulls[ordinal];
                          const bool rhs_null = rhs.nulls[ordinal];
                          if (lhs_null != rhs_null) {
                              return key.descending() ? rhs_null : lhs_null;
                          }
                          if (lhs_null) continue;

                          int comparison = 0;
                          if (key.cmp_kind() == 1) {
                              comparison =
                                  lhs.values[ordinal].compare(
                                      rhs.values[ordinal]);
                          } else {
                              comparison = compare_decimal_values(
                                  parse_decimal_value(lhs.values[ordinal]),
                                  parse_decimal_value(rhs.values[ordinal]));
                          }
                          if (comparison != 0) {
                              return key.descending() ? comparison > 0
                                                      : comparison < 0;
                          }
                      }
                      return false;
                  });
        return true;
    }

    void EmitOutputRows(
        const std::vector<OutputRow>& output_rows,
        pb::TxExecuteQueryBlock::Response* response) const {
        const size_t begin =
            std::min<size_t>(request_.offset(), output_rows.size());
        const size_t end =
            request_.limit() > 0
                ? std::min<size_t>(begin + request_.limit(),
                                   output_rows.size())
                : output_rows.size();

        std::string row_buffer;
        for (size_t row_idx = begin; row_idx < end; ++row_idx) {
            row_buffer.clear();
            append_result_field(row_buffer, {}, false);
            const OutputRow& row = output_rows[row_idx];
            for (size_t column_idx = 0; column_idx < row.values.size();
                 ++column_idx) {
                append_result_field(row_buffer, row.values[column_idx],
                                    row.nulls[column_idx]);
            }
            response->add_rows(row_buffer);
        }
    }

    bool RunEmitRows(
        const NodeResult& input,
        pb::TxExecuteQueryBlock::Response* response) {
        std::vector<int> output_positions(request_.output_size(), -1);
        for (int output_idx = 0; output_idx < request_.output_size();
             ++output_idx) {
            const pb::QueryBlockOutputExpr& expression =
                request_.output(output_idx);
            if (expression.source() != pb::QueryBlockOutputExpr::COLUMN) {
                return fail("row output source");
            }
            output_positions[output_idx] =
                input.table_pos(expression.column().table_idx());
            if (output_positions[output_idx] < 0) {
                return fail("row output table is not in input");
            }
        }

        std::vector<OutputRow> output_rows;
        output_rows.reserve(input.rows());
        for (size_t row_idx = 0; row_idx < input.rows(); ++row_idx) {
            OutputRow row;
            row.values.reserve(request_.output_size());
            row.nulls.reserve(request_.output_size());

            for (int output_idx = 0; output_idx < request_.output_size();
                 ++output_idx) {
                const pb::QueryBlockColumnRef& column =
                    request_.output(output_idx).column();
                const uint64_t ref =
                    input.refs[output_positions[output_idx]][row_idx];
                if (NullOf(column.table_idx(), ref, column.column())) {
                    row.values.emplace_back();
                    row.nulls.push_back(true);
                    continue;
                }

                std::string_view value =
                    ValueOf(column.table_idx(), ref, column.column());
                if (column.prefix_len() > 0 &&
                    value.size() > column.prefix_len()) {
                    value = value.substr(0, column.prefix_len());
                }
                row.values.emplace_back(value);
                row.nulls.push_back(false);
            }
            output_rows.push_back(std::move(row));
        }

        if (!SortOutputRows(&output_rows)) return false;
        EmitOutputRows(output_rows, response);
        return true;
    }

    bool RunAggregateAndEmit(
        const pb::QueryBlockAggregate& aggregate,
        pb::TxExecuteQueryBlock::Response* response) {
        const NodeResult& input = results_[aggregate.input()];
        const size_t input_rows = input.rows();

        const unsigned worker_count = static_cast<unsigned>(std::min<size_t>(
            WorkerCount(), std::max<size_t>(input_rows / 65536, 1)));
        const int group_count = aggregate.group_columns_size();

        // Accumulate rows [0,input_rows) into a fresh map of the given type,
        // single-threaded or across worker_count locals merged at the end.
        // Returns 0 on success (map ready to emit), 1 on parse fallback (an int
        // path hit a non-integer cell), 2 on structural failure.
        auto run_typed = [&](auto& groups) -> int {
            using MapT = std::decay_t<decltype(groups)>;
            std::atomic<bool> parse_fail{false};
            bool ok = true;
            if (worker_count <= 1) {
                ok = AccumulateRangeT(aggregate, input, 0, input_rows, &groups,
                                      &parse_fail);
            } else {
                std::vector<MapT> local_groups(worker_count);
                std::vector<char> worker_failed(worker_count, 0);
                std::vector<std::thread> workers;
                workers.reserve(worker_count);
                for (unsigned worker_idx = 0; worker_idx < worker_count;
                     ++worker_idx) {
                    workers.emplace_back([&, worker_idx] {
                        const size_t begin =
                            input_rows * worker_idx / worker_count;
                        const size_t end =
                            input_rows * (worker_idx + 1) / worker_count;
                        if (!AccumulateRangeT(aggregate, input, begin, end,
                                              &local_groups[worker_idx],
                                              &parse_fail)) {
                            worker_failed[worker_idx] = 1;
                        }
                    });
                }
                for (std::thread& worker : workers) worker.join();
                for (char failed : worker_failed) {
                    if (failed) {
                        ok = false;
                        break;
                    }
                }
                if (ok) {
                    // Reserve the destination once, after the first local is
                    // moved in, sized to the summed local counts. A per-step
                    // reserve would rehash the growing destination every merge.
                    size_t total_groups = 0;
                    for (const MapT& local : local_groups) {
                        total_groups += local.size();
                    }
                    bool first = true;
                    for (MapT& local : local_groups) {
                        MergeGroups(&groups, &local, aggregate);
                        if (first && !groups.empty()) {
                            groups.reserve(total_groups);
                            first = false;
                        }
                    }
                }
            }
            if (ok) return 0;
            return parse_fail.load() ? 1 : 2;
        };

        // Post-accumulation pipeline (implicit grouping fill -> HAVING ->
        // regroup / output rows -> ORDER BY / LIMIT / serialize), generic over
        // the map key so the typed fast paths and the string path share it.
        auto emit = [&](auto& groups) -> bool {
            using KeyType = typename std::decay_t<decltype(groups)>::key_type;
            // Implicit grouping emits one row over zero input; reachable only
            // for the string map, since the int paths always group by a
            // numeric column.
            if constexpr (std::is_same_v<KeyType, std::string>) {
                if (group_count == 0 && groups.empty()) {
                    GroupState state;
                    state.aggregates.resize(aggregate.aggs_size());
                    groups.emplace(std::string(), std::move(state));
                }
            }

        // A HAVING predicate that reads only aggregate values (q18:
        // sum(l_quantity) > 300) never touches the group keys, so the groups it
        // discards keep their deferred keys unformatted -- the point of the
        // deferral. Materialize keys here only when HAVING references a group
        // column ordinal (< group_count).
        bool having_uses_group = false;
        if (aggregate.has_having() && aggregate.having().has_expr()) {
            std::vector<uint32_t> having_cols;
            PredicateEvaluator::collect_columns(aggregate.having().expr(),
                                                &having_cols);
            for (const uint32_t ordinal : having_cols) {
                if (ordinal < static_cast<uint32_t>(group_count)) {
                    having_uses_group = true;
                    break;
                }
            }
        }

        // HAVING predicates read the first-stage aggregate row layout:
        // group values first, then aggregate values.
        if (aggregate.has_having() && aggregate.having().has_expr()) {
            PredicateEvaluator evaluator;
            std::vector<std::string> values;
            std::vector<std::string_view> cells;
            std::vector<bool> nulls;
            for (auto group_it = groups.begin(); group_it != groups.end();) {
                GroupState& state = group_it->second;
                if (having_uses_group) EnsureGroupKeys(aggregate, &state);
                values.clear();
                cells.clear();
                nulls.clear();
                values.reserve(group_count + aggregate.aggs_size());
                cells.reserve(group_count + aggregate.aggs_size());
                nulls.reserve(group_count + aggregate.aggs_size());

                for (int group_idx = 0; group_idx < group_count;
                     ++group_idx) {
                    // Placeholder when HAVING never names a group ordinal; the
                    // evaluator only reads ordinals the expression references.
                    values.push_back(having_uses_group ? state.keys[group_idx]
                                                        : std::string());
                    nulls.push_back(false);
                }
                for (int aggregate_idx = 0;
                     aggregate_idx < aggregate.aggs_size();
                     ++aggregate_idx) {
                    bool is_null = false;
                    values.push_back(AggregateValue(
                        aggregate.aggs(aggregate_idx), state,
                        aggregate_idx, &is_null));
                    nulls.push_back(is_null);
                }
                for (const std::string& value : values) {
                    cells.push_back(value);
                }

                evaluator.set_row_from_views(cells, nulls);
                if (evaluator.evaluate(aggregate.having().expr())) {
                    ++group_it;
                } else {
                    group_it = groups.erase(group_it);
                }
            }
        }

        std::vector<OutputRow> output_rows;
        if (aggregate.has_regroup()) {
            if (!BuildRegroupedRows(aggregate, group_count, groups,
                                    &output_rows)) {
                return false;
            }
        } else {
            output_rows.reserve(groups.size());
            for (auto& entry : groups) {
                GroupState& state = entry.second;
                // Survivors only: GROUP and group-referencing EXPR outputs read
                // the key cells, so materialize any deferred keys once here.
                EnsureGroupKeys(aggregate, &state);
                OutputRow row;
                row.values.reserve(request_.output_size());
                row.nulls.reserve(request_.output_size());
                for (const pb::QueryBlockOutputExpr& expression :
                     request_.output()) {
                    switch (expression.source()) {
                        case pb::QueryBlockOutputExpr::GROUP:
                            if (expression.ordinal() >=
                                static_cast<uint32_t>(group_count)) {
                                return fail(
                                    "group output ordinal out of range");
                            }
                            row.values.push_back(
                                state.keys[expression.ordinal()]);
                            row.nulls.push_back(false);
                            break;
                        case pb::QueryBlockOutputExpr::AGG: {
                            if (expression.ordinal() >=
                                static_cast<uint32_t>(
                                    aggregate.aggs_size())) {
                                return fail(
                                    "aggregate output ordinal out of range");
                            }
                            bool is_null = false;
                            row.values.push_back(AggregateValue(
                                aggregate.aggs(expression.ordinal()), state,
                                static_cast<int>(expression.ordinal()),
                                &is_null));
                            row.nulls.push_back(is_null);
                            break;
                        }
                        case pb::QueryBlockOutputExpr::EXPR: {
                            DecimalValue value;
                            if (!EvaluateOutputExpression(
                                    expression.expr(), aggregate, state,
                                    &value)) {
                                return false;
                            }
                            round_decimal_value(
                                &value,
                                static_cast<int>(expression.result_scale()));
                            row.values.push_back(
                                value.is_null ? std::string()
                                              : format_decimal_value(value));
                            row.nulls.push_back(value.is_null);
                            break;
                        }
                        default:
                            return fail(
                                "unsupported query-block output source");
                    }
                }
                output_rows.push_back(std::move(row));
            }
        }

            if (!SortOutputRows(&output_rows)) return false;
            EmitOutputRows(output_rows, response);
            return true;
        };  // emit

        // Typed group-key fast paths gate on numeric result type (cmp_kind==0)
        // and prefix_len==0: a STRING column can carry non-canonical numeric
        // text ("01" vs "1") that byte-groups apart but collapses to one int64,
        // whereas numeric columns store canonical val_str so parse-success is
        // value<->byte 1:1. A single INT column keys an int64 map, two INT
        // columns a packed 128-bit map. A parse fallback (status 1) re-runs the
        // whole aggregation on the string path; a structural failure (status 2)
        // propagates.
        const bool group0_int =
            group_count >= 1 &&
            aggregate.group_columns(0).prefix_len() == 0 &&
            aggregate.group_columns(0).cmp_kind() == 0;
        const bool group1_int =
            group_count >= 2 &&
            aggregate.group_columns(1).prefix_len() == 0 &&
            aggregate.group_columns(1).cmp_kind() == 0;
        if (group_count == 1 && group0_int) {
            IntGroupMap int_groups;
            const int status = run_typed(int_groups);
            if (status == 0) return emit(int_groups);
            if (status == 2) return false;
            // status == 1: fall through to the string path.
        } else if (group_count == 2 && group0_int && group1_int) {
            Int2GroupMap int2_groups;
            const int status = run_typed(int2_groups);
            if (status == 0) return emit(int2_groups);
            if (status == 2) return false;
            // status == 1: fall through to the string path.
        }

        GroupMap groups;
        const int status = run_typed(groups);
        if (status != 0) return false;  // string path never parse-fails
        return emit(groups);
    }

    bool RunNodes(pb::TxExecuteQueryBlock::Response* response) {
        results_.resize(request_.nodes_size());
        for (int node_idx = 0; node_idx < request_.nodes_size(); ++node_idx) {
            const pb::QueryBlockNode& node = request_.nodes(node_idx);
            if (node.has_scan()) {
                if (!RunScan(node.scan(), &results_[node_idx])) return false;
                continue;
            }
            if (node.has_join()) {
                if (!RunJoin(node.join(), &results_[node_idx])) return false;
                continue;
            }
            if (node.has_filter()) {
                if (!RunTupleFilter(node.filter(), &results_[node_idx])) {
                    return false;
                }
                continue;
            }
            if (node.has_sub_block()) {
                if (!RunSubBlock(node.sub_block(), &results_[node_idx])) {
                    return false;
                }
                continue;
            }
            if (node.has_aggregate()) {
                if (node_idx != request_.nodes_size() - 1) {
                    return fail("aggregate must be the root node");
                }
                return RunAggregateAndEmit(node.aggregate(), response);
            }
            return fail("unknown query-block node");
        }
        if (request_.nodes_size() == 0) return fail("root node missing");
        return RunEmitRows(results_.back(), response);
    }

    bool TablesAreStillQuiet() const {
        for (size_t table_idx = 0; table_idx < stores_.size(); ++table_idx) {
            PaxStore* store = stores_[table_idx];
            if (store->slots_allocated() != slot_snapshots_[table_idx] ||
                store->overflow_count() != overflow_snapshots_[table_idx]) {
                return false;
            }
            const std::vector<uint64_t>& snapshot =
                write_counter_snapshots_[table_idx];
            for (size_t group_idx = 0; group_idx < snapshot.size();
                 ++group_idx) {
                PaxGroup* group = store->group(group_idx);
                const uint64_t current =
                    group == nullptr
                        ? 0
                        : group->write_counter.load(std::memory_order_acquire);
                if ((current & 1u) != 0 || current != snapshot[group_idx]) {
                    return false;
                }
            }
        }
        return true;
    }

    unsigned WorkerCount() const {
        const unsigned hardware_threads = std::thread::hardware_concurrency();
        return std::min<unsigned>(hardware_threads == 0 ? 8 : hardware_threads,
                                  32);
    }

    LineairDB::Database* db_;
    const pb::TxExecuteQueryBlock::Request& request_;
    const std::unordered_set<std::string>* external_keys_ = nullptr;
    uint32_t external_filter_table_ = kNoExternalFilterTable;
    uint32_t external_filter_column_ = 0;
    std::string error_;
    std::vector<PaxStore*> stores_;
    std::vector<std::vector<uint64_t>> write_counter_snapshots_;
    std::vector<uint64_t> slot_snapshots_;
    std::vector<uint64_t> overflow_snapshots_;
    std::vector<NodeResult> results_;
    std::vector<VirtualTable> virtual_tables_;
};

const std::string& default_error(const Executor& executor) {
    static const std::string kDefaultError = "query-block execution failed";
    return executor.error().empty() ? kDefaultError : executor.error();
}

}  // namespace

void ExecuteQueryBlock(
    LineairDB::Database* db,
    const LineairDB::Protocol::TxExecuteQueryBlock::Request& request,
    LineairDB::Protocol::TxExecuteQueryBlock::Response* response) {
    if (response == nullptr) return;
    response->Clear();

    try {
        Executor executor(db, request);
        if (!executor.Run(response)) {
            set_failure(response, default_error(executor));
            return;
        }
        response->set_ok(true);
    } catch (const std::exception& e) {
        set_failure(response, e.what());
    } catch (...) {
        set_failure(response, "query-block execution failed");
    }
}

}  // namespace query_block
