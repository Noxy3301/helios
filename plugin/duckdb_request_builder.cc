// Builds the duckdb-query request from the resolved statement (Query_block +
// Item trees). Every switch is exhaustive with a refusing default: an Item or
// Table_ref kind without a rule here is never emitted.
#include "duckdb_request_builder.hh"

#include <cstdint>
#include <cstring>
#include <set>
#include <unordered_map>
#include <vector>

#include "sql/field.h"
#include "sql/item.h"
#include "sql/item_cmpfunc.h"
#include "sql/item_subselect.h"
#include "sql/item_timefunc.h"
#include "sql/item_func.h"
#include "sql/item_sum.h"
#include "sql/nested_join.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "sql/visible_fields.h"

#include "helios_field_types.h"

namespace {

namespace pb = Helios::Protocol;
using Resolved = pb::TxExecuteDuckdbQuery;

struct Builder {
    THD* thd;
    Resolved::Request* request;
    std::string* why;
    // Table_ref -> relation id; one relation (and one table descriptor) per
    // alias, so a self-join gets two independent scans.
    std::unordered_map<const Table_ref*, uint32_t> relation_ids;
    uint32_t next_relation_id = 0;
    // Derived relations whose select list carries a DECIMAL AVG; a column
    // reference into one keeps that lineage for the comparison guard.
    std::set<uint32_t> avg_relations;

    bool refuse(const std::string& reason) {
        if (why->empty()) *why = reason;
        return false;
    }
};

bool build_expr(Builder& b, Item* item, Resolved::Expr* out);
bool build_block(Builder& b, Query_block* block,
                 Resolved::QueryBlock* out, bool ignore_limit = false);

// The resolved Item behind resolver-added wrappers. Only wrappers whose
// transparency is understood are unwrapped; anything else stays and hits
// the exhaustive switch.
Item* real_item(Item* item) {
    while (item != nullptr && item->type() == Item::REF_ITEM) {
        auto* ref = down_cast<Item_ref*>(item);
        // A view reference on an outer join's inner side is NULL for
        // null-complemented rows even when the underlying expression is
        // not. Unwrapping it would drop that meaning; keep the wrapper and
        // let the exhaustive switch refuse it.
        if (ref->ref_type() == Item_ref::VIEW_REF && ref->is_nullable() &&
            !ref->real_item()->is_nullable()) {
            return item;
        }
        item = ref->real_item();
    }
    return item;
}

bool type_of(Builder& b, Item* item, Resolved::ResolvedType* out) {
    out->set_nullable(item->is_nullable());
    switch (item->data_type()) {
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_LONGLONG:
            if (item->unsigned_flag) {
                return b.refuse("unsigned integer is unsupported");
            }
            out->set_kind(Resolved::INT64);
            return true;
        case MYSQL_TYPE_NEWDECIMAL: {
            uint32_t precision = item->decimal_precision();
            const uint32_t scale = item->decimals;
            // Columns are DEC64-bound by the PAX descriptor check; computed
            // results (SUM, products, divisions) widen and cross the wire
            // as text. MySQL result types can claim up to 65 digits;
            // DuckDB's ceiling is 38, and the profile's actual values fit.
            if (precision > 38 && scale <= 30) precision = 38;
            if (precision == 0 || precision > 38 || scale > precision) {
                return b.refuse("decimal precision is out of range");
            }
            out->set_kind(Resolved::DECIMAL);
            out->set_precision(precision);
            out->set_scale(scale);
            return true;
        }
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            out->set_kind(Resolved::VARCHAR);
            out->set_collation_id(item->collation.collation->number);
            return true;
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
            out->set_kind(Resolved::DATE);
            return true;
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            out->set_kind(Resolved::DATETIME);
            return true;
        default:
            return b.refuse("type " + std::to_string(item->data_type()) +
                            " is unsupported");
    }
}

// DuckDB computes decimal AVG through DOUBLE; a squeezed comparison must
// not inherit that approximation, so its value tree is scanned for one.
bool contains_decimal_avg(const Builder& b, const Resolved::Expr& expr) {
    if (expr.has_column()) {
        return b.avg_relations.count(expr.column().relation_id()) != 0;
    }
    if (expr.has_aggregate()) {
        const auto& aggregate = expr.aggregate();
        if (aggregate.kind() == Resolved::Aggregate::AVG &&
            expr.result_type().kind() == Resolved::DECIMAL) {
            return true;
        }
        return aggregate.has_arg() && contains_decimal_avg(b, aggregate.arg());
    }
    if (expr.has_arithmetic()) {
        return contains_decimal_avg(b, expr.arithmetic().left()) ||
               contains_decimal_avg(b, expr.arithmetic().right());
    }
    if (expr.has_cast()) return contains_decimal_avg(b, expr.cast().arg());
    if (expr.has_case_when()) {
        for (const auto& branch : expr.case_when().branches()) {
            if (contains_decimal_avg(b, branch.then())) return true;
        }
        return expr.case_when().has_else_result() &&
               contains_decimal_avg(b, expr.case_when().else_result());
    }
    if (expr.has_function()) {
        for (const auto& arg : expr.function().args()) {
            if (contains_decimal_avg(b, arg)) return true;
        }
        return false;
    }
    if (expr.has_subquery()) {
        for (const auto& item : expr.subquery().query().select()) {
            if (contains_decimal_avg(b, item.expression())) return true;
        }
        return false;
    }
    return false;
}

// The type MySQL compares two operands under. Mirrors the numeric/temporal
// part of MySQL's comparison-context rules; string comparison needs the
// collation machinery and is refused until that phase.
bool compare_type_of(Builder& b, const Resolved::Expr& left,
                     const Resolved::Expr& right, Resolved::ResolvedType* out) {
    const auto lk = left.result_type().kind();
    const auto rk = right.result_type().kind();
    if (lk == Resolved::INT64 && rk == Resolved::INT64) {
        out->set_kind(Resolved::INT64);
        return true;
    }
    if ((lk == Resolved::DECIMAL || lk == Resolved::INT64) &&
        (rk == Resolved::DECIMAL || rk == Resolved::INT64)) {
        Resolved::ResolvedType int_type;
        int_type.set_kind(Resolved::DECIMAL);
        int_type.set_precision(19);  // BIGINT needs 19 integer digits
        int_type.set_scale(0);
        const Resolved::ResolvedType& lhs =
            lk == Resolved::DECIMAL ? left.result_type() : int_type;
        const Resolved::ResolvedType& rhs =
            rk == Resolved::DECIMAL ? right.result_type() : int_type;
        const uint32_t scale = std::max(lhs.scale(), rhs.scale());
        const uint32_t integer = std::max(lhs.precision() - lhs.scale(),
                                          rhs.precision() - rhs.scale());
        // Beyond DECIMAL(38), DuckDB's binder keeps the scale and squeezes the
        // integer digits into the remaining width. The comparison stays exact
        // whenever the values fit; a value that does not fit raises in the cast
        // and the statement fails rather than falling back, because MySQL only
        // retries on the primary for failures raised before execution.
        if (integer + scale > 38 &&
            (contains_decimal_avg(b, left) || contains_decimal_avg(b, right))) {
            return b.refuse("AVG operand in an over-wide decimal comparison");
        }
        out->set_kind(Resolved::DECIMAL);
        out->set_precision(std::min<uint32_t>(integer + scale, 38));
        out->set_scale(scale);
        return true;
    }
    const bool l_temporal = lk == Resolved::DATE || lk == Resolved::DATETIME;
    const bool r_temporal = rk == Resolved::DATE || rk == Resolved::DATETIME;
    if (l_temporal && r_temporal) {
        out->set_kind(lk == Resolved::DATETIME || rk == Resolved::DATETIME
                          ? Resolved::DATETIME
                          : Resolved::DATE);
        return true;
    }
    // A temporal operand against a string: MySQL always compares the pair
    // as DATETIME (can_compare_as_dates), even when the column is DATE, so
    // the string's time part participates.
    if ((l_temporal && rk == Resolved::VARCHAR) ||
        (r_temporal && lk == Resolved::VARCHAR)) {
        out->set_kind(Resolved::DATETIME);
        return true;
    }
    if (lk == Resolved::VARCHAR && rk == Resolved::VARCHAR) {
        const uint32_t lc = left.result_type().collation_id();
        const uint32_t rc = right.result_type().collation_id();
        // utf8mb4_0900_ai_ci, utf8mb4_0900_bin and binary.
        if (lc == rc && (lc == 255 || lc == 309 || lc == 63)) {
            out->set_kind(Resolved::VARCHAR);
            out->set_collation_id(lc);
            return true;
        }
        return b.refuse("string comparison collation " + std::to_string(lc) +
                        "/" + std::to_string(rc) + " is unsupported");
    }
    return b.refuse("comparison outside the numeric/temporal profile");
}

bool build_int_literal(Builder& b, Item* item, Resolved::Expr* out) {
    if (item->unsigned_flag &&
        static_cast<uint64_t>(item->val_int()) > INT64_MAX) {
        return b.refuse("integer literal above the signed range");
    }
    out->mutable_result_type()->set_kind(Resolved::INT64);
    out->mutable_literal()->set_int_value(item->val_int());
    return true;
}

// Parses the literal's own text into unscaled digits + scale, so no float
// path touches the value.
bool build_decimal_literal(Builder& b, Item* item, Resolved::Expr* out) {
    StringBuffer<64> buffer;
    String* text = item->val_str(&buffer);
    if (text == nullptr) return b.refuse("decimal literal has no text");
    const char* p = text->ptr();
    size_t n = text->length();
    bool negative = false;
    size_t i = 0;
    if (i < n && (p[i] == '+' || p[i] == '-')) negative = p[i++] == '-';
    int64_t unscaled = 0;
    uint32_t scale = 0;
    bool seen_dot = false;
    uint32_t digits = 0;
    for (; i < n; i++) {
        if (p[i] == '.') {
            if (seen_dot) return b.refuse("malformed decimal literal");
            seen_dot = true;
            continue;
        }
        if (p[i] < '0' || p[i] > '9') {
            return b.refuse("malformed decimal literal");
        }
        if (++digits > 18) {
            return b.refuse("decimal literal above 18 digits");
        }
        unscaled = unscaled * 10 + (p[i] - '0');
        if (seen_dot) scale++;
    }
    if (digits == 0) return b.refuse("malformed decimal literal");
    auto* type = out->mutable_result_type();
    type->set_kind(Resolved::DECIMAL);
    type->set_precision(digits);
    type->set_scale(scale);
    auto* dec = out->mutable_literal()->mutable_decimal_value();
    dec->set_unscaled(negative ? -unscaled : unscaled);
    dec->set_scale(scale);
    return true;
}

bool build_comparison(Builder& b, Item_func* function,
                      Resolved::Comparison::Op op, Resolved::Expr* out) {
    if (function->argument_count() != 2) return b.refuse("comparison arity");
    out->mutable_result_type()->set_kind(Resolved::BOOL);
    auto* cmp = out->mutable_comparison();
    cmp->set_op(op);
    if (!build_expr(b, function->arguments()[0], cmp->mutable_left()) ||
        !build_expr(b, function->arguments()[1], cmp->mutable_right())) {
        return false;
    }
    return compare_type_of(b, cmp->left(), cmp->right(),
                           cmp->mutable_compare_as());
}

bool build_arithmetic(Builder& b, Item_func* function,
                      Resolved::Arithmetic::Op op, Resolved::Expr* out) {
    if (function->argument_count() != 2) return b.refuse("arithmetic arity");
    if (!type_of(b, function, out->mutable_result_type())) return false;
    auto* arith = out->mutable_arithmetic();
    arith->set_op(op);
    if (!build_expr(b, function->arguments()[0], arith->mutable_left()) ||
        !build_expr(b, function->arguments()[1], arith->mutable_right())) {
        return false;
    }
    // MySQL evaluates a temporal operand in arithmetic as its YYYYMMDD
    // number; DuckDB does date arithmetic in days. Constant temporal
    // expressions fold to literals before reaching here.
    for (const auto* side : {&arith->left(), &arith->right()}) {
        const auto kind = side->result_type().kind();
        if (kind == Resolved::DATE || kind == Resolved::DATETIME) {
            return b.refuse("temporal operand in arithmetic is unsupported");
        }
    }
    *arith->mutable_result_as() = out->result_type();
    return true;
}

bool build_func(Builder& b, Item_func* function, Resolved::Expr* out) {
    switch (function->functype()) {
        case Item_func::EQ_FUNC:
            return build_comparison(b, function, Resolved::Comparison::EQ,
                                    out);
        case Item_func::NE_FUNC:
            return build_comparison(b, function, Resolved::Comparison::NE,
                                    out);
        case Item_func::LT_FUNC:
            return build_comparison(b, function, Resolved::Comparison::LT,
                                    out);
        case Item_func::LE_FUNC:
            return build_comparison(b, function, Resolved::Comparison::LE,
                                    out);
        case Item_func::GT_FUNC:
            return build_comparison(b, function, Resolved::Comparison::GT,
                                    out);
        case Item_func::GE_FUNC:
            return build_comparison(b, function, Resolved::Comparison::GE,
                                    out);
        case Item_func::NOT_FUNC: {
            if (function->argument_count() != 1) return b.refuse("NOT arity");
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            return build_expr(b, function->arguments()[0],
                              out->mutable_not_()->mutable_arg());
        }
        case Item_func::ISNULL_FUNC:
        case Item_func::ISNOTNULL_FUNC: {
            if (function->argument_count() != 1) {
                return b.refuse("IS NULL arity");
            }
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            auto* test = out->mutable_is_null();
            test->set_negated(function->functype() ==
                              Item_func::ISNOTNULL_FUNC);
            return build_expr(b, function->arguments()[0],
                              test->mutable_arg());
        }
        case Item_func::BETWEEN: {
            auto* between_item = down_cast<Item_func_between*>(function);
            if (function->argument_count() != 3) {
                return b.refuse("BETWEEN arity");
            }
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            auto* between = out->mutable_between();
            between->set_negated(between_item->negated);
            if (!build_expr(b, function->arguments()[0],
                            between->mutable_value()) ||
                !build_expr(b, function->arguments()[1],
                            between->mutable_low()) ||
                !build_expr(b, function->arguments()[2],
                            between->mutable_high())) {
                return false;
            }
            Resolved::ResolvedType low_type;
            if (!compare_type_of(b, between->value(), between->low(),
                                 &low_type)) {
                return false;
            }
            Resolved::ResolvedType high_type;
            if (!compare_type_of(b, between->value(), between->high(),
                                 &high_type)) {
                return false;
            }
            // One comparison type governs both bounds, as MySQL aggregates
            // all three operands before installing its comparators.
            auto* compare_as = between->mutable_compare_as();
            const auto lk2 = low_type.kind();
            const auto hk2 = high_type.kind();
            if (lk2 == hk2) {
                if (lk2 == Resolved::VARCHAR &&
                    low_type.collation_id() != high_type.collation_id()) {
                    return b.refuse("BETWEEN bound collations differ");
                }
                if (lk2 == Resolved::DECIMAL) {
                    // Compose a type that holds both pairs; beyond
                    // DECIMAL(38) the scale is kept and the integer digits
                    // squeeze, as in compare_type_of. The AVG guard reads the
                    // original operand widths: a pairwise squeeze already
                    // hides behind a clamped pair type.
                    uint32_t raw_integer = 0;
                    uint32_t raw_scale = 0;
                    for (const auto* operand :
                         {&between->value(), &between->low(),
                          &between->high()}) {
                        const auto& type = operand->result_type();
                        if (type.kind() == Resolved::DECIMAL) {
                            raw_integer = std::max(
                                raw_integer, type.precision() - type.scale());
                            raw_scale = std::max(raw_scale, type.scale());
                        } else {
                            raw_integer = std::max(raw_integer, 19u);
                        }
                    }
                    if (raw_integer + raw_scale > 38 &&
                        (contains_decimal_avg(b, between->value()) ||
                         contains_decimal_avg(b, between->low()) ||
                         contains_decimal_avg(b, between->high()))) {
                        return b.refuse(
                            "AVG operand in an over-wide decimal comparison");
                    }
                    const uint32_t scale =
                        std::max(low_type.scale(), high_type.scale());
                    const uint32_t integer =
                        std::max(low_type.precision() - low_type.scale(),
                                 high_type.precision() - high_type.scale());
                    compare_as->set_kind(Resolved::DECIMAL);
                    compare_as->set_precision(
                        std::min<uint32_t>(integer + scale, 38));
                    compare_as->set_scale(scale);
                } else {
                    *compare_as = low_type;
                }
            } else if ((lk2 == Resolved::DATE && hk2 == Resolved::DATETIME) ||
                       (lk2 == Resolved::DATETIME && hk2 == Resolved::DATE)) {
                compare_as->set_kind(Resolved::DATETIME);
            } else if ((lk2 == Resolved::INT64 && hk2 == Resolved::DECIMAL) ||
                       (lk2 == Resolved::DECIMAL && hk2 == Resolved::INT64)) {
                *compare_as =
                    lk2 == Resolved::DECIMAL ? low_type : high_type;
            } else {
                return b.refuse("BETWEEN bound types disagree");
            }
            return true;
        }
        case Item_func::IN_FUNC: {
            auto* in_item = down_cast<Item_func_in*>(function);
            if (function->argument_count() < 2) return b.refuse("IN arity");
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            auto* in_list = out->mutable_in_list();
            in_list->set_negated(in_item->negated);
            if (!build_expr(b, function->arguments()[0],
                            in_list->mutable_value())) {
                return false;
            }
            for (uint i = 1; i < function->argument_count(); i++) {
                if (!build_expr(b, function->arguments()[i],
                                in_list->add_list())) {
                    return false;
                }
            }
            // MySQL picks a comparator per member; DuckDB casts every
            // member to one common type. The translations agree only when
            // the members already share the value's kind.
            const auto value_kind = in_list->value().result_type().kind();
            for (const auto& element : in_list->list()) {
                if (element.result_type().kind() != value_kind) {
                    return b.refuse("IN list mixes value kinds");
                }
            }
            return true;
        }
        case Item_func::PLUS_FUNC:
            return build_arithmetic(b, function, Resolved::Arithmetic::ADD,
                                    out);
        case Item_func::MINUS_FUNC:
            return build_arithmetic(b, function, Resolved::Arithmetic::SUB,
                                    out);
        case Item_func::MUL_FUNC:
            return build_arithmetic(b, function, Resolved::Arithmetic::MUL,
                                    out);
        case Item_func::DIV_FUNC:
            return build_arithmetic(b, function, Resolved::Arithmetic::DIV,
                                    out);
        case Item_func::MOD_FUNC:
            return build_arithmetic(b, function, Resolved::Arithmetic::MOD,
                                    out);
        case Item_func::LIKE_FUNC: {
            auto* like_item = down_cast<Item_func_like*>(function);
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            auto* like = out->mutable_like();
            if (!build_expr(b, function->arguments()[0],
                            like->mutable_value()) ||
                !build_expr(b, function->arguments()[1],
                            like->mutable_pattern())) {
                return false;
            }
            if (like->value().result_type().kind() != Resolved::VARCHAR ||
                like->pattern().result_type().kind() != Resolved::VARCHAR) {
                return b.refuse("LIKE operands outside the string profile");
            }
            if (!like_item->escape_is_evaluated()) {
                return b.refuse("LIKE escape is not a constant");
            }
            like->set_escape(like_item->escape());
            const auto& value_type = like->value().result_type();
            const auto& pattern_type = like->pattern().result_type();
            if (pattern_type.kind() == Resolved::VARCHAR &&
                pattern_type.collation_id() != value_type.collation_id()) {
                return b.refuse("LIKE operand collations differ");
            }
            like->set_collation_id(value_type.collation_id());
            return true;
        }
        case Item_func::CASE_FUNC: {
            auto* case_item = down_cast<Item_func_case*>(function);
            if (case_item->get_first_expr_num() != -1) {
                return b.refuse("simple CASE is unsupported");
            }
            if (case_item->get_else_expr_num() == -1) {
                return b.refuse("CASE without ELSE is unsupported");
            }
            const uint count = function->argument_count();
            if (count < 3 || count % 2 == 0) {
                return b.refuse("CASE argument layout is unsupported");
            }
            if (!type_of(b, function, out->mutable_result_type())) {
                return false;
            }
            auto* case_when = out->mutable_case_when();
            const uint branches = (count - 1) / 2;
            for (uint i = 0; i < branches; i++) {
                auto* branch = case_when->add_branches();
                if (!build_expr(b, function->arguments()[2 * i],
                                branch->mutable_when())) {
                    return false;
                }
                if (branch->when().result_type().kind() != Resolved::BOOL) {
                    return b.refuse("CASE condition is not Boolean "
                                    "(simple CASE is unsupported)");
                }
                if (!build_expr(b, function->arguments()[2 * i + 1],
                                branch->mutable_then())) {
                    return false;
                }
            }
            return build_expr(b, function->arguments()[count - 1],
                              case_when->mutable_else_result());
        }
        case Item_func::YEAR_FUNC: {
            if (function->argument_count() != 1) return b.refuse("YEAR arity");
            out->mutable_result_type()->set_kind(Resolved::INT64);
            auto* call = out->mutable_function();
            call->set_fn(Resolved::FunctionCall::EXTRACT_YEAR);
            return build_expr(b, function->arguments()[0], call->add_args());
        }
        case Item_func::EXTRACT_FUNC: {
            auto* extract = down_cast<Item_extract*>(function);
            if (extract->int_type != INTERVAL_YEAR ||
                function->argument_count() != 1) {
                return b.refuse("EXTRACT outside YEAR is unsupported");
            }
            out->mutable_result_type()->set_kind(Resolved::INT64);
            auto* call = out->mutable_function();
            call->set_fn(Resolved::FunctionCall::EXTRACT_YEAR);
            return build_expr(b, function->arguments()[0], call->add_args());
        }
        default: {
            const std::string name = function->func_name();
            if (name == "<in_optimizer>" &&
                function->argument_count() == 1) {
                // The wrapper is an execution artifact around the
                // quantified subquery; the subselect carries the meaning.
                return build_expr(b, function->arguments()[0], out);
            }
            if (name == "substr" && function->argument_count() == 3) {
                // MySQL and DuckDB disagree on position 0 and negative
                // lengths; the profile admits only constant positive
                // positions and non-negative lengths, where they agree.
                Item* position = real_item(function->arguments()[1]);
                Item* length = real_item(function->arguments()[2]);
                if (position == nullptr || length == nullptr ||
                    position->type() != Item::INT_ITEM ||
                    length->type() != Item::INT_ITEM ||
                    position->val_int() < 1 || length->val_int() < 0) {
                    return b.refuse("substr outside the constant-positive "
                                    "profile");
                }
                if (!type_of(b, function, out->mutable_result_type())) {
                    return false;
                }
                auto* call = out->mutable_function();
                call->set_fn(Resolved::FunctionCall::SUBSTRING);
                for (uint i = 0; i < 3; i++) {
                    if (!build_expr(b, function->arguments()[i],
                                    call->add_args())) {
                        return false;
                    }
                }
                return true;
            }
            if (name == "ascii" && function->argument_count() == 1) {
                // Served by the registered byte-valued mysql_ascii, not
                // DuckDB's codepoint-valued ascii().
                out->mutable_result_type()->set_kind(Resolved::INT64);
                auto* call = out->mutable_function();
                call->set_fn(Resolved::FunctionCall::ASCII);
                return build_expr(b, function->arguments()[0],
                                  call->add_args());
            }
            return b.refuse(std::string("function '") + name +
                            "' is unsupported");
        }
    }
}

bool build_subselect(Builder& b, Item_subselect* subselect,
                     Resolved::Expr* out) {
    Query_expression* unit = subselect->unit;
    if (unit == nullptr || unit->is_set_operation()) {
        return b.refuse("subquery shape is unsupported");
    }
    auto* subquery = out->mutable_subquery();
    switch (subselect->substype()) {
        case Item_subselect::SINGLEROW_SUBS: {
            subquery->set_kind(Resolved::Subquery::SCALAR);
            if (!build_block(b, unit->first_query_block(),
                             subquery->mutable_query())) {
                return false;
            }
            if (subquery->query().select_size() != 1) {
                return b.refuse("scalar subquery with several columns");
            }
            *out->mutable_result_type() =
                subquery->query().select(0).expression().result_type();
            return true;
        }
        case Item_subselect::EXISTS_SUBS: {
            auto* exists = down_cast<Item_exists_subselect*>(subselect);
            bool negated;
            switch (exists->value_transform) {
                case Item::BOOL_IDENTITY:
                case Item::BOOL_IS_TRUE:
                    negated = false;
                    break;
                case Item::BOOL_NEGATED:
                case Item::BOOL_IS_FALSE:
                    negated = true;
                    break;
                default:
                    return b.refuse("EXISTS truth transform is unsupported");
            }
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            subquery->set_kind(Resolved::Subquery::EXISTS);
            subquery->set_negated(negated);
            return build_block(b, unit->first_query_block(),
                               subquery->mutable_query(),
                               /*ignore_limit=*/true);
        }
        case Item_subselect::IN_SUBS: {
            auto* in_subselect = down_cast<Item_in_subselect*>(subselect);
            bool negated;
            bool fold_unknown = false;  // IS TRUE / IS FALSE: UNKNOWN -> 0
            switch (in_subselect->value_transform) {
                case Item::BOOL_IDENTITY:
                    negated = false;
                    break;
                case Item::BOOL_IS_TRUE:
                    negated = false;
                    fold_unknown = true;
                    break;
                case Item::BOOL_NEGATED:
                    negated = true;
                    break;
                case Item::BOOL_IS_FALSE:
                    negated = true;
                    fold_unknown = true;
                    break;
                default:
                    return b.refuse("IN truth transform is unsupported");
            }
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            Resolved::Expr in_expr;
            in_expr.mutable_result_type()->set_kind(Resolved::BOOL);
            auto* in_subquery = in_expr.mutable_subquery();
            in_subquery->set_kind(Resolved::Subquery::IN);
            in_subquery->set_negated(negated);
            if (!build_expr(b, in_subselect->left_expr,
                            in_subquery->mutable_left())) {
                return false;
            }
            if (!build_block(b, unit->first_query_block(),
                             in_subquery->mutable_query(),
                             /*ignore_limit=*/true)) {
                return false;
            }
            if (in_subquery->query().select_size() != 1) {
                return b.refuse("IN subquery with several columns");
            }
            if (!fold_unknown) {
                *out->mutable_subquery() = std::move(*in_subquery);
                return true;
            }
            // CASE WHEN <in> THEN 1 ELSE 0 END evaluates the predicate once
            // and sends UNKNOWN to the ELSE, which is what IS TRUE and
            // IS FALSE mean.
            auto* fold = out->mutable_case_when();
            auto* branch = fold->add_branches();
            *branch->mutable_when() = std::move(in_expr);
            branch->mutable_then()->mutable_result_type()->set_kind(
                Resolved::BOOL);
            branch->mutable_then()->mutable_literal()->set_bool_value(true);
            fold->mutable_else_result()->mutable_result_type()->set_kind(
                Resolved::BOOL);
            fold->mutable_else_result()->mutable_literal()->set_bool_value(
                false);
            return true;
        }
        default:
            return b.refuse("subquery kind is unsupported");
    }
}

bool build_aggregate(Builder& b, Item_sum* sum, Resolved::Expr* out) {
    if (!type_of(b, sum, out->mutable_result_type())) return false;
    auto* aggregate = out->mutable_aggregate();
    bool distinct = false;
    Resolved::Aggregate::Kind kind;
    switch (sum->sum_func()) {
        case Item_sum::COUNT_FUNC: {
            out->mutable_result_type()->set_kind(Resolved::INT64);
            Item* arg = sum->argument_count() == 1
                            ? real_item(sum->get_arg(0))
                            : nullptr;
            // COUNT over a never-null constant is COUNT(*): the same rows
            // are counted, and this is the shape MySQL itself uses.
            if (arg != nullptr && arg->const_item() && !arg->is_nullable()) {
                aggregate->set_kind(Resolved::Aggregate::COUNT_STAR);
                return true;
            }
            kind = Resolved::Aggregate::COUNT;
            break;
        }
        case Item_sum::COUNT_DISTINCT_FUNC:
            out->mutable_result_type()->set_kind(Resolved::INT64);
            kind = Resolved::Aggregate::COUNT;
            distinct = true;
            break;
        case Item_sum::SUM_FUNC:
            kind = Resolved::Aggregate::SUM;
            break;
        case Item_sum::SUM_DISTINCT_FUNC:
            kind = Resolved::Aggregate::SUM;
            distinct = true;
            break;
        case Item_sum::AVG_FUNC:
            kind = Resolved::Aggregate::AVG;
            break;
        case Item_sum::AVG_DISTINCT_FUNC:
            kind = Resolved::Aggregate::AVG;
            distinct = true;
            break;
        case Item_sum::MIN_FUNC:
            kind = Resolved::Aggregate::MIN;
            break;
        case Item_sum::MAX_FUNC:
            kind = Resolved::Aggregate::MAX;
            break;
        default:
            return b.refuse("aggregate is unsupported");
    }
    if (sum->argument_count() != 1) return b.refuse("aggregate arity");
    aggregate->set_kind(kind);
    aggregate->set_distinct(distinct);
    return build_expr(b, sum->get_arg(0), aggregate->mutable_arg());
}

// The row path emits evaluation warnings per row; a plan-time fold must not
// leak a speculative one. Evaluation runs under an isolated diagnostics
// area, and any raised condition refuses the fold.
struct ScopedDiagnostics {
    THD* thd;
    Diagnostics_area area{false};
    explicit ScopedDiagnostics(THD* thd_arg) : thd(thd_arg) {
        thd->push_diagnostics_area(&area, false);
    }
    ~ScopedDiagnostics() { thd->pop_diagnostics_area(); }
    bool clean() const {
        return !area.is_error() && area.current_statement_cond_count() == 0;
    }
};

// const_item() alone proves table independence, not purity: a constant tree
// can still hide a side-effecting call (GET_LOCK) that must not run at plan
// time. Only literals, temporal casts, and interval arithmetic over pure
// arguments are evaluated.
bool const_tree_is_pure(Item* item) {
    item = real_item(item);
    if (item == nullptr) return false;
    if (item->basic_const_item()) return true;
    if (item->type() != Item::FUNC_ITEM) return false;
    auto* function = down_cast<Item_func*>(item);
    if (function->functype() != Item_func::DATEADD_FUNC &&
        function->functype() != Item_func::TYPECAST_FUNC) {
        return false;
    }
    for (uint i = 0; i < function->argument_count(); ++i) {
        if (!const_tree_is_pure(function->arguments()[i])) return false;
    }
    return true;
}

// DATE_ADD over a string constant resolves as a character type in MySQL;
// the folded string keeps that type and the comparison rules already treat
// it against temporal operands.
bool build_string_const(Builder& b, Item* item, Resolved::Expr* out) {
    if (!type_of(b, item, out->mutable_result_type())) return false;
    StringBuffer<64> buffer;
    String* text;
    bool clean;
    {
        ScopedDiagnostics diagnostics(b.thd);
        text = item->val_str(&buffer);
        clean = diagnostics.clean();
    }
    if (!clean) {
        return b.refuse("constant evaluation raised a condition");
    }
    if (item->null_value) {
        out->mutable_literal()->set_null_value(true);
        return true;
    }
    if (text == nullptr) {
        return b.refuse("constant string expression evaluation failed");
    }
    out->mutable_literal()->set_string_value(
        std::string(text->ptr(), text->length()));
    return true;
}

// MySQL keeps DATE'...' literals and constant temporal arithmetic (interval
// addition) as function items; evaluating them here lets them cross as plain
// literals instead of unsupported functions.
bool build_temporal_const(Builder& b, Item* item, Resolved::Expr* out) {
    if (!type_of(b, item, out->mutable_result_type())) return false;
    MYSQL_TIME time;
    auto* literal = out->mutable_literal();
    bool failed;
    bool clean;
    {
        ScopedDiagnostics diagnostics(b.thd);
        failed = item->get_date(&time, TIME_FUZZY_DATE);
        clean = diagnostics.clean();
    }
    if (!clean) {
        return b.refuse("constant evaluation raised a condition");
    }
    if (failed) {
        if (item->null_value) {
            literal->set_null_value(true);
            return true;
        }
        return b.refuse("constant temporal expression evaluation failed");
    }
    // Zero dates, year zero, and ALLOW_INVALID_DATES values ('2001-02-31')
    // have no DuckDB equivalent.
    static constexpr uint8_t kDaysInMonth[] = {31, 28, 31, 30, 31, 30,
                                               31, 31, 30, 31, 30, 31};
    const bool leap = (time.year % 4 == 0 && time.year % 100 != 0) ||
                      time.year % 400 == 0;
    if (time.year == 0 || time.month == 0 || time.month > 12 ||
        time.day == 0 ||
        time.day > kDaysInMonth[time.month - 1] +
                       (time.month == 2 && leap)) {
        return b.refuse("date constant outside the Gregorian calendar");
    }
    auto* date = out->result_type().kind() == Resolved::DATE
                     ? literal->mutable_date_value()
                     : literal->mutable_datetime_value()->mutable_date();
    date->set_year(time.year);
    date->set_month(time.month);
    date->set_day(time.day);
    if (out->result_type().kind() != Resolved::DATE) {
        auto* datetime = literal->mutable_datetime_value();
        datetime->set_hour(time.hour);
        datetime->set_minute(time.minute);
        datetime->set_second(time.second);
        datetime->set_microsecond(time.second_part);
    }
    return true;
}

bool build_expr(Builder& b, Item* item, Resolved::Expr* out) {
    item = real_item(item);
    if (item == nullptr) return b.refuse("null item");
    switch (item->data_type()) {
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
            if (item->const_item() && const_tree_is_pure(item)) {
                return build_temporal_const(b, item, out);
            }
            break;
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
            if (item->type() == Item::FUNC_ITEM && item->const_item() &&
                const_tree_is_pure(item)) {
                return build_string_const(b, item, out);
            }
            break;
        default:
            break;
    }
    switch (item->type()) {
        case Item::FIELD_ITEM: {
            auto* field_item = down_cast<Item_field*>(item);
            const Table_ref* table_ref = field_item->table_ref;
            auto found = b.relation_ids.find(table_ref);
            if (found == b.relation_ids.end()) {
                return b.refuse("column of a table outside the FROM tree");
            }
            if (!type_of(b, item, out->mutable_result_type())) return false;
            auto* column = out->mutable_column();
            column->set_relation_id(found->second);
            column->set_column_ordinal(field_item->field->field_index());
            return true;
        }
        case Item::INT_ITEM:
            return build_int_literal(b, item, out);
        case Item::DECIMAL_ITEM:
            return build_decimal_literal(b, item, out);
        case Item::STRING_ITEM: {
            StringBuffer<64> buffer;
            String* text = item->val_str(&buffer);
            if (text == nullptr) return b.refuse("string literal");
            auto* type = out->mutable_result_type();
            type->set_kind(Resolved::VARCHAR);
            type->set_collation_id(item->collation.collation->number);
            out->mutable_literal()->set_string_value(
                std::string(text->ptr(), text->length()));
            return true;
        }
        case Item::COND_ITEM: {
            auto* cond = down_cast<Item_cond*>(item);
            Resolved::Logical::Op op;
            if (cond->functype() == Item_func::COND_AND_FUNC) {
                op = Resolved::Logical::AND;
            } else if (cond->functype() == Item_func::COND_OR_FUNC) {
                op = Resolved::Logical::OR;
            } else {
                return b.refuse("logical connective is unsupported");
            }
            out->mutable_result_type()->set_kind(Resolved::BOOL);
            auto* logical = out->mutable_logical();
            logical->set_op(op);
            List_iterator<Item> arguments(*cond->argument_list());
            for (Item* argument = arguments++; argument != nullptr;
                 argument = arguments++) {
                // IN-to-EXISTS helpers are a MySQL execution strategy, not
                // statement meaning; the quantified predicate itself is
                // built where it appears.
                if (argument->created_by_in2exists()) continue;
                if (!build_expr(b, argument, logical->add_args())) {
                    return false;
                }
            }
            if (logical->args_size() == 0) {
                return b.refuse("logical connective with no surviving terms");
            }
            if (logical->args_size() == 1) {
                Resolved::Expr only = logical->args(0);
                *out = std::move(only);
                return true;
            }
            return true;
        }
        case Item::FUNC_ITEM:
            return build_func(b, down_cast<Item_func*>(item), out);
        case Item::SUM_FUNC_ITEM:
            return build_aggregate(b, down_cast<Item_sum*>(item), out);
        case Item::SUBSELECT_ITEM:
            return build_subselect(b, down_cast<Item_subselect*>(item),
                                   out);
        default:
            return b.refuse("item type " + std::to_string(item->type()) +
                            " is unsupported");
    }
}

bool build_base_table(Builder& b, Table_ref* table_ref,
                      Resolved::Relation* out) {
    TABLE* table = table_ref->table;
    if (table == nullptr || table->s == nullptr) {
        return b.refuse("table has no open TABLE");
    }
    std::vector<uint32_t> pax_kinds;
    std::vector<int32_t> pax_scales;
    std::vector<uint32_t> pax_widths =
        compute_pax_field_widths(table, &pax_kinds, &pax_scales);
    if (pax_widths.empty()) {
        return b.refuse("table is not PAX-eligible");
    }
    const uint32_t relation_id = b.next_relation_id++;
    if (!b.relation_ids.emplace(table_ref, relation_id).second) {
        return b.refuse("table appears twice in the FROM tree");
    }
    auto* base = out->mutable_base();
    base->set_relation_id(relation_id);
    base->set_table_desc_index(
        static_cast<uint32_t>(b.request->tables_size()));
    auto* table_desc = b.request->add_tables();
    table_desc->set_table_name(table->s->normalized_path.str);
    for (uint i = 0; i < table->s->fields; i++) {
        Field* field = table->field[i];
        auto* column = table_desc->add_columns();
        // Entry 0 of the width/kind/scale vectors is the row null-flags
        // field, not a named column.
        column->set_pax_kind(pax_kinds[i + 1]);
        column->set_pax_width(pax_widths[i + 1]);
        column->set_pax_scale(pax_scales[i + 1]);
        // A zero-length field is '' or NULL; only this bit tells them apart.
        column->set_null_bit(
            field->is_nullable()
                ? 1 + field->null_offset() * 8 +
                      static_cast<uint32_t>(__builtin_ctz(field->null_bit))
                : 0);
    }
    return true;
}

// Builds one join list. MySQL stores the list in reverse of the written
// order; restoring that order and folding left keeps LEFT JOIN sides
// correct.
bool build_nest(Builder& b, const mem_root_deque<Table_ref*>& nest,
                Resolved::Relation* out) {
    std::vector<Table_ref*> ordered;
    for (Table_ref* table_ref : nest) ordered.push_back(table_ref);
    std::reverse(ordered.begin(), ordered.end());
    if (ordered.empty()) return b.refuse("empty join list");

    Resolved::Relation accumulated;
    for (size_t i = 0; i < ordered.size(); i++) {
        Table_ref* table_ref = ordered[i];
        Resolved::Relation leaf;
        if (table_ref->nested_join != nullptr) {
            if (table_ref->is_sj_or_aj_nest()) {
                return b.refuse("semijoin nest is not yet translated");
            }
            if (!build_nest(b, table_ref->nested_join->m_tables, &leaf)) {
                return false;
            }
        } else if (table_ref->is_view_or_derived()) {
            if (!table_ref->uses_materialization()) {
                return b.refuse("merged view wrapper in the join list");
            }
            Query_expression* unit = table_ref->derived_query_expression();
            if (unit == nullptr || unit->is_set_operation()) {
                return b.refuse("derived table shape is unsupported");
            }
            const uint32_t relation_id = b.next_relation_id++;
            if (!b.relation_ids.emplace(table_ref, relation_id).second) {
                return b.refuse("derived table appears twice");
            }
            auto* derived = leaf.mutable_derived();
            derived->set_relation_id(relation_id);
            if (!build_block(b, unit->first_query_block(),
                             derived->mutable_query())) {
                return false;
            }
            for (const auto& item : derived->query().select()) {
                if (contains_decimal_avg(b, item.expression())) {
                    b.avg_relations.insert(relation_id);
                    break;
                }
            }
        } else {
            if (!build_base_table(b, table_ref, &leaf)) return false;
        }

        if (i == 0) {
            if (table_ref->join_cond() != nullptr || table_ref->outer_join) {
                return b.refuse("first join list entry carries a condition");
            }
            accumulated = std::move(leaf);
            continue;
        }
        Resolved::Relation joined;
        auto* join = joined.mutable_join();
        *join->mutable_left() = std::move(accumulated);
        *join->mutable_right() = std::move(leaf);
        if (table_ref->outer_join) {
            if (table_ref->join_cond() == nullptr) {
                return b.refuse("outer join without a condition");
            }
            join->set_kind(Resolved::JoinRel::LEFT);
            if (!build_expr(b, table_ref->join_cond(),
                            join->mutable_condition())) {
                return false;
            }
        } else if (table_ref->join_cond() != nullptr) {
            join->set_kind(Resolved::JoinRel::INNER);
            if (!build_expr(b, table_ref->join_cond(),
                            join->mutable_condition())) {
                return false;
            }
        } else {
            // Resolution moved inner-join conditions into WHERE; what is
            // left is a plain cross product under the WHERE filter.
            join->set_kind(Resolved::JoinRel::CROSS);
        }
        accumulated = std::move(joined);
    }
    *out = std::move(accumulated);
    return true;
}

bool build_block(Builder& b, Query_block* block,
                 Resolved::QueryBlock* out, bool ignore_limit) {
    if (block == nullptr) return b.refuse("missing query block");
    // FOUND_ROWS() reads the unbounded count from the executor; the OLAP path
    // only reports rows DuckDB returned after applying LIMIT.
    if (block->active_options() & OPTION_FOUND_ROWS) {
        return b.refuse("SQL_CALC_FOUND_ROWS is unsupported");
    }
    // A session SQL_SELECT_LIMIT applies only without an explicit LIMIT.
    if (block->select_limit == nullptr && block->m_use_select_limit &&
        b.thd->variables.select_limit != HA_POS_ERROR) {
        return b.refuse("session sql_select_limit is unsupported");
    }
    // The EXISTS strategy injects LIMIT 1 into the subquery it rewrites;
    // m_internal_limit marks exactly that injection and is dropped. A limit
    // the client wrote is meaning and crosses the wire. OFFSET is always
    // the client's: the injection never adds one.
    if (block->has_limit() && !(ignore_limit && block->m_internal_limit)) {
        out->set_limit(block->select_limit->val_uint());
    }
    if (block->offset_limit != nullptr) {
        out->set_offset(block->offset_limit->val_uint());
    }
    if (block->is_grouped() && block->olap != UNSPECIFIED_OLAP_TYPE) {
        return b.refuse("ROLLUP is unsupported");
    }
    if (!block->m_windows.is_empty()) {
        return b.refuse("window functions are unsupported");
    }
    if (!build_nest(b, block->m_table_nest, out->mutable_from())) {
        return false;
    }
    if (block->where_cond() != nullptr &&
        !block->where_cond()->created_by_in2exists()) {
        if (!build_expr(b, block->where_cond(), out->mutable_where())) {
            return false;
        }
    }
    for (ORDER* group = block->group_list.first; group != nullptr;
         group = group->next) {
        if (!build_expr(b, *group->item, out->add_group_by())) {
            return false;
        }
    }
    if (block->having_cond() != nullptr &&
        !block->having_cond()->created_by_in2exists()) {
        if (!build_expr(b, block->having_cond(), out->mutable_having())) {
            return false;
        }
    }
    for (Item* item : block->visible_fields()) {
        if (!build_expr(b, item, out->add_select()->mutable_expression())) {
            return false;
        }
    }
    for (ORDER* order = block->order_list.first; order != nullptr;
         order = order->next) {
        auto* key = out->add_order_by();
        if (!build_expr(b, *order->item, key->mutable_expression())) {
            return false;
        }
        const bool descending = order->direction == ORDER_DESC;
        key->set_descending(descending);
        // MySQL sorts NULLs first ascending and last descending.
        key->set_nulls_first(!descending);
    }
    if (block->is_distinct()) out->set_distinct(true);
    return true;
}

}  // namespace

bool build_olap_request(THD* thd, LEX* lex,
                        Resolved::Request* request, std::string* why) {
    request->Clear();
    why->clear();
    if (lex == nullptr || lex->unit == nullptr) {
        Builder{thd, request, why}.refuse("no statement");
        return false;
    }
    if (lex->unit->is_set_operation()) {
        Builder{thd, request, why}.refuse("set operations are unsupported");
        return false;
    }
    Builder b{thd, request, why};
    Query_block* block = lex->unit->first_query_block();
    if (!build_block(b, block, request->mutable_root())) {
        request->Clear();
        return false;
    }
    request->set_expected_output_count(
        static_cast<uint32_t>(request->root().select_size()));
    return true;
}
