// Builds DuckDB's parsed AST from a duckdb-query request. Every switch is
// exhaustive with a refusing default: a node kind without a rule here fails
// the request instead of flowing through.
#include "duckdb_ast_builder.hh"

#include <algorithm>
#include <limits>
#include <set>
#include <unordered_map>

#include "duckdb.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb_bridge {
namespace {

namespace pb = LineairDB::Protocol;
using Resolved = pb::TxExecuteDuckdbQuery;
using duckdb::ExpressionType;
using duckdb::LogicalType;
using duckdb::make_uniq;
using duckdb::ParsedExpression;
using duckdb::unique_ptr;
using duckdb::Value;

constexpr char kPaxScanFunction[] = "helios_pax_scan";
constexpr char kMysqlAiCiCollation[] = "utf8mb4_0900_ai_ci";
constexpr char kMysqlAiCiLike[] = "mysql_utf8mb4_0900_ai_ci_like";
constexpr char kMysqlAiCiNotLike[] = "mysql_utf8mb4_0900_ai_ci_not_like";
constexpr char kMysqlAiCiSortKey[] = "mysql_utf8mb4_0900_ai_ci_sort_key";

// MySQL collation ids whose comparison the bridge implements: 255 through
// the registered strnxfrm collation, 309/63 as byte comparison.
bool CollationIsByteSafe(uint32_t id) { return id == 309 || id == 63; }
bool CollationIsAiCi(uint32_t id) { return id == 255; }

// Shared build state. `error` is sticky: the first refusal wins and every
// later step returns null without overwriting it.
struct Builder {
    const Resolved::Request& request;
    const std::vector<uintptr_t>& handles;
    std::string error;
    // relation_id -> shape. Base tables carry their descriptor so a column
    // reference can compare the scan's physical type against the declared
    // one; derived tables carry only their select size.
    struct RelationInfo {
        size_t column_count = 0;
        const pb::TxExecuteDuckdbQuery::TableDesc* desc = nullptr;
    };
    std::unordered_map<uint32_t, RelationInfo> relations;
    std::set<uint32_t> used_table_descs;

    bool Refuse(const std::string& why) {
        if (error.empty()) error = why;
        return false;
    }
};

std::string RelationAlias(uint32_t relation_id) {
    return "_r" + std::to_string(relation_id);
}
std::string ColumnAlias(uint32_t ordinal) {
    return "_c" + std::to_string(ordinal);
}

bool ResolveType(Builder& b, const Resolved::ResolvedType& type,
                 LogicalType* out) {
    switch (type.kind()) {
        case Resolved::INT64:
            *out = LogicalType::BIGINT;
            return true;
        case Resolved::DECIMAL: {
            const uint32_t width = type.precision();
            const uint32_t scale = type.scale();
            // Storage cells are DEC64, but a computed result (SUM over a
            // DEC64 column) legitimately widens; DuckDB's ceiling is 38.
            if (width == 0 || width > 38 || scale > width) {
                return b.Refuse("decimal type is out of range");
            }
            *out = LogicalType::DECIMAL(static_cast<uint8_t>(width),
                                        static_cast<uint8_t>(scale));
            return true;
        }
        case Resolved::VARCHAR:
            *out = LogicalType::VARCHAR;
            return true;
        case Resolved::DATE:
            *out = LogicalType::DATE;
            return true;
        case Resolved::DATETIME:
            *out = LogicalType::TIMESTAMP;
            return true;
        case Resolved::BOOL:
            *out = LogicalType::BOOLEAN;
            return true;
        default:
            return b.Refuse("unknown type kind " +
                            std::to_string(type.kind()));
    }
}

unique_ptr<ParsedExpression> BuildExpr(Builder& b, const Resolved::Expr& expr);
duckdb::unique_ptr<duckdb::SelectStatement> BuildBlock(
    Builder& b, const Resolved::QueryBlock& block);

bool LiteralMatchesType(Resolved::Literal::ValueCase value,
                        Resolved::TypeKind kind) {
    switch (value) {
        case Resolved::Literal::kIntValue:
            return kind == Resolved::INT64;
        case Resolved::Literal::kDecimalValue:
            return kind == Resolved::DECIMAL;
        case Resolved::Literal::kStringValue:
            return kind == Resolved::VARCHAR;
        case Resolved::Literal::kDateValue:
            return kind == Resolved::DATE;
        case Resolved::Literal::kDatetimeValue:
            return kind == Resolved::DATETIME;
        case Resolved::Literal::kBoolValue:
            return kind == Resolved::BOOL;
        case Resolved::Literal::kNullValue:
            return true;  // typed by result_type, checked at construction
        default:
            return false;
    }
}

unique_ptr<ParsedExpression> BuildLiteral(Builder& b,
                                          const Resolved::Expr& expr) {
    const auto& lit = expr.literal();
    if (!LiteralMatchesType(lit.value_case(), expr.result_type().kind())) {
        b.Refuse("literal value does not match its declared type");
        return nullptr;
    }
    switch (lit.value_case()) {
        case Resolved::Literal::kIntValue:
            return make_uniq<duckdb::ConstantExpression>(
                Value::BIGINT(lit.int_value()));
        case Resolved::Literal::kDecimalValue: {
            const auto& dec = lit.decimal_value();
            uint32_t width = expr.result_type().precision();
            if (width == 0) width = 18;
            if (width > 18 || dec.scale() > width) {
                b.Refuse("decimal literal outside the DEC64 profile");
                return nullptr;
            }
            return make_uniq<duckdb::ConstantExpression>(
                Value::DECIMAL(static_cast<int64_t>(dec.unscaled()),
                               static_cast<uint8_t>(width),
                               static_cast<uint8_t>(dec.scale())));
        }
        case Resolved::Literal::kStringValue:
            return make_uniq<duckdb::ConstantExpression>(
                Value(lit.string_value()));
        case Resolved::Literal::kDateValue: {
            const auto& d = lit.date_value();
            return make_uniq<duckdb::ConstantExpression>(
                Value::DATE(d.year(), static_cast<int32_t>(d.month()),
                            static_cast<int32_t>(d.day())));
        }
        case Resolved::Literal::kDatetimeValue: {
            const auto& dt = lit.datetime_value();
            const auto& d = dt.date();
            return make_uniq<duckdb::ConstantExpression>(Value::TIMESTAMP(
                d.year(), static_cast<int32_t>(d.month()),
                static_cast<int32_t>(d.day()),
                static_cast<int32_t>(dt.hour()),
                static_cast<int32_t>(dt.minute()),
                static_cast<int32_t>(dt.second()),
                static_cast<int32_t>(dt.microsecond())));
        }
        case Resolved::Literal::kBoolValue:
            return make_uniq<duckdb::ConstantExpression>(
                Value::BOOLEAN(lit.bool_value()));
        case Resolved::Literal::kNullValue: {
            LogicalType type;
            if (!ResolveType(b, expr.result_type(), &type)) return nullptr;
            return make_uniq<duckdb::ConstantExpression>(Value(type));
        }
        default:
            b.Refuse("literal with no value");
            return nullptr;
    }
}

// A VARCHAR taking part in a comparison must use an implemented collation;
// refuses otherwise. Returns the input unchanged for non-VARCHAR types.
unique_ptr<ParsedExpression> AdmitComparisonString(
    Builder& b, const Resolved::ResolvedType& type,
    unique_ptr<ParsedExpression> built) {
    if (type.kind() != Resolved::VARCHAR) return built;
    if (CollationIsAiCi(type.collation_id())) {
        return make_uniq<duckdb::CollateExpression>(kMysqlAiCiCollation,
                                                    std::move(built));
    }
    if (CollationIsByteSafe(type.collation_id())) return built;
    b.Refuse("string collation " + std::to_string(type.collation_id()) +
             " is unsupported in a comparison");
    return nullptr;
}

// Attaches the registered MySQL collation to a VARCHAR expression used as a
// comparison, grouping, or ordering key. Byte-safe collations compare as
// bytes and need nothing; anything else was refused by the producer.
unique_ptr<ParsedExpression> CollateKey(Builder& b,
                                        const Resolved::ResolvedType& type,
                                        unique_ptr<ParsedExpression> built) {
    if (type.kind() != Resolved::VARCHAR) return built;
    if (CollationIsAiCi(type.collation_id())) {
        return make_uniq<duckdb::CollateExpression>(kMysqlAiCiCollation,
                                                    std::move(built));
    }
    if (CollationIsByteSafe(type.collation_id())) return built;
    b.Refuse("string collation " + std::to_string(type.collation_id()) +
             " is unsupported");
    return nullptr;
}

// Wraps `child` in a cast to `type` unless the wire says the child already
// has that exact type.
unique_ptr<ParsedExpression> CastTo(Builder& b, const Resolved::Expr& child,
                                    const Resolved::ResolvedType& type,
                                    unique_ptr<ParsedExpression> built) {
    if (type.kind() == Resolved::TYPE_UNSPECIFIED) return built;
    const auto& have = child.result_type();
    if (have.kind() == type.kind() && have.precision() == type.precision() &&
        have.scale() == type.scale()) {
        return built;
    }
    LogicalType target;
    if (!ResolveType(b, type, &target)) return nullptr;
    return make_uniq<duckdb::CastExpression>(target, std::move(built));
}

ExpressionType ComparisonOp(Builder& b, Resolved::Comparison::Op op) {
    switch (op) {
        case Resolved::Comparison::EQ:
            return ExpressionType::COMPARE_EQUAL;
        case Resolved::Comparison::NE:
            return ExpressionType::COMPARE_NOTEQUAL;
        case Resolved::Comparison::LT:
            return ExpressionType::COMPARE_LESSTHAN;
        case Resolved::Comparison::LE:
            return ExpressionType::COMPARE_LESSTHANOREQUALTO;
        case Resolved::Comparison::GT:
            return ExpressionType::COMPARE_GREATERTHAN;
        case Resolved::Comparison::GE:
            return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
        default:
            b.Refuse("unknown comparison operator");
            return ExpressionType::INVALID;
    }
}

unique_ptr<ParsedExpression> BuildFunction(Builder& b,
                                           const Resolved::Expr& expr) {
    const auto& fn = expr.function();
    duckdb::vector<unique_ptr<ParsedExpression>> args;
    for (const auto& arg : fn.args()) {
        auto built = BuildExpr(b, arg);
        if (built == nullptr) return nullptr;
        args.push_back(std::move(built));
    }
    switch (fn.fn()) {
        case Resolved::FunctionCall::EXTRACT_YEAR:
            if (args.size() != 1) {
                b.Refuse("extract_year arity");
                return nullptr;
            }
            return make_uniq<duckdb::FunctionExpression>("year",
                                                         std::move(args));
        case Resolved::FunctionCall::SUBSTRING:
            if (args.size() != 3) {
                b.Refuse("substring arity");
                return nullptr;
            }
            return make_uniq<duckdb::FunctionExpression>("substring",
                                                         std::move(args));
        case Resolved::FunctionCall::ASCII:
            if (args.size() != 1) {
                b.Refuse("ascii arity");
                return nullptr;
            }
            // The registered byte-valued form, not DuckDB's codepoint one.
            return make_uniq<duckdb::FunctionExpression>("mysql_ascii",
                                                         std::move(args));
        default:
            b.Refuse("unknown function " + std::to_string(fn.fn()));
            return nullptr;
    }
}

unique_ptr<ParsedExpression> BuildAggregate(Builder& b,
                                            const Resolved::Expr& expr) {
    const auto& agg = expr.aggregate();
    const char* name = nullptr;
    switch (agg.kind()) {
        case Resolved::Aggregate::COUNT_STAR:
            name = "count_star";
            break;
        case Resolved::Aggregate::COUNT:
            name = "count";
            break;
        case Resolved::Aggregate::SUM:
            name = "sum";
            break;
        case Resolved::Aggregate::AVG:
            name = "avg";
            break;
        case Resolved::Aggregate::MIN:
            name = "min";
            break;
        case Resolved::Aggregate::MAX:
            name = "max";
            break;
        default:
            b.Refuse("unknown aggregate " + std::to_string(agg.kind()));
            return nullptr;
    }
    duckdb::vector<unique_ptr<ParsedExpression>> args;
    if (agg.kind() == Resolved::Aggregate::COUNT_STAR) {
        if (agg.has_arg() || agg.distinct()) {
            b.Refuse("count_star takes no argument");
            return nullptr;
        }
    } else {
        if (!agg.has_arg()) {
            b.Refuse("aggregate without argument");
            return nullptr;
        }
        auto built = BuildExpr(b, agg.arg());
        if (built == nullptr) return nullptr;
        const auto& arg_type = agg.arg().result_type();
        if (arg_type.kind() == Resolved::VARCHAR &&
            !CollationIsAiCi(arg_type.collation_id()) &&
            !CollationIsByteSafe(arg_type.collation_id())) {
            b.Refuse("string collation " +
                     std::to_string(arg_type.collation_id()) +
                     " is unsupported in an aggregate");
            return nullptr;
        }
        if (arg_type.kind() == Resolved::VARCHAR &&
            CollationIsAiCi(arg_type.collation_id())) {
            if (agg.kind() == Resolved::Aggregate::MIN ||
                agg.kind() == Resolved::Aggregate::MAX) {
                // BindMinMax honours a collation carried by the argument
                // type; the CollateExpression puts it there.
                built = make_uniq<duckdb::CollateExpression>(
                    kMysqlAiCiCollation, std::move(built));
            } else if (agg.distinct()) {
                // DuckDB does not push a non-combinable collation into
                // aggregate children, so deduplicate the sort keys
                // themselves; COUNT of distinct keys equals COUNT of
                // distinct values under the collation.
                if (agg.kind() != Resolved::Aggregate::COUNT) {
                    b.Refuse("DISTINCT aggregate over a collated string");
                    return nullptr;
                }
                duckdb::vector<unique_ptr<ParsedExpression>> key_args;
                key_args.push_back(std::move(built));
                built = make_uniq<duckdb::FunctionExpression>(
                    kMysqlAiCiSortKey, std::move(key_args));
            }
        }
        args.push_back(std::move(built));
    }
    auto function =
        make_uniq<duckdb::FunctionExpression>(name, std::move(args));
    function->distinct = agg.distinct();
    return function;
}

unique_ptr<ParsedExpression> BuildSubquery(Builder& b,
                                           const Resolved::Expr& expr) {
    const auto& sub = expr.subquery();
    auto statement = BuildBlock(b, sub.query());
    if (statement == nullptr) return nullptr;
    auto node = make_uniq<duckdb::SubqueryExpression>();
    node->subquery = std::move(statement);
    switch (sub.kind()) {
        case Resolved::Subquery::SCALAR:
            if (sub.negated() || sub.has_left()) {
                b.Refuse("scalar subquery shape");
                return nullptr;
            }
            node->subquery_type = duckdb::SubqueryType::SCALAR;
            return node;
        case Resolved::Subquery::EXISTS:
            if (sub.has_left()) {
                b.Refuse("exists subquery shape");
                return nullptr;
            }
            node->subquery_type = duckdb::SubqueryType::EXISTS;
            break;
        case Resolved::Subquery::IN: {
            if (!sub.has_left()) {
                b.Refuse("in subquery without operand");
                return nullptr;
            }
            auto left = BuildExpr(b, sub.left());
            if (left == nullptr) return nullptr;
            // The shape DuckDB's own transformer produces for IN: ANY with
            // an equality comparison. NOT IN wraps it in NOT below.
            node->subquery_type = duckdb::SubqueryType::ANY;
            node->child = std::move(left);
            node->comparison_type = ExpressionType::COMPARE_EQUAL;
            break;
        }
        default:
            b.Refuse("unknown subquery kind");
            return nullptr;
    }
    if (sub.negated()) {
        return make_uniq<duckdb::OperatorExpression>(
            ExpressionType::OPERATOR_NOT, std::move(node));
    }
    return node;
}

unique_ptr<ParsedExpression> BuildExpr(Builder& b, const Resolved::Expr& expr) {
    switch (expr.node_case()) {
        case Resolved::Expr::kLiteral:
            return BuildLiteral(b, expr);
        case Resolved::Expr::kColumn: {
            const auto& col = expr.column();
            auto found = b.relations.find(col.relation_id());
            if (found == b.relations.end()) {
                b.Refuse("column names an unknown relation");
                return nullptr;
            }
            if (col.column_ordinal() >= found->second.column_count) {
                b.Refuse("column ordinal out of range");
                return nullptr;
            }
            unique_ptr<ParsedExpression> ref =
                make_uniq<duckdb::ColumnRefExpression>(
                    ColumnAlias(col.column_ordinal()),
                    RelationAlias(col.relation_id()));
            // PAX stores DATETIME and other untyped kinds as text; the scan
            // exposes them as VARCHAR. Casting at the reference gives every
            // consumer the declared type instead of the storage type.
            const auto* desc = found->second.desc;
            if (desc != nullptr &&
                desc->columns(static_cast<int>(col.column_ordinal()))
                        .pax_kind() == 0 &&
                expr.result_type().kind() != Resolved::VARCHAR &&
                expr.result_type().kind() != Resolved::TYPE_UNSPECIFIED) {
                LogicalType declared;
                if (!ResolveType(b, expr.result_type(), &declared)) {
                    return nullptr;
                }
                ref = make_uniq<duckdb::CastExpression>(declared,
                                                        std::move(ref));
            }
            return ref;
        }
        case Resolved::Expr::kLogical: {
            const auto& logical = expr.logical();
            ExpressionType type;
            if (logical.op() == Resolved::Logical::AND) {
                type = ExpressionType::CONJUNCTION_AND;
            } else if (logical.op() == Resolved::Logical::OR) {
                type = ExpressionType::CONJUNCTION_OR;
            } else {
                b.Refuse("unknown logical operator");
                return nullptr;
            }
            if (logical.args_size() < 2) {
                b.Refuse("logical operator arity");
                return nullptr;
            }
            duckdb::vector<unique_ptr<ParsedExpression>> children;
            for (const auto& arg : logical.args()) {
                auto built = BuildExpr(b, arg);
                if (built == nullptr) return nullptr;
                children.push_back(std::move(built));
            }
            return make_uniq<duckdb::ConjunctionExpression>(
                type, std::move(children));
        }
        case Resolved::Expr::kNot: {
            auto child = BuildExpr(b, expr.not_().arg());
            if (child == nullptr) return nullptr;
            return make_uniq<duckdb::OperatorExpression>(
                ExpressionType::OPERATOR_NOT, std::move(child));
        }
        case Resolved::Expr::kIsNull: {
            auto child = BuildExpr(b, expr.is_null().arg());
            if (child == nullptr) return nullptr;
            return make_uniq<duckdb::OperatorExpression>(
                expr.is_null().negated()
                    ? ExpressionType::OPERATOR_IS_NOT_NULL
                    : ExpressionType::OPERATOR_IS_NULL,
                std::move(child));
        }
        case Resolved::Expr::kComparison: {
            const auto& cmp = expr.comparison();
            const ExpressionType op = ComparisonOp(b, cmp.op());
            if (op == ExpressionType::INVALID) return nullptr;
            auto left = BuildExpr(b, cmp.left());
            if (left == nullptr) return nullptr;
            auto right = BuildExpr(b, cmp.right());
            if (right == nullptr) return nullptr;
            if (cmp.compare_as().kind() == Resolved::VARCHAR) {
                if (!CollationIsAiCi(cmp.compare_as().collation_id()) &&
                    !CollationIsByteSafe(cmp.compare_as().collation_id())) {
                    b.Refuse("comparison collation is unsupported");
                    return nullptr;
                }
                left = CollateKey(b, cmp.compare_as(), std::move(left));
                if (left == nullptr) return nullptr;
                right = CollateKey(b, cmp.compare_as(), std::move(right));
                if (right == nullptr) return nullptr;
            } else {
                left = CastTo(b, cmp.left(), cmp.compare_as(),
                              std::move(left));
                if (left == nullptr) return nullptr;
                right = CastTo(b, cmp.right(), cmp.compare_as(),
                               std::move(right));
                if (right == nullptr) return nullptr;
            }
            return make_uniq<duckdb::ComparisonExpression>(
                op, std::move(left), std::move(right));
        }
        case Resolved::Expr::kArithmetic: {
            const auto& arith = expr.arithmetic();
            const char* name = nullptr;
            switch (arith.op()) {
                case Resolved::Arithmetic::ADD:
                    name = "+";
                    break;
                case Resolved::Arithmetic::SUB:
                    name = "-";
                    break;
                case Resolved::Arithmetic::MUL:
                    name = "*";
                    break;
                case Resolved::Arithmetic::DIV:
                    name = "/";
                    break;
                case Resolved::Arithmetic::MOD:
                    name = "%";
                    break;
                default:
                    b.Refuse("unknown arithmetic operator");
                    return nullptr;
            }
            auto left = BuildExpr(b, arith.left());
            if (left == nullptr) return nullptr;
            auto right = BuildExpr(b, arith.right());
            if (right == nullptr) return nullptr;
            duckdb::vector<unique_ptr<ParsedExpression>> children;
            children.push_back(std::move(left));
            children.push_back(std::move(right));
            // MySQL's x/0 is NULL; DuckDB's "/" divides through. NULLIF
            // makes the divisor NULL and the division follow.
            if (arith.op() == Resolved::Arithmetic::DIV) {
                duckdb::vector<unique_ptr<ParsedExpression>> nullif_args;
                nullif_args.push_back(std::move(children[1]));
                nullif_args.push_back(make_uniq<duckdb::ConstantExpression>(
                    Value::BIGINT(0)));
                children[1] = make_uniq<duckdb::FunctionExpression>(
                    "nullif", std::move(nullif_args));
            }
            unique_ptr<ParsedExpression> function =
                make_uniq<duckdb::FunctionExpression>(name,
                                                      std::move(children));
            // The cast is unconditional: DuckDB types the result itself
            // (DECIMAL division becomes DOUBLE), so the wire's claim about
            // the child type says nothing about what DuckDB produced. The
            // cast repairs the type, not the intermediate rounding; exact
            // decimal division is the upgrade if a gate ever fails on it.
            if (arith.result_as().kind() != Resolved::TYPE_UNSPECIFIED) {
                LogicalType target;
                if (!ResolveType(b, arith.result_as(), &target)) {
                    return nullptr;
                }
                function = make_uniq<duckdb::CastExpression>(
                    target, std::move(function));
            }
            return function;
        }
        case Resolved::Expr::kBetween: {
            const auto& between = expr.between();
            auto value = BuildExpr(b, between.value());
            if (value == nullptr) return nullptr;
            auto low = BuildExpr(b, between.low());
            if (low == nullptr) return nullptr;
            auto high = BuildExpr(b, between.high());
            if (high == nullptr) return nullptr;
            if (between.compare_as().kind() == Resolved::VARCHAR) {
                value = AdmitComparisonString(b, between.compare_as(),
                                              std::move(value));
                if (value == nullptr) return nullptr;
                low = AdmitComparisonString(b, between.compare_as(),
                                            std::move(low));
                if (low == nullptr) return nullptr;
                high = AdmitComparisonString(b, between.compare_as(),
                                             std::move(high));
                if (high == nullptr) return nullptr;
            } else {
                value = CastTo(b, between.value(), between.compare_as(),
                               std::move(value));
                if (value == nullptr) return nullptr;
                low = CastTo(b, between.low(), between.compare_as(),
                             std::move(low));
                if (low == nullptr) return nullptr;
                high = CastTo(b, between.high(), between.compare_as(),
                              std::move(high));
                if (high == nullptr) return nullptr;
            }
            unique_ptr<ParsedExpression> built =
                make_uniq<duckdb::BetweenExpression>(
                    std::move(value), std::move(low), std::move(high));
            if (between.negated()) {
                built = make_uniq<duckdb::OperatorExpression>(
                    ExpressionType::OPERATOR_NOT, std::move(built));
            }
            return built;
        }
        case Resolved::Expr::kInList: {
            const auto& in_list = expr.in_list();
            if (in_list.list_size() == 0) {
                b.Refuse("empty IN list");
                return nullptr;
            }
            const auto& value_type = in_list.value().result_type();
            const bool strings = value_type.kind() == Resolved::VARCHAR;
            if (strings) {
                for (const auto& element : in_list.list()) {
                    const auto& t = element.result_type();
                    if (t.kind() != Resolved::VARCHAR ||
                        t.collation_id() != value_type.collation_id()) {
                        b.Refuse("IN list mixes string collations");
                        return nullptr;
                    }
                }
            }
            auto node = make_uniq<duckdb::OperatorExpression>(
                in_list.negated() ? ExpressionType::COMPARE_NOT_IN
                                  : ExpressionType::COMPARE_IN);
            auto value = BuildExpr(b, in_list.value());
            if (value == nullptr) return nullptr;
            if (strings) {
                value = AdmitComparisonString(b, value_type, std::move(value));
                if (value == nullptr) return nullptr;
            }
            node->children.push_back(std::move(value));
            for (const auto& element : in_list.list()) {
                auto built = BuildExpr(b, element);
                if (built == nullptr) return nullptr;
                if (strings) {
                    built = AdmitComparisonString(b, element.result_type(),
                                                  std::move(built));
                    if (built == nullptr) return nullptr;
                }
                node->children.push_back(std::move(built));
            }
            return node;
        }
        case Resolved::Expr::kCaseWhen: {
            const auto& case_when = expr.case_when();
            if (case_when.branches_size() == 0 ||
                !case_when.has_else_result()) {
                b.Refuse("case without branches or else");
                return nullptr;
            }
            auto node = make_uniq<duckdb::CaseExpression>();
            for (const auto& branch : case_when.branches()) {
                duckdb::CaseCheck check;
                check.when_expr = BuildExpr(b, branch.when());
                if (check.when_expr == nullptr) return nullptr;
                check.then_expr = BuildExpr(b, branch.then());
                if (check.then_expr == nullptr) return nullptr;
                node->case_checks.push_back(std::move(check));
            }
            node->else_expr = BuildExpr(b, case_when.else_result());
            if (node->else_expr == nullptr) return nullptr;
            // MySQL aggregated the THEN/ELSE types into result_type; DuckDB
            // would infer its own. Cast so both agree.
            if (expr.result_type().kind() != Resolved::TYPE_UNSPECIFIED) {
                LogicalType target;
                if (!ResolveType(b, expr.result_type(), &target)) {
                    return nullptr;
                }
                return make_uniq<duckdb::CastExpression>(target,
                                                         std::move(node));
            }
            return node;
        }
        case Resolved::Expr::kFunction:
            return BuildFunction(b, expr);
        case Resolved::Expr::kAggregate:
            return BuildAggregate(b, expr);
        case Resolved::Expr::kSubquery:
            return BuildSubquery(b, expr);
        case Resolved::Expr::kCast: {
            auto child = BuildExpr(b, expr.cast().arg());
            if (child == nullptr) return nullptr;
            LogicalType target;
            if (!ResolveType(b, expr.cast().to(), &target)) return nullptr;
            return make_uniq<duckdb::CastExpression>(target, std::move(child));
        }
        case Resolved::Expr::kLike: {
            const auto& like = expr.like();
            auto value = BuildExpr(b, like.value());
            if (value == nullptr) return nullptr;
            auto pattern = BuildExpr(b, like.pattern());
            if (pattern == nullptr) return nullptr;
            if (CollationIsAiCi(like.collation_id())) {
                // The registered function evaluates my_wildcmp under the
                // real MySQL collation, so escape and multibyte semantics
                // are MySQL's own.
                duckdb::vector<unique_ptr<ParsedExpression>> args;
                args.push_back(std::move(value));
                args.push_back(std::move(pattern));
                args.push_back(make_uniq<duckdb::ConstantExpression>(
                    Value::INTEGER(like.escape())));
                return make_uniq<duckdb::FunctionExpression>(
                    like.negated() ? kMysqlAiCiNotLike : kMysqlAiCiLike,
                    std::move(args));
            }
            if (!CollationIsByteSafe(like.collation_id())) {
                b.Refuse("LIKE collation " +
                         std::to_string(like.collation_id()) +
                         " is unsupported");
                return nullptr;
            }
            if (like.escape() != 0 && like.escape() != '\\') {
                b.Refuse("LIKE escape is unsupported");
                return nullptr;
            }
            duckdb::vector<unique_ptr<ParsedExpression>> args;
            args.push_back(std::move(value));
            args.push_back(std::move(pattern));
            unique_ptr<ParsedExpression> built =
                make_uniq<duckdb::FunctionExpression>("~~", std::move(args));
            if (like.negated()) {
                built = make_uniq<duckdb::OperatorExpression>(
                    ExpressionType::OPERATOR_NOT, std::move(built));
            }
            return built;
        }
        default:
            b.Refuse("expression with no node");
            return nullptr;
    }
}

duckdb::unique_ptr<duckdb::TableRef> BuildRelation(
    Builder& b, const Resolved::Relation& relation) {
    switch (relation.node_case()) {
        case Resolved::Relation::kBase: {
            const auto& base = relation.base();
            if (base.table_desc_index() >= b.handles.size() ||
                base.table_desc_index() >=
                    static_cast<size_t>(b.request.tables_size())) {
                b.Refuse("base table index out of range");
                return nullptr;
            }
            if (!b.used_table_descs.insert(base.table_desc_index()).second) {
                b.Refuse("table descriptor used by two relations");
                return nullptr;
            }
            const auto& desc = b.request.tables(base.table_desc_index());
            if (!b.relations
                     .emplace(base.relation_id(),
                              Builder::RelationInfo{
                                  static_cast<size_t>(desc.columns_size()),
                                  &desc})
                     .second) {
                b.Refuse("duplicate relation id");
                return nullptr;
            }
            duckdb::vector<unique_ptr<ParsedExpression>> args;
            args.push_back(make_uniq<duckdb::ConstantExpression>(
                Value::POINTER(b.handles[base.table_desc_index()])));
            auto ref = make_uniq<duckdb::TableFunctionRef>();
            ref->function = make_uniq<duckdb::FunctionExpression>(
                kPaxScanFunction, std::move(args));
            ref->alias = RelationAlias(base.relation_id());
            return std::move(ref);
        }
        case Resolved::Relation::kDerived: {
            const auto& derived = relation.derived();
            const int columns = derived.query().select_size();
            auto statement = BuildBlock(b, derived.query());
            if (statement == nullptr) return nullptr;
            if (!b.relations
                     .emplace(derived.relation_id(),
                              Builder::RelationInfo{
                                  static_cast<size_t>(columns), nullptr})
                     .second) {
                b.Refuse("duplicate relation id");
                return nullptr;
            }
            auto ref = make_uniq<duckdb::SubqueryRef>(
                std::move(statement), RelationAlias(derived.relation_id()));
            for (int i = 0; i < columns; ++i) {
                ref->column_name_alias.push_back(
                    ColumnAlias(static_cast<uint32_t>(i)));
            }
            return std::move(ref);
        }
        case Resolved::Relation::kJoin: {
            const auto& join = relation.join();
            duckdb::JoinType type;
            bool needs_condition = true;
            switch (join.kind()) {
                case Resolved::JoinRel::INNER:
                    type = duckdb::JoinType::INNER;
                    break;
                case Resolved::JoinRel::LEFT:
                    type = duckdb::JoinType::LEFT;
                    break;
                case Resolved::JoinRel::SEMI:
                    type = duckdb::JoinType::SEMI;
                    break;
                case Resolved::JoinRel::ANTI:
                    type = duckdb::JoinType::ANTI;
                    break;
                case Resolved::JoinRel::CROSS:
                    type = duckdb::JoinType::INNER;
                    needs_condition = false;
                    break;
                default:
                    b.Refuse("unknown join kind");
                    return nullptr;
            }
            auto ref = make_uniq<duckdb::JoinRef>(
                needs_condition ? duckdb::JoinRefType::REGULAR
                                : duckdb::JoinRefType::CROSS);
            ref->type = type;
            ref->left = BuildRelation(b, join.left());
            if (ref->left == nullptr) return nullptr;
            ref->right = BuildRelation(b, join.right());
            if (ref->right == nullptr) return nullptr;
            if (needs_condition) {
                if (!join.has_condition()) {
                    b.Refuse("join without condition");
                    return nullptr;
                }
                ref->condition = BuildExpr(b, join.condition());
                if (ref->condition == nullptr) return nullptr;
            } else if (join.has_condition()) {
                b.Refuse("cross join with condition");
                return nullptr;
            }
            return std::move(ref);
        }
        default:
            b.Refuse("relation with no node");
            return nullptr;
    }
}

duckdb::unique_ptr<duckdb::SelectStatement> BuildBlock(
    Builder& b, const Resolved::QueryBlock& block) {
    if (!block.has_from()) {
        b.Refuse("query block without FROM");
        return nullptr;
    }
    if (block.select_size() == 0) {
        b.Refuse("query block without select list");
        return nullptr;
    }
    auto node = make_uniq<duckdb::SelectNode>();
    node->from_table = BuildRelation(b, block.from());
    if (node->from_table == nullptr) return nullptr;
    if (block.has_where()) {
        node->where_clause = BuildExpr(b, block.where());
        if (node->where_clause == nullptr) return nullptr;
    }
    if (block.group_by_size() > 0) {
        duckdb::GroupingSet grouping_set;
        for (int i = 0; i < block.group_by_size(); ++i) {
            auto built = BuildExpr(b, block.group_by(i));
            if (built == nullptr) return nullptr;
            built = CollateKey(b, block.group_by(i).result_type(),
                               std::move(built));
            if (built == nullptr) return nullptr;
            node->groups.group_expressions.push_back(std::move(built));
            grouping_set.insert(static_cast<duckdb::idx_t>(i));
        }
        node->groups.grouping_sets.push_back(std::move(grouping_set));
    }
    if (block.has_having()) {
        node->having = BuildExpr(b, block.having());
        if (node->having == nullptr) return nullptr;
    }
    for (const auto& item : block.select()) {
        auto built = BuildExpr(b, item.expression());
        if (built == nullptr) return nullptr;
        // Group keys carry the MySQL collation; a select item must stay
        // structurally identical to its key or the binder cannot match
        // them. The collation does not change the printed value.
        built = CollateKey(b, item.expression().result_type(),
                           std::move(built));
        if (built == nullptr) return nullptr;
        node->select_list.push_back(std::move(built));
    }
    if (block.distinct()) {
        node->modifiers.push_back(make_uniq<duckdb::DistinctModifier>());
    }
    if (block.order_by_size() > 0) {
        auto order = make_uniq<duckdb::OrderModifier>();
        for (const auto& key : block.order_by()) {
            auto built = BuildExpr(b, key.expression());
            if (built == nullptr) return nullptr;
            built = CollateKey(b, key.expression().result_type(),
                               std::move(built));
            if (built == nullptr) return nullptr;
            order->orders.emplace_back(
                key.descending() ? duckdb::OrderType::DESCENDING
                                 : duckdb::OrderType::ASCENDING,
                key.nulls_first() ? duckdb::OrderByNullType::NULLS_FIRST
                                  : duckdb::OrderByNullType::NULLS_LAST,
                std::move(built));
        }
        node->modifiers.push_back(std::move(order));
    }
    if (block.has_limit() || block.has_offset()) {
        // LIMIT 2^64-1 is MySQL's idiom for "all rows"; clamp to BIGINT,
        // far beyond any real result.
        auto clamp = [](uint64_t v) {
            return make_uniq<duckdb::ConstantExpression>(
                Value::BIGINT(static_cast<int64_t>(std::min<uint64_t>(
                    v, std::numeric_limits<int64_t>::max()))));
        };
        auto limit = make_uniq<duckdb::LimitModifier>();
        if (block.has_limit()) limit->limit = clamp(block.limit());
        if (block.has_offset()) limit->offset = clamp(block.offset());
        node->modifiers.push_back(std::move(limit));
    }
    auto statement = make_uniq<duckdb::SelectStatement>();
    statement->node = std::move(node);
    return statement;
}

}  // namespace

AstBuildResult BuildSelectStatement(const Resolved::Request& request,
                                    const std::vector<uintptr_t>& handles) {
    AstBuildResult result;
    if (request.format_version() != 1) {
        result.error = "unsupported duckdb-query format version";
        return result;
    }
    if (static_cast<size_t>(request.tables_size()) != handles.size()) {
        result.error = "table handle count does not match the request";
        return result;
    }
    if (!request.has_root()) {
        result.error = "request without a root query block";
        return result;
    }
    Builder builder{request, handles, {}, {}};
    auto statement = BuildBlock(builder, request.root());
    if (statement == nullptr) {
        result.error = builder.error.empty() ? "duckdb-query build failed"
                                             : builder.error;
        return result;
    }
    if (request.expected_output_count() == 0 ||
        request.root().select_size() !=
            static_cast<int>(request.expected_output_count())) {
        result.error = "select list does not match the expected output count";
        return result;
    }
    result.statement.reset(statement.release());
    return result;
}

}  // namespace duckdb_bridge
