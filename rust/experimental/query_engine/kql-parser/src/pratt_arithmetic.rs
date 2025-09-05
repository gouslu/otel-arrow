// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;
use data_engine_parser_abstractions::*;
use pest::iterators::Pair;
use pest::pratt_parser::{Assoc::Left, Op, PrattParser};
use std::sync::LazyLock;

use crate::{
    Rule, scalar_array_function_expressions::*, scalar_conditional_function_expressions::*,
    scalar_conversion_function_expressions::*, scalar_mathematical_function_expressions::*,
    scalar_primitive_expressions::parse_accessor_expression, scalar_primitive_expressions::*,
    scalar_string_function_expressions::*, scalar_temporal_function_expressions::*,
};

static PRATT: LazyLock<PrattParser<Rule>> = LazyLock::new(|| {
    PrattParser::new()
        .op(Op::infix(Rule::plus_token, Left) | Op::infix(Rule::minus_token, Left))
        .op(Op::infix(Rule::multiply_token, Left)
            | Op::infix(Rule::divide_token, Left)
            | Op::infix(Rule::modulo_token, Left))
        .op(Op::prefix(Rule::unary_minus))
});

pub fn parse_arithmetic_with_pratt(
    pair: Pair<Rule>,
    scope: &dyn ParserScope,
) -> Result<ScalarExpression, ParserError> {
    let location = to_query_location(&pair);

    let arithmetic = PRATT
        .map_primary(|primary| parse_primary(primary, scope))
        .map_prefix(|op, rhs| {
            let rhs = rhs?;
            match op.as_rule() {
                Rule::unary_minus => Ok(ScalarExpression::Math(MathScalarExpression::Negate(
                    UnaryMathematicalScalarExpression::new(location.clone(), rhs),
                ))),
                _ => unreachable!("Unknown prefix operator: {:?}", op.as_rule()),
            }
        })
        .map_infix(|lhs, op, rhs| {
            let lhs = lhs?;
            let rhs = rhs?;
            let func = match op.as_rule() {
                Rule::plus_token => MathScalarExpression::Add(
                    BinaryMathematicalScalarExpression::new(location.clone(), lhs, rhs),
                ),
                Rule::minus_token => MathScalarExpression::Subtract(
                    BinaryMathematicalScalarExpression::new(location.clone(), lhs, rhs),
                ),
                Rule::multiply_token => MathScalarExpression::Multiply(
                    BinaryMathematicalScalarExpression::new(location.clone(), lhs, rhs),
                ),
                Rule::divide_token => MathScalarExpression::Divide(
                    BinaryMathematicalScalarExpression::new(location.clone(), lhs, rhs),
                ),
                Rule::modulo_token => MathScalarExpression::Modulus(
                    BinaryMathematicalScalarExpression::new(location.clone(), lhs, rhs),
                ),
                _ => unreachable!("Unknown operator: {:?}", op.as_rule()),
            };

            Ok(ScalarExpression::Math(func))
        })
        .parse(pair.into_inner());

    arithmetic
}

// Add this new public function to parse pratt_atom (needed by pratt_arithmetic module)
pub(crate) fn parse_primary(
    primary_rule: Pair<Rule>,
    scope: &dyn ParserScope,
) -> Result<ScalarExpression, ParserError> {
    use crate::scalar_expression::parse_scalar_expression;

    let pratt = match primary_rule.as_rule() {
        Rule::null_literal => ScalarExpression::Static(parse_standard_null_literal(primary_rule)),
        Rule::real_expression => ScalarExpression::Static(parse_real_expression(primary_rule)?),
        Rule::datetime_expression => {
            ScalarExpression::Static(parse_datetime_expression(primary_rule)?)
        }
        Rule::time_expression => ScalarExpression::Static(parse_timespan_expression(primary_rule)?),
        Rule::conditional_expression => parse_conditional_expression(primary_rule, scope)?,
        Rule::case_expression => parse_case_expression(primary_rule, scope)?,
        Rule::coalesce_expression => parse_coalesce_expression(primary_rule, scope)?,
        Rule::tostring_expression => parse_tostring_expression(primary_rule, scope)?,
        Rule::toint_expression => parse_toint_expression(primary_rule, scope)?,
        Rule::tobool_expression => parse_tobool_expression(primary_rule, scope)?,
        Rule::tofloat_expression => parse_tofloat_expression(primary_rule, scope)?,
        Rule::tolong_expression => parse_tolong_expression(primary_rule, scope)?,
        Rule::toreal_expression => parse_toreal_expression(primary_rule, scope)?,
        Rule::todouble_expression => parse_todouble_expression(primary_rule, scope)?,
        Rule::todatetime_expression => parse_todatetime_expression(primary_rule, scope)?,
        Rule::totimespan_expression => parse_totimespan_expression(primary_rule, scope)?,
        Rule::strlen_expression => parse_strlen_expression(primary_rule, scope)?,
        Rule::replace_string_expression => parse_replace_string_expression(primary_rule, scope)?,
        Rule::substring_expression => parse_substring_expression(primary_rule, scope)?,
        Rule::parse_json_expression => parse_parse_json_expression(primary_rule, scope)?,
        Rule::strcat_expression => parse_strcat_expression(primary_rule, scope)?,
        Rule::strcat_delim_expression => parse_strcat_delim_expression(primary_rule, scope)?,
        Rule::array_concat_expression => parse_array_concat_expression(primary_rule, scope)?,
        Rule::true_literal | Rule::false_literal => {
            ScalarExpression::Static(parse_standard_bool_literal(primary_rule))
        }
        Rule::double_literal => {
            ScalarExpression::Static(parse_standard_double_literal(primary_rule, None)?)
        }
        Rule::integer_literal => {
            ScalarExpression::Static(parse_standard_integer_literal(primary_rule)?)
        }
        Rule::string_literal => ScalarExpression::Static(parse_string_literal(primary_rule)),
        Rule::bin_expression => parse_bin_expression(primary_rule, scope)?,
        Rule::now_expression => parse_now_expression(primary_rule, scope)?,
        Rule::accessor_expression => {
            // Note: When used as a scalar expression it is valid for an
            // accessor to fold into a static at the root so
            // allow_root_scalar=true is passed here. Example: iff([logical],
            // [scalar], [scalar]) evaluated as iff([logical],
            // accessor(some_constant1), accessor(some_constant2)) can safely
            // fold to iff([logical], String("constant1"), String("constant2")).
            parse_accessor_expression(primary_rule, scope, true)?
        }
        Rule::scalar_expression => parse_scalar_expression(primary_rule, scope)?,
        _ => panic!("Unexpected rule in pratt_atom: {primary_rule}"),
    };

    Ok(pratt)
}

// Replace the existing parse_arithmetic function with this:
pub fn parse_arithmetic_expression(
    pair: Pair<Rule>,
    scope: &dyn ParserScope,
) -> Result<ScalarExpression, ParserError> {
    // Use the new Pratt parser instead of the old manual parsing
    parse_arithmetic_with_pratt(pair, scope)
}
