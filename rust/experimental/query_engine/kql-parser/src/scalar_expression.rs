// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;
use data_engine_parser_abstractions::*;
use pest::iterators::Pair;

use crate::{Rule, logical_expressions::parse_logical_expression, pratt_arithmetic};

pub(crate) fn parse_scalar_expression(
    scalar_expression_rule: Pair<Rule>,
    scope: &dyn ParserScope,
) -> Result<ScalarExpression, ParserError> {
    let scalar_rule = scalar_expression_rule.into_inner().next().unwrap();

    let scalar = match scalar_rule.as_rule() {
        Rule::arithmetic_expression => {
            pratt_arithmetic::parse_arithmetic_expression(scalar_rule, scope)?
        }
        Rule::logical_expression => {
            let l = parse_logical_expression(scalar_rule, scope)?;

            if let LogicalExpression::Scalar(s) = l {
                s
            } else {
                ScalarExpression::Logical(l.into())
            }
        }
        _ => panic!("Unexpected rule in scalar_expression: {scalar_rule}"),
    };

    Ok(scalar)
}

#[cfg(test)]
mod tests {
    use pest::Parser;

    use crate::{KqlPestParser, date_utils::create_utc};

    use super::*;

    #[test]
    fn test_pest_parse_scalar_expression_rule() {
        pest_test_helpers::test_pest_rule::<KqlPestParser, Rule>(
            Rule::scalar_expression,
            &[
                "1",
                "1e1",
                "real(1)",
                "datetime(6/9/2025)",
                "true",
                "false",
                "(true == true)",
                "\"hello world\"",
                "variable",
                "(1)",
                "iff(true, 0, 1)",
                "case(true, 1, false)",
                "case(true, 1, false, 2, 0)",
                "bool(null)",
                "int(null)",
                "long(null)",
                "real(null)",
                "datetime(null)",
                "time(null)",
                "timespan(null)",
                "guid(null)",
                "dynamic(null)",
                "tostring(\"hello\")",
                "tostring(42)",
            ],
            &["!"],
        );
    }

    #[test]
    fn test_parse_scalar_expression() {
        let run_test_success = |input: &str, expected: ScalarExpression| {
            println!("Testing: {input}");

            let state = ParserState::new(input);

            let mut result = KqlPestParser::parse(Rule::scalar_expression, input).unwrap();

            let expression = parse_scalar_expression(result.next().unwrap(), &state).unwrap();

            assert_eq!(expected, expression);
        };

        run_test_success(
            "1",
            ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
            )),
        );

        run_test_success(
            "(1)",
            ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
            )),
        );

        run_test_success(
            "1e1",
            ScalarExpression::Static(StaticScalarExpression::Double(DoubleScalarExpression::new(
                QueryLocation::new_fake(),
                1e1,
            ))),
        );

        run_test_success(
            "real(1)",
            ScalarExpression::Static(StaticScalarExpression::Double(DoubleScalarExpression::new(
                QueryLocation::new_fake(),
                1.0,
            ))),
        );

        run_test_success(
            "datetime(6/9/2025)",
            ScalarExpression::Static(StaticScalarExpression::DateTime(
                DateTimeScalarExpression::new(
                    QueryLocation::new_fake(),
                    create_utc(2025, 6, 9, 0, 0, 0, 0),
                ),
            )),
        );

        run_test_success(
            "true",
            ScalarExpression::Static(StaticScalarExpression::Boolean(
                BooleanScalarExpression::new(QueryLocation::new_fake(), true),
            )),
        );

        run_test_success(
            "false",
            ScalarExpression::Static(StaticScalarExpression::Boolean(
                BooleanScalarExpression::new(QueryLocation::new_fake(), false),
            )),
        );

        run_test_success(
            "(true == true)",
            ScalarExpression::Logical(
                LogicalExpression::EqualTo(EqualToLogicalExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                    false,
                ))
                .into(),
            ),
        );

        run_test_success(
            "\"hello world\"",
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
        );

        run_test_success(
            "identifier",
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "identifier",
                    )),
                )]),
            )),
        );

        run_test_success(
            "bool(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "datetime(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "dynamic(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "guid(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "int(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "long(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "real(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "double(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "timespan(null)",
            ScalarExpression::Static(StaticScalarExpression::Null(NullScalarExpression::new(
                QueryLocation::new_fake(),
            ))),
        );

        run_test_success(
            "case(true, 1, 0)",
            ScalarExpression::Case(CaseScalarExpression::new(
                QueryLocation::new_fake(),
                vec![(
                    LogicalExpression::Scalar(ScalarExpression::Static(
                        StaticScalarExpression::Boolean(BooleanScalarExpression::new(
                            QueryLocation::new_fake(),
                            true,
                        )),
                    )),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
                    )),
                )],
                ScalarExpression::Static(StaticScalarExpression::Integer(
                    IntegerScalarExpression::new(QueryLocation::new_fake(), 0),
                )),
            )),
        );

        run_test_success(
            "tostring(42)",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(true)",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(false)",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), false),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(bool(null))",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Null(
                        NullScalarExpression::new(QueryLocation::new_fake()),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(\"hello\")",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::String(
                        StringScalarExpression::new(QueryLocation::new_fake(), "hello"),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(variable)",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Source(SourceScalarExpression::new(
                        QueryLocation::new_fake(),
                        ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                            StaticScalarExpression::String(StringScalarExpression::new(
                                QueryLocation::new_fake(),
                                "variable",
                            )),
                        )]),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring((42))",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "tostring(datetime(6/9/2025))",
            ScalarExpression::Convert(ConvertScalarExpression::String(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::DateTime(
                        DateTimeScalarExpression::new(
                            QueryLocation::new_fake(),
                            create_utc(2025, 6, 9, 0, 0, 0, 0),
                        ),
                    )),
                ),
            )),
        );

        // Test toint expressions
        run_test_success(
            "toint(42)",
            ScalarExpression::Convert(ConvertScalarExpression::Integer(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "toint(true)",
            ScalarExpression::Convert(ConvertScalarExpression::Integer(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "toint(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::Integer(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );

        // Test tobool expressions
        run_test_success(
            "tobool(1)",
            ScalarExpression::Convert(ConvertScalarExpression::Boolean(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
                    )),
                ),
            )),
        );

        run_test_success(
            "tobool(true)",
            ScalarExpression::Convert(ConvertScalarExpression::Boolean(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "tobool(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::Boolean(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );

        // Test tofloat expressions
        run_test_success(
            "tofloat(42)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "tofloat(true)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "tofloat(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );

        // Test tolong expressions
        run_test_success(
            "tolong(42)",
            ScalarExpression::Convert(ConvertScalarExpression::Integer(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "tolong(true)",
            ScalarExpression::Convert(ConvertScalarExpression::Integer(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "tolong(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::Integer(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );

        // Test toreal expressions
        run_test_success(
            "toreal(42)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "toreal(true)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "toreal(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );

        // Test todouble expressions
        run_test_success(
            "todouble(42)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 42),
                    )),
                ),
            )),
        );

        run_test_success(
            "todouble(true)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Boolean(
                        BooleanScalarExpression::new(QueryLocation::new_fake(), true),
                    )),
                ),
            )),
        );

        run_test_success(
            "todouble(4.44)",
            ScalarExpression::Convert(ConvertScalarExpression::Double(
                ConversionScalarExpression::new(
                    QueryLocation::new_fake(),
                    ScalarExpression::Static(StaticScalarExpression::Double(
                        DoubleScalarExpression::new(QueryLocation::new_fake(), 4.44),
                    )),
                ),
            )),
        );
    }
}
