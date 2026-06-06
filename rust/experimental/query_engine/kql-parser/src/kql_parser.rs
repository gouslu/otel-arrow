// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_parser_abstractions::*;
use pest_derive::Parser;

use crate::query_expression::parse_query;

#[derive(Parser)]
#[grammar = "kql.pest"]
pub(crate) struct KqlPestParser;

pub struct KqlParser {}

impl Parser for KqlParser {
    fn parse_with_options(
        query: &str,
        options: ParserOptions,
    ) -> Result<ParserResult, Vec<ParserError>> {
        let pipeline = parse_query(query, options)?;
        Ok(ParserResult::new(pipeline))
    }
}

pub(crate) fn map_kql_errors(error: ParserError) -> ParserError {
    match error {
        ParserError::KeyNotFound { location, key } => ParserError::QueryLanguageDiagnostic {
            location,
            diagnostic_id: "KS142",
            message: format!(
                "The name '{key}' does not refer to any known column, table, variable or function"
            ),
        },
        e => e,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_engine_parser_abstractions::{ParserBudget, ParserError, ParserOptions};
    use std::time::{Duration, Instant};

    #[test]
    pub fn test_parse() {
        assert!(KqlParser::parse("a").is_ok());
        assert!(KqlParser::parse("let a = 1").is_err());
        assert!(KqlParser::parse("i | extend a = 1 i | extend b = 2").is_err());
    }

    #[test]
    fn budget_rejects_oversized_input() {
        let query = "source | extend a = 1";
        let options = ParserOptions::new().with_budget(
            ParserBudget::new().with_max_input_bytes(query.len() - 1),
        );

        let err = KqlParser::parse_with_options(query, options)
            .expect_err("expected BudgetExceeded for oversized input");

        assert_eq!(err.len(), 1);
        assert!(
            matches!(&err[0], ParserError::BudgetExceeded(msg) if msg.contains("input length")),
            "expected BudgetExceeded(input length...), got {:?}",
            err[0]
        );
    }

    #[test]
    fn budget_allows_input_within_limit() {
        let query = "source | extend a = 1";
        let options = ParserOptions::new()
            .with_budget(ParserBudget::new().with_max_input_bytes(query.len()));

        KqlParser::parse_with_options(query, options)
            .expect("expected parse to succeed when input length is exactly the budget");
    }

    #[test]
    fn budget_rejects_too_many_top_level_statements() {
        // Three top-level let statements; cap at one.
        let query = "let a = 1; let b = 2; let c = 3;";
        let options = ParserOptions::new()
            .with_budget(ParserBudget::new().with_max_top_level_statements(1));

        let err = KqlParser::parse_with_options(query, options)
            .expect_err("expected BudgetExceeded for too many statements");

        assert!(
            err.iter()
                .any(|e| matches!(e, ParserError::BudgetExceeded(msg)
                        if msg.contains("top-level statements"))),
            "expected BudgetExceeded(top-level statements...), got {err:?}"
        );
    }

    #[test]
    fn budget_rejects_when_deadline_has_already_passed() {
        let query = "source | extend a = 1";
        let options = ParserOptions::new().with_budget(
            ParserBudget::new().with_deadline(Instant::now() - Duration::from_secs(1)),
        );

        let err = KqlParser::parse_with_options(query, options)
            .expect_err("expected BudgetExceeded when deadline elapsed before parse");

        assert!(
            err.iter()
                .any(|e| matches!(e, ParserError::BudgetExceeded(msg)
                        if msg.contains("deadline"))),
            "expected BudgetExceeded(deadline...), got {err:?}"
        );
    }

    #[test]
    fn no_budget_means_no_limit() {
        // Long but valid sequence of statements parses fine when no budget set.
        let query = "let a = 1; let b = 2; let c = 3; let d = 4; let e = 5;";
        KqlParser::parse_with_options(query, ParserOptions::new())
            .expect("expected parse to succeed when no budget is configured");
    }
}
