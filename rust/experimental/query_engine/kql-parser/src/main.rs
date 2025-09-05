// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::io::{self, BufRead, Write};

// Import everything we need directly here
#[path = "kql_parser.rs"]
mod kql_parser;
use kql_parser::{KqlPestParser, Rule};

#[path = "query_expression.rs"]
mod query_expression;

#[path = "tabular_expressions.rs"]
mod tabular_expressions;

#[path = "aggregate_expressions.rs"]
mod aggregate_expressions;

#[path = "scalar_expression.rs"]
mod scalar_expression;
use scalar_expression::parse_scalar_expression;

#[path = "pratt_arithmetic.rs"]
mod pratt_arithmetic;

#[path = "scalar_primitive_expressions.rs"]
mod scalar_primitive_expressions;

#[path = "scalar_mathematical_function_expressions.rs"]
mod scalar_mathematical_function_expressions;

#[path = "scalar_string_function_expressions.rs"]
mod scalar_string_function_expressions;

#[path = "scalar_temporal_function_expressions.rs"]
mod scalar_temporal_function_expressions;

#[path = "scalar_conditional_function_expressions.rs"]
mod scalar_conditional_function_expressions;

#[path = "scalar_conversion_function_expressions.rs"]
mod scalar_conversion_function_expressions;

#[path = "scalar_array_function_expressions.rs"]
mod scalar_array_function_expressions;

#[path = "logical_expressions.rs"]
mod logical_expressions;

#[path = "shared_expressions.rs"]
mod shared_expressions;

#[path = "date_utils.rs"]
mod date_utils;

use data_engine_parser_abstractions::ParserState;
use pest::Parser;

fn main() -> io::Result<()> {
    println!("KQL Scalar Expression Test REPL");
    println!("Enter scalar expressions to parse. Type 'quit' to exit.");
    println!("Examples:");
    println!("  - Arithmetic: 2 + 3 * 4");
    println!("  - Unary minus: -5 + 3");
    println!("  - With parens: (2 + 3) * 4");
    println!("  - String: \"hello\"");
    println!("  - Boolean: true");
    println!("  - Logical: (5 > 3)");
    println!("-------------------------------------------");

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    print!("> ");
    stdout.flush()?;

    for line in stdin.lock().lines() {
        let input = line?;

        if input.trim() == "quit" {
            break;
        }

        if input.trim().is_empty() {
            print!("> ");
            stdout.flush()?;
            continue;
        }

        println!("\nParsing: {}\n", input);

        // Parse as scalar expression
        println!("=== Scalar Expression Parse Result ===");
        let state = ParserState::new(&input);

        match KqlPestParser::parse(Rule::scalar_expression, &input) {
            Ok(mut pairs) => {
                if let Some(pair) = pairs.next() {
                    // Show the parse tree
                    println!("Parse tree:");
                    print_pair(&pair, 0);

                    // Parse the expression
                    match parse_scalar_expression(pair, &state) {
                        Ok(expr) => {
                            println!("\n✓ Successfully parsed!");
                            println!("Expression AST:");
                            println!("{:#?}", expr);
                        }
                        Err(e) => {
                            println!("\n✗ Parse failed: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("✗ Pest parse failed: {}", e);
            }
        }

        println!("\n-------------------------------------------");
        print!("> ");
        stdout.flush()?;
    }

    Ok(())
}

fn print_pair(pair: &pest::iterators::Pair<Rule>, depth: usize) {
    let indent = "  ".repeat(depth);
    let rule = pair.as_rule();
    let text = pair.as_str();

    if pair.clone().into_inner().next().is_none() {
        // Leaf node
        println!("{}{:?} = \"{}\"", indent, rule, text);
    } else {
        // Non-leaf node
        println!("{}{:?}", indent, rule);
        for inner_pair in pair.clone().into_inner() {
            print_pair(&inner_pair, depth + 1);
        }
    }
}
