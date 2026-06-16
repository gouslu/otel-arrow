// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared pest grammar and parser for v2 schema source expressions.
//! Used by both the config validator (which discards the AST) and the
//! transformer (which consumes it). The pest grammar file is the spec
//! verbatim — single source of truth.

use pest::Parser;
use pest::iterators::Pair;

#[allow(missing_docs)]
mod inner {
    #[derive(pest_derive::Parser)]
    #[grammar = "exporters/azure_monitor_exporter/otlp_field.pest"]
    pub(super) struct OtlpFieldParser;
}

use inner::{OtlpFieldParser, Rule};

/// Resource-level fields addressable from a v2 source expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourceField {
    DroppedAttributesCount,
}

/// Scope-level fields addressable from a v2 source expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScopeField {
    Name,
    Version,
    DroppedAttributesCount,
}

/// Log-record-level fields addressable from a v2 source expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LogRecordField {
    TimeUnixNano,
    ObservedTimeUnixNano,
    TraceId,
    SpanId,
    Flags,
    SeverityNumber,
    SeverityText,
    Body,
    EventName,
    DroppedAttributesCount,
}

impl LogRecordField {
    /// Case-insensitive lookup used by the v1 config path (where keys come
    /// from YAML and are not constrained by the v2 grammar).
    pub(super) fn from_str_ci(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("time_unix_nano") {
            Some(Self::TimeUnixNano)
        } else if s.eq_ignore_ascii_case("observed_time_unix_nano") {
            Some(Self::ObservedTimeUnixNano)
        } else if s.eq_ignore_ascii_case("trace_id") {
            Some(Self::TraceId)
        } else if s.eq_ignore_ascii_case("span_id") {
            Some(Self::SpanId)
        } else if s.eq_ignore_ascii_case("flags") {
            Some(Self::Flags)
        } else if s.eq_ignore_ascii_case("severity_number") {
            Some(Self::SeverityNumber)
        } else if s.eq_ignore_ascii_case("severity_text") {
            Some(Self::SeverityText)
        } else if s.eq_ignore_ascii_case("body") {
            Some(Self::Body)
        } else if s.eq_ignore_ascii_case("event_name") {
            Some(Self::EventName)
        } else if s.eq_ignore_ascii_case("dropped_attributes_count") {
            Some(Self::DroppedAttributesCount)
        } else {
            None
        }
    }
}

/// Parsed v2 source expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OtlpField {
    ResourceAttribute(String),
    ResourceField(ResourceField),
    ScopeAttribute(String),
    ScopeField(ScopeField),
    LogRecordAttribute(String),
    LogRecordField(LogRecordField),
}

/// Parses a v2 source expression with the pest grammar and walks the tree
/// to build a typed AST. Errors are returned as human-readable strings.
pub(super) fn parse_otlp_field(expr: &str) -> Result<OtlpField, String> {
    let mut pairs = OtlpFieldParser::parse(Rule::source_expr, expr).map_err(|e| e.to_string())?;
    let source_expr = pairs.next().ok_or("empty parse")?;
    let path = first_inner(source_expr)?;
    let leaf = first_inner(path)?;
    match leaf.as_rule() {
        Rule::resource_path => build_resource(leaf),
        Rule::scope_path => build_scope(leaf),
        Rule::log_record_path => build_log_record(leaf),
        r => Err(format!("unexpected rule {r:?}")),
    }
}

fn build_resource(pair: Pair<'_, Rule>) -> Result<OtlpField, String> {
    let child = first_inner(pair)?;
    match child.as_rule() {
        Rule::attributes_access => Ok(OtlpField::ResourceAttribute(decode_attribute_key(child)?)),
        Rule::resource_field => Ok(OtlpField::ResourceField(match child.as_str() {
            "dropped_attributes_count" => ResourceField::DroppedAttributesCount,
            other => return Err(format!("unknown resource field '{other}'")),
        })),
        r => Err(format!("unexpected resource child {r:?}")),
    }
}

fn build_scope(pair: Pair<'_, Rule>) -> Result<OtlpField, String> {
    let child = first_inner(pair)?;
    match child.as_rule() {
        Rule::attributes_access => Ok(OtlpField::ScopeAttribute(decode_attribute_key(child)?)),
        Rule::scope_field => Ok(OtlpField::ScopeField(match child.as_str() {
            "name" => ScopeField::Name,
            "version" => ScopeField::Version,
            "dropped_attributes_count" => ScopeField::DroppedAttributesCount,
            other => return Err(format!("unknown scope field '{other}'")),
        })),
        r => Err(format!("unexpected scope child {r:?}")),
    }
}

fn build_log_record(pair: Pair<'_, Rule>) -> Result<OtlpField, String> {
    let child = first_inner(pair)?;
    match child.as_rule() {
        Rule::attributes_access => Ok(OtlpField::LogRecordAttribute(decode_attribute_key(child)?)),
        Rule::log_record_field => {
            let f = LogRecordField::from_str_ci(child.as_str())
                .ok_or_else(|| format!("unknown log_record field '{}'", child.as_str()))?;
            Ok(OtlpField::LogRecordField(f))
        }
        r => Err(format!("unexpected log_record child {r:?}")),
    }
}

fn decode_attribute_key(pair: Pair<'_, Rule>) -> Result<String, String> {
    let literal = first_inner(pair)?;
    let raw = literal.as_str();
    let inner = &raw[1..raw.len() - 1];
    decode_escaped(inner)
}

fn decode_escaped(s: &str) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        let esc = chars.next().ok_or("dangling backslash")?;
        let decoded = match esc {
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'b' => '\u{0008}',
            'f' => '\u{000C}',
            '\\' => '\\',
            '/' => '/',
            '\'' => '\'',
            '"' => '"',
            'u' => {
                let mut code: u32 = 0;
                for _ in 0..4 {
                    let h = chars
                        .next()
                        .and_then(|c| c.to_digit(16))
                        .ok_or("\\u escape requires 4 hex digits")?;
                    code = (code << 4) | h;
                }
                char::from_u32(code).ok_or_else(|| {
                    format!(
                        "\\u{code:04X} is not a valid Unicode scalar value \
                         (surrogates U+D800..U+DFFF are not allowed; write the literal character instead)"
                    )
                })?
            }
            other => return Err(format!("unknown escape sequence '\\{other}'")),
        };
        out.push(decoded);
    }
    Ok(out)
}

fn first_inner(pair: Pair<'_, Rule>) -> Result<Pair<'_, Rule>, String> {
    pair.into_inner().next().ok_or_else(|| "empty rule".into())
}
