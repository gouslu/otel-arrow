use otel_arrow_rust::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otel_arrow_rust::proto::opentelemetry::common::v1::any_value::Value as OtelAnyValueEnum;
use serde_json::{json, Value};

use super::config::{Config, SchemaConfig};

const ATTRIBUTES_FIELD: &str = "attributes";

/// Converts OTLP logs to Azure Log Analytics format
pub struct Transformer {
    schema: SchemaConfig,
}

impl Transformer {
    /// Create a new transformer with the given configuration
    pub fn new(config: &Config) -> Self {
        Self {
            schema: config.api.schema.clone(),
        }
    }

    /// Convert OTLP logs to flat JSON objects for Log Analytics
    pub fn convert_to_log_analytics(&self, request: &ExportLogsServiceRequest) -> Vec<Value> {
        let mut entries = Vec::new();
        
        for resource_logs in &request.resource_logs {
            // Extract resource attributes based on mapping
            let resource_attrs = self.apply_resource_mapping(&resource_logs.resource);

            for scope_logs in &resource_logs.scope_logs {
                // Extract scope attributes based on mapping
                let scope_attrs = self.apply_scope_mapping(&scope_logs.scope);

                for log_record in &scope_logs.log_records {
                    let mut entry = serde_json::Map::new();
                    
                    // Add resource and scope attributes first
                    for (k, v) in &resource_attrs {
                        let _ = entry.insert(k.clone(), v.clone());
                    }
                    for (k, v) in &scope_attrs {
                        let _ = entry.insert(k.clone(), v.clone());
                    }
                    
                    // Transform log record based on mapping
                    if let Err(e) = self.transform_log_record(&mut entry, log_record) {
                        log::warn!("Failed to transform log record: {}", e);
                        continue;
                    }
                    
                    entries.push(Value::Object(entry));
                }
            }
        }
        
        entries
    }

    /// Transform log record fields based on the log_record_mapping configuration
    fn transform_log_record(&self, destination: &mut serde_json::Map<String, Value>, log_record: &otel_arrow_rust::proto::opentelemetry::logs::v1::LogRecord) -> Result<(), String> {
        // If schema mapping is disabled, use legacy transform
        if self.schema.disable_schema_mapping {
            self.legacy_transform(destination, log_record);
            return Ok(());
        }

        // Extract log record fields based on mapping
        for (key, value) in &self.schema.log_record_mapping {
            if key == ATTRIBUTES_FIELD {
                // Handle nested attribute mapping
                if let Some(attr_mapping) = value.as_object() {
                    for (attr_key, attr_value) in attr_mapping {
                        if let Some(actual_value) = self.extract_attribute(&log_record.attributes, attr_key) {
                            if let Some(field_name) = attr_value.as_str() {
                                let _ = destination.insert(field_name.to_string(), actual_value);
                            }
                        }
                    }
                }
            } else {
                // Handle direct log record field mapping
                if let Ok(log_record_value) = self.extract_value_from_log_record(key, log_record) {
                    if let Some(field_name) = value.as_str() {
                        let _ = destination.insert(field_name.to_string(), log_record_value);
                    }
                }
            }
        }

        Ok(())
    }

    /// Legacy transform when schema mapping is disabled
    fn legacy_transform(&self, destination: &mut serde_json::Map<String, Value>, log_record: &otel_arrow_rust::proto::opentelemetry::logs::v1::LogRecord) {
        // Use timestamp or fallback to observed timestamp
        let timestamp = if log_record.time_unix_nano != 0 {
            self.format_timestamp(log_record.time_unix_nano)
        } else {
            self.format_timestamp(log_record.observed_time_unix_nano)
        };
        let _ = destination.insert("TimeGenerated".to_string(), json!(timestamp));

        // Add raw data as body
        if let Some(ref body) = log_record.body {
            if let Some(ref value) = body.value {
                let body_value = self.convert_any_value(value);
                let _ = destination.insert("RawData".to_string(), body_value);
            }
        }
    }

    /// Extract value from log record properties by field name
    fn extract_value_from_log_record(&self, key: &str, log_record: &otel_arrow_rust::proto::opentelemetry::logs::v1::LogRecord) -> Result<Value, String> {
        let key_lower = key.to_lowercase();
        match key_lower.as_str() {
            "time_unix_nano" => {
                let timestamp = self.format_timestamp_as_time(log_record.time_unix_nano);
                Ok(json!(timestamp))
            },
            "observed_time_unix_nano" => {
                let timestamp = self.format_timestamp_as_time(log_record.observed_time_unix_nano);
                Ok(json!(timestamp))
            },
            "trace_id" => {
                let trace_id = self.bytes_to_hex(&log_record.trace_id);
                Ok(json!(trace_id))
            },
            "span_id" => {
                let span_id = self.bytes_to_hex(&log_record.span_id);
                Ok(json!(span_id))
            },
            "flags" => Ok(json!(log_record.flags)),
            "severity_number" => Ok(json!(log_record.severity_number as i64)),
            "severity_text" => Ok(json!(log_record.severity_text)),
            "body" => {
                if let Some(ref body) = log_record.body {
                    if let Some(ref value) = body.value {
                        Ok(self.convert_any_value(value))
                    } else {
                        Ok(json!(null))
                    }
                } else {
                    Ok(json!(null))
                }
            },
            _ => Err(format!("Unknown field name: {}", key))
        }
    }

    /// Extract attribute value by key from the attributes map
    fn extract_attribute(&self, attributes: &[otel_arrow_rust::proto::opentelemetry::common::v1::KeyValue], key: &str) -> Option<Value> {
        for attr in attributes {
            if attr.key == key {
                if let Some(ref value) = attr.value {
                    if let Some(ref v) = value.value {
                        return Some(self.convert_any_value(v));
                    }
                }
            }
        }
        None
    }

    /// Apply resource mapping based on configuration
    fn apply_resource_mapping(&self, resource: &Option<otel_arrow_rust::proto::opentelemetry::resource::v1::Resource>) -> serde_json::Map<String, Value> {
        let mut attrs = serde_json::Map::new();
        
        if let Some(resource) = resource {
            for (key, mapped_name) in &self.schema.resource_mapping {
                if let Some(actual_value) = self.extract_attribute(&resource.attributes, key) {
                    let _ = attrs.insert(mapped_name.clone(), actual_value);
                }
            }
        }
        
        attrs
    }

    /// Apply scope mapping based on configuration
    fn apply_scope_mapping(&self, scope: &Option<otel_arrow_rust::proto::opentelemetry::common::v1::InstrumentationScope>) -> serde_json::Map<String, Value> {
        let mut attrs = serde_json::Map::new();
        
        if let Some(scope) = scope {
            for (key, mapped_name) in &self.schema.scope_mapping {
                let actual_value = match key.as_str() {
                    "name" => Some(json!(scope.name)),
                    "version" => Some(json!(scope.version)),
                    _ => self.extract_attribute(&scope.attributes, key)
                };
                
                if let Some(value) = actual_value {
                    let _ = attrs.insert(mapped_name.clone(), value);
                }
            }
        }
        
        attrs
    }

    /// Convert AnyValue to JSON Value
    fn convert_any_value(&self, value: &OtelAnyValueEnum) -> Value {
        match value {
            OtelAnyValueEnum::StringValue(s) => json!(s),
            OtelAnyValueEnum::IntValue(i) => json!(i),
            OtelAnyValueEnum::DoubleValue(d) => json!(d),
            OtelAnyValueEnum::BoolValue(b) => json!(b),
            OtelAnyValueEnum::ArrayValue(arr) => {
                let values: Vec<Value> = arr.values.iter()
                    .filter_map(|v| v.value.as_ref())
                    .map(|v| self.convert_any_value(v))
                    .collect();
                json!(values)
            },
            OtelAnyValueEnum::KvlistValue(kv) => {
                let mut map = serde_json::Map::new();
                for item in &kv.values {
                    if let Some(value) = &item.value {
                        if let Some(v) = &value.value {
                            let _ = map.insert(item.key.clone(), self.convert_any_value(v));
                        }
                    }
                }
                Value::Object(map)
            },
            OtelAnyValueEnum::BytesValue(bytes) => json!(self.bytes_to_hex(bytes)),
        }
    }

    /// Convert bytes to hex string
    fn bytes_to_hex(&self, bytes: &[u8]) -> String {
        bytes.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    }

    /// Format timestamp from Unix nano to datetime string (for display)
    fn format_timestamp(&self, time_unix_nano: u64) -> String {
        if time_unix_nano > 0 {
            let secs = (time_unix_nano / 1_000_000_000) as i64;
            let nanos = (time_unix_nano % 1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                dt.format("%Y-%m-%d %H:%M:%S%.3fZ").to_string()
            } else {
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3fZ").to_string()
            }
        } else {
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3fZ").to_string()
        }
    }

    /// Format timestamp as proper time object (equivalent to Go's AsTime())
    fn format_timestamp_as_time(&self, time_unix_nano: u64) -> String {
        if time_unix_nano > 0 {
            let secs = (time_unix_nano / 1_000_000_000) as i64;
            let nanos = (time_unix_nano % 1_000_000_000) as u32;
            if let Some(dt) = chrono::DateTime::from_timestamp(secs, nanos) {
                dt.to_rfc3339()
            } else {
                chrono::Utc::now().to_rfc3339()
            }
        } else {
            chrono::Utc::now().to_rfc3339()
        }
    }
}