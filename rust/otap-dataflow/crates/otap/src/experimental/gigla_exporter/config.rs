use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// Configuration for the GigLA Exporter matching the Collector's schema.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// HTTP client configuration (timeout, TLS, etc.)
    #[serde(flatten)]
    pub client_config: HashMap<String, Value>,

    /// API configuration for GigLA
    pub api: ApiConfig,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,
}

/// Authentication configuration for Azure
#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    /// Azure AD tenant ID (optional, uses AZURE_TENANT_ID env var if not set)
    pub tenant_id: Option<String>,

    /// OAuth scope for token acquisition (defaults to "https://monitor.azure.com/.default")
    #[serde(default = "default_scope")]
    pub scope: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            tenant_id: None,
            scope: default_scope(),
        }
    }
}

fn default_scope() -> String {
    "https://monitor.azure.com/.default".to_string()
}

/// API configuration for connecting to GigLA
#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    /// Data Collection Rule endpoint
    pub dcr_endpoint: String,

    /// Stream name for the logs
    pub stream_name: String,

    /// Data Collection Rule identifier
    pub dcr: String,

    /// Schema mapping configuration
    #[serde(default)]
    pub schema: SchemaConfig,
}

/// Schema mapping configuration
#[derive(Debug, Deserialize, Clone, Default)]
pub struct SchemaConfig {
    /// Resource attribute mappings
    #[serde(default)]
    pub resource_mapping: HashMap<String, String>,

    /// Scope attribute mappings
    #[serde(default)]
    pub scope_mapping: HashMap<String, String>,

    /// Log record field mappings
    #[serde(default)]
    pub log_record_mapping: HashMap<String, Value>,

    /// Disable automatic schema mapping
    #[serde(default)]
    pub disable_schema_mapping: bool,
}

impl Config {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate auth configuration
        if self.auth.scope.is_empty() {
            return Err("Invalid configuration: auth scope must be non-empty".to_string());
        }

        // Validate API configuration
        if self.api.dcr_endpoint.is_empty() {
            return Err("Invalid configuration: dcr_endpoint must be non-empty".to_string());
        }
        if self.api.stream_name.is_empty() {
            return Err("Invalid configuration: stream_name must be non-empty".to_string());
        }
        if self.api.dcr.is_empty() {
            return Err("Invalid configuration: dcr must be non-empty".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_config() {
        let config = Config {
            client_config: HashMap::new(),
            api: ApiConfig {
                dcr_endpoint: "https://example.com".to_string(),
                stream_name: "mystream".to_string(),
                dcr: "mydcr".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig {
                tenant_id: Some("mytenant".to_string()),
                scope: "https://monitor.azure.com/.default".to_string(),
            },
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_config_missing_api_fields() {
        let config = Config {
            client_config: HashMap::new(),
            api: ApiConfig {
                dcr_endpoint: "".to_string(),
                stream_name: "".to_string(),
                dcr: "".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig::default(),
        };

        assert!(config.validate().is_err());
    }
}
