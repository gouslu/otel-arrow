// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// Configuration for the Azure Monitor Exporter matching the Collector's schema.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// API configuration for Azure Monitor
    pub api: ApiConfig,

    /// Authentication configuration
    #[serde(default)]
    pub auth: AuthConfig,
}

/// Authentication method for Azure
#[derive(Debug, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    /// Use Managed Identity (system or user-assigned with client_id)
    #[serde(alias = "msi", alias = "managed_identity")]
    #[default]
    ManagedIdentity,

    /// Use Instance Metadata Service (IMDS) explicitly
    #[serde(alias = "imds")]
    Imds,

    /// Use developer tools (Azure CLI, Azure Developer CLI)
    #[serde(alias = "dev", alias = "developer", alias = "cli")]
    Development,
}

/// Authentication configuration for Azure
#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    /// Authentication method to use
    #[serde(default)]
    pub method: AuthMethod,

    /// Client ID for user-assigned managed identity (optional)
    /// Only used when method is ManagedIdentity
    /// If not provided with ManagedIdentity, system-assigned identity will be used
    pub client_id: Option<String>,

    /// OAuth scope for token acquisition (defaults to "https://monitor.azure.com/.default")
    #[serde(default = "default_scope")]
    pub scope: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            method: AuthMethod::default(),
            client_id: None,
            scope: default_scope(),
        }
    }
}

fn default_scope() -> String {
    "https://monitor.azure.com/.default".to_string()
}

/// API configuration for connecting to Azure Monitor
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
}

impl Config {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate auth configuration
        if self.auth.scope.is_empty() {
            return Err("Invalid configuration: auth scope must be non-empty".to_string());
        }

        // Validate client_id format if present
        if let Some(client_id) = &self.auth.client_id {
            if !Self::is_valid_guid(client_id) {
                return Err(format!(
                    "Invalid configuration: client_id '{}' is not a valid GUID. \
                     Expected format: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                    client_id
                ));
            }
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

    /// Validate GUID format (XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX)
    fn is_valid_guid(s: &str) -> bool {
        // GUID regex: 8-4-4-4-12 hexadecimal digits
        let parts: Vec<&str> = s.split('-').collect();

        if parts.len() != 5 {
            return false;
        }

        // Check each part has correct length and contains only hex digits
        let expected_lengths = [8, 4, 4, 4, 12];
        for (part, &expected_len) in parts.iter().zip(expected_lengths.iter()) {
            if part.len() != expected_len {
                return false;
            }
            if !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_config() {
        let config = Config {
            api: ApiConfig {
                dcr_endpoint: "https://example.com".to_string(),
                stream_name: "mystream".to_string(),
                dcr: "mydcr".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig {
                scope: "https://monitor.azure.com/.default".to_string(),
                client_id: Some("myclientid".to_string()),
                method: AuthMethod::ManagedIdentity,
            },
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_config_missing_api_fields() {
        let config = Config {
            api: ApiConfig {
                dcr_endpoint: "".to_string(),
                stream_name: "".to_string(),
                dcr: "".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig::default(),
        };

        assert!(config.validate().is_err());
        assert_eq!(
            config.validate().unwrap_err(),
            "Invalid configuration: dcr_endpoint must be non-empty"
        );
    }

    #[test]
    fn test_valid_guid() {
        assert!(Config::is_valid_guid("12345678-1234-1234-1234-123456789012"));
        assert!(Config::is_valid_guid("00000000-0000-0000-0000-000000000000"));
        assert!(Config::is_valid_guid("ABCDEF01-2345-6789-ABCD-EF0123456789"));
        assert!(Config::is_valid_guid("abcdef01-2345-6789-abcd-ef0123456789"));
    }

    #[test]
    fn test_invalid_guid() {
        // Wrong format
        assert!(!Config::is_valid_guid("12345678123412341234123456789012")); // No dashes
        assert!(!Config::is_valid_guid("12345678-1234-1234-1234"));           // Too short
        assert!(!Config::is_valid_guid("12345678-1234-1234-1234-123456789012-extra")); // Too long
        assert!(!Config::is_valid_guid("1234567-1234-1234-1234-123456789012")); // Wrong segment length
        assert!(!Config::is_valid_guid("12345678-123-1234-1234-123456789012")); // Wrong segment length

        // Invalid characters
        assert!(!Config::is_valid_guid("12345678-1234-1234-1234-12345678901G")); // G is not hex
        assert!(!Config::is_valid_guid("ZZZZZZZZ-1234-1234-1234-123456789012")); // Z is not hex
        assert!(!Config::is_valid_guid("not-a-guid"));
        assert!(!Config::is_valid_guid(""));
    }

    #[test]
    fn test_config_with_valid_client_id() {
        let config = Config {
            api: ApiConfig {
                dcr_endpoint: "https://example.com".to_string(),
                stream_name: "mystream".to_string(),
                dcr: "mydcr".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig {
                scope: "https://monitor.azure.com/.default".to_string(),
                client_id: Some("12345678-1234-1234-1234-123456789012".to_string()),
                method: AuthMethod::ManagedIdentity,
            },
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_invalid_client_id() {
        let config = Config {
            api: ApiConfig {
                dcr_endpoint: "https://example.com".to_string(),
                stream_name: "mystream".to_string(),
                dcr: "mydcr".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig {
                scope: "https://monitor.azure.com/.default".to_string(),
                client_id: Some("not-a-guid".to_string()),
                method: AuthMethod::ManagedIdentity,
            },
        };

        assert!(config.validate().is_err());
        let err = config.validate().unwrap_err();
        assert!(err.contains("not a valid GUID"));
    }

    #[test]
    fn test_config_with_no_client_id() {
        // System-assigned identity (no client_id) should be valid
        let config = Config {
            api: ApiConfig {
                dcr_endpoint: "https://example.com".to_string(),
                stream_name: "mystream".to_string(),
                dcr: "mydcr".to_string(),
                schema: SchemaConfig::default(),
            },
            auth: AuthConfig {
                scope: "https://monitor.azure.com/.default".to_string(),
                client_id: None,
                method: AuthMethod::ManagedIdentity,
            },
        };

        assert!(config.validate().is_ok());
    }
}
