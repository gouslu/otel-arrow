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
    
    /// Optional cache engine configuration
    pub cache_engine_config: Option<CacheEngineConfig>,
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

/// Cache engine configuration for reliability
#[derive(Debug, Deserialize, Clone)]
pub struct CacheEngineConfig {
    /// Path for cache storage
    pub cache_path: String,
    
    /// Maximum disk usage in MB
    pub max_disk_usage_mb: u64,
    
    /// Cache expiration in hours
    pub expiration_duration_hours: u32,
}

impl Config {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        // Validate auth configuration
        if self.auth.scope.is_empty() {
            return Err("Invalid configuration: auth scope must be non-empty".to_string());
        }
        
        // Validate cache engine config if present
        if let Some(ref cache_config) = self.cache_engine_config {
            if cache_config.cache_path.is_empty() {
                return Err(
                    "Invalid configuration: CacheEngineConfig requires non-empty cache_path"
                        .to_string(),
                );
            }
            if cache_config.max_disk_usage_mb == 0 {
                return Err(
                    "Invalid configuration: max_disk_usage_mb must be positive".to_string()
                );
            }
            if cache_config.expiration_duration_hours == 0 {
                return Err(
                    "Invalid configuration: expiration_duration_hours must be positive"
                        .to_string(),
                );
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
}