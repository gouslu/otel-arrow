// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the `azure_identity_auth` extension.
//!
//! Mirrors the auth surface of `azure_monitor_exporter` (`ManagedIdentity`
//! and `Development`). Refresh-loop tuning (retry delay, expiry skew,
//! minimum refresh cadence) is intentionally not exposed — see the
//! constants in [`extension`](super::extension).

use serde::Deserialize;

use super::error::Error;

/// Authentication method for Azure.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AuthMethod {
    /// System- or user-assigned managed identity.
    #[serde(alias = "msi", alias = "managed_identity")]
    #[default]
    ManagedIdentity,

    /// Developer tooling (Azure CLI / Azure Developer CLI).
    #[serde(alias = "dev", alias = "developer", alias = "cli")]
    Development,
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMethod::ManagedIdentity => write!(f, "managed_identity"),
            AuthMethod::Development => write!(f, "development"),
        }
    }
}

/// Top-level configuration.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Authentication method to use.
    #[serde(default)]
    pub method: AuthMethod,

    /// Client ID for a user-assigned managed identity. Only used when
    /// `method` is `ManagedIdentity`. If absent, system-assigned identity
    /// is used.
    #[serde(default)]
    pub client_id: Option<String>,

    /// OAuth scope for token acquisition (e.g.
    /// `https://monitor.azure.com/.default`).
    #[serde(default = "default_scope")]
    pub scope: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            method: AuthMethod::default(),
            client_id: None,
            scope: default_scope(),
        }
    }
}

impl Config {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), Error> {
        if self.scope.trim().is_empty() {
            return Err(Error::InvalidConfig {
                message: "oauth scope cannot be empty".to_string(),
            });
        }
        Ok(())
    }
}

fn default_scope() -> String {
    "https://monitor.azure.com/.default".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_managed_identity_and_monitor_scope() {
        let cfg = Config::default();
        assert_eq!(cfg.method, AuthMethod::ManagedIdentity);
        assert!(cfg.client_id.is_none());
        assert_eq!(cfg.scope, "https://monitor.azure.com/.default");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn empty_scope_fails_validation() {
        let cfg = Config {
            scope: "   ".to_string(),
            ..Config::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn auth_method_aliases_deserialize() {
        let cfg: Config = serde_json::from_str(r#"{"method":"msi"}"#).unwrap();
        assert_eq!(cfg.method, AuthMethod::ManagedIdentity);

        let cfg: Config = serde_json::from_str(r#"{"method":"cli"}"#).unwrap();
        assert_eq!(cfg.method, AuthMethod::Development);
    }
}
