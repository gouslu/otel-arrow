// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension configuration types.
//!
//! Extensions have a simpler configuration model than data-path nodes -- they
//! have no output ports, no wiring contracts, and no header policies.

pub use crate::extension_urn::ExtensionUrn;
use crate::{CapabilityId, ExtensionId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// User configuration for an extension instance.
///
/// Unlike [`NodeUserConfig`](crate::node::NodeUserConfig), extensions have no
/// output ports, wiring contracts, or transport header policies.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionUserConfig {
    /// The extension type URN identifying the plugin (factory) to use.
    pub r#type: ExtensionUrn,

    /// An optional description of this extension.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Extension-specific configuration (interpreted by the extension itself).
    #[serde(default)]
    #[schemars(extend("x-kubernetes-preserve-unknown-fields" = true))]
    pub config: Value,

    /// Capability dependencies mapping capability names to provider extensions.
    ///
    /// The engine constructs and starts providers before this extension. An
    /// extension factory claims each dependency through the same typed,
    /// one-shot capabilities API used by node factories.
    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        deserialize_with = "crate::deserialize_capability_bindings"
    )]
    pub capabilities: HashMap<CapabilityId, ExtensionId>,
}

impl ExtensionUserConfig {
    /// Creates a new `ExtensionUserConfig` with the specified type URN and config.
    #[must_use]
    pub fn new(r#type: ExtensionUrn, config: Value) -> Self {
        Self {
            r#type,
            description: None,
            config,
            capabilities: HashMap::new(),
        }
    }

    /// Creates a new `ExtensionUserConfig` with the specified type URN and
    /// default (null) config.
    #[must_use]
    pub fn with_type<U: Into<ExtensionUrn>>(r#type: U) -> Self {
        Self {
            r#type: r#type.into(),
            description: None,
            config: Value::Null,
            capabilities: HashMap::new(),
        }
    }

    /// Returns a clone of this extension config with credential header values
    /// redacted, for safe exposure through the admin/config snapshot APIs.
    ///
    /// Extension `config` is the same raw [`Value`] mechanism as
    /// [`NodeUserConfig::config`](crate::node::NodeUserConfig::config), so an
    /// extension that carries static `headers` (credentials) is redacted with
    /// the same policy as a node: every value under any `headers` object is
    /// replaced with
    /// [`REDACTED_HEADER_VALUE`](crate::node::REDACTED_HEADER_VALUE) while the
    /// keys are preserved. The stored config is left unchanged.
    #[must_use]
    pub fn redacted_for_snapshot(&self) -> ExtensionUserConfig {
        let mut redacted = self.clone();
        crate::node::redact_secret_headers(&mut redacted.config);
        redacted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_user_config_deserialize() {
        let yaml = r#"
type: "urn:otap:extension:sample_kv_store"
config:
  capacity: 100
"#;
        let config: ExtensionUserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.r#type.id(), "sample_kv_store");
        assert_eq!(config.config["capacity"], 100);
    }

    /// Scenario: An extension declares a capability dependency in YAML.
    /// Guarantees: The provider extension ID is retained for dependency resolution.
    #[test]
    fn test_extension_user_config_deserializes_capabilities() {
        let yaml = r#"
type: "urn:otap:extension:auth"
capabilities:
  some_cap: "provider"
"#;
        let config: ExtensionUserConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.capabilities.get("some_cap").map(AsRef::as_ref),
            Some("provider")
        );
    }

    /// Scenario: An extension repeats a capability dependency key in YAML.
    /// Guarantees: Configuration loading rejects the ambiguous dependency.
    #[test]
    fn extension_capabilities_reject_duplicate_keys() {
        let yaml = r#"
type: "urn:otap:extension:auth"
capabilities:
  some_cap: "provider_a"
  some_cap: "provider_b"
"#;
        let error = serde_yaml::from_str::<ExtensionUserConfig>(yaml)
            .expect_err("duplicate capability keys must be rejected");
        assert!(error.to_string().contains("duplicate capability key"));
    }

    #[test]
    fn redacted_for_snapshot_masks_extension_headers() {
        let yaml = r#"
type: "urn:otap:extension:headers_setter"
config:
  headers:
    authorization: "Bearer ext-super-secret"
"#;
        let cfg: ExtensionUserConfig = serde_yaml::from_str(yaml).unwrap();
        let redacted = cfg.redacted_for_snapshot();
        assert_eq!(
            redacted.config["headers"]["authorization"],
            crate::node::REDACTED_HEADER_VALUE
        );
        // The original extension config is left untouched (redaction is a copy).
        assert_eq!(
            cfg.config["headers"]["authorization"],
            "Bearer ext-super-secret"
        );
    }
}
