// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Azure Identity Auth Extension for OTAP.
//!
//! Provides Azure authentication services to the pipeline using Azure Identity.
//! This extension manages token acquisition and refresh, making credentials
//! available to other components (e.g., exporters) that need Azure authentication.
//!
//! # Features
//!
//! - Managed Identity authentication (system or user-assigned)
//! - Developer tools authentication (Azure CLI, Azure Developer CLI)
//! - Automatic token refresh with exponential backoff retry
//! - Shared credential access across pipeline components
//!
//! # Usage
//!
//! Configure the extension in the pipeline configuration:
//!
//! ```yaml
//! extensions:
//!   azure_auth:
//!     type: "urn:microsoft:extension:azure_identity_auth"
//!     config:
//!       method: managed_identity
//!       scope: "https://monitor.azure.com/.default"
//! ```
//!
//! Consumers bind the capability in node config and retrieve the handle from
//! resolved capabilities at factory time:
//!
//! ```ignore
//! let auth = capabilities.require::<
//!     BearerTokenProvider
//! >()?;
//! let mut token_rx = auth.subscribe_token_refresh();
//! ```

use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExtensionFactory;
use otap_df_engine::config::ExtensionConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::extension::ExtensionWrapper;
use otap_df_engine::node::NodeId;
use std::rc::Rc;
use std::sync::Arc;
use otap_df_engine::extension::bearer_token_provider::BearerTokenProvider;

use otap_df_otap::OTAP_EXTENSION_FACTORIES;

mod config;
mod error;
mod extension;

pub use config::{AuthMethod, Config};
pub use error::Error;
pub use extension::AzureIdentityAuthExtension;

/// URN identifying the Azure Identity Auth Extension in configuration pipelines.
pub const AZURE_IDENTITY_AUTH_EXTENSION_URN: &str = "urn:microsoft:extension:azure_identity_auth";

/// Register Azure Identity Auth Extension with the OTAP extension factory.
///
/// Uses the `distributed_slice` macro for automatic discovery by the dataflow engine.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static AZURE_IDENTITY_AUTH_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: AZURE_IDENTITY_AUTH_EXTENSION_URN,
    description: "Azure Identity authentication via managed identity or developer tools",
    documentation_url: "https://github.com/open-telemetry/otel-arrow/tree/main/rust/otap-dataflow/crates/contrib-nodes/src/extensions/azure_identity_auth_extension",
    capabilities: otap_df_engine::extension_capabilities!(
        AzureIdentityAuthExtension => BearerTokenProvider
    ),
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             extension_config: &ExtensionConfig| {
        // Deserialize user config JSON into typed Config
        let cfg: Config = serde_json::from_value(node_config.config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            }
        })?;

        // Validate the configuration
        cfg.validate()
            .map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
                error: e.to_string(),
            })?;

        // Create the extension
        let extension =
            AzureIdentityAuthExtension::new(node.name.to_string(), cfg).map_err(|e| {
                otap_df_config::error::Error::InvalidUserConfig {
                    error: e.to_string(),
                }
            })?;

        Ok(ExtensionWrapper::dual(
            extension.clone(),
            Rc::new(extension),
            node,
            node_config,
            extension_config,
        ))
    },
    validate_config: otap_df_config::validation::validate_typed_config::<Config>,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_urn() {
        assert_eq!(
            AZURE_IDENTITY_AUTH_EXTENSION_URN,
            "urn:microsoft:extension:azure_identity_auth"
        );
    }

    #[test]
    fn test_factory_name_matches_urn() {
        assert_eq!(
            AZURE_IDENTITY_AUTH_EXTENSION.name,
            AZURE_IDENTITY_AUTH_EXTENSION_URN
        );
    }
}
