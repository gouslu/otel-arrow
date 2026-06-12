// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Azure Identity Auth — Active + Shared extension exposing the
//! `BearerTokenProvider` capability.
//!
//! Tokens are acquired via `azure_identity` using either Managed Identity or
//! Developer Tools credentials and pushed onto an internal `watch` channel by
//! the extension's `start()` task. Consumers obtain the latest token via the
//! `BearerTokenProvider::get_token()` or `token_stream()` capability methods.

use linkme::distributed_slice;
use otap_df_config::extension::ExtensionUserConfig;
use otap_df_config::{ExtensionId, validation::validate_typed_config};
use otap_df_engine::ExtensionFactory;
use otap_df_engine::capability::bearer_token_provider::BearerTokenProvider;
use otap_df_engine::config::ExtensionConfig;
use otap_df_engine::context::ExtensionContext;
use otap_df_engine::extension::wrapper::ExtensionVariant;
use otap_df_engine::extension::{ExtensionBundle, ExtensionWrapper};
use otap_df_engine::extension_capabilities;
use otap_df_otap::OTAP_EXTENSION_FACTORIES;
use std::sync::Arc;

pub mod auth;
pub mod config;
mod error;
mod extension;
/// Telemetry metrics for the extension.
pub mod metrics;

pub use config::{AuthMethod, Config};
pub use error::Error;
pub use extension::AzureIdentityAuthExtension;
pub use metrics::{AzureIdentityAuthMetrics, AzureIdentityAuthMetricsTracker};

/// URN identifying the extension in pipeline configurations.
pub const AZURE_IDENTITY_AUTH_URN: &str = "urn:microsoft:extension:azure_identity_auth";

fn create(
    ext_ctx: &ExtensionContext,
    name: ExtensionId,
    user_config: Arc<ExtensionUserConfig>,
    extension_config: &ExtensionConfig,
) -> Result<ExtensionBundle, otap_df_config::error::Error> {
    let cfg: Config = serde_json::from_value(user_config.config.clone()).map_err(|e| {
        otap_df_config::error::Error::InvalidUserConfig {
            error: e.to_string(),
        }
    })?;
    cfg.validate()
        .map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
            error: e.to_string(),
        })?;
    let auth =
        auth::Auth::new(&cfg).map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
            error: e.to_string(),
        })?;
    let entity_key = ext_ctx.register_extension_entity(name.clone(), ExtensionVariant::Shared);
    let metric_set = ext_ctx.register_metric_set_for_entity::<AzureIdentityAuthMetrics>(entity_key);
    let metrics = AzureIdentityAuthMetricsTracker::new(metric_set);
    let ext = AzureIdentityAuthExtension::new(name.to_string(), auth, metrics);

    ExtensionWrapper::builder(name, user_config, extension_config)
        .active()
        .shared::<AzureIdentityAuthExtension>(ext)
        .build()
        .map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
            error: e.to_string(),
        })
}

/// Automatic registration with the OTAP pipeline factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static AZURE_IDENTITY_AUTH_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: AZURE_IDENTITY_AUTH_URN,
    description: "Active+Shared extension exposing BearerTokenProvider via azure_identity",
    documentation_url: "",
    capabilities: Some(extension_capabilities!(
        shared: AzureIdentityAuthExtension => [BearerTokenProvider]
    )),
    create,
    validate_config: validate_typed_config::<Config>,
};
