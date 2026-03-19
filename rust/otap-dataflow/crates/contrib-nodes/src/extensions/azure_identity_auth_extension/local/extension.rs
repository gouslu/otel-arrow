// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local (!Send) Azure Identity Auth Extension.
//!
//! Uses `Rc`-based sharing — the single `Rc<Self>` instance serves both
//! the lifecycle task and all local capability consumers. No `Arc` overhead
//! for extension-owned state; the credential stays `Arc` because the
//! Azure SDK requires it.

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::capability::bearer_token_provider::BearerToken;
use otap_df_engine::local::capability::BearerTokenProvider;
use otap_df_engine::extension::{ControlChannel, EffectHandler};
use otap_df_engine::local::extension::Extension;
use otap_df_engine::terminal_state::TerminalState;
use std::sync::Arc;
use tokio::sync::watch;

use crate::extensions::azure_identity_auth_extension::config::Config;
use crate::extensions::azure_identity_auth_extension::core;
use crate::extensions::azure_identity_auth_extension::error::Error;

/// Local variant of the Azure Identity Auth Extension.
///
/// Designed for single-threaded `LocalSet` execution. The extension instance
/// is wrapped in `Rc` by the engine — all consumers and the lifecycle task
/// share the same allocation via `Rc::clone`.
///
/// The `watch::Sender` is owned directly (not behind `Arc`) since `Rc`
/// sharing eliminates the need for atomic reference counting.
pub struct AzureIdentityAuthExtension {
    name: String,
    credential: Arc<dyn TokenCredential>,
    credential_type: &'static str,
    scope: String,
    client_id: Option<String>,
    token_sender: watch::Sender<Option<BearerToken>>,
}

impl AzureIdentityAuthExtension {
    /// Creates a new local Azure Identity Auth Extension.
    pub fn new(name: String, config: Config) -> Result<Self, Error> {
        let (credential, credential_type) =
            core::create_credential(&config.method, &config.client_id)?;
        let (token_sender, _) = watch::channel(None);

        Ok(Self {
            name,
            credential,
            credential_type,
            scope: config.scope,
            client_id: config.client_id,
            token_sender,
        })
    }
}

impl core::TokenBroadcaster for AzureIdentityAuthExtension {
    fn send_token(&self, token: Option<BearerToken>) {
        let _ = self.token_sender.send(token);
    }
}

#[async_trait(?Send)]
impl BearerTokenProvider for AzureIdentityAuthExtension {
    async fn get_token(&self) -> Result<BearerToken, otap_df_engine::capability::registry::Error> {
        let access_token =
            core::get_token_with_retry(self.credential.as_ref(), &self.scope).await?;

        Ok(BearerToken::new(
            access_token.token.secret().to_string(),
            access_token.expires_on.unix_timestamp(),
        ))
    }

    fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
        self.token_sender.subscribe()
    }
}

#[async_trait(?Send)]
impl Extension for AzureIdentityAuthExtension {
    async fn start(
        self: std::rc::Rc<Self>,
        ctrl_chan: ControlChannel,
        _: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        core::run_event_loop(
            &self.name,
            self.credential_type,
            &self.scope,
            self.client_id.as_deref(),
            self.credential.as_ref(),
            self.as_ref(),
            ctrl_chan,
        )
        .await
    }
}
