// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) Azure Identity Auth Extension.
//!
//! Uses `Arc`-based sharing — clones share state via atomic reference counting.
//! `Clone + Send` allows distribution across threads and into `Box<dyn shared::Extension>`.

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::bearer_token_provider::BearerToken;
use otap_df_engine::extension::bearer_token_provider::shared::BearerTokenProvider;
use otap_df_engine::extension::{ControlChannel, EffectHandler};
use otap_df_engine::shared::extension::Extension;
use otap_df_engine::terminal_state::TerminalState;
use std::sync::Arc;
use tokio::sync::watch;

use crate::extensions::azure_identity_auth_extension::config::{AuthMethod, Config};
use crate::extensions::azure_identity_auth_extension::core;
use crate::extensions::azure_identity_auth_extension::error::Error;

/// Shared variant of the Azure Identity Auth Extension.
///
/// `Clone + Send` — all state is behind `Arc`. Clones share the same
/// credential provider and token broadcast channel.
#[derive(Clone)]
pub struct AzureIdentityAuthExtension {
    name: String,
    credential: Arc<dyn TokenCredential>,
    credential_type: &'static str,
    scope: String,
    method: AuthMethod,
    client_id: Option<String>,
    token_sender: Arc<watch::Sender<Option<BearerToken>>>,
}

impl AzureIdentityAuthExtension {
    /// Creates a new shared Azure Identity Auth Extension.
    pub fn new(name: String, config: Config) -> Result<Self, Error> {
        let (credential, credential_type) =
            core::create_credential(&config.method, &config.client_id)?;
        let (token_sender, _) = watch::channel(None);
        let token_sender = Arc::new(token_sender);

        Ok(Self {
            name,
            credential,
            credential_type,
            scope: config.scope,
            method: config.method,
            client_id: config.client_id,
            token_sender,
        })
    }
}

impl core::TokenBroadcaster for AzureIdentityAuthExtension {
    fn send_token(&self, token: Option<BearerToken>) {
        let _ = self.token_sender.send(token);
    }

    fn subscribe(&self) -> watch::Receiver<Option<BearerToken>> {
        self.token_sender.subscribe()
    }
}

#[async_trait]
impl BearerTokenProvider for AzureIdentityAuthExtension {
    async fn get_token(&self) -> Result<BearerToken, otap_df_engine::extension::registry::Error> {
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

#[async_trait]
impl Extension for AzureIdentityAuthExtension {
    async fn start(
        self: Box<Self>,
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
