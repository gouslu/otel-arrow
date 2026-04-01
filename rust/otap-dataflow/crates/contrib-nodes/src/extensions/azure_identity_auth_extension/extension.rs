// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Azure Identity Auth Extension.
//!
//! Provides Azure authentication via managed identity or developer tools.
//! Uses `Arc`-based sharing — clones share state via atomic reference counting.
//! `Clone + Send` allows distribution across threads and into `Box<dyn shared::Extension>`.

use async_trait::async_trait;
use azure_core::credentials::{AccessToken, TokenCredential};
use azure_identity::{
    DeveloperToolsCredential, DeveloperToolsCredentialOptions, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId,
};
use otap_df_engine::capability::bearer_token_provider::BearerToken;
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::{ControlChannel, EffectHandler};
use otap_df_engine::shared::capability::BearerTokenProvider;
use otap_df_engine::shared::extension::Extension;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_telemetry::{otel_debug, otel_error, otel_info, otel_warn};
use std::sync::Arc;
use tokio::sync::watch;

use super::config::{AuthMethod, Config};
use super::error::Error;

// ── Constants ───────────────────────────────────────────────────────────────

/// Minimum delay between token refresh retry attempts in seconds.
const MIN_RETRY_DELAY_SECS: f64 = 5.0;
/// Maximum delay between token refresh retry attempts in seconds.
const MAX_RETRY_DELAY_SECS: f64 = 30.0;
/// Maximum jitter percentage (±10%) to add to retry delays.
const MAX_RETRY_JITTER_RATIO: f64 = 0.10;

/// Buffer time before token expiry to trigger refresh (in seconds).
const TOKEN_EXPIRY_BUFFER_SECS: u64 = 299;
/// Minimum interval between token refresh attempts (in seconds).
const MIN_TOKEN_REFRESH_INTERVAL_SECS: u64 = 10;
/// Retry interval when token refresh fails (in seconds).
const TOKEN_REFRESH_RETRY_SECS: u64 = 10;

// ── Extension ───────────────────────────────────────────────────────────────

/// Azure Identity Auth Extension.
///
/// `Clone + Send` — all state is behind `Arc`. Clones share the same
/// credential provider and token broadcast channel.
#[derive(Clone)]
pub struct AzureIdentityAuthExtension {
    name: String,
    credential: Arc<dyn TokenCredential>,
    credential_type: &'static str,
    scope: String,
    client_id: Option<String>,
    token_sender: Arc<watch::Sender<Option<BearerToken>>>,
}

impl AzureIdentityAuthExtension {
    /// Creates a new Azure Identity Auth Extension.
    pub fn new(name: String, config: Config) -> Result<Self, Error> {
        let (credential, credential_type) = create_credential(&config.method, &config.client_id)?;
        let (token_sender, _) = watch::channel(None);
        let token_sender = Arc::new(token_sender);

        Ok(Self {
            name,
            credential,
            credential_type,
            scope: config.scope,
            client_id: config.client_id,
            token_sender,
        })
    }

    fn send_token(&self, token: Option<BearerToken>) {
        let _ = self.token_sender.send(token);
    }
}

#[async_trait]
impl BearerTokenProvider for AzureIdentityAuthExtension {
    async fn get_token(&self) -> Result<BearerToken, otap_df_engine::capability::registry::Error> {
        let access_token = get_token_with_retry(self.credential.as_ref(), &self.scope).await?;

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
        mut ctrl_chan: ControlChannel,
        _: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        otel_info!(
            "azure_identity_auth.start",
            name = self.name.as_str(),
            credential_type = self.credential_type,
            scope = self.scope.as_str(),
            client_id = self.client_id.as_deref().unwrap_or("none"),
        );

        let mut next_token_refresh = tokio::time::Instant::now();

        loop {
            tokio::select! {
                biased;

                _ = tokio::time::sleep_until(next_token_refresh) => {
                    match get_token_with_retry(self.credential.as_ref(), &self.scope).await {
                        Ok(access_token) => {
                            let bearer_token = BearerToken::new(
                                access_token.token.secret().to_string(),
                                access_token.expires_on.unix_timestamp(),
                            );

                            self.send_token(Some(bearer_token.clone()));

                            next_token_refresh = get_next_token_refresh(&bearer_token);

                            let refresh_in = next_token_refresh.saturating_duration_since(tokio::time::Instant::now());
                            let total_secs = refresh_in.as_secs();
                            let hours = total_secs / 3600;
                            let minutes = (total_secs % 3600) / 60;
                            let seconds = total_secs % 60;

                            otel_info!(
                                "azure_identity_auth.token_acquired",
                                refresh_in = format!("{}h {}m {}s", hours, minutes, seconds)
                            );
                        }
                        Err(e) => {
                            otel_error!(
                                "azure_identity_auth.token_acquisition_failed",
                                error = ?e,
                                retry_secs = TOKEN_REFRESH_RETRY_SECS
                            );
                            next_token_refresh = tokio::time::Instant::now()
                                + tokio::time::Duration::from_secs(TOKEN_REFRESH_RETRY_SECS);
                        }
                    }
                }

                msg = ctrl_chan.recv() => {
                    match msg? {
                        ExtensionControlMsg::Shutdown { reason, .. } => {
                            otel_info!(
                                "azure_identity_auth.shutdown",
                                reason = %reason
                            );
                            break;
                        }
                        ExtensionControlMsg::Config { config } => {
                            otel_info!(
                                "azure_identity_auth.config_update",
                                config = ?config
                            );
                        }
                        ExtensionControlMsg::CollectTelemetry { .. } => {}
                    }
                }
            }
        }

        Ok(TerminalState::default())
    }
}

// ── Credential creation ─────────────────────────────────────────────────────

/// Creates a credential provider based on the configuration.
fn create_credential(
    method: &AuthMethod,
    client_id: &Option<String>,
) -> Result<(Arc<dyn TokenCredential>, &'static str), Error> {
    match method {
        AuthMethod::ManagedIdentity => {
            let mut options = ManagedIdentityCredentialOptions::default();

            let credential_type = if let Some(client_id) = client_id {
                options.user_assigned_id = Some(UserAssignedId::ClientId(client_id.clone()));
                "user_assigned_managed_identity"
            } else {
                "system_assigned_managed_identity"
            };

            Ok((
                ManagedIdentityCredential::new(Some(options))
                    .map_err(|e| Error::create_credential(AuthMethod::ManagedIdentity, e))?,
                credential_type,
            ))
        }
        AuthMethod::Development => Ok((
            DeveloperToolsCredential::new(Some(DeveloperToolsCredentialOptions::default()))
                .map_err(|e| Error::create_credential(AuthMethod::Development, e))?,
            "developer_tools",
        )),
    }
}

// ── Token helpers ───────────────────────────────────────────────────────────

/// Gets a token with retry logic and exponential backoff.
async fn get_token_with_retry(
    credential: &dyn TokenCredential,
    scope: &str,
) -> Result<AccessToken, Error> {
    let mut attempt = 0_i32;
    loop {
        attempt += 1;

        match credential
            .get_token(
                &[scope],
                Some(azure_core::credentials::TokenRequestOptions::default()),
            )
            .await
        {
            Ok(token) => {
                otel_debug!(
                    "azure_identity_auth.get_token_succeeded",
                    expires_on = %token.expires_on
                );
                return Ok(token);
            }
            Err(e) => {
                otel_warn!(
                    "azure_identity_auth.get_token_failed",
                    attempt = attempt,
                    error = %e
                );
            }
        }

        let base_delay_secs = MIN_RETRY_DELAY_SECS * 2.0_f64.powi(attempt - 1);
        let capped_delay_secs = base_delay_secs.min(MAX_RETRY_DELAY_SECS);

        let jitter_range = capped_delay_secs * MAX_RETRY_JITTER_RATIO;
        let jitter = if jitter_range > 0.0 {
            let random_factor = rand::random::<f64>() * 2.0 - 1.0;
            random_factor * jitter_range
        } else {
            0.0
        };

        let delay_secs = (capped_delay_secs + jitter).max(1.0);
        let delay = tokio::time::Duration::from_secs_f64(delay_secs);

        otel_warn!(
            "azure_identity_auth.retry_scheduled",
            delay_secs = %delay_secs
        );
        tokio::time::sleep(delay).await;
    }
}

/// Calculates when the next token refresh should occur.
fn get_next_token_refresh(token: &BearerToken) -> tokio::time::Instant {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let duration_remaining = if token.expires_on > now_secs {
        std::time::Duration::from_secs((token.expires_on - now_secs) as u64)
    } else {
        std::time::Duration::ZERO
    };

    let token_valid_until = tokio::time::Instant::now() + duration_remaining;
    let next_token_refresh =
        token_valid_until - tokio::time::Duration::from_secs(TOKEN_EXPIRY_BUFFER_SECS);
    std::cmp::max(
        next_token_refresh,
        tokio::time::Instant::now()
            + tokio::time::Duration::from_secs(MIN_TOKEN_REFRESH_INTERVAL_SECS),
    )
}
