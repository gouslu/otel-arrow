// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared logic for the Azure Identity Auth Extension.
//!
//! Contains credential creation, token retry logic, refresh scheduling,
//! and the event loop — used by both local and shared variants.

use azure_core::credentials::{AccessToken, TokenCredential};
use azure_identity::{
    DeveloperToolsCredential, DeveloperToolsCredentialOptions, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId,
};
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::ControlChannel;
use otap_df_engine::extension::bearer_token_provider::BearerToken;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_telemetry::{otel_debug, otel_error, otel_info, otel_warn};
use std::sync::Arc;

use super::config::AuthMethod;
use super::error::Error;

/// Minimum delay between token refresh retry attempts in seconds.
pub(crate) const MIN_RETRY_DELAY_SECS: f64 = 5.0;
/// Maximum delay between token refresh retry attempts in seconds.
pub(crate) const MAX_RETRY_DELAY_SECS: f64 = 30.0;
/// Maximum jitter percentage (±10%) to add to retry delays.
pub(crate) const MAX_RETRY_JITTER_RATIO: f64 = 0.10;

/// Buffer time before token expiry to trigger refresh (in seconds).
pub(crate) const TOKEN_EXPIRY_BUFFER_SECS: u64 = 299;
/// Minimum interval between token refresh attempts (in seconds).
pub(crate) const MIN_TOKEN_REFRESH_INTERVAL_SECS: u64 = 10;
/// Retry interval when token refresh fails (in seconds).
pub(crate) const TOKEN_REFRESH_RETRY_SECS: u64 = 10;

/// Creates a credential provider based on the configuration.
///
/// Returns the credential and a human-readable description of the credential type.
/// The credential is always `Arc` because the Azure SDK requires it.
pub(crate) fn create_credential(
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

/// Gets a token directly from the credential provider.
pub(crate) async fn get_token_internal(
    credential: &dyn TokenCredential,
    scope: &str,
) -> Result<AccessToken, Error> {
    credential
        .get_token(
            &[scope],
            Some(azure_core::credentials::TokenRequestOptions::default()),
        )
        .await
        .map_err(Error::token_acquisition)
}

/// Gets a token with retry logic and exponential backoff.
pub(crate) async fn get_token_with_retry(
    credential: &dyn TokenCredential,
    scope: &str,
) -> Result<AccessToken, Error> {
    let mut attempt = 0_i32;
    loop {
        attempt += 1;

        match get_token_internal(credential, scope).await {
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
pub(crate) fn get_next_token_refresh(token: &BearerToken) -> tokio::time::Instant {
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

/// Trait for broadcasting token updates — abstracts over Arc<watch::Sender> vs watch::Sender.
pub(crate) trait TokenBroadcaster {
    fn send_token(&self, token: Option<BearerToken>);
    fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
}

/// Runs the extension event loop.
///
/// Generic over the token broadcaster — local uses direct `watch::Sender`,
/// shared uses `Arc<watch::Sender>`.
pub(crate) async fn run_event_loop(
    name: &str,
    credential_type: &str,
    scope: &str,
    client_id: Option<&str>,
    credential: &dyn TokenCredential,
    token_broadcaster: &dyn TokenBroadcaster,
    mut ctrl_chan: ControlChannel,
) -> Result<TerminalState, EngineError> {
    otel_info!(
        "azure_identity_auth.start",
        name = name,
        credential_type = credential_type,
        scope = scope,
        client_id = client_id.unwrap_or("none"),
    );

    let mut next_token_refresh = tokio::time::Instant::now();

    loop {
        tokio::select! {
            biased;

            _ = tokio::time::sleep_until(next_token_refresh) => {
                match get_token_with_retry(credential, scope).await {
                    Ok(access_token) => {
                        let bearer_token = BearerToken::new(
                            access_token.token.secret().to_string(),
                            access_token.expires_on.unix_timestamp(),
                        );

                        token_broadcaster.send_token(Some(bearer_token.clone()));

                        next_token_refresh = get_next_token_refresh(&bearer_token);

                        let refresh_in = next_token_refresh.saturating_duration_since(tokio::time::Instant::now());
                        let total_secs = refresh_in.as_secs();
                        let hours = total_secs / 3600;
                        let minutes = (total_secs % 3600) / 60;
                        let seconds = total_secs % 60;

                        otel_info!(
                            "azure_identity_auth.token_refreshed",
                            refresh_in = format!("{}h {}m {}s", hours, minutes, seconds)
                        );
                    }
                    Err(e) => {
                        otel_error!(
                            "azure_identity_auth.token_refresh_loop_failed",
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
