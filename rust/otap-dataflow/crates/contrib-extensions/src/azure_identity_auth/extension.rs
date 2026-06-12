// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Active+Shared extension implementing the `BearerTokenProvider` capability.
//!
//! `start()` runs a refresh loop that publishes fresh tokens into an internal
//! `tokio::sync::watch` channel. On a failed acquisition the loop reschedules
//! itself after [`TOKEN_REFRESH_RETRY_SECS`] and keeps trying for the
//! lifetime of the extension. Capability consumers either read the latest
//! cached token via `get_token()` (fast path = watch cache; slow path =
//! single one-off fetch) or observe the stream of refreshes via
//! `token_stream()`.
//!
//! All state is held behind `Arc<Inner>` so the engine can clone the
//! extension freely — every clone observes the same token state.

use async_trait::async_trait;
use futures::Stream;
use futures::StreamExt;
use otap_df_engine::capability::bearer_token_provider::{
    BearerToken, BearerTokenProvider, shared::BearerTokenProvider as SharedBearerTokenProvider,
};
use otap_df_engine::capability::{CapabilityError, CapabilityErrorSource};
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::EffectHandler;
use otap_df_engine::shared::extension::{ControlChannel, Extension as SharedExtension};
use otap_df_engine::terminal_state::TerminalState;
use otap_df_telemetry::{otel_error, otel_info};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;

use super::auth::Auth;
use super::metrics::AzureIdentityAuthMetricsTracker;

// ── Refresh-loop tuning ─────────────────────────────────────────────────────
//
// Constants mirror the PoC at
// `crates/contrib-nodes/src/extensions/azure_identity_auth_extension`.

/// Refresh tokens this many seconds before `expires_on` (~5 minutes).
const TOKEN_EXPIRY_BUFFER_SECS: u64 = 299;
/// Floor on the time between successful refreshes — protects against
/// the credential returning a token that is already inside the skew window.
const MIN_TOKEN_REFRESH_INTERVAL_SECS: u64 = 10;
/// Reschedule interval after a failed token refresh. The outer `select!`
/// timer fires after this delay and tries `Auth::get_token` again. The
/// loop keeps retrying for the lifetime of the extension.
const TOKEN_REFRESH_RETRY_SECS: u64 = 10;

// ── Extension ───────────────────────────────────────────────────────────────

/// Azure Identity Auth extension.
///
/// Clones share the underlying `Inner` (Arc), so every capability consumer
/// and the active task observe the same token state.
#[derive(Clone)]
pub struct AzureIdentityAuthExtension {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    auth: Auth,
    tx: watch::Sender<Option<BearerToken>>,
    /// Pre-bound `(extension, capability)` factory used at every
    /// `BearerTokenProvider` error site so labeling stays consistent.
    cap_err: CapabilityErrorSource<BearerTokenProvider>,
    /// Serializes slow-path token fetches so concurrent `get_token()` callers
    /// (and the `start()` refresh loop) coalesce onto a single in-flight
    /// credential call instead of stampeding IMDS / the dev-tools backend.
    fetch_lock: tokio::sync::Mutex<()>,
    /// Telemetry tracker. `std::sync::Mutex` because writes are short and
    /// never held across an `.await`; both the background refresh loop and
    /// slow-path consumers contend on the same handle.
    metrics: Mutex<AzureIdentityAuthMetricsTracker>,
}

impl AzureIdentityAuthExtension {
    /// Construct a new extension from a pre-built [`Auth`] and metrics tracker.
    #[must_use]
    pub fn new(name: String, auth: Auth, metrics: AzureIdentityAuthMetricsTracker) -> Self {
        let (tx, _rx) = watch::channel(None);
        let cap_err = CapabilityErrorSource::<BearerTokenProvider>::new(name.clone());
        Self {
            inner: Arc::new(Inner {
                name,
                auth,
                tx,
                cap_err,
                fetch_lock: tokio::sync::Mutex::new(()),
                metrics: Mutex::new(metrics),
            }),
        }
    }
}

#[async_trait]
impl SharedBearerTokenProvider for AzureIdentityAuthExtension {
    async fn get_token(&self) -> Result<BearerToken, CapabilityError> {
        // Fast path: the watch already holds a token that is not yet inside
        // the refresh-skew window.
        if let Some(token) = fresh_cached(&self.inner) {
            return Ok(token);
        }
        // Slow path: single credential call (the Azure SDK does its own
        // caching internally). On error, surface it to the caller — they
        // can decide whether to retry. The background refresh loop is
        // responsible for keeping the cache fresh; this is only a
        // bootstrap / cache-miss path.
        // `fetch_and_publish` re-checks the cache under a mutex so concurrent
        // slow-path callers coalesce onto a single in-flight credential call.
        fetch_and_publish(&self.inner)
            .await
            .map_err(|e| self.inner.cap_err.wrap(e))
    }

    fn token_stream(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<BearerToken, CapabilityError>> + Send + 'static>> {
        // Active impl: subscribe to the internal `watch` channel that the
        // refresh task feeds. `WatchStream` yields the current value first
        // (possibly `None` if no refresh has completed yet) and then every
        // subsequent update; `filter_map` drops the initial `None` and
        // wraps the rest in `Ok`. Acquisition failures are handled and
        // retried inside `start()` and logged via `otel_error!`, so we
        // never surface `Err` on the stream.
        let rx = self.inner.tx.subscribe();
        Box::pin(WatchStream::new(rx).filter_map(|opt| async move { opt.map(Ok) }))
    }
}

#[async_trait]
impl SharedExtension for AzureIdentityAuthExtension {
    async fn start(
        self: Box<Self>,
        mut ctrl: ControlChannel,
        _eh: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        otel_info!(
            "azure_identity_auth.start",
            name = self.inner.name.as_str(),
            credential_type = self.inner.auth.credential_type(),
            scope = self.inner.auth.scope(),
        );

        let mut next_refresh = tokio::time::Instant::now();

        loop {
            tokio::select! {
                biased;

                msg = ctrl.recv() => match msg {
                    Ok(ExtensionControlMsg::Shutdown { reason, .. }) => {
                        otel_info!(
                            "azure_identity_auth.shutdown",
                            name = self.inner.name.as_str(),
                            reason = reason.as_str()
                        );
                        break;
                    }
                    Ok(ExtensionControlMsg::Config { .. }) => {
                        // Currently no-op; refresh cadence is governed by
                        // token lifetime.
                    }
                    Ok(ExtensionControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                        // Best-effort flush — log on error but keep running.
                        if let Err(e) =
                            self.inner.metrics.lock().expect("metrics mutex poisoned")
                                .report(&mut metrics_reporter)
                        {
                            otel_error!(
                                "azure_identity_auth.metrics_report_failed",
                                name = self.inner.name.as_str(),
                                error = %e
                            );
                        }
                    }
                    Err(_) => break,
                },

                _ = tokio::time::sleep_until(next_refresh) => {
                    match fetch_and_publish(&self.inner).await {
                        Ok(token) => {
                            next_refresh = get_next_token_refresh(&token);
                            log_token_acquired(&self.inner.name, next_refresh);
                        }
                        Err(e) => {
                            otel_error!(
                                "azure_identity_auth.token_acquisition_failed",
                                name = self.inner.name.as_str(),
                                error = %e,
                                retry_secs = TOKEN_REFRESH_RETRY_SECS
                            );
                            next_refresh = tokio::time::Instant::now()
                                + tokio::time::Duration::from_secs(TOKEN_REFRESH_RETRY_SECS);
                        }
                    }
                }
            }
        }

        Ok(TerminalState::default())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Fetch a token via the configured credential and publish it to subscribers.
///
/// Uses double-checked locking on `inner.fetch_lock` to coalesce concurrent
/// callers: if another task already refreshed the cache while we were
/// waiting for the mutex, we return that token without hitting the
/// credential again.
async fn fetch_and_publish(inner: &Inner) -> Result<BearerToken, super::error::Error> {
    let _guard = inner.fetch_lock.lock().await;
    if let Some(token) = fresh_cached(inner) {
        return Ok(token);
    }
    let started = Instant::now();
    let access_token = match inner.auth.get_token().await {
        Ok(t) => t,
        Err(e) => {
            inner
                .metrics
                .lock()
                .expect("metrics mutex poisoned")
                .record_failure();
            return Err(e);
        }
    };
    let latency_ms = started.elapsed().as_secs_f64() * 1000.0;
    let token = BearerToken::new(
        access_token.token.secret().to_string(),
        unix_secs_to_instant(access_token.expires_on.unix_timestamp()),
    );
    // `send` errors only when there are no receivers; that's fine — the value
    // is still stored and any later `subscribe()` will see it.
    let _ = inner.tx.send(Some(token.clone()));
    {
        let mut m = inner.metrics.lock().expect("metrics mutex poisoned");
        m.record_success(latency_ms);
        m.record_publish();
    }
    Ok(token)
}

/// Returns the cached token iff it is outside the refresh-skew window.
fn fresh_cached(inner: &Inner) -> Option<BearerToken> {
    let token = inner.tx.borrow().clone()?;
    let buffer = std::time::Duration::from_secs(TOKEN_EXPIRY_BUFFER_SECS);
    match token.expires_on {
        Some(exp) => (exp > Instant::now() + buffer).then_some(token),
        None => Some(token),
    }
}

/// Compute the next refresh `Instant`, clamped to a small minimum so we
/// never busy-loop on a near-expired token.
fn get_next_token_refresh(token: &BearerToken) -> tokio::time::Instant {
    let floor = tokio::time::Instant::now()
        + tokio::time::Duration::from_secs(MIN_TOKEN_REFRESH_INTERVAL_SECS);
    let Some(expires_on) = token.expires_on else {
        // Non-expiring token — push refresh far out; the loop is still woken
        // by control messages and shutdown.
        return tokio::time::Instant::now() + tokio::time::Duration::from_secs(365 * 24 * 60 * 60);
    };
    let token_valid_until = tokio::time::Instant::from_std(expires_on);
    let candidate = token_valid_until
        .checked_sub(tokio::time::Duration::from_secs(TOKEN_EXPIRY_BUFFER_SECS))
        .unwrap_or_else(tokio::time::Instant::now);
    std::cmp::max(candidate, floor)
}

fn log_token_acquired(name: &str, next_refresh: tokio::time::Instant) {
    let refresh_in = next_refresh.saturating_duration_since(tokio::time::Instant::now());
    let total = refresh_in.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    otel_info!(
        "azure_identity_auth.token_acquired",
        name = name,
        refresh_in = format!("{h}h {m}m {s}s").as_str()
    );
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Convert an absolute UNIX-seconds timestamp (as returned by the Azure SDK)
/// into a monotonic [`Instant`] anchored at "now".
///
/// We compute `Instant::now() + (absolute_expiry − now_unix())`, saturating
/// at zero for already-expired inputs. Clock-skew effects only affect this
/// single conversion; thereafter the `Instant` is immune to wall-clock jumps.
fn unix_secs_to_instant(unix_secs: i64) -> Instant {
    let delta_secs = unix_secs.saturating_sub(now_unix()).max(0) as u64;
    Instant::now() + std::time::Duration::from_secs(delta_secs)
}
