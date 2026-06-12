// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry metrics for the `azure-identity-auth` extension.
//!
//! Recorded sites:
//! - `start()`'s background refresh loop: success latency + publish count on
//!   success, failure counter on credential errors.
//! - The slow-path `BearerTokenProvider::get_token()` cache-miss branch:
//!   success latency on success, failure counter on credential errors.
//!
//! Metrics are flushed by the engine via `ExtensionControlMsg::CollectTelemetry`.
//! All record/report operations go through a short-lived `std::sync::Mutex`
//! lock — no `.await` is held across the critical section.

use otap_df_telemetry::instrument::{Counter, Mmsc};
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry_macros::metric_set;

/// Telemetry metric set for the azure-identity-auth extension.
#[metric_set(name = "extension.azure_identity_auth")]
#[derive(Debug, Default, Clone)]
pub struct AzureIdentityAuthMetrics {
    /// Successful credential acquisitions (background refresh + slow-path).
    #[metric(unit = "{token}")]
    pub auth_successes: Counter<u64>,
    /// Failed credential acquisitions (background refresh + slow-path).
    #[metric(unit = "{error}")]
    pub auth_failures: Counter<u64>,
    /// Tokens published to capability consumers via the watch channel.
    /// Increments once per successful refresh that updated the cache.
    #[metric(unit = "{token}")]
    pub auth_token_publish: Counter<u64>,
    /// Latency of successful credential acquisitions, in milliseconds.
    #[metric(unit = "ms")]
    pub auth_success_latency: Mmsc,
}

/// Holds the metric set behind a `MetricSet<T>`; mutation is gated by the
/// caller's `std::sync::Mutex<Self>` so writes from the extension's
/// background loop and concurrent capability slow paths serialize cleanly.
pub struct AzureIdentityAuthMetricsTracker {
    metrics: MetricSet<AzureIdentityAuthMetrics>,
}

impl std::fmt::Debug for AzureIdentityAuthMetricsTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureIdentityAuthMetricsTracker").finish()
    }
}

impl AzureIdentityAuthMetricsTracker {
    /// Wrap a registered metric set.
    #[must_use]
    pub fn new(metrics: MetricSet<AzureIdentityAuthMetrics>) -> Self {
        Self { metrics }
    }

    /// Record a successful credential acquisition.
    #[inline]
    pub fn record_success(&mut self, latency_ms: f64) {
        self.metrics.auth_successes.inc();
        self.metrics.auth_success_latency.record(latency_ms);
    }

    /// Record a failed credential acquisition.
    #[inline]
    pub fn record_failure(&mut self) {
        self.metrics.auth_failures.inc();
    }

    /// Record a token publish to the watch channel.
    #[inline]
    pub fn record_publish(&mut self) {
        self.metrics.auth_token_publish.inc();
    }

    /// Report metrics to the telemetry system.
    pub fn report(
        &mut self,
        reporter: &mut otap_df_telemetry::reporter::MetricsReporter,
    ) -> Result<(), otap_df_telemetry::error::Error> {
        reporter.report(&mut self.metrics)
    }
}
