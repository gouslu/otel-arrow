// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use futures::StreamExt;
use otap_df_channel::error::RecvError;
use otap_df_config::SignalType;
use otap_df_engine::ConsumerEffectHandlerExtension;
use otap_df_engine::capability::bearer_token_provider::local;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{ExporterInbox, Message};
use otap_df_engine::terminal_state::TerminalState;
use otap_df_pdata::otlp::OtlpProtoBytes;
use otap_df_pdata::views::otap::OtapLogsView;
use otap_df_pdata::views::otlp::bytes::logs::RawLogsData;
use otap_df_pdata::{OtapArrowRecords, OtapPayload};

use super::client::LogsIngestionClientPool;
use super::config::Config;
use super::error::Error;
use super::gzip_batcher::FinalizeResult;
use super::gzip_batcher::{self, GzipBatcher};
use super::heartbeat::Heartbeat;
use super::in_flight_exports::{CompletedExport, InFlightExports};
use super::metrics::{AzureMonitorExporterMetrics, AzureMonitorExporterMetricsRc};
use super::state::AzureMonitorExporterState;
use super::transformer::Transformer;
use otap_df_otap::pdata::{Context, OtapPdata};
use reqwest::header::HeaderValue;

use otap_df_telemetry::{otel_debug, otel_error, otel_info, otel_warn};

use bytes::Bytes;
use std::cell::RefCell;
use std::rc::Rc;

/// Max concurrent HTTP requests in flight to the Logs Ingestion API.
const MAX_IN_FLIGHT_EXPORTS: usize = 16;
const PERIODIC_EXPORT_INTERVAL: u64 = 3;

/// Azure Monitor exporter.
pub struct AzureMonitorExporter {
    config: Config,
    transformer: Transformer,
    gzip_batcher: GzipBatcher,
    state: AzureMonitorExporterState,
    metrics: AzureMonitorExporterMetricsRc,
    client_pool: LogsIngestionClientPool,
    in_flight_exports: InFlightExports,
    last_batch_queued_at: tokio::time::Instant,
    heartbeat: Option<Heartbeat>,
    /// Source of bearer tokens. Provided by the `azure-identity-auth`
    /// extension (or any other implementation of `BearerTokenProvider`);
    /// resolved by the factory and injected here so the constructor is
    /// capability-agnostic and unit-testable.
    token_provider: Box<dyn local::BearerTokenProvider>,
}

impl AzureMonitorExporter {
    /// Build a new exporter from configuration.
    pub fn new(
        pipeline_ctx: PipelineContext,
        config: Config,
        token_provider: Box<dyn local::BearerTokenProvider>,
    ) -> Result<Self, Error> {
        // Validate configuration
        config
            .validate()
            .map_err(|e| Error::Config(e.to_string()))?;

        // Register metrics with the telemetry system
        let metric_set = pipeline_ctx.register_metrics::<AzureMonitorExporterMetrics>();
        let metrics: AzureMonitorExporterMetricsRc = Rc::new(RefCell::new(
            super::metrics::AzureMonitorExporterMetricsTracker::new(metric_set),
        ));

        // Create log transformer
        let transformer = Transformer::new(&config);

        // Create Gzip batcher
        let gzip_batcher = GzipBatcher::new(config.api.gzip_compression_level);

        // Create heartbeat handler
        let heartbeat = if config.heartbeat.enabled {
            Some(Heartbeat::new(&config.api, &config.heartbeat.overrides)?)
        } else {
            None
        };

        Ok(Self {
            config,
            transformer,
            gzip_batcher,
            state: AzureMonitorExporterState::new(),
            metrics: metrics.clone(),
            client_pool: LogsIngestionClientPool::new(MAX_IN_FLIGHT_EXPORTS + 1, metrics),
            in_flight_exports: InFlightExports::new(MAX_IN_FLIGHT_EXPORTS),
            last_batch_queued_at: tokio::time::Instant::now(),
            heartbeat,
            token_provider,
        })
    }

    /// Update all gauges (in-flight exports + state map sizes).
    #[inline]
    fn sync_gauges(&self) {
        let mut m = self.metrics.borrow_mut();
        m.set_in_flight_exports(self.in_flight_exports.len() as u64);
        m.set_batch_to_msg_count(self.state.batch_to_msg.len() as u64);
        m.set_msg_to_batch_count(self.state.msg_to_batch.len() as u64);
        m.set_msg_to_data_count(self.state.msg_to_data.len() as u64);
    }

    async fn finalize_export(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
        completed_export: CompletedExport,
    ) -> Result<(), EngineError> {
        let CompletedExport {
            batch_id,
            client,
            result,
            row_count,
        } = completed_export;

        // Return the client to the pool
        self.client_pool.release(client);

        match result {
            Ok(duration) => {
                self.handle_export_success(effect_handler, batch_id, row_count, duration)
                    .await
            }
            Err(e) => {
                self.handle_export_failure(effect_handler, batch_id, row_count, e)
                    .await
            }
        }
    }

    async fn handle_export_success(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
        batch_id: u64,
        row_count: u64,
        duration: std::time::Duration,
    ) -> Result<(), EngineError> {
        // Export succeeded - Ack only fully-completed messages
        let completed_messages = self.state.remove_batch_success(batch_id);
        {
            let mut m = self.metrics.borrow_mut();
            m.add_messages(completed_messages.len() as u64);
            m.add_rows(row_count);
            m.add_batch();
        }

        otel_debug!(
            "azure_monitor_exporter.export.success",
            batch_id = batch_id,
            row_count = row_count,
            duration_ms = duration.as_millis() as u64
        );

        for (_, context, payload) in completed_messages {
            effect_handler
                .notify_ack(AckMsg::new(OtapPdata::new(context, payload)))
                .await?;
        }
        Ok(())
    }

    async fn handle_export_failure(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
        batch_id: u64,
        row_count: u64,
        error: Error,
    ) -> Result<(), EngineError> {
        // Export failed - Nack ALL messages in this batch, remove entirely
        let failed_messages = self.state.remove_batch_failure(batch_id);
        {
            let mut m = self.metrics.borrow_mut();
            m.add_failed_messages(failed_messages.len() as u64);
            m.add_failed_rows(row_count);
            m.add_failed_batch();
        }

        otel_warn!("azure_monitor_exporter.export.failed", batch_id = batch_id, error = %error);

        for (_, context, payload) in failed_messages {
            effect_handler
                .notify_nack(NackMsg::new(
                    error.to_string(),
                    OtapPdata::new(context, payload),
                ))
                .await?;
        }
        Ok(())
    }

    async fn queue_pending_batch(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let pending_batch = match self.gzip_batcher.take_pending_batch() {
            Some(batch) => batch,
            None => return Ok(()), // No pending batch - nothing to do
        };

        self.metrics
            .borrow_mut()
            .add_batch_uncompressed_size(pending_batch.uncompressed_size as f64);
        self.metrics
            .borrow_mut()
            .add_batch_size(pending_batch.compressed_data.len() as f64);

        let client = self.client_pool.take();
        if let Some(completed_export) = self
            .in_flight_exports
            .push_export(
                client,
                pending_batch.batch_id,
                pending_batch.row_count,
                pending_batch.compressed_data,
            )
            .await
        {
            self.finalize_export(effect_handler, completed_export)
                .await?;
        }

        self.last_batch_queued_at = tokio::time::Instant::now();

        Ok(())
    }

    async fn handle_logs(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
        context: Context,
        payload: OtapPayload,
        log_entries: Vec<Bytes>,
        msg_id: u64,
    ) -> Result<(), EngineError> {
        if context.may_return_payload() {
            self.state.add_msg_to_data(msg_id, context, payload);
        } else {
            self.state
                .add_msg_to_data(msg_id, context, OtapPayload::empty(SignalType::Logs));
        }

        for log_entry in log_entries {
            let entry_len = log_entry.len();
            match self.gzip_batcher.push(log_entry) {
                Ok(gzip_batcher::PushResult::Ok(batch_id)) => {
                    // current batch id is being associated with the current message
                    self.state.add_batch_msg_relationship(batch_id, msg_id);
                }
                Ok(gzip_batcher::PushResult::BatchReady(new_batch_id)) => {
                    // new batch id is being associated with the current message
                    self.state.add_batch_msg_relationship(new_batch_id, msg_id);
                    self.queue_pending_batch(effect_handler).await?;
                }
                Ok(gzip_batcher::PushResult::TooLarge) => {
                    let error = Error::LogEntryTooLarge;
                    self.metrics.borrow_mut().add_log_entry_too_large();
                    otel_warn!(
                        "azure_monitor_exporter.message.log_entry_too_large",
                        msg_id = msg_id,
                        size_bytes = entry_len
                    );
                    if let Some((context, payload)) = self.state.remove_msg_to_data(msg_id) {
                        effect_handler
                            .notify_nack(NackMsg::new(
                                error.to_string(),
                                OtapPdata::new(context, payload),
                            ))
                            .await?;
                    }
                    return Err(EngineError::InternalError {
                        message: error.to_string(),
                    });
                }
                Err(error) => {
                    otel_error!("azure_monitor_exporter.message.batch_push_failed", msg_id = msg_id, error = %error);
                    if let Some((context, payload)) = self.state.remove_msg_to_data(msg_id) {
                        effect_handler
                            .notify_nack(NackMsg::new(
                                error.to_string(),
                                OtapPdata::new(context, payload),
                            ))
                            .await?;
                    }
                    return Err(EngineError::InternalError {
                        message: error.to_string(),
                    });
                }
            }
        }

        if let Some((context, payload)) = self.state.delete_msg_data_if_orphaned(msg_id) {
            otel_debug!(
                "azure_monitor_exporter.message.no_valid_entries",
                msg_id = msg_id
            );
            effect_handler
                .notify_nack(NackMsg::new(
                    "No valid log entries produced",
                    OtapPdata::new(context, payload),
                ))
                .await?;
        }

        Ok(())
    }

    async fn drain_in_flight_exports(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let completed_exports = self.in_flight_exports.drain().await;
        for completed_export in completed_exports {
            self.finalize_export(effect_handler, completed_export)
                .await?;
        }
        Ok(())
    }

    async fn queue_current_batch(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        match self.gzip_batcher.finalize() {
            Ok(FinalizeResult::Ok) => {
                return self.queue_pending_batch(effect_handler).await;
            }
            Ok(FinalizeResult::Empty) => Ok(()),
            Err(error) => Err(EngineError::InternalError {
                message: error.to_string(),
            }),
        }
    }

    async fn handle_shutdown(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        self.queue_current_batch(effect_handler).await?;
        self.drain_in_flight_exports(effect_handler).await?;

        for (msg_id, context, payload) in self.state.drain_all() {
            otel_warn!(
                "azure_monitor_exporter.shutdown.orphaned_message",
                msg_id = msg_id
            );
            effect_handler
                .notify_nack(NackMsg::new(
                    "Shutdown before export completed",
                    OtapPdata::new(context, payload),
                ))
                .await?;
        }

        otel_info!("azure_monitor_exporter.exporter.shutdown");

        Ok(())
    }

    async fn handle_message(
        &mut self,
        effect_handler: &EffectHandler<OtapPdata>,
        msg: Result<Message<OtapPdata>, RecvError>,
        msg_id: &mut u64,
    ) -> Result<(), EngineError> {
        match msg {
            Ok(Message::PData(pdata)) => {
                *msg_id += 1;
                let (context, payload) = pdata.into_parts();

                let log_entries = match &payload {
                    OtapPayload::OtapArrowRecords(otap_records) => match otap_records {
                        OtapArrowRecords::Logs(_) => {
                            let logs_view = OtapLogsView::try_from(otap_records).map_err(|e| {
                                let error = Error::LogsViewCreationFailed { source: e };
                                EngineError::InternalError {
                                    message: error.to_string(),
                                }
                            })?;
                            Some(self.transformer.convert_to_log_analytics(&logs_view))
                        }
                        OtapArrowRecords::Metrics(_) | OtapArrowRecords::Traces(_) => {
                            otel_warn!(
                                "azure_monitor_exporter.message.unsupported_signal",
                                signal = "metrics_or_traces",
                                format = "otap_arrow"
                            );
                            None
                        }
                    },
                    OtapPayload::OtlpBytes(otlp_bytes) => match otlp_bytes {
                        OtlpProtoBytes::ExportLogsRequest(bytes) => {
                            let logs_view = RawLogsData::new(bytes.as_ref());
                            Some(self.transformer.convert_to_log_analytics(&logs_view))
                        }
                        OtlpProtoBytes::ExportMetricsRequest(_)
                        | OtlpProtoBytes::ExportTracesRequest(_) => {
                            otel_warn!(
                                "azure_monitor_exporter.message.unsupported_signal",
                                signal = "metrics_or_traces",
                                format = "otlp_proto"
                            );
                            None
                        }
                    },
                };

                if let Some(log_entries) = log_entries {
                    self.handle_logs(effect_handler, context, payload, log_entries, *msg_id)
                        .await?;
                }
            }

            Ok(_) => {} // Ignore other message types

            Err(e) => {
                let error = Error::ChannelRecv(e);
                return Err(EngineError::InternalError {
                    message: error.to_string(),
                });
            }
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for AzureMonitorExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, EngineError> {
        otel_info!(
            "azure_monitor_exporter.start",
            endpoint = self.config.api.dcr_endpoint.as_str(),
            stream = self.config.api.stream_name.as_str(),
            dcr = self.config.api.dcr.as_str(),
            gzip_compression_level = self.config.api.gzip_compression_level
        );

        let mut msg_id = 0;

        self.client_pool
            .initialize(&self.config.api)
            .await
            .map_err(|e| {
                let error = Error::ClientPoolInit(Box::new(e));
                EngineError::InternalError {
                    message: error.to_string(),
                }
            })?;

        let mut next_periodic_export = tokio::time::Instant::now()
            + tokio::time::Duration::from_secs(PERIODIC_EXPORT_INTERVAL);
        let mut next_heartbeat_send = tokio::time::Instant::now();

        // Subscribe to the bearer-token stream. The extension publishes the
        // current (and every subsequent) token here; until the first one
        // arrives we refuse pdata, matching the legacy in-process behavior.
        // We track when the current token expires (as a monotonic `Instant`)
        // instead of a plain boolean so a stalled provider that fails to
        // refresh before expiry automatically flips us back to "not accepting
        // pdata".
        let mut tokens = self.token_provider.token_stream();
        // Initialize to "now" — i.e., already expired. The first published
        // token bumps this forward; until then `has_token` evaluates false
        // and pdata is gated off, matching the legacy in-process behavior.
        let mut token_expiry_at = tokio::time::Instant::now();
        let mut first_token_seen = false;
        // Safety margin: stop accepting pdata this many seconds before the
        // current token's actual expiry, so in-flight requests don't race the
        // expiry boundary. The extension is expected to publish a refreshed
        // token well before this window opens.
        const TOKEN_EXPIRY_BUFFER_SECS: u64 = 60;

        loop {
            let has_token = token_expiry_at
                > tokio::time::Instant::now()
                    + tokio::time::Duration::from_secs(TOKEN_EXPIRY_BUFFER_SECS);
            let at_capacity = self.in_flight_exports.len() >= MAX_IN_FLIGHT_EXPORTS;
            let accepting_pdata = has_token && !at_capacity;

            tokio::select! {
                biased;

                // A fresh token from the auth extension — rebuild the bearer
                // header and propagate it to the client pool and heartbeat.
                Some(item) = tokens.next() => {
                    let token = match item {
                        Ok(t) => t,
                        Err(e) => {
                            otel_error!(
                                "azure_monitor_exporter.auth.token_error",
                                error = %e
                            );
                            continue;
                        }
                    };
                    match HeaderValue::from_str(&format!("Bearer {}", token.token.secret())) {
                        Ok(header) => {
                            self.client_pool.update_auth(header.clone());
                            if let Some(ref mut hb) = self.heartbeat {
                                hb.update_auth(header);
                            }
                            if !first_token_seen {
                                otel_info!("azure_monitor_exporter.auth.first_token_received");
                                first_token_seen = true;
                            }
                            token_expiry_at = match token.expires_on {
                                Some(exp) => tokio::time::Instant::from_std(exp),
                                None => tokio::time::Instant::now()
                                    + tokio::time::Duration::from_secs(365 * 24 * 60 * 60),
                            };
                        }
                        Err(e) => {
                            // Should never happen for a syntactically-valid token,
                            // but log defensively and keep the previous header.
                            otel_error!(
                                "azure_monitor_exporter.auth.header_creation_failed",
                                error = ?e
                            );
                        }
                    }
                }

                _ = tokio::time::sleep_until(next_heartbeat_send), if has_token && self.heartbeat.is_some() => {
                    next_heartbeat_send = tokio::time::Instant::now() + self.config.heartbeat.frequency;
                    self.metrics.borrow_mut().add_heartbeat();
                    if let Some(ref mut hb) = self.heartbeat {
                        match hb.send().await {
                            Ok(_) => otel_debug!("azure_monitor_exporter.heartbeat.sent"),
                            Err(e) => otel_warn!("azure_monitor_exporter.heartbeat.send_failed", error = %e),
                        }
                    }
                }

                completed = self.in_flight_exports.next_completion() => {
                    if let Some(completed_export) = completed {
                        self.finalize_export(&effect_handler, completed_export).await?;
                    }
                }

                _ = tokio::time::sleep_until(next_periodic_export), if accepting_pdata => {
                    next_periodic_export = tokio::time::Instant::now() + tokio::time::Duration::from_secs(PERIODIC_EXPORT_INTERVAL);

                    if self.last_batch_queued_at.elapsed() >= std::time::Duration::from_secs(PERIODIC_EXPORT_INTERVAL) && self.gzip_batcher.has_pending_data() {
                        otel_debug!("azure_monitor_exporter.export.periodic_flush");
                        self.queue_current_batch(&effect_handler).await?;
                    }
                }

                // TODO: Ensure that when rejecting pdata, data loss doesn't occur. (pending on lquerel's msg channel rework)
                // Control always flows; pdata guarded by has_token && !at_capacity
                msg = msg_chan.recv_when(accepting_pdata) => {
                    match msg {
                        Ok(Message::Control(NodeControlMsg::CollectTelemetry { mut metrics_reporter })) => {
                            self.sync_gauges();
                            if tracing::enabled!(tracing::Level::DEBUG) {
                                let m = self.metrics.borrow();
                                let cl = m.client_success_latency();
                                let bs = m.batch_size();
                                otel_debug!(
                                    "azure_monitor_exporter.metrics.collect",
                                    successful_rows = m.successful_row_count(),
                                    successful_batches = m.successful_batch_count(),
                                    successful_messages = m.successful_msg_count(),
                                    failed_rows = m.failed_row_count(),
                                    failed_batches = m.failed_batch_count(),
                                    failed_messages = m.failed_msg_count(),
                                    client_success_latency_avg_ms = if cl.count > 0 { cl.sum / cl.count as f64 } else { 0.0 },
                                    client_success_latency_min_ms = if cl.count > 0 { cl.min } else { 0.0 },
                                    client_success_latency_max_ms = if cl.count > 0 { cl.max } else { 0.0 },
                                    client_success_latency_count = cl.count,
                                    batch_size_avg_bytes = if bs.count > 0 { bs.sum / bs.count as f64 } else { 0.0 },
                                    batch_size_min_bytes = if bs.count > 0 { bs.min } else { 0.0 },
                                    batch_size_max_bytes = if bs.count > 0 { bs.max } else { 0.0 },
                                    batch_size_count = bs.count,
                                    in_flight = self.in_flight_exports.len()
                                );
                            }
                            let _ = self.metrics.borrow_mut().report(&mut metrics_reporter);
                        }
                        Ok(Message::Control(NodeControlMsg::Shutdown { deadline, .. })) => {
                            self.handle_shutdown(&effect_handler).await?;
                            let snapshot = self.metrics.borrow().metrics().snapshot();
                            return Ok(TerminalState::new(
                                deadline,
                                [snapshot],
                            ));
                        }
                        other => {
                            self.handle_message(&effect_handler, other, &mut msg_id).await?;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::{ApiConfig, HeartbeatConfig, SchemaConfig};
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use http::StatusCode;
    use otap_df_channel::mpsc;
    use otap_df_engine::Interests;
    use otap_df_engine::capability::bearer_token_provider::{BearerToken, local};
    use otap_df_engine::context::{ControllerContext, PipelineContext};
    use otap_df_engine::local::exporter::EffectHandler;
    use otap_df_engine::local::message::LocalReceiver;
    use otap_df_engine::message::Receiver;
    use otap_df_engine::node::NodeId;
    use otap_df_otap::pdata::Context;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use otap_df_telemetry::reporter::MetricsReporter;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    /// Minimal stand-in for an extension-backed `BearerTokenProvider`. Returns a
    /// fixed token from both the cache fast path (`get_token`) and the live
    /// stream — sufficient for unit tests that construct the exporter without
    /// driving its `start()` loop.
    struct StubBearerTokenProvider;

    #[async_trait::async_trait(?Send)]
    impl local::BearerTokenProvider for StubBearerTokenProvider {
        async fn get_token(
            &self,
        ) -> Result<BearerToken, otap_df_engine::capability::CapabilityError> {
            Ok(BearerToken::new(
                "stub",
                Instant::now() + Duration::from_secs(3600),
            ))
        }

        fn token_stream(&self) -> otap_df_engine::capability::bearer_token_provider::TokenStream {
            Box::pin(stream::once(async {
                Ok(BearerToken::new(
                    "stub",
                    Instant::now() + Duration::from_secs(3600),
                ))
            }))
        }
    }

    fn stub_token_provider() -> Box<dyn local::BearerTokenProvider> {
        Box::new(StubBearerTokenProvider)
    }

    fn create_test_pipeline_ctx() -> PipelineContext {
        otap_df_otap::crypto::ensure_crypto_provider();
        let registry = TelemetryRegistryHandle::new();
        let controller = ControllerContext::new(registry);
        controller.pipeline_context_with("grp".into(), "pipeline".into(), 0, 1, 0)
    }

    fn create_test_config() -> Config {
        Config {
            api: ApiConfig {
                dcr_endpoint: "https://example.com".to_string(),
                stream_name: "stream".to_string(),
                dcr: "dcr-id".to_string(),
                schema: SchemaConfig {
                    resource_mapping: HashMap::new(),
                    scope_mapping: HashMap::new(),
                    log_record_mapping: HashMap::new(),
                },
                azure_monitor_source_resourceid: None,
                gzip_compression_level: 6,
                user_agent: None,
            },
            heartbeat: HeartbeatConfig::default(),
        }
    }

    fn make_msg_channel(
        capacity: usize,
    ) -> (
        mpsc::Sender<NodeControlMsg<OtapPdata>>,
        mpsc::Sender<OtapPdata>,
        ExporterInbox<OtapPdata>,
    ) {
        let (control_tx, control_rx) = mpsc::Channel::<NodeControlMsg<OtapPdata>>::new(capacity);
        let (pdata_tx, pdata_rx) = mpsc::Channel::<OtapPdata>::new(capacity);
        (
            control_tx,
            pdata_tx,
            ExporterInbox::new(
                Receiver::Local(LocalReceiver::mpsc(control_rx)),
                Receiver::Local(LocalReceiver::mpsc(pdata_rx)),
                0,
                Interests::empty(),
            ),
        )
    }

    #[test]
    fn test_new_validates_config() {
        let config = create_test_config();
        let pipeline_ctx = create_test_pipeline_ctx();
        let _ = AzureMonitorExporter::new(pipeline_ctx, config, stub_token_provider()).unwrap();
    }

    #[tokio::test]
    async fn test_handle_export_success() {
        let config = create_test_config();
        let pipeline_ctx = create_test_pipeline_ctx();
        let mut exporter =
            AzureMonitorExporter::new(pipeline_ctx, config, stub_token_provider()).unwrap();

        let (_, reporter) = MetricsReporter::create_new_and_receiver(10);
        let node_id = NodeId {
            index: 0,
            name: "test_exporter".to_string().into(),
        };
        let effect_handler = EffectHandler::new(node_id, reporter);

        let batch_id = 1;
        let msg_id = 100;
        let context = Context::default();
        let payload =
            OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(Bytes::from("test")));

        exporter
            .state
            .add_msg_to_data(msg_id, context.clone(), payload);
        exporter.state.add_batch_msg_relationship(batch_id, msg_id);

        // This might fail due to missing sender in effect_handler, but state should be updated
        let _ = exporter
            .handle_export_success(&effect_handler, batch_id, 10, Duration::from_secs(1))
            .await;

        // Verify stats
        let m = exporter.metrics.borrow();
        assert_eq!(m.successful_batch_count(), 1);
        assert_eq!(m.successful_msg_count(), 1);
        assert_eq!(m.successful_row_count(), 10);
        drop(m);

        // Verify state cleared
        assert!(exporter.state.batch_to_msg.is_empty());
        assert!(exporter.state.msg_to_data.is_empty());
    }

    #[tokio::test]
    async fn test_handle_export_failure() {
        let config = create_test_config();
        let pipeline_ctx = create_test_pipeline_ctx();
        let mut exporter =
            AzureMonitorExporter::new(pipeline_ctx, config, stub_token_provider()).unwrap();

        let (_, reporter) = MetricsReporter::create_new_and_receiver(10);
        let node_id = NodeId {
            index: 0,
            name: "test_exporter".to_string().into(),
        };
        let effect_handler = EffectHandler::new(node_id, reporter);

        let batch_id = 1;
        let msg_id = 100;
        let context = Context::default();
        let payload =
            OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(Bytes::from("test")));

        exporter
            .state
            .add_msg_to_data(msg_id, context.clone(), payload);
        exporter.state.add_batch_msg_relationship(batch_id, msg_id);

        let error = Error::ServerError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: "Simulated error".to_string(),
            retry_after: None,
        };

        let _ = exporter
            .handle_export_failure(&effect_handler, batch_id, 10, error)
            .await;

        // Verify stats
        let m = exporter.metrics.borrow();
        assert_eq!(m.failed_batch_count(), 1);
        assert_eq!(m.failed_msg_count(), 1);
        assert_eq!(m.failed_row_count(), 10);
        drop(m);

        // Verify state cleared
        assert!(exporter.state.batch_to_msg.is_empty());
        assert!(exporter.state.msg_to_data.is_empty());
    }

    // Azure Monitor can temporarily stop accepting new pdata while it is at
    // capacity. Once shutdown is latched, the exporter channel must still drain
    // already buffered pdata before delivering the final Shutdown message.
    #[tokio::test]
    async fn test_shutdown_drains_buffered_pdata_while_at_capacity() {
        let (control_tx, pdata_tx, mut msg_chan) = make_msg_channel(8);
        let at_capacity = true;

        pdata_tx
            .send_async(OtapPdata::new(
                Context::default(),
                OtapPayload::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(Bytes::new())),
            ))
            .await
            .unwrap();

        control_tx
            .send_async(NodeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_millis(200),
                reason: "test".to_owned(),
            })
            .await
            .unwrap();

        control_tx
            .send_async(NodeControlMsg::TimerTick {})
            .await
            .unwrap();

        let msg = msg_chan.recv_when(!at_capacity).await.unwrap();
        assert!(matches!(
            msg,
            Message::Control(NodeControlMsg::TimerTick {})
        ));

        let msg = msg_chan.recv_when(!at_capacity).await.unwrap();
        assert!(matches!(msg, Message::PData(_)));

        drop(pdata_tx);

        let msg = msg_chan.recv_when(!at_capacity).await.unwrap();
        assert!(matches!(
            msg,
            Message::Control(NodeControlMsg::Shutdown { .. })
        ));
    }
}
