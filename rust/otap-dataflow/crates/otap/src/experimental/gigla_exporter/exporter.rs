use async_trait::async_trait;
use otel_arrow_rust::proto::opentelemetry::collector::logs::v1::ExportLogsServiceRequest;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{Message, MessageChannel};
use otap_df_engine::terminal_state::TerminalState;
use prost::Message as _;

use crate::pdata::{OtapPdata, OtapPayload, OtlpProtoBytes};

use super::config::Config;

/// GigLA exporter sending telemetry to the GigLA backend.
///
/// This exporter processes OTLP logs and sends them to Azure GigLA
/// (Geneva Infrastructure General-purpose Logging Analytics).
pub struct GigLaExporter {
    config: Config,
}

impl GigLaExporter {
    /// Build a new exporter from configuration.
    pub fn new(config: Config) -> Result<Self, otap_df_config::error::Error> {
        // Validate configuration
        config
            .validate()
            .map_err(|e| otap_df_config::error::Error::InvalidUserConfig { error: e })?;
        
        Ok(Self { config })
    }

    /// Handle a single pdata message.
    async fn handle_pdata(
        &self,
        pdata: OtapPdata,
        effect_handler: &EffectHandler<OtapPdata>,
    ) -> Result<(), String> {
        // Early return if export is disabled
        if self.config.disable_gig_export {
            effect_handler
                .info("[GigLaExporter] Export disabled by configuration")
                .await;
            return Ok(());
        }

        let (_ctx, payload) = pdata.into_parts();

        match payload {
            OtapPayload::OtlpBytes(bytes) => match bytes {
                OtlpProtoBytes::ExportLogsRequest(raw) => {
                    let request = ExportLogsServiceRequest::decode(raw.as_slice())
                        .map_err(|e| format!("Failed to decode logs request: {e}"))?;
                    
                    // TODO: Transform and send to GigLA endpoint
                    // For now, just log the payload for debugging
                    effect_handler
                        .info(&format!(
                            "[GigLaExporter] Processing logs for stream '{}': {} resource logs",
                            self.config.api.stream_name,
                            request.resource_logs.len()
                        ))
                        .await;
                }
                OtlpProtoBytes::ExportMetricsRequest(_) => {
                    effect_handler
                        .info(
                            "[GigLaExporter] Metrics not supported; dropping payload",
                        )
                        .await;
                }
                OtlpProtoBytes::ExportTracesRequest(_) => {
                    effect_handler
                        .info(
                            "[GigLaExporter] Traces not supported; dropping payload",
                        )
                        .await;
                }
            },
            OtapPayload::OtapArrowRecords(_) => {
                effect_handler
                    .info(
                        "[GigLaExporter] Arrow format not supported; dropping payload",
                    )
                    .await;
            }
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for GigLaExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        effect_handler
            .info(&format!(
                "[GigLaExporter] Starting: endpoint={}, stream={}, dcr={}",
                self.config.api.dcr_endpoint,
                self.config.api.stream_name,
                self.config.api.dcr
            ))
            .await;

        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    effect_handler.info("[GigLaExporter] Shutting down").await;
                    return Ok(TerminalState::new(
                        deadline,
                        std::iter::empty::<otap_df_telemetry::metrics::MetricSetSnapshot>(),
                    ));
                }
                Message::Control(NodeControlMsg::CollectTelemetry { metrics_reporter }) => {
                    // TODO: Add metrics support
                    let _ = metrics_reporter;
                }
                Message::PData(pdata) => {
                    if let Err(e) = self.handle_pdata(pdata, &effect_handler).await {
                        effect_handler
                            .info(&format!("[GigLaExporter] Error processing data: {}", e))
                            .await;
                    }
                }
                _ => {
                    // Ignore other message types
                }
            }
        }
    }
}