// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Minimal shared exporter that demonstrates the use of the key-value store capability.
//!
//! This exporter ACKs all incoming pdata and writes a counter to the KV store
//! on each message, proving that a shared (Send) exporter can consume a shared
//! extension capability.

use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::ExporterFactory;
use otap_df_engine::capability::key_value_store::KeyValueStore;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NodeControlMsg};
use otap_df_engine::error::Error;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::shared::capability::KeyValueStore as SharedKeyValueStoreTrait;
use otap_df_engine::shared::exporter::{EffectHandler, Exporter, MessageChannel};
use otap_df_engine::terminal_state::TerminalState;
use otap_df_otap::OTAP_EXPORTER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use otap_df_telemetry::{otel_debug, otel_info, otel_warn};
use std::sync::Arc;

/// The URN for the shared KV exporter.
pub const SHARED_KV_EXPORTER_URN: &str = "urn:otel:exporter:shared_kv";

/// A minimal shared exporter that uses the key-value store capability.
pub struct SharedKvExporter {
    kv: Box<dyn SharedKeyValueStoreTrait>,
}

/// Register the shared KV exporter with the OTAP exporter factory.
#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static SHARED_KV_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SHARED_KV_EXPORTER_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig,
             capabilities: &otap_df_engine::capability::registry::Capabilities| {
        let kv = capabilities.require_shared::<KeyValueStore>()?;

        Ok(ExporterWrapper::shared(
            SharedKvExporter { kv },
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otap_df_config::validation::no_config,
};

#[async_trait]
impl Exporter<OtapPdata> for SharedKvExporter {
    async fn start(
        mut self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        otel_info!("shared_kv_exporter.start");

        let mut count: u64 = 0;

        // Read any previously persisted count from the KV store.
        match self.kv.get("exported_count").await {
            Ok(Some(bytes)) if bytes.len() == 8 => {
                count = u64::from_le_bytes(bytes.try_into().expect("length checked above"));
                otel_info!("shared_kv_exporter.restored_count", restored_count = count);
            }
            Ok(Some(_)) => {
                otel_warn!("shared_kv_exporter.invalid_stored_count");
            }
            Ok(None) => {
                otel_debug!("shared_kv_exporter.no_stored_count");
            }
            Err(e) => {
                otel_warn!("shared_kv_exporter.kv_get_failed", error = %e);
            }
        }

        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    // Persist final count to KV store before shutting down.
                    match self
                        .kv
                        .set("exported_count", count.to_le_bytes().to_vec())
                        .await
                    {
                        Ok(()) => otel_info!(
                            "shared_kv_exporter.shutdown.persisted",
                            exported_count = count
                        ),
                        Err(e) => otel_warn!(
                            "shared_kv_exporter.shutdown.persist_failed",
                            exported_count = count,
                            error = %e
                        ),
                    }
                    break;
                }
                Message::PData(data) => {
                    count += 1;
                    otel_debug!("shared_kv_exporter.received", total_count = count);

                    // Persist count every 100 messages to exercise the KV store regularly.
                    if count.is_multiple_of(100) {
                        match self
                            .kv
                            .set("exported_count", count.to_le_bytes().to_vec())
                            .await
                        {
                            Ok(()) => otel_debug!(
                                "shared_kv_exporter.kv_persisted",
                                exported_count = count
                            ),
                            Err(e) => otel_warn!(
                                "shared_kv_exporter.kv_persist_failed",
                                exported_count = count,
                                error = %e
                            ),
                        }
                    }

                    effect_handler.route_ack(AckMsg::new(data)).await?;
                }
                _ => {}
            }
        }

        Ok(TerminalState::default())
    }
}
