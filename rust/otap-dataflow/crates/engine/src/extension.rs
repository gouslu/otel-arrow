// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension wrapper and infrastructure.
//!
//! Extensions are PData-free — they never process pipeline data, only control
//! messages. This module defines [`ControlChannel`], [`EffectHandler`], and
//! the [`ExtensionWrapper`] struct that the engine uses to start and manage
//! extension instances.
//!
//! For the local (!Send) and shared (Send) Extension traits, see
//! [`local::extension`](crate::local::extension) and
//! [`shared::extension`](crate::shared::extension).
//!
//! For the registry and sealed trait infrastructure, see
//! [`registry`](registry).
//!
//! For built-in extension traits, see
//! [`bearer_token_provider`](bearer_token_provider).

pub mod registry;

/// Extension traits that components can implement to expose capabilities.
pub mod bearer_token_provider;

use crate::channel_metrics::ChannelMetricsRegistry;
use crate::channel_mode::{SharedMode, wrap_control_channel_metrics};
use crate::config::ExtensionConfig;
use crate::context::PipelineContext;
use crate::control::ExtensionControlMsg;
use crate::entity_context::NodeTelemetryGuard;
use crate::error::Error;
use crate::node::NodeId;
use crate::shared::extension as shared_ext;
use crate::shared::message::{SharedReceiver, SharedSender};
use crate::terminal_state::TerminalState;
use otap_df_channel::error::RecvError;
use otap_df_config::node::NodeUserConfig;
use otap_df_telemetry::reporter::MetricsReporter;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{Sleep, sleep_until};

// ── ControlChannel ──────────────────────────────────────────────────────────

/// A channel for receiving control messages for extensions.
///
/// Extensions only receive control messages (shutdown, timer ticks, config updates).
/// They do not process pipeline data (PData).
///
/// When a `Shutdown` message arrives with a future deadline, the channel waits
/// until the deadline expires, then returns the `Shutdown`. No further messages
/// are delivered during this grace period.
pub struct ControlChannel {
    control_rx: Option<SharedReceiver<ExtensionControlMsg>>,
    /// Once a Shutdown is seen, this is set to `Some(instant)` at which point
    /// no more messages will be accepted.
    shutting_down_deadline: Option<Instant>,
    /// Holds the Shutdown message until after we've finished draining.
    pending_shutdown: Option<ExtensionControlMsg>,
}

impl ControlChannel {
    /// Creates a new `ControlChannel` with the given control receiver.
    #[must_use]
    pub const fn new(control_rx: SharedReceiver<ExtensionControlMsg>) -> Self {
        ControlChannel {
            control_rx: Some(control_rx),
            shutting_down_deadline: None,
            pending_shutdown: None,
        }
    }

    /// Asynchronously receives the next control message to process.
    ///
    /// # Errors
    ///
    /// Returns a [`RecvError`] if the channel is closed.
    pub async fn recv(&mut self) -> Result<ExtensionControlMsg, RecvError> {
        let mut sleep_until_deadline: Option<Pin<Box<Sleep>>> = None;

        loop {
            if self.control_rx.is_none() {
                return Err(RecvError::Closed);
            }

            // Draining mode: Shutdown pending
            if let Some(dl) = self.shutting_down_deadline {
                if Instant::now() >= dl {
                    let shutdown = self
                        .pending_shutdown
                        .take()
                        .expect("pending_shutdown must exist");
                    self.shutdown();
                    return Ok(shutdown);
                }

                if sleep_until_deadline.is_none() {
                    sleep_until_deadline = Some(Box::pin(sleep_until(dl.into())));
                }

                tokio::select! {
                    biased;
                    _ = sleep_until_deadline.as_mut().expect("sleep_until_deadline must exist") => {
                        let shutdown = self.pending_shutdown
                            .take()
                            .expect("pending_shutdown must exist");
                        self.shutdown();
                        return Ok(shutdown);
                    }
                }
            }

            // Normal mode: no shutdown yet
            tokio::select! {
                biased;
                ctrl = self.control_rx.as_mut().expect("control_rx must exist").recv() => match ctrl {
                    Ok(ExtensionControlMsg::Shutdown { deadline, reason }) => {
                        if deadline.duration_since(Instant::now()).is_zero() {
                            self.shutdown();
                            return Ok(ExtensionControlMsg::Shutdown { deadline, reason });
                        }
                        self.shutting_down_deadline = Some(deadline);
                        self.pending_shutdown = Some(ExtensionControlMsg::Shutdown { deadline, reason });
                        continue;
                    }
                    Ok(msg) => return Ok(msg),
                    Err(e)  => return Err(e),
                },
            }
        }
    }

    fn shutdown(&mut self) {
        self.shutting_down_deadline = None;
        drop(self.control_rx.take().expect("control_rx must exist"));
    }
}

// ── EffectHandler ───────────────────────────────────────────────────────────

/// The effect handler for extensions.
///
/// Provides extensions with the ability to:
/// - Print info messages
/// - Access node identity
///
/// Extensions manage their own timers directly via `tokio::time` rather than
/// through the engine's timer infrastructure, keeping the extension system
/// fully PData-free.
#[derive(Clone)]
pub struct EffectHandler {
    node_id: NodeId,
    #[allow(dead_code)]
    metrics_reporter: MetricsReporter,
}

impl EffectHandler {
    /// Creates a new `EffectHandler` for the given extension node.
    #[must_use]
    pub const fn new(node_id: NodeId, metrics_reporter: MetricsReporter) -> Self {
        EffectHandler {
            node_id,
            metrics_reporter,
        }
    }

    /// Returns the id of the extension associated with this handler.
    #[must_use]
    pub fn extension_id(&self) -> NodeId {
        self.node_id.clone()
    }

    /// Print an info message to stdout.
    pub async fn info(&self, message: &str) {
        use tokio::io::{AsyncWriteExt, stdout};
        let mut out = stdout();
        let _ = out.write_all(message.as_bytes()).await;
        let _ = out.write_all(b"\n").await;
        let _ = out.flush().await;
    }
}

// ── ExtensionWrapper ────────────────────────────────────────────────────────

/// Wrapper for extension instances in the pipeline engine.
///
/// Extensions are NOT generic over PData — they operate exclusively on
/// [`ExtensionControlMsg`], keeping the extension system entirely decoupled
/// from the data-plane type.
///
/// An extension may provide a shared lifecycle, a local lifecycle, or both.
/// The engine registers capabilities and starts lifecycles for whichever
/// variants are present.
///
/// # Constructors
///
/// ```ignore
/// ExtensionWrapper::shared(ext, node, config, ext_config)
/// ExtensionWrapper::local(Rc::new(ext), node, config, ext_config)
/// ExtensionWrapper::dual(ext.clone(), Rc::new(ext), node, config, ext_config)
/// ```
pub struct ExtensionWrapper {
    /// Index identifier for the node.
    node_id: NodeId,
    /// The user configuration for the node.
    user_config: Arc<NodeUserConfig>,
    /// The runtime configuration for the extension.
    runtime_config: ExtensionConfig,
    /// Shared extension lifecycle (Send, clone-based).
    shared_extension: Option<Box<dyn shared_ext::Extension>>,
    /// Local extension lifecycle (Rc-based, true single instance).
    local_extension: Option<std::rc::Rc<dyn crate::local::extension::Extension>>,
    /// Type-erased shared instance for capability registration.
    shared_any: Option<Box<dyn registry::CloneAnySend>>,
    /// Type-erased local instance for capability registration.
    local_any: Option<std::rc::Rc<dyn std::any::Any>>,
    /// Capabilities descriptor — set by the engine after `create()`.
    capabilities: registry::ExtensionCapabilities,
    /// A sender for control messages.
    control_sender: SharedSender<ExtensionControlMsg>,
    /// A receiver for control messages.
    control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
    /// Telemetry guard for node lifecycle cleanup.
    telemetry: Option<NodeTelemetryGuard>,
}

impl ExtensionWrapper {
    /// Create a **shared** extension (Send, clone-based).
    pub fn shared<E>(
        extension: E,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: shared_ext::Extension + Clone + Send + 'static,
    {
        let shared_any: Box<dyn registry::CloneAnySend> = Box::new(extension.clone());
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        Self {
            node_id,
            user_config,
            runtime_config: config.clone(),
            shared_extension: Some(Box::new(extension)),
            local_extension: None,
            shared_any: Some(shared_any),
            local_any: None,
            capabilities: registry::ExtensionCapabilities {
                names: &[],
                register_shared: |_| Vec::new(),
                register_local: |_| Vec::new(),
            },
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Create a **local** extension (Rc-based, true single instance).
    pub fn local<E>(
        extension: std::rc::Rc<E>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: crate::local::extension::Extension + 'static,
    {
        let local_any: std::rc::Rc<dyn std::any::Any> = extension.clone();
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        Self {
            node_id,
            user_config,
            runtime_config: config.clone(),
            shared_extension: None,
            local_extension: Some(extension),
            shared_any: None,
            local_any: Some(local_any),
            capabilities: registry::ExtensionCapabilities {
                names: &[],
                register_shared: |_| Vec::new(),
                register_local: |_| Vec::new(),
            },
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Create a **dual** extension with both shared and local lifecycles.
    pub fn dual<E>(
        shared: E,
        local: std::rc::Rc<E>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: shared_ext::Extension + crate::local::extension::Extension + Clone + Send + 'static,
    {
        let shared_any: Box<dyn registry::CloneAnySend> = Box::new(shared.clone());
        let local_any: std::rc::Rc<dyn std::any::Any> = local.clone();
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        Self {
            node_id,
            user_config,
            runtime_config: config.clone(),
            shared_extension: Some(Box::new(shared)),
            local_extension: Some(local),
            shared_any: Some(shared_any),
            local_any: Some(local_any),
            capabilities: registry::ExtensionCapabilities {
                names: &[],
                register_shared: |_| Vec::new(),
                register_local: |_| Vec::new(),
            },
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Sets the capabilities descriptor. Called by the engine after `create()`.
    pub fn set_capabilities(&mut self, caps: registry::ExtensionCapabilities) {
        self.capabilities = caps;
    }

    /// Returns the node ID of this extension.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    /// Returns the user configuration for this extension.
    #[must_use]
    pub fn user_config(&self) -> Arc<NodeUserConfig> {
        self.user_config.clone()
    }

    /// Materializes capability registrations and inserts them into the registry.
    ///
    /// Registers shared capabilities if a shared instance is present, and
    /// local capabilities if a local instance is present.
    pub fn register_traits(&self, registry: &mut registry::CapabilityRegistry, name: &str) {
        if let Some(ref shared_any) = self.shared_any {
            let regs = (self.capabilities.register_shared)(shared_any.as_ref().as_any_ref());
            registry.register_all_shared(name, regs);
        }
        if let Some(ref local_any) = self.local_any {
            let regs = (self.capabilities.register_local)(std::rc::Rc::clone(local_any));
            registry.register_all_local(name, regs);
        }
    }

    pub(crate) fn with_node_telemetry_guard(mut self, guard: NodeTelemetryGuard) -> Self {
        self.telemetry = Some(guard);
        self
    }

    pub(crate) fn take_telemetry_guard(&mut self) -> Option<NodeTelemetryGuard> {
        self.telemetry.take()
    }

    pub(crate) fn with_control_channel_metrics(
        mut self,
        pipeline_ctx: &PipelineContext,
        channel_metrics: &mut ChannelMetricsRegistry,
        channel_metrics_enabled: bool,
    ) -> Self {
        let control_receiver = self
            .control_receiver
            .take()
            .expect("control_receiver already taken");
        let (control_sender, control_receiver) =
            wrap_control_channel_metrics::<SharedMode, ExtensionControlMsg>(
                &self.node_id,
                pipeline_ctx,
                channel_metrics,
                channel_metrics_enabled,
                self.runtime_config.control_channel.capacity as u64,
                self.control_sender,
                control_receiver,
            );
        self.control_sender = control_sender;
        self.control_receiver = Some(control_receiver);
        self
    }

    /// Returns an `ExtensionControlSender` for sending control messages.
    pub(crate) fn extension_control_sender(
        &self,
    ) -> crate::control::ExtensionControlSender {
        crate::control::ExtensionControlSender {
            node_id: self.node_id.clone(),
            sender: crate::message::Sender::Shared(self.control_sender.clone()),
        }
    }

    /// Starts the extension lifecycle.
    ///
    /// Prefers the local lifecycle if available, otherwise uses shared.
    pub async fn start(self, metrics_reporter: MetricsReporter) -> Result<TerminalState, Error> {
        let effect_handler = EffectHandler::new(self.node_id, metrics_reporter);
        let control_receiver = self
            .control_receiver
            .expect("control_receiver missing from ExtensionWrapper");
        let ctrl_chan = ControlChannel::new(control_receiver);

        if let Some(local_ext) = self.local_extension {
            local_ext.start(ctrl_chan, effect_handler).await
        } else if let Some(shared_ext) = self.shared_extension {
            shared_ext.start(ctrl_chan, effect_handler).await
        } else {
            panic!("ExtensionWrapper has no extension instance — this is a bug")
        }
    }
}

// ── TelemetryWrapped impl ───────────────────────────────────────────────────

impl crate::TelemetryWrapped for ExtensionWrapper {
    fn with_control_channel_metrics(
        self,
        pipeline_ctx: &PipelineContext,
        channel_metrics: &mut ChannelMetricsRegistry,
        channel_metrics_enabled: bool,
    ) -> Self {
        ExtensionWrapper::with_control_channel_metrics(
            self,
            pipeline_ctx,
            channel_metrics,
            channel_metrics_enabled,
        )
    }

    fn with_node_telemetry_guard(self, guard: NodeTelemetryGuard) -> Self {
        ExtensionWrapper::with_node_telemetry_guard(self, guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::ExtensionControlMsg;
    use crate::shared::extension::Extension;
    use crate::testing::{CtrlMsgCounters, test_node};
    use async_trait::async_trait;
    use otap_df_config::node::NodeUserConfig;
    use serde_json::Value;

    #[derive(Clone)]
    struct TestExtension {
        counter: CtrlMsgCounters,
    }

    impl TestExtension {
        fn new(counter: CtrlMsgCounters) -> Self {
            TestExtension { counter }
        }
    }

    #[async_trait]
    impl Extension for TestExtension {
        async fn start(
            self: Box<Self>,
            mut ctrl_chan: ControlChannel,
            _effect_handler: EffectHandler,
        ) -> Result<TerminalState, Error> {
            loop {
                match ctrl_chan.recv().await? {
                    ExtensionControlMsg::Config { .. } => {
                        self.counter.increment_config();
                    }
                    ExtensionControlMsg::Shutdown { .. } => {
                        self.counter.increment_shutdown();
                        break;
                    }
                    ExtensionControlMsg::CollectTelemetry { .. } => {}
                }
            }
            Ok(TerminalState::default())
        }
    }

    #[test]
    fn test_shared_wrapper_creation() {
        let counter = CtrlMsgCounters::new();
        let extension = TestExtension::new(counter);
        let node_id = test_node("test_extension");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));
        let config = ExtensionConfig::new("test_extension");

        let _wrapper = ExtensionWrapper::shared(
            extension,
            node_id,
            user_config,
            &config,
        );
    }
}
