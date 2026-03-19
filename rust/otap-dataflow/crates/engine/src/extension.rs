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
/// An extension is either **local** or **shared**, never both:
///
/// - **Local** — `Rc<Self>` lifecycle, `Rc<dyn Trait>` capabilities.
/// - **Shared** — `Box<Self>` lifecycle, `Box<dyn Trait>` capabilities.
///
/// # Constructors
///
/// ```ignore
/// ExtensionWrapper::local(rc_ext, caps, node, config, ext_config)
/// ExtensionWrapper::shared(ext, caps, node, config, ext_config)
/// ```
pub enum ExtensionWrapper {
    /// A local (!Send) extension.
    ///
    /// Uses `Rc` for true single-instance sharing — the extension task and
    /// all capability consumers share the same allocation.
    Local {
        /// Index identifier for the node.
        node_id: NodeId,
        /// The user configuration for the node.
        user_config: Arc<NodeUserConfig>,
        /// The runtime configuration for the extension.
        runtime_config: ExtensionConfig,
        /// The extension instance (Rc for true single instance).
        extension: std::rc::Rc<dyn crate::local::extension::Extension>,
        /// Local capability registrations to publish (Rc-backed).
        capabilities: Vec<registry::local::CapabilityRegistration>,
        /// A sender for control messages.
        control_sender: SharedSender<ExtensionControlMsg>,
        /// A receiver for control messages.
        control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
        /// Telemetry guard for node lifecycle cleanup.
        telemetry: Option<NodeTelemetryGuard>,
    },
    /// A shared (Send) extension.
    ///
    /// Uses clone-based distribution — consumers get clones that share
    /// `Arc`-wrapped internal state.
    Shared {
        /// Index identifier for the node.
        node_id: NodeId,
        /// The user configuration for the node.
        user_config: Arc<NodeUserConfig>,
        /// The runtime configuration for the extension.
        runtime_config: ExtensionConfig,
        /// The extension instance.
        extension: Box<dyn crate::shared::extension::Extension>,
        /// Shared capability registrations to publish.
        capabilities: Vec<registry::shared::CapabilityRegistration>,
        /// A sender for control messages.
        control_sender: SharedSender<ExtensionControlMsg>,
        /// A receiver for control messages.
        control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
        /// Telemetry guard for node lifecycle cleanup.
        telemetry: Option<NodeTelemetryGuard>,
    },
}

impl ExtensionWrapper {
    /// Create a **local** extension.
    ///
    /// Uses `Rc` for true single-instance sharing — the same allocation serves
    /// both the extension task and all capability consumers.
    ///
    /// The `capabilities` closure receives `&Rc<E>` so it can produce
    /// capability registrations without the caller needing to clone the Rc.
    ///
    /// # Example
    ///
    /// ```ignore
    /// ExtensionWrapper::local(
    ///     Rc::new(extension),
    ///     |rc| local_extension_capabilities!(rc => BearerTokenProvider),
    ///     node, config, ext_config,
    /// )
    /// ```
    pub fn local<E>(
        extension: std::rc::Rc<E>,
        capabilities: impl FnOnce(&std::rc::Rc<E>) -> Vec<registry::local::CapabilityRegistration>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: crate::local::extension::Extension + 'static,
    {
        let caps = capabilities(&extension);
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        ExtensionWrapper::Local {
            node_id,
            user_config,
            runtime_config: config.clone(),
            extension,
            capabilities: caps,
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Create a **shared** extension.
    ///
    /// Uses clone-based distribution — the extension type must be `Clone + Send`.
    /// Consumers get clones that share `Arc`-wrapped internal state.
    ///
    /// The `capabilities` closure receives `&E` so it can produce
    /// capability registrations without the caller needing to bind a variable.
    ///
    /// # Example
    ///
    /// ```ignore
    /// ExtensionWrapper::shared(
    ///     extension,
    ///     |ext| shared_extension_capabilities!(ext => BearerTokenProvider),
    ///     node, config, ext_config,
    /// )
    /// ```
    pub fn shared<E>(
        extension: E,
        capabilities: impl FnOnce(&E) -> Vec<registry::shared::CapabilityRegistration>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: crate::shared::extension::Extension + 'static,
    {
        let caps = capabilities(&extension);
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        ExtensionWrapper::Shared {
            node_id,
            user_config,
            runtime_config: config.clone(),
            extension: Box::new(extension),
            capabilities: caps,
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Returns the node ID of this extension.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        match self {
            ExtensionWrapper::Local { node_id, .. }
            | ExtensionWrapper::Shared { node_id, .. } => node_id.clone(),
        }
    }

    /// Returns the user configuration for this extension.
    #[must_use]
    pub fn user_config(&self) -> Arc<NodeUserConfig> {
        match self {
            ExtensionWrapper::Local { user_config, .. }
            | ExtensionWrapper::Shared { user_config, .. } => user_config.clone(),
        }
    }

    /// Drains the stored capability registrations and inserts them into
    /// the registry under the given name.
    pub fn register_traits(&mut self, registry: &mut registry::CapabilityRegistry, name: &str) {
        match self {
            ExtensionWrapper::Local {
                capabilities, ..
            } => {
                registry.register_all_local(name, std::mem::take(capabilities));
            }
            ExtensionWrapper::Shared {
                capabilities, ..
            } => {
                registry.register_all_shared(name, std::mem::take(capabilities));
            }
        }
    }

    pub(crate) fn with_node_telemetry_guard(mut self, guard: NodeTelemetryGuard) -> Self {
        match &mut self {
            ExtensionWrapper::Local { telemetry, .. }
            | ExtensionWrapper::Shared { telemetry, .. } => {
                *telemetry = Some(guard);
            }
        }
        self
    }

    pub(crate) fn take_telemetry_guard(&mut self) -> Option<NodeTelemetryGuard> {
        match self {
            ExtensionWrapper::Local { telemetry, .. }
            | ExtensionWrapper::Shared { telemetry, .. } => telemetry.take(),
        }
    }

    pub(crate) fn with_control_channel_metrics(
        self,
        pipeline_ctx: &PipelineContext,
        channel_metrics: &mut ChannelMetricsRegistry,
        channel_metrics_enabled: bool,
    ) -> Self {
        match self {
            ExtensionWrapper::Local {
                node_id,
                user_config,
                runtime_config,
                extension,
                capabilities,
                control_sender,
                control_receiver,
                telemetry,
            } => {
                let control_receiver = control_receiver.expect("control_receiver already taken");
                let (control_sender, control_receiver) =
                    wrap_control_channel_metrics::<SharedMode, ExtensionControlMsg>(
                        &node_id,
                        pipeline_ctx,
                        channel_metrics,
                        channel_metrics_enabled,
                        runtime_config.control_channel.capacity as u64,
                        control_sender,
                        control_receiver,
                    );
                ExtensionWrapper::Local {
                    node_id,
                    user_config,
                    runtime_config,
                    extension,
                    capabilities,
                    control_sender,
                    control_receiver: Some(control_receiver),
                    telemetry,
                }
            }
            ExtensionWrapper::Shared {
                node_id,
                user_config,
                runtime_config,
                extension,
                capabilities,
                control_sender,
                control_receiver,
                telemetry,
            } => {
                let control_receiver = control_receiver.expect("control_receiver already taken");
                let (control_sender, control_receiver) =
                    wrap_control_channel_metrics::<SharedMode, ExtensionControlMsg>(
                        &node_id,
                        pipeline_ctx,
                        channel_metrics,
                        channel_metrics_enabled,
                        runtime_config.control_channel.capacity as u64,
                        control_sender,
                        control_receiver,
                    );
                ExtensionWrapper::Shared {
                    node_id,
                    user_config,
                    runtime_config,
                    extension,
                    capabilities,
                    control_sender,
                    control_receiver: Some(control_receiver),
                    telemetry,
                }
            }
        }
    }

    /// Returns an `ExtensionControlSender` for sending control messages.
    pub(crate) fn extension_control_sender(
        &self,
    ) -> crate::control::ExtensionControlSender {
        match self {
            ExtensionWrapper::Local {
                node_id,
                control_sender,
                ..
            }
            | ExtensionWrapper::Shared {
                node_id,
                control_sender,
                ..
            } => crate::control::ExtensionControlSender {
                node_id: node_id.clone(),
                sender: crate::message::Sender::Shared(control_sender.clone()),
            },
        }
    }

    /// Starts the extension and begins its operation.
    pub async fn start(self, metrics_reporter: MetricsReporter) -> Result<TerminalState, Error> {
        match self {
            ExtensionWrapper::Local {
                node_id,
                extension,
                control_receiver,
                ..
            } => {
                let effect_handler = EffectHandler::new(node_id, metrics_reporter);
                let control_receiver =
                    control_receiver.expect("control_receiver missing from ExtensionWrapper");
                let ctrl_chan = ControlChannel::new(control_receiver);
                extension.start(ctrl_chan, effect_handler).await
            }
            ExtensionWrapper::Shared {
                node_id,
                extension,
                control_receiver,
                ..
            } => {
                let effect_handler = EffectHandler::new(node_id, metrics_reporter);
                let control_receiver =
                    control_receiver.expect("control_receiver missing from ExtensionWrapper");
                let ctrl_chan = ControlChannel::new(control_receiver);
                extension.start(ctrl_chan, effect_handler).await
            }
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
            |_| Vec::new(),
            node_id,
            user_config,
            &config,
        );
    }
}
