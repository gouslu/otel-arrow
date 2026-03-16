// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension wrapper and unified Extension trait.
//!
//! Extensions are PData-free — they never process pipeline data, only control
//! messages. This module defines the [`Extension`] trait, [`ControlChannel`],
//! [`EffectHandler`], and the [`ExtensionWrapper`] struct that the engine uses
//! to start and manage extension instances.
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
use async_trait::async_trait;
use otap_df_channel::error::RecvError;
use otap_df_config::node::NodeUserConfig;
use otap_df_telemetry::reporter::MetricsReporter;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{Sleep, sleep_until};

// ── Extension trait ─────────────────────────────────────────────────────────

/// A trait for pipeline extensions.
///
/// Extensions are long-lived components that run alongside the pipeline and
/// expose functionality (e.g., authentication, service discovery) to other
/// components through the [`CapabilityRegistry`](crate::extension::registry::CapabilityRegistry).
///
/// Unlike receivers, processors, and exporters, extensions are NOT generic over
/// PData — they never process pipeline data.
///
/// # Thread Safety
///
/// The `Extension` trait requires the `Send` bound, enabling use in both
/// single-threaded and multi-threaded runtime contexts.
#[async_trait]
pub trait Extension: Send {
    /// Starts the extension.
    ///
    /// The pipeline engine calls this to start the extension in a dedicated task.
    /// Extensions are started BEFORE receivers, processors, and exporters so that
    /// their capabilities are available when data-path components initialize.
    ///
    /// The extension is taken as `Box<Self>` so the method takes ownership once
    /// `start` is called. This lets it move into an independent task, after which
    /// the pipeline can only reach it through the control-message channel.
    ///
    /// # Parameters
    ///
    /// - `ctrl_chan`: A channel to receive control messages. Extensions do not
    ///   receive PData messages — only control messages (shutdown, timer, config).
    /// - `effect_handler`: A handler to perform side effects such as
    ///   info logging.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if an unrecoverable error occurs.
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: ControlChannel,
        _effect_handler: EffectHandler,
    ) -> Result<TerminalState, Error> {
        // Default: no background task. Wait for shutdown and exit.
        loop {
            match ctrl_chan.recv().await? {
                ExtensionControlMsg::Shutdown { .. } => break,
                _ => {}
            }
        }
        Ok(TerminalState::default())
    }
}

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
/// Two variants exist:
///
/// - **Active** — an extension with a background task that processes control
///   messages (shutdown, config updates). Created via [`ExtensionWrapper::active`].
///   Example: an auth extension that periodically refreshes tokens.
///
/// - **Passive** — an extension that only provides capabilities at build time,
///   with no background task. Created via [`ExtensionWrapper::passive`].
///   Example: a static configuration provider.
pub enum ExtensionWrapper {
    /// An extension with a background task that processes control messages.
    Active {
        /// Index identifier for the node.
        node_id: NodeId,
        /// The user configuration for the node.
        user_config: Arc<NodeUserConfig>,
        /// The runtime configuration for the extension.
        runtime_config: ExtensionConfig,
        /// The extension instance.
        extension: Box<dyn Extension>,
        /// Capability registrations to publish.
        capabilities: Vec<registry::CapabilityRegistration>,
        /// A sender for control messages.
        control_sender: SharedSender<ExtensionControlMsg>,
        /// A receiver for control messages.
        control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
        /// Telemetry guard for node lifecycle cleanup.
        telemetry: Option<NodeTelemetryGuard>,
    },
    /// An extension that only provides capabilities without a background task.
    Passive {
        /// Index identifier for the node.
        node_id: NodeId,
        /// The user configuration for the node.
        user_config: Arc<NodeUserConfig>,
        /// Capability registrations to publish.
        capabilities: Vec<registry::CapabilityRegistration>,
        /// Telemetry guard for node lifecycle cleanup.
        telemetry: Option<NodeTelemetryGuard>,
    },
}

impl ExtensionWrapper {
    /// Creates an **Active** extension with a background task and control channel.
    ///
    /// Active extensions implement the [`Extension`] trait and are spawned as
    /// dedicated async tasks. They receive control messages (shutdown, config
    /// updates) via a control channel.
    ///
    /// Capabilities are produced by the factory using the
    /// [`extension_capabilities!`](crate::extension_capabilities) macro and
    /// passed in at construction time.
    pub fn active<E>(
        capabilities: Vec<registry::CapabilityRegistration>,
        extension: E,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: Extension + 'static,
    {
        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        ExtensionWrapper::Active {
            node_id,
            user_config,
            runtime_config: config.clone(),
            extension: Box::new(extension),
            capabilities,
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Creates a **Passive** extension that only publishes capabilities.
    ///
    /// Passive extensions register capability traits at build time but do not
    /// run any async task. Suitable for stateless service providers that expose
    /// pre-built objects (e.g., a configured HTTP client).
    pub fn passive(
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        capabilities: Vec<registry::CapabilityRegistration>,
    ) -> Self {
        ExtensionWrapper::Passive {
            node_id,
            user_config,
            capabilities,
            telemetry: None,
        }
    }

    /// Returns `true` if this is an Active extension that needs to be spawned.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self, ExtensionWrapper::Active { .. })
    }

    /// Returns the node ID of this extension.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        match self {
            ExtensionWrapper::Active { node_id, .. }
            | ExtensionWrapper::Passive { node_id, .. } => node_id.clone(),
        }
    }

    /// Returns the user configuration for this extension.
    #[must_use]
    pub fn user_config(&self) -> Arc<NodeUserConfig> {
        match self {
            ExtensionWrapper::Active { user_config, .. }
            | ExtensionWrapper::Passive { user_config, .. } => user_config.clone(),
        }
    }

    /// Drains the stored capability registrations and inserts them into
    /// the registry under the given name.
    ///
    /// Called by the engine during pipeline build. Both Active and Passive
    /// extensions store their capabilities at construction time (produced
    /// by the factory via [`extension_capabilities!`](crate::extension_capabilities)).
    pub fn register_traits(&mut self, registry: &mut registry::CapabilityRegistry, name: &str) {
        let registrations = match self {
            ExtensionWrapper::Active { capabilities, .. }
            | ExtensionWrapper::Passive { capabilities, .. } => std::mem::take(capabilities),
        };
        registry.register_all(name, registrations);
    }

    pub(crate) fn with_node_telemetry_guard(mut self, guard: NodeTelemetryGuard) -> Self {
        match &mut self {
            ExtensionWrapper::Active { telemetry, .. }
            | ExtensionWrapper::Passive { telemetry, .. } => {
                *telemetry = Some(guard);
            }
        }
        self
    }

    pub(crate) fn take_telemetry_guard(&mut self) -> Option<NodeTelemetryGuard> {
        match self {
            ExtensionWrapper::Active { telemetry, .. }
            | ExtensionWrapper::Passive { telemetry, .. } => telemetry.take(),
        }
    }

    pub(crate) fn with_control_channel_metrics(
        self,
        pipeline_ctx: &PipelineContext,
        channel_metrics: &mut ChannelMetricsRegistry,
        channel_metrics_enabled: bool,
    ) -> Self {
        match self {
            ExtensionWrapper::Active {
                node_id,
                user_config,
                runtime_config,
                extension,
                capabilities,
                control_sender,
                control_receiver,
                telemetry,
            } => {
                let control_receiver =
                    control_receiver.expect("control_receiver already taken");

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

                ExtensionWrapper::Active {
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
            // Passive extensions have no control channel — nothing to wrap.
            passive @ ExtensionWrapper::Passive { .. } => passive,
        }
    }

    /// Returns an `ExtensionControlSender` for sending control messages.
    ///
    /// Returns `Some` for Active extensions and `None` for Passive extensions
    /// (which have no control channel).
    pub(crate) fn extension_control_sender(
        &self,
    ) -> Option<crate::control::ExtensionControlSender> {
        match self {
            ExtensionWrapper::Active {
                node_id,
                control_sender,
                ..
            } => Some(crate::control::ExtensionControlSender {
                node_id: node_id.clone(),
                sender: crate::message::Sender::Shared(control_sender.clone()),
            }),
            ExtensionWrapper::Passive { .. } => None,
        }
    }

    /// Starts the extension and begins its operation.
    ///
    /// Only valid for Active extensions. Passive extensions do not have a
    /// background task — they should not be spawned.
    ///
    /// # Panics
    ///
    /// Panics if called on a Passive extension.
    pub async fn start(
        self,
        metrics_reporter: MetricsReporter,
    ) -> Result<TerminalState, Error> {
        match self {
            ExtensionWrapper::Active {
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
            ExtensionWrapper::Passive { .. } => {
                panic!("start() called on a Passive extension — Passive extensions have no background task")
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
    fn test_extension_wrapper_creation() {
        let counter = CtrlMsgCounters::new();
        let extension = TestExtension::new(counter);
        let node_id = test_node("test_extension");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));
        let config = ExtensionConfig::new("test_extension");

        let _wrapper = ExtensionWrapper::active(Vec::new(), extension, node_id, user_config, &config);
    }
}
