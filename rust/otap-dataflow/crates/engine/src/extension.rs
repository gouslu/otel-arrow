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
use otap_df_telemetry::otel_debug;
use crate::terminal_state::TerminalState;
use otap_df_channel::error::RecvError;
use otap_df_config::node::NodeUserConfig;
use otap_df_telemetry::reporter::MetricsReporter;
use std::any::TypeId;
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
/// Use the builder to construct:
/// ```ignore
/// ExtensionWrapper::builder(node, config, ext_config)
///     .with_local(Rc::new(local_ext))
///     .with_shared(shared_ext)
///     .build()
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
    /// A sender for control messages (used by local variant, or the sole variant).
    control_sender: SharedSender<ExtensionControlMsg>,
    /// A receiver for control messages (used by local variant, or the sole variant).
    control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
    /// A second sender for control messages (used by the shared variant when
    /// both local and shared are present as independent types).
    shared_control_sender: Option<SharedSender<ExtensionControlMsg>>,
    /// A second receiver for control messages (shared variant, independent mode).
    shared_control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
    /// Telemetry guard for node lifecycle cleanup.
    telemetry: Option<NodeTelemetryGuard>,
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Builder for `ExtensionWrapper`. Shared parameters are set once, then
/// local/shared extension variants are added via `with_local`/`with_shared`.
///
/// At least one variant must be added before calling `build()`.
pub struct ExtensionWrapperBuilder {
    node_id: NodeId,
    user_config: Arc<NodeUserConfig>,
    runtime_config: ExtensionConfig,
    shared_extension: Option<Box<dyn shared_ext::Extension>>,
    local_extension: Option<std::rc::Rc<dyn crate::local::extension::Extension>>,
    shared_any: Option<Box<dyn registry::CloneAnySend>>,
    local_any: Option<std::rc::Rc<dyn std::any::Any>>,
    shared_type_id: Option<TypeId>,
    local_type_id: Option<TypeId>,
}

impl ExtensionWrapperBuilder {
    /// Add a **local** (!Send) extension variant.
    pub fn with_local<E>(mut self, extension: std::rc::Rc<E>) -> Self
    where
        E: crate::local::extension::Extension + 'static,
    {
        otel_debug!(
            "extension.builder.with_local",
            node_id = self.node_id.name.as_ref(),
        );
        let local_any: std::rc::Rc<dyn std::any::Any> = extension.clone();
        self.local_extension = Some(extension);
        self.local_any = Some(local_any);
        self.local_type_id = Some(TypeId::of::<E>());
        self
    }

    /// Add a **shared** (Send) extension variant.
    pub fn with_shared<E>(mut self, extension: E) -> Self
    where
        E: shared_ext::Extension + Clone + Send + 'static,
    {
        otel_debug!(
            "extension.builder.with_shared",
            node_id = self.node_id.name.as_ref(),
        );
        let shared_any: Box<dyn registry::CloneAnySend> = Box::new(extension.clone());
        self.shared_extension = Some(Box::new(extension));
        self.shared_any = Some(shared_any);
        self.shared_type_id = Some(TypeId::of::<E>());
        self
    }

    /// Build the `ExtensionWrapper`.
    ///
    /// # Panics
    ///
    /// - Panics if neither `with_local` nor `with_shared` was called.
    /// - Panics if both `with_local` and `with_shared` were called with the
    ///   same concrete type. Use `with_shared()` alone when a single shared
    ///   type should serve both local and shared consumers.
    pub fn build(self) -> ExtensionWrapper {
        assert!(
            self.shared_extension.is_some() || self.local_extension.is_some(),
            "ExtensionWrapper must have at least one variant (local or shared)"
        );

        let both_present = self.local_extension.is_some() && self.shared_extension.is_some();

        // When both variants are provided, they must be different concrete types.
        if let (Some(local_tid), Some(shared_tid)) = (self.local_type_id, self.shared_type_id) {
            assert!(
                local_tid != shared_tid,
                "with_local() and with_shared() called with the same concrete type — \
                 use with_shared() alone when a single type should serve both \
                 local and shared consumers"
            );
        }

        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(self.runtime_config.control_channel.capacity);

        // Create a second control channel when both variants are present
        // (they are always independent types per the TypeId check above).
        let (shared_control_sender, shared_control_receiver) = if both_present {
            let (tx, rx) =
                tokio::sync::mpsc::channel(self.runtime_config.control_channel.capacity);
            (Some(SharedSender::mpsc(tx)), Some(SharedReceiver::mpsc(rx)))
        } else {
            (None, None)
        };

        otel_debug!(
            "extension.builder.build",
            node_id = self.node_id.name.as_ref(),
            both_variants = both_present,
        );

        ExtensionWrapper {
            node_id: self.node_id,
            user_config: self.user_config,
            runtime_config: self.runtime_config,
            shared_extension: self.shared_extension,
            local_extension: self.local_extension,
            shared_any: self.shared_any,
            local_any: self.local_any,
            capabilities: registry::ExtensionCapabilities {
                names: &[],
                register_shared: |_| Vec::new(),
                register_local: |_| Vec::new(),
            },
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            shared_control_sender,
            shared_control_receiver,
            telemetry: None,
        }
    }
}

impl ExtensionWrapper {
    /// Start building an `ExtensionWrapper` with shared parameters.
    ///
    /// Call `.with_local()`, `.with_shared()`, or both, then `.build()`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Both variants
    /// ExtensionWrapper::builder(node, config, ext_config)
    ///     .with_local(Rc::new(local_ext))
    ///     .with_shared(shared_ext)
    ///     .build()
    ///
    /// // Local only
    /// ExtensionWrapper::builder(node, config, ext_config)
    ///     .with_local(Rc::new(local_ext))
    ///     .build()
    ///
    /// // Shared only
    /// ExtensionWrapper::builder(node, config, ext_config)
    ///     .with_shared(shared_ext)
    ///     .build()
    /// ```
    pub fn builder(
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> ExtensionWrapperBuilder {
        ExtensionWrapperBuilder {
            node_id,
            user_config,
            runtime_config: config.clone(),
            shared_extension: None,
            local_extension: None,
            shared_any: None,
            local_any: None,
            shared_type_id: None,
            local_type_id: None,
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

    /// Returns `true` if this wrapper holds a local extension instance.
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.local_extension.is_some()
    }

    /// Returns `true` if this wrapper holds a shared extension instance.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        self.shared_extension.is_some()
    }

    /// Drop the local variant if present. Called by the engine when no
    /// consumer used local capabilities for this extension.
    pub fn drop_local(&mut self) {
        if self.local_extension.is_some() {
            otel_debug!(
                "extension.drop_local_unused",
                node_id = self.node_id.name.as_ref(),
            );
            self.local_extension = None;
            self.local_any = None;
        }
    }

    /// Drop the shared variant if present. Called by the engine when no
    /// consumer used shared capabilities for this extension.
    pub fn drop_shared(&mut self) {
        if self.shared_extension.is_some() {
            otel_debug!(
                "extension.drop_shared_unused",
                node_id = self.node_id.name.as_ref(),
            );
            self.shared_extension = None;
            self.shared_any = None;
        }
    }

    /// Materializes capability registrations and inserts them into the registry.
    ///
    /// Registers shared capabilities if a shared instance is present, and
    /// local capabilities if a local instance is present.
    pub fn register_traits(&self, registry: &mut registry::CapabilityRegistry, name: &str) {
        if let Some(ref shared_any) = self.shared_any {
            otel_debug!(
                "extension.register_traits.shared",
                node_id = self.node_id.name.as_ref(),
            );
            let regs = (self.capabilities.register_shared)(shared_any.as_ref().as_any_ref());
            registry.register_all_shared(name, regs);
        }
        if let Some(ref local_any) = self.local_any {
            otel_debug!(
                "extension.register_traits.local",
                node_id = self.node_id.name.as_ref(),
            );
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

        // Wrap the second control channel if present (independent lifecycles).
        if let (Some(shared_sender), Some(shared_receiver)) =
            (self.shared_control_sender.take(), self.shared_control_receiver.take())
        {
            let (wrapped_sender, wrapped_receiver) =
                wrap_control_channel_metrics::<SharedMode, ExtensionControlMsg>(
                    &self.node_id,
                    pipeline_ctx,
                    channel_metrics,
                    channel_metrics_enabled,
                    self.runtime_config.control_channel.capacity as u64,
                    shared_sender,
                    shared_receiver,
                );
            self.shared_control_sender = Some(wrapped_sender);
            self.shared_control_receiver = Some(wrapped_receiver);
        }

        self
    }

    /// Returns `ExtensionControlSender`(s) for sending control messages.
    ///
    /// Returns one sender for single-variant or piggyback mode, two senders
    /// for independent-lifecycle mode (one per variant).
    pub(crate) fn extension_control_senders(
        &self,
    ) -> Vec<crate::control::ExtensionControlSender> {
        let mut senders = vec![crate::control::ExtensionControlSender {
            node_id: self.node_id.clone(),
            sender: crate::message::Sender::Shared(self.control_sender.clone()),
        }];
        if let Some(ref shared_sender) = self.shared_control_sender {
            senders.push(crate::control::ExtensionControlSender {
                node_id: self.node_id.clone(),
                sender: crate::message::Sender::Shared(shared_sender.clone()),
            });
        }
        senders
    }

    /// Starts the extension lifecycle(s).
    ///
    /// - If both local and shared variants are present (always independent
    ///   types per the TypeId guard), spawns the shared variant as a background
    ///   task and awaits the local variant on the current thread.
    /// - If only one variant is present, runs it directly.
    pub async fn start(self, metrics_reporter: MetricsReporter) -> Result<TerminalState, Error> {
        let node_name = self.node_id.name.clone();
        let effect_handler = EffectHandler::new(self.node_id, metrics_reporter);
        let control_receiver = self
            .control_receiver
            .expect("control_receiver missing from ExtensionWrapper");
        let ctrl_chan = ControlChannel::new(control_receiver);

        match (self.local_extension, self.shared_extension) {
            (Some(local_ext), Some(shared_ext)) => {
                otel_debug!(
                    "extension.start.both",
                    node_id = node_name.as_ref(),
                );

                // Shared variant gets its own control channel and runs
                // on a spawned Send task.
                let shared_ctrl_rx = self
                    .shared_control_receiver
                    .expect("shared_control_receiver missing — both variants present");
                let shared_ctrl_chan = ControlChannel::new(shared_ctrl_rx);
                let shared_effect = effect_handler.clone();
                let shared_node_name = node_name.clone();
                let shared_handle = tokio::task::spawn(async move {
                    otel_debug!(
                        "extension.start.shared_task",
                        node_id = shared_node_name.as_ref(),
                    );
                    shared_ext.start(shared_ctrl_chan, shared_effect).await
                });

                // Local variant runs on the current LocalSet thread.
                otel_debug!(
                    "extension.start.local_task",
                    node_id = node_name.as_ref(),
                );
                let local_result = local_ext.start(ctrl_chan, effect_handler).await;

                // Wait for the shared variant to finish too.
                let shared_result = shared_handle
                    .await
                    .map_err(|e| Error::InternalError {
                        message: format!("shared extension task panicked: {e}"),
                    })?;

                // Return the first error, or merge terminal states.
                match (local_result, shared_result) {
                    (Err(e), _) | (_, Err(e)) => Err(e),
                    (Ok(local_ts), Ok(shared_ts)) => Ok(local_ts.merge(shared_ts)),
                }
            }
            (Some(local_ext), None) => {
                otel_debug!(
                    "extension.start.local",
                    node_id = node_name.as_ref(),
                );
                local_ext.start(ctrl_chan, effect_handler).await
            }
            (None, Some(shared_ext)) => {
                otel_debug!(
                    "extension.start.shared",
                    node_id = node_name.as_ref(),
                );
                shared_ext.start(ctrl_chan, effect_handler).await
            }
            (None, None) => {
                panic!("ExtensionWrapper has no extension instance — this is a bug")
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

        let _wrapper = ExtensionWrapper::builder(node_id, user_config, &config)
            .with_shared(extension)
            .build();
    }
}
