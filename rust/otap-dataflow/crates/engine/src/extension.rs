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

// ── Builder types ───────────────────────────────────────────────────────────

/// Builder for active extensions (with a background task).
///
/// Obtained via [`ExtensionWrapper::active()`]. Select registration mode with
/// `.cloned()`, then call `.shared(...)`.
pub struct ActiveBuilder;

/// Active extension builder after selecting cloned registration mode.
pub struct ActiveClonedBuilder;

/// Builder for passive extensions (no background task, capabilities only).
///
/// Obtained via [`ExtensionWrapper::passive()`]. Select registration mode with
/// `.cloned()` or `.instance()`, then call `.shared(...)`.
pub struct PassiveBuilder;

/// Passive extension builder after selecting registration source mode.
pub struct PassiveModeBuilder {
    source: registry::RegistrationSource,
}

impl ActiveBuilder {
    /// Select cloned registration mode for active extensions.
    #[must_use]
    pub const fn cloned(self) -> ActiveClonedBuilder {
        ActiveClonedBuilder
    }

    /// Backward-compatible alias. Prefer `active().cloned().shared(...)`.
    pub fn shared_cloned<E>(
        self,
        capabilities: Vec<registry::shared::CapabilityRegistration>,
        extension: E,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> ExtensionWrapper
    where
        E: shared_ext::Extension + 'static,
    {
        self.cloned()
            .shared(capabilities, extension, node_id, user_config, config)
    }
}

impl ActiveClonedBuilder {
    /// Creates an active extension with shared (Send) cloned registrations.
    pub fn shared<E>(
        self,
        mut capabilities: Vec<registry::shared::CapabilityRegistration>,
        extension: E,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> ExtensionWrapper
    where
        E: shared_ext::Extension + 'static,
    {
        for reg in &mut capabilities {
            reg.source = registry::RegistrationSource::Cloned;
        }

        let (control_sender, control_receiver) =
            tokio::sync::mpsc::channel(config.control_channel.capacity);

        ExtensionWrapper::ActiveShared {
            node_id,
            user_config,
            runtime_config: config.clone(),
            extension: Box::new(extension),
            registration_source: registry::RegistrationSource::Cloned,
            shared_capabilities: capabilities,
            local_capabilities: Vec::new(),
            control_sender: SharedSender::mpsc(control_sender),
            control_receiver: Some(SharedReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }
}

impl PassiveBuilder {
    /// Select cloned registration mode for passive extensions.
    #[must_use]
    pub const fn cloned(self) -> PassiveModeBuilder {
        PassiveModeBuilder {
            source: registry::RegistrationSource::Cloned,
        }
    }

    /// Select instance registration mode for passive extensions.
    #[must_use]
    pub const fn instance(self) -> PassiveModeBuilder {
        PassiveModeBuilder {
            source: registry::RegistrationSource::Instance,
        }
    }

    /// Backward-compatible alias. Prefer `passive().cloned().shared(...)`.
    pub fn shared_cloned(
        self,
        capabilities: Vec<registry::shared::CapabilityRegistration>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
    ) -> ExtensionWrapper {
        self.cloned().shared(capabilities, node_id, user_config)
    }

    /// Backward-compatible alias. Prefer `passive().instance().shared(...)`.
    pub fn shared_instance(
        self,
        capabilities: Vec<registry::shared::CapabilityRegistration>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
    ) -> ExtensionWrapper {
        self.instance().shared(capabilities, node_id, user_config)
    }
}

impl PassiveModeBuilder {
    /// Creates a passive extension with shared registrations for the selected source mode.
    pub fn shared(
        self,
        mut capabilities: Vec<registry::shared::CapabilityRegistration>,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
    ) -> ExtensionWrapper {
        for reg in &mut capabilities {
            reg.source = self.source;
        }

        ExtensionWrapper::PassiveShared {
            node_id,
            user_config,
            registration_source: self.source,
            shared_capabilities: capabilities,
            local_capabilities: Vec::new(),
            telemetry: None,
        }
    }
}

// ── ExtensionWrapper ────────────────────────────────────────────────────────

/// Wrapper for extension instances in the pipeline engine.
///
/// Extensions are NOT generic over PData — they operate exclusively on
/// [`ExtensionControlMsg`], keeping the extension system entirely decoupled
/// from the data-plane type.
///
/// Two variants exist, combining lifecycle (active/passive) with shared-first
/// capability registration:
///
/// - **ActiveShared** — a Send extension with a background task.
/// - **PassiveShared** — no background task, Send capabilities only.
///
/// Use the builder API to construct:
/// ```ignore
/// ExtensionWrapper::active().cloned().shared(caps, ext, node_id, user_config, &config)
/// ExtensionWrapper::active().cloned().shared(shared_caps, ext, node_id, user_config, &config).local(local_caps)
/// ExtensionWrapper::passive().cloned().shared(shared_caps, node_id, user_config).local(local_caps)
/// ExtensionWrapper::passive().instance().shared(shared_caps, node_id, user_config).local(local_caps)
/// ```
pub enum ExtensionWrapper {
    /// A shared (Send) extension with a background task.
    ActiveShared {
        /// Index identifier for the node.
        node_id: NodeId,
        /// The user configuration for the node.
        user_config: Arc<NodeUserConfig>,
        /// The runtime configuration for the extension.
        runtime_config: ExtensionConfig,
        /// The extension instance (Send).
        extension: Box<dyn shared_ext::Extension>,
        /// Registration source mode for shared/local capabilities on this wrapper.
        registration_source: registry::RegistrationSource,
        /// Shared capability registrations to publish.
        shared_capabilities: Vec<registry::shared::CapabilityRegistration>,
        /// Optional local capability registrations to publish.
        local_capabilities: Vec<registry::local::CapabilityRegistration>,
        /// A sender for control messages.
        control_sender: SharedSender<ExtensionControlMsg>,
        /// A receiver for control messages.
        control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
        /// Telemetry guard for node lifecycle cleanup.
        telemetry: Option<NodeTelemetryGuard>,
    },
    /// A shared (Send) extension that only provides capabilities, no background task.
    PassiveShared {
        /// Index identifier for the node.
        node_id: NodeId,
        /// The user configuration for the node.
        user_config: Arc<NodeUserConfig>,
        /// Registration source mode for shared/local capabilities on this wrapper.
        registration_source: registry::RegistrationSource,
        /// Shared capability registrations to publish.
        shared_capabilities: Vec<registry::shared::CapabilityRegistration>,
        /// Optional local capability registrations to publish.
        local_capabilities: Vec<registry::local::CapabilityRegistration>,
        /// Telemetry guard for node lifecycle cleanup.
        telemetry: Option<NodeTelemetryGuard>,
    },
}

impl ExtensionWrapper {
    /// Start building an **active** extension (with a background task).
    ///
    /// Chain with `.cloned().shared()` to finalize:
    /// ```ignore
    /// ExtensionWrapper::active().cloned().shared(caps, ext, node_id, user_config, &config)
    /// ```
    #[must_use]
    pub fn active() -> ActiveBuilder {
        ActiveBuilder
    }

    /// Start building a **passive** extension (capabilities only, no background task).
    ///
    /// Chain with `.cloned().shared()` or `.instance().shared()` to finalize:
    /// ```ignore
    /// ExtensionWrapper::passive().cloned().shared(caps, node_id, user_config)
    /// ExtensionWrapper::passive().instance().shared(caps, node_id, user_config)
    /// ```
    #[must_use]
    pub fn passive() -> PassiveBuilder {
        PassiveBuilder
    }

    /// Augments a shared extension wrapper with optional local capability
    /// registrations.
    ///
    /// Add local capability registrations in shared-first construction flow.
    ///
    /// Local registration mode must match the wrapper's selected source mode.
    #[must_use]
    pub fn local(
        mut self,
        mut local_capabilities: Vec<registry::local::CapabilityRegistration>,
    ) -> Self {
        match &mut self {
            ExtensionWrapper::ActiveShared {
                registration_source,
                local_capabilities: local_regs,
                ..
            }
            | ExtensionWrapper::PassiveShared {
                registration_source,
                local_capabilities: local_regs,
                ..
            } => {
                for reg in &mut local_capabilities {
                    reg.source = *registration_source;
                }
                local_regs.extend(local_capabilities);
                self
            }
        }
    }

    /// Backward-compatible alias. Prefer `local(...)`.
    #[must_use]
    pub fn local_cloned(self, local_capabilities: Vec<registry::local::CapabilityRegistration>) -> Self {
        self.local(local_capabilities)
    }

    /// Backward-compatible alias for cloned local registration.
    ///
    /// Prefer [`ExtensionWrapper::local`].
    #[must_use]
    pub fn local_instance(self, local_capabilities: Vec<registry::local::CapabilityRegistration>) -> Self {
        self.local(local_capabilities)
    }

    /// Returns `true` if this is an Active extension that needs to be spawned.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self, ExtensionWrapper::ActiveShared { .. })
    }

    /// Returns the node ID of this extension.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        match self {
            ExtensionWrapper::ActiveShared { node_id, .. }
            | ExtensionWrapper::PassiveShared { node_id, .. } => node_id.clone(),
        }
    }

    /// Returns the user configuration for this extension.
    #[must_use]
    pub fn user_config(&self) -> Arc<NodeUserConfig> {
        match self {
            ExtensionWrapper::ActiveShared { user_config, .. }
            | ExtensionWrapper::PassiveShared { user_config, .. } => user_config.clone(),
        }
    }

    /// Drains the stored capability registrations and inserts them into
    /// the registry under the given name.
    pub fn register_traits(&mut self, registry: &mut registry::CapabilityRegistry, name: &str) {
        let (shared_regs, local_regs) = match self {
            ExtensionWrapper::ActiveShared {
                shared_capabilities,
                local_capabilities,
                ..
            }
            | ExtensionWrapper::PassiveShared {
                shared_capabilities,
                local_capabilities,
                ..
            } => (
                std::mem::take(shared_capabilities),
                std::mem::take(local_capabilities),
            ),
        };
        registry.register_all_shared(name, shared_regs);
        registry.register_all_local(name, local_regs);
    }

    pub(crate) fn with_node_telemetry_guard(mut self, guard: NodeTelemetryGuard) -> Self {
        match &mut self {
            ExtensionWrapper::ActiveShared { telemetry, .. }
            | ExtensionWrapper::PassiveShared { telemetry, .. } => {
                *telemetry = Some(guard);
            }
        }
        self
    }

    pub(crate) fn take_telemetry_guard(&mut self) -> Option<NodeTelemetryGuard> {
        match self {
            ExtensionWrapper::ActiveShared { telemetry, .. }
            | ExtensionWrapper::PassiveShared { telemetry, .. } => telemetry.take(),
        }
    }

    pub(crate) fn with_control_channel_metrics(
        self,
        pipeline_ctx: &PipelineContext,
        channel_metrics: &mut ChannelMetricsRegistry,
        channel_metrics_enabled: bool,
    ) -> Self {
        match self {
            ExtensionWrapper::ActiveShared {
                node_id,
                user_config,
                runtime_config,
                extension,
                registration_source,
                shared_capabilities,
                local_capabilities,
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
                ExtensionWrapper::ActiveShared {
                    node_id,
                    user_config,
                    runtime_config,
                    extension,
                    registration_source,
                    shared_capabilities,
                    local_capabilities,
                    control_sender,
                    control_receiver: Some(control_receiver),
                    telemetry,
                }
            }
            // Passive extensions have no control channel — nothing to wrap.
            passive @ ExtensionWrapper::PassiveShared { .. } => passive,
        }
    }

    /// Returns an `ExtensionControlSender` for sending control messages.
    ///
    /// Returns `Some` for Active extensions and `None` for Passive extensions.
    pub(crate) fn extension_control_sender(
        &self,
    ) -> Option<crate::control::ExtensionControlSender> {
        match self {
            ExtensionWrapper::ActiveShared {
                node_id,
                control_sender,
                ..
            } => Some(crate::control::ExtensionControlSender {
                node_id: node_id.clone(),
                sender: crate::message::Sender::Shared(control_sender.clone()),
            }),
            ExtensionWrapper::PassiveShared { .. } => None,
        }
    }

    /// Starts the extension and begins its operation.
    ///
    /// Only valid for Active extensions. Passive extensions do not have a
    /// background task.
    ///
    /// # Panics
    ///
    /// Panics if called on a Passive extension.
    pub async fn start(self, metrics_reporter: MetricsReporter) -> Result<TerminalState, Error> {
        match self {
            ExtensionWrapper::ActiveShared {
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
            ExtensionWrapper::PassiveShared { .. } => {
                panic!(
                    "start() called on a Passive extension — Passive extensions have no background task"
                )
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
    use crate::extension::registry::RegistrationSource;
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
    fn test_extension_wrapper_creation() {
        let counter = CtrlMsgCounters::new();
        let extension = TestExtension::new(counter);
        let node_id = test_node("test_extension");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));
        let config = ExtensionConfig::new("test_extension");

        let _wrapper = ExtensionWrapper::active().shared_cloned(
            Vec::new(),
            extension,
            node_id,
            user_config,
            &config,
        );
    }

    #[test]
    fn test_extension_wrapper_shared_then_local() {
        let counter = CtrlMsgCounters::new();
        let extension = TestExtension::new(counter);
        let node_id = test_node("test_extension_dual");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));
        let config = ExtensionConfig::new("test_extension_dual");

        let wrapper = ExtensionWrapper::active()
            .cloned()
            .shared(Vec::new(), extension, node_id, user_config, &config)
            .local(Vec::new());

        assert!(wrapper.is_active());
    }

    #[test]
    fn test_passive_wrapper_shared_then_local() {
        let node_id = test_node("test_passive_extension_dual");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));

        let wrapper = ExtensionWrapper::passive()
            .cloned()
            .shared(Vec::new(), node_id, user_config)
            .local(Vec::new());

        assert!(!wrapper.is_active());
    }

    fn shared_instance_registration_for_tests() -> registry::shared::CapabilityRegistration {
        registry::shared::CapabilityRegistration::new_with_source(
            std::any::TypeId::of::<Box<dyn std::fmt::Debug + Send>>(),
            1usize,
            |any| {
                let value = any.downcast_ref::<usize>().unwrap();
                Box::new(Box::new(*value) as Box<dyn std::fmt::Debug + Send>)
            },
            "test_capability",
            RegistrationSource::Instance,
        )
    }

    fn local_instance_registration_for_tests() -> registry::local::CapabilityRegistration {
        registry::local::CapabilityRegistration::new_with_source(
            std::any::TypeId::of::<Box<dyn std::fmt::Debug>>(),
            1usize,
            |any| {
                let value = any.downcast_ref::<usize>().unwrap();
                Box::new(Box::new(*value) as Box<dyn std::fmt::Debug>)
            },
            "test_capability",
            RegistrationSource::Instance,
        )
    }

    #[test]
    fn test_active_normalizes_shared_registration_mode_to_cloned() {
        let counter = CtrlMsgCounters::new();
        let extension = TestExtension::new(counter);
        let node_id = test_node("test_active_instance_shared_rejected");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));
        let config = ExtensionConfig::new("test_active_instance_shared_rejected");

        let wrapper = ExtensionWrapper::active().cloned().shared(
            vec![shared_instance_registration_for_tests()],
            extension,
            node_id,
            user_config,
            &config,
        );

        match wrapper {
            ExtensionWrapper::ActiveShared {
                shared_capabilities,
                ..
            } => {
                assert!(shared_capabilities
                    .iter()
                    .all(|reg| reg.source == RegistrationSource::Cloned));
            }
            ExtensionWrapper::PassiveShared { .. } => unreachable!(),
        }
    }

    #[test]
    fn test_active_normalizes_local_registration_mode_to_cloned() {
        let counter = CtrlMsgCounters::new();
        let extension = TestExtension::new(counter);
        let node_id = test_node("test_active_instance_local_rejected");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));
        let config = ExtensionConfig::new("test_active_instance_local_rejected");

        let wrapper = ExtensionWrapper::active()
            .cloned()
            .shared(Vec::new(), extension, node_id, user_config, &config)
            .local(vec![local_instance_registration_for_tests()]);

        match wrapper {
            ExtensionWrapper::ActiveShared {
                local_capabilities,
                ..
            } => {
                assert!(local_capabilities
                    .iter()
                    .all(|reg| reg.source == RegistrationSource::Cloned));
            }
            ExtensionWrapper::PassiveShared { .. } => unreachable!(),
        }
    }

    #[test]
    fn test_passive_accepts_instance_registration_modes() {
        let node_id = test_node("test_passive_instance_allowed");
        let user_config = Arc::new(NodeUserConfig::with_user_config(
            "urn:otap:extension:test".into(),
            Value::Null,
        ));

        let wrapper = ExtensionWrapper::passive()
            .instance()
            .shared(
                vec![shared_instance_registration_for_tests()],
                node_id,
                user_config,
            )
            .local(vec![local_instance_registration_for_tests()]);

        assert!(!wrapper.is_active());
    }
}
