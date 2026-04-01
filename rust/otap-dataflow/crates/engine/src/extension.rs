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
//! [`bearer_token_provider`](crate::capability::bearer_token_provider).

use crate::channel_metrics::ChannelMetricsRegistry;
use crate::channel_mode::{SharedMode, wrap_control_channel_metrics};
use crate::config::ExtensionConfig;
use crate::context::PipelineContext;
use crate::control::ExtensionControlMsg;
use crate::entity_context::NodeTelemetryGuard;
use crate::error::Error;
use crate::local::extension as local_ext;
use crate::node::NodeId;
use crate::shared::extension as shared_ext;
use crate::shared::message::{SharedReceiver, SharedSender};
use crate::terminal_state::TerminalState;
use otap_df_channel::error::RecvError;
use otap_df_config::node::NodeUserConfig;
use otap_df_telemetry::otel_debug;
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

// ── Active / Passive wrappers ────────────────────────────────────────────────

/// Wraps an extension type to signal it has an active event loop.
///
/// The engine spawns a task and creates a control channel for active extensions.
/// The inner type must implement the appropriate `Extension` trait.
///
/// # Usage
/// ```ignore
/// builder.with_shared(Active(ext)).build()
/// builder.with_local(Active(Rc::new(ext))).build()
/// ```
pub struct Active<E>(pub E);

/// Wraps an extension type to signal it is passive (capabilities only).
///
/// No task is spawned, no control channel is created. The extension only
/// provides capabilities for consumers to look up.
///
/// # Usage
/// ```ignore
/// builder.with_shared(Passive(ext)).build()
/// builder.with_local(Passive(Rc::new(ext))).build()
/// ```
pub struct Passive<E>(pub E);

/// Decomposed result of a shared extension provider.
#[doc(hidden)]
pub struct SharedDecomposed {
    pub any: Box<dyn crate::capability::registry::CloneAnySend>,
    pub extension: Option<Box<dyn shared_ext::Extension>>,
    pub type_id: TypeId,
}

/// Decomposed result of a local extension provider.
#[doc(hidden)]
pub struct LocalDecomposed {
    pub any: std::rc::Rc<dyn std::any::Any>,
    pub extension: Option<std::rc::Rc<dyn local_ext::Extension>>,
    pub type_id: TypeId,
}

/// Sealed trait for shared extension providers (Active or Passive).
pub trait SharedProvider: sealed_provider::SealedShared {
    /// Decompose into type-erased components.
    fn decompose(self) -> SharedDecomposed;
}

/// Sealed trait for local extension providers (Active or Passive).
pub trait LocalProvider: sealed_provider::SealedLocal {
    /// Decompose into type-erased components.
    fn decompose(self) -> LocalDecomposed;
}

mod sealed_provider {
    pub trait SealedShared {}
    pub trait SealedLocal {}
}

impl<E: shared_ext::Extension + Clone + Send + 'static> sealed_provider::SealedShared
    for Active<E>
{
}

impl<E: shared_ext::Extension + Clone + Send + 'static> SharedProvider for Active<E> {
    fn decompose(self) -> SharedDecomposed {
        let any: Box<dyn crate::capability::registry::CloneAnySend> = Box::new(self.0.clone());
        let ext: Box<dyn shared_ext::Extension> = Box::new(self.0);
        SharedDecomposed {
            any,
            extension: Some(ext),
            type_id: TypeId::of::<E>(),
        }
    }
}

impl<E: Clone + Send + 'static> sealed_provider::SealedShared for Passive<E> {}

impl<E: Clone + Send + 'static> SharedProvider for Passive<E> {
    fn decompose(self) -> SharedDecomposed {
        let any: Box<dyn crate::capability::registry::CloneAnySend> = Box::new(self.0);
        SharedDecomposed {
            any,
            extension: None,
            type_id: TypeId::of::<E>(),
        }
    }
}

impl<E: local_ext::Extension + 'static> sealed_provider::SealedLocal for Active<std::rc::Rc<E>> {}

impl<E: local_ext::Extension + 'static> LocalProvider for Active<std::rc::Rc<E>> {
    fn decompose(self) -> LocalDecomposed {
        let any: std::rc::Rc<dyn std::any::Any> = self.0.clone();
        let ext: std::rc::Rc<dyn local_ext::Extension> = self.0;
        LocalDecomposed {
            any,
            extension: Some(ext),
            type_id: TypeId::of::<E>(),
        }
    }
}

impl<E: 'static> sealed_provider::SealedLocal for Passive<std::rc::Rc<E>> {}

impl<E: 'static> LocalProvider for Passive<std::rc::Rc<E>> {
    fn decompose(self) -> LocalDecomposed {
        let any: std::rc::Rc<dyn std::any::Any> = self.0;
        LocalDecomposed {
            any,
            extension: None,
            type_id: TypeId::of::<E>(),
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
/// An extension may provide capabilities (passive) or capabilities plus
/// an active event loop. Use `Active(ext)` or `Passive(ext)` wrappers
/// with the builder to signal the intent.
///
/// Use the builder to construct:
/// ```ignore
/// // Active extension with event loop
/// ExtensionWrapper::builder(node, config, ext_config)
///     .with_shared(Active(ext))
///     .build()
///
/// // Passive extension — capabilities only, no task spawned
/// ExtensionWrapper::builder(node, config, ext_config)
///     .with_shared(Passive(ext))
///     .build()
/// ```
pub struct ExtensionWrapper {
    /// Index identifier for the node.
    node_id: NodeId,
    /// The user configuration for the node.
    user_config: Arc<NodeUserConfig>,
    /// The runtime configuration for the extension.
    runtime_config: ExtensionConfig,
    /// Shared extension lifecycle (Send, clone-based). None for passive.
    shared_extension: Option<Box<dyn shared_ext::Extension>>,
    /// Local extension lifecycle (Rc-based, true single instance). None for passive.
    local_extension: Option<std::rc::Rc<dyn local_ext::Extension>>,
    /// Type-erased shared instance for capability registration.
    shared_any: Option<Box<dyn crate::capability::registry::CloneAnySend>>,
    /// Type-erased local instance for capability registration.
    local_any: Option<std::rc::Rc<dyn std::any::Any>>,
    /// Capabilities descriptor — set by the engine after `create()`.
    capabilities: crate::capability::registry::ExtensionCapabilities,
    /// A sender for control messages. None for fully passive extensions.
    control_sender: Option<SharedSender<ExtensionControlMsg>>,
    /// A receiver for control messages. None for fully passive extensions.
    control_receiver: Option<SharedReceiver<ExtensionControlMsg>>,
    /// A second sender for control messages (shared variant in dual-lifecycle mode).
    shared_control_sender: Option<SharedSender<ExtensionControlMsg>>,
    /// A second receiver for control messages (shared variant, dual-lifecycle mode).
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
    shared_any: Option<Box<dyn crate::capability::registry::CloneAnySend>>,
    local_any: Option<std::rc::Rc<dyn std::any::Any>>,
    shared_type_id: Option<TypeId>,
    local_type_id: Option<TypeId>,
}

impl ExtensionWrapperBuilder {
    /// Add a **local** (!Send) extension variant.
    ///
    /// Use `Active(Rc::new(ext))` for extensions with an event loop,
    /// or `Passive(Rc::new(ext))` for capability-only extensions.
    pub fn with_local(mut self, provider: impl LocalProvider) -> Self {
        let decomposed = provider.decompose();
        otel_debug!(
            "extension.builder.with_local",
            node_id = self.node_id.name.as_ref(),
            active = decomposed.extension.is_some(),
        );
        self.local_any = Some(decomposed.any);
        self.local_extension = decomposed.extension;
        self.local_type_id = Some(decomposed.type_id);
        self
    }

    /// Add a **shared** (Send) extension variant.
    ///
    /// Use `Active(ext)` for extensions with an event loop,
    /// or `Passive(ext)` for capability-only extensions.
    pub fn with_shared(mut self, provider: impl SharedProvider) -> Self {
        let decomposed = provider.decompose();
        otel_debug!(
            "extension.builder.with_shared",
            node_id = self.node_id.name.as_ref(),
            active = decomposed.extension.is_some(),
        );
        self.shared_any = Some(decomposed.any);
        self.shared_extension = decomposed.extension;
        self.shared_type_id = Some(decomposed.type_id);
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
            self.shared_any.is_some() || self.local_any.is_some(),
            "ExtensionWrapper must have at least one variant (local or shared)"
        );

        // When both variants are provided, they must be different concrete types.
        if let (Some(local_tid), Some(shared_tid)) = (self.local_type_id, self.shared_type_id) {
            assert!(
                local_tid != shared_tid,
                "with_local() and with_shared() called with the same concrete type — \
                 use with_shared() alone when a single type should serve both \
                 local and shared consumers"
            );
        }

        let has_local_lifecycle = self.local_extension.is_some();
        let has_shared_lifecycle = self.shared_extension.is_some();
        let has_any_lifecycle = has_local_lifecycle || has_shared_lifecycle;
        let has_both_lifecycles = has_local_lifecycle && has_shared_lifecycle;

        // Only create control channels when at least one active lifecycle exists.
        let (control_sender, control_receiver) = if has_any_lifecycle {
            let (tx, rx) = tokio::sync::mpsc::channel(self.runtime_config.control_channel.capacity);
            (Some(SharedSender::mpsc(tx)), Some(SharedReceiver::mpsc(rx)))
        } else {
            (None, None)
        };

        // Create a second control channel when both variants have active lifecycles.
        let (shared_control_sender, shared_control_receiver) = if has_both_lifecycles {
            let (tx, rx) = tokio::sync::mpsc::channel(self.runtime_config.control_channel.capacity);
            (Some(SharedSender::mpsc(tx)), Some(SharedReceiver::mpsc(rx)))
        } else {
            (None, None)
        };

        otel_debug!(
            "extension.builder.build",
            node_id = self.node_id.name.as_ref(),
            has_local_lifecycle = has_local_lifecycle,
            has_shared_lifecycle = has_shared_lifecycle,
        );

        ExtensionWrapper {
            node_id: self.node_id,
            user_config: self.user_config,
            runtime_config: self.runtime_config,
            shared_extension: self.shared_extension,
            local_extension: self.local_extension,
            shared_any: self.shared_any,
            local_any: self.local_any,
            capabilities: crate::capability::registry::ExtensionCapabilities {
                names: &[],
                register_shared: |_| Vec::new(),
                register_local: |_| Vec::new(),
            },
            control_sender,
            control_receiver,
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
    /// Use `Active(ext)` for extensions with event loops, `Passive(ext)` for
    /// capability-only extensions.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Active shared extension
    /// ExtensionWrapper::builder(node, config, ext_config)
    ///     .with_shared(Active(ext))
    ///     .build()
    ///
    /// // Passive shared extension
    /// ExtensionWrapper::builder(node, config, ext_config)
    ///     .with_shared(Passive(ext))
    ///     .build()
    ///
    /// // Both variants, active
    /// ExtensionWrapper::builder(node, config, ext_config)
    ///     .with_local(Active(Rc::new(local_ext)))
    ///     .with_shared(Active(shared_ext))
    ///     .build()
    /// ```
    #[must_use]
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
    pub fn set_capabilities(&mut self, caps: crate::capability::registry::ExtensionCapabilities) {
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

    /// Returns `true` if this extension is passive (no active lifecycle).
    ///
    /// Passive extensions only provide capabilities — no task is spawned,
    /// no control channel is created.
    #[must_use]
    pub fn is_passive(&self) -> bool {
        self.local_extension.is_none() && self.shared_extension.is_none()
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
    pub fn register_traits(
        &self,
        registry: &mut crate::capability::registry::CapabilityRegistry,
        name: &str,
    ) {
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
        // Skip if passive (no control channels).
        if self.control_sender.is_none() {
            return self;
        }

        let control_sender = self.control_sender.take().expect("checked above");
        let control_receiver = self
            .control_receiver
            .take()
            .expect("control_receiver already taken");
        let (wrapped_sender, wrapped_receiver) =
            wrap_control_channel_metrics::<SharedMode, ExtensionControlMsg>(
                &self.node_id,
                pipeline_ctx,
                channel_metrics,
                channel_metrics_enabled,
                self.runtime_config.control_channel.capacity as u64,
                control_sender,
                control_receiver,
            );
        self.control_sender = Some(wrapped_sender);
        self.control_receiver = Some(wrapped_receiver);

        // Wrap the second control channel if present (independent lifecycles).
        if let (Some(shared_sender), Some(shared_receiver)) = (
            self.shared_control_sender.take(),
            self.shared_control_receiver.take(),
        ) {
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
    /// Returns empty for passive extensions, one sender for single-variant
    /// active mode, two senders for dual-lifecycle active mode.
    pub(crate) fn extension_control_senders(&self) -> Vec<crate::control::ExtensionControlSender> {
        let mut senders = Vec::new();
        if let Some(ref sender) = self.control_sender {
            senders.push(crate::control::ExtensionControlSender {
                node_id: self.node_id.clone(),
                sender: crate::message::Sender::Shared(sender.clone()),
            });
        }
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
    /// - If no lifecycle is present (passive), this should not be called.
    pub async fn start(self, metrics_reporter: MetricsReporter) -> Result<TerminalState, Error> {
        let node_name = self.node_id.name.clone();
        let effect_handler = EffectHandler::new(self.node_id, metrics_reporter);
        let control_receiver = self
            .control_receiver
            .expect("start() called on passive extension — this is a bug");
        let ctrl_chan = ControlChannel::new(control_receiver);

        match (self.local_extension, self.shared_extension) {
            (Some(local_ext), Some(shared_ext)) => {
                otel_debug!("extension.start.both", node_id = node_name.as_ref(),);

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
                otel_debug!("extension.start.local_task", node_id = node_name.as_ref(),);
                let local_result = local_ext.start(ctrl_chan, effect_handler).await;

                // Wait for the shared variant to finish too.
                let shared_result = shared_handle.await.map_err(|e| Error::InternalError {
                    message: format!("shared extension task panicked: {e}"),
                })?;

                // Return the first error, or merge terminal states.
                match (local_result, shared_result) {
                    (Err(e), _) | (_, Err(e)) => Err(e),
                    (Ok(local_ts), Ok(shared_ts)) => Ok(local_ts.merge(shared_ts)),
                }
            }
            (Some(local_ext), None) => {
                otel_debug!("extension.start.local", node_id = node_name.as_ref(),);
                local_ext.start(ctrl_chan, effect_handler).await
            }
            (None, Some(shared_ext)) => {
                otel_debug!("extension.start.shared", node_id = node_name.as_ref(),);
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
            .with_shared(Active(extension))
            .build();
    }
}
