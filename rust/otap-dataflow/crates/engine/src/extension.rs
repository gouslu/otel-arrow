// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension system: trait, wrapper, error types, sealing, macros, and re-exports.
//!
//! This module provides everything needed to define and integrate extensions
//! into the pipeline engine:
//!
//! - [`Extension`] trait — lifecycle interface for extension components
//! - [`ExtensionWrapper`] — embeds an extension into the pipeline graph
//! - [`EffectHandler`] — side-effect API given to running extensions
//! - Error types, the `Sealed` trait hierarchy, and registration macros
//!
//! The registry and per-trait definitions live under their respective modules:
//!
//! - [`crate::shared::extension`] — `SharedExtensionRegistry`, `SharedExtensionTrait`, `BearerTokenProvider`
//! - [`crate::local::extension`] — `LocalExtensionRegistry`, `LocalExtensionTrait`, `ExtensionRegistrar`, `BearerTokenProvider`

use crate::channel_metrics::ChannelMetricsRegistry;
use crate::channel_mode::{LocalMode, wrap_control_channel_metrics};
use crate::config::ExtensionConfig;
use crate::context::PipelineContext;
use crate::control::{Controllable, NodeControlMsg, PipelineCtrlMsgSender};
use crate::effect_handler::{EffectHandlerCore, TelemetryTimerCancelHandle, TimerCancelHandle};
use crate::entity_context::NodeTelemetryGuard;
use crate::error::Error;
use crate::local::message::{LocalReceiver, LocalSender};
use crate::message;
use crate::message::{Receiver, Sender};
use crate::node::{Node, NodeId};
use crate::terminal_state::TerminalState;
use async_trait::async_trait;
use otap_df_channel::error::SendError;
use otap_df_channel::mpsc;
use otap_df_config::node::NodeUserConfig;
use otap_df_telemetry::reporter::MetricsReporter;
use std::sync::Arc;
use std::time::Duration;

// ── Re-exports ───────────────────────────────────────────────────────────────

pub use crate::local::extension::ExtensionRegistrar;
pub use crate::shared::extension::bearer_token_provider::{BearerToken, Secret};

// ── Error types ──────────────────────────────────────────────────────────────

/// Error when retrieving an extension trait.
#[derive(Debug)]
pub enum ExtensionError {
    /// Extension not found by name.
    NotFound {
        /// The name of the extension that was not found.
        name: String,
    },
    /// Extension found but doesn't implement the requested trait.
    TraitNotImplemented {
        /// The name of the extension.
        name: String,
        /// The expected trait name.
        expected: &'static str,
    },
}

impl std::fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtensionError::NotFound { name } => {
                write!(f, "extension '{}' not found", name)
            }
            ExtensionError::TraitNotImplemented { name, expected } => {
                write!(
                    f,
                    "extension '{}' does not implement trait {}",
                    name, expected
                )
            }
        }
    }
}

impl std::error::Error for ExtensionError {}

/// Error type for extension operations.
///
/// Thread-safe error type compatible with any `thiserror`-derived error.
pub type ExtensionOperationError = Box<dyn std::error::Error + Send + Sync>;

// ── Sealing ──────────────────────────────────────────────────────────────────

// Private module for sealing - external crates cannot implement Sealed
pub(crate) mod private {
    pub trait Sealed {}
}

// ── Extension trait ──────────────────────────────────────────────────────────

/// Trait for pipeline extensions that provide shared capabilities.
///
/// Extensions are long-lived components that run alongside the pipeline and
/// expose functionality (e.g., authentication, service discovery) to other
/// components through the [`ExtensionRegistry`].
///
/// Unlike receivers, processors, and exporters, extensions:
/// - Do NOT process pipeline data (PData)
/// - Do NOT have input/output pdata channels
/// - Only receive control messages (shutdown, timer ticks, config updates)
/// - Require `Send` but NOT `Sync`
///
/// # Parameters
///
/// The `PData` type parameter is required for compatibility with the pipeline's
/// control message infrastructure, but extensions never process PData directly.
///
/// # Example
///
/// ```ignore
/// use async_trait::async_trait;
/// use otap_df_engine::extension::{Extension, EffectHandler};
/// use otap_df_engine::message::{Message, MessageChannel};
/// use otap_df_engine::control::NodeControlMsg;
/// use otap_df_engine::terminal_state::TerminalState;
/// use otap_df_engine::error::Error;
///
/// struct MyAuthExtension { /* ... */ }
///
/// #[async_trait(?Send)]
/// impl<PData> Extension<PData> for MyAuthExtension {
///     async fn start(
///         self: Box<Self>,
///         mut msg_chan: MessageChannel<PData>,
///         effect_handler: EffectHandler<PData>,
///     ) -> Result<TerminalState, Error> {
///         loop {
///             match msg_chan.recv().await? {
///                 Message::Control(NodeControlMsg::Shutdown { .. }) => break,
///                 _ => {}
///             }
///         }
///         Ok(TerminalState::default())
///     }
/// }
/// ```
#[async_trait(?Send)]
pub trait Extension<PData>: Send {
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
    /// - `msg_chan`: A channel to receive control messages. Extensions do not
    ///   receive PData messages — only control messages (shutdown, timer, config).
    /// - `effect_handler`: A handler to perform side effects such as network
    ///   operations, timers, and extension registry lookups.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] if an unrecoverable error occurs.
    async fn start(
        self: Box<Self>,
        msg_chan: message::MessageChannel<PData>,
        effect_handler: EffectHandler<PData>,
    ) -> Result<TerminalState, Error>;
}

// ── EffectHandler ────────────────────────────────────────────────────────────

/// Effect handler for extensions.
///
/// Provides extensions with the ability to:
/// - Look up other extensions via the [`ExtensionRegistry`]
/// - Start periodic timers
/// - Print info messages
/// - Access node identity
///
/// This handler requires `Send` but not `Sync`.
#[derive(Clone)]
pub struct EffectHandler<PData> {
    pub(crate) core: EffectHandlerCore<PData>,
}

impl<PData> EffectHandler<PData> {
    /// Creates a new `EffectHandler` for the given extension node.
    #[must_use]
    pub const fn new(node_id: NodeId, metrics_reporter: MetricsReporter) -> Self {
        EffectHandler {
            core: EffectHandlerCore::new(node_id, metrics_reporter),
        }
    }

    /// Returns the id of the extension associated with this handler.
    #[must_use]
    pub fn extension_id(&self) -> NodeId {
        self.core.node_id()
    }

    /// Print an info message to stdout.
    pub async fn info(&self, message: &str) {
        self.core.info(message).await;
    }

    /// Starts a cancellable periodic timer that emits TimerTick on the control channel.
    /// Returns a handle that can be used to cancel the timer.
    pub async fn start_periodic_timer(
        &self,
        duration: Duration,
    ) -> Result<TimerCancelHandle<PData>, Error> {
        self.core.start_periodic_timer(duration).await
    }

    /// Starts a cancellable periodic telemetry timer.
    pub async fn start_periodic_telemetry(
        &self,
        duration: Duration,
    ) -> Result<TelemetryTimerCancelHandle<PData>, Error> {
        self.core.start_periodic_telemetry(duration).await
    }
}

// ── ExtensionWrapper ─────────────────────────────────────────────────────────

/// Wrapper around an extension instance that integrates it into the pipeline.
///
/// There is no Local/Shared split for extensions — they always use local
/// channels and are spawned on the single-threaded `LocalSet`.
pub struct ExtensionWrapper<PData> {
    /// Index identifier for the node.
    node_id: NodeId,
    /// The user configuration for the node.
    user_config: Arc<NodeUserConfig>,
    /// The runtime configuration for the extension.
    runtime_config: ExtensionConfig,
    /// The extension instance.
    extension: Box<dyn Extension<PData>>,
    /// Registrar closure that populates the `ExtensionRegistry` with this
    /// extension's trait implementations.
    ///
    /// Produced by the `shared_extension_traits!` or `local_extension_traits!`
    /// macro. Taken during pipeline build and invoked once to register
    /// `Arc<dyn Trait>` (shared) or `Rc<dyn Trait>` (local) entries.
    registrar: Option<ExtensionRegistrar>,
    /// A sender for control messages.
    control_sender: LocalSender<NodeControlMsg<PData>>,
    /// A receiver for control messages.
    control_receiver: Option<LocalReceiver<NodeControlMsg<PData>>>,
    /// Telemetry guard for node lifecycle cleanup.
    telemetry: Option<NodeTelemetryGuard>,
}

#[async_trait(?Send)]
impl<PData> Controllable<PData> for ExtensionWrapper<PData> {
    fn control_sender(&self) -> Sender<NodeControlMsg<PData>> {
        Sender::Local(self.control_sender.clone())
    }
}

impl<PData> ExtensionWrapper<PData> {
    /// Creates a new `ExtensionWrapper` with the given extension, registrar, and configuration.
    ///
    /// # Arguments
    ///
    /// * `extension` - The extension instance that handles the lifecycle (must be `Send`)
    /// * `registrar` - A registrar closure from `shared_extension_traits!` or
    ///   `local_extension_traits!` that will populate the `ExtensionRegistry`
    ///   with trait entries for this extension.
    /// * `node_id` - The node identifier
    /// * `user_config` - The user configuration
    /// * `config` - The extension runtime configuration
    pub fn new<E>(
        extension: E,
        registrar: ExtensionRegistrar,
        node_id: NodeId,
        user_config: Arc<NodeUserConfig>,
        config: &ExtensionConfig,
    ) -> Self
    where
        E: Extension<PData> + 'static,
    {
        let (control_sender, control_receiver) =
            mpsc::Channel::new(config.control_channel.capacity);

        ExtensionWrapper {
            node_id,
            user_config,
            runtime_config: config.clone(),
            extension: Box::new(extension),
            registrar: Some(registrar),
            control_sender: LocalSender::mpsc(control_sender),
            control_receiver: Some(LocalReceiver::mpsc(control_receiver)),
            telemetry: None,
        }
    }

    /// Takes the registrar closure from this wrapper, leaving `None` in its place.
    ///
    /// This is called during pipeline initialization to register the extension's
    /// trait implementations in the central `ExtensionRegistry`.
    pub fn take_registrar(&mut self) -> Option<ExtensionRegistrar> {
        self.registrar.take()
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
            wrap_control_channel_metrics::<LocalMode, PData>(
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

    /// Starts the extension and begins its operation.
    pub async fn start(
        self,
        pipeline_ctrl_msg_tx: PipelineCtrlMsgSender<PData>,
        metrics_reporter: MetricsReporter,
    ) -> Result<TerminalState, Error> {
        let ExtensionWrapper {
            node_id,
            extension,
            control_receiver,
            ..
        } = self;

        let mut effect_handler = EffectHandler::new(node_id, metrics_reporter);
        effect_handler
            .core
            .set_pipeline_ctrl_msg_sender(pipeline_ctrl_msg_tx);

        let control_receiver =
            control_receiver.expect("control_receiver missing from ExtensionWrapper");

        // Extensions only receive control messages, no pdata.
        // Create a dummy pdata receiver that will never produce data.
        let (_dummy_tx, dummy_rx) = mpsc::Channel::<PData>::new(1);
        let message_channel = message::MessageChannel::new(
            Receiver::Local(control_receiver),
            Receiver::Local(LocalReceiver::mpsc(dummy_rx)),
        );
        extension.start(message_channel, effect_handler).await
    }
}

#[async_trait(?Send)]
impl<PData> Node<PData> for ExtensionWrapper<PData> {
    fn is_shared(&self) -> bool {
        false
    }

    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    fn user_config(&self) -> Arc<NodeUserConfig> {
        self.user_config.clone()
    }

    async fn send_control_msg(
        &self,
        msg: NodeControlMsg<PData>,
    ) -> Result<(), SendError<NodeControlMsg<PData>>> {
        self.control_sender.send(msg).await
    }
}

// ── TelemetryWrapped impl ───────────────────────────────────────────────────

impl<PData> crate::TelemetryWrapped for ExtensionWrapper<PData> {
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

// ── impl_extension_trait! ────────────────────────────────────────────────────

/// Generates both `shared` and `local` trait implementations from a single body.
///
/// When the `shared::Trait` and `local::Trait` implementations are identical
/// (as is common for extension types that are inherently `Send + Sync`), this
/// macro eliminates the duplication by expanding one body into two `impl` blocks:
///
/// - `#[async_trait]` `impl shared::Trait for Type { ... }`
/// - `#[async_trait(?Send)]` `impl local::Trait for Type { ... }`
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::extension::BearerToken;
///
/// impl_extension_trait! {
///     impl BearerTokenProvider for MyExtension {
///         async fn get_token(&self) -> Result<BearerToken, otap_df_engine::extension::ExtensionOperationError> {
///             todo!()
///         }
///         fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
///             todo!()
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_extension_trait {
    (impl $trait_name:ident for $type:ty { $($body:tt)* }) => {
        #[async_trait::async_trait]
        impl $crate::shared::extension::$trait_name for $type {
            $($body)*
        }

        #[async_trait::async_trait(?Send)]
        impl $crate::local::extension::$trait_name for $type {
            $($body)*
        }
    };
}

// ── Registration macros ──────────────────────────────────────────────────────

/// Generates a registrar closure that registers **shared** `Arc<dyn Trait>` entries.
///
/// The instance is wrapped in `Arc` — accessible by both local and shared components.
/// Use this for cold-path traits (auth, service discovery) where thread-safety
/// overhead is negligible.
///
/// For registering both shared and local traits on the same instance, prefer
/// [`extension_traits!`] instead.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::shared::extension;
/// let registrar = shared_extension_traits!(auth_service => extension::BearerTokenProvider);
/// ```
#[macro_export]
macro_rules! shared_extension_traits {
    ($instance:expr => $($trait:path),* $(,)?) => {{
        let __arc = std::sync::Arc::new($instance);
        let __registrar: $crate::local::extension::ExtensionRegistrar = Box::new({
            move |registry: &mut $crate::local::extension::LocalExtensionRegistry, name: &str| {
                $(
                    {
                        // Compile-time check: ensure the trait is a valid SharedExtensionTrait.
                        const _: fn() = || {
                            fn assert_shared_trait<T: ?Sized + $crate::shared::extension::SharedExtensionTrait>() {}
                            assert_shared_trait::<dyn $trait>();
                        };
                        // Coerce Arc<ConcreteType> → Arc<dyn Trait> (zero-cost)
                        registry.register::<dyn $trait>(name, __arc.clone() as std::sync::Arc<dyn $trait>);
                    }
                )*
            }
        });
        __registrar
    }};
}

/// Generates a registrar closure that registers **local** `Rc<dyn Trait>` entries.
///
/// The instance is captured by value (`Send`) and wrapped in `Rc` on the worker
/// thread — never crosses a thread boundary. Accessible only by local components.
/// Use this for hot-path traits (rate limiters, quotas) where atomic/mutex
/// overhead matters.
///
/// For registering both shared and local traits on the same instance, prefer
/// [`extension_traits!`] instead.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::local::extension::RateLimiter;
/// let registrar = local_extension_traits!(rate_limiter => RateLimiter);
/// ```
#[macro_export]
macro_rules! local_extension_traits {
    ($instance:expr => $($trait:path),* $(,)?) => {{
        let __registrar: $crate::local::extension::ExtensionRegistrar = Box::new({
            let __instance = $instance;
            move |registry: &mut $crate::local::extension::LocalExtensionRegistry, name: &str| {
                // Wrap in Rc on the worker thread (never crosses thread boundary)
                let __rc = std::rc::Rc::new(__instance);
                $(
                    {
                        // Compile-time check: ensure the trait is a valid LocalExtensionTrait.
                        const _: fn() = || {
                            fn assert_local_trait<T: ?Sized + $crate::local::extension::LocalExtensionTrait>() {}
                            assert_local_trait::<dyn $trait>();
                        };
                        // Coerce Rc<ConcreteType> → Rc<dyn Trait> (zero-cost)
                        registry.register_local::<dyn $trait>(name, __rc.clone() as std::rc::Rc<dyn $trait>);
                    }
                )*
            }
        });
        __registrar
    }};
}

/// Generates a registrar closure that registers both **shared** and **local** traits
/// on the same extension instance.
///
/// This is the preferred macro when an extension exposes traits for both shared
/// components (`Arc<dyn Trait>`) and local components (`Rc<dyn Trait>`). The
/// instance is cloned once — wrapped in `Arc` for shared traits and captured
/// by value for local traits (wrapped in `Rc` on the worker thread).
///
/// # Syntax
///
/// ```ignore
/// extension_traits!(instance =>
///     shared(SharedTrait1, SharedTrait2),
///     local(LocalTrait1, LocalTrait2),
/// )
/// ```
///
/// Either `shared(...)` or `local(...)` may be omitted if only one kind is needed,
/// but prefer [`shared_extension_traits!`] or [`local_extension_traits!`] in that case.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::shared::extension::BearerTokenProvider as SharedBearerTokenProvider;
/// use otap_df_engine::local::extension::BearerTokenProvider as LocalBearerTokenProvider;
/// let registrar = extension_traits!(extension =>
///     shared(SharedBearerTokenProvider),
///     local(LocalBearerTokenProvider),
/// );
/// ```
#[macro_export]
macro_rules! extension_traits {
    // shared + local
    ($instance:expr => shared($($shared_trait:path),* $(,)?), local($($local_trait:path),* $(,)?) $(,)?) => {{
        let __instance = $instance;
        let __arc = std::sync::Arc::new(__instance.clone());
        let __registrar: $crate::local::extension::ExtensionRegistrar = Box::new({
            move |registry: &mut $crate::local::extension::LocalExtensionRegistry, name: &str| {
                // Register shared (Arc) traits
                $(
                    {
                        const _: fn() = || {
                            fn assert_shared<T: ?Sized + $crate::shared::extension::SharedExtensionTrait>() {}
                            assert_shared::<dyn $shared_trait>();
                        };
                        registry.register::<dyn $shared_trait>(name, __arc.clone() as std::sync::Arc<dyn $shared_trait>);
                    }
                )*
                // Register local (Rc) traits
                let __rc = std::rc::Rc::new(__instance);
                $(
                    {
                        const _: fn() = || {
                            fn assert_local<T: ?Sized + $crate::local::extension::LocalExtensionTrait>() {}
                            assert_local::<dyn $local_trait>();
                        };
                        registry.register_local::<dyn $local_trait>(name, __rc.clone() as std::rc::Rc<dyn $local_trait>);
                    }
                )*
            }
        });
        __registrar
    }};
    // shared only (forwarded)
    ($instance:expr => shared($($shared_trait:path),* $(,)?) $(,)?) => {
        $crate::shared_extension_traits!($instance => $($shared_trait),*)
    };
    // local only (forwarded)
    ($instance:expr => local($($local_trait:path),* $(,)?) $(,)?) => {
        $crate::local_extension_traits!($instance => $($local_trait),*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::NodeControlMsg;
    use crate::message::Message;
    use crate::testing::{CtrlMsgCounters, TestMsg, test_node};
    use otap_df_config::node::NodeUserConfig;
    use serde_json::Value;

    struct TestExtension {
        counter: CtrlMsgCounters,
    }

    impl TestExtension {
        fn new(counter: CtrlMsgCounters) -> Self {
            TestExtension { counter }
        }
    }

    #[async_trait(?Send)]
    impl Extension<TestMsg> for TestExtension {
        async fn start(
            self: Box<Self>,
            mut msg_chan: message::MessageChannel<TestMsg>,
            _effect_handler: EffectHandler<TestMsg>,
        ) -> Result<TerminalState, Error> {
            loop {
                match msg_chan.recv().await? {
                    Message::Control(NodeControlMsg::TimerTick { .. }) => {
                        self.counter.increment_timer_tick();
                    }
                    Message::Control(NodeControlMsg::Config { .. }) => {
                        self.counter.increment_config();
                    }
                    Message::Control(NodeControlMsg::Shutdown { .. }) => {
                        self.counter.increment_shutdown();
                        break;
                    }
                    Message::Control(NodeControlMsg::CollectTelemetry { .. }) => {}
                    Message::Control(NodeControlMsg::Ack(_)) => {}
                    Message::Control(NodeControlMsg::Nack(_)) => {}
                    Message::Control(NodeControlMsg::DelayedData { .. }) => {}
                    Message::PData(_) => {}
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

        // Use a no-op registrar that registers nothing.
        let registrar: ExtensionRegistrar = Box::new(|_registry, _name| {});
        let wrapper = ExtensionWrapper::new(
            extension,
            registrar,
            node_id,
            user_config,
            &config,
        );

        assert!(!wrapper.is_shared());
    }
}
