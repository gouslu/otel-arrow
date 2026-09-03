// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Controller-hosted engine and pipeline-group extensions.
//!
//! Extensions declared above pipeline scope are instantiated once on a
//! controller-owned `LocalSet`. Only their shared variant is retained because
//! every capability handle can cross pipeline-thread boundaries.

use crate::PipelineFactory;
use crate::capability::registry::CapabilityRegistry;
use crate::capability::{ExtensionCapabilities, SharedInstanceFactory};
use crate::channel_metrics::{ChannelMetricsHandle, ChannelMetricsRegistry};
use crate::config::ExtensionConfig;
use crate::context::{ControllerContext, ExtensionContext};
use crate::entity_context::{EntityTelemetryGuard, EntityTelemetryHandle};
use crate::error::Error;
use crate::extension::ExtensionWrapper;
use crate::extension_lifecycle::{EXTENSION_SHUTDOWN_GRACE, ExtensionLifecycle, LifecycleEvent};
use crate::extension_monitor::ExtensionMetricsMonitor;
use crate::terminal_state::TerminalMetricsDeadline;
use futures::future::{join_all, try_join_all};
use futures::stream::{FuturesUnordered, StreamExt};
use otel_arrow_dfe_config::engine::OtelDataflowSpec;
use otel_arrow_dfe_config::pipeline::PipelineExtensions;
use otel_arrow_dfe_config::policy::{Policies, ResolvedPolicies};
use otel_arrow_dfe_config::{ExtensionId, MetricLevel, PipelineGroupId};
use otel_arrow_dfe_telemetry::otel_warn;
use otel_arrow_dfe_telemetry::reporter::MetricsReporter;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::{sync::oneshot, task};

const EXTENSION_MONITOR_TICK_INTERVAL: Duration = Duration::from_secs(1);
const EXTENSION_MONITOR_COLLECT_TELEMETRY_INTERVAL: Duration = Duration::from_secs(10);

/// A pipeline-owned snapshot of shared capability providers inherited from
/// engine and pipeline-group scopes.
#[derive(Default)]
pub struct InheritedExtensionRegistrations {
    known_extensions: HashSet<ExtensionId>,
    registrations: Vec<SharedExtensionRegistration>,
}

impl InheritedExtensionRegistrations {
    pub(crate) fn remove_shadowed_by(&mut self, pipeline_extensions: &PipelineExtensions) {
        self.known_extensions
            .retain(|extension_id| !pipeline_extensions.contains_key(extension_id));
        self.registrations
            .retain(|registration| !pipeline_extensions.contains_key(&registration.extension_id));
    }

    pub(crate) fn extend_known_extensions(&self, known_extensions: &mut HashSet<ExtensionId>) {
        known_extensions.extend(self.known_extensions.iter().cloned());
    }

    pub(crate) fn register_into(&self, registry: &mut CapabilityRegistry) -> Result<(), Error> {
        for registration in &self.registrations {
            (registration.capabilities.register_shared)(
                registration.extension_id.clone(),
                registration.instance_factory.clone(),
                registry,
            )
            .map_err(|error| Error::CapabilityRegistrationFailed {
                extension: registration.extension_id.clone(),
                message: error.to_string(),
            })?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct SharedExtensionRegistration {
    extension_id: ExtensionId,
    capabilities: ExtensionCapabilities,
    instance_factory: SharedInstanceFactory,
}

#[derive(Default)]
struct ScopeCatalog {
    known_extensions: HashSet<ExtensionId>,
    registrations: Vec<SharedExtensionRegistration>,
}

#[derive(Default)]
struct HierarchicalExtensionCatalog {
    engine: ScopeCatalog,
    groups: HashMap<PipelineGroupId, ScopeCatalog>,
}

#[derive(Default)]
struct HierarchicalExtensionRegistryState {
    installed: bool,
    catalog: HierarchicalExtensionCatalog,
}

/// Immutable capability catalogs for controller-hosted extension scopes.
///
/// The internal mutex is used only to make the type shareable across
/// controller and rollout threads. The catalog is installed once during
/// startup and read thereafter.
#[derive(Clone, Default)]
pub struct HierarchicalExtensionRegistry {
    state: Arc<Mutex<HierarchicalExtensionRegistryState>>,
}

impl HierarchicalExtensionRegistry {
    fn install(&self, catalog: HierarchicalExtensionCatalog) -> Result<(), Error> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.installed {
            return Err(Error::InternalError {
                message: "hierarchical extension catalog was installed more than once".into(),
            });
        }
        state.catalog = catalog;
        state.installed = true;
        Ok(())
    }

    /// Returns the ancestor registrations visible from a pipeline.
    ///
    /// Pipeline declarations shadow group and engine declarations. Group
    /// declarations shadow engine declarations with the same extension ID.
    #[must_use]
    pub fn registrations_for_pipeline(
        &self,
        pipeline_group_id: &PipelineGroupId,
        pipeline_extensions: &PipelineExtensions,
    ) -> InheritedExtensionRegistrations {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut inherited = InheritedExtensionRegistrations::default();
        let group = state.catalog.groups.get(pipeline_group_id);

        if let Some(group) = group {
            for extension_id in &group.known_extensions {
                if !pipeline_extensions.contains_key(extension_id) {
                    let _ = inherited.known_extensions.insert(extension_id.clone());
                }
            }
            inherited.registrations.extend(
                group
                    .registrations
                    .iter()
                    .filter(|registration| {
                        !pipeline_extensions.contains_key(&registration.extension_id)
                    })
                    .cloned(),
            );
        }

        for extension_id in &state.catalog.engine.known_extensions {
            let shadowed_by_group =
                group.is_some_and(|group| group.known_extensions.contains(extension_id));
            if !pipeline_extensions.contains_key(extension_id) && !shadowed_by_group {
                let _ = inherited.known_extensions.insert(extension_id.clone());
            }
        }
        inherited.registrations.extend(
            state
                .catalog
                .engine
                .registrations
                .iter()
                .filter(|registration| {
                    !pipeline_extensions.contains_key(&registration.extension_id)
                        && !group.is_some_and(|group| {
                            group.known_extensions.contains(&registration.extension_id)
                        })
                })
                .cloned(),
        );

        inherited
    }
}

struct PreparedExtensionScope {
    context: ExtensionContext,
    extensions: Vec<(
        ExtensionWrapper,
        otel_arrow_dfe_telemetry::registry::EntityKey,
    )>,
    channel_metrics: Vec<ChannelMetricsHandle>,
    telemetry_policy: otel_arrow_dfe_config::policy::TelemetryPolicy,
}

impl PreparedExtensionScope {
    fn start(self, metrics_reporter: &MetricsReporter) -> RunningExtensionScope {
        let terminal_metrics_deadline = TerminalMetricsDeadline::default();
        let monitor = if self.telemetry_policy.pipeline_metrics {
            ExtensionMetricsMonitor::new(
                self.context.clone(),
                EXTENSION_MONITOR_TICK_INTERVAL,
                EXTENSION_MONITOR_COLLECT_TELEMETRY_INTERVAL,
            )
        } else {
            ExtensionMetricsMonitor::disabled(self.context.clone())
        };
        let lifecycle = ExtensionLifecycle::spawn_current(
            self.extensions,
            metrics_reporter.clone(),
            terminal_metrics_deadline.clone(),
            &self.context,
            monitor,
        );
        RunningExtensionScope {
            lifecycle,
            channel_metrics: self.channel_metrics,
            metrics_reporter: metrics_reporter.clone(),
            terminal_metrics_deadline,
        }
    }
}

struct RunningExtensionScope {
    lifecycle: ExtensionLifecycle,
    channel_metrics: Vec<ChannelMetricsHandle>,
    metrics_reporter: MetricsReporter,
    terminal_metrics_deadline: TerminalMetricsDeadline,
}

impl RunningExtensionScope {
    async fn wait_ready(&mut self) -> Result<(), Error> {
        self.lifecycle.wait_all_spawned().await?;
        self.lifecycle.wait_all_ready().await
    }

    fn report_channel_metrics(&self, metrics_reporter: &mut MetricsReporter) {
        for metrics in &self.channel_metrics {
            if let Err(error) = metrics.report(metrics_reporter) {
                otel_warn!(
                    "hierarchical_extension.channel_metrics.reporting_failed",
                    error = error.to_string()
                );
            }
        }
    }

    async fn shutdown(&mut self) {
        self.terminal_metrics_deadline
            .record(Instant::now() + EXTENSION_SHUTDOWN_GRACE);
        self.lifecycle
            .initiate_shutdown(Some("hierarchical extension host shutdown"));
        self.lifecycle.drain_until_deadline().await;

        let mut reporter = self.metrics_reporter.clone();
        self.report_channel_metrics(&mut reporter);
        let deadline = self.terminal_metrics_deadline.clone().get();
        if let Err(error) = self
            .lifecycle
            .finish_metrics_reporting_until(&reporter, deadline)
            .await
        {
            otel_warn!(
                "hierarchical_extension.metrics.final_reporting_failed",
                error = error.to_string()
            );
        }
    }

    async fn run(mut self, mut shutdown_rx: oneshot::Receiver<()>) -> Result<(), Error> {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    self.shutdown().await;
                    return Ok(());
                }
                event = self.lifecycle.next_event() => {
                    match event {
                        LifecycleEvent::MonitorTick(now) => {
                            let mut reporter = self.metrics_reporter.clone();
                            self.lifecycle.monitor_tick(now, &mut reporter);
                            self.report_channel_metrics(&mut reporter);
                        }
                        LifecycleEvent::Completion(Ok(Ok(()))) => {
                            unreachable!(
                                "an extension completion before host shutdown must be upgraded to an error"
                            );
                        }
                        LifecycleEvent::Completion(Ok(Err(error))) => {
                            self.shutdown().await;
                            return Err(error);
                        }
                        LifecycleEvent::Completion(Err(error)) => {
                            self.shutdown().await;
                            return Err(Error::JoinTaskError {
                                is_canceled: error.is_cancelled(),
                                is_panic: error.is_panic(),
                                error: error.to_string(),
                            });
                        }
                    }
                }
            }
        }
    }
}

/// Prepared controller-owned extension scopes.
///
/// Construction is synchronous and does not spawn tasks. Call [`start`](Self::start)
/// from inside the host thread's `LocalSet`.
pub struct PreparedHierarchicalExtensionHost {
    engine_scope: Option<PreparedExtensionScope>,
    group_scopes: Vec<PreparedExtensionScope>,
    catalog: HierarchicalExtensionCatalog,
    registry: HierarchicalExtensionRegistry,
    metrics_reporter: MetricsReporter,
}

impl PreparedHierarchicalExtensionHost {
    /// Starts every engine and group extension, waits for spawn and readiness
    /// barriers, and publishes the immutable capability catalog.
    pub async fn start(self) -> Result<RunningHierarchicalExtensionHost, Error> {
        let Self {
            engine_scope,
            group_scopes,
            catalog,
            registry,
            metrics_reporter,
        } = self;
        let mut engine_scope = engine_scope.map(|scope| scope.start(&metrics_reporter));
        if let Some(scope) = engine_scope.as_mut()
            && let Err(error) = scope.wait_ready().await
        {
            scope.shutdown().await;
            return Err(error);
        }

        // Every group starts only after the engine-level barrier succeeds.
        // Groups are peers, so their readiness waits run concurrently.
        let mut group_scopes: Vec<_> = group_scopes
            .into_iter()
            .map(|scope| scope.start(&metrics_reporter))
            .collect();
        if let Err(error) = try_join_all(
            group_scopes
                .iter_mut()
                .map(RunningExtensionScope::wait_ready),
        )
        .await
        {
            let _ = join_all(group_scopes.iter_mut().map(RunningExtensionScope::shutdown)).await;
            if let Some(scope) = engine_scope.as_mut() {
                scope.shutdown().await;
            }
            return Err(error);
        }

        if let Err(error) = registry.install(catalog) {
            let _ = join_all(group_scopes.iter_mut().map(RunningExtensionScope::shutdown)).await;
            if let Some(scope) = engine_scope.as_mut() {
                scope.shutdown().await;
            }
            return Err(error);
        }

        Ok(RunningHierarchicalExtensionHost {
            engine_scope,
            group_scopes,
        })
    }
}

/// Running controller-owned engine and pipeline-group extension scopes.
pub struct RunningHierarchicalExtensionHost {
    engine_scope: Option<RunningExtensionScope>,
    group_scopes: Vec<RunningExtensionScope>,
}

impl RunningHierarchicalExtensionHost {
    /// Drives all scopes until cancellation or the first extension failure.
    ///
    /// On failure, `notify_failure` runs before any surviving scope is stopped.
    /// The host then keeps those providers alive until `shutdown` completes so
    /// the controller can drain descendant pipelines first.
    pub async fn run<F, N>(self, shutdown: F, notify_failure: N) -> Result<(), Error>
    where
        F: Future<Output = ()> + 'static,
        N: FnOnce(String),
    {
        let engine_scope_present = self.engine_scope.is_some();
        let mut scopes =
            Vec::with_capacity(usize::from(engine_scope_present) + self.group_scopes.len());
        if let Some(engine_scope) = self.engine_scope {
            scopes.push(engine_scope);
        }
        scopes.extend(self.group_scopes);
        if scopes.is_empty() {
            shutdown.await;
            return Ok(());
        }

        let group_start_index = usize::from(engine_scope_present);
        let mut tasks = FuturesUnordered::new();
        let mut task_ids = HashMap::new();
        let mut shutdown_senders = Vec::with_capacity(scopes.len());
        let mut completed = vec![false; scopes.len()];
        for (index, scope) in scopes.into_iter().enumerate() {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let handle = task::spawn_local(async move {
                let result = scope.run(shutdown_rx).await;
                (index, result)
            });
            let _ = task_ids.insert(handle.id(), index);
            tasks.push(handle);
            shutdown_senders.push(Some(shutdown_tx));
        }

        let mut first_error = None;
        tokio::pin!(shutdown);
        let failure_observed = tokio::select! {
            _ = &mut shutdown => false,
            Some(joined) = tasks.next() => {
                let (index, error) = Self::route_scope_completion(
                    joined,
                    &mut task_ids,
                    &mut shutdown_senders,
                    &mut completed,
                );
                first_error = error.or_else(|| {
                    Some(Error::InternalError {
                        message: format!(
                            "hierarchical extension scope {index} exited before host shutdown"
                        ),
                    })
                });
                true
            }
        };

        if failure_observed {
            notify_failure(
                first_error
                    .as_ref()
                    .expect("scope failure must produce an error")
                    .to_string(),
            );
            (&mut shutdown).await;
        }

        // Stop all group scopes concurrently. The engine scope remains alive
        // until every group has finished.
        for shutdown_tx in shutdown_senders.iter_mut().skip(group_start_index) {
            if let Some(shutdown_tx) = shutdown_tx.take() {
                let _ = shutdown_tx.send(());
            }
        }
        while completed
            .iter()
            .skip(group_start_index)
            .any(|is_completed| !is_completed)
        {
            let Some(joined) = tasks.next().await else {
                break;
            };
            let (_, error) = Self::route_scope_completion(
                joined,
                &mut task_ids,
                &mut shutdown_senders,
                &mut completed,
            );
            if first_error.is_none() {
                first_error = error;
            }
        }

        // The declaration root stops last.
        if engine_scope_present && !completed[0] {
            if let Some(shutdown_tx) = shutdown_senders[0].take() {
                let _ = shutdown_tx.send(());
            }
            while !completed[0] {
                let Some(joined) = tasks.next().await else {
                    break;
                };
                let (_, error) = Self::route_scope_completion(
                    joined,
                    &mut task_ids,
                    &mut shutdown_senders,
                    &mut completed,
                );
                if first_error.is_none() {
                    first_error = error;
                }
            }
        }

        first_error.map_or(Ok(()), Err)
    }

    fn route_scope_completion(
        joined: Result<(usize, Result<(), Error>), task::JoinError>,
        task_ids: &mut HashMap<task::Id, usize>,
        shutdown_senders: &mut [Option<oneshot::Sender<()>>],
        completed: &mut [bool],
    ) -> (usize, Option<Error>) {
        match joined {
            Ok((index, result)) => {
                task_ids.retain(|_, mapped_index| *mapped_index != index);
                completed[index] = true;
                let shutdown_was_requested = shutdown_senders[index].is_none();
                shutdown_senders[index] = None;
                let error = match result {
                    Ok(()) if shutdown_was_requested => None,
                    Ok(()) => Some(Error::InternalError {
                        message: format!(
                            "hierarchical extension scope {index} exited before host shutdown"
                        ),
                    }),
                    Err(error) => Some(error),
                };
                (index, error)
            }
            Err(error) => {
                let index = task_ids.remove(&error.id()).unwrap_or(0);
                if index < completed.len() {
                    completed[index] = true;
                    shutdown_senders[index] = None;
                }
                (
                    index,
                    Some(Error::JoinTaskError {
                        is_canceled: error.is_cancelled(),
                        is_panic: error.is_panic(),
                        error: error.to_string(),
                    }),
                )
            }
        }
    }
}

impl<PData: 'static + Clone + Debug> PipelineFactory<PData> {
    /// Prepares all engine and group extension declarations for the dedicated
    /// hierarchical-extension host thread.
    pub fn prepare_hierarchical_extensions(
        &'static self,
        config: &OtelDataflowSpec,
        controller_context: &ControllerContext,
        metrics_reporter: MetricsReporter,
        registry: HierarchicalExtensionRegistry,
    ) -> Result<PreparedHierarchicalExtensionHost, Error> {
        let engine_policies = Policies::resolve([&config.policies]);
        let (engine_scope, engine_catalog) = self.prepare_extension_scope(
            "engine",
            &config.extensions,
            controller_context.engine_extension_context(),
            &engine_policies,
        )?;

        let mut group_scopes = Vec::new();
        let mut catalog = HierarchicalExtensionCatalog {
            engine: engine_catalog,
            groups: HashMap::new(),
        };

        let mut groups: Vec<_> = config.groups.iter().collect();
        groups.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
        for (pipeline_group_id, pipeline_group) in groups {
            let policies = pipeline_group.policies.as_ref().map_or_else(
                || Policies::resolve([&config.policies]),
                |group_policies| Policies::resolve([group_policies, &config.policies]),
            );
            let scope_name = format!("pipeline group `{pipeline_group_id}`");
            let (scope, group_catalog) = self.prepare_extension_scope(
                &scope_name,
                &pipeline_group.extensions,
                controller_context.pipeline_group_extension_context(pipeline_group_id.clone()),
                &policies,
            )?;
            if let Some(scope) = scope {
                group_scopes.push(scope);
            }
            if !group_catalog.known_extensions.is_empty() {
                let _ = catalog
                    .groups
                    .insert(pipeline_group_id.clone(), group_catalog);
            }
        }

        Ok(PreparedHierarchicalExtensionHost {
            engine_scope,
            group_scopes,
            catalog,
            registry,
            metrics_reporter,
        })
    }

    fn prepare_extension_scope(
        &self,
        scope_name: &str,
        extensions: &PipelineExtensions,
        context: ExtensionContext,
        policies: &ResolvedPolicies,
    ) -> Result<(Option<PreparedExtensionScope>, ScopeCatalog), Error> {
        let mut configured_extensions: Vec<_> = extensions.iter().collect();
        configured_extensions.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));

        let mut wrappers = Vec::with_capacity(configured_extensions.len());
        let mut channel_metrics = ChannelMetricsRegistry::default();
        let mut catalog = ScopeCatalog::default();
        for (extension_id, user_config) in configured_extensions {
            let _ = catalog.known_extensions.insert(extension_id.clone());
            let raw_urn = user_config.r#type.as_str();
            let factory = self
                .get_extension_factory_map()
                .get(raw_urn)
                .ok_or_else(|| Error::UnknownExtension {
                    plugin_urn: raw_urn.to_string(),
                })?;

            if factory
                .capabilities
                .as_ref()
                .is_some_and(|capabilities| capabilities.shared.is_empty())
            {
                return Err(Error::HierarchicalExtensionRequiresShared {
                    extension: extension_id.clone(),
                    scope: scope_name.to_owned(),
                });
            }

            let runtime_config = ExtensionConfig::with_control_channel_capacity(
                extension_id.clone(),
                policies.channel_capacity.control.node,
            );
            let mut bundle = (factory.create)(
                &context,
                extension_id.clone(),
                user_config.clone(),
                &runtime_config,
            )
            .map_err(|error| Error::ConfigError(Box::new(error)))?;
            let Some(mut shared) = bundle.take_shared() else {
                return Err(Error::HierarchicalExtensionRequiresShared {
                    extension: extension_id.clone(),
                    scope: scope_name.to_owned(),
                });
            };

            if let Some(capabilities) = factory.capabilities.as_ref() {
                let instance_factory = shared
                    .shared_instance_factory()
                    .expect("a shared wrapper always has a shared instance factory")
                    .clone();
                catalog.registrations.push(SharedExtensionRegistration {
                    extension_id: extension_id.clone(),
                    capabilities: capabilities.clone(),
                    instance_factory,
                });
            }

            let entity_key =
                context.register_extension_entity(extension_id.clone(), shared.variant());
            let entity_handle = EntityTelemetryHandle::new(context.metrics_registry(), entity_key);
            let channel_metrics_enabled = policies.telemetry.runtime_metrics >= MetricLevel::Basic;
            shared = shared.with_control_channel_metrics(
                &entity_handle,
                &context,
                &mut channel_metrics,
                channel_metrics_enabled,
            );
            shared = shared.with_entity_telemetry_guard(EntityTelemetryGuard::new(entity_handle));
            wrappers.push((shared, entity_key));
        }

        let scope = (!wrappers.is_empty()).then(|| PreparedExtensionScope {
            context,
            extensions: wrappers,
            channel_metrics: channel_metrics.into_handles(),
            telemetry_policy: policies.telemetry.clone(),
        });
        Ok((scope, catalog))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExtensionFactory;
    use crate::capability::ExtensionCapability;
    use crate::capability::KnownCapability;
    use crate::capability::registry::{
        CapabilityRegistry, ConsumedTracker, SharedCapabilityEntry, resolve_bindings,
    };
    use crate::control::ExtensionControlMsg;
    use crate::extension::ExtensionWrapper;
    use crate::shared::extension;
    use crate::terminal_state::TerminalState;
    use async_trait::async_trait;
    use otel_arrow_dfe_config::extension::ExtensionUserConfig;
    use otel_arrow_dfe_config::{CapabilityId, ExtensionId};
    use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
    use std::any::Any;
    use std::rc::Rc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tokio::task::LocalSet;

    const LIFECYCLE_EXTENSION_URN: &str = "urn:test:extension:hierarchy_lifecycle";
    const LOCAL_ONLY_EXTENSION_URN: &str = "urn:test:extension:hierarchy_local_only";
    const DUAL_EXTENSION_URN: &str = "urn:test:extension:hierarchy_dual";

    trait HierarchyStateLocal {
        fn increment(&self) -> usize;
    }

    trait HierarchyStateShared: Send + Sync {
        fn increment(&self) -> usize;
    }

    struct HierarchyStateCapability;

    impl crate::capability::CapabilitySealed for HierarchyStateCapability {}

    impl ExtensionCapability for HierarchyStateCapability {
        const NAME: &'static str = "hierarchy_test_state";
        type Local = dyn HierarchyStateLocal;
        type Shared = dyn HierarchyStateShared;

        fn wrap_shared_as_local(shared: Box<Self::Shared>) -> Box<Self::Local> {
            struct Adapter(Box<dyn HierarchyStateShared>);
            impl HierarchyStateLocal for Adapter {
                fn increment(&self) -> usize {
                    self.0.increment()
                }
            }
            Box::new(Adapter(shared))
        }
    }

    #[allow(unsafe_code)]
    #[linkme::distributed_slice(crate::capability::KNOWN_CAPABILITIES)]
    #[linkme(crate = linkme)]
    static HIERARCHY_STATE_CAPABILITY: KnownCapability = KnownCapability {
        name: "hierarchy_test_state",
        description: "Hierarchy shared-state test capability",
        type_id: || std::any::TypeId::of::<HierarchyStateCapability>(),
    };

    impl HierarchyStateCapability {
        fn shared_entry<E>(
            extension_id: ExtensionId,
            factory: SharedInstanceFactory,
        ) -> SharedCapabilityEntry
        where
            E: HierarchyStateShared + 'static,
        {
            let produce = move || -> Box<dyn Any + Send> {
                let concrete: Box<E> = factory
                    .produce()
                    .downcast()
                    .expect("hierarchy test instance factory");
                let shared: Box<dyn HierarchyStateShared> = concrete;
                Box::new(shared) as Box<dyn Any + Send>
            };
            let adapt_as_local: fn(Box<dyn Any + Send>) -> Box<dyn Any> = |erased| {
                let shared: Box<Box<dyn HierarchyStateShared>> =
                    erased.downcast().expect("hierarchy test envelope");
                let local = <HierarchyStateCapability as ExtensionCapability>::wrap_shared_as_local(
                    *shared,
                );
                Box::new(local) as Box<dyn Any>
            };
            SharedCapabilityEntry::new(extension_id, produce, adapt_as_local)
        }
    }

    #[derive(Clone)]
    struct SharedStateProbe {
        label: &'static str,
        value: Arc<AtomicUsize>,
    }

    impl HierarchyStateShared for SharedStateProbe {
        fn increment(&self) -> usize {
            self.value.fetch_add(1, Ordering::SeqCst) + 1
        }
    }

    fn test_capabilities() -> ExtensionCapabilities {
        crate::extension_capabilities!(
            shared: SharedStateProbe => [HierarchyStateCapability]
        )
    }

    fn shared_registration(
        extension_id: &'static str,
        label: &'static str,
        value: Arc<AtomicUsize>,
    ) -> SharedExtensionRegistration {
        let prototype = SharedStateProbe { label, value };
        SharedExtensionRegistration {
            extension_id: extension_id.into(),
            capabilities: test_capabilities(),
            instance_factory: SharedInstanceFactory::new(move || {
                Box::new(prototype.clone()) as Box<dyn Any + Send>
            }),
        }
    }

    fn scope_catalog(registrations: Vec<SharedExtensionRegistration>) -> ScopeCatalog {
        ScopeCatalog {
            known_extensions: registrations
                .iter()
                .map(|registration| registration.extension_id.clone())
                .collect(),
            registrations,
        }
    }

    fn inherited_labels(
        inherited: &InheritedExtensionRegistrations,
    ) -> HashMap<String, &'static str> {
        inherited
            .registrations
            .iter()
            .map(|registration| {
                let instance = registration
                    .instance_factory
                    .produce()
                    .downcast::<SharedStateProbe>()
                    .expect("test factory should produce SharedStateProbe");
                (registration.extension_id.to_string(), instance.label)
            })
            .collect()
    }

    fn resolve_hierarchy_state(
        inherited: &InheritedExtensionRegistrations,
    ) -> Box<dyn HierarchyStateShared> {
        let mut registry = CapabilityRegistry::new();
        inherited
            .register_into(&mut registry)
            .expect("inherited capability should register");
        let bindings = HashMap::from([(
            CapabilityId::from("hierarchy_test_state"),
            ExtensionId::from("shared"),
        )]);
        let mut known_extensions = HashSet::new();
        inherited.extend_known_extensions(&mut known_extensions);
        let mut tracker = ConsumedTracker::new();
        let capabilities = resolve_bindings(&bindings, &registry, &known_extensions, &mut tracker)
            .expect("inherited capability should resolve");
        capabilities
            .require_shared::<HierarchyStateCapability>()
            .expect("inherited shared capability should be consumable")
    }

    fn resolve_hierarchy_state_as_local(
        inherited: &InheritedExtensionRegistrations,
    ) -> Box<dyn HierarchyStateLocal> {
        let mut registry = CapabilityRegistry::new();
        inherited
            .register_into(&mut registry)
            .expect("inherited capability should register");
        let bindings = HashMap::from([(
            CapabilityId::from("hierarchy_test_state"),
            ExtensionId::from("shared"),
        )]);
        let mut known_extensions = HashSet::new();
        inherited.extend_known_extensions(&mut known_extensions);
        let mut tracker = ConsumedTracker::new();
        let capabilities = resolve_bindings(&bindings, &registry, &known_extensions, &mut tracker)
            .expect("inherited capability should resolve");
        capabilities
            .require_local::<HierarchyStateCapability>()
            .expect("inherited shared capability should adapt to local")
    }

    /// Scenario: an engine-scoped cloned provider is distributed through multiple
    /// independently resolved descendant catalogs.
    /// Guarantees: every descendant clone retains the same Arc-backed root state
    /// created at the declaration scope.
    #[test]
    fn cloned_engine_provider_preserves_root_state_across_descendants() {
        let root_state = Arc::new(AtomicUsize::new(0));
        let registry = HierarchicalExtensionRegistry::default();
        registry
            .install(HierarchicalExtensionCatalog {
                engine: scope_catalog(vec![shared_registration(
                    "shared",
                    "engine",
                    Arc::clone(&root_state),
                )]),
                groups: HashMap::new(),
            })
            .expect("catalog should install");

        let group_a = registry.registrations_for_pipeline(
            &PipelineGroupId::from("a"),
            &PipelineExtensions::default(),
        );
        let group_b = registry.registrations_for_pipeline(
            &PipelineGroupId::from("b"),
            &PipelineExtensions::default(),
        );
        let a = resolve_hierarchy_state(&group_a);
        let b = resolve_hierarchy_state(&group_b);
        let local = resolve_hierarchy_state_as_local(&group_a);
        assert_eq!(a.increment(), 1);
        assert_eq!(b.increment(), 2);
        assert_eq!(local.increment(), 3);
        assert_eq!(root_state.load(Ordering::SeqCst), 3);
    }

    /// Scenario: an inherited provider uses constructed instance policy rather
    /// than cloning one configured prototype.
    /// Guarantees: each consumer receives independent mutable state even when
    /// the factory registration itself is cloned through hierarchy catalogs.
    #[test]
    fn constructed_inherited_provider_remains_fresh_per_consumer() {
        let invocations = Arc::new(AtomicUsize::new(0));
        let produced = Arc::clone(&invocations);
        let registration = SharedExtensionRegistration {
            extension_id: "constructed".into(),
            capabilities: test_capabilities(),
            instance_factory: SharedInstanceFactory::new(move || {
                let initial = produced.fetch_add(1, Ordering::SeqCst);
                Box::new(SharedStateProbe {
                    label: "constructed",
                    value: Arc::new(AtomicUsize::new(initial)),
                }) as Box<dyn Any + Send>
            }),
        };
        let first = registration
            .instance_factory
            .produce()
            .downcast::<SharedStateProbe>()
            .expect("constructed factory should produce SharedStateProbe");
        let second = registration
            .instance_factory
            .clone()
            .produce()
            .downcast::<SharedStateProbe>()
            .expect("cloned constructed factory should produce SharedStateProbe");

        assert!(!Arc::ptr_eq(&first.value, &second.value));
        assert_eq!(first.value.load(Ordering::SeqCst), 0);
        assert_eq!(second.value.load(Ordering::SeqCst), 1);
        assert_eq!(invocations.load(Ordering::SeqCst), 2);
    }

    /// Scenario: engine, group, sibling-group, and pipeline scopes reuse the same
    /// extension IDs.
    /// Guarantees: pipeline declarations shadow group and engine declarations,
    /// group declarations shadow engine declarations, and sibling declarations
    /// remain invisible.
    #[test]
    fn lexical_resolution_applies_shadowing_and_sibling_isolation() {
        let state = || Arc::new(AtomicUsize::new(0));
        let registry = HierarchicalExtensionRegistry::default();
        registry
            .install(HierarchicalExtensionCatalog {
                engine: scope_catalog(vec![
                    shared_registration("same", "engine", state()),
                    shared_registration("engine-only", "engine-only", state()),
                ]),
                groups: HashMap::from([
                    (
                        PipelineGroupId::from("a"),
                        scope_catalog(vec![
                            shared_registration("same", "group-a", state()),
                            shared_registration("group-only", "group-a-only", state()),
                        ]),
                    ),
                    (
                        PipelineGroupId::from("b"),
                        scope_catalog(vec![shared_registration(
                            "sibling-only",
                            "group-b-only",
                            state(),
                        )]),
                    ),
                ]),
            })
            .expect("catalog should install");

        let group_a = registry.registrations_for_pipeline(
            &PipelineGroupId::from("a"),
            &PipelineExtensions::default(),
        );
        let labels = inherited_labels(&group_a);
        assert_eq!(labels.get("same"), Some(&"group-a"));
        assert_eq!(labels.get("engine-only"), Some(&"engine-only"));
        assert_eq!(labels.get("group-only"), Some(&"group-a-only"));
        assert!(!labels.contains_key("sibling-only"));

        let mut pipeline_extensions = PipelineExtensions::default();
        pipeline_extensions.insert(
            "same".into(),
            ExtensionUserConfig::with_type("urn:test:extension:pipeline"),
        );
        let shadowed =
            registry.registrations_for_pipeline(&PipelineGroupId::from("a"), &pipeline_extensions);
        assert!(!inherited_labels(&shadowed).contains_key("same"));

        let group_b = registry.registrations_for_pipeline(
            &PipelineGroupId::from("b"),
            &PipelineExtensions::default(),
        );
        let labels = inherited_labels(&group_b);
        assert_eq!(labels.get("same"), Some(&"engine"));
        assert_eq!(labels.get("sibling-only"), Some(&"group-b-only"));
        assert!(!labels.contains_key("group-only"));
    }

    #[derive(Clone)]
    struct LifecycleProbe {
        name: &'static str,
        events: Arc<Mutex<Vec<String>>>,
        release: Option<Arc<Notify>>,
        runtime_failure: Option<Arc<Notify>>,
        signal_ready: bool,
        readiness_timeout: Duration,
    }

    static LIFECYCLE_PROBES: OnceLock<Mutex<HashMap<String, LifecycleProbe>>> = OnceLock::new();

    fn lifecycle_probes() -> &'static Mutex<HashMap<String, LifecycleProbe>> {
        LIFECYCLE_PROBES.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn register_lifecycle_probe(key: &str, probe: LifecycleProbe) {
        let _ = lifecycle_probes()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(key.to_owned(), probe);
    }

    fn lifecycle_probe(key: &str) -> LifecycleProbe {
        lifecycle_probes()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(key)
            .cloned()
            .unwrap_or_else(|| panic!("missing lifecycle probe `{key}`"))
    }

    fn push_event(events: &Mutex<Vec<String>>, event: String) {
        events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(event);
    }

    #[derive(Clone)]
    struct LifecycleExtension {
        probe: LifecycleProbe,
    }

    #[async_trait]
    impl extension::Extension for LifecycleExtension {
        async fn start(
            self: Box<Self>,
            mut control: extension::ControlChannel,
            effect_handler: crate::extension::EffectHandler,
        ) -> Result<TerminalState, Error> {
            push_event(&self.probe.events, format!("{}:start", self.probe.name));
            if let Some(release) = &self.probe.release {
                release.notified().await;
            }
            if self.probe.signal_ready {
                effect_handler.signal_ready();
                push_event(&self.probe.events, format!("{}:ready", self.probe.name));
            }
            if let Some(runtime_failure) = &self.probe.runtime_failure {
                runtime_failure.notified().await;
                return Err(Error::InternalError {
                    message: format!("{} requested runtime failure", self.probe.name),
                });
            }
            loop {
                match control.recv().await {
                    Ok(ExtensionControlMsg::Shutdown { .. }) | Err(_) => {
                        push_event(&self.probe.events, format!("{}:stop", self.probe.name));
                        return Ok(TerminalState::default());
                    }
                    Ok(_) => {}
                }
            }
        }
    }

    fn lifecycle_extension_create(
        _context: &ExtensionContext,
        name: ExtensionId,
        user_config: Arc<ExtensionUserConfig>,
        runtime_config: &ExtensionConfig,
    ) -> Result<crate::extension::ExtensionBundle, otel_arrow_dfe_config::error::Error> {
        let key = user_config
            .config
            .get("probe")
            .and_then(serde_json::Value::as_str)
            .expect("test extension config should contain a probe key");
        let probe = lifecycle_probe(key);
        Ok(ExtensionWrapper::builder(name, user_config, runtime_config)
            .background()
            .with_readiness_probe_timeout_override(probe.readiness_timeout)
            .shared(LifecycleExtension { probe })
            .build()
            .expect("test lifecycle extension should build"))
    }

    fn local_only_extension_create(
        _context: &ExtensionContext,
        _name: ExtensionId,
        _user_config: Arc<ExtensionUserConfig>,
        _runtime_config: &ExtensionConfig,
    ) -> Result<crate::extension::ExtensionBundle, otel_arrow_dfe_config::error::Error> {
        unreachable!("local-only metadata must be rejected before factory creation")
    }

    #[derive(Clone)]
    struct DualLocalExtension {
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait(?Send)]
    impl crate::local::extension::Extension for DualLocalExtension {
        async fn start(
            self: Rc<Self>,
            mut control: crate::local::extension::ControlChannel,
            _effect_handler: crate::extension::EffectHandler,
        ) -> Result<TerminalState, Error> {
            push_event(&self.events, "dual:local-start".to_owned());
            loop {
                match control.recv().await {
                    Ok(ExtensionControlMsg::Shutdown { .. }) | Err(_) => {
                        push_event(&self.events, "dual:local-stop".to_owned());
                        return Ok(TerminalState::default());
                    }
                    Ok(_) => {}
                }
            }
        }
    }

    fn dual_extension_create(
        _context: &ExtensionContext,
        name: ExtensionId,
        user_config: Arc<ExtensionUserConfig>,
        runtime_config: &ExtensionConfig,
    ) -> Result<crate::extension::ExtensionBundle, otel_arrow_dfe_config::error::Error> {
        let key = user_config
            .config
            .get("probe")
            .and_then(serde_json::Value::as_str)
            .expect("test extension config should contain a probe key");
        let probe = lifecycle_probe(key);
        Ok(ExtensionWrapper::builder(name, user_config, runtime_config)
            .active()
            .shared(LifecycleExtension {
                probe: probe.clone(),
            })
            .local(Rc::new(DualLocalExtension {
                events: probe.events,
            }))
            .build()
            .expect("dual hierarchy test extension should build"))
    }

    fn validate_test_extension_config(
        _config: &serde_json::Value,
    ) -> Result<(), otel_arrow_dfe_config::error::Error> {
        Ok(())
    }

    const LIFECYCLE_EXTENSION_FACTORY: ExtensionFactory = ExtensionFactory {
        name: LIFECYCLE_EXTENSION_URN,
        description: "hierarchy lifecycle test extension",
        documentation_url: "",
        capabilities: None,
        create: lifecycle_extension_create,
        validate_config: validate_test_extension_config,
    };

    const LOCAL_ONLY_EXTENSION_FACTORY: ExtensionFactory = ExtensionFactory {
        name: LOCAL_ONLY_EXTENSION_URN,
        description: "local-only hierarchy rejection test extension",
        documentation_url: "",
        capabilities: Some(ExtensionCapabilities {
            shared: &[],
            local: &["test"],
            register_shared: |_, _, _| Ok(()),
            register_local: |_, _, _| Ok(()),
        }),
        create: local_only_extension_create,
        validate_config: validate_test_extension_config,
    };

    const DUAL_EXTENSION_FACTORY: ExtensionFactory = ExtensionFactory {
        name: DUAL_EXTENSION_URN,
        description: "dual hierarchy variant selection test extension",
        documentation_url: "",
        capabilities: Some(ExtensionCapabilities {
            shared: &["test"],
            local: &["test"],
            register_shared: |_, _, _| Ok(()),
            register_local: |_, _, _| Ok(()),
        }),
        create: dual_extension_create,
        validate_config: validate_test_extension_config,
    };

    static HIERARCHY_TEST_PIPELINE_FACTORY: PipelineFactory<()> = PipelineFactory::new(
        &[],
        &[],
        &[],
        &[
            LIFECYCLE_EXTENSION_FACTORY,
            LOCAL_ONLY_EXTENSION_FACTORY,
            DUAL_EXTENSION_FACTORY,
        ],
    );

    async fn wait_for_event(events: &Mutex<Vec<String>>, expected: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .any(|event| event == expected)
                {
                    return;
                }
                task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for event `{expected}`"));
    }

    fn test_host_inputs(
        config: &OtelDataflowSpec,
    ) -> (
        PreparedHierarchicalExtensionHost,
        flume::Receiver<otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot>,
    ) {
        let controller_context = ControllerContext::new(TelemetryRegistryHandle::new());
        let (metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(128);
        let prepared = HIERARCHY_TEST_PIPELINE_FACTORY
            .prepare_hierarchical_extensions(
                config,
                &controller_context,
                metrics_reporter,
                HierarchicalExtensionRegistry::default(),
            )
            .expect("hierarchical host should prepare");
        (prepared, metrics_rx)
    }

    /// Scenario: engine and two group extensions opt into readiness, with the
    /// engine and both groups initially blocked on independent release gates.
    /// Guarantees: the engine readiness barrier completes before either group
    /// starts, peer groups start concurrently, and shutdown stops all groups
    /// before the engine scope.
    #[test]
    fn lifecycle_uses_level_readiness_and_reverse_shutdown_barriers() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let local = LocalSet::new();
        runtime.block_on(local.run_until(async {
            let events = Arc::new(Mutex::new(Vec::new()));
            let engine_release = Arc::new(Notify::new());
            let group_a_release = Arc::new(Notify::new());
            let group_b_release = Arc::new(Notify::new());
            for (key, name, release) in [
                ("barrier-engine", "engine", Arc::clone(&engine_release)),
                ("barrier-group-a", "group-a", Arc::clone(&group_a_release)),
                ("barrier-group-b", "group-b", Arc::clone(&group_b_release)),
            ] {
                register_lifecycle_probe(
                    key,
                    LifecycleProbe {
                        name,
                        events: Arc::clone(&events),
                        release: Some(release),
                        runtime_failure: None,
                        signal_ready: true,
                        readiness_timeout: Duration::from_secs(1),
                    },
                );
            }
            let config = OtelDataflowSpec::from_yaml(
                r#"
version: otel_dataflow/v1
extensions:
  root:
    type: urn:test:extension:hierarchy_lifecycle
    config:
      probe: barrier-engine
groups:
  a:
    extensions:
      scoped:
        type: urn:test:extension:hierarchy_lifecycle
        config:
          probe: barrier-group-a
  b:
    extensions:
      scoped:
        type: urn:test:extension:hierarchy_lifecycle
        config:
          probe: barrier-group-b
"#,
            )
            .expect("hierarchical config should parse");
            let (prepared, _metrics_rx) = test_host_inputs(&config);
            let start_task = task::spawn_local(prepared.start());

            wait_for_event(&events, "engine:start").await;
            {
                let current = events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                assert!(!current.iter().any(|event| event.starts_with("group-")));
            }

            engine_release.notify_one();
            wait_for_event(&events, "group-a:start").await;
            wait_for_event(&events, "group-b:start").await;
            group_a_release.notify_one();
            group_b_release.notify_one();
            let running = start_task
                .await
                .expect("host startup task should not panic")
                .expect("all hierarchy scopes should become ready");

            let run_task = task::spawn_local(running.run(async {}, |_| {}));
            run_task
                .await
                .expect("host run task should not panic")
                .expect("host should shut down cleanly");

            let current = events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let position = |event: &str| {
                current
                    .iter()
                    .position(|candidate| candidate == event)
                    .unwrap_or_else(|| panic!("missing lifecycle event `{event}`"))
            };
            assert!(position("engine:ready") < position("group-a:start"));
            assert!(position("engine:ready") < position("group-b:start"));
            assert!(position("group-a:stop") < position("engine:stop"));
            assert!(position("group-b:stop") < position("engine:stop"));
        }));
    }

    /// Scenario: an engine-scoped extension opts into readiness but never
    /// signals before its configured timeout.
    /// Guarantees: startup reports the standard readiness-timeout error and no
    /// group extension is started past the failed engine-level barrier.
    #[test]
    fn engine_readiness_timeout_blocks_group_startup() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let local = LocalSet::new();
        runtime.block_on(local.run_until(async {
            let events = Arc::new(Mutex::new(Vec::new()));
            register_lifecycle_probe(
                "timeout-engine",
                LifecycleProbe {
                    name: "engine-timeout",
                    events: Arc::clone(&events),
                    release: None,
                    runtime_failure: None,
                    signal_ready: false,
                    readiness_timeout: Duration::from_millis(20),
                },
            );
            register_lifecycle_probe(
                "timeout-group",
                LifecycleProbe {
                    name: "group-after-timeout",
                    events: Arc::clone(&events),
                    release: None,
                    runtime_failure: None,
                    signal_ready: true,
                    readiness_timeout: Duration::from_secs(1),
                },
            );
            let config = OtelDataflowSpec::from_yaml(
                r#"
version: otel_dataflow/v1
extensions:
  root:
    type: urn:test:extension:hierarchy_lifecycle
    config:
      probe: timeout-engine
groups:
  a:
    extensions:
      scoped:
        type: urn:test:extension:hierarchy_lifecycle
        config:
          probe: timeout-group
"#,
            )
            .expect("hierarchical config should parse");
            let (prepared, _metrics_rx) = test_host_inputs(&config);
            let error = match prepared.start().await {
                Ok(_) => panic!("engine readiness timeout should fail startup"),
                Err(error) => error,
            };

            assert!(matches!(error, Error::ExtensionReadinessTimeout { .. }));
            let current = events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(current.iter().any(|event| event == "engine-timeout:start"));
            assert!(
                !current
                    .iter()
                    .any(|event| event == "group-after-timeout:start")
            );
        }));
    }

    /// Scenario: a local-only extension factory is declared at engine scope.
    /// Guarantees: hierarchy preparation rejects the declaration before calling
    /// its factory because ancestor scopes can distribute only shared variants.
    #[test]
    fn ancestor_scope_rejects_local_only_extension() {
        let config = OtelDataflowSpec::from_yaml(
            r#"
version: otel_dataflow/v1
extensions:
  local:
    type: urn:test:extension:hierarchy_local_only
"#,
        )
        .expect("structural config should parse");
        let controller_context = ControllerContext::new(TelemetryRegistryHandle::new());
        let (_metrics_rx, metrics_reporter) = MetricsReporter::create_new_and_receiver(8);
        let result = HIERARCHY_TEST_PIPELINE_FACTORY.prepare_hierarchical_extensions(
            &config,
            &controller_context,
            metrics_reporter,
            HierarchicalExtensionRegistry::default(),
        );

        assert!(matches!(
            result,
            Err(Error::HierarchicalExtensionRequiresShared { .. })
        ));
    }

    /// Scenario: a factory that provides both local and shared variants is
    /// declared at engine scope.
    /// Guarantees: the hierarchy host runs and stops only the shared variant;
    /// the local variant is discarded before lifecycle spawning.
    #[test]
    fn ancestor_scope_retains_only_shared_variant_from_dual_bundle() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let local = LocalSet::new();
        runtime.block_on(local.run_until(async {
            let events = Arc::new(Mutex::new(Vec::new()));
            register_lifecycle_probe(
                "dual-root",
                LifecycleProbe {
                    name: "dual-shared",
                    events: Arc::clone(&events),
                    release: None,
                    runtime_failure: None,
                    signal_ready: false,
                    readiness_timeout: Duration::from_secs(1),
                },
            );
            let config = OtelDataflowSpec::from_yaml(
                r#"
version: otel_dataflow/v1
extensions:
  dual:
    type: urn:test:extension:hierarchy_dual
    config:
      probe: dual-root
"#,
            )
            .expect("hierarchical config should parse");
            let (prepared, _metrics_rx) = test_host_inputs(&config);
            let running = prepared.start().await.expect("shared variant should start");
            running
                .run(async {}, |_| {})
                .await
                .expect("shared variant should stop cleanly");

            let current = events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(current.iter().any(|event| event == "dual-shared:start"));
            assert!(current.iter().any(|event| event == "dual-shared:stop"));
            assert!(!current.iter().any(|event| event == "dual:local-start"));
            assert!(!current.iter().any(|event| event == "dual:local-stop"));
        }));
    }

    /// Scenario: one group extension fails after all hierarchy levels have
    /// passed readiness while an engine provider remains healthy.
    /// Guarantees: the failure callback fires before the surviving engine
    /// scope stops, and that provider remains alive until controller
    /// cancellation represents completed descendant-pipeline teardown.
    #[test]
    fn runtime_failure_keeps_surviving_ancestors_alive_until_cancellation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should build");
        let local = LocalSet::new();
        runtime.block_on(local.run_until(async {
            let events = Arc::new(Mutex::new(Vec::new()));
            let group_failure = Arc::new(Notify::new());
            register_lifecycle_probe(
                "runtime-engine",
                LifecycleProbe {
                    name: "runtime-engine",
                    events: Arc::clone(&events),
                    release: None,
                    runtime_failure: None,
                    signal_ready: true,
                    readiness_timeout: Duration::from_secs(1),
                },
            );
            register_lifecycle_probe(
                "runtime-group",
                LifecycleProbe {
                    name: "runtime-group",
                    events: Arc::clone(&events),
                    release: None,
                    runtime_failure: Some(Arc::clone(&group_failure)),
                    signal_ready: true,
                    readiness_timeout: Duration::from_secs(1),
                },
            );
            let config = OtelDataflowSpec::from_yaml(
                r#"
version: otel_dataflow/v1
extensions:
  root:
    type: urn:test:extension:hierarchy_lifecycle
    config:
      probe: runtime-engine
groups:
  a:
    extensions:
      scoped:
        type: urn:test:extension:hierarchy_lifecycle
        config:
          probe: runtime-group
"#,
            )
            .expect("hierarchical config should parse");
            let (prepared, _metrics_rx) = test_host_inputs(&config);
            let running = prepared
                .start()
                .await
                .expect("hierarchy should pass readiness");
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let failure_events = Arc::clone(&events);
            let run_task = task::spawn_local(running.run(
                async move {
                    let _ = shutdown_rx.await;
                },
                move |_| {
                    push_event(&failure_events, "host:failure-notified".to_owned());
                },
            ));

            group_failure.notify_one();
            wait_for_event(&events, "host:failure-notified").await;
            assert!(
                !events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .iter()
                    .any(|event| event == "runtime-engine:stop")
            );

            let _ = shutdown_tx.send(());
            let error = run_task
                .await
                .expect("hierarchy run task should not panic")
                .expect_err("group runtime failure should be returned");
            assert!(matches!(error, Error::InternalError { .. }));

            let current = events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let notified = current
                .iter()
                .position(|event| event == "host:failure-notified")
                .expect("failure callback event should exist");
            let engine_stop = current
                .iter()
                .position(|event| event == "runtime-engine:stop")
                .expect("engine stop event should exist");
            assert!(notified < engine_stop);
        }));
    }
}
