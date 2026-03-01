// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension registry — true single instance shared via `Arc<Mutex>`.
//!
//! # Architecture
//!
//! The extension system uses a two-phase approach:
//!
//! 1. **Build phase** ([`ExtensionRegistryBuilder`]): Accumulates extension
//!    trait objects from registrar closures. Each extension is boxed as
//!    `Box<dyn Trait>` and type-erased as `Box<dyn Any + Send>`. The builder
//!    is `Send` so it can cross thread boundaries.
//!
//! 2. **Run phase** ([`ExtensionRegistry`]): An `Arc`-backed, cheaply `Clone`-able
//!    registry. All components share the **same** extension instances — true
//!    single instance, no cloning. Access is via [`ExtensionRegistry::with_extension`],
//!    which acquires a `parking_lot::Mutex` lock and passes `&T` to a closure.
//!
//! # Mutex-based access
//!
//! Access uses `parking_lot::Mutex` — a fast, uncontended lock (~2-3ns on
//! single-threaded runtimes). The closure-based API ensures the lock is held
//! for the minimum duration and automatically released.
//!
//! - [`with_extension`](ExtensionRegistry::with_extension) acquires the lock,
//!   downcasts to `&T`, calls your closure, and releases the lock.
//! - The lock is **not** held across `.await` — extract owned values (futures,
//!   receivers, etc.) from the closure and await them outside.
//! - Re-entrant access to *different* extensions is fine (separate mutex per
//!   entry). Accessing the *same* extension while already holding its lock
//!   will return `Err(AlreadyBorrowed)` (via `try_lock`).
//!
//! # Why Mutex instead of lock-free?
//!
//! `Arc` requires `T: Sync` to be `Send`. Extension trait objects are
//! `dyn Any + Send` (not `Sync`). A lock-free design would require
//! `unsafe impl Sync` — which is fragile if the registry is ever passed to
//! `tokio::spawn`, tonic, or any multi-threaded context. The `parking_lot`
//! Mutex is effectively zero-cost on single-threaded runtimes and remains
//! sound regardless of runtime configuration.
//!
//! # Interior mutability
//!
//! Extensions are accessed via `&T` inside the closure. For mutation,
//! extensions can use `Cell`, `RefCell`, or other single-threaded interior
//! mutability primitives — `parking_lot::Mutex` only requires `T: Send`
//! (not `T: Sync`), and `Cell<T>`/`RefCell<T>` are `Send` when `T: Send`.
//!
//! # Type Erasure
//!
//! Trait objects are double-boxed:
//! - Inner: `Box<dyn BearerTokenProvider>` (the actual trait object)
//! - Outer: `Box<dyn Any + Send>` (type-erased storage)
//!
//! The `TypeId` key is `TypeId::of::<Box<dyn Trait>>()` — note that `Box<dyn Trait>`
//! is `Sized`, so it has a valid `TypeId` and supports `Any::downcast_ref`.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use thiserror::Error;

// ── Error Types ──────────────────────────────────────────────────────────────

/// Errors that can occur when retrieving extensions from the registry.
#[derive(Debug, Error, Clone)]
pub enum ExtensionError {
    /// The named extension was not found in the registry.
    #[error("extension '{name}' not found")]
    NotFound {
        /// Name of the extension.
        name: String,
    },

    /// The extension exists but does not expose the requested trait.
    #[error("extension '{name}' does not implement trait '{expected}'")]
    TraitNotImplemented {
        /// Name of the extension.
        name: String,
        /// Expected trait name.
        expected: &'static str,
    },

    /// The extension is currently borrowed by another caller.
    ///
    /// This happens when `with_extension` is called re-entrantly for the
    /// *same* (name, trait) pair. Access to *different* extensions or
    /// different traits of the same extension is always fine.
    #[error("extension '{name}' is already borrowed")]
    AlreadyBorrowed {
        /// Name of the extension.
        name: String,
    },
}

// ── ExtensionRegistrar ───────────────────────────────────────────────────────

/// A closure produced by the [`extension_traits!`] macro that registers an
/// extension's trait objects with the [`ExtensionRegistryBuilder`].
///
/// Called once per extension during pipeline build. The closure captures the
/// concrete extension instance and registers boxed trait objects for each trait
/// the extension exposes.
///
/// Must be `Send` because the extension instance is created on the main thread
/// and consumed on the worker thread.
pub type ExtensionRegistrar = Box<dyn FnOnce(&mut ExtensionRegistryBuilder, &str) + Send>;

// ── ExtensionRegistryBuilder ─────────────────────────────────────────────────

/// Accumulates extension trait objects during pipeline build.
///
/// Each extension contributes one or more trait objects via its registrar closure
/// (produced by the [`extension_traits!`] macro). Call [`build`](Self::build) to
/// create a shared [`ExtensionRegistry`] backed by `Arc`.
///
/// # Example
///
/// ```ignore
/// let mut builder = ExtensionRegistryBuilder::new();
/// for ext in &mut extensions {
///     let name = ext.node_id().name.to_string();
///     if let Some(registrar) = ext.take_registrar() {
///         registrar(&mut builder, &name);
///     }
/// }
/// // Build once — all components share the same registry (Arc clone):
/// let registry = builder.build();
/// let reg_for_exporter = registry.clone(); // cheap Arc clone
/// ```
pub struct ExtensionRegistryBuilder {
    entries: HashMap<(String, TypeId), Box<dyn Any + Send>>,
}

impl Default for ExtensionRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtensionRegistryBuilder {
    /// Creates a new empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers a trait object for the given extension name and trait type.
    ///
    /// This is called by the [`extension_traits!`] macro. Each call registers
    /// one (name, trait) pair. Multiple traits per extension result in multiple
    /// calls with the same `name` but different `trait_type_id`.
    ///
    /// # Arguments
    ///
    /// * `name` — Extension instance name (e.g., "azure_identity_auth")
    /// * `trait_type_id` — `TypeId::of::<Box<dyn Trait>>()` identifying the trait
    /// * `instance` — `Box<Box<dyn Trait>>` erased as `Box<dyn Any + Send>`
    pub fn register(
        &mut self,
        name: &str,
        trait_type_id: TypeId,
        instance: Box<dyn Any + Send>,
    ) {
        let _ = self
            .entries
            .insert((name.to_string(), trait_type_id), instance);
    }

    /// Creates a shared [`ExtensionRegistry`] backed by `Arc<Mutex>`.
    ///
    /// Each entry is wrapped in its own `Mutex` so different extensions can
    /// be accessed concurrently without contention.
    ///
    /// Consumes the builder since the data is moved into the registry.
    #[must_use]
    pub fn build(self) -> ExtensionRegistry {
        let entries = self
            .entries
            .into_iter()
            .map(|(key, boxed)| (key, Mutex::new(boxed)))
            .collect();
        ExtensionRegistry {
            entries: Arc::new(entries),
        }
    }

    /// Returns `true` if no extensions have been registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of unique extension names registered.
    #[must_use]
    pub fn extension_count(&self) -> usize {
        let mut names: Vec<&str> = self.entries.keys().map(|(n, _)| n.as_str()).collect();
        names.sort();
        names.dedup();
        names.len()
    }

    /// Returns an iterator over unique extension names.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        let mut names: Vec<&str> = self.entries.keys().map(|(n, _)| n.as_str()).collect();
        names.sort();
        names.dedup();
        names.into_iter()
    }
}

impl std::fmt::Debug for ExtensionRegistryBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.names().collect();
        f.debug_struct("ExtensionRegistryBuilder")
            .field("extensions", &names)
            .finish()
    }
}

// ── ExtensionRegistry ────────────────────────────────────────────────────────

/// Shared extension registry — true single instance across all components.
///
/// Created by [`ExtensionRegistryBuilder::build`] and cheaply cloned via `Arc`.
/// All components share the same extension instances. Access is via
/// [`with_extension`](Self::with_extension), which acquires a per-entry
/// `parking_lot::Mutex` lock and passes `&T` to a closure.
///
/// # Usage pattern
///
/// Extract owned values from the closure — don't hold the lock across `.await`:
///
/// ```ignore
/// // Get an owned future (lock released before await):
/// let token_future = extension_registry.with_extension::<dyn BearerTokenProvider, _>(
///     "azure_identity_auth",
///     |auth| auth.get_token(),
/// )?;
/// let token = token_future.await?;
///
/// // Get an owned subscription handle:
/// let mut token_rx = extension_registry.with_extension::<dyn BearerTokenProvider, _>(
///     "azure_identity_auth",
///     |auth| auth.subscribe_token_refresh(),
/// )?;
/// ```
///
/// # Concurrency
///
/// Each (name, trait) pair has its own `Mutex`. Accessing different extensions
/// or different traits of the same extension is never blocked. Re-entrant
/// access to the *same* (name, trait) pair returns `Err(AlreadyBorrowed)`.
#[derive(Clone)]
pub struct ExtensionRegistry {
    /// `(extension_name, TypeId::of::<Box<dyn Trait>>())` → `Mutex<Box<dyn Any + Send>>`
    ///
    /// The `Box<dyn Any + Send>` contains a `Box<dyn Trait>` that can be recovered
    /// via `downcast_ref::<Box<dyn Trait>>()`.
    ///
    /// Each entry has its own `Mutex` for fine-grained locking.
    entries: Arc<HashMap<(String, TypeId), Mutex<Box<dyn Any + Send>>>>,
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtensionRegistry {
    /// Creates a new empty registry.
    ///
    /// Prefer using [`ExtensionRegistryBuilder::build`] to create registries
    /// from registered extensions.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Arc::new(HashMap::new()),
        }
    }

    /// Access an extension trait by name via a scoped closure.
    ///
    /// Acquires the per-entry `Mutex` lock (via `try_lock` to prevent
    /// deadlocks), downcasts to `&T`, calls the closure, and releases
    /// the lock. The closure receives `&T` and can extract owned values
    /// (futures, receivers, cloned data, etc.).
    ///
    /// # Important
    ///
    /// Do **not** `.await` inside the closure — the lock would be held
    /// across the await point. Instead, extract an owned future and await
    /// it outside:
    ///
    /// ```ignore
    /// let future = registry.with_extension::<dyn MyTrait, _>("name", |ext| {
    ///     ext.get_async_result()  // returns BoxFuture<'static, ...>
    /// })?;
    /// let result = future.await?;  // lock already released
    /// ```
    ///
    /// # Type Parameter
    ///
    /// `T` is the **trait object type**, e.g., `dyn BearerTokenProvider`. The trait
    /// must be `'static` (required for `TypeId`).
    ///
    /// # Errors
    ///
    /// Returns [`ExtensionError::NotFound`] if no extension with that name exists.
    /// Returns [`ExtensionError::TraitNotImplemented`] if the extension exists
    /// but does not expose the requested trait.
    /// Returns [`ExtensionError::AlreadyBorrowed`] if the same (name, trait)
    /// pair is already locked (re-entrant access).
    pub fn with_extension<T: ?Sized + 'static, R>(
        &self,
        name: &str,
        f: impl FnOnce(&T) -> R,
    ) -> Result<R, ExtensionError> {
        let key = (name.to_string(), TypeId::of::<Box<T>>());
        let mutex = self.entries.get(&key).ok_or_else(|| {
            let has_any = self.entries.keys().any(|(n, _)| n == name);
            if has_any {
                ExtensionError::TraitNotImplemented {
                    name: name.to_string(),
                    expected: std::any::type_name::<T>(),
                }
            } else {
                ExtensionError::NotFound {
                    name: name.to_string(),
                }
            }
        })?;

        let guard = mutex.try_lock().ok_or_else(|| ExtensionError::AlreadyBorrowed {
            name: name.to_string(),
        })?;

        let boxed_trait: &Box<T> = guard
            .downcast_ref::<Box<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");

        Ok(f(boxed_trait.as_ref()))
    }

    /// Check if an extension exists by name (regardless of trait).
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.entries.keys().any(|(n, _)| n == name)
    }

    /// Returns the number of unique extension names in the registry.
    #[must_use]
    pub fn extension_count(&self) -> usize {
        let mut names: Vec<&String> = self.entries.keys().map(|(n, _)| n).collect();
        names.sort();
        names.dedup();
        names.len()
    }

    /// Returns `true` if no extensions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns an iterator over unique extension names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        let mut names: Vec<&String> = self.entries.keys().map(|(n, _)| n).collect();
        names.sort();
        names.dedup();
        names.into_iter()
    }
}

impl std::fmt::Debug for ExtensionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&String> = self.names().collect();
        f.debug_struct("ExtensionRegistry")
            .field("extensions", &names)
            .finish()
    }
}

// ── Registration Macros ──────────────────────────────────────────────────────

/// Creates a registrar closure that registers extension trait objects.
///
/// The macro captures the concrete extension instance and registers boxed
/// trait objects for each listed trait. The instance is cloned once per trait
/// (to produce independent trait objects for multi-trait extensions).
///
/// The instance type must implement `Clone + Send + 'static` and all listed traits.
///
/// # Syntax
///
/// ```ignore
/// let registrar = extension_traits!(instance => Trait1, Trait2);
/// ```
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::extensions::BearerTokenProvider;
///
/// let registrar = extension_traits!(my_auth_extension =>
///     BearerTokenProvider,
/// );
/// ```
///
/// # Compile-Time Checks
///
/// The macro verifies at compile time that each trait is `Send + 'static` and
/// that the concrete type implements the trait.
#[macro_export]
macro_rules! extension_traits {
    ($instance:expr => $($trait:path),* $(,)?) => {{
        let __instance = $instance;
        let __registrar: $crate::extensions::ExtensionRegistrar = Box::new({
            move |builder: &mut $crate::extensions::ExtensionRegistryBuilder, name: &str| {
                $(
                    {
                        // Compile-time check: the trait must be Send + 'static
                        const _: fn() = || {
                            fn assert_send_static<T: ?Sized + Send + 'static>() {}
                            assert_send_static::<dyn $trait>();
                        };

                        // Clone the instance for this trait registration
                        let __inst = __instance.clone();
                        let trait_obj: Box<dyn $trait> = Box::new(__inst);
                        builder.register(
                            name,
                            std::any::TypeId::of::<Box<dyn $trait>>(),
                            Box::new(trait_obj) as Box<dyn std::any::Any + Send>,
                        );
                    }
                )*
            }
        });
        __registrar
    }};
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::BoxFuture;
    use tokio::sync::watch;

    // ── Test trait ───────────────────────────────────────────────────────

    trait TestCapability: Send + 'static {
        fn get_value(&self) -> i32;
    }

    trait AnotherCapability: Send + 'static {
        fn get_name(&self) -> String;
    }

    // ── Test extension ──────────────────────────────────────────────────

    #[derive(Clone)]
    struct TestExtension {
        value: i32,
        name: String,
    }

    impl TestCapability for TestExtension {
        fn get_value(&self) -> i32 {
            self.value
        }
    }

    impl AnotherCapability for TestExtension {
        fn get_name(&self) -> String {
            self.name.clone()
        }
    }

    // ── Basic tests ─────────────────────────────────────────────────────

    #[test]
    fn test_empty_builder() {
        let builder = ExtensionRegistryBuilder::new();
        assert!(builder.is_empty());
        assert_eq!(builder.extension_count(), 0);

        let registry = builder.build();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_register_and_retrieve_single_trait() {
        let ext = TestExtension {
            value: 42,
            name: "test".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "my_ext");

        assert_eq!(builder.extension_count(), 1);

        let registry = builder.build();
        let value = registry
            .with_extension::<dyn TestCapability, _>("my_ext", |ext| ext.get_value())
            .unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn test_register_multiple_traits() {
        let ext = TestExtension {
            value: 99,
            name: "hello".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability, AnotherCapability);
        registrar(&mut builder, "multi_ext");

        let registry = builder.build();
        let value = registry
            .with_extension::<dyn TestCapability, _>("multi_ext", |ext| ext.get_value())
            .unwrap();
        let name = registry
            .with_extension::<dyn AnotherCapability, _>("multi_ext", |ext| ext.get_name())
            .unwrap();
        assert_eq!(value, 99);
        assert_eq!(name, "hello");
    }

    #[test]
    fn test_not_found_error() {
        let registry = ExtensionRegistry::new();
        let result = registry.with_extension::<dyn TestCapability, _>("nonexistent", |_| ());
        assert!(matches!(
            result,
            Err(ExtensionError::NotFound { name }) if name == "nonexistent"
        ));
    }

    #[test]
    fn test_trait_not_implemented_error() {
        let ext = TestExtension {
            value: 1,
            name: "x".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "partial_ext");

        let registry = builder.build();
        let result =
            registry.with_extension::<dyn AnotherCapability, _>("partial_ext", |_| ());
        assert!(matches!(
            result,
            Err(ExtensionError::TraitNotImplemented { name, .. }) if name == "partial_ext"
        ));
    }

    #[test]
    fn test_shared_instance_via_clone() {
        // All clones share the same extension instances (Arc-backed).
        let ext = TestExtension {
            value: 10,
            name: "shared".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();
        let clone1 = registry.clone();
        let clone2 = registry.clone();

        // All clones access the same instance
        let v1 = clone1
            .with_extension::<dyn TestCapability, _>("ext", |ext| ext.get_value())
            .unwrap();
        let v2 = clone2
            .with_extension::<dyn TestCapability, _>("ext", |ext| ext.get_value())
            .unwrap();
        assert_eq!(v1, 10);
        assert_eq!(v2, 10);

        // Verify they share the same Arc
        assert!(Arc::ptr_eq(&registry.entries, &clone1.entries));
        assert!(Arc::ptr_eq(&registry.entries, &clone2.entries));
    }

    #[test]
    fn test_multiple_extensions() {
        let auth = TestExtension {
            value: 1,
            name: "auth".to_string(),
        };
        let rate = TestExtension {
            value: 2,
            name: "rate".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let r1 = extension_traits!(auth => TestCapability, AnotherCapability);
        r1(&mut builder, "auth");
        let r2 = extension_traits!(rate => TestCapability, AnotherCapability);
        r2(&mut builder, "rate");

        assert_eq!(builder.extension_count(), 2);

        let registry = builder.build();
        let auth_val = registry
            .with_extension::<dyn TestCapability, _>("auth", |ext| ext.get_value())
            .unwrap();
        let rate_val = registry
            .with_extension::<dyn TestCapability, _>("rate", |ext| ext.get_value())
            .unwrap();
        assert_eq!(auth_val, 1);
        assert_eq!(rate_val, 2);
    }

    #[test]
    fn test_contains() {
        let ext = TestExtension {
            value: 0,
            name: "".to_string(),
        };
        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "exists");

        let registry = builder.build();
        assert!(registry.contains("exists"));
        assert!(!registry.contains("nope"));
    }

    #[test]
    fn test_extension_count() {
        let ext = TestExtension {
            value: 0,
            name: "".to_string(),
        };
        let mut builder = ExtensionRegistryBuilder::new();
        let r1 = extension_traits!(ext.clone() => TestCapability, AnotherCapability);
        r1(&mut builder, "a");
        let r2 = extension_traits!(ext => TestCapability);
        r2(&mut builder, "b");

        let registry = builder.build();
        assert_eq!(registry.extension_count(), 2);
    }

    #[test]
    fn test_debug_formatting() {
        let ext = TestExtension {
            value: 0,
            name: "".to_string(),
        };
        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "debug_ext");

        let debug = format!("{:?}", builder);
        assert!(debug.contains("debug_ext"));

        let registry = builder.build();
        let debug = format!("{:?}", registry);
        assert!(debug.contains("debug_ext"));
    }

    // ── Send / Clone assertions ─────────────────────────────────────────

    #[test]
    fn test_builder_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ExtensionRegistryBuilder>();
    }

    #[test]
    fn test_registry_is_send_and_clone() {
        fn assert_send_clone<T: Send + Clone>() {}
        assert_send_clone::<ExtensionRegistry>();
    }

    #[test]
    fn test_registrar_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ExtensionRegistrar>();
    }

    // ── Sequential access (no deadlock) ─────────────────────────────────

    #[test]
    fn test_sequential_access_same_extension() {
        let ext = TestExtension {
            value: 42,
            name: "multi".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();

        // Sequential access is fine — lock released between calls
        let v1 = registry
            .with_extension::<dyn TestCapability, _>("ext", |ext| ext.get_value())
            .unwrap();
        let v2 = registry
            .with_extension::<dyn TestCapability, _>("ext", |ext| ext.get_value())
            .unwrap();
        assert_eq!(v1, 42);
        assert_eq!(v2, 42);
    }

    #[test]
    fn test_different_traits_same_extension_concurrent() {
        // Different traits of the same extension have separate mutexes
        let ext = TestExtension {
            value: 42,
            name: "test".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability, AnotherCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();

        // Access different traits — each has its own mutex, no contention
        let value = registry
            .with_extension::<dyn TestCapability, _>("ext", |ext| ext.get_value())
            .unwrap();
        let name = registry
            .with_extension::<dyn AnotherCapability, _>("ext", |ext| ext.get_name())
            .unwrap();
        assert_eq!(value, 42);
        assert_eq!(name, "test");
    }

    // ── Async trait pattern test ────────────────────────────────────────

    trait AsyncCapability: Send + 'static {
        fn do_work(
            &self,
        ) -> BoxFuture<'static, Result<String, Box<dyn std::error::Error + Send + Sync>>>;
    }

    #[derive(Clone)]
    struct AsyncExtension {
        prefix: String,
    }

    impl AsyncCapability for AsyncExtension {
        fn do_work(
            &self,
        ) -> BoxFuture<'static, Result<String, Box<dyn std::error::Error + Send + Sync>>>
        {
            let prefix = self.prefix.clone();
            Box::pin(async move { Ok(format!("{prefix}_done")) })
        }
    }

    #[tokio::test]
    async fn test_async_extension_pattern() {
        let ext = AsyncExtension {
            prefix: "test".to_string(),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => AsyncCapability);
        registrar(&mut builder, "async_ext");

        let registry = builder.build();

        // Get the future inside the closure, await outside (lock released)
        let fut = registry
            .with_extension::<dyn AsyncCapability, _>("async_ext", |ext| ext.do_work())
            .unwrap();
        let result = fut.await.unwrap();
        assert_eq!(result, "test_done");
    }

    // ── Reactive subscription pattern test ──────────────────────────────

    trait Subscribable: Send + 'static {
        fn subscribe(&self) -> watch::Receiver<i32>;
    }

    #[derive(Clone)]
    struct WatchExtension {
        tx: watch::Sender<i32>,
    }

    impl Subscribable for WatchExtension {
        fn subscribe(&self) -> watch::Receiver<i32> {
            self.tx.subscribe()
        }
    }

    #[tokio::test]
    async fn test_subscription_pattern() {
        let (tx, _rx) = watch::channel(0);
        let ext = WatchExtension { tx: tx.clone() };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => Subscribable);
        registrar(&mut builder, "watch_ext");

        let registry = builder.build();

        // Get subscription handle inside closure, use outside
        let mut rx = registry
            .with_extension::<dyn Subscribable, _>("watch_ext", |ext| ext.subscribe())
            .unwrap();

        // Send a value and verify receipt
        tx.send(42).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 42);
    }

    // ── Clone shares state ──────────────────────────────────────────────

    #[test]
    fn test_clone_shares_same_arc() {
        let ext = TestExtension {
            value: 0,
            name: "".to_string(),
        };
        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();
        let clone = registry.clone();

        // Both point to the same Arc
        assert!(Arc::ptr_eq(&registry.entries, &clone.entries));
    }

    // ── Re-entrant access returns AlreadyBorrowed ───────────────────────

    #[test]
    fn test_reentrant_access_returns_error() {
        // Re-entrant access to the SAME (name, trait) pair returns AlreadyBorrowed
        trait ReentrantCapability: Send + 'static {
            fn try_reenter(&self, registry: &ExtensionRegistry) -> Result<(), ExtensionError>;
        }

        #[derive(Clone)]
        struct ReentrantExt;

        impl ReentrantCapability for ReentrantExt {
            fn try_reenter(&self, registry: &ExtensionRegistry) -> Result<(), ExtensionError> {
                // Try to access the same extension while it's already locked
                registry.with_extension::<dyn ReentrantCapability, _>("ext", |_| ())
            }
        }

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ReentrantExt => ReentrantCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();

        // The outer call succeeds, but the inner re-entrant call returns AlreadyBorrowed
        let result = registry
            .with_extension::<dyn ReentrantCapability, _>("ext", |ext| {
                ext.try_reenter(&registry)
            })
            .unwrap();
        assert!(matches!(result, Err(ExtensionError::AlreadyBorrowed { .. })));
    }

    // ── Interior mutability with Cell ──────────────────────────────────

    trait Counter: Send + 'static {
        fn increment(&self);
        fn count(&self) -> u64;
    }

    #[derive(Clone)]
    struct CellCounter {
        count: std::cell::Cell<u64>,
    }

    impl Counter for CellCounter {
        fn increment(&self) {
            self.count.set(self.count.get() + 1);
        }

        fn count(&self) -> u64 {
            self.count.get()
        }
    }

    #[test]
    fn test_interior_mutability_with_cell() {
        let ext = CellCounter {
            count: std::cell::Cell::new(0),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => Counter);
        registrar(&mut builder, "counter");

        let registry = builder.build();

        // Mutate through shared reference — Cell works because Mutex
        // only requires T: Send (not T: Sync), and Cell<u64> is Send.
        registry
            .with_extension::<dyn Counter, _>("counter", |c| {
                assert_eq!(c.count(), 0);
                c.increment();
                c.increment();
                c.increment();
                assert_eq!(c.count(), 3);
            })
            .unwrap();

        // State persists across calls
        let count = registry
            .with_extension::<dyn Counter, _>("counter", |c| c.count())
            .unwrap();
        assert_eq!(count, 3);
    }
}
