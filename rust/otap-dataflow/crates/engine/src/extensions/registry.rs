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
//!    single instance, no cloning. Access is via [`ExtensionRegistry::handle`],
//!    which returns an [`ExtensionHandle`] for acquiring a `parking_lot::Mutex` lock.
//!
//! # Mutex-based access
//!
//! Access uses `parking_lot::Mutex` — a fast, uncontended lock (~2-3ns on
//! single-threaded runtimes). The handle + closure-based API ensures the lock
//! is held for the minimum duration and automatically released.
//!
//! - [`handle`](ExtensionRegistry::handle) returns an [`ExtensionHandle`] that
//!   captures the mutex reference. [`ExtensionHandle::lock`] acquires the lock
//!   (blocking), downcasts to `&T`, calls your closure, and releases the lock.
//! - The lock is **not** held across `.await` — extract owned values (futures,
//!   receivers, etc.) from the closure and await them outside.
//! - Accessing *different* extensions or different traits of the same extension
//!   is never blocked (separate mutex per entry).
//! - In debug builds, re-entrant access to the *same* (name, trait) pair on the
//!   same thread panics with a clear message. In release builds it would deadlock.
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
//! The lock uses `Mutex::lock()` (blocking) rather than `try_lock()` so it
//! works correctly in multi-threaded contexts (e.g., tonic gRPC handlers)
//! where brief cross-thread contention is normal. Re-entrancy bugs are
//! caught via `debug_assert!` in debug builds.
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

// ── Nesting detection (debug builds only) ────────────────────────────────────

// Thread-local counter tracking how many extension locks are currently held on
// this thread. Any nesting (same key or different keys) is banned to prevent
// both re-entrant deadlocks and ABBA deadlocks. Zero cost in release builds.
#[cfg(debug_assertions)]
std::thread_local! {
    static HELD_COUNT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// In debug builds, assert that no extension lock is currently held on this
/// thread. Panics if nesting is detected — this catches both same-key
/// re-entrancy and ABBA (different-key, opposite-order) deadlock patterns.
#[cfg(debug_assertions)]
fn assert_no_nesting(name: &str) {
    HELD_COUNT.with(|count| {
        assert!(
            count.get() == 0,
            "Nested extension access detected while accessing '{}': another extension lock is \
             already held on this thread. This risks deadlock. Extract values from the `lock` \
             closure instead of nesting calls.",
            name
        );
    });
}

/// In debug builds, increment the held lock count.
#[cfg(debug_assertions)]
fn mark_held() {
    HELD_COUNT.with(|count| {
        count.set(count.get() + 1);
    });
}

/// In debug builds, decrement the held lock count.
#[cfg(debug_assertions)]
fn unmark_held() {
    HELD_COUNT.with(|count| {
        count.set(count.get() - 1);
    });
}

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
/// [`handle`](Self::handle), which returns an [`ExtensionHandle`] that can be
/// used to acquire a per-entry `parking_lot::Mutex` lock.
///
/// # Usage pattern
///
/// Obtain a handle once during initialization, then use it repeatedly.
/// Extract owned values from the closure — don't hold the lock across `.await`:
///
/// ```ignore
/// // Get handle once:
/// let auth = extension_registry.handle::<dyn BearerTokenProvider>("azure_identity_auth")?;
///
/// // Get an owned future (lock released before await):
/// let token = auth.lock(|a| a.get_token()).await?;
///
/// // Get an owned subscription handle:
/// let mut token_rx = auth.lock(|a| a.subscribe_token_refresh());
/// ```
///
/// # Concurrency
///
/// Each (name, trait) pair has its own `Mutex`. Accessing different extensions
/// or different traits of the same extension is never blocked. The lock is
/// acquired via `Mutex::lock()` (blocking), which is safe for both single-
/// threaded and multi-threaded (tonic) contexts.
///
/// # Re-entrancy detection (debug builds only)
///
/// In debug builds, a thread-local counter detects nested `lock`
/// calls and panics with a clear message. This catches both same-key re-entrancy
/// and ABBA (different-key, opposite-order) deadlock patterns. In release builds,
/// nesting would silently deadlock. Always extract values from the closure
/// instead of nesting calls.
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

    /// Obtain a lightweight handle for repeated access to an extension trait.
    ///
    /// The handle captures the mutex reference, extension name, and trait type
    /// so callers don't repeat them on every access. Use [`ExtensionHandle::lock`]
    /// to acquire the lock and access the extension.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth = registry.handle::<dyn BearerTokenProvider>("auth")?;
    /// let mut token_rx = auth.lock(|a| a.subscribe_token_refresh());
    /// let token = auth.lock(|a| a.get_token()).await?;
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
    pub fn handle<T: ?Sized + 'static>(
        &self,
        name: &str,
    ) -> Result<ExtensionHandle<'_, T>, ExtensionError> {
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

        Ok(ExtensionHandle {
            mutex,
            name: name.to_string(),
            _marker: std::marker::PhantomData,
        })
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

// ── ExtensionHandle ──────────────────────────────────────────────────────────

/// A lightweight handle to a specific extension trait in the registry.
///
/// Created by [`ExtensionRegistry::handle`]. Captures the registry reference,
/// extension name, and trait type so callers don't repeat them on every access.
///
/// # Example
///
/// ```ignore
/// // Create handle once — specifies trait + name:
/// let auth = extension_registry.handle::<dyn BearerTokenProvider>("azure_auth")?;
///
/// // Use repeatedly — concise closure API:
/// let mut token_rx = auth.lock(|a| a.subscribe_token_refresh());
/// let token = auth.lock(|a| a.get_token()).await?;
/// ```
pub struct ExtensionHandle<'a, T: ?Sized + 'static> {
    mutex: &'a Mutex<Box<dyn Any + Send>>,
    name: String,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T: ?Sized + 'static> ExtensionHandle<'a, T> {
    /// Access the extension via a scoped closure.
    ///
    /// Acquires the per-entry Mutex lock (blocking), downcasts to `&T`, calls
    /// the closure, and releases the lock. Extract owned values (futures,
    /// receivers, etc.) — don't `.await` inside the closure.
    ///
    /// # Panics (debug builds only)
    ///
    /// Panics if any extension lock is already held on the current thread
    /// (nesting detected). In release builds, nesting would deadlock.
    pub fn lock<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        #[cfg(debug_assertions)]
        assert_no_nesting(&self.name);
        #[cfg(debug_assertions)]
        mark_held();

        let guard = self.mutex.lock();

        let boxed_trait: &Box<T> = guard
            .downcast_ref::<Box<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");

        let result = f(boxed_trait.as_ref());

        drop(guard);
        #[cfg(debug_assertions)]
        unmark_held();

        result
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
        let handle = registry.handle::<dyn TestCapability>("my_ext").unwrap();
        let value = handle.lock(|ext| ext.get_value());
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
        let cap = registry.handle::<dyn TestCapability>("multi_ext").unwrap();
        let another = registry.handle::<dyn AnotherCapability>("multi_ext").unwrap();
        assert_eq!(cap.lock(|ext| ext.get_value()), 99);
        assert_eq!(another.lock(|ext| ext.get_name()), "hello");
    }

    #[test]
    fn test_not_found_error() {
        let registry = ExtensionRegistry::new();
        let result = registry.handle::<dyn TestCapability>("nonexistent");
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
        let result = registry.handle::<dyn AnotherCapability>("partial_ext");
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
        let h1 = clone1.handle::<dyn TestCapability>("ext").unwrap();
        let h2 = clone2.handle::<dyn TestCapability>("ext").unwrap();
        assert_eq!(h1.lock(|ext| ext.get_value()), 10);
        assert_eq!(h2.lock(|ext| ext.get_value()), 10);

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
        let auth_h = registry.handle::<dyn TestCapability>("auth").unwrap();
        let rate_h = registry.handle::<dyn TestCapability>("rate").unwrap();
        assert_eq!(auth_h.lock(|ext| ext.get_value()), 1);
        assert_eq!(rate_h.lock(|ext| ext.get_value()), 2);
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
        let handle = registry.handle::<dyn TestCapability>("ext").unwrap();
        let v1 = handle.lock(|ext| ext.get_value());
        let v2 = handle.lock(|ext| ext.get_value());
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
        let cap = registry.handle::<dyn TestCapability>("ext").unwrap();
        let another = registry.handle::<dyn AnotherCapability>("ext").unwrap();
        assert_eq!(cap.lock(|ext| ext.get_value()), 42);
        assert_eq!(another.lock(|ext| ext.get_name()), "test");
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
        let handle = registry.handle::<dyn AsyncCapability>("async_ext").unwrap();
        let fut = handle.lock(|ext| ext.do_work());
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
        let handle = registry.handle::<dyn Subscribable>("watch_ext").unwrap();
        let mut rx = handle.lock(|ext| ext.subscribe());

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

    // ── Re-entrant access panics in debug builds ──────────────────────

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Nested extension access detected")]
    fn test_reentrant_access_panics_in_debug() {
        // Nested access to the SAME (name, trait) pair panics in debug builds
        trait ReentrantCapability: Send + 'static {
            fn try_reenter(&self, registry: &ExtensionRegistry);
        }

        #[derive(Clone)]
        struct ReentrantExt;

        impl ReentrantCapability for ReentrantExt {
            fn try_reenter(&self, registry: &ExtensionRegistry) {
                // Try to access the same extension while it's already locked — panics
                let handle = registry.handle::<dyn ReentrantCapability>("ext").unwrap();
                handle.lock(|_| ());
            }
        }

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ReentrantExt => ReentrantCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();

        // The outer call triggers the inner re-entrant call which panics
        let handle = registry.handle::<dyn ReentrantCapability>("ext").unwrap();
        handle.lock(|ext| {
            ext.try_reenter(&registry);
        });
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
        let handle = registry.handle::<dyn Counter>("counter").unwrap();
        handle.lock(|c| {
            assert_eq!(c.count(), 0);
            c.increment();
            c.increment();
            c.increment();
            assert_eq!(c.count(), 3);
        });

        // State persists across calls
        let count = handle.lock(|c| c.count());
        assert_eq!(count, 3);
    }

    // ── ExtensionHandle ─────────────────────────────────────────────────

    #[test]
    fn test_handle_basic_usage() {
        let ext = CellCounter {
            count: std::cell::Cell::new(0),
        };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => Counter);
        registrar(&mut builder, "counter");

        let registry = builder.build();

        // Get handle once, use multiple times
        let handle = registry.handle::<dyn Counter>("counter").unwrap();
        handle.lock(|c| c.increment());
        handle.lock(|c| c.increment());
        handle.lock(|c| c.increment());
        assert_eq!(handle.lock(|c| c.count()), 3);
    }

    #[test]
    fn test_handle_not_found() {
        let registry = ExtensionRegistryBuilder::new().build();
        let result = registry.handle::<dyn Counter>("missing");
        assert!(matches!(result, Err(ExtensionError::NotFound { .. })));
    }

    #[test]
    fn test_handle_trait_not_implemented() {
        let ext = TestExtension {
            value: 1,
            name: "x".to_string(),
        };
        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => TestCapability);
        registrar(&mut builder, "ext");

        let registry = builder.build();
        let result = registry.handle::<dyn Counter>("ext");
        assert!(matches!(
            result,
            Err(ExtensionError::TraitNotImplemented { .. })
        ));
    }

    #[tokio::test]
    async fn test_handle_subscription_pattern() {
        let (tx, _rx) = watch::channel(0);
        let ext = WatchExtension { tx: tx.clone() };

        let mut builder = ExtensionRegistryBuilder::new();
        let registrar = extension_traits!(ext => Subscribable);
        registrar(&mut builder, "watch_ext");

        let registry = builder.build();

        let handle = registry.handle::<dyn Subscribable>("watch_ext").unwrap();
        let mut rx = handle.lock(|s| s.subscribe());

        tx.send(99).unwrap();
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), 99);
    }
}
