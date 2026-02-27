// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension registry for storing and retrieving extension trait implementations by name.
//!
//! This registry uses a clone-and-box approach: each extension stores a single
//! concrete instance, along with cast functions that clone the concrete type and
//! return a `Box<dyn Trait>`. This is entirely safe — no raw pointer manipulation,
//! no `unsafe impl Sync`, no fat-pointer transmutation.
//!
//! The concrete service type must implement `Clone`. Typically the clone is cheap
//! because the service holds `Arc`-wrapped shared state internally (e.g.,
//! `Arc<Mutex<TokenState>>`).
//!
//! # Example
//!
//! ```ignore
//! // An extension registers its capabilities using the macro:
//! let service = MyAuthService::new(...);
//! let traits = extension_traits!(MyAuthService => BearerTokenProvider);
//!
//! // Pass to ExtensionWrapper which builds the registry entry:
//! ExtensionWrapper::new(extension, service, traits, node_id, config, ...);
//!
//! // A consumer retrieves a boxed trait object:
//! let provider: Box<dyn BearerTokenProvider> = registry
//!     .get_extension::<dyn BearerTokenProvider>("azure_auth")?;
//! provider.get_token().await?;
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;

/// A cast function that clones the concrete instance and returns it as a
/// boxed trait object, double-boxed for type-erased storage.
///
/// The function:
/// 1. Downcasts `&dyn Any` to `&ConcreteType`
/// 2. Clones the concrete instance
/// 3. Boxes it as `Box<dyn Trait>`
/// 4. Wraps in another `Box<dyn Any + Send>` for type erasure
///
/// Returns `None` if the downcast fails.
pub type CastFn = fn(&dyn Any) -> Option<Box<dyn Any + Send>>;

/// Marker trait for TypeId lookup of trait types.
/// Used to get a stable TypeId for `dyn Trait` types.
pub trait TraitId<T: ?Sized> {}

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

/// A function that clones the concrete service instance and returns it
/// as a type-erased `Box<dyn Any + Send>`.
///
/// This is captured at construction time (monomorphized for the concrete type)
/// so that type-erased entries can be cloned without knowing the concrete type.
pub type CloneFn = fn(&dyn Any) -> Box<dyn Any + Send>;

/// Cast functions for an extension's trait implementations.
///
/// This is the return type of the [`extension_traits!`] macro. It contains
/// the mapping from trait TypeIds to cast functions that clone the concrete
/// instance and return a `Box<dyn Trait>`, plus a clone function for the
/// concrete instance itself.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::extension_traits;
/// use otap_df_engine::extensions::BearerTokenProvider;
///
/// #[derive(Clone)]
/// struct MyAuthService { /* ... */ }
/// impl BearerTokenProvider for MyAuthService { /* ... */ }
///
/// let traits = extension_traits!(MyAuthService => BearerTokenProvider);
/// ```
pub struct ExtensionTraits {
    casters: HashMap<TypeId, CastFn>,
    /// Clones the concrete service instance (type-erased).
    clone_fn: CloneFn,
}

impl ExtensionTraits {
    /// Create from casters and a clone function (used by the macro).
    #[must_use]
    pub fn from_parts(
        casters: HashMap<TypeId, CastFn>,
        clone_fn: CloneFn,
    ) -> Self {
        Self { casters, clone_fn }
    }

    /// Create empty casters for a service type that exposes no traits.
    ///
    /// Useful for extensions that participate in the pipeline lifecycle
    /// but don't expose any capabilities to other components.
    #[must_use]
    pub fn for_service<T: Clone + Send + 'static>() -> Self {
        fn do_clone<S: Clone + Send + 'static>(any: &dyn Any) -> Box<dyn Any + Send> {
            let val = any
                .downcast_ref::<S>()
                .expect("TypeId mismatch in ExtensionTraits clone — this is a bug");
            Box::new(val.clone())
        }
        Self {
            casters: HashMap::new(),
            clone_fn: do_clone::<T>,
        }
    }

    /// Decompose into the caster map and clone function.
    #[must_use]
    pub fn into_parts(self) -> (HashMap<TypeId, CastFn>, CloneFn) {
        (self.casters, self.clone_fn)
    }

    /// Check if a trait is registered.
    #[must_use]
    pub fn contains<T: ?Sized + 'static>(&self) -> bool {
        self.casters.contains_key(&TypeId::of::<dyn TraitId<T>>())
    }

    /// Returns true if no traits are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.casters.is_empty()
    }

    /// Returns the number of registered traits.
    #[must_use]
    pub fn len(&self) -> usize {
        self.casters.len()
    }
}

impl std::fmt::Debug for ExtensionTraits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtensionTraits")
            .field("trait_count", &self.casters.len())
            .finish()
    }
}

/// Macro to generate cast functions for an extension's trait implementations.
///
/// Each generated cast function clones the concrete service instance and boxes
/// it as the trait object. The concrete type **must** implement `Clone`.
///
/// # Arguments
///
/// * First: The concrete type name (must implement `Clone`)
/// * After `=>`: Comma-separated list of trait names this type implements
///
/// Returns an [`ExtensionTraits`] that can be passed to `ExtensionWrapper::new()`.
///
/// # Type Safety
///
/// Only traits that implement [`crate::extensions::ExtensionTrait`] can be used
/// with this macro. This is enforced at compile time — attempting to use an
/// arbitrary trait will result in a compilation error. The macro also verifies
/// that the concrete type implements `Clone` and each specified trait.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::extension_traits;
/// use otap_df_engine::extensions::BearerTokenProvider;
///
/// #[derive(Clone)]
/// struct MyAuthService { /* ... */ }
/// impl BearerTokenProvider for MyAuthService { /* ... */ }
///
/// let traits = extension_traits!(MyAuthService => BearerTokenProvider);
/// ExtensionWrapper::new(extension, service, traits, node_id, user_config, config);
/// ```
#[macro_export]
macro_rules! extension_traits {
    ($concrete_ty:ty => $($trait:ident),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut casters: std::collections::HashMap<
            std::any::TypeId,
            $crate::extensions::registry::CastFn
        > = std::collections::HashMap::new();
        $(
            {
                // Compile-time check: ensure the trait is a valid ExtensionTrait.
                const _: fn() = || {
                    fn assert_extension_trait<T: ?Sized + $crate::extensions::ExtensionTrait>() {}
                    assert_extension_trait::<dyn $trait>();
                };

                // Compile-time check: ensure the concrete type is Clone.
                const _: fn() = || {
                    fn assert_clone<T: Clone>() {}
                    assert_clone::<$concrete_ty>();
                };

                // Cast function: clone the concrete instance, box as trait, double-box
                // for type erasure. All safe — no raw pointers, no transmute.
                fn __cast(any: &dyn std::any::Any) -> Option<Box<dyn std::any::Any + Send>> {
                    let concrete = any.downcast_ref::<$concrete_ty>()?;
                    let cloned: $concrete_ty = concrete.clone();
                    let trait_obj: Box<dyn $trait> = Box::new(cloned);
                    Some(Box::new(trait_obj))
                }
                let _ = casters.insert(
                    std::any::TypeId::of::<dyn $crate::extensions::registry::TraitId<dyn $trait>>(),
                    __cast as $crate::extensions::registry::CastFn,
                );
            }
        )*
        // Clone function for the concrete instance (type-erased).
        fn __clone_instance(any: &dyn std::any::Any) -> Box<dyn std::any::Any + Send> {
            let concrete = any
                .downcast_ref::<$concrete_ty>()
                .expect("TypeId mismatch in ExtensionEntry clone — this is a bug");
            Box::new(concrete.clone())
        }
        $crate::extensions::registry::ExtensionTraits::from_parts(
            casters,
            __clone_instance as $crate::extensions::registry::CloneFn,
        )
    }};
}

/// Internal storage for an extension instance and its cast functions.
///
/// This is used internally by the registry to store extensions.
/// Users should not create this directly — use the [`extension_traits!`] macro
/// with `ExtensionWrapper::new()`.
pub struct ExtensionEntry {
    /// The single concrete instance, type-erased.
    instance: Box<dyn Any + Send>,
    /// One cast function per registered trait.
    casters: HashMap<TypeId, CastFn>,
    /// Clones the concrete instance inside the box.
    clone_fn: CloneFn,
}

impl Clone for ExtensionEntry {
    fn clone(&self) -> Self {
        Self {
            instance: (self.clone_fn)(self.instance.as_ref()),
            casters: self.casters.clone(),
            clone_fn: self.clone_fn,
        }
    }
}

impl ExtensionEntry {
    /// Create a new entry from an instance and casters.
    pub fn new<T: Send + 'static>(instance: T, casters: ExtensionTraits) -> Self {
        let (casters, clone_fn) = casters.into_parts();
        Self {
            instance: Box::new(instance),
            casters,
            clone_fn,
        }
    }

    /// Create a new entry from an already-boxed instance and casters.
    ///
    /// This is used during pipeline build when the service has been
    /// type-erased by `ExtensionWrapper`.
    pub fn from_boxed(instance: Box<dyn Any + Send>, casters: ExtensionTraits) -> Self {
        let (casters, clone_fn) = casters.into_parts();
        Self {
            instance,
            casters,
            clone_fn,
        }
    }

    /// Get a boxed trait object from the entry.
    ///
    /// Clones the concrete instance and returns it as `Box<dyn Trait>`.
    pub fn get<T: ?Sized + 'static>(&self) -> Option<Box<T>> {
        let cast = self.casters.get(&TypeId::of::<dyn TraitId<T>>())?;
        let boxed_any: Box<dyn Any + Send> = cast(self.instance.as_ref())?;
        // The cast function produced Box<Box<dyn T>> erased as Box<dyn Any + Send>.
        // Downcast to Box<Box<T>> then unwrap the outer Box.
        let boxed_trait: Box<Box<T>> = boxed_any.downcast().ok()?;
        Some(*boxed_trait)
    }

    /// Check if the entry contains a trait implementation.
    #[must_use]
    pub fn contains<T: ?Sized + 'static>(&self) -> bool {
        self.casters.contains_key(&TypeId::of::<dyn TraitId<T>>())
    }

    /// Returns the number of trait implementations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.casters.len()
    }

    /// Returns true if no traits are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.casters.is_empty()
    }
}

impl std::fmt::Debug for ExtensionEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtensionEntry")
            .field("trait_count", &self.casters.len())
            .finish()
    }
}

/// Registry for extension trait implementations.
///
/// Extensions register themselves here during pipeline build so other components
/// can look them up by name and retrieve boxed trait objects.
///
/// The registry is `Clone + Send` — each pipeline component receives its own
/// clone at startup. `Sync` is intentionally **not** implemented, consistent
/// with the shared-nothing, single-threaded `LocalSet` architecture.
///
/// Each `get_extension` call clones the concrete service instance (typically
/// cheap since services hold `Arc` state) and returns an owned `Box<dyn Trait>`.
#[derive(Default, Clone)]
pub struct ExtensionRegistry {
    extensions: HashMap<String, ExtensionEntry>,
}

impl ExtensionRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            extensions: HashMap::new(),
        }
    }

    /// Create a registry from a map of extension entries.
    #[must_use]
    pub fn from_map(extensions: HashMap<String, ExtensionEntry>) -> Self {
        Self { extensions }
    }

    /// Get a boxed trait object by extension name.
    ///
    /// Clones the concrete service instance and returns it as `Box<dyn Trait>`.
    /// The clone is typically cheap since services hold `Arc`-wrapped shared state.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The trait type (e.g., `dyn BearerTokenProvider`).
    ///
    /// # Errors
    ///
    /// Returns `ExtensionError::NotFound` if no extension with that name exists.
    /// Returns `ExtensionError::TraitNotImplemented` if the extension doesn't implement the trait.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let provider: Box<dyn BearerTokenProvider> = registry
    ///     .get_extension::<dyn BearerTokenProvider>("azure_auth")?;
    /// provider.get_token().await?;
    /// ```
    pub fn get_extension<T: ?Sized + 'static>(
        &self,
        name: &str,
    ) -> Result<Box<T>, ExtensionError> {
        let entry = self
            .extensions
            .get(name)
            .ok_or_else(|| ExtensionError::NotFound {
                name: name.to_string(),
            })?;

        entry
            .get::<T>()
            .ok_or_else(|| ExtensionError::TraitNotImplemented {
                name: name.to_string(),
                expected: std::any::type_name::<T>(),
            })
    }

    /// Check if an extension exists by name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.extensions.contains_key(name)
    }

    /// Returns the number of registered extensions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.extensions.len()
    }

    /// Returns true if no extensions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.extensions.is_empty()
    }

    /// Returns an iterator over extension names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.extensions.keys()
    }
}

impl std::fmt::Debug for ExtensionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtensionRegistry")
            .field("extensions", &self.extensions.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Builder for constructing an [`ExtensionRegistry`].
///
/// Use this to register extension entries before creating the immutable registry.
///
/// # Example
///
/// ```ignore
/// let mut builder = ExtensionRegistryBuilder::new();
///
/// let service = MyAuthService::new(...);
/// let traits = extension_traits!(MyAuthService => BearerTokenProvider);
/// builder.register("azure_auth", service, traits);
///
/// let registry = builder.build();
/// ```
#[derive(Default)]
pub struct ExtensionRegistryBuilder {
    /// The map of extension names to entries being built.
    pub extensions: HashMap<String, ExtensionEntry>,
}

impl ExtensionRegistryBuilder {
    /// Create a new empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            extensions: HashMap::new(),
        }
    }

    /// Register an extension with a name, instance, and casters.
    pub fn register<T: Send + 'static>(
        &mut self,
        name: String,
        instance: T,
        casters: ExtensionTraits,
    ) {
        let _ = self
            .extensions
            .insert(name, ExtensionEntry::new(instance, casters));
    }

    /// Register an extension from a pre-boxed service instance and casters.
    ///
    /// Used during pipeline build when the service has been type-erased
    /// by `ExtensionWrapper`.
    pub fn register_boxed(
        &mut self,
        name: String,
        instance: Box<dyn Any + Send>,
        casters: ExtensionTraits,
    ) {
        let _ = self
            .extensions
            .insert(name, ExtensionEntry::from_boxed(instance, casters));
    }

    /// Build the immutable registry.
    #[must_use]
    pub fn build(self) -> ExtensionRegistry {
        ExtensionRegistry::from_map(self.extensions)
    }
}

impl std::fmt::Debug for ExtensionRegistryBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExtensionRegistryBuilder")
            .field("extensions", &self.extensions.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::BearerToken;
    use crate::extensions::BearerTokenProvider;
    use tokio::sync::watch;

    #[derive(Clone)]
    struct TestTokenProvider {
        token: String,
    }

    #[async_trait::async_trait]
    impl BearerTokenProvider for TestTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, crate::extensions::Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    #[test]
    fn test_extension_casters() {
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);
        assert_eq!(casters.len(), 1);
        assert!(casters.contains::<dyn BearerTokenProvider>());
    }

    #[test]
    fn test_extension_entry() {
        let instance = TestTokenProvider {
            token: "test_token".to_string(),
        };
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);
        let entry = ExtensionEntry::new(instance, casters);

        assert_eq!(entry.len(), 1);
        assert!(entry.contains::<dyn BearerTokenProvider>());

        let provider: Box<dyn BearerTokenProvider> = entry.get().unwrap();
        drop(provider);
    }

    #[test]
    fn test_registry_get_extension() {
        let instance = TestTokenProvider {
            token: "test_token".to_string(),
        };
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);
        let entry = ExtensionEntry::new(instance, casters);

        let mut map = HashMap::new();
        let _ = map.insert("test_ext".to_string(), entry);

        let registry = ExtensionRegistry::from_map(map);

        let result: Result<Box<dyn BearerTokenProvider>, _> =
            registry.get_extension("test_ext");
        assert!(result.is_ok());

        let not_found: Result<Box<dyn BearerTokenProvider>, _> =
            registry.get_extension("missing");
        assert!(matches!(not_found, Err(ExtensionError::NotFound { .. })));
    }

    #[test]
    fn test_registry_builder() {
        let mut builder = ExtensionRegistryBuilder::new();
        assert!(builder.extensions.is_empty());

        let instance = TestTokenProvider {
            token: "builder_test".to_string(),
        };
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);

        builder.register("my_extension".to_string(), instance, casters);

        let registry = builder.build();
        assert_eq!(registry.len(), 1);
        assert!(registry.contains("my_extension"));
        let _: Box<dyn BearerTokenProvider> =
            registry.get_extension("my_extension").unwrap();
    }

    #[test]
    fn test_get_extension_returns_independent_clones() {
        let instance = TestTokenProvider {
            token: "clone_test".to_string(),
        };
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);
        let entry = ExtensionEntry::new(instance, casters);

        let registry =
            ExtensionRegistry::from_map(HashMap::from([("ext".to_string(), entry)]));

        // Each call returns an independent clone
        let a: Box<dyn BearerTokenProvider> = registry.get_extension("ext").unwrap();
        let b: Box<dyn BearerTokenProvider> = registry.get_extension("ext").unwrap();
        drop(a);
        drop(b);
    }

    #[test]
    fn test_extension_error_display() {
        let not_found = ExtensionError::NotFound {
            name: "missing_ext".to_string(),
        };
        let display = format!("{}", not_found);
        assert!(display.contains("missing_ext"));
        assert!(display.contains("not found"));

        let not_impl = ExtensionError::TraitNotImplemented {
            name: "my_ext".to_string(),
            expected: "BearerTokenProvider",
        };
        let display = format!("{}", not_impl);
        assert!(display.contains("my_ext"));
        assert!(display.contains("BearerTokenProvider"));
    }

    #[test]
    fn test_registry_debug() {
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);
        let entry = ExtensionEntry::new(instance, casters);

        let registry =
            ExtensionRegistry::from_map(HashMap::from([("test_ext".to_string(), entry)]));
        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("ExtensionRegistry"));
        assert!(debug_str.contains("test_ext"));
    }

    #[tokio::test]
    async fn test_get_extension_actually_works() {
        let instance = TestTokenProvider {
            token: "real_token".to_string(),
        };
        let casters = crate::extension_traits!(TestTokenProvider => BearerTokenProvider);

        let mut builder = ExtensionRegistryBuilder::new();
        builder.register("auth".to_string(), instance, casters);
        let registry = builder.build();

        let provider: Box<dyn BearerTokenProvider> =
            registry.get_extension("auth").unwrap();
        let token = provider.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "real_token");
    }
}
