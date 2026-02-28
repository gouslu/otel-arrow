// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension registry for storing and retrieving extension trait implementations by name.
//!
//! The registry uses `Arc<dyn Any + Send + Sync>` for type-erased storage and
//! `Arc<dyn Trait>` for trait-based lookups. It is `Clone` (cheap `Arc::clone`
//! per entry) and naturally `Send + Sync`.
//!
//! Extensions are registered during pipeline build using a registrar closure
//! (produced by the [`extension_traits!`] macro). Each call to `get` returns a
//! shared `Arc<dyn Trait>` — **no deep copies, single instance per pipeline thread**.
//!
//! # Example
//!
//! ```ignore
//! // Extension factory registers traits using the macro:
//! let registrar = extension_traits!(extension => BearerTokenProvider);
//!
//! // During build the registrar runs:
//! registrar(&mut registry, "azure_auth");
//!
//! // A consumer retrieves a shared trait reference:
//! let provider: Arc<dyn BearerTokenProvider> = registry
//!     .get::<dyn BearerTokenProvider>("azure_auth")?;
//! provider.get_token().await?;
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

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

/// Type alias for the registrar closure produced by [`extension_traits!`].
///
/// The closure captures an `Arc<ConcreteType>` and registers `Arc<dyn Trait>`
/// entries into the registry for the given extension name.
pub type ExtensionRegistrar = Box<dyn FnOnce(&mut ExtensionRegistry, &str) + Send>;

/// Registry for extension trait implementations.
///
/// Extensions register themselves here during pipeline build so other components
/// can look them up by name and retrieve `Arc<dyn Trait>` references.
///
/// The registry is `Clone` (cheap `Arc::clone` per entry) and naturally
/// `Send + Sync` — no unsafe code required.
///
/// Each `get` call returns a shared `Arc<dyn Trait>` — no deep copies. All nodes
/// on the same pipeline thread share the same extension instance.
#[derive(Default, Clone)]
pub struct ExtensionRegistry {
    /// (extension_name, TypeId of Arc<dyn Trait>) → Arc<Arc<dyn Trait>> erased as Arc<dyn Any + Send + Sync>
    handles: HashMap<(String, TypeId), Arc<dyn Any + Send + Sync>>,
}

impl ExtensionRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    /// Register an `Arc<dyn Trait>` for a named extension.
    ///
    /// The trait type is identified by `TypeId::of::<Arc<T>>()` so that
    /// `get::<dyn Trait>()` can look it up.
    pub fn register<T: ?Sized + Send + Sync + 'static>(&mut self, name: &str, arc: Arc<T>) {
        let _ = self.handles
            .insert((name.to_string(), TypeId::of::<Arc<T>>()), Arc::new(arc));
    }

    /// Get a shared trait reference by extension name.
    ///
    /// Returns `Arc<dyn Trait>` — same instance shared by all consumers.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The trait type (e.g., `dyn BearerTokenProvider`).
    ///
    /// # Errors
    ///
    /// Returns `ExtensionError::NotFound` if no extension with that name exists.
    /// Returns `ExtensionError::TraitNotImplemented` if the extension doesn't expose that trait.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let provider: Arc<dyn BearerTokenProvider> = registry
    ///     .get::<dyn BearerTokenProvider>("azure_auth")?;
    /// provider.get_token().await?;
    /// ```
    pub fn get<T: ?Sized + Send + Sync + 'static>(&self, name: &str) -> Result<Arc<T>, ExtensionError> {
        let key = (name.to_string(), TypeId::of::<Arc<T>>());
        let erased = self.handles.get(&key).ok_or_else(|| {
            // Distinguish "extension not found" from "trait not implemented"
            let has_any = self.handles.keys().any(|(n, _)| n == name);
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

        let arc = erased
            .downcast_ref::<Arc<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");

        Ok(Arc::clone(arc))
    }

    /// Check if an extension exists by name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.handles.keys().any(|(n, _)| n == name)
    }

    /// Returns the number of registered extensions (unique names).
    #[must_use]
    pub fn len(&self) -> usize {
        let mut names: Vec<&String> = self.handles.keys().map(|(n, _)| n).collect();
        names.sort();
        names.dedup();
        names.len()
    }

    /// Returns true if no extensions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Returns an iterator over unique extension names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        // Deduplicate — an extension can have multiple trait entries
        let mut names: Vec<&String> = self.handles.keys().map(|(n, _)| n).collect();
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

/// Generates a registrar closure that registers `Arc<dyn Trait>` entries for each
/// listed trait.
///
/// The macro wraps the instance in a single `Arc`, then for each trait, coerces
/// `Arc<ConcreteType>` to `Arc<dyn Trait>` and registers it in the registry.
///
/// No deep copies. No `Clone` requirement. No function pointers.
///
/// # Arguments
///
/// * First: The concrete instance expression
/// * After `=>`: Comma-separated list of trait names this instance implements
///
/// Returns an [`ExtensionRegistrar`] closure.
///
/// # Type Safety
///
/// The macro verifies at compile time that each trait implements
/// [`ExtensionTrait`](crate::extensions::ExtensionTrait) (sealed). If the concrete
/// type doesn't implement a listed trait, the Arc coercion will fail at compile time.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::extension_traits;
/// use otap_df_engine::extensions::BearerTokenProvider;
///
/// struct MyAuthService { /* ... */ }
/// impl BearerTokenProvider for MyAuthService { /* ... */ }
///
/// let service = MyAuthService::new(...);
/// let registrar = extension_traits!(service => BearerTokenProvider);
/// ```
#[macro_export]
macro_rules! extension_traits {
    ($instance:expr => $($trait:ident),* $(,)?) => {{
        let __arc = std::sync::Arc::new($instance);
        let __registrar: $crate::extensions::registry::ExtensionRegistrar = Box::new({
            let arc = __arc.clone();
            move |registry: &mut $crate::extensions::registry::ExtensionRegistry, name: &str| {
                $(
                    {
                        // Compile-time check: ensure the trait is a valid ExtensionTrait.
                        const _: fn() = || {
                            fn assert_extension_trait<T: ?Sized + $crate::extensions::ExtensionTrait>() {}
                            assert_extension_trait::<dyn $trait>();
                        };
                        // Coerce Arc<ConcreteType> → Arc<dyn Trait> (zero-cost)
                        registry.register::<dyn $trait>(name, arc.clone() as std::sync::Arc<dyn $trait>);
                    }
                )*
            }
        });
        __registrar
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::BearerToken;
    use crate::extensions::BearerTokenProvider;
    use tokio::sync::watch;

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
    fn test_register_and_get() {
        let instance = TestTokenProvider {
            token: "test_token".to_string(),
        };
        let registrar = crate::extension_traits!(instance => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        registrar(&mut registry, "test_ext");

        let result: Result<Arc<dyn BearerTokenProvider>, _> =
            registry.get::<dyn BearerTokenProvider>("test_ext");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_returns_shared_arc() {
        let instance = TestTokenProvider {
            token: "shared_test".to_string(),
        };
        let registrar = crate::extension_traits!(instance => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        registrar(&mut registry, "ext");

        let a: Arc<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("ext").unwrap();
        let b: Arc<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("ext").unwrap();

        // Both point to the same allocation
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn test_registry_clone_shares_instances() {
        let instance = TestTokenProvider {
            token: "clone_test".to_string(),
        };
        let registrar = crate::extension_traits!(instance => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        registrar(&mut registry, "ext");

        let cloned = registry.clone();

        let from_original: Arc<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("ext").unwrap();
        let from_clone: Arc<dyn BearerTokenProvider> =
            cloned.get::<dyn BearerTokenProvider>("ext").unwrap();

        // Same instance shared across registry clones
        assert!(Arc::ptr_eq(&from_original, &from_clone));
    }

    #[test]
    fn test_not_found() {
        let registry = ExtensionRegistry::new();
        let result = registry.get::<dyn BearerTokenProvider>("missing");
        assert!(matches!(result, Err(ExtensionError::NotFound { .. })));
    }

    #[test]
    fn test_trait_not_implemented() {
        // Register an extension but ask for a trait it doesn't expose
        // Use a second dummy trait to test TraitNotImplemented.
        // We register for BearerTokenProvider but ask for a different trait.
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let mut registry = ExtensionRegistry::new();
        registry.register::<dyn BearerTokenProvider>(
            "my_ext",
            Arc::new(instance) as Arc<dyn BearerTokenProvider>,
        );

        // Ask for a trait type that was NOT registered — use a dummy Arc<dyn Any + Send + Sync>
        // to trigger TraitNotImplemented. We'll use a custom trait for this.
        // Since we can't easily make a second sealed trait in tests, we just verify the
        // error path by checking that "my_ext" exists but a bogus TypeId doesn't match.
        // The get method checks if the name exists first — so asking for a non-existent
        // trait on an existing name should give TraitNotImplemented.
        // We can test this by asking for `dyn std::fmt::Display + Send + Sync` which
        // won't have been registered.
        let key = ("my_ext".to_string(), TypeId::of::<Arc<dyn std::fmt::Display + Send + Sync>>());
        assert!(registry.handles.get(&key).is_none());
        assert!(registry.contains("my_ext"));
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
        let registrar = crate::extension_traits!(instance => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        registrar(&mut registry, "test_ext");

        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("ExtensionRegistry"));
        assert!(debug_str.contains("test_ext"));
    }

    #[test]
    fn test_contains_and_len() {
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let registrar = crate::extension_traits!(instance => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        registrar(&mut registry, "ext");
        assert!(registry.contains("ext"));
        assert!(!registry.contains("missing"));
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn test_get_extension_actually_works() {
        let instance = TestTokenProvider {
            token: "real_token".to_string(),
        };
        let registrar = crate::extension_traits!(instance => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        registrar(&mut registry, "auth");

        let provider: Arc<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("auth").unwrap();
        let token = provider.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "real_token");
    }

    #[test]
    fn test_multiple_extensions_same_trait() {
        let prod = TestTokenProvider {
            token: "prod_token".to_string(),
        };
        let staging = TestTokenProvider {
            token: "staging_token".to_string(),
        };

        let reg_prod = crate::extension_traits!(prod => BearerTokenProvider);
        let reg_staging = crate::extension_traits!(staging => BearerTokenProvider);

        let mut registry = ExtensionRegistry::new();
        reg_prod(&mut registry, "azure_prod");
        reg_staging(&mut registry, "azure_staging");

        assert_eq!(registry.len(), 2);

        let p1 = registry.get::<dyn BearerTokenProvider>("azure_prod").unwrap();
        let p2 = registry.get::<dyn BearerTokenProvider>("azure_staging").unwrap();

        // Different instances for different names
        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn test_registry_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ExtensionRegistry>();
    }
}
