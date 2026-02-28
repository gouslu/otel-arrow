// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local extension registry (no bounds, stored as `Rc<dyn Trait>`).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::extensions::ExtensionError;
use crate::shared::extensions::{SharedExtensionRegistry, SharedExtensionTrait};
use super::LocalExtensionTrait;

/// Type alias for the registrar closure produced by [`shared_extension_traits!`]
/// and [`local_extension_traits!`].
///
/// The closure captures a concrete extension instance (`Send`) and registers
/// trait entries into the [`LocalExtensionRegistry`] for the given extension name.
/// It receives `&mut LocalExtensionRegistry` so it can register both shared
/// (`Arc`) and local (`Rc`) traits.
///
/// Although `LocalExtensionRegistry` is `!Send` (it contains `Rc`), the closure
/// only receives it as a borrow parameter — the closure itself is `Send` because
/// it only captures `Send` data.
pub type ExtensionRegistrar = Box<dyn FnOnce(&mut LocalExtensionRegistry, &str) + Send>;

/// Registry for extension trait implementations.
///
/// `!Send`, `!Sync`, `Clone` (cheap `Rc::clone` / `Arc::clone` per entry).
///
/// The primary registry type — passed to all components. Can access both:
/// - **Local traits** via [`get`](Self::get) → `Rc<dyn Trait>` (hot-path, no atomic overhead)
/// - **Shared traits** via [`get_shared`](Self::get_shared) → `Arc<dyn Trait>` (cold-path)
///
/// Shared components can call [`into_shared`](Self::into_shared) to obtain a
/// [`SharedExtensionRegistry`] that is `Send + Sync`.
///
/// This follows the same performance pattern as channel metrics:
/// `Rc<RefCell<...>>` for local hot-path vs `Arc<Mutex<...>>` for shared.
#[derive(Default, Clone)]
pub struct LocalExtensionRegistry {
    /// Shared (Arc) registry — accessible by both local and shared components.
    shared: SharedExtensionRegistry,
    /// Local (Rc) entries — only accessible by local components.
    /// (extension_name, TypeId of Rc<dyn Trait>) → Rc<Rc<dyn Trait>> erased as Rc<dyn Any>
    local: HashMap<(String, TypeId), Rc<dyn Any>>,
}

impl LocalExtensionRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: SharedExtensionRegistry::new(),
            local: HashMap::new(),
        }
    }

    /// Register a shared (`Arc`) trait entry.
    ///
    /// The trait will be accessible by both local components (via `get_shared`)
    /// and shared components (via the extracted `SharedExtensionRegistry`).
    pub fn register<T: ?Sized + SharedExtensionTrait + 'static>(
        &mut self,
        name: &str,
        arc: Arc<T>,
    ) {
        self.shared.register(name, arc);
    }

    /// Register a local (`Rc`) trait entry.
    ///
    /// The trait will only be accessible by local components via `get`.
    /// This avoids atomic/mutex overhead for hot-path extensions.
    pub fn register_local<T: ?Sized + LocalExtensionTrait + 'static>(&mut self, name: &str, rc: Rc<T>) {
        let _ = self.local.insert(
            (name.to_string(), TypeId::of::<Rc<T>>()),
            Rc::new(rc),
        );
    }

    /// Get a shared trait reference by extension name.
    ///
    /// Returns `Arc<dyn Trait>` — delegates to the inner shared registry.
    pub fn get_shared<T: ?Sized + SharedExtensionTrait + 'static>(
        &self,
        name: &str,
    ) -> Result<Arc<T>, ExtensionError> {
        self.shared.get(name)
    }

    /// Get a local trait reference by extension name.
    ///
    /// Returns `Rc<dyn Trait>` — no atomic overhead, ideal for hot-path extensions.
    ///
    /// # Errors
    ///
    /// Returns `ExtensionError::NotFound` if no extension with that name exists
    /// in either the local or shared maps.
    /// Returns `ExtensionError::TraitNotImplemented` if the extension exists but
    /// doesn't expose this particular local trait.
    pub fn get<T: ?Sized + LocalExtensionTrait + 'static>(
        &self,
        name: &str,
    ) -> Result<Rc<T>, ExtensionError> {
        let key = (name.to_string(), TypeId::of::<Rc<T>>());
        let erased = self.local.get(&key).ok_or_else(|| {
            // Check both local and shared maps for error differentiation
            let has_any = self.local.keys().any(|(n, _)| n == name)
                || self.shared.contains(name);
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

        let rc = erased
            .downcast_ref::<Rc<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");

        Ok(Rc::clone(rc))
    }

    /// Returns a reference to the inner shared registry.
    #[must_use]
    pub fn shared(&self) -> &SharedExtensionRegistry {
        &self.shared
    }

    /// Extracts the shared registry, consuming this local registry.
    ///
    /// Used by the engine to pass the shared registry to shared components.
    #[must_use]
    pub fn into_shared(self) -> SharedExtensionRegistry {
        self.shared
    }

    /// Check if an extension exists by name in either map.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.shared.contains(name) || self.local.keys().any(|(n, _)| n == name)
    }

    /// Returns the number of registered extensions (unique names across both maps).
    #[must_use]
    pub fn len(&self) -> usize {
        let mut names: Vec<&String> = self
            .shared
            .handles
            .keys()
            .chain(self.local.keys())
            .map(|(n, _)| n)
            .collect();
        names.sort();
        names.dedup();
        names.len()
    }

    /// Returns true if no extensions are registered in either map.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shared.is_empty() && self.local.is_empty()
    }

    /// Returns an iterator over unique extension names from both maps.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        let mut names: Vec<&String> = self
            .shared
            .handles
            .keys()
            .chain(self.local.keys())
            .map(|(n, _)| n)
            .collect();
        names.sort();
        names.dedup();
        names.into_iter()
    }
}

impl std::fmt::Debug for LocalExtensionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&String> = self.names().collect();
        f.debug_struct("LocalExtensionRegistry")
            .field("extensions", &names)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::BearerToken;
    use crate::shared::extensions::BearerTokenProvider as SharedBearerTokenProvider;
    use tokio::sync::watch;

    struct TestTokenProvider {
        token: String,
    }

    #[async_trait::async_trait]
    impl SharedBearerTokenProvider for TestTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, crate::extensions::Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    // ── Shared registry tests ────────────────────────────────────────────────

    #[test]
    fn test_shared_register_and_get() {
        let instance = TestTokenProvider {
            token: "test_token".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "test_ext");

        let result: Result<Arc<dyn SharedBearerTokenProvider>, _> =
            registry.get_shared::<dyn SharedBearerTokenProvider>("test_ext");
        assert!(result.is_ok());
    }

    #[test]
    fn test_shared_get_returns_shared_arc() {
        let instance = TestTokenProvider {
            token: "shared_test".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "ext");

        let a: Arc<dyn SharedBearerTokenProvider> =
            registry.get_shared::<dyn SharedBearerTokenProvider>("ext").unwrap();
        let b: Arc<dyn SharedBearerTokenProvider> =
            registry.get_shared::<dyn SharedBearerTokenProvider>("ext").unwrap();

        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn test_shared_registry_clone_shares_instances() {
        let instance = TestTokenProvider {
            token: "clone_test".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "ext");

        let cloned = registry.clone();

        let from_original: Arc<dyn SharedBearerTokenProvider> =
            registry.get_shared::<dyn SharedBearerTokenProvider>("ext").unwrap();
        let from_clone: Arc<dyn SharedBearerTokenProvider> =
            cloned.get_shared::<dyn SharedBearerTokenProvider>("ext").unwrap();

        assert!(Arc::ptr_eq(&from_original, &from_clone));
    }

    #[test]
    fn test_shared_not_found() {
        let registry = LocalExtensionRegistry::new();
        let result = registry.get_shared::<dyn SharedBearerTokenProvider>("missing");
        assert!(matches!(result, Err(ExtensionError::NotFound { .. })));
    }

    #[test]
    fn test_shared_trait_not_implemented() {
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let mut registry = SharedExtensionRegistry::new();
        registry.register::<dyn SharedBearerTokenProvider>(
            "my_ext",
            Arc::new(instance) as Arc<dyn SharedBearerTokenProvider>,
        );

        let key = (
            "my_ext".to_string(),
            TypeId::of::<Arc<dyn std::fmt::Display + Send + Sync>>(),
        );
        assert!(registry.handles.get(&key).is_none());
        assert!(registry.contains("my_ext"));
    }

    // ── Local registry tests ─────────────────────────────────────────────────

    trait TestLocalTrait {
        fn value(&self) -> u64;
    }

    struct TestLocalImpl {
        val: u64,
    }

    impl TestLocalTrait for TestLocalImpl {
        fn value(&self) -> u64 {
            self.val
        }
    }

    // Seal the test-only local trait.
    impl crate::extensions::private::Sealed for dyn TestLocalTrait {}
    impl LocalExtensionTrait for dyn TestLocalTrait {}

    #[test]
    fn test_local_register_and_get() {
        let mut registry = LocalExtensionRegistry::new();
        let instance = TestLocalImpl { val: 42 };
        registry.register_local::<dyn TestLocalTrait>("limiter", Rc::new(instance));

        let result = registry.get::<dyn TestLocalTrait>("limiter");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().value(), 42);
    }

    #[test]
    fn test_local_get_returns_shared_rc() {
        let mut registry = LocalExtensionRegistry::new();
        let instance = TestLocalImpl { val: 99 };
        registry.register_local::<dyn TestLocalTrait>("ext", Rc::new(instance));

        let a = registry.get::<dyn TestLocalTrait>("ext").unwrap();
        let b = registry.get::<dyn TestLocalTrait>("ext").unwrap();

        assert!(Rc::ptr_eq(&a, &b));
    }

    #[test]
    fn test_local_not_found() {
        let registry = LocalExtensionRegistry::new();
        let result = registry.get::<dyn TestLocalTrait>("missing");
        assert!(matches!(result, Err(ExtensionError::NotFound { .. })));
    }

    #[test]
    fn test_local_trait_not_implemented_when_only_shared_exists() {
        let instance = TestTokenProvider {
            token: "t".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "auth");

        let result = registry.get::<dyn TestLocalTrait>("auth");
        assert!(matches!(
            result,
            Err(ExtensionError::TraitNotImplemented { .. })
        ));
    }

    // ── Mixed registry tests ─────────────────────────────────────────────────

    #[test]
    fn test_contains_checks_both_maps() {
        let instance = TestTokenProvider {
            token: "t".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "shared_ext");

        let local_impl = TestLocalImpl { val: 1 };
        registry.register_local::<dyn TestLocalTrait>("local_ext", Rc::new(local_impl));

        assert!(registry.contains("shared_ext"));
        assert!(registry.contains("local_ext"));
        assert!(!registry.contains("missing"));
    }

    #[test]
    fn test_len_counts_unique_names_across_maps() {
        let instance = TestTokenProvider {
            token: "t".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "ext_a");

        let local_impl = TestLocalImpl { val: 1 };
        registry.register_local::<dyn TestLocalTrait>("ext_b", Rc::new(local_impl));

        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
    }

    // ── Error display ────────────────────────────────────────────────────────

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

    // ── Debug ────────────────────────────────────────────────────────────────

    #[test]
    fn test_local_registry_debug() {
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "test_ext");

        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("LocalExtensionRegistry"));
        assert!(debug_str.contains("test_ext"));
    }

    #[test]
    fn test_shared_registry_debug() {
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let mut registry = SharedExtensionRegistry::new();
        registry.register::<dyn SharedBearerTokenProvider>(
            "ext",
            Arc::new(instance) as Arc<dyn SharedBearerTokenProvider>,
        );

        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("SharedExtensionRegistry"));
        assert!(debug_str.contains("ext"));
    }

    // ── Send/Sync assertions ─────────────────────────────────────────────────

    #[test]
    fn test_shared_registry_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SharedExtensionRegistry>();
    }

    // LocalExtensionRegistry is intentionally !Send and !Sync (contains Rc).
    // This is verified by the compiler — any attempt to send it across threads
    // will fail to compile, just like Rc<RefCell<...>> for local channel metrics.

    // ── Functional tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_get_extension_actually_works() {
        let instance = TestTokenProvider {
            token: "real_token".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        registrar(&mut registry, "auth");

        let provider: Arc<dyn SharedBearerTokenProvider> =
            registry.get_shared::<dyn SharedBearerTokenProvider>("auth").unwrap();
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

        let reg_prod = crate::shared_extension_traits!(prod => SharedBearerTokenProvider);
        let reg_staging = crate::shared_extension_traits!(staging => SharedBearerTokenProvider);

        let mut registry = LocalExtensionRegistry::new();
        reg_prod(&mut registry, "azure_prod");
        reg_staging(&mut registry, "azure_staging");

        assert_eq!(registry.len(), 2);

        let p1 = registry
            .get_shared::<dyn SharedBearerTokenProvider>("azure_prod")
            .unwrap();
        let p2 = registry
            .get_shared::<dyn SharedBearerTokenProvider>("azure_staging")
            .unwrap();

        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    /// Proves each pipeline gets its own extension instance.
    #[test]
    fn test_separate_pipelines_get_separate_instances() {
        let instance1 = TestTokenProvider {
            token: "pipeline1_token".to_string(),
        };
        let registrar1 = crate::shared_extension_traits!(instance1 => SharedBearerTokenProvider);
        let mut registry1 = LocalExtensionRegistry::new();
        registrar1(&mut registry1, "azure_auth");

        let instance2 = TestTokenProvider {
            token: "pipeline2_token".to_string(),
        };
        let registrar2 = crate::shared_extension_traits!(instance2 => SharedBearerTokenProvider);
        let mut registry2 = LocalExtensionRegistry::new();
        registrar2(&mut registry2, "azure_auth");

        let p1: Arc<dyn SharedBearerTokenProvider> = registry1
            .get_shared::<dyn SharedBearerTokenProvider>("azure_auth")
            .unwrap();
        let p2: Arc<dyn SharedBearerTokenProvider> = registry2
            .get_shared::<dyn SharedBearerTokenProvider>("azure_auth")
            .unwrap();

        assert!(
            !Arc::ptr_eq(&p1, &p2),
            "separate pipelines must have separate extension instances"
        );

        let registry1_clone = registry1.clone();
        let p1_again: Arc<dyn SharedBearerTokenProvider> = registry1_clone
            .get_shared::<dyn SharedBearerTokenProvider>("azure_auth")
            .unwrap();
        assert!(
            Arc::ptr_eq(&p1, &p1_again),
            "same pipeline must share the same extension instance"
        );
    }

    /// Proves that extracting the shared registry preserves Arc identity.
    #[test]
    fn test_into_shared_preserves_arc_identity() {
        let instance = TestTokenProvider {
            token: "test".to_string(),
        };
        let registrar = crate::shared_extension_traits!(instance => SharedBearerTokenProvider);

        let mut local_reg = LocalExtensionRegistry::new();
        registrar(&mut local_reg, "auth");

        let from_local: Arc<dyn SharedBearerTokenProvider> = local_reg
            .get_shared::<dyn SharedBearerTokenProvider>("auth")
            .unwrap();

        let shared_reg = local_reg.into_shared();
        let from_shared: Arc<dyn SharedBearerTokenProvider> = shared_reg
            .get::<dyn SharedBearerTokenProvider>("auth")
            .unwrap();

        assert!(Arc::ptr_eq(&from_local, &from_shared));
    }
}
