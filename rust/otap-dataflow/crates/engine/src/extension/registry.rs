// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension registry for storing and retrieving extension trait implementations by name.
//!
//! The registry stores `Box<dyn Any + Send>` for type-erased storage and produces
//! `Box<dyn Trait>` for trait-based lookups. It is `Clone` and `Send` — cloning
//! deep-copies each stored extension (which is cheap when the extension itself
//! wraps shared state in `Arc`).
//!
//! Extensions that publish traits override
//! [`Extension::extension_capabilities`](crate::extension::Extension::extension_capabilities),
//! using the [`extension_capabilities!`] macro in the factory to declare their
//! trait implementations. The engine inserts the results into the registry
//! during pipeline build.
//!
//! # Extension writer contract
//!
//! Extension structs that publish traits must be `Clone + Send + 'static`.
//! Shared mutable state (e.g. credentials, token senders) should be held behind
//! `Arc` so that independent clones still observe the same state.
//!
//! Extensions that don't publish any traits (pure background tasks) have no
//! `Clone` requirement.
//!
//! # Example
//!
//! ```ignore
//! // In the factory:
//! let ext = MyExtension::new(config)?;
//! let caps = extension_capabilities!(ext, BearerTokenProvider);
//! Ok(ExtensionWrapper::active(caps, ext, node_id, user_config, &cfg))
//!
//! // A consumer retrieves an owned trait object:
//! let provider: Box<dyn BearerTokenProvider> = registry
//!     .get::<dyn BearerTokenProvider>("azure_auth")?;
//! provider.get_token().await?;
//! ```

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use linkme::distributed_slice;

// ── Static capability name registry ─────────────────────────────────────────

/// All known capability names, collected at link time.
///
/// Each capability trait file adds an entry via [`register_capability!`].
/// This lets the engine distinguish a typo from a real capability that
/// simply isn't provided by any extension in the current config.
#[allow(unsafe_code)]
#[distributed_slice]
pub static KNOWN_CAPABILITIES: [&'static str];

// ── Sealed trait infrastructure ─────────────────────────────────────────────

// Sealed module — `pub(crate)` so the `register_capability!` macro can
// expand impls for `dyn Trait` types, while external crates cannot.
// Manual `impl Sealed` is blocked by `MacroToken`'s private field.
pub(crate) mod private {
    /// Proof that a capability was registered via [`register_capability!`](crate::register_capability).
    ///
    /// The private field makes this type unconstructable outside this module,
    /// so the only way to satisfy `Sealed::MACRO_TOKEN` is through the
    /// `MACRO_SEAL` const below — which only the macro uses.
    #[doc(hidden)]
    pub struct MacroToken(());

    /// The sole `MacroToken` instance. Used by [`register_capability!`](crate::register_capability) only.
    #[doc(hidden)]
    pub const MACRO_SEAL: MacroToken = MacroToken(());

    /// Sealing trait — prevents external crates from implementing
    /// [`ExtensionCapability`](super::ExtensionCapability).
    ///
    /// Within this crate, the `MACRO_TOKEN` associated const requires a
    /// [`MacroToken`] value, which cannot be constructed outside this module.
    /// Use [`register_capability!`](crate::register_capability) instead of
    /// implementing this trait manually.
    pub trait Sealed {
        /// Must be set to [`MACRO_SEAL`]. Only the macro can do this.
        #[doc(hidden)]
        const MACRO_TOKEN: MacroToken;
    }
}

/// Marker trait for extension trait types that can be stored in the
/// [`CapabilityRegistry`].
///
/// This trait is **sealed** and can only be implemented via the
/// [`register_capability!`](crate::register_capability) macro.
pub trait ExtensionCapability: private::Sealed {
    /// The stable, human-readable name for this capability.
    ///
    /// Used in YAML configuration for capability bindings:
    /// ```yaml
    /// capabilities:
    ///   bearer_token_provider: my_auth_extension
    /// ```
    const NAME: &'static str;

    /// A short description of what this capability provides.
    ///
    /// Surfaced in error messages, generated documentation, and CLI inspection.
    const DESCRIPTION: &'static str;
}

/// Error type for extension trait operations.
///
/// Thread-safe error type compatible with any `thiserror`-derived error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

// ── CloneAnySend helper trait ────────────────────────────────────────────────

/// Internal trait for type-erased, cloneable, `Send` storage.
///
/// Each concrete `T: Clone + Send + 'static` gets a blanket implementation.
/// `Box<dyn CloneAnySend>` implements `Clone` via `clone_box()`.
pub(crate) trait CloneAnySend: Send {
    /// Deep-clone into a new boxed trait object.
    fn clone_box(&self) -> Box<dyn CloneAnySend>;
    /// Access the concrete value as `&dyn Any` for downcasting.
    fn as_any_ref(&self) -> &dyn Any;
}

impl<T: Clone + Send + 'static> CloneAnySend for T {
    fn clone_box(&self) -> Box<dyn CloneAnySend> {
        Box::new(self.clone())
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl Clone for Box<dyn CloneAnySend> {
    fn clone(&self) -> Self {
        // Explicit double-deref so method resolution dispatches through the
        // vtable of `dyn CloneAnySend` (→ concrete type), NOT through the
        // blanket `CloneAnySend for Box<dyn CloneAnySend>` which would recurse.
        (**self).clone_box()
    }
}

// ── RegistryEntry ────────────────────────────────────────────────────────────

/// A single entry in the registry: a cloneable concrete value plus a coerce
/// function that knows how to produce `Box<dyn Any + Send>` (containing a
/// `Box<dyn Trait>`) from a `&dyn Any` reference pointing at the concrete type.
///
/// The `coerce` function pointer is monomorphised at registration time (inside
/// the [`extension_capabilities!`] macro) and is `Copy`, so the entry is
/// cheaply cloneable.
struct RegistryEntry {
    /// The concrete extension value, type-erased but cloneable.
    value: Box<dyn CloneAnySend>,
    /// Clones the concrete value out of `&dyn Any` and wraps it as
    /// `Box<Box<dyn Trait>>` erased to `Box<dyn Any + Send>`.
    coerce: fn(&dyn Any) -> Box<dyn Any + Send>,
    /// Human-readable capability name (from `ExtensionCapability::NAME`).
    capability_name: &'static str,
}

impl Clone for RegistryEntry {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            coerce: self.coerce,
            capability_name: self.capability_name,
        }
    }
}

// ── CapabilityRegistration ────────────────────────────────────────────────────────

/// A self-contained registration for one trait that an extension implements.
///
/// Produced by the [`extension_capabilities!`] macro. Each registration carries:
/// - A cloned copy of the concrete extension value (type-erased)
/// - A monomorphised `coerce` function pointer for producing `Box<dyn Trait>`
/// - The `TypeId` of `Box<dyn Trait>` for registry lookup
///
/// Extension factories produce these and pass them to
/// [`ExtensionWrapper::active`](crate::extension::ExtensionWrapper::active) or
/// [`ExtensionWrapper::passive`](crate::extension::ExtensionWrapper::passive);
/// the engine drains them during pipeline build.
pub struct CapabilityRegistration {
    /// `TypeId` of `Box<dyn Trait>` — used as registry lookup key.
    trait_id: TypeId,
    /// The concrete extension value, type-erased but cloneable.
    value: Box<dyn CloneAnySend>,
    /// Monomorphised fn: given `&dyn Any` pointing at the concrete extension
    /// type, clone it, wrap in `Box<dyn Trait>`, and return as
    /// `Box<dyn Any + Send>`.
    coerce: fn(&dyn Any) -> Box<dyn Any + Send>,
    /// Human-readable capability name (from `ExtensionCapability::NAME`).
    capability_name: &'static str,
}

impl CapabilityRegistration {
    /// Creates a new trait registration.
    ///
    /// This is intended for use by the [`extension_capabilities!`] macro — not for
    /// direct use by extension writers.
    #[doc(hidden)]
    pub fn new(
        trait_id: TypeId,
        value: impl Clone + Send + 'static,
        coerce: fn(&dyn Any) -> Box<dyn Any + Send>,
        capability_name: &'static str,
    ) -> Self {
        Self {
            trait_id,
            value: Box::new(value),
            coerce,
            capability_name,
        }
    }
}

// ── Public types ─────────────────────────────────────────────────────────────

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

// ── CapabilityRegistry ────────────────────────────────────────────────────────

/// Registry for extension trait implementations.
///
/// Extensions register themselves here during pipeline build so other components
/// can look them up by name and retrieve `Box<dyn Trait>` references.
///
/// The registry is `Clone` and `Send`. Cloning deep-copies each stored
/// extension value (cheap when the extension wraps shared state in `Arc`).
/// Each `get` call returns a freshly-cloned `Box<dyn Trait>`.
#[derive(Default, Clone)]
pub struct CapabilityRegistry {
    /// `(extension_name, TypeId::of::<Box<dyn Trait>>())` → `RegistryEntry`
    handles: HashMap<(String, TypeId), RegistryEntry>,
}

impl CapabilityRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    /// Insert pre-built trait registrations for an extension.
    ///
    /// Each [`CapabilityRegistration`] carries a cloned value and coerce function.
    /// This method inserts them into the registry keyed by `(name, trait_id)`.
    ///
    /// Called by the engine during pipeline build — not intended for direct use
    /// by extension writers.
    pub(crate) fn register_all(&mut self, name: &str, registrations: Vec<CapabilityRegistration>) {
        for reg in registrations {
            let entry = RegistryEntry {
                value: reg.value,
                coerce: reg.coerce,
                capability_name: reg.capability_name,
            };
            let _ = self.handles.insert((name.to_string(), reg.trait_id), entry);
        }
    }

    /// Get an owned clone of a trait implementation by extension name.
    ///
    /// Returns `Some(Box<dyn Trait>)` if found, `None` if no extension with
    /// that name exists or if it doesn't expose the requested trait.
    ///
    /// The returned value is a fresh clone produced from the stored extension
    /// value. The clone shares any `Arc`-wrapped state with the original and
    /// with other clones.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The trait type (e.g., `dyn BearerTokenProvider`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let provider: Box<dyn BearerTokenProvider> = registry
    ///     .get::<dyn BearerTokenProvider>("azure_auth")
    ///     .expect("auth extension required");
    /// provider.get_token().await?;
    /// ```
    pub fn get<T: ?Sized + 'static>(&self, name: &str) -> Option<Box<T>> {
        let key = (name.to_string(), TypeId::of::<Box<T>>());
        let entry = self.handles.get(&key)?;

        // Coerce produces Box<dyn Any + Send> that is actually Box<Box<dyn Trait>>.
        // Explicit deref (*entry.value) ensures we dispatch through the vtable
        // of `dyn CloneAnySend` to reach the concrete type, not the blanket
        // impl on `Box<dyn CloneAnySend>` itself.
        let erased = (entry.coerce)((*entry.value).as_any_ref());
        let double_boxed = erased
            .downcast::<Box<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");

        Some(*double_boxed)
    }

    /// Check if an extension exists by name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.handles.keys().any(|(n, _)| n == name)
    }

    /// Returns the number of registered extensions (unique names).
    #[must_use]
    pub fn len(&self) -> usize {
        self.handles
            .keys()
            .map(|(n, _)| n)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Returns true if no extensions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Returns an iterator over unique extension names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.handles
            .keys()
            .map(|(n, _)| n)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
    }

    /// Build a per-node registry from capability bindings.
    ///
    /// Takes a map of `capability_name → extension_instance_name` (from the
    /// node's `capabilities:` config section) and produces a new registry
    /// where each entry is keyed by the **capability name** instead of the
    /// extension instance name.
    ///
    /// This allows nodes to look up capabilities by their stable trait name
    /// (e.g., `"bearer_token_provider"`) without knowing which extension
    /// instance provides it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A binding references an extension instance that doesn't exist.
    /// - A capability name is not a known type (not in [`KNOWN_CAPABILITIES`]).
    /// - A capability is a known type but no loaded extension provides it.
    /// - A binding references a capability that the specific extension doesn't provide.
    pub fn resolve_bindings(
        &self,
        bindings: &HashMap<String, String>,
    ) -> Result<Capabilities, otap_df_config::error::Error> {
        let mut capabilities = Capabilities::new();
        for (capability_name, extension_name) in bindings {
            // 1. Extension must exist in the registry.
            if !self.contains(extension_name) {
                return Err(otap_df_config::error::Error::InvalidUserConfig {
                    error: format!(
                        "Capability binding '{capability_name}' references extension \
                         '{extension_name}', but no extension with that name exists. \
                         Check the 'extensions' section of your pipeline config.",
                    ),
                });
            }

            // 2. Capability name must be a known type (registered at link time).
            let is_known_type = KNOWN_CAPABILITIES.iter().any(|&name| name == capability_name);
            if !is_known_type {
                let all_known: Vec<&str> = KNOWN_CAPABILITIES.iter().copied().collect();
                return Err(otap_df_config::error::Error::InvalidUserConfig {
                    error: format!(
                        "Unknown capability '{capability_name}'. \
                         Known capability types: [{}].",
                        all_known.join(", "),
                    ),
                });
            }

            // 3. Some loaded extension must actually provide this capability.
            let provided_anywhere = self
                .handles
                .values()
                .any(|entry| entry.capability_name == capability_name);
            if !provided_anywhere {
                let extension_names: Vec<&String> = self.names().collect();
                return Err(otap_df_config::error::Error::InvalidUserConfig {
                    error: format!(
                        "Capability '{capability_name}' is a known type but no loaded \
                         extension provides it. Loaded extensions: [{}]. \
                         Add an extension that provides '{capability_name}' to your config.",
                        extension_names.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "),
                    ),
                });
            }

            // 4. The specific extension must provide the requested capability.
            let matched = self
                .handles
                .iter()
                .find(|((name, _), entry)| {
                    name == extension_name && entry.capability_name == capability_name
                });

            match matched {
                Some(((_, type_id), entry)) => {
                    capabilities.insert_entry(*type_id, entry.clone());
                }
                None => {
                    let available: Vec<&str> = self
                        .handles
                        .iter()
                        .filter(|((name, _), _)| name == extension_name)
                        .map(|(_, entry)| entry.capability_name)
                        .collect();
                    return Err(otap_df_config::error::Error::InvalidUserConfig {
                        error: format!(
                            "Extension '{extension_name}' does not provide capability \
                             '{capability_name}'. It provides: [{}].",
                            available.join(", "),
                        ),
                    });
                }
            }
        }
        Ok(capabilities)
    }
}

impl std::fmt::Debug for CapabilityRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&String> = self.names().collect();
        f.debug_struct("CapabilityRegistry")
            .field("extensions", &names)
            .finish()
    }
}

/// Registers a trait as a known extension capability.
///
/// This macro does three things:
/// 1. `impl Sealed for dyn $trait` — seals the trait
/// 2. `impl ExtensionCapability for dyn $trait` — sets `NAME`
/// 3. Registers the name in [`KNOWN_CAPABILITIES`] via `distributed_slice`
///
/// This is the only way to declare a new capability type. Using one macro
/// for all three steps makes it impossible to forget the static registration.
///
/// # Usage (in each extension trait file inside `extension/`)
///
/// ```ignore
/// crate::register_capability!(
///     BearerTokenProvider,
///     "bearer_token_provider",
///     "Provides bearer tokens for authenticated requests",
/// );
/// ```
#[macro_export]
macro_rules! register_capability {
    ($trait:ident, $name:literal, $description:literal $(,)?) => {
        impl $crate::extension::registry::private::Sealed for dyn $trait {
            const MACRO_TOKEN: $crate::extension::registry::private::MacroToken =
                $crate::extension::registry::private::MACRO_SEAL;
        }
        impl $crate::extension::registry::ExtensionCapability for dyn $trait {
            const NAME: &'static str = $name;
            const DESCRIPTION: &'static str = $description;
        }

        #[allow(unsafe_code)]
        #[$crate::distributed_slice($crate::extension::registry::KNOWN_CAPABILITIES)]
        static _KNOWN_CAP: &str = $name;
    };
}

/// Declares which capability traits an extension instance implements.
///
/// Returns `Vec<CapabilityRegistration>` — self-contained registrations each
/// carrying a cloned copy of the extension and a monomorphised coerce function.
///
/// Used in extension **factories** to produce capabilities that are passed to
/// [`ExtensionWrapper::active`](crate::extension::ExtensionWrapper::active) or
/// [`ExtensionWrapper::passive`](crate::extension::ExtensionWrapper::passive).
///
/// # Usage
///
/// ```ignore
/// let ext = MyExtension::new(config)?;
/// let caps = extension_capabilities!(ext => BearerTokenProvider, SomeOtherTrait);
/// Ok(ExtensionWrapper::active(caps, ext, node_id, user_config, &cfg))
/// ```
///
/// # Compile-time guarantees
///
/// - Each listed trait implements [`ExtensionCapability`] (sealed).
/// - The concrete type implements each listed trait plus `Clone + Send + 'static`.
#[macro_export]
macro_rules! extension_capabilities {
    ($instance:expr => $($trait:ident),+ $(,)?) => {{
        // Bind once — avoids multiple evaluations if $instance is an expression.
        let instance = &$instance;
        let mut registrations = Vec::<$crate::extension::registry::CapabilityRegistration>::new();
        $(
            {
                // Compile-time: trait must be a sealed ExtensionCapability.
                const _: fn() = || {
                    fn _assert<T: ?Sized + $crate::extension::registry::ExtensionCapability>() {}
                    _assert::<dyn $trait>();
                };

                // Single generic helper — T is inferred from `instance`.
                fn make_registration<T: Clone + Send + 'static + $trait>(
                    val: &T,
                ) -> $crate::extension::registry::CapabilityRegistration {
                    fn coerce<T: Clone + Send + 'static + $trait>(
                        any: &dyn std::any::Any,
                    ) -> Box<dyn std::any::Any + Send> {
                        let concrete = any
                            .downcast_ref::<T>()
                            .expect("registry entry type mismatch — this is a bug");
                        let boxed: Box<dyn $trait> = Box::new(concrete.clone());
                        Box::new(boxed) as Box<dyn std::any::Any + Send>
                    }

                    $crate::extension::registry::CapabilityRegistration::new(
                        std::any::TypeId::of::<Box<dyn $trait>>(),
                        val.clone(),
                        coerce::<T>,
                        <dyn $trait as $crate::extension::registry::ExtensionCapability>::NAME,
                    )
                }

                registrations.push(make_registration(instance));
            }
        )+
        registrations
    }};
}

/// Produces a `&'static [&'static str]` of capability names from trait types.
///
/// Use this in [`ExtensionFactory`](crate::ExtensionFactory) definitions so
/// the `capabilities` field is derived from the same sealed
/// [`ExtensionCapability::NAME`] constants — no hand-written strings that
/// could drift from what [`extension_capabilities!`](crate::extension_capabilities)
/// actually registers at runtime.
///
/// # Usage
///
/// ```ignore
/// capabilities: otap_df_engine::extension_capability_names!(BearerTokenProvider),
/// ```
#[macro_export]
macro_rules! extension_capability_names {
    ($($trait:ident),+ $(,)?) => {
        &[$(<dyn $trait as $crate::extension::registry::ExtensionCapability>::NAME),+]
    };
}

// ── Capabilities ─────────────────────────────────────────────────────────

/// Per-node capability instances resolved from config bindings.
///
/// Built by the engine from the node's `capabilities` config section and
/// the global [`CapabilityRegistry`]. Nodes receive this at factory time
/// and look up capabilities by type only — no extension names needed.
///
/// # Example
///
/// ```ignore
/// // Required capability — fails with a clear error if not bound
/// let auth = capabilities.require::<dyn BearerTokenProvider>()?;
///
/// // Optional capability — returns None if not bound
/// let enrichment = capabilities.optional::<dyn DatasetLookup>();
/// ```
pub struct Capabilities {
    resolved: HashMap<TypeId, RegistryEntry>,
    /// Tracks which TypeIds were accessed via `require()` or `optional()`.
    /// Uses `RefCell` so that `require`/`optional` can take `&self`.
    accessed: RefCell<HashSet<TypeId>>,
}

impl std::fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.resolved.values().map(|e| e.capability_name).collect();
        f.debug_struct("Capabilities")
            .field("capabilities", &names)
            .finish()
    }
}

impl Capabilities {
    /// Creates an empty `Capabilities`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            resolved: HashMap::new(),
            accessed: RefCell::new(HashSet::new()),
        }
    }

    /// Insert a resolved capability. Called by the engine during build.
    fn insert_entry(&mut self, type_id: TypeId, entry: RegistryEntry) {
        let _ = self.resolved.insert(type_id, entry);
    }

    /// Require a capability by trait type.
    ///
    /// Returns the capability if bound, or a standardized error with
    /// the capability name and guidance on how to fix the config.
    ///
    /// Use this for capabilities that the node cannot function without.
    pub fn require<T: ExtensionCapability + ?Sized + 'static>(
        &self,
    ) -> Result<Box<T>, otap_df_config::error::Error> {
        self.get::<T>().ok_or_else(|| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: format!(
                    "Missing required capability '{}'. Add to your node config:\n  capabilities:\n    {}: <extension_instance_name>",
                    T::NAME,
                    T::NAME,
                ),
            }
        })
    }

    /// Get an optional capability by trait type.
    ///
    /// Returns `Some(Box<dyn Trait>)` if the capability was bound,
    /// `None` if it was not configured for this node.
    ///
    /// Use this for capabilities that enhance the node but are not required.
    pub fn optional<T: ?Sized + 'static>(&self) -> Option<Box<T>> {
        self.get::<T>()
    }

    /// Returns `true` if no capabilities are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.resolved.is_empty()
    }

    /// Returns the capability names that were resolved from config bindings
    /// but never consumed by the factory via `require()` or `optional()`.
    ///
    /// Called by the engine after the factory `create()` returns to detect
    /// misconfigured or unnecessary capability bindings.
    #[must_use]
    pub fn unused_bindings(&self) -> Vec<&'static str> {
        let accessed = self.accessed.borrow();
        self.resolved
            .iter()
            .filter(|(type_id, _)| !accessed.contains(type_id))
            .map(|(_, entry)| entry.capability_name)
            .collect()
    }

    /// Internal typed lookup — clones via the stored coerce function.
    fn get<T: ?Sized + 'static>(&self) -> Option<Box<T>> {
        let key = TypeId::of::<Box<T>>();
        let entry = self.resolved.get(&key)?;
        let _ = self.accessed.borrow_mut().insert(key);
        let erased = (entry.coerce)((*entry.value).as_any_ref());
        let double_boxed = erased
            .downcast::<Box<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");
        Some(*double_boxed)
    }
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extension::bearer_token_provider::BearerToken;
    use crate::extension::bearer_token_provider::BearerTokenProvider;
    use tokio::sync::watch;

    #[derive(Clone)]
    struct TestTokenProvider {
        token: String,
    }

    #[async_trait::async_trait]
    impl BearerTokenProvider for TestTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    /// Helper: register a TestTokenProvider with the given name.
    fn register_provider(registry: &mut CapabilityRegistry, name: &str, token: &str) {
        let instance = TestTokenProvider {
            token: token.to_string(),
        };
        let regs = crate::extension_capabilities!(instance => BearerTokenProvider);
        registry.register_all(name, regs);
    }

    #[test]
    fn test_register_and_get() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "test_ext", "test_token");

        let result: Option<Box<dyn BearerTokenProvider>> =
            registry.get::<dyn BearerTokenProvider>("test_ext");
        assert!(result.is_some());
    }

    #[test]
    fn test_get_returns_independent_clones() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "ext", "shared_test");

        let a: Box<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("ext").unwrap();
        let b: Box<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("ext").unwrap();

        // Both are independent clones (different pointers)
        assert!(!std::ptr::eq(
            &*a as *const dyn BearerTokenProvider,
            &*b as *const dyn BearerTokenProvider,
        ));
    }

    #[test]
    fn test_registry_clone_produces_deep_copy() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "ext", "clone_test");

        let cloned = registry.clone();

        let from_original: Box<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("ext").unwrap();
        let from_clone: Box<dyn BearerTokenProvider> =
            cloned.get::<dyn BearerTokenProvider>("ext").unwrap();

        // Deep copy — different pointers
        assert!(!std::ptr::eq(
            &*from_original as *const dyn BearerTokenProvider,
            &*from_clone as *const dyn BearerTokenProvider,
        ));
    }

    #[test]
    fn test_not_found() {
        let registry = CapabilityRegistry::new();
        let result = registry.get::<dyn BearerTokenProvider>("missing");
        assert!(result.is_none());
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
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "test_ext", "test");

        let debug_str = format!("{:?}", registry);
        assert!(debug_str.contains("CapabilityRegistry"));
        assert!(debug_str.contains("test_ext"));
    }

    #[test]
    fn test_contains_and_len() {
        let mut registry = CapabilityRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);

        register_provider(&mut registry, "ext", "test");
        assert!(registry.contains("ext"));
        assert!(!registry.contains("missing"));
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn test_get_extension_actually_works() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "auth", "real_token");

        let provider: Box<dyn BearerTokenProvider> =
            registry.get::<dyn BearerTokenProvider>("auth").unwrap();
        let token = provider.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "real_token");
    }

    #[test]
    fn test_multiple_extensions_same_trait() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_prod", "prod_token");
        register_provider(&mut registry, "azure_staging", "staging_token");

        assert_eq!(registry.len(), 2);

        let _p1 = registry
            .get::<dyn BearerTokenProvider>("azure_prod")
            .unwrap();
        let _p2 = registry
            .get::<dyn BearerTokenProvider>("azure_staging")
            .unwrap();
    }

    #[test]
    fn test_registry_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<CapabilityRegistry>();
    }

    #[test]
    fn test_resolve_bindings_unknown_extension() {
        let registry = CapabilityRegistry::new();
        let bindings = HashMap::from([
            ("bearer_token_provider".to_string(), "nonexistent".to_string()),
        ]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nonexistent"), "should name the missing extension: {msg}");
        assert!(msg.contains("no extension with that name exists"), "{msg}");
    }

    #[test]
    fn test_resolve_bindings_unknown_capability_name() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([
            ("totally_made_up".to_string(), "azure_auth".to_string()),
        ]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Unknown capability"), "should say unknown: {msg}");
        assert!(msg.contains("totally_made_up"), "{msg}");
        assert!(msg.contains("bearer_token_provider"), "should list known caps: {msg}");
    }

    #[test]
    fn test_resolve_bindings_valid() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([
            ("bearer_token_provider".to_string(), "azure_auth".to_string()),
        ]);
        let caps = registry.resolve_bindings(&bindings).unwrap();
        assert!(!caps.is_empty());
    }

    /// Helper: register a fake extension that only has entries under a custom
    /// capability name (simulates a second trait type for testing).
    fn register_fake_capability(registry: &mut CapabilityRegistry, ext_name: &str, cap_name: &'static str) {
        let instance = TestTokenProvider { token: "fake".to_string() };
        // Build a registration but override the capability_name.
        let reg = CapabilityRegistration::new(
            // Use a different TypeId so it doesn't collide with BearerTokenProvider.
            // We use TypeId::of::<Box<dyn std::fmt::Debug>>() as a stand-in.
            TypeId::of::<Box<dyn std::fmt::Debug>>(),
            instance,
            |any| {
                let concrete = any.downcast_ref::<TestTokenProvider>().unwrap();
                Box::new(Box::new(concrete.clone()) as Box<dyn BearerTokenProvider>)
            },
            cap_name,
        );
        registry.register_all(ext_name, vec![reg]);
    }

    #[test]
    fn test_resolve_bindings_known_type_no_provider() {
        // Extension "other_ext" exists but only provides "other_cap", not bearer_token_provider.
        let mut registry = CapabilityRegistry::new();
        register_fake_capability(&mut registry, "other_ext", "other_cap");
        let bindings = HashMap::from([
            ("bearer_token_provider".to_string(), "other_ext".to_string()),
        ]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("no loaded extension provides it"), "should say no provider: {msg}");
        assert!(msg.contains("bearer_token_provider"), "{msg}");
    }

    #[test]
    fn test_resolve_bindings_extension_lacks_specific_cap() {
        // Two extensions: azure_auth provides bearer_token_provider,
        // other_ext provides other_cap. Binding bearer_token_provider → other_ext
        // should fail with "does not provide capability".
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        register_fake_capability(&mut registry, "other_ext", "other_cap");
        let bindings = HashMap::from([
            ("bearer_token_provider".to_string(), "other_ext".to_string()),
        ]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not provide capability"), "should say missing: {msg}");
        assert!(msg.contains("other_ext"), "{msg}");
        assert!(msg.contains("other_cap"), "should list what it provides: {msg}");
    }

    #[test]
    fn test_require_missing_capability() {
        let caps = Capabilities::new();
        let result = caps.require::<dyn BearerTokenProvider>();
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("Missing required capability"), "{msg}");
        assert!(msg.contains("bearer_token_provider"), "{msg}");
    }

    #[test]
    fn test_unused_bindings_detected() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([
            ("bearer_token_provider".to_string(), "azure_auth".to_string()),
        ]);
        let caps = registry.resolve_bindings(&bindings).unwrap();

        // Before any access, all bindings are unused.
        let unused = caps.unused_bindings();
        assert_eq!(unused, vec!["bearer_token_provider"]);

        // After accessing, none are unused.
        let _ = caps.require::<dyn BearerTokenProvider>().unwrap();
        let unused = caps.unused_bindings();
        assert!(unused.is_empty(), "after require(), should be empty: {unused:?}");
    }
}
