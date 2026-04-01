// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension registry for storing and retrieving extension trait implementations by name.
//!
//! The registry stores `Box<dyn Any + Send>` for type-erased storage and produces
//! `Box<dyn Trait>` for trait-based lookups. It is `Clone` — cloning
//! deep-copies each stored extension (which is cheap when the extension itself
//! wraps shared state in `Arc`).
//!
//! Extensions that publish traits use the [`extension_capabilities!`] macro,
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
//! // In a node factory, consumers ask for a capability:
//! let auth = capabilities.require_local::<BearerTokenProvider>()?;
//! // Or for shared (Send) consumers:
//! let kv = capabilities.require_shared::<KeyValueStore>()?;
//!
//! // Lower-level trait lookup remains available inside engine internals:
//! let provider: Box<dyn shared::BearerTokenProvider> = registry
//!     .get::<dyn shared::BearerTokenProvider>("azure_auth")?;
//! provider.get_token().await?;
//! ```

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

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

/// Trait for type-erased, cloneable, `Send` storage.
///
/// Each concrete `T: Clone + Send + 'static` gets a blanket implementation.
/// `Box<dyn CloneAnySend>` implements `Clone` via `clone_box()`.
pub trait CloneAnySend: Send {
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
        (**self).clone_box()
    }
}

// ── local module ─────────────────────────────────────────────────────────────

/// Local (!Send) capability registry types.
///
/// Entries store `Rc<dyn Any>` — all consumers share the same allocation via
/// `Rc::clone`. True single-instance.
pub mod local {
    use std::any::{Any, TypeId};
    use std::rc::Rc;

    /// A single local registry entry: Rc-backed, single instance.
    #[derive(Clone)]
    pub struct RegistryEntry {
        pub(crate) value: Rc<dyn Any>,
        pub(crate) coerce: fn(Rc<dyn Any>) -> Box<dyn Any>,
        pub(crate) capability_name: &'static str,
    }

    /// A registration for one local capability trait implementation.
    pub struct CapabilityRegistration {
        pub(crate) trait_id: TypeId,
        pub(crate) value: Rc<dyn Any>,
        pub(crate) coerce: fn(Rc<dyn Any>) -> Box<dyn Any>,
        pub(crate) capability_name: &'static str,
    }
}

// ── shared module ────────────────────────────────────────────────────────────

/// Shared (Send) capability registry types.
///
/// Entries store `Box<dyn CloneAnySend>` — each consumer gets a clone (cheap
/// when the extension wraps shared state in `Arc`).
pub mod shared {
    pub(crate) use super::CloneAnySend;
    use std::any::{Any, TypeId};

    /// A single shared registry entry: clone-based.
    pub struct RegistryEntry {
        pub(crate) value: Box<dyn CloneAnySend>,
        pub(crate) coerce: fn(&dyn Any) -> Box<dyn Any + Send>,
        pub(crate) capability_name: &'static str,
        /// TypeId of the corresponding local `Rc<dyn local::Trait>` for fallback.
        pub(crate) local_trait_id: Option<TypeId>,
        /// Produces a local entry from this shared entry (for shared→local fallback).
        pub(crate) adapt_to_local: Option<fn(&Self) -> super::local::RegistryEntry>,
    }

    impl Clone for RegistryEntry {
        fn clone(&self) -> Self {
            Self {
                value: self.value.clone(),
                coerce: self.coerce,
                capability_name: self.capability_name,
                local_trait_id: self.local_trait_id,
                adapt_to_local: self.adapt_to_local,
            }
        }
    }

    /// A registration for one shared capability trait implementation.
    pub struct CapabilityRegistration {
        pub(crate) trait_id: TypeId,
        pub(crate) value: Box<dyn CloneAnySend>,
        pub(crate) coerce: fn(&dyn Any) -> Box<dyn Any + Send>,
        pub(crate) capability_name: &'static str,
        /// TypeId of the corresponding local Rc<dyn Trait> for fallback lookup.
        pub(crate) local_trait_id: Option<TypeId>,
        /// Produces a local entry from this shared entry.
        pub(crate) adapt_to_local: Option<fn(&RegistryEntry) -> super::local::RegistryEntry>,
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
/// The registry is `Clone`. Cloning deep-copies each stored extension value
/// (cheap when the extension wraps shared state in `Arc`).
/// Each `get` call returns a freshly-cloned `Box<dyn Trait>`.
#[derive(Default, Clone)]
pub struct CapabilityRegistry {
    /// `(extension_name, TypeId::of::<Box<dyn Trait>>())` → local entry
    local_handles: HashMap<(String, TypeId), local::RegistryEntry>,
    /// `(extension_name, TypeId::of::<Box<dyn Trait>>())` → shared entry
    shared_handles: HashMap<(String, TypeId), shared::RegistryEntry>,
}

impl CapabilityRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            local_handles: HashMap::new(),
            shared_handles: HashMap::new(),
        }
    }

    /// Insert pre-built trait registrations for an extension.
    ///
    /// Each [`CapabilityRegistration`] carries either a cloned value + coerce function
    /// or a factory function. This method inserts them into the registry keyed by
    /// `(name, trait_id)`.
    ///
    /// Called by the engine during pipeline build — not intended for direct use
    /// by extension writers.
    pub(crate) fn register_all_shared(
        &mut self,
        name: &str,
        registrations: Vec<shared::CapabilityRegistration>,
    ) {
        for reg in registrations {
            let entry = shared::RegistryEntry {
                value: reg.value,
                coerce: reg.coerce,
                capability_name: reg.capability_name,
                local_trait_id: reg.local_trait_id,
                adapt_to_local: reg.adapt_to_local,
            };
            let _ = self
                .shared_handles
                .insert((name.to_string(), reg.trait_id), entry);
        }
    }

    pub(crate) fn register_all_local(
        &mut self,
        name: &str,
        registrations: Vec<local::CapabilityRegistration>,
    ) {
        for reg in registrations {
            let entry = local::RegistryEntry {
                value: reg.value,
                coerce: reg.coerce,
                capability_name: reg.capability_name,
            };
            let _ = self
                .local_handles
                .insert((name.to_string(), reg.trait_id), entry);
        }
    }

    /// Get a shared trait implementation by extension name.
    #[must_use]
    pub fn get<T: ?Sized + 'static>(&self, name: &str) -> Option<Box<T>> {
        let key = (name.to_string(), TypeId::of::<Box<T>>());
        let entry = self.shared_handles.get(&key)?;
        // Deref chain: &Box<dyn CloneAnySend> → &dyn CloneAnySend → concrete type.
        let erased = (entry.coerce)(entry.value.as_ref().as_any_ref());
        let double_boxed = erased
            .downcast::<Box<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");
        Some(*double_boxed)
    }

    /// Check if an extension exists by name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.local_handles.keys().any(|(n, _)| n == name)
            || self.shared_handles.keys().any(|(n, _)| n == name)
    }

    /// Returns the number of registered extensions (unique names).
    #[must_use]
    pub fn len(&self) -> usize {
        self.local_handles
            .keys()
            .chain(self.shared_handles.keys())
            .map(|(n, _)| n)
            .collect::<HashSet<_>>()
            .len()
    }

    /// Returns true if no extensions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.local_handles.is_empty() && self.shared_handles.is_empty()
    }

    /// Returns an iterator over unique extension names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.local_handles
            .keys()
            .chain(self.shared_handles.keys())
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
            let is_known_type = KNOWN_CAPABILITIES
                .iter()
                .any(|&name| name == capability_name);
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
                .local_handles
                .values()
                .any(|entry| entry.capability_name == capability_name)
                || self
                    .shared_handles
                    .values()
                    .any(|entry| entry.capability_name == capability_name);
            if !provided_anywhere {
                let extension_names: Vec<&String> = self.names().collect();
                return Err(otap_df_config::error::Error::InvalidUserConfig {
                    error: format!(
                        "Capability '{capability_name}' is a known type but no loaded \
                         extension provides it. Loaded extensions: [{}]. \
                         Add an extension that provides '{capability_name}' to your config.",
                        extension_names
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", "),
                    ),
                });
            }

            // 4. The specific extension must provide the requested capability.
            //    Populate both local and shared entries — the consumer picks
            //    which variant to use via require_local() or require_shared().
            let mut found_any = false;

            let matched_local_entries: Vec<_> = self
                .local_handles
                .iter()
                .filter(|((name, _), entry)| {
                    name == extension_name && entry.capability_name == capability_name
                })
                .collect();

            let matched_shared_entries: Vec<_> = self
                .shared_handles
                .iter()
                .filter(|((name, _), entry)| {
                    name == extension_name && entry.capability_name == capability_name
                })
                .collect();

            for ((_, type_id), entry) in &matched_local_entries {
                capabilities.insert_local_entry(*type_id, (*entry).clone());
                found_any = true;
            }

            for ((_, type_id), entry) in &matched_shared_entries {
                capabilities.insert_shared_entry(*type_id, (*entry).clone());
                found_any = true;

                // Pre-populate local fallback from shared via adapter.
                if let (Some(local_tid), Some(adapt)) = (entry.local_trait_id, entry.adapt_to_local)
                {
                    if !capabilities.has_local_entry(local_tid) {
                        capabilities.insert_local_entry(local_tid, adapt(entry));
                    }
                }
            }

            if !found_any {
                let available: Vec<&str> = self
                    .local_handles
                    .iter()
                    .filter(|((name, _), _)| name == extension_name)
                    .map(|(_, entry)| entry.capability_name)
                    .chain(
                        self.shared_handles
                            .iter()
                            .filter(|((name, _), _)| name == extension_name)
                            .map(|(_, entry)| entry.capability_name),
                    )
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

/// Registers a capability: seals the handle type, sets metadata, registers in
/// [`KNOWN_CAPABILITIES`], and generates `shared_capabilities()` /
/// `local_capabilities()` glue code.
///
/// This is the **only** way to declare a new capability type. A single macro
/// call does everything — sealing, metadata, link-time registration, and
/// coercion functions — so nothing can be forgotten.
///
/// # Usage
///
/// ```ignore
/// crate::register_capability!(
///     BearerTokenProvider,          // handle type (the enum)
///     local::BearerTokenProvider,   // local (!Send) trait
///     shared::BearerTokenProvider,  // shared (Send) trait
///     "bearer_token_provider",      // stable name for config bindings
///     "Provides bearer tokens for authenticated HTTP/gRPC requests",
/// );
/// ```
#[macro_export]
macro_rules! register_capability {
    ($handle:ident, $local_trait:path, $shared_trait:path, $name:literal, $description:literal $(,)?) => {
        // ── Seal the handle type ────────────────────────────────────────
        impl $crate::capability::registry::private::Sealed for $handle {
            const MACRO_TOKEN: $crate::capability::registry::private::MacroToken =
                $crate::capability::registry::private::MACRO_SEAL;
        }
        impl $crate::capability::registry::ExtensionCapability for $handle {
            const NAME: &'static str = $name;
            const DESCRIPTION: &'static str = $description;
        }

        // ── Link-time capability name registration ──────────────────────
        ::paste::paste! {
            #[allow(unsafe_code)]
            #[$crate::distributed_slice($crate::capability::registry::KNOWN_CAPABILITIES)]
            static [<_KNOWN_CAP_ $handle:upper>]: &str = $name;
        }

        // ── Coercion glue: shared_capabilities / local_capabilities ─────
        impl $handle {
            /// Build shared capability registrations for this handle.
            pub fn shared_capabilities<T>(
                instance: &T,
            ) -> Vec<$crate::capability::registry::shared::CapabilityRegistration>
            where
                T: Clone + Send + 'static + $shared_trait,
            {
                fn make_registration<TInner: Clone + Send + 'static + $shared_trait>(
                    val: &TInner,
                ) -> $crate::capability::registry::shared::CapabilityRegistration {
                    fn coerce<TInner: Clone + Send + 'static + $shared_trait>(
                        any: &dyn std::any::Any,
                    ) -> Box<dyn std::any::Any + Send> {
                        let concrete = any
                            .downcast_ref::<TInner>()
                            .expect("registry entry type mismatch — this is a bug");
                        let boxed: Box<dyn $shared_trait> = Box::new(concrete.clone());
                        Box::new(boxed) as Box<dyn std::any::Any + Send>
                    }

                    $crate::capability::registry::shared::CapabilityRegistration {
                        trait_id: std::any::TypeId::of::<Box<dyn $shared_trait>>(),
                        value: Box::new(val.clone()),
                        coerce: coerce::<TInner>,
                        capability_name:
                            <$handle as $crate::capability::registry::ExtensionCapability>::NAME,
                        local_trait_id: Some(
                            std::any::TypeId::of::<std::rc::Rc<dyn $local_trait>>(),
                        ),
                        adapt_to_local: Some($handle::_adapt_shared_entry_to_local),
                    }
                }

                vec![make_registration(instance)]
            }

            /// Build local capability registrations (Rc-based) for this handle.
            pub fn local_capabilities<T>(
                instance: &std::rc::Rc<T>,
            ) -> Vec<$crate::capability::registry::local::CapabilityRegistration>
            where
                T: 'static + $local_trait,
            {
                fn make_registration<TInner: 'static + $local_trait>(
                    rc: &std::rc::Rc<TInner>,
                ) -> $crate::capability::registry::local::CapabilityRegistration {
                    fn coerce<TInner: 'static + $local_trait>(
                        rc_any: std::rc::Rc<dyn std::any::Any>,
                    ) -> Box<dyn std::any::Any> {
                        let concrete: std::rc::Rc<TInner> = rc_any
                            .downcast::<TInner>()
                            .expect("registry entry type mismatch — this is a bug");
                        let trait_obj: std::rc::Rc<dyn $local_trait> = concrete;
                        Box::new(trait_obj) as Box<dyn std::any::Any>
                    }

                    $crate::capability::registry::local::CapabilityRegistration {
                        trait_id: std::any::TypeId::of::<std::rc::Rc<dyn $local_trait>>(),
                        value: std::rc::Rc::clone(rc) as std::rc::Rc<dyn std::any::Any>,
                        coerce: coerce::<TInner>,
                        capability_name:
                            <$handle as $crate::capability::registry::ExtensionCapability>::NAME,
                    }
                }

                vec![make_registration(instance)]
            }
        }
    };
}

/// Extension capabilities descriptor.
///
/// Carries capability names (for documentation/validation) and registration
/// functions for both shared and local variants. Produced by
/// [`extension_capabilities!`](crate::extension_capabilities).
///
/// Both `register_shared` and `register_local` are always present. The engine
/// calls whichever is needed based on what the `create` fn returned.
#[derive(Clone, Copy)]
pub struct ExtensionCapabilities {
    /// Human-readable capability names.
    pub names: &'static [&'static str],
    /// Registration function for shared capabilities.
    pub register_shared: fn(&dyn Any) -> Vec<shared::CapabilityRegistration>,
    /// Registration function for local capabilities (Rc-based).
    pub register_local: fn(Rc<dyn Any>) -> Vec<local::CapabilityRegistration>,
}

/// Produces an extension capabilities descriptor with both shared and local
/// registration functions.
///
/// # Single-type usage (one type implements both local and shared traits):
///
/// ```ignore
/// extension_capabilities!(MyExtension => BearerTokenProvider, HealthCheck)
/// ```
///
/// # Dual-type usage (separate local and shared implementations):
///
/// ```ignore
/// extension_capabilities!(
///     shared: shared::MyExtension,
///     local: local::MyExtension
///     => BearerTokenProvider, HealthCheck
/// )
/// ```
#[macro_export]
macro_rules! extension_capabilities {
    // Single type — implements both local and shared traits
    ($type:ty => $($handle:path),+ $(,)?) => {
        $crate::capability::registry::ExtensionCapabilities {
            names: &[$(<$handle as $crate::capability::registry::ExtensionCapability>::NAME),+],
            register_shared: |any: &dyn std::any::Any| -> Vec<$crate::capability::registry::shared::CapabilityRegistration> {
                let ext = any.downcast_ref::<$type>()
                    .expect("extension type mismatch — this is a bug");
                let mut caps = Vec::new();
                $(caps.extend(<$handle>::shared_capabilities(ext));)+
                caps
            },
            register_local: |rc_any: std::rc::Rc<dyn std::any::Any>| -> Vec<$crate::capability::registry::local::CapabilityRegistration> {
                let rc = rc_any.downcast::<$type>()
                    .expect("extension type mismatch — this is a bug");
                let mut caps = Vec::new();
                $(caps.extend(<$handle>::local_capabilities(&rc));)+
                caps
            },
        }
    };
    // Dual types — separate local and shared implementations
    (shared: $shared_type:ty, local: $local_type:ty => $($handle:path),+ $(,)?) => {
        $crate::capability::registry::ExtensionCapabilities {
            names: &[$(<$handle as $crate::capability::registry::ExtensionCapability>::NAME),+],
            register_shared: |any: &dyn std::any::Any| -> Vec<$crate::capability::registry::shared::CapabilityRegistration> {
                let ext = any.downcast_ref::<$shared_type>()
                    .expect("extension type mismatch — this is a bug");
                let mut caps = Vec::new();
                $(caps.extend(<$handle>::shared_capabilities(ext));)+
                caps
            },
            register_local: |rc_any: std::rc::Rc<dyn std::any::Any>| -> Vec<$crate::capability::registry::local::CapabilityRegistration> {
                let rc = rc_any.downcast::<$local_type>()
                    .expect("extension type mismatch — this is a bug");
                let mut caps = Vec::new();
                $(caps.extend(<$handle>::local_capabilities(&rc));)+
                caps
            },
        }
    };
}

// ── Capabilities ─────────────────────────────────────────────────────────

// ── Capabilities (per-node resolved bindings) ────────────────────────────────

/// Per-node resolved capability bindings.
///
/// Produced by [`CapabilityRegistry::resolve_bindings`] during pipeline build.
/// Nodes use `require_local()`, `require_shared()`, `optional_local()`, or
/// `optional_shared()` in their factory to retrieve capabilities.
///
/// # Example
///
/// ```ignore
/// // Required local capability — prefers local, falls back to shared via adapter
/// let auth = capabilities.require_local::<BearerTokenProvider>()?;
///
/// // Required shared capability — Send-compatible, no enum wrapper
/// let kv = capabilities.require_shared::<KeyValueStore>()?;
/// ```
pub struct Capabilities {
    local_resolved: HashMap<TypeId, local::RegistryEntry>,
    shared_resolved: HashMap<TypeId, shared::RegistryEntry>,
    /// Tracks which capability names were accessed via `require_local()`, `require_shared()`, etc.
    /// Uses `RefCell` so that these methods can take `&self`.
    accessed_capability_names: RefCell<HashSet<&'static str>>,
    /// Tracks whether any local variant was consumed.
    accessed_local: RefCell<bool>,
    /// Tracks whether any shared variant was consumed.
    accessed_shared: RefCell<bool>,
}

impl std::fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self
            .local_resolved
            .values()
            .map(|e| e.capability_name)
            .chain(self.shared_resolved.values().map(|e| e.capability_name))
            .collect();
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
            local_resolved: HashMap::new(),
            shared_resolved: HashMap::new(),
            accessed_capability_names: RefCell::new(HashSet::new()),
            accessed_local: RefCell::new(false),
            accessed_shared: RefCell::new(false),
        }
    }

    /// Insert a resolved local capability. Called by the engine during build.
    fn insert_local_entry(&mut self, type_id: TypeId, entry: local::RegistryEntry) {
        let _ = self.local_resolved.insert(type_id, entry);
    }

    /// Check if a local entry exists for the given TypeId.
    fn has_local_entry(&self, type_id: TypeId) -> bool {
        self.local_resolved.contains_key(&type_id)
    }

    /// Insert a resolved shared capability. Called by the engine during build.
    fn insert_shared_entry(&mut self, type_id: TypeId, entry: shared::RegistryEntry) {
        let _ = self.shared_resolved.insert(type_id, entry);
    }

    /// Returns `true` if no capabilities are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.local_resolved.is_empty() && self.shared_resolved.is_empty()
    }

    /// Returns the capability names that were resolved from config bindings
    /// but never consumed by the factory via `require_local()`, `require_shared()`, etc.
    ///
    /// Called by the engine after the factory `create()` returns to detect
    /// misconfigured or unnecessary capability bindings.
    #[must_use]
    pub fn unused_bindings(&self) -> Vec<&'static str> {
        let accessed = self.accessed_capability_names.borrow();
        self.local_resolved
            .values()
            .map(|entry| entry.capability_name)
            .chain(
                self.shared_resolved
                    .values()
                    .map(|entry| entry.capability_name),
            )
            .filter(|name| !accessed.contains(name))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Returns `true` if any local variant was consumed by `require_local()` or `optional_local()`.
    #[must_use]
    pub fn consumed_local(&self) -> bool {
        *self.accessed_local.borrow()
    }

    /// Returns `true` if any shared variant was consumed by `require_shared()` or `optional_shared()`.
    #[must_use]
    pub fn consumed_shared(&self) -> bool {
        *self.accessed_shared.borrow()
    }

    // ── Typed capability API ────────────────────────────────────────────

    /// Require a local capability.
    ///
    /// Returns `Rc<dyn local::Trait>`. Pre-populated fallback from shared
    /// extensions means this is a flat lookup — no adapter logic at call time.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth: Rc<dyn local::BearerTokenProvider> =
    ///     capabilities.require_local::<dyn local::BearerTokenProvider>()?;
    /// ```
    pub fn require_local<T: crate::local::capability::Sealed + ?Sized + 'static>(
        &self,
    ) -> Result<Rc<T>, otap_df_config::error::Error> {
        self.get_local_raw::<T>().ok_or_else(|| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: "Missing required capability. Add to your node config:\n  capabilities:\n    <capability_name>: <extension_instance_name>".to_string(),
            }
        }).inspect(|_| {
            *self.accessed_local.borrow_mut() = true;
        })
    }

    /// Require a shared capability.
    ///
    /// Returns `Box<dyn shared::Trait>` (which is `Send`). Only the shared
    /// variant is considered — local-only extensions will cause an error.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let kv: Box<dyn shared::KeyValueStore> =
    ///     capabilities.require_shared::<dyn shared::KeyValueStore>()?;
    /// ```
    pub fn require_shared<T: crate::shared::capability::Sealed + ?Sized + 'static>(
        &self,
    ) -> Result<Box<T>, otap_df_config::error::Error> {
        self.get_shared_raw::<T>().ok_or_else(|| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: "Missing required shared capability. The extension must provide a shared (Send) implementation.\n  capabilities:\n    <capability_name>: <extension_instance_name>".to_string(),
            }
        }).inspect(|_| {
            *self.accessed_shared.borrow_mut() = true;
        })
    }

    /// Get an optional local capability.
    ///
    /// Returns `None` if the capability was not configured for this node.
    pub fn optional_local<T: crate::local::capability::Sealed + ?Sized + 'static>(
        &self,
    ) -> Option<Rc<T>> {
        let result = self.get_local_raw::<T>();
        if result.is_some() {
            *self.accessed_local.borrow_mut() = true;
        }
        result
    }

    /// Get an optional shared capability.
    ///
    /// Returns `None` if the capability was not configured or no shared
    /// variant is available.
    pub fn optional_shared<T: crate::shared::capability::Sealed + ?Sized + 'static>(
        &self,
    ) -> Option<Box<T>> {
        let result = self.get_shared_raw::<T>();
        if result.is_some() {
            *self.accessed_shared.borrow_mut() = true;
        }
        result
    }

    /// Internal local typed lookup — returns `Rc<dyn Trait>` for true single-instance sharing.
    fn get_local_raw<T: ?Sized + 'static>(&self) -> Option<Rc<T>> {
        let key = TypeId::of::<Rc<T>>();
        let entry = self.local_resolved.get(&key)?;
        let _ = self
            .accessed_capability_names
            .borrow_mut()
            .insert(entry.capability_name);
        let erased = (entry.coerce)(Rc::clone(&entry.value));
        let inner = erased
            .downcast::<Rc<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");
        Some(*inner)
    }

    /// Internal shared typed lookup — returns `Box<dyn Trait>`.
    fn get_shared_raw<T: ?Sized + 'static>(&self) -> Option<Box<T>> {
        let key = TypeId::of::<Box<T>>();
        let entry = self.shared_resolved.get(&key)?;
        let _ = self
            .accessed_capability_names
            .borrow_mut()
            .insert(entry.capability_name);
        let erased = (entry.coerce)(entry.value.as_ref().as_any_ref());
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
    use crate::capability::bearer_token_provider::BearerToken;
    use crate::capability::bearer_token_provider::BearerTokenProvider as BearerTokenProviderHandle;
    use crate::local::capability::BearerTokenProvider as LocalBearerTokenProvider;
    use crate::shared::capability::BearerTokenProvider as SharedBearerTokenProvider;
    use tokio::sync::watch;

    #[derive(Clone)]
    struct TestTokenProvider {
        token: String,
    }

    #[async_trait::async_trait]
    impl SharedBearerTokenProvider for TestTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    #[async_trait::async_trait(?Send)]
    impl LocalBearerTokenProvider for TestTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    #[derive(Clone)]
    struct DualTokenProvider {
        local_token: String,
        shared_token: String,
    }

    #[async_trait::async_trait(?Send)]
    impl LocalBearerTokenProvider for DualTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.local_token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    #[async_trait::async_trait]
    impl SharedBearerTokenProvider for DualTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.shared_token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    #[derive(Clone)]
    struct LocalOnlyTokenProvider {
        token: String,
    }

    #[async_trait::async_trait(?Send)]
    impl LocalBearerTokenProvider for LocalOnlyTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    #[async_trait::async_trait]
    impl SharedBearerTokenProvider for LocalOnlyTokenProvider {
        async fn get_token(&self) -> Result<BearerToken, Error> {
            Ok(BearerToken::new(self.token.clone(), 0))
        }

        fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
            let (tx, rx) = watch::channel(None);
            drop(tx);
            rx
        }
    }

    /// Helper: register a shared TestTokenProvider with the given name.
    fn register_provider(registry: &mut CapabilityRegistry, name: &str, token: &str) {
        let instance = TestTokenProvider {
            token: token.to_string(),
        };
        let caps = crate::extension_capabilities!(
            TestTokenProvider => BearerTokenProviderHandle
        );
        registry.register_all_shared(name, (caps.register_shared)(&instance));
    }

    fn register_dual_provider(
        registry: &mut CapabilityRegistry,
        name: &str,
        local_token: &str,
        shared_token: &str,
    ) {
        let instance = DualTokenProvider {
            local_token: local_token.to_string(),
            shared_token: shared_token.to_string(),
        };

        let caps = crate::extension_capabilities!(
            DualTokenProvider => BearerTokenProviderHandle
        );
        registry.register_all_shared(name, (caps.register_shared)(&instance));

        let rc: Rc<dyn Any> = Rc::new(DualTokenProvider {
            local_token: local_token.to_string(),
            shared_token: shared_token.to_string(),
        });
        registry.register_all_local(name, (caps.register_local)(rc));
    }

    fn register_local_only_provider(registry: &mut CapabilityRegistry, name: &str, token: &str) {
        let rc: Rc<dyn Any> = Rc::new(LocalOnlyTokenProvider {
            token: token.to_string(),
        });
        let caps = crate::extension_capabilities!(
            LocalOnlyTokenProvider => BearerTokenProviderHandle
        );
        registry.register_all_local(name, (caps.register_local)(rc));
    }

    #[test]
    fn test_shared_registration_and_get() {
        let mut registry = CapabilityRegistry::new();

        let instance = TestTokenProvider {
            token: "shared_token".to_string(),
        };
        let caps = crate::extension_capabilities!(TestTokenProvider => BearerTokenProviderHandle);
        registry.register_all_shared("ext", (caps.register_shared)(&instance));

        let provider: Box<dyn SharedBearerTokenProvider> = registry
            .get::<dyn SharedBearerTokenProvider>("ext")
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let token = rt.block_on(provider.get_token()).unwrap();
        assert_eq!(token.token.secret(), "shared_token");
    }

    #[test]
    fn test_local_registration_and_resolve() {
        let mut registry = CapabilityRegistry::new();

        let rc: Rc<dyn Any> = Rc::new(LocalOnlyTokenProvider {
            token: "local_token".to_string(),
        });
        let caps =
            crate::extension_capabilities!(LocalOnlyTokenProvider => BearerTokenProviderHandle);
        registry.register_all_local("ext", (caps.register_local)(Rc::clone(&rc)));

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "ext".to_string())]);
        let caps = registry.resolve_bindings(&bindings).unwrap();
        let handle = caps
            .require_local::<dyn LocalBearerTokenProvider>()
            .unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let token = rt.block_on(handle.get_token()).unwrap();
        assert_eq!(token.token.secret(), "local_token");
    }

    #[test]
    fn test_register_and_get() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "test_ext", "test_token");

        let result: Option<Box<dyn SharedBearerTokenProvider>> =
            registry.get::<dyn SharedBearerTokenProvider>("test_ext");
        assert!(result.is_some());
    }

    #[test]
    fn test_get_returns_independent_clones() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "ext", "shared_test");

        let a: Box<dyn SharedBearerTokenProvider> = registry
            .get::<dyn SharedBearerTokenProvider>("ext")
            .unwrap();
        let b: Box<dyn SharedBearerTokenProvider> = registry
            .get::<dyn SharedBearerTokenProvider>("ext")
            .unwrap();

        // Both are independent clones (different pointers)
        assert!(!std::ptr::eq(
            &*a as *const dyn SharedBearerTokenProvider,
            &*b as *const dyn SharedBearerTokenProvider,
        ));
    }

    #[test]
    fn test_registry_clone_produces_deep_copy() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "ext", "clone_test");

        let cloned = registry.clone();

        let from_original: Box<dyn SharedBearerTokenProvider> = registry
            .get::<dyn SharedBearerTokenProvider>("ext")
            .unwrap();
        let from_clone: Box<dyn SharedBearerTokenProvider> =
            cloned.get::<dyn SharedBearerTokenProvider>("ext").unwrap();

        // Deep copy — different pointers
        assert!(!std::ptr::eq(
            &*from_original as *const dyn SharedBearerTokenProvider,
            &*from_clone as *const dyn SharedBearerTokenProvider,
        ));
    }

    #[test]
    fn test_not_found() {
        let registry = CapabilityRegistry::new();
        let result = registry.get::<dyn SharedBearerTokenProvider>("missing");
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

        let provider: Box<dyn SharedBearerTokenProvider> = registry
            .get::<dyn SharedBearerTokenProvider>("auth")
            .unwrap();
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
            .get::<dyn SharedBearerTokenProvider>("azure_prod")
            .unwrap();
        let _p2 = registry
            .get::<dyn SharedBearerTokenProvider>("azure_staging")
            .unwrap();
    }

    #[test]
    fn test_resolve_bindings_unknown_extension() {
        let registry = CapabilityRegistry::new();
        let bindings = HashMap::from([(
            "bearer_token_provider".to_string(),
            "nonexistent".to_string(),
        )]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent"),
            "should name the missing extension: {msg}"
        );
        assert!(msg.contains("no extension with that name exists"), "{msg}");
    }

    #[test]
    fn test_resolve_bindings_unknown_capability_name() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([("totally_made_up".to_string(), "azure_auth".to_string())]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Unknown capability"),
            "should say unknown: {msg}"
        );
        assert!(msg.contains("totally_made_up"), "{msg}");
        assert!(
            msg.contains("bearer_token_provider"),
            "should list known caps: {msg}"
        );
    }

    #[test]
    fn test_resolve_bindings_valid() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([(
            "bearer_token_provider".to_string(),
            "azure_auth".to_string(),
        )]);
        let caps = registry.resolve_bindings(&bindings).unwrap();
        assert!(!caps.is_empty());
    }

    /// Helper: register a fake extension that only has entries under a custom
    /// capability name (simulates a second trait type for testing).
    fn register_fake_capability(
        registry: &mut CapabilityRegistry,
        ext_name: &str,
        cap_name: &'static str,
    ) {
        let instance = TestTokenProvider {
            token: "fake".to_string(),
        };
        // Build a registration but override the capability_name.
        let reg = shared::CapabilityRegistration {
            trait_id: TypeId::of::<Box<dyn std::fmt::Debug>>(),
            value: Box::new(instance),
            coerce: |any: &dyn Any| -> Box<dyn Any + Send> {
                let concrete = any.downcast_ref::<TestTokenProvider>().unwrap();
                Box::new(Box::new(concrete.clone()) as Box<dyn SharedBearerTokenProvider>)
            },
            capability_name: cap_name,
            local_trait_id: None,
            adapt_to_local: None,
        };
        registry.register_all_shared(ext_name, vec![reg]);
    }

    #[test]
    fn test_resolve_bindings_known_type_no_provider() {
        // Extension "other_ext" exists but only provides "other_cap", not bearer_token_provider.
        let mut registry = CapabilityRegistry::new();
        register_fake_capability(&mut registry, "other_ext", "other_cap");
        let bindings =
            HashMap::from([("bearer_token_provider".to_string(), "other_ext".to_string())]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no loaded extension provides it"),
            "should say no provider: {msg}"
        );
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
        let bindings =
            HashMap::from([("bearer_token_provider".to_string(), "other_ext".to_string())]);
        let err = registry.resolve_bindings(&bindings).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("does not provide capability"),
            "should say missing: {msg}"
        );
        assert!(msg.contains("other_ext"), "{msg}");
        assert!(
            msg.contains("other_cap"),
            "should list what it provides: {msg}"
        );
    }

    #[test]
    fn test_require_missing_capability() {
        let caps = Capabilities::new();
        let result = caps.require_local::<dyn LocalBearerTokenProvider>();
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("Missing required capability"), "{msg}");
    }

    #[test]
    fn test_unused_bindings_detected() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([(
            "bearer_token_provider".to_string(),
            "azure_auth".to_string(),
        )]);
        let caps = registry.resolve_bindings(&bindings).unwrap();

        // Before any access, all bindings are unused.
        let unused = caps.unused_bindings();
        assert_eq!(unused, vec!["bearer_token_provider"]);

        // After accessing, none are unused.
        let _ = caps
            .require_local::<dyn LocalBearerTokenProvider>()
            .unwrap();
        let unused = caps.unused_bindings();
        assert!(
            unused.is_empty(),
            "after require_local(), should be empty: {unused:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_local_consumer_prefers_local_variant() {
        let mut registry = CapabilityRegistry::new();
        register_dual_provider(&mut registry, "auth", "local_token", "shared_token");

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "auth".to_string())]);
        let caps = registry.resolve_bindings(&bindings).unwrap();

        let auth = caps
            .require_local::<dyn LocalBearerTokenProvider>()
            .unwrap();
        let token = auth.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "local_token");
    }

    #[tokio::test]
    async fn test_shared_consumer_uses_shared_variant() {
        let mut registry = CapabilityRegistry::new();
        register_dual_provider(&mut registry, "auth", "local_token", "shared_token");

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "auth".to_string())]);
        let caps = registry.resolve_bindings(&bindings).unwrap();

        let auth = caps
            .require_shared::<dyn SharedBearerTokenProvider>()
            .unwrap();
        let token = auth.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "shared_token");
    }

    #[test]
    fn test_shared_consumer_rejects_local_only_provider() {
        let mut registry = CapabilityRegistry::new();
        register_local_only_provider(&mut registry, "auth", "local_only_token");

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "auth".to_string())]);
        let caps = registry.resolve_bindings(&bindings).unwrap();

        // Shared consumer should not get local-only capability
        let result = caps.require_shared::<dyn SharedBearerTokenProvider>();
        assert!(result.is_err());
    }
}
