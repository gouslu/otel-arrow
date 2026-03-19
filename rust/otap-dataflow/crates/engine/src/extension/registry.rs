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
//! // In a node factory, consumers ask for a handle:
//! let auth = capabilities.require::<BearerTokenProvider>()?;
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

    /// Sealing trait for capability handle types.
    ///
    /// This prevents external crates from implementing
    /// [`CapabilityHandle`](super::CapabilityHandle) and bypassing
    /// engine-controlled handle dispatch semantics.
    pub trait HandleSealed {
        /// Must be set to [`MACRO_SEAL`]. Only registration macros can do this.
        #[doc(hidden)]
        const HANDLE_MACRO_TOKEN: MacroToken;
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

    /// A registration for one shared capability trait implementation.
    pub struct CapabilityRegistration {
        pub(crate) trait_id: TypeId,
        pub(crate) value: Box<dyn CloneAnySend>,
        pub(crate) coerce: fn(&dyn Any) -> Box<dyn Any + Send>,
        pub(crate) capability_name: &'static str,
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
            //    which variant to use via ConsumerType at require() time.
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
    ($trait:path, $name:literal, $description:literal, $static_name:ident $(,)?) => {
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
        static $static_name: &str = $name;
    };
    ($trait:path, $name:literal, $description:literal $(,)?) => {
        $crate::register_capability!($trait, $name, $description, _KNOWN_CAP);
    };
}

/// Registers a capability handle type as a known extension capability.
///
/// This is the preferred registration path for public capability exposure,
/// because capability lookup is handle-based (`Capabilities::require::<Handle>()`).
#[macro_export]
macro_rules! register_capability_handle {
    ($handle:ty, $name:literal, $description:literal, $static_name:ident $(,)?) => {
        impl $crate::extension::registry::private::Sealed for $handle {
            const MACRO_TOKEN: $crate::extension::registry::private::MacroToken =
                $crate::extension::registry::private::MACRO_SEAL;
        }
        impl $crate::extension::registry::private::HandleSealed for $handle {
            const HANDLE_MACRO_TOKEN: $crate::extension::registry::private::MacroToken =
                $crate::extension::registry::private::MACRO_SEAL;
        }
        impl $crate::extension::registry::ExtensionCapability for $handle {
            const NAME: &'static str = $name;
            const DESCRIPTION: &'static str = $description;
        }

        #[allow(unsafe_code)]
        #[$crate::distributed_slice($crate::extension::registry::KNOWN_CAPABILITIES)]
        static $static_name: &str = $name;
    };
    ($handle:ty, $name:literal, $description:literal $(,)?) => {
        $crate::register_capability_handle!($handle, $name, $description, _KNOWN_CAP_HANDLE);
    };
}

/// Registers the local/shared trait mappings for a capability handle.
///
/// Generates two associated functions:
/// - `shared_capabilities(&T)` — shared registrations (clone-based)
/// - `local_capabilities(&Rc<T>)` — local registrations (Rc-based)
#[macro_export]
macro_rules! register_capability_handle_traits {
    ($handle:ty, $local_trait:path, $shared_trait:path $(,)?) => {
        impl $handle {
            /// Build shared capability registrations for this handle.
            pub fn shared_capabilities<T>(instance: &T) -> Vec<$crate::extension::registry::shared::CapabilityRegistration>
            where
                T: Clone + Send + 'static + $shared_trait,
            {
                fn make_registration<TInner: Clone + Send + 'static + $shared_trait>(
                    val: &TInner,
                ) -> $crate::extension::registry::shared::CapabilityRegistration {
                    fn coerce<TInner: Clone + Send + 'static + $shared_trait>(
                        any: &dyn std::any::Any,
                    ) -> Box<dyn std::any::Any + Send> {
                        let concrete = any
                            .downcast_ref::<TInner>()
                            .expect("registry entry type mismatch — this is a bug");
                        let boxed: Box<dyn $shared_trait> = Box::new(concrete.clone());
                        Box::new(boxed) as Box<dyn std::any::Any + Send>
                    }

                    $crate::extension::registry::shared::CapabilityRegistration {
                        trait_id: std::any::TypeId::of::<Box<dyn $shared_trait>>(),
                        value: Box::new(val.clone()),
                        coerce: coerce::<TInner>,
                        capability_name: <$handle as $crate::extension::registry::ExtensionCapability>::NAME,
                    }
                }

                vec![make_registration(instance)]
            }

            /// Build local capability registrations (Rc-based) for this handle.
            pub fn local_capabilities<T>(instance: &std::rc::Rc<T>) -> Vec<$crate::extension::registry::local::CapabilityRegistration>
            where
                T: 'static + $local_trait,
            {
                fn make_registration<TInner: 'static + $local_trait>(
                    rc: &std::rc::Rc<TInner>,
                ) -> $crate::extension::registry::local::CapabilityRegistration {
                    fn coerce<TInner: 'static + $local_trait>(
                        rc_any: std::rc::Rc<dyn std::any::Any>,
                    ) -> Box<dyn std::any::Any> {
                        let concrete: std::rc::Rc<TInner> = rc_any
                            .downcast::<TInner>()
                            .expect("registry entry type mismatch — this is a bug");
                        let trait_obj: std::rc::Rc<dyn $local_trait> = concrete;
                        Box::new(trait_obj) as Box<dyn std::any::Any>
                    }

                    $crate::extension::registry::local::CapabilityRegistration {
                        trait_id: std::any::TypeId::of::<std::rc::Rc<dyn $local_trait>>(),
                        value: std::rc::Rc::clone(rc) as std::rc::Rc<dyn std::any::Any>,
                        coerce: coerce::<TInner>,
                        capability_name: <$handle as $crate::extension::registry::ExtensionCapability>::NAME,
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
/// Takes the concrete extension type and one or more capability handles.
/// The type must implement both the local and shared trait variants for each handle.
///
/// ```ignore
/// capabilities: extension_capabilities!(MyExtension => BearerTokenProvider, HealthCheck),
/// ```
#[macro_export]
macro_rules! extension_capabilities {
    ($type:ty => $($handle:path),+ $(,)?) => {
        $crate::extension::registry::ExtensionCapabilities {
            names: &[$(<$handle as $crate::extension::registry::ExtensionCapability>::NAME),+],
            register_shared: |any: &dyn std::any::Any| -> Vec<$crate::extension::registry::shared::CapabilityRegistration> {
                let ext = any.downcast_ref::<$type>()
                    .expect("extension type mismatch — this is a bug");
                let mut caps = Vec::new();
                $(caps.extend(<$handle>::shared_capabilities(ext));)+
                caps
            },
            register_local: |rc_any: std::rc::Rc<dyn std::any::Any>| -> Vec<$crate::extension::registry::local::CapabilityRegistration> {
                let rc = rc_any.downcast::<$type>()
                    .expect("extension type mismatch — this is a bug");
                let mut caps = Vec::new();
                $(caps.extend(<$handle>::local_capabilities(&rc));)+
                caps
            },
        }
    };
}

// ── Capabilities ─────────────────────────────────────────────────────────

/// Per-node capability instances resolved from config bindings.
///
/// Built by the engine from the node's `capabilities` config section and
/// the global [`CapabilityRegistry`]. Nodes receive this at factory time
/// and look up capabilities by type only — no extension names needed.
///
// ── CapabilityHandle ─────────────────────────────────────────────────────────

/// Trait for handle types that dispatch between local and shared capability variants.
///
/// Implement this on handle enums (e.g., `BearerTokenProvider`) so the
/// registry can construct the right variant based on consumer context.
///
/// Local variants use `Rc<dyn Trait>` for true single-instance sharing.
/// Shared variants use `Box<dyn Trait>` with clone-based distribution.
pub trait CapabilityHandle: private::HandleSealed + Sized {
    /// Stable capability name used in node config bindings.
    const CAPABILITY_NAME: &'static str;

    /// The !Send trait type for local consumers.
    type Local: ?Sized + 'static;
    /// The Send trait type for shared consumers.
    type Shared: ?Sized + 'static;

    /// Construct the handle wrapping a local variant (`Rc`-based).
    fn from_local(local: Rc<Self::Local>) -> Self;
    /// Construct the handle wrapping a shared variant (`Box`-based).
    fn from_shared(shared: Box<Self::Shared>) -> Self;
}

/// Whether the consumer is a local or shared node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerType {
    /// Local node on a single-threaded LocalSet.
    Local,
    /// Shared node that may run on multi-threaded executors.
    Shared,
}

// ── Capabilities (per-node resolved bindings) ────────────────────────────────

/// Per-node resolved capability bindings.
///
/// Produced by [`CapabilityRegistry::resolve_bindings`] during pipeline build.
/// Nodes use `require()`, `optional()`, or `get_handle()` in their factory to
/// retrieve capabilities.
///
/// # Example
///
/// ```ignore
/// // Required capability — fails with a clear error if not bound
/// let auth = capabilities.require::<BearerTokenProvider>(
///     ConsumerType::Local,
///     "bearer_token_provider",
/// )?;
///
/// // Optional capability — returns None if not bound
/// let enrichment = capabilities.optional::<DatasetLookupHandle>();
/// ```
pub struct Capabilities {
    local_resolved: HashMap<TypeId, local::RegistryEntry>,
    shared_resolved: HashMap<TypeId, shared::RegistryEntry>,
    /// Tracks which capability names were accessed via `require()` or `optional()`.
    /// Uses `RefCell` so that `require`/`optional` can take `&self`.
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
    /// but never consumed by the factory via `require()` or `optional()`.
    ///
    /// Called by the engine after the factory `create()` returns to detect
    /// misconfigured or unnecessary capability bindings.
    #[must_use]
    pub fn unused_bindings(&self) -> Vec<&'static str> {
        let accessed = self.accessed_capability_names.borrow();
        self.local_resolved
            .values()
            .map(|entry| entry.capability_name)
            .chain(self.shared_resolved.values().map(|entry| entry.capability_name))
            .filter(|name| !accessed.contains(name))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Returns `true` if any local variant was consumed by `require()` or `optional()`.
    #[must_use]
    pub fn consumed_local(&self) -> bool {
        *self.accessed_local.borrow()
    }

    /// Returns `true` if any shared variant was consumed by `require()` or `optional()`.
    #[must_use]
    pub fn consumed_shared(&self) -> bool {
        *self.accessed_shared.borrow()
    }

    /// Require a capability handle.
    ///
    /// The `consumer_type` determines which variant is returned:
    /// - `ConsumerType::Local` — tries local first, falls back to shared
    /// - `ConsumerType::Shared` — shared only
    ///
    /// # Example
    ///
    /// ```ignore
    /// let auth = capabilities.require::<BearerTokenProvider>(ConsumerType::Local)?;
    /// auth.get_token().await?;
    /// ```
    pub fn require<H: CapabilityHandle>(
        &self,
        consumer_type: ConsumerType,
    ) -> Result<H, otap_df_config::error::Error> {
        self.get::<H>(consumer_type).ok_or_else(|| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: format!(
                    "Missing required capability '{}'. Add to your node config:\n  capabilities:\n    {}: <extension_instance_name>",
                    H::CAPABILITY_NAME,
                    H::CAPABILITY_NAME,
                ),
            }
        })
    }

    /// Get an optional capability handle.
    ///
    /// Returns `None` if the capability was not configured for this node.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(auth) = capabilities.optional::<BearerTokenProvider>(ConsumerType::Local) {
    ///     auth.get_token().await?;
    /// }
    /// ```
    pub fn optional<H: CapabilityHandle>(&self, consumer_type: ConsumerType) -> Option<H> {
        self.get::<H>(consumer_type)
    }

    /// Get a capability handle, selecting the local or shared variant
    /// based on the consumer type.
    ///
    /// For local consumers, tries the local variant first. If unavailable,
    /// falls back to the shared variant (shared impls work on local nodes too).
    /// For shared consumers, always uses the shared variant.
    fn get<H: CapabilityHandle>(&self, consumer_type: ConsumerType) -> Option<H> {
        let resolved = match consumer_type {
            ConsumerType::Local => {
                // Prefer local variant; fall back to shared
                if let Some(local) = self.get_local_raw::<H::Local>() {
                    *self.accessed_local.borrow_mut() = true;
                    Some(H::from_local(local))
                } else {
                    self.get_shared_raw::<H::Shared>().map(|shared| {
                        *self.accessed_shared.borrow_mut() = true;
                        H::from_shared(shared)
                    })
                }
            }
            ConsumerType::Shared => self.get_shared_raw::<H::Shared>().map(|shared| {
                *self.accessed_shared.borrow_mut() = true;
                H::from_shared(shared)
            }),
        };

        if resolved.is_some() {
            let _ = self
                .accessed_capability_names
                .borrow_mut()
                .insert(H::CAPABILITY_NAME);
        }

        resolved
    }

    /// Internal local typed lookup — returns `Rc<dyn Trait>` for true single-instance sharing.
    fn get_local_raw<T: ?Sized + 'static>(&self) -> Option<Rc<T>> {
        let key = TypeId::of::<Rc<T>>();
        let entry = self.local_resolved.get(&key)?;
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
    use crate::extension::bearer_token_provider::BearerToken;
    use crate::extension::bearer_token_provider::BearerTokenProvider as BearerTokenProviderHandle;
    use crate::extension::bearer_token_provider::local::BearerTokenProvider as LocalBearerTokenProvider;
    use crate::extension::bearer_token_provider::shared::BearerTokenProvider as SharedBearerTokenProvider;
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

        let provider: Box<dyn SharedBearerTokenProvider> =
            registry.get::<dyn SharedBearerTokenProvider>("ext").unwrap();

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
        let caps = crate::extension_capabilities!(LocalOnlyTokenProvider => BearerTokenProviderHandle);
        registry.register_all_local("ext", (caps.register_local)(Rc::clone(&rc)));

        let bindings = HashMap::from([(
            "bearer_token_provider".to_string(),
            "ext".to_string(),
        )]);
        let caps = registry
            .resolve_bindings(&bindings)
            .unwrap();
        let handle = caps.require::<BearerTokenProviderHandle>(ConsumerType::Local).unwrap();

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

        let a: Box<dyn SharedBearerTokenProvider> =
            registry.get::<dyn SharedBearerTokenProvider>("ext").unwrap();
        let b: Box<dyn SharedBearerTokenProvider> =
            registry.get::<dyn SharedBearerTokenProvider>("ext").unwrap();

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

        let from_original: Box<dyn SharedBearerTokenProvider> =
            registry.get::<dyn SharedBearerTokenProvider>("ext").unwrap();
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

        let provider: Box<dyn SharedBearerTokenProvider> =
            registry.get::<dyn SharedBearerTokenProvider>("auth").unwrap();
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
        let err = registry
            .resolve_bindings(&bindings)
            .unwrap_err();
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
        let err = registry
            .resolve_bindings(&bindings)
            .unwrap_err();
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
        let caps = registry
            .resolve_bindings(&bindings)
            .unwrap();
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
        let err = registry
            .resolve_bindings(&bindings)
            .unwrap_err();
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
        let err = registry
            .resolve_bindings(&bindings)
            .unwrap_err();
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
        let result = caps.require::<BearerTokenProviderHandle>(ConsumerType::Local);
        assert!(result.is_err());
        let msg = result.err().unwrap().to_string();
        assert!(msg.contains("Missing required capability"), "{msg}");
        assert!(msg.contains("bearer_token_provider"), "{msg}");
    }

    #[test]
    fn test_unused_bindings_detected() {
        let mut registry = CapabilityRegistry::new();
        register_provider(&mut registry, "azure_auth", "token");
        let bindings = HashMap::from([(
            "bearer_token_provider".to_string(),
            "azure_auth".to_string(),
        )]);
        let caps = registry
            .resolve_bindings(&bindings)
            .unwrap();

        // Before any access, all bindings are unused.
        let unused = caps.unused_bindings();
        assert_eq!(unused, vec!["bearer_token_provider"]);

        // After accessing, none are unused.
        let _ = caps.require::<BearerTokenProviderHandle>(ConsumerType::Local).unwrap();
        let unused = caps.unused_bindings();
        assert!(
            unused.is_empty(),
            "after require(), should be empty: {unused:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_local_consumer_prefers_local_variant() {
        let mut registry = CapabilityRegistry::new();
        register_dual_provider(&mut registry, "auth", "local_token", "shared_token");

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "auth".to_string())]);
        let caps = registry
            .resolve_bindings(&bindings)
            .unwrap();

        let auth = caps.require::<BearerTokenProviderHandle>(ConsumerType::Local).unwrap();
        let token = auth.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "local_token");
    }

    #[tokio::test]
    async fn test_shared_consumer_uses_shared_variant() {
        let mut registry = CapabilityRegistry::new();
        register_dual_provider(&mut registry, "auth", "local_token", "shared_token");

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "auth".to_string())]);
        let caps = registry
            .resolve_bindings(&bindings)
            .unwrap();

        let auth = caps.require::<BearerTokenProviderHandle>(ConsumerType::Shared).unwrap();
        let token = auth.get_token().await.unwrap();
        assert_eq!(token.token.secret(), "shared_token");
    }

    #[test]
    fn test_shared_consumer_rejects_local_only_provider() {
        let mut registry = CapabilityRegistry::new();
        register_local_only_provider(&mut registry, "auth", "local_only_token");

        let bindings = HashMap::from([("bearer_token_provider".to_string(), "auth".to_string())]);
        let caps = registry
            .resolve_bindings(&bindings)
            .unwrap();

        // Shared consumer should not get local-only capability
        let result = caps.require::<BearerTokenProviderHandle>(ConsumerType::Shared);
        assert!(result.is_err());
    }
}
