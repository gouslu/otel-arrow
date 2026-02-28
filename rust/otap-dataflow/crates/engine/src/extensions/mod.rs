// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension traits and registry for capability-based lookups.
//!
//! This module provides:
//! - [`ExtensionRegistry`](registry::ExtensionRegistry) - Primary registry (Rc+Arc, `!Send`)
//! - [`shared::ExtensionRegistry`](registry::SharedExtensionRegistry) - Send+Sync subset (Arc-only)
//! - Common extension traits like [`shared::BearerTokenProvider`] and [`local::BearerTokenProvider`]
//!
//! # Adding New Extension Traits
//!
//! Use [`define_extension_trait!`] to define a matched pair of shared + local
//! extension traits from a single method definition:
//!
//! ```ignore
//! define_extension_trait! {
//!     /// Provides bearer tokens for authentication.
//!     pub BearerTokenProvider {
//!         async fn get_token(&self) -> Result<BearerToken, crate::extensions::Error>;
//!         fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>>;
//!     }
//! }
//! ```
//!
//! This generates `shared::BearerTokenProvider` (`Send + Sync`, stored as `Arc`)
//! and `local::BearerTokenProvider` (no bounds, stored as `Rc`). Both are sealed
//! and registered with the appropriate marker traits. External crates can implement
//! existing extension traits on their types, but cannot define new extension trait
//! types.

pub mod registry;

// Re-export commonly used types
pub use registry::{ExtensionError, ExtensionRegistrar, ExtensionRegistry};

/// Extension traits that components can implement to expose capabilities.
pub mod bearer_token_provider;

// Private module for sealing - external crates cannot implement Sealed
mod private {
    pub trait Sealed {}
}

/// Marker trait for **shared** extension trait types stored in the extension registry.
///
/// Shared traits are stored as `Arc<dyn Trait>` and accessible by both local and
/// shared components. Use for cold-path traits where thread-safety overhead is
/// negligible (e.g., auth token providers, service discovery).
///
/// This trait is **sealed** — only `dyn` extension traits defined in this module
/// can implement it. External crates can implement existing traits on their types
/// but cannot add new extension trait types.
///
/// Requires `Send + Sync` for Arc compatibility.
pub trait SharedExtensionTrait: private::Sealed + Send + Sync {}

/// Marker trait for **local** extension trait types stored in the extension registry.
///
/// Local traits are stored as `Rc<dyn Trait>` and accessible only by local
/// components. Use for hot-path traits where avoiding atomic/mutex overhead
/// matters (e.g., rate limiters, quotas).
///
/// This trait is **sealed** — only `dyn` extension traits defined in this module
/// can implement it. No `Send` or `Sync` required.
///
/// Follows the same pattern as channel metrics: `Rc<RefCell<...>>` for local
/// hot-path vs `Arc<Mutex<...>>` for shared.
pub trait LocalExtensionTrait: private::Sealed {}

/// Defines a matched pair of shared + local extension traits from a single
/// method definition.
///
/// The shared trait gets `#[async_trait]` with `Send + Sync` bounds (stored as
/// `Arc<dyn Trait>`). The local trait gets `#[async_trait(?Send)]` with no bounds
/// (stored as `Rc<dyn Trait>`). Both are sealed and registered with the
/// appropriate marker traits.
///
/// The macro generates `shared` and `local` submodules, each containing a trait
/// with the same name. Use `shared::TraitName` and `local::TraitName` to refer
/// to them.
///
/// # Syntax
///
/// ```ignore
/// define_extension_trait! {
///     /// Doc comments (applied to both traits).
///     pub TraitName {
///         async fn method(&self) -> ReturnType;
///         fn sync_method(&self) -> OtherType;
///     }
/// }
/// ```
///
/// # Generated Output
///
/// - `pub mod shared { pub trait TraitName: Send + Sync { ... } }` with `#[async_trait]`
/// - `pub mod local { pub trait TraitName { ... } }` with `#[async_trait(?Send)]`
/// - Sealed + `SharedExtensionTrait` for `dyn shared::TraitName`
/// - Sealed + `LocalExtensionTrait` for `dyn local::TraitName`
///
/// **Note:** Method signatures must use absolute paths (e.g.,
/// `crate::extensions::Error`) since methods are generated inside submodules.
///
/// # Example
///
/// ```ignore
/// define_extension_trait! {
///     /// Provides bearer tokens for authentication.
///     pub BearerTokenProvider {
///         async fn get_token(&self) -> Result<BearerToken, crate::extensions::Error>;
///         fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>>;
///     }
/// }
/// ```
macro_rules! define_extension_trait {
    (
        $(#[$meta:meta])*
        $vis:vis $name:ident {
            $($methods:tt)*
        }
    ) => {
        // ── Shared trait (Arc, Send + Sync) ──────────────────────
        /// Shared extension trait (requires `Send + Sync`, stored as `Arc`).
        $vis mod shared {
            use super::*;

            $(#[$meta])*
            #[async_trait::async_trait]
            pub trait $name: Send + Sync {
                $($methods)*
            }

            impl $crate::extensions::private::Sealed for dyn $name {}
            impl $crate::extensions::SharedExtensionTrait for dyn $name {}
        }

        // ── Local trait (Rc, no bounds) ──────────────────────────
        /// Local extension trait (no bounds, stored as `Rc`).
        $vis mod local {
            use super::*;

            $(#[$meta])*
            #[async_trait::async_trait(?Send)]
            pub trait $name {
                $($methods)*
            }

            impl $crate::extensions::private::Sealed for dyn $name {}
            impl $crate::extensions::LocalExtensionTrait for dyn $name {}
        }
    };
}

// Make the macro available within the crate
pub(crate) use define_extension_trait;

/// Error type for extension operations.
///
/// Thread-safe error type compatible with any `thiserror`-derived error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub use bearer_token_provider::{BearerToken, Secret};

/// Shared extension traits (`Send + Sync`, stored as `Arc<dyn Trait>`).
///
/// Use these for cold-path traits accessible by both local and shared components.
/// Also re-exports [`ExtensionRegistry`](registry::SharedExtensionRegistry) — the
/// `Send + Sync` registry passed to shared components.
pub mod shared {
    pub use super::bearer_token_provider::shared::BearerTokenProvider;
    pub use super::registry::SharedExtensionRegistry as ExtensionRegistry;
}

/// Local extension traits (no bounds, stored as `Rc<dyn Trait>`).
///
/// Use these for hot-path traits where avoiding atomic/mutex overhead matters.
/// Also re-exports [`ExtensionRegistry`](registry::ExtensionRegistry) — the
/// primary `!Send` registry passed to local components.
pub mod local {
    pub use super::bearer_token_provider::local::BearerTokenProvider;
    pub use super::registry::ExtensionRegistry;
}
