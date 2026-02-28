// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Cross-cutting extension glue: error types, sealing, macros, and re-exports.
//!
//! The actual implementations live under their respective modules:
//!
//! - [`crate::shared::extensions`] — `SharedExtensionRegistry`, `SharedExtensionTrait`, `BearerTokenProvider`
//! - [`crate::local::extensions`] — `LocalExtensionRegistry`, `LocalExtensionTrait`, `ExtensionRegistrar`, `BearerTokenProvider`
//!
//! This module provides the common definitions that both sides share:
//! error types, the `Sealed` trait for the extension trait hierarchy,
//! the `impl_extension_trait!` macro, and the registration macros.

// ── Re-exports ───────────────────────────────────────────────────────────────

pub use crate::local::extensions::ExtensionRegistrar;
pub use crate::shared::extensions::bearer_token_provider::{BearerToken, Secret};

// ── Error types ──────────────────────────────────────────────────────────────

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

/// Error type for extension operations.
///
/// Thread-safe error type compatible with any `thiserror`-derived error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

// ── Sealing ──────────────────────────────────────────────────────────────────

// Private module for sealing - external crates cannot implement Sealed
pub(crate) mod private {
    pub trait Sealed {}
}

// ── impl_extension_trait! ────────────────────────────────────────────────────

/// Generates both `shared` and `local` trait implementations from a single body.
///
/// When the `shared::Trait` and `local::Trait` implementations are identical
/// (as is common for extension types that are inherently `Send + Sync`), this
/// macro eliminates the duplication by expanding one body into two `impl` blocks:
///
/// - `#[async_trait]` `impl shared::Trait for Type { ... }`
/// - `#[async_trait(?Send)]` `impl local::Trait for Type { ... }`
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::extensions::BearerToken;
///
/// impl_extension_trait! {
///     impl BearerTokenProvider for MyExtension {
///         async fn get_token(&self) -> Result<BearerToken, otap_df_engine::extensions::Error> {
///             todo!()
///         }
///         fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
///             todo!()
///         }
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_extension_trait {
    (impl $trait_name:ident for $type:ty { $($body:tt)* }) => {
        #[async_trait::async_trait]
        impl $crate::shared::extensions::$trait_name for $type {
            $($body)*
        }

        #[async_trait::async_trait(?Send)]
        impl $crate::local::extensions::$trait_name for $type {
            $($body)*
        }
    };
}

// ── Registration macros ──────────────────────────────────────────────────────

/// Generates a registrar closure that registers **shared** `Arc<dyn Trait>` entries.
///
/// The instance is wrapped in `Arc` — accessible by both local and shared components.
/// Use this for cold-path traits (auth, service discovery) where thread-safety
/// overhead is negligible.
///
/// For registering both shared and local traits on the same instance, prefer
/// [`extension_traits!`] instead.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::shared::extensions;
/// let registrar = shared_extension_traits!(auth_service => extensions::BearerTokenProvider);
/// ```
#[macro_export]
macro_rules! shared_extension_traits {
    ($instance:expr => $($trait:path),* $(,)?) => {{
        let __arc = std::sync::Arc::new($instance);
        let __registrar: $crate::local::extensions::ExtensionRegistrar = Box::new({
            move |registry: &mut $crate::local::extensions::LocalExtensionRegistry, name: &str| {
                $(
                    {
                        // Compile-time check: ensure the trait is a valid SharedExtensionTrait.
                        const _: fn() = || {
                            fn assert_shared_trait<T: ?Sized + $crate::shared::extensions::SharedExtensionTrait>() {}
                            assert_shared_trait::<dyn $trait>();
                        };
                        // Coerce Arc<ConcreteType> → Arc<dyn Trait> (zero-cost)
                        registry.register::<dyn $trait>(name, __arc.clone() as std::sync::Arc<dyn $trait>);
                    }
                )*
            }
        });
        __registrar
    }};
}

/// Generates a registrar closure that registers **local** `Rc<dyn Trait>` entries.
///
/// The instance is captured by value (`Send`) and wrapped in `Rc` on the worker
/// thread — never crosses a thread boundary. Accessible only by local components.
/// Use this for hot-path traits (rate limiters, quotas) where atomic/mutex
/// overhead matters.
///
/// For registering both shared and local traits on the same instance, prefer
/// [`extension_traits!`] instead.
///
/// # Example
///
/// ```ignore
/// let registrar = local_extension_traits!(rate_limiter => local_ext::RateLimiter);
/// ```
#[macro_export]
macro_rules! local_extension_traits {
    ($instance:expr => $($trait:path),* $(,)?) => {{
        let __registrar: $crate::local::extensions::ExtensionRegistrar = Box::new({
            let __instance = $instance;
            move |registry: &mut $crate::local::extensions::LocalExtensionRegistry, name: &str| {
                // Wrap in Rc on the worker thread (never crosses thread boundary)
                let __rc = std::rc::Rc::new(__instance);
                $(
                    {
                        // Compile-time check: ensure the trait is a valid LocalExtensionTrait.
                        const _: fn() = || {
                            fn assert_local_trait<T: ?Sized + $crate::local::extensions::LocalExtensionTrait>() {}
                            assert_local_trait::<dyn $trait>();
                        };
                        // Coerce Rc<ConcreteType> → Rc<dyn Trait> (zero-cost)
                        registry.register_local::<dyn $trait>(name, __rc.clone() as std::rc::Rc<dyn $trait>);
                    }
                )*
            }
        });
        __registrar
    }};
}

/// Generates a registrar closure that registers both **shared** and **local** traits
/// on the same extension instance.
///
/// This is the preferred macro when an extension exposes traits for both shared
/// components (`Arc<dyn Trait>`) and local components (`Rc<dyn Trait>`). The
/// instance is cloned once — wrapped in `Arc` for shared traits and captured
/// by value for local traits (wrapped in `Rc` on the worker thread).
///
/// # Syntax
///
/// ```ignore
/// extension_traits!(instance =>
///     shared(SharedTrait1, SharedTrait2),
///     local(LocalTrait1, LocalTrait2),
/// )
/// ```
///
/// Either `shared(...)` or `local(...)` may be omitted if only one kind is needed,
/// but prefer [`shared_extension_traits!`] or [`local_extension_traits!`] in that case.
///
/// # Example
///
/// ```ignore
/// use otap_df_engine::{shared::extensions as shared_ext, local::extensions as local_ext};
/// let registrar = extension_traits!(extension =>
///     shared(shared_ext::BearerTokenProvider),
///     local(local_ext::BearerTokenProvider),
/// );
/// ```
#[macro_export]
macro_rules! extension_traits {
    // shared + local
    ($instance:expr => shared($($shared_trait:path),* $(,)?), local($($local_trait:path),* $(,)?) $(,)?) => {{
        let __instance = $instance;
        let __arc = std::sync::Arc::new(__instance.clone());
        let __registrar: $crate::local::extensions::ExtensionRegistrar = Box::new({
            move |registry: &mut $crate::local::extensions::LocalExtensionRegistry, name: &str| {
                // Register shared (Arc) traits
                $(
                    {
                        const _: fn() = || {
                            fn assert_shared<T: ?Sized + $crate::shared::extensions::SharedExtensionTrait>() {}
                            assert_shared::<dyn $shared_trait>();
                        };
                        registry.register::<dyn $shared_trait>(name, __arc.clone() as std::sync::Arc<dyn $shared_trait>);
                    }
                )*
                // Register local (Rc) traits
                let __rc = std::rc::Rc::new(__instance);
                $(
                    {
                        const _: fn() = || {
                            fn assert_local<T: ?Sized + $crate::local::extensions::LocalExtensionTrait>() {}
                            assert_local::<dyn $local_trait>();
                        };
                        registry.register_local::<dyn $local_trait>(name, __rc.clone() as std::rc::Rc<dyn $local_trait>);
                    }
                )*
            }
        });
        __registrar
    }};
    // shared only (forwarded)
    ($instance:expr => shared($($shared_trait:path),* $(,)?) $(,)?) => {
        $crate::shared_extension_traits!($instance => $($shared_trait),*)
    };
    // local only (forwarded)
    ($instance:expr => local($($local_trait:path),* $(,)?) $(,)?) => {
        $crate::local_extension_traits!($instance => $($local_trait),*)
    };
}
