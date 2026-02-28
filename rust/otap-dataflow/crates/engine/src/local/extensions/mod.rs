// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local extension traits and registry (no bounds, stored as `Rc<dyn Trait>`).
//!
//! Use these for hot-path traits where avoiding atomic/mutex overhead matters.
//!
//! - [`ExtensionRegistry`] — the primary `!Send` registry passed to local components.
//! - [`BearerTokenProvider`] — bearer token authentication trait.

pub mod bearer_token_provider;
pub mod registry;

// ── Re-exports ───────────────────────────────────────────────────────────────

pub use bearer_token_provider::BearerTokenProvider;
pub use registry::{ExtensionRegistrar, LocalExtensionRegistry};

/// Public alias so that `local::extensions::ExtensionRegistry` mirrors
/// `shared::extensions::ExtensionRegistry`.
pub type ExtensionRegistry = LocalExtensionRegistry;

// ── LocalExtensionTrait (marker) ─────────────────────────────────────────────

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
pub trait LocalExtensionTrait: crate::extensions::private::Sealed {}
