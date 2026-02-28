// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared extension traits and registry (`Send + Sync`, stored as `Arc<dyn Trait>`).
//!
//! Use these for cold-path traits accessible by both local and shared components.
//!
//! - [`ExtensionRegistry`] — the `Send + Sync` registry passed to shared components.
//! - [`BearerTokenProvider`] — bearer token authentication trait.

pub mod bearer_token_provider;
pub mod registry;

// ── Re-exports ───────────────────────────────────────────────────────────────

pub use bearer_token_provider::BearerTokenProvider;
pub use bearer_token_provider::{BearerToken, Secret};
pub use registry::SharedExtensionRegistry;

/// Public alias so that `shared::extensions::ExtensionRegistry` mirrors
/// `local::extensions::ExtensionRegistry`.
pub type ExtensionRegistry = SharedExtensionRegistry;

// ── SharedExtensionTrait (marker) ────────────────────────────────────────────

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
pub trait SharedExtensionTrait: crate::extensions::private::Sealed + Send + Sync {}
