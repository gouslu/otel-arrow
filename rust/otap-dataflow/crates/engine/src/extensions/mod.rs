// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension traits and registry for capability-based lookups.
//!
//! This module provides:
//! - [`ExtensionRegistry`](registry::ExtensionRegistry) - An Rc-based registry for extension traits
//! - Common extension traits like [`BearerTokenProvider`](bearer_token_provider::BearerTokenProvider)
//!
//! # Adding New Extension Traits
//!
//! New extension traits must be defined in this module. External crates can implement
//! existing extension traits on their types, but cannot define new extension trait types.
//!
//! This restriction is enforced at compile time via the sealed trait pattern.

pub mod registry;

// Re-export commonly used types
pub use registry::{ExtensionError, ExtensionRegistrar, ExtensionRegistry};

/// Extension traits that components can implement to expose capabilities.
pub mod bearer_token_provider;

// Private module for sealing - external crates cannot implement Sealed
mod private {
    pub trait Sealed {}
}

/// Marker trait for extension trait types that can be stored in the extension registry.
///
/// This trait is **sealed** - it can only be implemented for `dyn` extension traits
/// defined in this module. External crates cannot add new extension trait types,
/// but they CAN implement existing traits like [`BearerTokenProvider`] on their types.
///
/// # How It Works
///
/// - `ExtensionTrait` is implemented for `dyn BearerTokenProvider` (and other extension traits)
/// - External crates can `impl BearerTokenProvider for MyType` freely
/// - External crates CANNOT create new traits usable with the extension registry
///
/// This ensures the extension system only supports well-defined, documented capabilities.
///
/// # Thread Safety
///
/// Extension traits are `!Send` and `!Sync`. The Rc-based registry is designed for
/// single-threaded `LocalSet` usage — it never crosses thread boundaries. If a
/// consumer needs `Send + Sync` (e.g., for tonic), they wrap the `Rc` in
/// `Arc<Mutex<...>>` themselves.
pub trait ExtensionTrait: private::Sealed {}

// Implement ExtensionTrait for each extension trait's dyn type.
// This is the ONLY place where ExtensionTrait can be implemented.
impl private::Sealed for dyn BearerTokenProvider {}
impl ExtensionTrait for dyn BearerTokenProvider {}

impl private::Sealed for dyn BearerTokenProviderSync {}
impl ExtensionTrait for dyn BearerTokenProviderSync {}

/// Error type for extension operations.
///
/// Thread-safe error type compatible with any `thiserror`-derived error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub use bearer_token_provider::{
    BearerToken, BearerTokenProvider, BearerTokenProviderSync, Secret,
};
