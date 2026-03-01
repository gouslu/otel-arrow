// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension system — true single instance, `Arc<Mutex>`-backed shared registry.
//!
//! This module provides:
//! - [`ExtensionRegistryBuilder`] — accumulates extension registrations during pipeline build.
//! - [`ExtensionRegistry`] — shared, `Clone`-able registry with [`handle`](ExtensionRegistry::handle) + [`lock`](ExtensionHandle::lock) API.
//! - Extension traits like [`BearerTokenProvider`] — `Send + 'static` (for builder phase).
//!
//! # Design
//!
//! All components share the **same** `ExtensionRegistry` via `Arc`. The builder
//! accumulates extension trait objects during pipeline build, then [`build`](ExtensionRegistryBuilder::build)
//! wraps each in a `parking_lot::Mutex`. Each component receives a cheap `Arc` clone.
//!
//! Access is via [`handle`](ExtensionRegistry::handle), which returns an
//! [`ExtensionHandle`]. Call [`lock`](ExtensionHandle::lock) on the handle to
//! acquire a per-entry mutex lock and access `&T` via a closure. The lock is
//! fast (~2-3ns uncontended) and sound regardless of runtime configuration.
//!
//! # Adding New Extension Traits
//!
//! Define a trait with `Send + 'static` supertraits (needed for the builder's
//! thread-crossing). Async methods should return `BoxFuture<'static, ...>`:
//!
//! ```ignore
//! use futures::future::BoxFuture;
//!
//! pub trait MyCapability: Send + 'static {
//!     fn do_something(&self) -> BoxFuture<'static, Result<(), Error>>;
//!     fn subscribe(&self) -> watch::Receiver<State>;
//! }
//! ```
//!
//! # Registering Extension Traits
//!
//! Use the [`extension_traits!`] macro to create a registrar closure:
//!
//! ```ignore
//! let registrar = extension_traits!(my_instance => MyCapability, OtherCapability);
//! ```
//!
//! # Consuming Extension Traits
//!
//! Components access extensions via [`handle`](ExtensionRegistry::handle) +
//! [`lock`](ExtensionHandle::lock).
//! Extract owned values from the closure — don't await inside it:
//!
//! ```ignore
//! let ext = extension_registry.handle::<dyn MyCapability>("extension_name")?;
//! let rx = ext.lock(|e| e.subscribe());
//! ```

pub mod registry;

/// Extension traits that components can implement to expose capabilities.
pub mod bearer_token_provider;

// Re-export commonly used types.
pub use registry::{
    ExtensionError, ExtensionHandle, ExtensionRegistrar, ExtensionRegistry,
    ExtensionRegistryBuilder,
};

/// Error type for extension operations.
///
/// Thread-safe error type compatible with any `thiserror`-derived error.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub use bearer_token_provider::{BearerToken, BearerTokenProvider, Secret};
