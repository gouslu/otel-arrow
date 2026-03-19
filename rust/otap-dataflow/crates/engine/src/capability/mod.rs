// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Capability handle definitions and registry.
//!
//! Each capability has a handle enum that dispatches between local and shared
//! trait variants. The registry stores and resolves capability bindings.
//!
//! Capability traits themselves live in [`crate::local::capability`] and
//! [`crate::shared::capability`].

/// Capability registry and sealed trait infrastructure.
pub mod registry;

/// Bearer token provider capability handle.
pub mod bearer_token_provider;

/// Key-value store capability handle.
pub mod key_value_store;
