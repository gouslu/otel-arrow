// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Capability types and registry.
//!
//! Each capability is defined via the `#[capability]` proc macro, which generates
//! local/shared trait variants, a `SharedAsLocal` adapter, sealed trait impls,
//! a `KNOWN_CAPABILITIES` entry, coercion functions, and a zero-sized registration
//! struct. The registry stores and resolves capability bindings.
//!
//! Consumer code uses `capabilities.require_local::<dyn Trait>()` or
//! `capabilities.require_shared::<dyn Trait>()` in node factories.

/// Capability registry and sealed trait infrastructure.
pub mod registry;

/// Bearer token provider capability registration.
pub mod bearer_token_provider;

/// Key-value store capability registration.
pub mod key_value_store;
