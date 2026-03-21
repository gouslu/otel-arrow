// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Capability handle definitions and registry.
//!
//! Each capability is defined via the `#[capability]` proc macro, which generates
//! local/shared trait variants, a `SharedAsLocal` adapter, a handle enum, and
//! registry glue. The registry stores and resolves capability bindings.
//!
//! Consumer code uses `capabilities.require_local::<H>()` or
//! `capabilities.require_shared::<H>()` in node factories.

/// Capability registry and sealed trait infrastructure.
pub mod registry;

/// Bearer token provider capability handle.
pub mod bearer_token_provider;

/// Key-value store capability handle.
pub mod key_value_store;
