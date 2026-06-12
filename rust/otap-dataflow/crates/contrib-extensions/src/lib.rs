// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Contributed OTAP extensions.
//!
//! Each extension is feature-gated. Enable an extension's Cargo feature to
//! link it into the binary so its `#[distributed_slice]` factory registration
//! takes effect.

#[cfg(feature = "azure-identity-auth-extension")]
pub mod azure_identity_auth;
