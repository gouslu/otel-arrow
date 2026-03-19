// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// Azure Identity Auth Extension
#[cfg(feature = "azure-identity-auth-extension")]
pub mod azure_identity_auth_extension;

/// Sample shared-only key-value store extension (demonstrates `with_shared()` only pattern)
pub mod sample_shared_key_value_store;

/// Sample optimized key-value store extension (demonstrates dual `with_local()` + `with_shared()` pattern)
pub mod sample_optimized_key_value_store;
