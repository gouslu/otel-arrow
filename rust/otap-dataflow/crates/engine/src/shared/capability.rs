// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) capability traits for extensions.

/// Shared bearer token provider trait.
pub mod bearer_token_provider;
/// Shared key-value store trait.
pub mod key_value_store;

// Re-export capability traits for convenience:
// `shared::capability::BearerTokenProvider` instead of
// `shared::capability::bearer_token_provider::BearerTokenProvider`
pub use bearer_token_provider::BearerTokenProvider;
pub use key_value_store::KeyValueStore;
