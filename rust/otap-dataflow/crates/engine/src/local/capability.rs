// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local (!Send) capability traits for extensions.

/// Local bearer token provider trait.
pub mod bearer_token_provider;
/// Local key-value store trait.
pub mod key_value_store;

// Re-export capability traits for convenience:
// `local::capability::BearerTokenProvider` instead of
// `local::capability::bearer_token_provider::BearerTokenProvider`
pub use bearer_token_provider::BearerTokenProvider;
pub use key_value_store::KeyValueStore;
