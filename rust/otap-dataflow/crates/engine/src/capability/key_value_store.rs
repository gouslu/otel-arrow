// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Key-value store capability.
//!
//! The `#[capability]` macro generates local/shared trait variants, a
//! `SharedAsLocal` adapter, sealed impls, a `KNOWN_CAPABILITIES` entry,
//! and coercion functions from a single trait definition.
//! Mirrors Go's `storage.Client` interface.

use otap_df_engine_macros::capability;

type Error = super::registry::Error;

/// The `#[capability]` macro generates everything from this trait definition.
#[capability(
    name = "key_value_store",
    description = "Provides key-value storage (get/set/delete) for pipeline components"
)]
pub trait KeyValueStore {
    /// Retrieves the value associated with the given key.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Stores a value under the given key, replacing any existing value.
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error>;

    /// Removes the value associated with the given key.
    async fn delete(&self, key: &str) -> Result<(), Error>;
}
