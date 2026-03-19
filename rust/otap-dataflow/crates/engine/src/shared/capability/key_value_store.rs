// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) key-value store trait.

use async_trait::async_trait;
use crate::capability::registry::Error;

/// A key-value store for shared (Send) contexts.
///
/// Implementations must be Send.
#[async_trait]
pub trait KeyValueStore: Send {
    /// Retrieves the value associated with the given key.
    /// Returns `None` if the key does not exist.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;

    /// Stores a value under the given key, replacing any existing value.
    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error>;

    /// Removes the value associated with the given key.
    /// No-op if the key does not exist.
    async fn delete(&self, key: &str) -> Result<(), Error>;
}
