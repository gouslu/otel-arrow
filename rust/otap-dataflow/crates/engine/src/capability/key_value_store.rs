// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Key-value store capability.
//!
//! Types, local/shared traits, and the dispatch handle — all in one place.
//! Mirrors Go's `storage.Client` interface.

use async_trait::async_trait;
use std::rc::Rc;

// Register the capability.
crate::register_capability!(
    KeyValueStore,
    local::KeyValueStore,
    shared::KeyValueStore,
    "key_value_store",
    "Provides key-value storage (get/set/delete) for pipeline components",
);

// ── Local trait ─────────────────────────────────────────────────────────────

// The local/shared trait variants are defined inline here so that types
// and traits live together without cross-folder dependencies. Extension
// authors should import via the root-level re-exports at
// `local::capability::KeyValueStore` and `shared::capability::KeyValueStore`
// — not through these inline mods.

/// Local (!Send) key-value store trait.
#[doc(hidden)]
pub mod local {
    use super::*;

    /// A key-value store for local (!Send) contexts.
    #[async_trait(?Send)]
    pub trait KeyValueStore {
        /// Retrieves the value associated with the given key.
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, super::super::registry::Error>;

        /// Stores a value under the given key, replacing any existing value.
        async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), super::super::registry::Error>;

        /// Removes the value associated with the given key.
        async fn delete(&self, key: &str) -> Result<(), super::super::registry::Error>;
    }
}

// ── Shared trait ────────────────────────────────────────────────────────────

/// Shared (Send) key-value store trait.
#[doc(hidden)]
pub mod shared {
    use super::*;

    /// A key-value store for shared (Send) contexts.
    #[async_trait]
    pub trait KeyValueStore: Send {
        /// Retrieves the value associated with the given key.
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, super::super::registry::Error>;

        /// Stores a value under the given key, replacing any existing value.
        async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), super::super::registry::Error>;

        /// Removes the value associated with the given key.
        async fn delete(&self, key: &str) -> Result<(), super::super::registry::Error>;
    }
}

// ── Handle ──────────────────────────────────────────────────────────────────

/// Handle that dispatches to either the local or shared variant.
pub enum KeyValueStore {
    /// Rc-based variant for local consumers.
    Local(Rc<dyn local::KeyValueStore>),
    /// Box-based variant for shared consumers.
    Shared(Box<dyn shared::KeyValueStore>),
}

impl KeyValueStore {
    /// Retrieves the value associated with the given key.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, super::registry::Error> {
        match self {
            Self::Local(s) => s.get(key).await,
            Self::Shared(s) => s.get(key).await,
        }
    }

    /// Stores a value under the given key.
    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), super::registry::Error> {
        match self {
            Self::Local(s) => s.set(key, value).await,
            Self::Shared(s) => s.set(key, value).await,
        }
    }

    /// Removes the value associated with the given key.
    pub async fn delete(&self, key: &str) -> Result<(), super::registry::Error> {
        match self {
            Self::Local(s) => s.delete(key).await,
            Self::Shared(s) => s.delete(key).await,
        }
    }
}

impl super::registry::CapabilityHandle for KeyValueStore {
    const CAPABILITY_NAME: &'static str =
        <Self as super::registry::ExtensionCapability>::NAME;

    type Local = dyn local::KeyValueStore;
    type Shared = dyn shared::KeyValueStore;

    fn from_local(local: Rc<dyn local::KeyValueStore>) -> Self {
        Self::Local(local)
    }

    fn from_shared(shared: Box<<Self as super::registry::CapabilityHandle>::Shared>) -> Self {
        Self::Shared(shared)
    }
}
