// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Key-value store extension capability.
//!
//! Provides `local::KeyValueStore` (!Send) and `shared::KeyValueStore` (Send)
//! variants, plus a `KeyValueStore` handle that dispatches to whichever
//! variant the engine selects for the consumer.
//!
//! This capability mirrors Go's `storage.Client` interface from
//! `go.opentelemetry.io/collector/extension/xextension/storage`.
//! Consumers use it for checkpointing, offset tracking, and other
//! persistent or ephemeral key-value storage needs.

use async_trait::async_trait;
use std::rc::Rc;

// Register the capability: handle type, local/shared traits, name, description.
crate::register_capability!(
    KeyValueStore,
    local::KeyValueStore,
    shared::KeyValueStore,
    "key_value_store",
    "Provides key-value storage (get/set/delete) for pipeline components",
);

/// !Send variant for local nodes running on a single-threaded LocalSet.
///
/// Implementations can use `Rc`, `RefCell`, and other !Send types.
pub mod local {
    use super::*;

    /// A key-value store for local (!Send) contexts.
    #[async_trait(?Send)]
    pub trait KeyValueStore {
        /// Retrieves the value associated with the given key.
        /// Returns `None` if the key does not exist.
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, super::super::registry::Error>;

        /// Stores a value under the given key, replacing any existing value.
        async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), super::super::registry::Error>;

        /// Removes the value associated with the given key.
        /// No-op if the key does not exist.
        async fn delete(&self, key: &str) -> Result<(), super::super::registry::Error>;
    }
}

/// Send variant for shared nodes that may run on multi-threaded executors.
///
/// Implementations must be Send.
pub mod shared {
    use super::*;

    /// A key-value store for shared (Send) contexts.
    #[async_trait]
    pub trait KeyValueStore: Send {
        /// Retrieves the value associated with the given key.
        /// Returns `None` if the key does not exist.
        async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, super::super::registry::Error>;

        /// Stores a value under the given key, replacing any existing value.
        async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), super::super::registry::Error>;

        /// Removes the value associated with the given key.
        /// No-op if the key does not exist.
        async fn delete(&self, key: &str) -> Result<(), super::super::registry::Error>;
    }
}

/// Handle that dispatches to either the local or shared variant.
///
/// Consumers call methods on the handle without knowing which variant
/// they received. The engine selects the variant at pipeline build time
/// based on extension scope and consumer node type.
pub enum KeyValueStore {
    /// Rc-based variant — true single-instance sharing for local consumers.
    Local(Rc<dyn local::KeyValueStore>),
    /// Box-based variant — clone-distributed for shared consumers.
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
