// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Key-value store extension capability.
//!
//! The local and shared trait variants live in their natural homes:
//! - [`crate::local::key_value_store::KeyValueStore`]
//! - [`crate::shared::key_value_store::KeyValueStore`]
//!
//! This module defines the handle enum that dispatches to whichever
//! variant the engine selects. Mirrors Go's `storage.Client` interface.

use std::rc::Rc;

// Register the capability: handle type, local/shared traits, name, description.
crate::register_capability!(
    KeyValueStore,
    crate::local::capability::KeyValueStore,
    crate::shared::capability::KeyValueStore,
    "key_value_store",
    "Provides key-value storage (get/set/delete) for pipeline components",
);

/// Handle that dispatches to either the local or shared variant.
pub enum KeyValueStore {
    /// Rc-based variant — true single-instance sharing for local consumers.
    Local(Rc<dyn crate::local::capability::KeyValueStore>),
    /// Box-based variant — clone-distributed for shared consumers.
    Shared(Box<dyn crate::shared::capability::KeyValueStore>),
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

    type Local = dyn crate::local::capability::KeyValueStore;
    type Shared = dyn crate::shared::capability::KeyValueStore;

    fn from_local(local: Rc<dyn crate::local::capability::KeyValueStore>) -> Self {
        Self::Local(local)
    }

    fn from_shared(shared: Box<<Self as super::registry::CapabilityHandle>::Shared>) -> Self {
        Self::Shared(shared)
    }
}
