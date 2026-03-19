// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared key-value store extension implementation.
//!
//! All state behind `Arc<RwLock<>>` — clone-safe and `Send`.

use async_trait::async_trait;
use otap_df_engine::local::capability::KeyValueStore as LocalKeyValueStore;
use otap_df_engine::shared::capability::KeyValueStore;
use otap_df_engine::capability::registry::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared-only in-memory key-value store.
///
/// Uses `Arc<RwLock<HashMap>>` — all clones share the same data.
/// Demonstrates the `with_shared()` only pattern: no local variant needed
/// because the backing store is inherently thread-safe.
#[derive(Clone)]
pub struct SampleSharedKeyValueStore {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SampleSharedKeyValueStore {
    /// Creates a new empty shared key-value store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl KeyValueStore for SampleSharedKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.read().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        let _ = self.data.write().await.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let _ = self.data.write().await.remove(key);
        Ok(())
    }
}

// Also implement local trait — this Send type works fine on a single-threaded
// LocalSet. This is the piggyback pattern: one type serves both variants.
#[async_trait(?Send)]
impl LocalKeyValueStore for SampleSharedKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.read().await.get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        let _ = self.data.write().await.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let _ = self.data.write().await.remove(key);
        Ok(())
    }
}
