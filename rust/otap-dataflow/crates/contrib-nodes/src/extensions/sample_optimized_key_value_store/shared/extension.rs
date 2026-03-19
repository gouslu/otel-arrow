// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) key-value store extension implementation.
//!
//! Uses `Arc<RwLock<HashMap>>` — clone-safe and `Send`.

use async_trait::async_trait;
use otap_df_engine::extension::key_value_store::shared::KeyValueStore;
use otap_df_engine::extension::registry::Error;
use otap_df_engine::shared::extension::Extension;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared key-value store — `Arc<RwLock<HashMap>>`, thread-safe.
#[derive(Clone)]
pub struct SampleOptimizedKeyValueStore {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl SampleOptimizedKeyValueStore {
    /// Creates a new empty shared key-value store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl KeyValueStore for SampleOptimizedKeyValueStore {
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

#[async_trait]
// Passive extension — no background work needed, uses default start().
#[async_trait]
impl Extension for SampleOptimizedKeyValueStore {}
