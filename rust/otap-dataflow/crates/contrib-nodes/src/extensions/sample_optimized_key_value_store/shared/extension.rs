// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) key-value store extension implementation.
//!
//! Uses `Arc<RwLock<HashMap>>` — clone-safe and `Send`.

use async_trait::async_trait;
use otap_df_engine::capability::registry::Error;
use otap_df_engine::shared::capability::KeyValueStore;
use otap_df_telemetry::otel_debug;
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
        let result = self.data.read().await.get(key).cloned();
        otel_debug!(
            "sample_optimized_kv.shared.get",
            key = key,
            found = result.is_some()
        );
        Ok(result)
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        otel_debug!(
            "sample_optimized_kv.shared.set",
            key = key,
            value_len = value.len()
        );
        let _ = self.data.write().await.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        otel_debug!("sample_optimized_kv.shared.delete", key = key);
        let _ = self.data.write().await.remove(key);
        Ok(())
    }
}
