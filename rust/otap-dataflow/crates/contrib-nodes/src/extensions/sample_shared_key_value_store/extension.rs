// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared key-value store extension implementation.
//!
//! All state behind `Arc<RwLock<>>` — clone-safe and `Send`.

use async_trait::async_trait;
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::key_value_store::shared::KeyValueStore;
use otap_df_engine::extension::key_value_store::local::KeyValueStore as LocalKeyValueStore;
use otap_df_engine::extension::registry::Error;
use otap_df_engine::extension::{ControlChannel, EffectHandler};
use otap_df_engine::shared::extension::Extension;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_telemetry::otel_info;
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

#[async_trait]
impl Extension for SampleSharedKeyValueStore {
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: ControlChannel,
        _effect_handler: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        otel_info!("sample_shared_kv_store.start");

        loop {
            match ctrl_chan.recv().await? {
                ExtensionControlMsg::Shutdown { deadline, .. } => {
                    otel_info!("sample_shared_kv_store.shutdown");
                    return Ok(TerminalState::default());
                }
                _ => {}
            }
        }
    }
}
