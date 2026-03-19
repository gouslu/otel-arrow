// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local (!Send) key-value store extension implementation.
//!
//! No `Arc`, no `RwLock` — just `Rc<RefCell<HashMap>>`. True zero-cost
//! sharing on a single-threaded LocalSet.

use async_trait::async_trait;
use otap_df_engine::control::ExtensionControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::extension::key_value_store::local::KeyValueStore;
use otap_df_engine::extension::registry::Error;
use otap_df_engine::extension::{ControlChannel, EffectHandler};
use otap_df_engine::local::extension::Extension;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_telemetry::otel_info;
use std::cell::RefCell;
use std::collections::HashMap;

/// Local key-value store — `Rc<RefCell<HashMap>>`, no locking.
pub struct SampleOptimizedKeyValueStore {
    data: RefCell<HashMap<String, Vec<u8>>>,
}

impl SampleOptimizedKeyValueStore {
    /// Creates a new empty local key-value store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: RefCell::new(HashMap::new()),
        }
    }
}

#[async_trait(?Send)]
impl KeyValueStore for SampleOptimizedKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        Ok(self.data.borrow().get(key).cloned())
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        let _ = self.data.borrow_mut().insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let _ = self.data.borrow_mut().remove(key);
        Ok(())
    }
}

#[async_trait(?Send)]
impl Extension for SampleOptimizedKeyValueStore {
    async fn start(
        self: std::rc::Rc<Self>,
        mut ctrl_chan: ControlChannel,
        _effect_handler: EffectHandler,
    ) -> Result<TerminalState, EngineError> {
        otel_info!("sample_optimized_kv_store.local.start");

        loop {
            match ctrl_chan.recv().await? {
                ExtensionControlMsg::Shutdown { deadline, .. } => {
                    otel_info!("sample_optimized_kv_store.local.shutdown");
                    return Ok(TerminalState::default());
                }
                _ => {}
            }
        }
    }
}
