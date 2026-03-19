// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local (!Send) key-value store extension implementation.
//!
//! No `Arc`, no `RwLock` — just `Rc<RefCell<HashMap>>`. True zero-cost
//! sharing on a single-threaded LocalSet.

use async_trait::async_trait;
use otap_df_engine::local::capability::KeyValueStore;
use otap_df_engine::capability::registry::Error;
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
