// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) optimized key-value store.
//!
//! Uses `Arc<RwLock<HashMap>>` — thread-safe but with locking overhead.
//! This is the fallback for shared consumers or multi-threaded runtimes.

pub mod extension;
pub use extension::SampleOptimizedKeyValueStore;
