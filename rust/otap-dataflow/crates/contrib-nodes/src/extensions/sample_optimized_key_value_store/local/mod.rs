// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local (!Send) optimized key-value store.
//!
//! Uses `Rc<RefCell<HashMap>>` — zero locking overhead on single-threaded
//! runtimes. This is the performance-optimized path for local consumers.

pub mod extension;
pub use extension::SampleOptimizedKeyValueStore;
