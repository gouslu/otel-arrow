// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Re-exports of local (!Send) capability traits.
//!
//! Canonical definitions live in `crate::capability::<name>::local`.
//! These re-exports let extension authors import via `local::capability::`.

pub use crate::capability::bearer_token_provider::local::BearerTokenProvider;
pub use crate::capability::key_value_store::local::KeyValueStore;
