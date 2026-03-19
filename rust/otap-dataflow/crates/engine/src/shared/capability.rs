// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Re-exports of shared (Send) capability traits.
//!
//! Canonical definitions live in `crate::capability::<name>::shared`.
//! These re-exports let extension authors import via `shared::capability::`.

pub use crate::capability::bearer_token_provider::shared::BearerTokenProvider;
pub use crate::capability::key_value_store::shared::KeyValueStore;
