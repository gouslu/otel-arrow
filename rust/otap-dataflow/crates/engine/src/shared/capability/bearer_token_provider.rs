// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared (Send) bearer token provider trait.

use async_trait::async_trait;
use crate::capability::bearer_token_provider::BearerToken;
use crate::capability::registry::Error;

/// A bearer token provider for shared (Send) contexts.
///
/// Implementations must be Send. The returned future is Send.
#[async_trait]
pub trait BearerTokenProvider: Send {
    /// Returns an authentication token.
    async fn get_token(&self) -> Result<BearerToken, Error>;

    /// Subscribes to token refresh events.
    fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
}
