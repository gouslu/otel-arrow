// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Local bearer token provider trait (no bounds, stored as `Rc`).
//!
//! This is the local variant of the bearer token provider trait.
//! For the shared variant (`Send + Sync`, stored as `Arc`), see
//! [`crate::shared::extensions::BearerTokenProvider`].

pub use crate::shared::extensions::bearer_token_provider::{BearerToken, Secret};

/// A trait for components that can provide bearer authentication tokens.
///
/// This is the **local** variant (no bounds, stored as `Rc`).
/// Use this for local components where atomic/mutex overhead matters.
/// Implementations can use `Rc`, `RefCell`, etc.
///
/// For the shared variant (`Send + Sync`, stored as `Arc`), see
/// [`crate::shared::extensions::BearerTokenProvider`].
#[async_trait::async_trait(?Send)]
pub trait BearerTokenProvider {
    /// Returns an authentication token.
    ///
    /// # Errors
    ///
    /// Returns an error if the token cannot be obtained.
    async fn get_token(&self) -> Result<BearerToken, crate::extensions::Error>;

    /// Subscribes to token refresh events.
    ///
    /// Returns a new receiver that will be notified whenever the token
    /// is refreshed. Each call creates an independent subscription.
    /// The receiver always contains the latest token value (or `None`
    /// if no token has been acquired yet).
    fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
}

impl crate::extensions::private::Sealed for dyn BearerTokenProvider {}
impl crate::local::extensions::LocalExtensionTrait for dyn BearerTokenProvider {}
