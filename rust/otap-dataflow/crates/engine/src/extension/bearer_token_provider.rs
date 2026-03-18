// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bearer token provider extension capability.
//!
//! Provides `local::BearerTokenProvider` (!Send) and `shared::BearerTokenProvider` (Send)
//! variants, plus a `BearerTokenProviderHandle` that dispatches to whichever
//! variant the engine selects for the consumer.

use async_trait::async_trait;
use std::borrow::Cow;

// Register both local and shared variants as known capabilities.
// Using unique static names to avoid linker collisions.
crate::register_capability!(
    local::BearerTokenProvider,
    "bearer_token_provider",
    "Provides bearer tokens for authenticated HTTP/gRPC requests (local variant)",
    _KNOWN_CAP_BEARER_LOCAL,
);

crate::register_capability!(
    shared::BearerTokenProvider,
    "bearer_token_provider",
    "Provides bearer tokens for authenticated HTTP/gRPC requests (shared variant)",
    _KNOWN_CAP_BEARER_SHARED,
);

/// Represents a secret value that should not be exposed in logs or debug output.
///
/// The [`Debug`] implementation will not print the actual secret value.
#[derive(Clone, Eq)]
pub struct Secret(Cow<'static, str>);

impl Secret {
    /// Creates a new `Secret`.
    #[must_use]
    pub fn new<T>(value: T) -> Self
    where
        T: Into<Cow<'static, str>>,
    {
        Self(value.into())
    }

    /// Returns the secret value.
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.secret() == other.secret()
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&'static str> for Secret {
    fn from(value: &'static str) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret")
    }
}

/// Represents a bearer token with its expiration time.
///
/// The token value is wrapped in [`Secret`] to prevent accidental exposure
/// in logs or debug output.
#[derive(Debug, Clone)]
pub struct BearerToken {
    /// The token value.
    pub token: Secret,

    /// The expiration time as a UNIX timestamp (seconds since epoch).
    pub expires_on: i64,
}

impl BearerToken {
    /// Creates a new bearer token.
    #[must_use]
    pub fn new<T>(token: T, expires_on: i64) -> Self
    where
        T: Into<Secret>,
    {
        Self {
            token: token.into(),
            expires_on,
        }
    }
}

/// !Send variant for local nodes running on a single-threaded LocalSet.
///
/// Implementations can use `Rc`, `RefCell`, and other !Send types.
/// The returned future is !Send.
pub mod local {
    use super::*;

    /// A bearer token provider for local (!Send) contexts.
    #[async_trait(?Send)]
    pub trait BearerTokenProvider {
        /// Returns an authentication token.
        async fn get_token(&self) -> Result<BearerToken, super::super::registry::Error>;

        /// Subscribes to token refresh events.
        fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
    }
}

/// Send variant for shared nodes that may run on multi-threaded executors.
///
/// Implementations must be Send. The returned future is Send.
pub mod shared {
    use super::*;

    /// A bearer token provider for shared (Send) contexts.
    #[async_trait]
    pub trait BearerTokenProvider: Send {
        /// Returns an authentication token.
        async fn get_token(&self) -> Result<BearerToken, super::super::registry::Error>;

        /// Subscribes to token refresh events.
        fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
    }
}

/// Handle that dispatches to either the local or shared variant.
///
/// Consumers call methods on the handle without knowing which variant
/// they received. The engine selects the variant at pipeline build time
/// based on extension scope and consumer node type.
pub enum BearerTokenProviderHandle {
    /// !Send variant — used for local consumers of pipeline-scoped extensions.
    Local(Box<dyn local::BearerTokenProvider>),
    /// Send variant — used for shared consumers or cross-scope extensions.
    Shared(Box<dyn shared::BearerTokenProvider>),
}

impl BearerTokenProviderHandle {
    /// Returns an authentication token from the underlying provider.
    pub async fn get_token(&self) -> Result<BearerToken, super::registry::Error> {
        match self {
            Self::Local(p) => p.get_token().await,
            Self::Shared(p) => p.get_token().await,
        }
    }

    /// Subscribes to token refresh events from the underlying provider.
    pub fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>> {
        match self {
            Self::Local(p) => p.subscribe_token_refresh(),
            Self::Shared(p) => p.subscribe_token_refresh(),
        }
    }
}

impl super::registry::CapabilityHandle for BearerTokenProviderHandle {
    type Local = dyn local::BearerTokenProvider;
    type Shared = dyn shared::BearerTokenProvider;

    fn from_local(local: Box<<Self as super::registry::CapabilityHandle>::Local>) -> Self {
        Self::Local(local)
    }

    fn from_shared(shared: Box<<Self as super::registry::CapabilityHandle>::Shared>) -> Self {
        Self::Shared(shared)
    }
}
