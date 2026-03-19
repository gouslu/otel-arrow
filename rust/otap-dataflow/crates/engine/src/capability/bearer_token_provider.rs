// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bearer token provider capability.
//!
//! Types, local/shared traits, and the dispatch handle — all in one place.
//! Use `local::BearerTokenProvider` or `shared::BearerTokenProvider` for
//! trait implementations. Use the top-level `BearerTokenProvider` handle
//! in consumers.

use async_trait::async_trait;
use std::borrow::Cow;
use std::rc::Rc;

// Register the capability.
crate::register_capability!(
    BearerTokenProvider,
    local::BearerTokenProvider,
    shared::BearerTokenProvider,
    "bearer_token_provider",
    "Provides bearer tokens for authenticated HTTP/gRPC requests",
);

// ── Shared data types ───────────────────────────────────────────────────────

/// Represents a secret value that should not be exposed in logs or debug output.
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

// ── Local trait ─────────────────────────────────────────────────────────────

// The local/shared trait variants are defined inline here so that types
// (BearerToken, Secret, Error) and traits live together without cross-folder
// dependencies. However, extension authors should import via the root-level
// re-exports at `local::capability::BearerTokenProvider` and
// `shared::capability::BearerTokenProvider` — not through these inline mods.
// The #[doc(hidden)] attribute steers discovery toward the re-exports.

/// Local (!Send) bearer token provider trait.
#[doc(hidden)]
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

// ── Shared trait ────────────────────────────────────────────────────────────

/// Shared (Send) bearer token provider trait.
#[doc(hidden)]
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

// ── Handle ──────────────────────────────────────────────────────────────────

/// Handle that dispatches to either the local or shared variant.
pub enum BearerTokenProvider {
    /// Rc-based variant for local consumers.
    Local(Rc<dyn local::BearerTokenProvider>),
    /// Box-based variant for shared consumers.
    Shared(Box<dyn shared::BearerTokenProvider>),
}

impl BearerTokenProvider {
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

impl super::registry::CapabilityHandle for BearerTokenProvider {
    const CAPABILITY_NAME: &'static str =
        <Self as super::registry::ExtensionCapability>::NAME;

    type Local = dyn local::BearerTokenProvider;
    type Shared = dyn shared::BearerTokenProvider;

    fn from_local(local: Rc<dyn local::BearerTokenProvider>) -> Self {
        Self::Local(local)
    }

    fn from_shared(shared: Box<<Self as super::registry::CapabilityHandle>::Shared>) -> Self {
        Self::Shared(shared)
    }
}
