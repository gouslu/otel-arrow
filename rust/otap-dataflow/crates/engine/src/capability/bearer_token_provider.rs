// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bearer token provider extension capability.
//!
//! The local and shared trait variants live in their natural homes:
//! - [`crate::local::bearer_token_provider::BearerTokenProvider`]
//! - [`crate::shared::bearer_token_provider::BearerTokenProvider`]
//!
//! This module defines the handle enum that dispatches to whichever
//! variant the engine selects, plus shared data types (`BearerToken`, `Secret`).

use std::borrow::Cow;
use std::rc::Rc;

// Register the capability: handle type, local/shared traits, name, description.
crate::register_capability!(
    BearerTokenProvider,
    crate::local::capability::BearerTokenProvider,
    crate::shared::capability::BearerTokenProvider,
    "bearer_token_provider",
    "Provides bearer tokens for authenticated HTTP/gRPC requests",
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

/// Handle that dispatches to either the local or shared variant.
///
/// Consumers call methods on the handle without knowing which variant
/// they received. The engine selects the variant at pipeline build time
/// based on extension scope and consumer node type.
pub enum BearerTokenProvider {
    /// Rc-based variant — true single-instance sharing for local consumers.
    Local(Rc<dyn crate::local::capability::BearerTokenProvider>),
    /// Box-based variant — clone-distributed for shared consumers.
    Shared(Box<dyn crate::shared::capability::BearerTokenProvider>),
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

    type Local = dyn crate::local::capability::BearerTokenProvider;
    type Shared = dyn crate::shared::capability::BearerTokenProvider;

    fn from_local(local: Rc<dyn crate::local::capability::BearerTokenProvider>) -> Self {
        Self::Local(local)
    }

    fn from_shared(shared: Box<<Self as super::registry::CapabilityHandle>::Shared>) -> Self {
        Self::Shared(shared)
    }
}
