// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Bearer token provider capability.
//!
//! Shared data types (`BearerToken`, `Secret`) are defined here alongside the
//! `#[capability]` macro invocation. The macro generates local/shared trait
//! variants, a `SharedAsLocal` adapter, sealed trait impls, a
//! `KNOWN_CAPABILITIES` entry, and type-erased coercion functions.
//!
//! Extension authors import via `local::capability::BearerTokenProvider` and
//! `shared::capability::BearerTokenProvider`. Consumers use
//! `capabilities.require_local::<dyn BearerTokenProvider>()` or
//! `capabilities.require_shared::<dyn BearerTokenProvider>()`.

use std::borrow::Cow;

use otap_df_engine_macros::capability;

type Error = super::registry::Error;

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

// ── Capability ──────────────────────────────────────────────────────────────

/// The `#[capability]` macro generates everything from this trait definition:
/// local/shared trait variants, `SharedAsLocal` adapter, sealed impls,
/// a zero-sized registration struct, `KNOWN_CAPABILITIES` entry, and
/// coercion functions.
#[capability(
    name = "bearer_token_provider",
    description = "Provides bearer tokens for authenticated HTTP/gRPC requests"
)]
pub trait BearerTokenProvider {
    /// Returns an authentication token.
    async fn get_token(&self) -> Result<BearerToken, Error>;

    /// Subscribes to token refresh events.
    fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
}
