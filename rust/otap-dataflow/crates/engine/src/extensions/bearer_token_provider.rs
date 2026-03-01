// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Token provider extension trait.
//!
//! A single `BearerTokenProvider` trait — `Send + 'static` (needed for
//! the builder phase). Async methods return `BoxFuture<'static>` so the
//! returned future is independent of the `&self` borrow.

use futures::future::BoxFuture;
use std::borrow::Cow;

/// Represents a secret value that should not be exposed in logs or debug output.
///
/// The [`Debug`] implementation will not print the actual secret value.
#[derive(Clone, Eq)]
pub struct Secret(Cow<'static, str>);

impl Secret {
    /// Creates a new `Secret`.
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

// Constant-time comparison to prevent timing attacks.
// Note: LLVM may optimize this in unexpected ways.
impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        let a = self.secret();
        let b = other.secret();

        if a.len() != b.len() {
            return false;
        }

        a.bytes()
            .zip(b.bytes())
            .fold(0, |acc, (a, b)| acc | (a ^ b))
            == 0
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

/// A trait for components that can provide bearer authentication tokens.
///
/// Extensions implementing this trait can be looked up by other components
/// (e.g., exporters) to obtain tokens for authentication.
///
/// # Design
///
/// - `Send + 'static` — needed for the builder phase (crossing thread
///   boundaries). The registry itself is `Rc`-backed (`!Send`).
///
/// - `get_token()` returns `BoxFuture<'static, ...>` — an owned future that
///   does not borrow `&self`. This allows the future to be awaited
///   independently.
///
/// - `subscribe_token_refresh()` returns a `watch::Receiver` by value — an
///   independent subscription handle.
///
/// # Implementing
///
/// ```ignore
/// use futures::future::BoxFuture;
/// use otap_df_engine::extensions::{BearerToken, BearerTokenProvider, Error};
/// use tokio::sync::watch;
///
/// impl BearerTokenProvider for MyAuthExtension {
///     fn get_token(&self) -> BoxFuture<'static, Result<BearerToken, Error>> {
///         let credential = self.credential.clone();
///         let scope = self.scope.clone();
///         Box::pin(async move {
///             let token = credential.get_token(&[&scope]).await?;
///             Ok(BearerToken::new(token.token.secret().to_string(), token.expires_on))
///         })
///     }
///
///     fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> {
///         self.token_tx.subscribe()
///     }
/// }
/// ```
///
/// # Consuming
///
/// Access via [`handle`](crate::extensions::ExtensionRegistry::handle) +
/// [`lock`](crate::extensions::ExtensionHandle::lock).
/// Extract owned values from the closure — don't `.await` inside it:
///
/// ```ignore
/// let auth = extension_registry.handle::<dyn BearerTokenProvider>("azure_identity_auth")?;
///
/// // Get a token subscription:
/// let mut token_rx = auth.lock(|a| a.subscribe_token_refresh());
///
/// // Or get a token future (await it OUTSIDE the closure):
/// let token = auth.lock(|a| a.get_token()).await?;
/// ```
pub trait BearerTokenProvider: Send + 'static {
    /// Returns an authentication token.
    ///
    /// Returns a `BoxFuture<'static>` — the future owns all data it needs and
    /// does not borrow `&self`. Implementors should clone any shared state
    /// (e.g., `Arc<Credential>`) into the future.
    ///
    /// # Errors
    ///
    /// Returns an error if the token cannot be obtained.
    fn get_token(&self) -> BoxFuture<'static, Result<BearerToken, crate::extensions::Error>>;

    /// Subscribes to token refresh events.
    ///
    /// Returns a new receiver that will be notified whenever the token
    /// is refreshed. Each call creates an independent subscription.
    /// The receiver always contains the latest token value (or `None`
    /// if no token has been acquired yet).
    fn subscribe_token_refresh(&self) -> tokio::sync::watch::Receiver<Option<BearerToken>>;
}
