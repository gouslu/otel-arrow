// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Token provider extension trait.
//!
//! This is the canonical definition site for the bearer token types and the
//! `BearerTokenProvider` extension trait pair (shared + local).

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

// ── BearerTokenProvider (shared) ─────────────────────────────────────────────

/// A trait for components that can provide bearer authentication tokens.
///
/// This is the **shared** variant (`Send + Sync`, stored as `Arc`).
/// Use this for shared components and thread-safe contexts (tonic interceptors,
/// shared receivers).
///
/// For the local variant (no bounds, stored as `Rc`), see
/// [`crate::local::extensions::BearerTokenProvider`].
///
/// # Implementing
///
/// When both implementations are identical (common for `Send + Sync` types),
/// use [`impl_extension_trait!`] to avoid duplication:
///
/// ```ignore
/// impl_extension_trait! {
///     impl BearerTokenProvider for MyAuthExtension {
///         async fn get_token(&self) -> Result<BearerToken, otap_df_engine::extensions::Error> { ... }
///         fn subscribe_token_refresh(&self) -> watch::Receiver<Option<BearerToken>> { ... }
///     }
/// }
/// ```
///
/// Or implement each variant separately when the implementations differ:
///
/// ```ignore
/// #[async_trait]
/// impl otap_df_engine::shared::extensions::BearerTokenProvider for MyExt { ... }
///
/// #[async_trait(?Send)]
/// impl otap_df_engine::local::extensions::BearerTokenProvider for MyExt { ... }
/// ```
#[async_trait::async_trait]
pub trait BearerTokenProvider: Send + Sync {
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
impl crate::shared::extensions::SharedExtensionTrait for dyn BearerTokenProvider {}
