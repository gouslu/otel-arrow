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
//! `capabilities.require_local::<BearerTokenProvider>()` or
//! `capabilities.require_shared::<BearerTokenProvider>()`.

use std::borrow::Cow;
use std::pin::Pin;

use futures::stream::Stream;
use otap_df_engine_macros::capability;

use super::CapabilityError;

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
///
/// `expires_on` is a monotonic [`std::time::Instant`] rather than an absolute
/// UNIX timestamp: consumers compare it against `Instant::now()` to decide
/// whether the token is still usable, which is immune to wall-clock jumps
/// (NTP adjustments, sleep/resume) that would otherwise be able to fool a
/// pre-expiry check. Producers are expected to convert their underlying
/// absolute expiry to an `Instant` once, at the point of issuance.
///
/// `expires_on = None` means the token never expires (e.g., static API key).
///
/// Because `Instant` is process-local and not serializable, `BearerToken` is
/// only meaningful within a single process — which matches how capabilities
/// are wired today (extension and consumer share an address space).
///
/// Marked `#[non_exhaustive]` so fields can be added (e.g., `token_type`,
/// `scope`) without an API break; construct via [`BearerToken::new`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BearerToken {
    /// The token value.
    pub token: Secret,
    /// The monotonic instant at which the token expires, or `None` if the
    /// token does not expire.
    pub expires_on: Option<std::time::Instant>,
}

impl BearerToken {
    /// Creates a new bearer token that expires at the given instant.
    #[must_use]
    pub fn new<T>(token: T, expires_on: std::time::Instant) -> Self
    where
        T: Into<Secret>,
    {
        Self {
            token: token.into(),
            expires_on: Some(expires_on),
        }
    }

    /// Creates a new bearer token that never expires.
    #[must_use]
    pub fn non_expiring<T>(token: T) -> Self
    where
        T: Into<Secret>,
    {
        Self {
            token: token.into(),
            expires_on: None,
        }
    }
}

// ── Capability ──────────────────────────────────────────────────────────────

/// Per-subscriber stream of token-acquisition outcomes returned by
/// [`BearerTokenProvider::token_stream`].
///
/// Deliberately **not** `Send`: the stream is polled on the thread that
/// created it (the consuming node's `LocalSet`) and never moved across
/// threads, so requiring `Send` would only restrict `!Send` providers for
/// no benefit. A `Send` provider may still return this — `Send`-ness of the
/// provider does not require its returned values to be `Send`, and a `Send`
/// stream coerces into this `!Send`-bounded box.
pub type TokenStream = Pin<Box<dyn Stream<Item = Result<BearerToken, CapabilityError>> + 'static>>;

/// Provides bearer tokens for authenticated requests.
///
/// Two entry points. Both are implementable by any extension shape — active
/// or passive — without contortions:
///
/// - [`get_token`](Self::get_token) — async one-shot. Active impls serve
///   from their internal cache; passive impls call the credential backend
///   on demand. Concurrent callers MUST be coalesced onto a single
///   in-flight fetch.
///
/// - [`token_stream`](Self::token_stream) — sequence of token-acquisition
///   outcomes. Active impls typically back this with a `watch::Sender` fed
///   by a background task. Passive impls can back this with a self-clone
///   that polls `get_token` and sleeps until just before the previous
///   token's expiry — no background task, no `&self` capture, no shared
///   state beyond what the impl already owns.
///
/// Caching is implicit: `get_token` returns the cached token if still valid
/// and fetches otherwise.
///
/// The `#[capability]` macro generates local/shared trait variants, a
/// `SharedAsLocal` adapter, sealed impls, a zero-sized registration struct,
/// a `KNOWN_CAPABILITIES` entry, and coercion functions from this definition.
#[capability(
    name = "bearer_token_provider",
    description = "Provides bearer tokens for authenticated HTTP/gRPC requests"
)]
pub trait BearerTokenProvider {
    /// Returns a currently-valid bearer token.
    ///
    /// Impls MUST cache internally and return the cached token if it is
    /// still valid (i.e., not expired and not inside any refresh-skew
    /// window the impl applies). Otherwise the impl fetches a fresh token,
    /// caches it, and returns it. Concurrent callers MUST be coalesced
    /// onto a single in-flight fetch.
    ///
    /// Errors propagate the underlying credential failure wrapped in
    /// [`CapabilityError`]. Transient failures are still returned as `Err`;
    /// callers decide whether to retry.
    async fn get_token(&self) -> Result<BearerToken, CapabilityError>;

    /// Returns a stream of token-acquisition outcomes.
    ///
    /// Each item is one acquisition: `Ok` on a successful refresh, `Err`
    /// on a transient credential failure. Consumers typically wire this
    /// into a `select!` loop and swap their cached auth header whenever a
    /// new `Ok` arrives.
    ///
    /// # Contract
    ///
    /// - The stream MUST yield at least once whenever the provider holds a
    ///   valid token.
    /// - `None` is terminal — the provider will not emit again on this
    ///   subscription. Consumers MAY re-resolve the capability to obtain a
    ///   new stream.
    /// - Dropping the stream MUST NOT affect the provider's own state.
    ///
    /// # Implementing
    ///
    /// - **Active** impls back this with a broadcast/watch channel fed by
    ///   their refresh task.
    /// - **Passive** impls back this with a self-clone + polling loop:
    ///
    /// ```ignore
    /// fn token_stream(&self) -> TokenStream {
    ///     let me = self.clone(); // self is an Arc-backed handle
    ///     Box::pin(futures::stream::unfold(
    ///         (me, None::<tokio::time::Instant>),
    ///         |(me, wake)| async move {
    ///             if let Some(at) = wake { tokio::time::sleep_until(at).await; }
    ///             let result = me.get_token().await;
    ///             let next = compute_next_wake(&result);
    ///             Some((result, (me, Some(next))))
    ///         },
    ///     ))
    /// }
    /// ```
    ///
    /// Both shapes give the caller identical observable behavior: a stream
    /// of tokens that arrives at refresh boundaries.
    fn token_stream(&self) -> TokenStream;
}

// ── Tests ───────────────────────────────────────────────────────────────────
//
// These tests exist to prove the trait can be cleanly implemented by a
// *passive* provider — one with no background refresh task, no watch
// channel, no broadcast bus — using only the same `Arc<Inner>`/`Clone`
// idiom that every active extension already uses for its own state. If
// this implementation is forced into contortions, the trait shape is
// wrong; if it falls out naturally, the trait holds for both active and
// passive shapes.

#[cfg(test)]
mod passive_provider_tests {
    use super::shared::BearerTokenProvider as _;
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Minimal passive provider. No background task, no channel. State is
    /// behind `Arc<Inner>` and the struct derives `Clone` — exactly the
    /// shape every shared extension in this codebase already adopts for
    /// its own state sharing, so passive impls add no new convention.
    #[derive(Clone)]
    struct PassiveProvider {
        inner: Arc<PassiveInner>,
    }

    struct PassiveInner {
        /// Number of credential-backend calls. Used by tests to assert
        /// laziness (no background task) and refresh cadence.
        calls: AtomicUsize,
        /// Synthetic token TTL.
        ttl: Duration,
    }

    impl PassiveProvider {
        fn new(ttl: Duration) -> Self {
            Self {
                inner: Arc::new(PassiveInner {
                    calls: AtomicUsize::new(0),
                    ttl,
                }),
            }
        }

        fn call_count(&self) -> usize {
            self.inner.calls.load(Ordering::SeqCst)
        }

        async fn fetch(&self) -> Result<BearerToken, CapabilityError> {
            let n = self.inner.calls.fetch_add(1, Ordering::SeqCst);
            Ok(BearerToken::new(
                Secret::new(format!("token-{n}")),
                std::time::Instant::now() + self.inner.ttl,
            ))
        }
    }

    #[async_trait]
    impl shared::BearerTokenProvider for PassiveProvider {
        async fn get_token(&self) -> Result<BearerToken, CapabilityError> {
            // Passive: just call the backend (which is assumed to do its
            // own caching internally, e.g., the Azure SDK).
            self.fetch().await
        }

        fn token_stream(&self) -> TokenStream {
            // Passive: clone `self` into an unfold loop. The struct is a
            // thin `Arc<PassiveInner>` wrapper, so the clone is one
            // atomic refcount bump — no spawn, no watch channel, no
            // broadcast bus. Each poll triggers a backend call and a
            // sleep until just before the previous token's expiry.
            let me = self.clone();
            Box::pin(futures::stream::unfold(
                (me, None::<tokio::time::Instant>),
                |(me, wake)| async move {
                    if let Some(at) = wake {
                        tokio::time::sleep_until(at).await;
                    }
                    let result = me.fetch().await;
                    let next = match &result {
                        Ok(t) => match t.expires_on {
                            Some(exp) => {
                                let skew = Duration::from_millis(1);
                                let target = exp.checked_sub(skew).unwrap_or(exp);
                                tokio::time::Instant::from_std(target)
                            }
                            None => tokio::time::Instant::now() + Duration::from_secs(3600),
                        },
                        Err(_) => tokio::time::Instant::now() + Duration::from_millis(100),
                    };
                    Some((result, (me, Some(next))))
                },
            ))
        }
    }

    /// `get_token` works without any active machinery. Each call hits the
    /// backend exactly once — passive impls rely on the backend's own
    /// cache, which is the contract the trait documents.
    #[tokio::test]
    async fn passive_get_token_returns_distinct_tokens() {
        let p = PassiveProvider::new(Duration::from_secs(60));
        let a = shared::BearerTokenProvider::get_token(&p).await.unwrap();
        let b = shared::BearerTokenProvider::get_token(&p).await.unwrap();
        assert_eq!(a.token.secret(), "token-0");
        assert_eq!(b.token.secret(), "token-1");
        assert_eq!(p.call_count(), 2);
    }

    /// The stream yields the first token immediately on first poll — no
    /// initial `None` to filter, no warm-up race, no surprise from a
    /// missing seed value.
    #[tokio::test]
    async fn passive_token_stream_yields_first_token_on_first_poll() {
        let p = PassiveProvider::new(Duration::from_secs(60));
        let mut s = p.token_stream();
        let t0 = s.next().await.unwrap().unwrap();
        assert_eq!(t0.token.secret(), "token-0");
    }

    /// Subsequent items arrive after the configured TTL — the stream
    /// refreshes itself purely by polling `get_token` and sleeping. No
    /// background task, no channel, no shared signaling.
    #[tokio::test(start_paused = true)]
    async fn passive_token_stream_refreshes_after_ttl() {
        let p = PassiveProvider::new(Duration::from_millis(50));
        let mut s = p.token_stream();
        let t0 = s.next().await.unwrap().unwrap();
        assert_eq!(t0.token.secret(), "token-0");
        let t1 = s.next().await.unwrap().unwrap();
        assert_eq!(t1.token.secret(), "token-1");
    }

    /// Constructing the stream MUST NOT spawn anything or fetch eagerly.
    /// This is the headline property of a passive impl: zero work until
    /// the consumer polls.
    #[tokio::test(start_paused = true)]
    async fn passive_stream_is_lazy_no_background_work() {
        let p = PassiveProvider::new(Duration::from_secs(60));
        let _s = p.token_stream();
        tokio::time::advance(Duration::from_secs(120)).await;
        assert_eq!(
            p.call_count(),
            0,
            "passive stream must be lazy — no spawn, no eager fetch even after virtual time advances"
        );
    }

    /// Dropping the stream MUST NOT affect the provider — the trait
    /// contract requires it. For passive impls this is automatic: the
    /// stream owns only an Arc clone, so dropping it just decrements the
    /// refcount.
    #[tokio::test]
    async fn dropping_stream_leaves_provider_usable() {
        let p = PassiveProvider::new(Duration::from_secs(60));
        {
            let mut s = p.token_stream();
            let _ = s.next().await;
        }
        let t = shared::BearerTokenProvider::get_token(&p).await.unwrap();
        assert_eq!(t.token.secret(), "token-1");
    }

    /// Multiple concurrent subscriptions are independent — each owns its
    /// own Arc clone and its own unfold state. No shared signaling means
    /// no cross-subscription interference.
    #[tokio::test(start_paused = true)]
    async fn passive_streams_are_independent() {
        let p = PassiveProvider::new(Duration::from_millis(50));
        let mut s1 = p.token_stream();
        let mut s2 = p.token_stream();
        let a = s1.next().await.unwrap().unwrap();
        let b = s2.next().await.unwrap().unwrap();
        // Each subscription independently calls the backend.
        assert_eq!(a.token.secret(), "token-0");
        assert_eq!(b.token.secret(), "token-1");
        assert_eq!(p.call_count(), 2);
    }

    /// Non-expiring tokens (`expires_on: None`) round-trip cleanly — the
    /// constructor exists and the field is `None` as documented.
    #[test]
    fn non_expiring_token_constructor() {
        let t = BearerToken::non_expiring(Secret::new("static-key"));
        assert!(t.expires_on.is_none());
        assert_eq!(t.token.secret(), "static-key");
    }
}
