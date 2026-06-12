// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Errors returned from capability *method calls* (e.g.,
//! `BearerTokenProvider::get_token`).
//!
//! This is intentionally separate from [`crate::error::Error`], which
//! describes engine-internal failures (channel errors, node lifecycle
//! errors, registry errors, etc.). A capability call error is what one
//! pipeline component reports to another when a capability-backed
//! request fails — the consumer doesn't care about the engine's
//! internal taxonomy, only about *which* capability on *which*
//! extension failed and why.
//!
//! Build-time / registry errors (`CapabilityAlreadyConsumed`, etc.)
//! remain in [`crate::capability::registry::Error`] (an alias of the
//! engine [`crate::error::Error`]) because they *are* engine-internal.

use std::marker::PhantomData;

use super::ExtensionCapability;

/// Error returned from a capability method call.
///
/// Carries the extension + capability identity for diagnostics and the
/// real underlying error as a boxed `dyn Error` so the full source
/// chain is preserved across the trait-object boundary (callers can
/// walk it via `std::error::Error::source()` or
/// [`crate::error::format_error_sources`], or downcast it to a known
/// concrete type).
///
/// The type is intentionally a single struct rather than a categorized
/// enum: classifying transient vs. permanent failures at this layer is
/// usually unreliable (cloud RBAC, network blips, etc. all look the
/// same). Provider implementations are expected to handle their own
/// retry / refresh strategy internally; the error surfaced here is
/// what the consumer sees *after* those strategies have been
/// exhausted (or skipped).
#[derive(thiserror::Error, Debug)]
#[error("capability `{capability}` on extension `{extension}` failed: {source}")]
pub struct CapabilityError {
    /// Name of the extension whose capability instance produced the error.
    pub extension: String,
    /// Capability name (e.g., `"bearer_token_provider"`).
    pub capability: &'static str,
    /// Underlying error — exposed via `std::error::Error::source()`.
    #[source]
    pub source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl CapabilityError {
    /// Wrap any `Error` as a `CapabilityError`.
    ///
    /// Prefer constructing via [`CapabilityErrorSource::wrap`] when the
    /// `(extension, capability)` binding is fixed for an extension
    /// instance — that pattern pre-binds both fields once at
    /// construction time so error sites stay short and consistent.
    #[must_use]
    pub fn new<E>(extension: impl Into<String>, capability: &'static str, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            extension: extension.into(),
            capability,
            source: Box::new(source),
        }
    }
}

/// Pre-bound `(extension, capability)` factory for [`CapabilityError`].
///
/// Extension implementations typically build one of these per
/// capability they expose, store it in their `Inner` state, and call
/// [`wrap`](Self::wrap) at every error site. This keeps both the
/// extension name and the capability name out of every `map_err`
/// site, and guarantees consistent labeling for centralized telemetry
/// (logs, metrics) keyed on `(extension, capability)`.
///
/// The capability binding is a compile-time type parameter, so a
/// source built for one capability cannot accidentally produce errors
/// tagged with another.
///
/// # Example
///
/// ```rust,ignore
/// struct Inner {
///     name: String,
///     cap_err: CapabilityErrorSource<BearerTokenProvider>,
///     /* ... */
/// }
///
/// // construction
/// let cap_err = CapabilityErrorSource::<BearerTokenProvider>::new(name.clone());
///
/// // at an error site
/// .map_err(|e| inner.cap_err.wrap(e))
/// ```
#[derive(Clone, Debug)]
pub struct CapabilityErrorSource<C: ExtensionCapability> {
    extension: String,
    _cap: PhantomData<fn() -> C>,
}

impl<C: ExtensionCapability> CapabilityErrorSource<C> {
    /// Build a source bound to `extension` for capability `C`.
    #[must_use]
    pub fn new(extension: impl Into<String>) -> Self {
        Self {
            extension: extension.into(),
            _cap: PhantomData,
        }
    }

    /// The extension name this source is bound to.
    #[must_use]
    pub fn extension(&self) -> &str {
        &self.extension
    }

    /// Wrap an underlying error into a [`CapabilityError`] tagged with
    /// this source's `(extension, capability)` pair.
    #[must_use]
    pub fn wrap<E>(&self, source: E) -> CapabilityError
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        CapabilityError::new(self.extension.clone(), C::NAME, source)
    }
}
