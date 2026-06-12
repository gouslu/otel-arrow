// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Errors for the azure_identity_auth extension.

use thiserror::Error;

use super::config::AuthMethod;

/// Errors produced by the azure_identity_auth extension.
#[derive(Debug, Error)]
pub enum Error {
    /// User-supplied configuration is invalid.
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        /// Human-readable explanation.
        message: String,
    },

    /// Failed to construct the Azure credential.
    #[error("failed to create {method} credential: {source}")]
    CreateCredential {
        /// Authentication method that failed to construct.
        method: AuthMethod,
        /// Underlying error from `azure_identity`.
        #[source]
        source: azure_core::Error,
    },

    /// The configured credential failed to acquire a token.
    #[error("token acquisition failed: {source}")]
    TokenAcquisition {
        /// Underlying error from `azure_core`.
        #[source]
        source: azure_core::Error,
    },
}
