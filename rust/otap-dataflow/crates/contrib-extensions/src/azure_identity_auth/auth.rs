// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Azure credential wrapper.
//!
//! Ported from `azure_monitor_exporter::auth` without the metrics dependency.

use azure_core::credentials::{AccessToken, TokenCredential};
use azure_identity::{
    DeveloperToolsCredential, DeveloperToolsCredentialOptions, ManagedIdentityCredential,
    ManagedIdentityCredentialOptions, UserAssignedId,
};
use std::sync::Arc;

use super::config::{AuthMethod, Config};
use super::error::Error;

/// Holds an Azure `TokenCredential`, the scope it will request tokens for,
/// and a label describing which credential flow is in use.
#[derive(Clone)]
pub struct Auth {
    credential: Arc<dyn TokenCredential>,
    scope: String,
    credential_type: &'static str,
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Auth")
            .field("scope", &self.scope)
            .field("credential_type", &self.credential_type)
            .finish()
    }
}

impl Auth {
    /// Build an `Auth` from the supplied configuration.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let (credential, credential_type) = Self::create_credential(config)?;
        Ok(Self {
            credential,
            scope: config.scope.clone(),
            credential_type,
        })
    }

    /// Build an `Auth` from an explicit credential (used in tests).
    #[cfg(test)]
    pub fn from_credential(credential: Arc<dyn TokenCredential>, scope: String) -> Self {
        Self {
            credential,
            scope,
            credential_type: "test",
        }
    }

    /// Configured OAuth scope.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Human-readable label for the underlying credential flow.
    #[must_use]
    pub fn credential_type(&self) -> &'static str {
        self.credential_type
    }

    /// Single token acquisition attempt — no retry.
    ///
    /// Retry behavior, if any, is the caller's responsibility. The
    /// background refresh loop in `extension::start` reschedules on error
    /// via its outer `select!` timer; consumer slow paths surface the
    /// error directly to give the caller a chance to react.
    pub async fn get_token(&self) -> Result<AccessToken, Error> {
        self.credential
            .get_token(
                &[&self.scope],
                Some(azure_core::credentials::TokenRequestOptions::default()),
            )
            .await
            .map_err(|source| Error::TokenAcquisition { source })
    }

    fn create_credential(
        config: &Config,
    ) -> Result<(Arc<dyn TokenCredential>, &'static str), Error> {
        match config.method {
            AuthMethod::ManagedIdentity => {
                let mut options = ManagedIdentityCredentialOptions::default();
                let credential_type = if let Some(client_id) = &config.client_id {
                    options.user_assigned_id = Some(UserAssignedId::ClientId(client_id.clone()));
                    "user_assigned_managed_identity"
                } else {
                    "system_assigned_managed_identity"
                };
                let credential =
                    ManagedIdentityCredential::new(Some(options)).map_err(|source| {
                        Error::CreateCredential {
                            method: AuthMethod::ManagedIdentity,
                            source,
                        }
                    })?;
                Ok((credential, credential_type))
            }
            AuthMethod::Development => {
                let credential =
                    DeveloperToolsCredential::new(Some(DeveloperToolsCredentialOptions::default()))
                        .map_err(|source| Error::CreateCredential {
                            method: AuthMethod::Development,
                            source,
                        })?;
                Ok((credential, "developer_tools"))
            }
        }
    }
}
