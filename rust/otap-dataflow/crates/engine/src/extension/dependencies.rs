// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Variant-scoped capability dependencies passed to extension factories.

use crate::capability::{ExtensionCapability, registry};

/// Capability dependencies available to an extension's physical variants.
///
/// The local and shared scopes are resolved independently so each physical
/// implementation can claim the same configured binding once. Construction
/// returns opaque variant-bound values that only the matching extension
/// builder method can consume, preventing a dual bundle from attributing a
/// dependency to one variant while storing it in the other.
#[derive(Debug)]
pub struct ExtensionDependencies {
    local: LocalDependencies,
    shared: SharedDependencies,
}

impl ExtensionDependencies {
    pub(crate) fn new(local: registry::Capabilities, shared: registry::Capabilities) -> Self {
        Self {
            local: LocalDependencies {
                capabilities: local,
            },
            shared: SharedDependencies {
                capabilities: shared,
            },
        }
    }

    /// Creates empty dependency scopes for direct factory tests.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(
            registry::Capabilities::empty(),
            registry::Capabilities::empty(),
        )
    }

    /// Constructs a local implementation with access to its local dependency scope.
    pub fn bind_local<E>(
        &self,
        build: impl FnOnce(&LocalDependencies) -> Result<E, registry::Error>,
    ) -> Result<LocalExtensionDependency<E>, registry::Error> {
        build(&self.local).map(LocalExtensionDependency)
    }

    /// Constructs a shared implementation with access to its shared dependency scope.
    pub fn bind_shared<E>(
        &self,
        build: impl FnOnce(&SharedDependencies) -> Result<E, registry::Error>,
    ) -> Result<SharedExtensionDependency<E>, registry::Error> {
        build(&self.shared).map(SharedExtensionDependency)
    }
}

/// A local extension implementation bundled with its dependency ownership.
///
/// The implementation is intentionally opaque and can only be consumed by a
/// matching `local_with_dependencies` extension builder method.
#[doc(hidden)]
pub struct LocalExtensionDependency<E>(E);

impl<E> LocalExtensionDependency<E> {
    pub(crate) fn into_inner(self) -> E {
        self.0
    }
}

/// A shared extension implementation bundled with its dependency ownership.
///
/// The implementation is intentionally opaque and can only be consumed by a
/// matching `shared_with_dependencies` extension builder method.
#[doc(hidden)]
pub struct SharedExtensionDependency<E>(E);

impl<E> SharedExtensionDependency<E> {
    pub(crate) fn into_inner(self) -> E {
        self.0
    }
}

/// Capability dependencies available to a local extension implementation.
#[derive(Debug)]
pub struct LocalDependencies {
    capabilities: registry::Capabilities,
}

impl LocalDependencies {
    /// Acquires a required local capability.
    ///
    /// A shared-only provider is adapted through its `SharedAsLocal`
    /// implementation when the capability supports that fallback.
    pub fn require<C: ExtensionCapability>(&self) -> Result<Box<C::Local>, registry::Error> {
        self.capabilities.require_local::<C>()
    }

    /// Acquires an optional local capability.
    pub fn optional<C: ExtensionCapability>(
        &self,
    ) -> Result<Option<Box<C::Local>>, registry::Error> {
        self.capabilities.optional_local::<C>()
    }
}

/// Capability dependencies available to a shared extension implementation.
#[derive(Debug)]
pub struct SharedDependencies {
    capabilities: registry::Capabilities,
}

impl SharedDependencies {
    /// Acquires a required shared capability.
    pub fn require<C: ExtensionCapability>(&self) -> Result<Box<C::Shared>, registry::Error> {
        self.capabilities.require_shared::<C>()
    }

    /// Acquires an optional shared capability.
    pub fn optional<C: ExtensionCapability>(
        &self,
    ) -> Result<Option<Box<C::Shared>>, registry::Error> {
        self.capabilities.optional_shared::<C>()
    }
}
