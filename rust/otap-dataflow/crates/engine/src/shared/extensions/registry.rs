// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared extension registry (`Send + Sync`, stored as `Arc<dyn Trait>`).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use crate::extensions::ExtensionError;
use super::SharedExtensionTrait;

/// Registry for shared extension trait implementations.
///
/// `Send + Sync`, `Clone`. Contains only `Arc<dyn Trait>` entries.
///
/// Passed to **shared components** (those using `#[async_trait]` with Send futures).
/// Shared components can only access shared (thread-safe) extension traits.
///
/// Each `get` call returns a shared `Arc<dyn Trait>` — no deep copies. All nodes
/// on the same pipeline thread share the same extension instance.
#[derive(Default, Clone)]
pub struct SharedExtensionRegistry {
    /// (extension_name, TypeId of Arc<dyn Trait>) → Arc<Arc<dyn Trait>> erased as Arc<dyn Any + Send + Sync>
    pub(crate) handles: HashMap<(String, TypeId), Arc<dyn Any + Send + Sync>>,
}

impl SharedExtensionRegistry {
    /// Create a new empty shared registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
        }
    }

    /// Register an `Arc<dyn Trait>` for a named extension.
    ///
    /// The trait type is identified by `TypeId::of::<Arc<T>>()` so that
    /// `get::<dyn Trait>()` can look it up.
    pub fn register<T: ?Sized + SharedExtensionTrait + 'static>(&mut self, name: &str, arc: Arc<T>) {
        let _ = self.handles.insert(
            (name.to_string(), TypeId::of::<Arc<T>>()),
            Arc::new(arc),
        );
    }

    /// Get a shared trait reference by extension name.
    ///
    /// Returns `Arc<dyn Trait>` — same instance shared by all consumers.
    ///
    /// # Errors
    ///
    /// Returns `ExtensionError::NotFound` if no extension with that name exists.
    /// Returns `ExtensionError::TraitNotImplemented` if the extension doesn't expose that trait.
    pub fn get<T: ?Sized + SharedExtensionTrait + 'static>(
        &self,
        name: &str,
    ) -> Result<Arc<T>, ExtensionError> {
        let key = (name.to_string(), TypeId::of::<Arc<T>>());
        let erased = self.handles.get(&key).ok_or_else(|| {
            let has_any = self.handles.keys().any(|(n, _)| n == name);
            if has_any {
                ExtensionError::TraitNotImplemented {
                    name: name.to_string(),
                    expected: std::any::type_name::<T>(),
                }
            } else {
                ExtensionError::NotFound {
                    name: name.to_string(),
                }
            }
        })?;

        let arc = erased
            .downcast_ref::<Arc<T>>()
            .expect("TypeId matched but downcast failed — this is a bug");

        Ok(Arc::clone(arc))
    }

    /// Check if an extension exists by name.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.handles.keys().any(|(n, _)| n == name)
    }

    /// Returns the number of registered extensions (unique names).
    #[must_use]
    pub fn len(&self) -> usize {
        let mut names: Vec<&String> = self.handles.keys().map(|(n, _)| n).collect();
        names.sort();
        names.dedup();
        names.len()
    }

    /// Returns true if no extensions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// Returns an iterator over unique extension names.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        let mut names: Vec<&String> = self.handles.keys().map(|(n, _)| n).collect();
        names.sort();
        names.dedup();
        names.into_iter()
    }
}

impl std::fmt::Debug for SharedExtensionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&String> = self.names().collect();
        f.debug_struct("SharedExtensionRegistry")
            .field("extensions", &names)
            .finish()
    }
}
