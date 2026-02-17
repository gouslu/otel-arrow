// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier

use std::{any::TypeId, collections::HashMap};

use crate::extensions::{CastFn, Extension};

/// A wrapper for an extension instance, along with its associated cast functions for registered traits.
pub struct ExtensionWrapper {
    /// The extension instance.
    pub extension: Box<dyn Extension>,

    /// One cast function per registered trait, keyed by `TypeId::of::<dyn TraitId<dyn Trait>>()`.
    pub casters: HashMap<TypeId, CastFn>,
}