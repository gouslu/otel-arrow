// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier

// Allow unsafe code in this module for fat pointer transmutation.
// The safety invariants are documented and upheld by the implementation.
#![allow(unsafe_code)]

use std::{any::{Any, TypeId}, collections::HashMap};

use crate::{extension::ExtensionWrapper, extensions::Extension};

pub trait TraitId<T: ?Sized> {}

/// A type alias for a function that casts a `&dyn Extension` to an `Option<[usize; 2]>`.
pub type CastFn = fn(&dyn Extension) -> Option<[usize; 2]>;

pub struct ExtensionRegistry {
    extensions: HashMap<String, ExtensionWrapper>,
}

impl ExtensionRegistry {
    pub fn new() -> Self {
        Self {
            extensions: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: String, extension: ExtensionWrapper) {
        _ = self.extensions.insert(name, extension);
    }

    pub fn get_extension<T: ?Sized + 'static>(&self, name: &str) -> Option<&ExtensionWrapper> {
        let w = self.extensions.get(name)?;
        let cast = w.casters.get(&TypeId::of::<dyn TraitId<T>>())?;
        let fat = cast(w.extension.as_ref())?;
        // SAFETY: `fat` was produced by `trait_ref_to_raw::<T>` from a valid
        // `&T` derived from the boxed instance. The box is alive for `&self`.
        Some(unsafe { raw_to_trait_ref(fat) })
    }
}

/// Reconstruct a `&dyn Trait` from a `[usize; 2]` fat pointer.
///
/// # Safety
/// The caller must ensure `fat` was produced by `trait_ref_to_raw` with the
/// same `Trait` type, and that the underlying data is still alive.
#[inline]
pub unsafe fn raw_to_trait_ref<'a, T: ?Sized + 'a>(fat: [usize; 2]) -> &'a T {
    std::mem::transmute_copy(&fat)
}

/// Convert a `&dyn Trait` fat pointer into `[usize; 2]` for storage.
///
/// # Safety
/// Relies on the standard Rust fat-pointer layout: `[data_ptr, vtable_ptr]`.
#[inline]
pub unsafe fn trait_ref_to_raw<T: ?Sized>(r: &T) -> [usize; 2] {
    std::mem::transmute_copy(&r)
}

/// A helper macro to create a caster map for an extension that implements multiple traits.
#[macro_export]
macro_rules! extension_bundle {
    ($concrete_ty:ty => $($trait:ident),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut casters: std::collections::HashMap<std::any::TypeId, $crate::extensions::CastFn>
            = std::collections::HashMap::new();
        $(
            {
                // Inner fn is monomorphic — $concrete_ty is substituted by the macro,
                // so there are no captures and this coerces to a fn pointer.
                #[allow(unsafe_code)]
                fn __cast(any: &dyn $crate::extensions::Extension) -> Option<[usize; 2]> {
                    let concrete = any.downcast_ref::<$concrete_ty>()?;
                    let trait_ref: &dyn $trait = concrete;
                    // SAFETY: trait_ref_to_raw converts a valid trait reference to raw parts.
                    Some(unsafe { $crate::extensions::trait_ref_to_raw(trait_ref) })
                }
                casters.insert(
                    std::any::TypeId::of::<dyn $crate::extensions::TraitId<dyn $trait>>(),
                    __cast as $crate::extensions::CastFn,
                );
            }
        )*
        casters
    }};
}
