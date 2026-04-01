// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Proc macros for the async pipeline engine
//!
//! This crate provides procedural macros that help generate boilerplate code
//! for factory registries, distributed slices, and capability infrastructure
//! in the pipeline engine.

use proc_macro::TokenStream;

mod capability;
mod pipeline_factory;

/// Attribute macro to generate distributed slices and initialize a factory registry.
///
/// This macro generates distributed slices for factories and initializes the annotated
/// XYZ_FACTORY_PIPELINE static variable. It requires a prefix parameter to avoid name
/// conflicts when used multiple times in the same scope.
///
/// # Usage
///
/// ```rust,ignore
/// use otap_df_engine::{PipelineFactory, build_factory};
/// use otap_df_engine_macros::pipeline_factory;
///
/// #[pipeline_factory(MY_PREFIX, MyData)]
/// static XYZ_FACTORY_PIPELINE: PipelineFactory<MyData> = build_factory();
/// ```
#[proc_macro_attribute]
pub fn pipeline_factory(args: TokenStream, input: TokenStream) -> TokenStream {
    pipeline_factory::expand(args, input)
}

/// Attribute macro that generates the full capability infrastructure from a
/// single trait definition.
///
/// Given a trait with async and/or sync methods, this generates:
///
/// 1. `pub mod local` — `#[async_trait(?Send)]` variant of the trait (only if async methods exist)
/// 2. `pub mod shared` — `#[async_trait]` + `Send` variant of the trait (only if async methods exist)
/// 3. `SharedAsLocal` adapter — wraps shared impl for local consumers
/// 4. Zero-sized registration struct — namespace for registry helper methods
/// 5. Sealed trait impls — compile-time enforcement that only engine-defined capabilities are accepted
/// 6. Registration plumbing — sealing, `KNOWN_CAPABILITIES` link-time entry,
///    and type-erased coercion functions for the capability registry
///
/// `#[async_trait]` is only emitted when the trait contains async methods.
///
/// # Example
///
/// ```rust,ignore
/// use otap_df_engine_macros::capability;
///
/// #[capability(
///     name = "key_value_store",
///     description = "Provides key-value storage for pipeline components",
/// )]
/// pub trait KeyValueStore {
///     async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error>;
///     async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), Error>;
///     async fn delete(&self, key: &str) -> Result<(), Error>;
/// }
/// ```
#[proc_macro_attribute]
pub fn capability(attr: TokenStream, item: TokenStream) -> TokenStream {
    capability::expand(attr, item)
}
