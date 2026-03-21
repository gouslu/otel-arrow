// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! `#[capability]` proc macro implementation.
//!
//! Generates the full local/shared capability infrastructure from a single
//! trait definition. See the `#[capability]` doc comment in `lib.rs` for usage.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Ident, Token,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// Arguments for the `#[capability]` attribute macro.
pub(crate) struct CapabilityArgs {
    pub name: String,
    pub description: String,
}

impl Parse for CapabilityArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut name = None;
        let mut description = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            let _: Token![=] = input.parse()?;
            let value: syn::LitStr = input.parse()?;
            match key.to_string().as_str() {
                "name" => name = Some(value.value()),
                "description" => description = Some(value.value()),
                _ => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown attribute `{key}`"),
                    ));
                }
            }
            if input.peek(Token![,]) {
                let _: Token![,] = input.parse()?;
            }
        }
        Ok(Self {
            name: name.ok_or_else(|| input.error("missing `name` attribute"))?,
            description: description
                .ok_or_else(|| input.error("missing `description` attribute"))?,
        })
    }
}

/// Entry point for `#[capability(name = "...", description = "...")]`.
pub fn expand(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as CapabilityArgs);
    let trait_def = parse_macro_input!(item as syn::ItemTrait);
    match generate(args, trait_def) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate(
    args: CapabilityArgs,
    trait_def: syn::ItemTrait,
) -> syn::Result<proc_macro2::TokenStream> {
    let vis = &trait_def.vis;
    let trait_name = &trait_def.ident;
    let cap_name = syn::LitStr::new(&args.name, trait_def.ident.span());
    let cap_desc = syn::LitStr::new(&args.description, trait_def.ident.span());

    // Collect trait methods.
    let methods: Vec<&syn::TraitItemFn> = trait_def
        .items
        .iter()
        .filter_map(|item| match item {
            syn::TraitItem::Fn(f) => Some(f),
            _ => None,
        })
        .collect();

    // Detect whether any method is async — only emit #[async_trait] when needed.
    let has_async = methods.iter().any(|m| m.sig.asyncness.is_some());

    // Method signatures for the local/shared trait definitions (sig + semicolon).
    let trait_method_sigs: Vec<_> = methods
        .iter()
        .map(|m| {
            let sig = &m.sig;
            quote! { #sig; }
        })
        .collect();

    // Adapter delegation: each method delegates to self.0, with .await for async.
    let adapter_methods: Vec<_> = methods
        .iter()
        .map(|m| {
            let sig = &m.sig;
            let method_name = &sig.ident;
            let is_async = sig.asyncness.is_some();
            let param_names = extract_param_names(sig);
            let body = if is_async {
                quote! { self.0.#method_name(#(#param_names),*).await }
            } else {
                quote! { self.0.#method_name(#(#param_names),*) }
            };
            quote! { #sig { #body } }
        })
        .collect();

    // Dispatch methods on the handle enum.
    let dispatch_methods: Vec<_> = methods
        .iter()
        .map(|m| {
            let sig = &m.sig;
            let method_name = &sig.ident;
            let is_async = sig.asyncness.is_some();
            let param_names = extract_param_names(sig);
            // Preserve doc attributes from the original trait method.
            let doc_attrs: Vec<_> = m
                .attrs
                .iter()
                .filter(|a| a.path().is_ident("doc"))
                .collect();
            let (local_call, shared_call) = if is_async {
                (
                    quote! { inner.#method_name(#(#param_names),*).await },
                    quote! { inner.#method_name(#(#param_names),*).await },
                )
            } else {
                (
                    quote! { inner.#method_name(#(#param_names),*) },
                    quote! { inner.#method_name(#(#param_names),*) },
                )
            };
            quote! {
                #(#doc_attrs)*
                pub #sig {
                    match self {
                        Self::Local(inner) => #local_call,
                        Self::Shared(inner) => #shared_call,
                    }
                }
            }
        })
        .collect();

    // Preserve doc attributes from the original trait for the handle enum.
    let doc_attrs: Vec<_> = trait_def
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("doc"))
        .collect();

    // Only emit #[async_trait] attributes when the trait has async methods.
    let local_async_attr = if has_async {
        quote! { #[async_trait::async_trait(?Send)] }
    } else {
        quote! {}
    };
    let shared_async_attr = if has_async {
        quote! { #[async_trait::async_trait] }
    } else {
        quote! {}
    };

    Ok(quote! {
        // ── Local trait (!Send) ─────────────────────────────────────────
        #[doc(hidden)]
        pub mod local {
            use super::*;

            #local_async_attr
            pub trait #trait_name {
                #(#trait_method_sigs)*
            }
        }

        // ── Shared trait (Send) ─────────────────────────────────────────
        #[doc(hidden)]
        pub mod shared {
            use super::*;

            #shared_async_attr
            pub trait #trait_name: Send {
                #(#trait_method_sigs)*
            }
        }

        // ── SharedAsLocal adapter ───────────────────────────────────────
        struct SharedAsLocal(Box<dyn shared::#trait_name>);

        #local_async_attr
        impl local::#trait_name for SharedAsLocal {
            #(#adapter_methods)*
        }

        // ── Handle enum ─────────────────────────────────────────────────
        #(#doc_attrs)*
        #vis enum #trait_name {
            /// Rc-based variant for local consumers.
            Local(std::rc::Rc<dyn local::#trait_name>),
            /// Box-based variant for shared consumers.
            Shared(Box<dyn shared::#trait_name>),
        }

        impl #trait_name {
            #(#dispatch_methods)*
        }

        // ── CapabilityHandle impl ───────────────────────────────────────
        impl crate::capability::registry::CapabilityHandle for #trait_name {
            const CAPABILITY_NAME: &'static str =
                <Self as crate::capability::registry::ExtensionCapability>::NAME;

            type Local = dyn local::#trait_name;
            type Shared = dyn shared::#trait_name;

            fn from_local(local: std::rc::Rc<dyn local::#trait_name>) -> Self {
                Self::Local(local)
            }

            fn from_shared(
                shared: Box<<Self as crate::capability::registry::CapabilityHandle>::Shared>,
            ) -> Self {
                Self::Shared(shared)
            }

            fn adapt_shared_as_local(
                shared: Box<<Self as crate::capability::registry::CapabilityHandle>::Shared>,
            ) -> std::rc::Rc<<Self as crate::capability::registry::CapabilityHandle>::Local> {
                std::rc::Rc::new(SharedAsLocal(shared))
            }
        }

        // ── Capability registration (sealed traits, link-time, glue) ────
        crate::register_capability!(
            #trait_name,
            local::#trait_name,
            shared::#trait_name,
            #cap_name,
            #cap_desc,
        );
    })
}

/// Extract parameter names (excluding `&self`) from a method signature.
fn extract_param_names(sig: &syn::Signature) -> Vec<&Ident> {
    sig.inputs
        .iter()
        .filter_map(|arg| match arg {
            syn::FnArg::Typed(pat) => {
                if let syn::Pat::Ident(ident) = &*pat.pat {
                    Some(&ident.ident)
                } else {
                    None
                }
            }
            syn::FnArg::Receiver(_) => None,
        })
        .collect()
}
