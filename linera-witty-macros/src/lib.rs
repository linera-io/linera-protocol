// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Linera Witty Macros
//!
//! This crate contains the procedural macros used by the `linera-witty` crate.

#![deny(missing_docs)]

mod util;
mod wit_load;
mod wit_store;
mod wit_type;

use proc_macro::TokenStream;
use proc_macro2::Span;
use proc_macro_error::{abort, proc_macro_error};
use quote::{quote, ToTokens};
use syn::{parse_macro_input, Data, DeriveInput, Ident};

/// Derives `WitType` for a Rust type.
///
/// All fields in the type must also implement `WitType`.
#[proc_macro_error]
#[proc_macro_derive(WitType)]
pub fn derive_wit_type(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let body = match &input.data {
        Data::Struct(struct_item) => wit_type::derive_for_struct(&struct_item.fields),
        Data::Enum(enum_item) => wit_type::derive_for_enum(&input.ident, enum_item.variants.iter()),
        Data::Union(_union_item) => {
            abort!(input.ident, "Can't derive `WitType` for `union`s")
        }
    };

    derive_trait(input, body, Ident::new("WitType", Span::call_site()))
}

/// Derives `WitLoad` for the Rust type.
///
/// All fields in the type must also implement `WitLoad`.
#[proc_macro_error]
#[proc_macro_derive(WitLoad)]
pub fn derive_wit_load(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let body = match &input.data {
        Data::Struct(struct_item) => wit_load::derive_for_struct(&struct_item.fields),
        Data::Enum(enum_item) => wit_load::derive_for_enum(&input.ident, enum_item.variants.iter()),
        Data::Union(_union_item) => {
            abort!(input.ident, "Can't derive `WitLoad` for `union`s")
        }
    };

    derive_trait(input, body, Ident::new("WitLoad", Span::call_site()))
}

/// Derives `WitStore` for the Rust type.
///
/// All fields in the type must also implement `WitStore`.
#[proc_macro_error]
#[proc_macro_derive(WitStore)]
pub fn derive_wit_store(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let body = match &input.data {
        Data::Struct(struct_item) => wit_store::derive_for_struct(&struct_item.fields),
        Data::Enum(enum_item) => {
            wit_store::derive_for_enum(&input.ident, enum_item.variants.iter())
        }
        Data::Union(_union_item) => {
            abort!(input.ident, "Can't derive `WitStore` for `union`s")
        }
    };

    derive_trait(input, body, Ident::new("WitStore", Span::call_site()))
}

/// Derives a trait named `trait_name` with the specified `body`.
///
/// Contains the common code to extract and apply the type's generics for the trait implementation.
fn derive_trait(input: DeriveInput, body: impl ToTokens, trait_name: Ident) -> TokenStream {
    let (generic_parameters, type_generics, where_clause) = input.generics.split_for_impl();
    let type_name = &input.ident;

    quote! {
        impl #generic_parameters #trait_name for #type_name #type_generics #where_clause {
            #body
        }
    }
    .into()
}
