// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper functions shared between different macro implementations.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Fields, Ident};

/// Returns the code with a pattern to match a heterogenous list using the `field_names` as
/// bindings.
///
/// This function receives `field_names` instead of a `Fields` instance because some fields might
/// not have names, so binding names must be created for them.
pub fn hlist_bindings_for(field_names: impl Iterator<Item = Ident>) -> TokenStream {
    quote! { linera_witty::hlist_pat![#( #field_names ),*] }
}

/// Returns the code with a pattern to match a heterogenous list using the `field_names` as
/// bindings.
pub fn hlist_type_for(fields: &Fields) -> TokenStream {
    let field_types = fields.iter().map(|field| &field.ty);
    quote! { linera_witty::HList![#( #field_types ),*] }
}
