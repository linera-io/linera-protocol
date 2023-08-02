// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitType` trait.

use crate::util::hlist_type_for;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Fields;

#[path = "unit_tests/wit_type.rs"]
mod tests;

/// Returns the body of the `WitType` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct(fields: &Fields) -> TokenStream {
    let fields_hlist = hlist_type_for(fields);

    quote! {
        const SIZE: u32 = <#fields_hlist as linera_witty::WitType>::SIZE;

        type Layout = <#fields_hlist as linera_witty::WitType>::Layout;
    }
}
