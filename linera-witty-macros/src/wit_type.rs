// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitType` trait.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Fields, Type};

/// Returns the body of the `WitType` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct(fields: &Fields) -> TokenStream {
    let field_types = fields.iter().map(|field| &field.ty);
    let size = struct_size_calculation(field_types.clone(), &quote! { 0 });
    let layout = struct_layout_type(field_types);

    quote! {
        const SIZE: u32 = #size;

        type Layout = #layout;
    }
}

/// Returns an expression that calculates the size in memory of the sequence of `field_types`.
fn struct_size_calculation<'fields>(
    field_types: impl Iterator<Item = &'fields Type>,
    prefix_size: &TokenStream,
) -> TokenStream {
    let field_size_calculations = field_types.map(|field_type| {
        quote! {
            let field_alignment =
                <<#field_type as linera_witty::WitType>::Layout as linera_witty::Layout>::ALIGNMENT;
            let field_size = <#field_type as linera_witty::WitType>::SIZE;
            let padding = (-(size as i32) & (field_alignment as i32 - 1)) as u32;

            size += padding;
            size += field_size;
        }
    });

    quote! {{
        let mut size = #prefix_size;
        #(#field_size_calculations)*
        size
    }}
}

/// Returns the layout type for the sequence of `field_types`.
fn struct_layout_type<'fields>(field_types: impl Iterator<Item = &'fields Type>) -> TokenStream {
    field_types.fold(quote! { linera_witty::HNil }, |current, field_type| {
        quote! {
            <#current as std::ops::Add<<#field_type as linera_witty::WitType>::Layout>>::Output
        }
    })
}
