// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitStore` trait.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Fields, Ident};

#[path = "unit_tests/wit_store.rs"]
mod tests;

/// Returns the body of the `WitStore` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct(fields: &Fields) -> TokenStream {
    let field_names = field_names(fields);
    let pattern = fields_pattern(fields, field_names.clone());
    let fields_hlist_value = quote! { linera_witty::hlist![#( #field_names ),*] };

    quote! {
        fn store<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<(), linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let Self #pattern = self;

            #fields_hlist_value.store(memory, location)
        }

        fn lower<Instance>(
            &self,
            memory: &mut linera_witty::Memory<'_, Instance>,
        ) -> Result<<Self::Layout as linera_witty::Layout>::Flat, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let Self #pattern = self;

            #fields_hlist_value.lower(memory)
        }
    }
}

/// Returns the code with a pattern to match the `fields` using the provided `bindings`.
fn fields_pattern(fields: &Fields, bindings: impl Iterator<Item = Ident>) -> TokenStream {
    match fields {
        Fields::Unit => quote! {},
        Fields::Named(_) => quote! { { #( #bindings ),* } },
        Fields::Unnamed(_) => quote! { ( #( #bindings ),* ) },
    }
}

/// Returns an iterator over names for bindings used to deconstruct the provided `fields`.
fn field_names(fields: &Fields) -> impl Iterator<Item = Ident> + Clone + '_ {
    fields.iter().enumerate().map(|(index, field)| {
        field
            .ident
            .as_ref()
            .cloned()
            .unwrap_or_else(|| format_ident!("field{index}"))
    })
}
