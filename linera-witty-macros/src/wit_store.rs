// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitStore` trait.

use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{Fields, Ident, Index, Type};

/// Returns the body of the `WitStore` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct(fields: &Fields) -> TokenStream {
    let field_names = field_names(fields);
    let field_bindings = field_bindings(fields);
    let field_types = fields.iter().map(|field| &field.ty);
    let field_pairs = field_bindings.clone().zip(field_types);

    let store_fields = field_pairs.map(store_field);

    let lower_fields = field_names.clone().map(|field_name| {
        quote! {
            let field_layout = WitStore::lower(&self.#field_name, memory)?;
            let flat_layout = flat_layout + field_layout;
        }
    });

    let construction = match fields {
        Fields::Unit => quote! {},
        Fields::Named(_) => quote! { { #( #field_bindings ),* } },
        Fields::Unnamed(_) => quote! { ( #( #field_bindings ),* ) },
    };

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
            let Self #construction = self;

            #( #store_fields )*

            Ok(())
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
            let flat_layout = linera_witty::HList![];

            #( #lower_fields )*

            Ok(flat_layout)
        }
    }
}

/// Returns an iterator over the names of the provided `fields`.
fn field_names(fields: &Fields) -> impl Iterator<Item = TokenStream> + Clone + '_ {
    fields.iter().enumerate().map(|(index, field)| {
        field
            .ident
            .as_ref()
            .map(ToTokens::to_token_stream)
            .unwrap_or_else(|| Index::from(index).to_token_stream())
    })
}

/// Returns an iterator over names for bindings used to deconstruct the provided `fields`.
fn field_bindings(fields: &Fields) -> impl Iterator<Item = Ident> + Clone + '_ {
    fields.iter().enumerate().map(|(index, field)| {
        field
            .ident
            .as_ref()
            .cloned()
            .unwrap_or_else(|| format_ident!("field{index}"))
    })
}

/// Returns the code to store a field.
fn store_field((field_name, field_type): (Ident, &Type)) -> TokenStream {
    quote! {
        location = location.after_padding_for::<#field_type>();
        WitStore::store(#field_name, memory, location)?;
        location = location.after::<#field_type>();
    }
}
