// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitLoad` trait.

#[path = "unit_tests/wit_load.rs"]
mod tests;

use proc_macro2::TokenStream;
use quote::{format_ident, quote, ToTokens};
use syn::{Fields, Ident};

/// Returns the body of the `WitLoad` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct(fields: &Fields) -> TokenStream {
    let field_pairs: Vec<_> = field_names_and_types(fields).collect();

    let load_fields = loads_for_fields(field_pairs.iter().cloned());
    let construction = construction_for_fields(field_pairs.iter().cloned(), fields);

    let lift_fields = field_pairs.iter().map(|(field_name, field_type)| {
        quote! {
            let (field_layout, flat_layout) = linera_witty::Split::split(flat_layout);
            let #field_name = <#field_type as WitLoad>::lift_from(field_layout, memory)?;
        }
    });

    quote! {
        fn load<Instance>(
            memory: &linera_witty::Memory<'_, Instance>,
            mut location: linera_witty::GuestPointer,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            #( #load_fields )*

            Ok(Self #construction)
        }

        fn lift_from<Instance>(
            flat_layout: <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            #( #lift_fields )*

            Ok(Self #construction)
        }
    }
}

/// Returns an iterator over the names and types of the provided `fields`.
fn field_names_and_types(
    fields: &Fields,
) -> impl Iterator<Item = (Ident, TokenStream)> + Clone + '_ {
    let field_names = fields.iter().enumerate().map(|(index, field)| {
        field
            .ident
            .as_ref()
            .cloned()
            .unwrap_or_else(|| format_ident!("field{index}"))
    });

    let field_types = fields.iter().map(|field| field.ty.to_token_stream());

    field_names.zip(field_types)
}

/// Returns the code generated to load a single field.
///
/// Assumes that `location` points to where the field starts in memory, and advances it to the end
/// of the field. A binding with the field name is created in the generated code.
fn loads_for_fields(
    field_names_and_types: impl Iterator<Item = (Ident, TokenStream)> + Clone,
) -> impl Iterator<Item = TokenStream> {
    field_names_and_types.map(|(field_name, field_type)| {
        quote! {
            location = location.after_padding_for::<#field_type>();
            let #field_name = <#field_type as linera_witty::WitLoad>::load(memory, location)?;
            location = location.after::<#field_type>();
        }
    })
}

/// Returns the code to construct an instance of the field type.
///
/// Assumes that bindings were created with the field names.
fn construction_for_fields(
    field_names_and_types: impl Iterator<Item = (Ident, TokenStream)>,
    fields: &Fields,
) -> TokenStream {
    let field_names = field_names_and_types.map(|(name, _)| name);

    match fields {
        Fields::Unit => quote! {},
        Fields::Named(_) => quote! { { #( #field_names ),* } },
        Fields::Unnamed(_) => quote! {( #( #field_names ),* ) },
    }
}
