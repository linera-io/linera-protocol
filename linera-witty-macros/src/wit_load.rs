// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitLoad` trait.

#[path = "unit_tests/wit_load.rs"]
mod tests;

use crate::util::{hlist_bindings_for, hlist_type_for};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Fields, Ident};

/// Returns the body of the `WitLoad` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct(fields: &Fields) -> TokenStream {
    let field_names = field_names(fields);
    let fields_hlist_binding = hlist_bindings_for(field_names.clone());
    let fields_hlist_type = hlist_type_for(fields);
    let construction = construction_for_fields(field_names, fields);

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
            let #fields_hlist_binding =
                <#fields_hlist_type as linera_witty::WitLoad>::load(memory, location)?;

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
            let #fields_hlist_binding =
                <#fields_hlist_type as linera_witty::WitLoad>::lift_from(flat_layout, memory)?;

            Ok(Self #construction)
        }
    }
}

/// Returns an iterator over the names of the provided `fields`.
fn field_names(fields: &Fields) -> impl Iterator<Item = Ident> + Clone + '_ {
    fields.iter().enumerate().map(|(index, field)| {
        field
            .ident
            .as_ref()
            .cloned()
            .unwrap_or_else(|| format_ident!("field{index}"))
    })
}

/// Returns the code to construct an instance of the field type.
///
/// Assumes that bindings were created with the field names.
fn construction_for_fields(
    field_names: impl Iterator<Item = Ident>,
    fields: &Fields,
) -> TokenStream {
    match fields {
        Fields::Unit => quote! {},
        Fields::Named(_) => quote! { { #( #field_names ),* } },
        Fields::Unnamed(_) => quote! {( #( #field_names ),* ) },
    }
}
