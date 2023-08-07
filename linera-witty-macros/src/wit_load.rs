// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitLoad` trait.

#[path = "unit_tests/wit_load.rs"]
mod tests;

use crate::util::{hlist_bindings_for, hlist_type_for};
use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{format_ident, quote};
use syn::{Fields, Ident, LitInt, Variant};

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

/// Returns the body of the `WitLoad` implementation for the Rust `enum` with the specified
/// `variants`.
pub fn derive_for_enum<'variants>(
    name: &Ident,
    variants: impl DoubleEndedIterator<Item = &'variants Variant> + Clone,
) -> TokenStream {
    let variant_count = variants.clone().count();
    let variant_types = variants.clone().map(variant_type);
    let variants = variants.enumerate();

    let discriminant_type = if variant_count <= u8::MAX.into() {
        quote! { u8 }
    } else if variant_count <= u16::MAX.into() {
        quote! { u16 }
    } else if variant_count <= u32::MAX as usize {
        quote! { u32 }
    } else {
        abort!(name, "Too many variants in `enum`");
    };

    let align_to_cases = variant_types.fold(quote! {}, |location, variant_type| {
        quote! {
            #location.after_padding_for::<#variant_type>()
        }
    });

    let load_variants = variants.clone().map(|(index, variant)| {
        let variant_name = &variant.ident;
        let index = LitInt::new(&index.to_string(), variant_name.span());
        let field_names = field_names(&variant.fields);
        let field_bindings = hlist_bindings_for(field_names.clone());
        let fields_type = hlist_type_for(&variant.fields);
        let construction = construction_for_fields(field_names, &variant.fields);

        quote! {
            #index => {
                let #field_bindings =
                    <#fields_type as linera_witty::WitLoad>::load(memory, location)?;

                Ok(#name::#variant_name #construction)
            }
        }
    });

    let lift_variants = variants.map(|(index, variant)| {
        let variant_name = &variant.ident;
        let index = LitInt::new(&index.to_string(), variant_name.span());
        let field_names = field_names(&variant.fields);
        let field_bindings = hlist_bindings_for(field_names.clone());
        let fields_type = hlist_type_for(&variant.fields);
        let construction = construction_for_fields(field_names, &variant.fields);

        quote! {
            #index => {
                let #field_bindings = <#fields_type as linera_witty::WitLoad>::lift_from(
                    linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                    memory,
                )?;

                Ok(#name::#variant_name #construction)
            }
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
            let discriminant = <#discriminant_type as linera_witty::WitLoad>::load(
                memory,
                location,
            )?;
            location = location.after::<#discriminant_type>() #align_to_cases;

            match discriminant {
                #( #load_variants )*
                _ => Err(linera_witty::RuntimeError::InvalidVariant),
            }
        }

        fn lift_from<Instance>(
            linera_witty::hlist_pat![discriminant_flat_type, ...flat_layout]:
                <Self::Layout as linera_witty::Layout>::Flat,
            memory: &linera_witty::Memory<'_, Instance>,
        ) -> Result<Self, linera_witty::RuntimeError>
        where
            Instance: linera_witty::InstanceWithMemory,
            <Instance::Runtime as linera_witty::Runtime>::Memory:
                linera_witty::RuntimeMemory<Instance>,
        {
            let discriminant = <#discriminant_type as linera_witty::WitLoad>::lift_from(
                linera_witty::hlist![discriminant_flat_type],
                memory,
            )?;

            match discriminant {
                #( #lift_variants )*
                _ => Err(linera_witty::RuntimeError::InvalidVariant),
            }
        }
    }
}

/// Returns the expression for a heterogeneous list representing a variant's type.
fn variant_type(variant: &Variant) -> TokenStream {
    hlist_type_for(&variant.fields)
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
