// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitLoad` trait.

#[path = "unit_tests/wit_load.rs"]
mod tests;

use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::quote;
use syn::{Ident, LitInt, Variant};

use crate::util::FieldsInformation;

/// Returns the body of the `WitLoad` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct<'input>(fields: impl Into<FieldsInformation<'input>>) -> TokenStream {
    let fields = fields.into();
    let fields_hlist_binding = fields.hlist_bindings();
    let fields_hlist_type = fields.hlist_type();
    let construction = fields.construction();
    let fallback_bindings = fields.bindings_for_skipped_fields();

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

            #fallback_bindings

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

            #fallback_bindings

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
    let variant_fields = variants
        .clone()
        .map(|variant| FieldsInformation::from(&variant.fields))
        .collect::<Vec<_>>();
    let variant_types = variant_fields.iter().map(FieldsInformation::hlist_type);
    let variants = variants.zip(&variant_fields).enumerate();

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

    let load_variants = variants.clone().map(|(index, (variant, fields))| {
        let variant_name = &variant.ident;
        let index = LitInt::new(&index.to_string(), variant_name.span());
        let field_bindings = fields.hlist_bindings();
        let fields_type = fields.hlist_type();
        let construction = fields.construction();
        let fallback_bindings = fields.bindings_for_skipped_fields();

        quote! {
            #index => {
                let #field_bindings =
                    <#fields_type as linera_witty::WitLoad>::load(memory, location)?;

                #fallback_bindings

                Ok(#name::#variant_name #construction)
            }
        }
    });

    let lift_variants = variants.map(|(index, (variant, fields))| {
        let variant_name = &variant.ident;
        let index = LitInt::new(&index.to_string(), variant_name.span());
        let field_bindings = fields.hlist_bindings();
        let fields_type = fields.hlist_type();
        let construction = fields.construction();
        let fallback_bindings = fields.bindings_for_skipped_fields();

        quote! {
            #index => {
                let #field_bindings = <#fields_type as linera_witty::WitLoad>::lift_from(
                    linera_witty::JoinFlatLayouts::from_joined(flat_layout),
                    memory,
                )?;

                #fallback_bindings

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
                discriminant => Err(linera_witty::RuntimeError::InvalidVariant {
                    type_name: ::std::any::type_name::<Self>(),
                    discriminant: discriminant.into(),
                }),
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
                discriminant => Err(linera_witty::RuntimeError::InvalidVariant {
                    type_name: ::std::any::type_name::<Self>(),
                    discriminant: discriminant.into(),
                }),
            }
        }
    }
}
