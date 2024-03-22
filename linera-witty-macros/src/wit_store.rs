// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitStore` trait.

use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::quote;
use syn::{Ident, LitInt, Variant};

use crate::util::FieldsInformation;

#[path = "unit_tests/wit_store.rs"]
mod tests;

/// Returns the body of the `WitStore` implementation for the Rust `struct` with the specified
/// `fields`.
pub fn derive_for_struct<'input>(fields: impl Into<FieldsInformation<'input>>) -> TokenStream {
    let fields = fields.into();
    let pattern = fields.destructuring();
    let fields_hlist_value = fields.hlist_value();

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

/// Returns the body of the `WitStore` implementation for the Rust `enum` with the specified
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

    let discriminant_type = if variant_count <= u8::MAX.into() {
        quote! { u8 }
    } else if variant_count <= u16::MAX.into() {
        quote! { u16 }
    } else if variant_count <= u32::MAX as usize {
        quote! { u32 }
    } else {
        abort!(name, "Too many variants in `enum`");
    };

    let variants = variants
        .zip(&variant_fields)
        .enumerate()
        .map(|(index, (variant, fields))| {
            let discriminant = LitInt::new(
                &format!("{index}_{discriminant_type}"),
                variant.ident.span(),
            );
            (discriminant, (variant, fields))
        });

    let align_to_cases = variant_fields.iter().fold(quote! {}, |location, variant| {
        let variant_type = variant.hlist_type();
        quote! {
            #location.after_padding_for::<#variant_type>()
        }
    });

    let store_variants = variants.clone().map(|(discriminant, (variant, fields))| {
        let variant_name = &variant.ident;
        let pattern = fields.destructuring();
        let fields_hlist_value = fields.hlist_value();

        quote! {
            #name::#variant_name #pattern => {
                #discriminant.store(memory, location)?;
                location = location.after::<#discriminant_type>() #align_to_cases;

                #fields_hlist_value.store(memory, location)
            }
        }
    });

    let lower_variants = variants.map(|(discriminant, (variant, fields))| {
        let variant_name = &variant.ident;
        let pattern = fields.destructuring();
        let fields_hlist_value = fields.hlist_value();

        quote! {
            #name::#variant_name #pattern => {
                let variant_flat_layout = #fields_hlist_value.lower(memory)?;

                let flat_layout: <Self::Layout as linera_witty::Layout>::Flat =
                    linera_witty::JoinFlatLayouts::into_joined(
                        #discriminant.lower(memory)? + variant_flat_layout,
                    );

                Ok(flat_layout)
            }
        }
    });

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
            match self {
                #( #store_variants )*
            }
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
            match self {
                #( #lower_variants )*
            }
        }
    }
}
