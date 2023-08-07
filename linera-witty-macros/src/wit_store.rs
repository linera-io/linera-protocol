// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derivation of the `WitStore` trait.

use crate::util::hlist_type_for;
use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{format_ident, quote};
use syn::{Fields, Ident, LitInt, Variant};

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

/// Returns the body of the `WitStore` implementation for the Rust `enum` with the specified
/// `variants`.
pub fn derive_for_enum<'variants>(
    name: &Ident,
    variants: impl DoubleEndedIterator<Item = &'variants Variant> + Clone,
) -> TokenStream {
    let variant_count = variants.clone().count();

    let discriminant_type = if variant_count <= u8::MAX.into() {
        quote! { u8 }
    } else if variant_count <= u16::MAX.into() {
        quote! { u16 }
    } else if variant_count <= u32::MAX as usize {
        quote! { u32 }
    } else {
        abort!(name, "Too many variants in `enum`");
    };

    let align_to_cases = variants
        .clone()
        .map(|variant| hlist_type_for(&variant.fields))
        .fold(quote! {}, |location, variant_type| {
            quote! {
                #location.after_padding_for::<#variant_type>()
            }
        });

    let store_variants = variants.clone().enumerate().map(|(index, variant)| {
        let variant_name = &variant.ident;
        let discriminant =
            LitInt::new(&format!("{index}_{discriminant_type}"), variant_name.span());

        let field_names = field_names(&variant.fields);
        let pattern = fields_pattern(&variant.fields, field_names.clone());
        let fields_hlist_value = quote! { linera_witty::hlist![#( #field_names ),*] };

        quote! {
            #name::#variant_name #pattern => {
                #discriminant.store(memory, location)?;
                location = location.after::<#discriminant_type>() #align_to_cases;

                #fields_hlist_value.store(memory, location)
            }
        }
    });

    let lower_variants = variants.enumerate().map(|(index, variant)| {
        let variant_name = &variant.ident;
        let discriminant =
            LitInt::new(&format!("{index}_{discriminant_type}"), variant_name.span());

        let field_names = field_names(&variant.fields);
        let pattern = fields_pattern(&variant.fields, field_names.clone());
        let fields_hlist_value = quote! { linera_witty::hlist![#( #field_names ),*] };

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
