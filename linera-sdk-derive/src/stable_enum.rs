// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code generation for the `StableEnum` derive macro.
//!
//! Each variant of the input enum is assigned a `u32` "stable tag" computed at
//! macro-expansion time from the variant's name:
//!
//! 1. Compute `Keccak-256(variant_name)`.
//! 2. Read the first 4 bytes as a big-endian `u32`.
//! 3. Mask the top 5 bits to `00001`, i.e. `(val & 0x07FF_FFFF) | 0x0800_0000`.
//!
//! Step 3 forces the tag into the range `[2^27, 2^28 - 1]`, which is exactly
//! the range whose ULEB128 encoding is always 4 bytes.

use proc_macro2::{Ident, Span, TokenStream as TokenStream2};
use sha3::{Digest, Keccak256};
use syn::{__private::quote::quote, spanned::Spanned, Error, Fields, ItemEnum, Result, Variant};

/// Where the `StableEnumTrace` trait lives, from the perspective of the code
/// being generated.
pub enum CrateRoot {
    /// Generated code lives outside `linera-sdk`: use `::linera_sdk::...`.
    LineraSdk,
    /// Generated code lives inside `linera-sdk`: use `crate::...`.
    Crate,
}

impl CrateRoot {
    fn trait_path(&self) -> TokenStream2 {
        match self {
            CrateRoot::LineraSdk => quote! { ::linera_sdk::formats::StableEnumTrace },
            CrateRoot::Crate => quote! { crate::formats::StableEnumTrace },
        }
    }
}

/// Compute the stable tag for a variant name.
fn compute_tag(variant_name: &str) -> u32 {
    let hash = Keccak256::digest(variant_name.as_bytes());
    let val = u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]]);
    (val & 0x07FF_FFFF) | 0x0800_0000
}

/// Compute all variant tags, returning an error on a (vanishingly unlikely) collision.
fn variant_tags(input: &ItemEnum) -> Result<Vec<(String, u32, &Variant)>> {
    let mut out = Vec::with_capacity(input.variants.len());
    for variant in &input.variants {
        let name = variant.ident.to_string();
        let tag = compute_tag(&name);
        if let Some((other_name, _, _)) =
            out.iter().find(|(_, t, _): &&(String, u32, &Variant)| *t == tag)
        {
            return Err(Error::new(
                variant.span(),
                format!(
                    "Keccak-256 tag collision between variants `{}` and `{}` (tag = {:#010x}). \
                     Rename one of the variants.",
                    other_name, name, tag
                ),
            ));
        }
        out.push((name, tag, variant));
    }
    Ok(out)
}

fn reject_generics(input: &ItemEnum) -> Result<()> {
    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(Error::new(
            input.generics.span(),
            "#[derive(StableEnum)] does not yet support generic enums",
        ));
    }
    Ok(())
}

/// Emit the combined `Serialize` + `Deserialize` + `StableEnumTrace` impls.
pub fn generate_all(input: &ItemEnum, crate_root: CrateRoot) -> Result<TokenStream2> {
    let ser = generate_serialize(input)?;
    let de = generate_deserialize(input)?;
    let tr = generate_trace(input, crate_root)?;
    Ok(quote! {
        #ser
        #de
        #tr
    })
}

pub fn generate_serialize(input: &ItemEnum) -> Result<TokenStream2> {
    reject_generics(input)?;
    let enum_ident = &input.ident;
    let enum_name_str = enum_ident.to_string();
    let tagged = variant_tags(input)?;

    let arms = tagged.iter().map(|(name, tag, variant)| {
        let variant_ident = &variant.ident;
        let name_lit = name.as_str();
        match &variant.fields {
            Fields::Unit => quote! {
                Self::#variant_ident => ::serde::Serializer::serialize_unit_variant(
                    __serializer, #enum_name_str, #tag, #name_lit,
                ),
            },
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => quote! {
                Self::#variant_ident(__f0) => ::serde::Serializer::serialize_newtype_variant(
                    __serializer, #enum_name_str, #tag, #name_lit, __f0,
                ),
            },
            Fields::Unnamed(fields) => {
                let len = fields.unnamed.len();
                let idents: Vec<Ident> = (0..len)
                    .map(|i| Ident::new(&format!("__f{i}"), Span::call_site()))
                    .collect();
                quote! {
                    Self::#variant_ident(#(#idents),*) => {
                        let mut __tv = ::serde::Serializer::serialize_tuple_variant(
                            __serializer, #enum_name_str, #tag, #name_lit, #len,
                        )?;
                        #(
                            ::serde::ser::SerializeTupleVariant::serialize_field(&mut __tv, #idents)?;
                        )*
                        ::serde::ser::SerializeTupleVariant::end(__tv)
                    }
                }
            }
            Fields::Named(fields) => {
                let len = fields.named.len();
                let field_idents: Vec<&Ident> = fields
                    .named
                    .iter()
                    .map(|f| f.ident.as_ref().expect("named field has ident"))
                    .collect();
                let field_strs: Vec<String> = field_idents.iter().map(|i| i.to_string()).collect();
                quote! {
                    Self::#variant_ident { #(#field_idents),* } => {
                        let mut __sv = ::serde::Serializer::serialize_struct_variant(
                            __serializer, #enum_name_str, #tag, #name_lit, #len,
                        )?;
                        #(
                            ::serde::ser::SerializeStructVariant::serialize_field(
                                &mut __sv, #field_strs, #field_idents,
                            )?;
                        )*
                        ::serde::ser::SerializeStructVariant::end(__sv)
                    }
                }
            }
        }
    });

    Ok(quote! {
        #[automatically_derived]
        impl ::serde::Serialize for #enum_ident {
            fn serialize<__S>(&self, __serializer: __S) -> ::core::result::Result<__S::Ok, __S::Error>
            where
                __S: ::serde::Serializer,
            {
                match self {
                    #(#arms)*
                }
            }
        }
    })
}

pub fn generate_deserialize(input: &ItemEnum) -> Result<TokenStream2> {
    reject_generics(input)?;
    let enum_ident = &input.ident;
    let enum_name_str = enum_ident.to_string();
    let tagged = variant_tags(input)?;

    let variant_name_lits: Vec<String> = tagged.iter().map(|(n, _, _)| n.clone()).collect();

    let arms = tagged.iter().map(|(name, tag, variant)| {
        let variant_ident = &variant.ident;
        let name_lit = name.as_str();
        match &variant.fields {
            Fields::Unit => quote! {
                #tag => {
                    ::serde::de::VariantAccess::unit_variant(__variant)?;
                    ::core::result::Result::Ok(#enum_ident::#variant_ident)
                }
            },
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let ty = &fields.unnamed.first().unwrap().ty;
                quote! {
                    #tag => {
                        let __inner = ::serde::de::VariantAccess::newtype_variant::<#ty>(__variant)?;
                        ::core::result::Result::Ok(#enum_ident::#variant_ident(__inner))
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let len = fields.unnamed.len();
                let types: Vec<_> = fields.unnamed.iter().map(|f| &f.ty).collect();
                let bindings: Vec<Ident> = (0..len)
                    .map(|i| Ident::new(&format!("__f{i}"), Span::call_site()))
                    .collect();
                let next_elements = bindings.iter().zip(types.iter()).enumerate().map(|(i, (b, t))| {
                    let expected = format!("tuple variant `{name_lit}` with {len} elements");
                    quote! {
                        let #b: #t = ::serde::de::SeqAccess::next_element::<#t>(&mut __seq)?
                            .ok_or_else(|| ::serde::de::Error::invalid_length(#i, &#expected))?;
                    }
                });
                let expecting_msg = format!("tuple variant `{name_lit}`");
                quote! {
                    #tag => {
                        struct __TupleVisitor;
                        impl<'de> ::serde::de::Visitor<'de> for __TupleVisitor {
                            type Value = (#(#types,)*);
                            fn expecting(&self, __f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                                __f.write_str(#expecting_msg)
                            }
                            fn visit_seq<__A>(self, mut __seq: __A) -> ::core::result::Result<Self::Value, __A::Error>
                            where
                                __A: ::serde::de::SeqAccess<'de>,
                            {
                                #(#next_elements)*
                                ::core::result::Result::Ok((#(#bindings,)*))
                            }
                        }
                        let (#(#bindings,)*) = ::serde::de::VariantAccess::tuple_variant(
                            __variant, #len, __TupleVisitor,
                        )?;
                        ::core::result::Result::Ok(#enum_ident::#variant_ident(#(#bindings,)*))
                    }
                }
            }
            Fields::Named(fields) => {
                let len = fields.named.len();
                let field_idents: Vec<&Ident> = fields
                    .named
                    .iter()
                    .map(|f| f.ident.as_ref().expect("named field has ident"))
                    .collect();
                let field_strs: Vec<String> = field_idents.iter().map(|i| i.to_string()).collect();
                let types: Vec<_> = fields.named.iter().map(|f| &f.ty).collect();
                let next_elements = field_idents
                    .iter()
                    .zip(types.iter())
                    .enumerate()
                    .map(|(i, (b, t))| {
                        let expected = format!("struct variant `{name_lit}` with {len} fields");
                        quote! {
                            let #b: #t = ::serde::de::SeqAccess::next_element::<#t>(&mut __seq)?
                                .ok_or_else(|| ::serde::de::Error::invalid_length(#i, &#expected))?;
                        }
                    });
                let expecting_msg = format!("struct variant `{name_lit}`");
                quote! {
                    #tag => {
                        struct __StructVisitor;
                        impl<'de> ::serde::de::Visitor<'de> for __StructVisitor {
                            type Value = (#(#types,)*);
                            fn expecting(&self, __f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                                __f.write_str(#expecting_msg)
                            }
                            fn visit_seq<__A>(self, mut __seq: __A) -> ::core::result::Result<Self::Value, __A::Error>
                            where
                                __A: ::serde::de::SeqAccess<'de>,
                            {
                                #(#next_elements)*
                                ::core::result::Result::Ok((#(#field_idents,)*))
                            }
                        }
                        const __FIELDS: &[&::core::primitive::str] = &[#(#field_strs),*];
                        let (#(#field_idents,)*) = ::serde::de::VariantAccess::struct_variant(
                            __variant, __FIELDS, __StructVisitor,
                        )?;
                        ::core::result::Result::Ok(#enum_ident::#variant_ident { #(#field_idents,)* })
                    }
                }
            }
        }
    });

    let expecting_msg = format!("enum `{enum_name_str}` (stable-tagged)");

    Ok(quote! {
        #[automatically_derived]
        impl<'de> ::serde::Deserialize<'de> for #enum_ident {
            fn deserialize<__D>(__deserializer: __D) -> ::core::result::Result<Self, __D::Error>
            where
                __D: ::serde::Deserializer<'de>,
            {
                struct __OuterVisitor;
                impl<'de> ::serde::de::Visitor<'de> for __OuterVisitor {
                    type Value = #enum_ident;
                    fn expecting(&self, __f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
                        __f.write_str(#expecting_msg)
                    }
                    fn visit_enum<__A>(self, __access: __A) -> ::core::result::Result<Self::Value, __A::Error>
                    where
                        __A: ::serde::de::EnumAccess<'de>,
                    {
                        let (__tag, __variant) =
                            ::serde::de::EnumAccess::variant::<::core::primitive::u32>(__access)?;
                        match __tag {
                            #(#arms)*
                            __unknown => ::core::result::Result::Err(
                                <__A::Error as ::serde::de::Error>::custom(::std::format!(
                                    "unknown stable variant tag for `{}`: {:#010x}",
                                    #enum_name_str, __unknown,
                                )),
                            ),
                        }
                    }
                }
                const __VARIANTS: &[&::core::primitive::str] = &[#(#variant_name_lits),*];
                ::serde::Deserializer::deserialize_enum(
                    __deserializer,
                    #enum_name_str,
                    __VARIANTS,
                    __OuterVisitor,
                )
            }
        }
    })
}

pub fn generate_trace(input: &ItemEnum, crate_root: CrateRoot) -> Result<TokenStream2> {
    reject_generics(input)?;
    let enum_ident = &input.ident;
    let enum_name_str = enum_ident.to_string();
    let tagged = variant_tags(input)?;
    let trait_path = crate_root.trait_path();

    let variants_const_entries = tagged.iter().map(|(name, tag, _)| {
        let n = name.as_str();
        quote! { (#n, #tag) }
    });

    // For each variant, generate a block that:
    //   1. calls trace_type_once for each field's type using the caller-supplied samples
    //   2. constructs the variant and calls trace_value with our scratch samples
    let blocks = tagged.iter().map(|(_, _, variant)| {
        let variant_ident = &variant.ident;
        match &variant.fields {
            Fields::Unit => quote! {
                let (__fmt, _) = ::serde_reflection::Tracer::trace_value(
                    __tracer,
                    &mut __scratch,
                    &#enum_ident::#variant_ident,
                )?;
                __format = ::core::option::Option::Some(__fmt);
            },
            Fields::Unnamed(fields) => {
                let bindings: Vec<Ident> = (0..fields.unnamed.len())
                    .map(|i| Ident::new(&format!("__field_{i}"), Span::call_site()))
                    .collect();
                let traces = bindings.iter().zip(fields.unnamed.iter()).map(|(b, f)| {
                    let ty = &f.ty;
                    quote! {
                        let (_, #b): (_, #ty) =
                            ::serde_reflection::Tracer::trace_type_once(__tracer, __samples)?;
                    }
                });
                quote! {
                    #(#traces)*
                    let (__fmt, _) = ::serde_reflection::Tracer::trace_value(
                        __tracer,
                        &mut __scratch,
                        &#enum_ident::#variant_ident(#(#bindings),*),
                    )?;
                    __format = ::core::option::Option::Some(__fmt);
                }
            }
            Fields::Named(fields) => {
                let field_idents: Vec<&Ident> = fields
                    .named
                    .iter()
                    .map(|f| f.ident.as_ref().expect("named field has ident"))
                    .collect();
                let bindings: Vec<Ident> = field_idents
                    .iter()
                    .map(|i| Ident::new(&format!("__field_{i}"), i.span()))
                    .collect();
                let traces = bindings.iter().zip(fields.named.iter()).map(|(b, f)| {
                    let ty = &f.ty;
                    quote! {
                        let (_, #b): (_, #ty) =
                            ::serde_reflection::Tracer::trace_type_once(__tracer, __samples)?;
                    }
                });
                quote! {
                    #(#traces)*
                    let (__fmt, _) = ::serde_reflection::Tracer::trace_value(
                        __tracer,
                        &mut __scratch,
                        &#enum_ident::#variant_ident {
                            #( #field_idents: #bindings ),*
                        },
                    )?;
                    __format = ::core::option::Option::Some(__fmt);
                }
            }
        }
    });

    Ok(quote! {
        // The impl uses `serde_reflection`, which Linera applications only
        // depend on for native (non-wasm) builds (e.g. format snapshot tests).
        #[cfg(not(target_arch = "wasm32"))]
        #[automatically_derived]
        impl #trait_path for #enum_ident {
            const STABLE_VARIANTS: &'static [(&'static ::core::primitive::str, ::core::primitive::u32)] = &[
                #(#variants_const_entries),*
            ];

            fn trace_all_variants(
                __tracer: &mut ::serde_reflection::Tracer,
                __samples: &::serde_reflection::Samples,
            ) -> ::serde_reflection::Result<::serde_reflection::Format> {
                // `trace_value` requires `&mut Samples` even though we have no
                // need to feed its recordings back to the caller, so we use a
                // scratch instance here.
                let mut __scratch = ::serde_reflection::Samples::default();
                let mut __format: ::core::option::Option<::serde_reflection::Format> =
                    ::core::option::Option::None;
                #({ #blocks })*
                __format.ok_or_else(|| ::serde_reflection::Error::Custom(
                    ::std::format!(
                        "stable enum `{}` has no variants",
                        #enum_name_str,
                    ),
                ))
            }
        }
    })
}
