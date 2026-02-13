// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Derive macro for EIP-712 struct encoding.
//!
//! Generates implementations of the `Eip712Struct` trait, producing type strings,
//! type hashes, struct hashes, JSON values, and JSON type definitions from annotated
//! struct definitions.

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Attribute, DeriveInput, Field, Ident, LitStr, Token,
};

/// Parsed struct-level `#[eip712(...)]` attributes.
struct StructAttrs {
    name: String,
    references: Vec<Ident>,
}

/// Parsed field-level `#[eip712(soltype = "...")]` attribute.
struct FieldAttrs {
    soltype: String,
}

/// Parser for the content inside `#[eip712(...)]` at the struct level.
impl Parse for StructAttrs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut references = Vec::new();

        while !input.is_empty() {
            let ident: Ident = input.parse()?;
            match ident.to_string().as_str() {
                "name" => {
                    let _: Token![=] = input.parse()?;
                    let lit: LitStr = input.parse()?;
                    name = Some(lit.value());
                }
                "references" => {
                    let content;
                    syn::parenthesized!(content in input);
                    while !content.is_empty() {
                        let ref_ident: Ident = content.parse()?;
                        references.push(ref_ident);
                        if !content.is_empty() {
                            let _: Token![,] = content.parse()?;
                        }
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unknown eip712 struct attribute: `{other}`"),
                    ));
                }
            }
            if !input.is_empty() {
                let _: Token![,] = input.parse()?;
            }
        }

        let name =
            name.ok_or_else(|| input.error("missing required `name = \"...\"` attribute"))?;
        Ok(StructAttrs { name, references })
    }
}

fn parse_struct_attrs(attrs: &[Attribute]) -> syn::Result<StructAttrs> {
    for attr in attrs {
        if attr.path().is_ident("eip712") {
            return attr.parse_args::<StructAttrs>();
        }
    }
    Err(syn::Error::new(
        proc_macro2::Span::call_site(),
        "missing #[eip712(...)] attribute on struct",
    ))
}

fn parse_field_attrs(field: &Field) -> syn::Result<FieldAttrs> {
    for attr in &field.attrs {
        if attr.path().is_ident("eip712") {
            let mut soltype = None;
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("soltype") {
                    let value = meta.value()?;
                    let lit: LitStr = value.parse()?;
                    soltype = Some(lit.value());
                    Ok(())
                } else {
                    Err(meta.error("expected `soltype`"))
                }
            })?;
            return soltype
                .map(|s| FieldAttrs { soltype: s })
                .ok_or_else(|| syn::Error::new_spanned(attr, "missing `soltype` in #[eip712]"));
        }
    }
    Err(syn::Error::new_spanned(
        field,
        "missing #[eip712(soltype = \"...\")] attribute",
    ))
}

/// Generates the hash encoding expression for a field based on its soltype.
fn hash_encode_expr(soltype: &str, field_access: &TokenStream2) -> TokenStream2 {
    match soltype {
        "string" => quote! { encode_string(&#field_access) },
        "uint64" => quote! { encode_u64(#field_access) },
        "uint32" => quote! { encode_u32(#field_access) },
        "uint128" => quote! { encode_u128(#field_access) },
        "bytes32" => quote! { encode_bytes32(#field_access) },
        "bool" => quote! { encode_bool(#field_access) },
        "string[]" => {
            quote! {{
                let __hashes: Vec<[u8; 32]> = #field_access.iter().map(|s| encode_string(s)).collect();
                encode_hash_array(&__hashes)
            }}
        }
        "uint64[]" => {
            quote! {{
                let __hashes: Vec<[u8; 32]> = #field_access.iter().map(|v| encode_u64(*v)).collect();
                encode_hash_array(&__hashes)
            }}
        }
        other if other.ends_with("[]") => {
            // Struct array: T[] where T implements Eip712Struct
            quote! {{
                let __hashes: Vec<[u8; 32]> = #field_access.iter().map(|v| v.hash_struct()).collect();
                encode_hash_array(&__hashes)
            }}
        }
        _ => {
            panic!("unsupported soltype for hash encoding: `{soltype}`");
        }
    }
}

/// Generates the JSON value expression for a field based on its soltype.
fn json_value_expr(soltype: &str, field_access: &TokenStream2) -> TokenStream2 {
    match soltype {
        "string" => quote! { serde_json::Value::String(#field_access.clone()) },
        "uint64" => {
            quote! { serde_json::Value::String(#field_access.to_string()) }
        }
        "uint32" => {
            quote! { serde_json::Value::Number(serde_json::Number::from(#field_access)) }
        }
        "uint128" => quote! { serde_json::Value::String(#field_access.to_string()) },
        "bytes32" => {
            quote! { serde_json::Value::String(format!("0x{}", alloy_primitives::hex::encode(#field_access))) }
        }
        "bool" => quote! { serde_json::Value::Bool(#field_access) },
        "string[]" => {
            quote! {
                serde_json::Value::Array(
                    #field_access.iter().map(|s| serde_json::Value::String(s.clone())).collect()
                )
            }
        }
        "uint64[]" => {
            quote! {
                serde_json::Value::Array(
                    #field_access.iter().map(|v| serde_json::Value::String(v.to_string())).collect()
                )
            }
        }
        other if other.ends_with("[]") => {
            // Struct array: T[] where T implements Eip712Struct
            quote! {
                serde_json::Value::Array(
                    #field_access.iter().map(|v| v.to_eip712_json()).collect()
                )
            }
        }
        _ => {
            panic!("unsupported soltype for JSON encoding: `{soltype}`");
        }
    }
}

#[proc_macro_derive(Eip712Struct, attributes(eip712))]
pub fn derive_eip712_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_eip712_struct(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn impl_eip712_struct(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let struct_attrs = parse_struct_attrs(&input.attrs)?;
    let struct_name = &input.ident;
    let eip712_name = &struct_attrs.name;

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "Eip712Struct only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "Eip712Struct can only be derived for structs",
            ))
        }
    };

    // Collect field info: (rust_ident, camelCase_name, soltype)
    let mut field_info = Vec::new();
    for field in fields {
        let field_attrs = parse_field_attrs(field)?;
        let ident = field.ident.as_ref().unwrap();
        let camel_name = ident.to_string().to_case(Case::Camel);
        field_info.push((ident.clone(), camel_name, field_attrs.soltype));
    }

    // Build PRIMARY_DEF: "Name(soltype camelName,...)"
    let field_parts: Vec<String> = field_info
        .iter()
        .map(|(_, camel, sol)| format!("{sol} {camel}"))
        .collect();
    let primary_def = format!("{}({})", eip712_name, field_parts.join(","));

    // Build type_hash computation
    let type_hash_body = if struct_attrs.references.is_empty() {
        quote! {
            static H: std::sync::LazyLock<[u8; 32]> = std::sync::LazyLock::new(|| {
                alloy_primitives::keccak256(#struct_name::PRIMARY_DEF).0
            });
            &H
        }
    } else {
        let ref_types = &struct_attrs.references;
        quote! {
            static H: std::sync::LazyLock<[u8; 32]> = std::sync::LazyLock::new(|| {
                let mut refs = vec![#( #ref_types::PRIMARY_DEF ),*];
                refs.sort();
                let mut full = String::from(#struct_name::PRIMARY_DEF);
                for r in refs {
                    full.push_str(r);
                }
                alloy_primitives::keccak256(full.as_bytes()).0
            });
            &H
        }
    };

    // Build hash_struct body
    let field_count = field_info.len();
    let capacity = field_count + 1; // +1 for type hash
    let hash_lines: Vec<TokenStream2> = field_info
        .iter()
        .map(|(ident, _, soltype)| {
            let field_access = quote! { self.#ident };
            let expr = hash_encode_expr(soltype, &field_access);
            quote! { buf.extend_from_slice(&#expr); }
        })
        .collect();

    // Build to_eip712_json body
    let json_entries: Vec<TokenStream2> = field_info
        .iter()
        .map(|(ident, camel, soltype)| {
            let field_access = quote! { self.#ident };
            let val_expr = json_value_expr(soltype, &field_access);
            quote! { map.insert(#camel.to_string(), #val_expr); }
        })
        .collect();

    // Build eip712_json_type_def body
    let type_def_entries: Vec<TokenStream2> = field_info
        .iter()
        .map(|(_, camel, soltype)| {
            quote! {
                serde_json::json!({"name": #camel, "type": #soltype})
            }
        })
        .collect();

    Ok(quote! {
        impl Eip712Struct for #struct_name {
            const PRIMARY_DEF: &'static str = #primary_def;

            fn eip712_type_hash() -> &'static [u8; 32] {
                #type_hash_body
            }

            fn hash_struct(&self) -> [u8; 32] {
                let mut buf = Vec::with_capacity(#capacity * 32);
                buf.extend_from_slice(Self::eip712_type_hash());
                #( #hash_lines )*
                alloy_primitives::keccak256(&buf).0
            }

            fn to_eip712_json(&self) -> serde_json::Value {
                let mut map = serde_json::Map::new();
                #( #json_entries )*
                serde_json::Value::Object(map)
            }

            fn eip712_json_type_def() -> serde_json::Value {
                serde_json::json!([
                    #( #type_def_entries ),*
                ])
            }
        }
    })
}
