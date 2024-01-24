// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-views`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{parse_macro_input, Attribute, ItemStruct, Lit, LitStr, MetaNameValue, Type, TypePath};

fn get_seq_parameter(generics: syn::Generics) -> Vec<syn::Ident> {
    let mut generic_vect = Vec::new();
    for param in generics.params {
        if let syn::GenericParam::Type(param) = param {
            generic_vect.push(param.ident);
        }
    }
    generic_vect
}

fn get_type_field(field: syn::Field) -> Option<syn::Ident> {
    match field.ty {
        syn::Type::Path(typepath) => {
            if let Some(x) = typepath.path.segments.into_iter().next() {
                return Some(x.ident);
            }
            None
        }
        _ => None,
    }
}

fn custom_attribute(attributes: &[Attribute], key: &str) -> Option<LitStr> {
    attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("view"))
        .filter_map(|attribute| match attribute.parse_args() {
            Ok(MetaNameValue {
                path,
                value:
                    syn::Expr::Lit(syn::ExprLit {
                        lit: Lit::Str(value),
                        ..
                    }),
                ..
            }) => path.is_ident(key).then_some(value),
            _ => panic!(
                r#"Invalid `view` attribute syntax. \
                Expected syntax: `#[view(key = "value")]`"#,
            ),
        })
        .next()
}

fn context_and_constraints(
    attributes: &[Attribute],
    template_vect: &[syn::Ident],
) -> (Type, Option<TokenStream2>) {
    let context;
    let constraints;

    if let Some(context_literal) = custom_attribute(attributes, "context") {
        context = context_literal.parse().expect("Invalid context");
        constraints = None;
    } else {
        context = Type::Path(TypePath {
            qself: None,
            path: template_vect
                .first()
                .expect("failed to find the first generic parameter")
                .clone()
                .into(),
        });
        constraints = Some(quote! {
            where
                #context: linera_views::common::Context + Send + Sync + Clone + 'static,
                linera_views::views::ViewError: From<#context::Error>,
        });
    }

    (context, constraints)
}

fn generate_view_code(input: ItemStruct, root: bool) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut name_quotes = Vec::new();
    let mut load_future_quotes = Vec::new();
    let mut load_ident_quotes = Vec::new();
    let mut load_result_quotes = Vec::new();
    let mut rollback_quotes = Vec::new();
    let mut flush_quotes = Vec::new();
    let mut clear_quotes = Vec::new();
    for (idx, e) in input.fields.into_iter().enumerate() {
        let name = e.clone().ident.unwrap();
        let fut = format_ident!("{}_fut", name.to_string());
        let idx_lit = syn::LitInt::new(&idx.to_string(), Span::call_site());
        let type_ident = get_type_field(e).expect("Failed to find the type");
        load_future_quotes.push(quote! {
            let index = #idx_lit;
            let base_key = context.derive_tag_key(linera_views::common::MIN_VIEW_TAG, &index)?;
            let #fut = #type_ident::load(context.clone_with_base_key(base_key));
        });
        load_ident_quotes.push(quote! {
            #fut
        });
        load_result_quotes.push(quote! {
            let #name = result.#idx_lit?;
        });
        name_quotes.push(quote! { #name });
        rollback_quotes.push(quote! { self.#name.rollback(); });
        flush_quotes.push(quote! { self.#name.flush(batch)?; });
        clear_quotes.push(quote! { self.#name.clear(); });
    }
    let first_name_quote = name_quotes
        .first()
        .expect("list of names should be non-empty");

    let increment_counter = if root {
        quote! {
            #[cfg(not(target_arch = "wasm32"))]
            linera_views::increment_counter(
                &linera_views::LOAD_VIEW_COUNTER,
                stringify!(#struct_name),
                &context.base_key(),
            );
        }
    } else {
        quote! {}
    };

    quote! {
        #[async_trait::async_trait]
        impl #generics linera_views::views::View<#context> for #struct_name #generics
        #context_constraints
        {
            fn context(&self) -> &#context {
                use linera_views::views::View;
                self.#first_name_quote.context()
            }

            async fn load(context: #context) -> Result<Self, linera_views::views::ViewError> {
                use linera_views::{futures::join, common::Context};
                #increment_counter
                #(#load_future_quotes)*
                let result = join!(#(#load_ident_quotes),*);
                #(#load_result_quotes)*
                Ok(Self {#(#name_quotes),*})
            }


            fn rollback(&mut self) {
                #(#rollback_quotes)*
            }

            fn flush(&mut self, batch: &mut linera_views::batch::Batch) -> Result<(), linera_views::views::ViewError> {
                use linera_views::views::View;
                #(#flush_quotes)*
                Ok(())
            }

            fn clear(&mut self) {
                #(#clear_quotes)*
            }
        }
    }
}

fn generate_save_delete_view_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut flushes = Vec::new();
    let mut deletes = Vec::new();
    for e in input.fields {
        let name = e.clone().ident.unwrap();
        flushes.push(quote! { self.#name.flush(&mut batch)?; });
        deletes.push(quote! { self.#name.delete(batch); });
    }

    quote! {
        #[async_trait::async_trait]
        impl #generics linera_views::views::RootView<#context> for #struct_name #generics
        #context_constraints
        {
            async fn save(&mut self) -> Result<(), linera_views::views::ViewError> {
                use linera_views::{common::Context, batch::Batch, views::View};
                #[cfg(not(target_arch = "wasm32"))]
                linera_views::increment_counter(
                    &linera_views::SAVE_VIEW_COUNTER,
                    stringify!(#struct_name),
                    &self.context().base_key(),
                );
                let mut batch = Batch::new();
                #(#flushes)*
                self.context().write_batch(batch).await?;
                Ok(())
            }
        }
    }
}

fn generate_hash_view_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut field_hashes_mut = Vec::new();
    let mut field_hashes = Vec::new();
    for e in input.fields {
        let name = e.clone().ident.unwrap();
        field_hashes_mut.push(quote! { hasher.write_all(self.#name.hash_mut().await?.as_ref())?; });
        field_hashes.push(quote! { hasher.write_all(self.#name.hash().await?.as_ref())?; });
    }

    quote! {
        #[async_trait::async_trait]
        impl #generics linera_views::views::HashableView<#context> for #struct_name #generics
        #context_constraints
        {
            type Hasher = linera_views::sha3::Sha3_256;

            async fn hash_mut(&mut self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::views::ViewError> {
                use linera_views::views::{Hasher, HashableView};
                use std::io::Write;
                let mut hasher = Self::Hasher::default();
                #(#field_hashes_mut)*
                Ok(hasher.finalize())
            }

            async fn hash(&self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::views::ViewError> {
                use linera_views::views::{Hasher, HashableView};
                use std::io::Write;
                let mut hasher = Self::Hasher::default();
                #(#field_hashes)*
                Ok(hasher.finalize())
            }
        }
    }
}

fn generate_crypto_hash_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let hash_type = syn::Ident::new(&format!("{}Hash", struct_name), Span::call_site());
    quote! {
        #[async_trait::async_trait]
        impl #generics linera_views::views::CryptoHashView<#context> for #struct_name #generics
        #context_constraints
        {
            async fn crypto_hash(&self) -> Result<linera_base::crypto::CryptoHash, linera_views::views::ViewError> {
                use linera_base::crypto::{BcsHashable, CryptoHash};
                use linera_views::{
                    batch::Batch,
                    generic_array::GenericArray,
                    sha3::{digest::OutputSizeUser, Sha3_256},
                    views::HashableView,
                };
                use serde::{Serialize, Deserialize};
                #[derive(Serialize, Deserialize)]
                struct #hash_type(GenericArray<u8, <Sha3_256 as OutputSizeUser>::OutputSize>);
                impl BcsHashable for #hash_type {}
                let hash = self.hash().await?;
                Ok(CryptoHash::new(&#hash_type(hash)))
            }
        }
    }
}

#[proc_macro_derive(View, attributes(view))]
pub fn derive_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    generate_view_code(input, false).into()
}

#[proc_macro_derive(HashableView, attributes(view))]
pub fn derive_hash_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let mut stream = generate_view_code(input.clone(), false);
    stream.extend(generate_hash_view_code(input));
    stream.into()
}

#[proc_macro_derive(RootView, attributes(view))]
pub fn derive_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let mut stream = generate_view_code(input.clone(), true);
    stream.extend(generate_save_delete_view_code(input));
    stream.into()
}

#[proc_macro_derive(CryptoHashView, attributes(view))]
pub fn derive_crypto_hash_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let mut stream = generate_view_code(input.clone(), false);
    stream.extend(generate_hash_view_code(input.clone()));
    stream.extend(generate_crypto_hash_code(input));
    stream.into()
}

#[proc_macro_derive(CryptoHashRootView, attributes(view))]
pub fn derive_crypto_hash_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let mut stream = generate_view_code(input.clone(), true);
    stream.extend(generate_save_delete_view_code(input.clone()));
    stream.extend(generate_hash_view_code(input.clone()));
    stream.extend(generate_crypto_hash_code(input));
    stream.into()
}

#[proc_macro_derive(HashableRootView, attributes(view))]
#[cfg(test)]
pub fn derive_hashable_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let mut stream = generate_view_code(input.clone(), true);
    stream.extend(generate_save_delete_view_code(input.clone()));
    stream.extend(generate_hash_view_code(input));
    stream.into()
}

#[cfg(test)]
pub mod tests {

    use crate::*;
    use quote::quote;
    use syn::parse_quote;

    fn pretty(tokens: TokenStream2) -> String {
        prettyplease::unparse(
            &syn::parse2::<syn::File>(tokens).expect("failed to parse test output"),
        )
    }

    #[test]
    fn test_generate_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_display_snapshot!(pretty(generate_view_code(input, true)));
        }
    }

    #[test]
    fn test_generate_hash_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_display_snapshot!(pretty(generate_hash_view_code(input)));
        }
    }

    #[test]
    fn test_generate_save_delete_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_display_snapshot!(pretty(generate_save_delete_view_code(input)));
        }
    }

    #[test]
    fn test_generate_crypto_hash_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_display_snapshot!(pretty(generate_crypto_hash_code(input)));
        }
    }

    pub struct SpecificContextInfo {
        attribute: Option<TokenStream2>,
        context: Type,
        generics: TokenStream2,
    }

    impl SpecificContextInfo {
        pub fn empty() -> Self {
            SpecificContextInfo {
                attribute: None,
                context: syn::parse_str("C").unwrap(),
                generics: quote! { <C> },
            }
        }

        pub fn new(context: &str) -> Self {
            SpecificContextInfo {
                attribute: Some(quote! { #[view(context = #context)] }),
                context: syn::parse_str(context).unwrap(),
                generics: quote! {},
            }
        }

        pub fn test_cases() -> impl Iterator<Item = Self> {
            Some(Self::empty()).into_iter().chain(
                [
                    "CustomContext",
                    "custom::path::to::ContextType",
                    "custom::GenericContext<T>",
                ]
                .into_iter()
                .map(Self::new),
            )
        }

        pub fn test_view_input(&self) -> ItemStruct {
            let SpecificContextInfo {
                attribute,
                context,
                generics,
                ..
            } = self;

            parse_quote! {
                #attribute
                struct TestView #generics {
                    register: RegisterView<#context, usize>,
                    collection: CollectionView<#context, usize, RegisterView<#context, usize>>,
                }
            }
        }
    }
}
