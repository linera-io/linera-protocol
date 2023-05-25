// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-views`.

pub(crate) mod util;

extern crate heck;
extern crate proc_macro;
extern crate syn;
use crate::util::{concat, get_graphql_pair_for_underscore, snakify, string_to_ident};
use heck::AsUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, Attribute, Field, GenericArgument, ItemStruct, Lit, LitStr, MetaNameValue,
    PathArguments, Type, TypePath,
};

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
        .filter(|attribute| attribute.path.is_ident("view"))
        .filter_map(|attribute| match attribute.parse_args() {
            Ok(MetaNameValue {
                path,
                lit: Lit::Str(value),
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
                .get(0)
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
    let mut delete_quotes = Vec::new();
    let mut clear_quotes = Vec::new();
    for (idx, e) in input.fields.into_iter().enumerate() {
        let name = e.clone().ident.unwrap();
        let fut = format_ident!("{}_fut", name.to_string());
        let idx_lit = syn::LitInt::new(&idx.to_string(), Span::call_site());
        let type_ident = get_type_field(e).expect("Failed to find the type");
        load_future_quotes.push(quote! {
            let index = #idx_lit;
            let base_key = context.derive_key(&index)?;
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
        delete_quotes.push(quote! { self.#name.delete(batch); });
        clear_quotes.push(quote! { self.#name.clear(); });
    }
    let first_name_quote = name_quotes
        .get(0)
        .expect("list of names should be non-empty");

    let increment_counter = if root {
        quote! {
            linera_views::increment_counter(
                linera_views::LOAD_VIEW_COUNTER,
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

            fn delete(self, batch: &mut linera_views::batch::Batch) {
                use linera_views::views::View;
                #(#delete_quotes)*
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
                linera_views::increment_counter(
                    linera_views::SAVE_VIEW_COUNTER,
                    stringify!(#struct_name),
                    &self.context().base_key(),
                );
                let mut batch = Batch::new();
                #(#flushes)*
                self.context().write_batch(batch).await?;
                Ok(())
            }

            async fn write_delete(self) -> Result<(), linera_views::views::ViewError> {
                use linera_views::{common::Context, batch::Batch, views::View};
                let context = self.context().clone();
                let batch = Batch::build(move |batch| {
                    Box::pin(async move {
                        #(#deletes)*
                        Ok(())
                    })
                }).await?;
                context.write_batch(batch).await?;
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

fn generic_argument_from_type_path(type_path: &TypePath) -> Vec<&Type> {
    let arguments = &type_path
        .path
        .segments
        .iter()
        .next()
        .expect("type path should have at least one segment")
        .arguments;

    let angle = match arguments {
        PathArguments::AngleBracketed(angle) => angle,
        _ => {
            unreachable!("")
        }
    };

    angle
        .args
        .pairs()
        .map(|pair| *pair.value())
        .map(|value| match value {
            GenericArgument::Type(r#type) => r#type,
            _ => panic!("Only types are supported as generic arguments in views."),
        })
        .collect()
}

fn generic_offset(
    context: &Type,
    context_constraints: &Option<TokenStream2>,
    generics: &[&Type],
) -> usize {
    let requires_generic_context = context_constraints.is_some();
    let first_generic_argument_is_context = requires_generic_context || generics[0] == context;

    if first_generic_argument_is_context {
        1
    } else {
        0
    }
}

fn generate_graphql_code_for_field(
    struct_name: syn::Ident,
    context: Type,
    context_constraints: Option<TokenStream2>,
    field: Field,
) -> (TokenStream2, Option<TokenStream2>) {
    let field_name = field
        .ident
        .clone()
        .expect("anonymous fields not supported.");
    let view_type = get_type_field(field.clone()).expect("could not get view type.");
    let type_path = match field.ty {
        Type::Path(type_path) => type_path,
        _ => panic!(),
    };

    let (field_name_underscore, field_name_str) = get_graphql_pair_for_underscore(&field_name);

    let view_name = view_type.to_string();
    match view_type.to_string().as_str() {
        "RegisterView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let generic_ident = generic_arguments
                .get(generic_types_offset)
                .expect("no generic specified for 'RegisterView'");
            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self) -> &#generic_ident {
                    self.#field_name.get()
                }
            };
            (r#impl, None)
        }
        "CollectionView"
        | "ReentrantCollectionView"
        | "CustomCollectionView"
        | "ReentrantCustomCollectionView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let index_ident = generic_arguments
                .get(generic_types_offset)
                .unwrap_or_else(|| panic!("no index specified for '{}'", view_name));
            let generic_ident = generic_arguments
                .get(generic_types_offset + 1)
                .unwrap_or_else(|| panic!("no generic type specified for '{}'", view_name));

            let index_name = snakify(index_ident);

            let camel_index_name = format!("{}", field_name);
            let camel_index_name = AsUpperCamelCase(&camel_index_name);
            let entry_name = format!("{}{}Entry", struct_name, camel_index_name);
            let entry_name = string_to_ident(&entry_name);
            let context_generics = context_constraints.as_ref().map(|_| quote! { <#context> });

            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(
                    &self,
                    #index_name: #index_ident,
                ) -> Result<#entry_name #context_generics, async_graphql::Error> {
                    Ok(#entry_name {
                        #index_name: #index_name.clone(),
                        guard: self.#field_name.try_load_entry(&#index_name).await?,
                    })
                }
            };

            let generic_method_name = snakify(generic_ident);

            let (guard, lifetime) = match view_name.as_str() {
                "ReentrantCollectionView" | "ReentrantCustomCollectionView" => {
                    (quote!(tokio::sync::OwnedRwLockReadGuard), quote!())
                }
                "CollectionView" | "CustomCollectionView" => (
                    quote!(linera_views::collection_view::ReadGuardedView),
                    quote!('a,),
                ),
                _ => {
                    todo!()
                }
            };
            let context_generics_with_lifetime = context_constraints
                .as_ref()
                .map(|_| quote! { <#lifetime #context> })
                .unwrap_or_else(|| quote! { <#lifetime> });
            let (index_name_underscore, index_name_str) =
                get_graphql_pair_for_underscore(&index_name);
            let (generic_underscore, generic_str) =
                get_graphql_pair_for_underscore(&generic_method_name);

            let r#struct = quote! {
                pub struct #entry_name #context_generics_with_lifetime
                #context_constraints
                {
                    #index_name: #index_ident,
                    guard: #guard<#lifetime #generic_ident>,
                }

                #[async_graphql::Object]
                impl #context_generics_with_lifetime #entry_name #context_generics_with_lifetime
                #context_constraints
                {
                    #[graphql(derived(name = #index_name_str))]
                    async fn #index_name_underscore(&self) -> &#index_ident {
                        &self.#index_name
                    }

                    #[graphql(derived(name = #generic_str))]
                    async fn #generic_underscore(&self) -> &#generic_ident {
                        use std::ops::Deref;
                        self.guard.deref()
                    }
                }
            };

            (r#impl, Some(r#struct))
        }
        "SetView" | "CustomSetView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let generic_ident = generic_arguments
                .get(generic_types_offset)
                .unwrap_or_else(|| panic!("no generic type specified for '{}'", view_name));

            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self) -> Result<Vec<#generic_ident>, async_graphql::Error> {
                    Ok(self.#field_name.indices().await?)
                }
            };
            (r#impl, None)
        }
        "LogView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let generic_ident = generic_arguments
                .get(generic_types_offset)
                .expect("no generic type specified for 'LogView'");

            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self,
                    start: Option<usize>,
                    end: Option<usize>
                ) -> Result<Vec<#generic_ident>, async_graphql::Error> {
                    let range = std::ops::Range {
                        start: start.unwrap_or(0),
                        end: end.unwrap_or(self.#field_name.count()),
                    };
                    Ok(self.#field_name.read(range).await?)
                }
            };
            (r#impl, None)
        }
        "WrappedHashableContainerView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let generic_ident = generic_arguments
                .get(generic_types_offset)
                .expect("no generic specified for 'WrappedHashableContainerView'");
            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self) -> &#generic_ident {
                    use std::ops::Deref;
                    self.#field_name.deref()
                }
            };
            (r#impl, None)
        }
        "QueueView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let generic_ident = generic_arguments
                .get(generic_types_offset)
                .expect("no generic type specified for 'QueueView'");

            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self, count: Option<usize>) -> Result<Vec<#generic_ident>, async_graphql::Error> {
                    let count = count.unwrap_or_else(|| self.#field_name.count());
                    Ok(self.#field_name.read_front(count).await?)
                }
            };
            (r#impl, None)
        }
        "ByteMapView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let generic_ident = generic_arguments
                .get(generic_types_offset)
                .expect("no generic type specified for 'ByteMapView'");

            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self, short_key: Vec<u8>) -> Result<Option<#generic_ident>, async_graphql::Error> {
                    Ok(self.#field_name.get(&short_key).await?)
                }
            };
            (r#impl, None)
        }
        "MapView" | "CustomMapView" => {
            let generic_arguments = generic_argument_from_type_path(&type_path);
            let generic_types_offset =
                generic_offset(&context, &context_constraints, &generic_arguments);
            let index_ident = generic_arguments
                .get(generic_types_offset)
                .unwrap_or_else(|| panic!("no index specified for '{}'", view_name));
            let generic_ident = generic_arguments
                .get(generic_types_offset + 1)
                .unwrap_or_else(|| panic!("no generic type specified for '{}'", view_name));

            let index_name = snakify(index_ident);
            let field_keys = concat(&field_name, "_keys");
            let (field_keys_underscore, field_keys_str) =
                get_graphql_pair_for_underscore(&field_keys);

            let r#impl = quote! {
                #[graphql(derived(name = #field_name_str))]
                async fn #field_name_underscore(&self, #index_name: #index_ident) -> Result<Option<#generic_ident>, async_graphql::Error> {
                    Ok(self.#field_name.get(&#index_name).await?)
                }
                #[graphql(derived(name = #field_keys_str))]
                async fn #field_keys_underscore(&self, count: Option<u64>)
                    -> Result<Vec<#index_ident>, async_graphql::Error>
                {
                    let count = count.unwrap_or(u64::MAX).try_into().unwrap_or(usize::MAX);
                    let mut keys = vec![];
                    if count == 0 {
                        return Ok(keys);
                    }
                    self.#field_name.for_each_index_while(|key| {
                        keys.push(key);
                        Ok(keys.len() < count)
                    }).await?;
                    Ok(keys)
                }
            };
            (r#impl, None)
        }
        _ => {
            let r#impl = quote! {
                async fn #field_name(&self) -> &#type_path {
                    &self.#field_name
                }
            };
            (r#impl, None)
        }
    }
}

fn generate_graphql_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut impls = vec![];
    let mut structs = vec![];

    for field in input.fields {
        let (r#impl, r#struct) = generate_graphql_code_for_field(
            struct_name.clone(),
            context.clone(),
            context_constraints.clone(),
            field,
        );
        impls.push(r#impl);
        if let Some(r#struct) = r#struct {
            structs.push(r#struct);
        }
    }

    quote! {
        #(#structs)*

        #[async_graphql::Object]
        impl #generics #struct_name #generics
        #context_constraints
        {
            #

            (#impls)

            *
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

#[proc_macro_derive(GraphQLView, attributes(view))]
pub fn derive_graphql_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    generate_graphql_code(input).into()
}

#[cfg(test)]
pub mod tests {

    use crate::*;
    use quote::quote;
    use syn::parse_quote;

    fn assert_eq_no_whitespace(mut actual: String, mut expected: String) {
        // Intentionally left here for debugging purposes
        println!("{}", actual);

        actual.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_generate_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            let output = generate_view_code(input, true);

            let SpecificContextInfo {
                context,
                constraints,
                generics,
                ..
            } = context;

            let expected = quote!(
                #[async_trait::async_trait]
                impl #generics linera_views::views::View<#context> for TestView #generics
                #constraints
                {
                    fn context(&self) -> &#context {
                        use linera_views::views::View;
                        self.register.context()
                    }
                    async fn load(
                        context: #context
                    ) -> Result<Self, linera_views::views::ViewError> {
                        use linera_views::{futures::join, common::Context};
                        linera_views::increment_counter(
                            linera_views::LOAD_VIEW_COUNTER,
                            stringify!(TestView),
                            &context.base_key(),
                        );
                        let index = 0;
                        let base_key = context.derive_key(&index)?;
                        let register_fut =
                            RegisterView::load(context.clone_with_base_key(base_key));
                        let index = 1;
                        let base_key = context.derive_key(&index)?;
                        let collection_fut =
                            CollectionView::load(context.clone_with_base_key(base_key));
                        let result = join!(register_fut, collection_fut);
                        let register = result.0?;
                        let collection = result.1?;
                        Ok(Self {
                            register,
                            collection
                        })
                    }
                    fn rollback(&mut self) {
                        self.register.rollback();
                        self.collection.rollback();
                    }
                    fn flush(
                        &mut self,
                        batch: &mut linera_views::batch::Batch
                    ) -> Result<(), linera_views::views::ViewError> {
                        use linera_views::views::View;
                        self.register.flush(batch)?;
                        self.collection.flush(batch)?;
                        Ok(())
                    }
                    fn delete(self, batch: &mut linera_views::batch::Batch) {
                        use linera_views::views::View;
                        self.register.delete(batch);
                        self.collection.delete(batch);
                    }
                    fn clear(&mut self) {
                        self.register.clear();
                        self.collection.clear();
                    }
                }
            );

            assert_eq!(output.to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_generate_hash_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            let output = generate_hash_view_code(input);

            let SpecificContextInfo {
                context,
                constraints,
                generics,
                ..
            } = context;

            let expected = quote!(
                #[async_trait::async_trait]
                impl #generics linera_views::views::HashableView<#context> for TestView #generics
                #constraints
                {
                    type Hasher = linera_views::sha3::Sha3_256;
                    async fn hash_mut(
                        &mut self
                    ) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output,
                        linera_views::views::ViewError
                    > {
                        use linera_views::views::{Hasher, HashableView};
                        use std::io::Write;
                        let mut hasher = Self::Hasher::default();
                        hasher.write_all(self.register.hash_mut().await?.as_ref())?;
                        hasher.write_all(self.collection.hash_mut().await?.as_ref())?;
                        Ok(hasher.finalize())
                    }
                    async fn hash(
                        &self
                    ) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output,
                        linera_views::views::ViewError
                    > {
                        use linera_views::views::{Hasher, HashableView};
                        use std::io::Write;
                        let mut hasher = Self::Hasher::default();
                        hasher.write_all(self.register.hash().await?.as_ref())?;
                        hasher.write_all(self.collection.hash().await?.as_ref())?;
                        Ok(hasher.finalize())
                    }
                }
            );

            assert_eq!(output.to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_generate_save_delete_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            let output = generate_save_delete_view_code(input);

            let SpecificContextInfo {
                context,
                constraints,
                generics,
                ..
            } = context;

            let expected = quote!(
                #[async_trait::async_trait]
                impl #generics linera_views::views::RootView<#context> for TestView #generics
                #constraints
                {
                    async fn save(&mut self) -> Result<(), linera_views::views::ViewError> {
                        use linera_views::{common::Context, batch::Batch, views::View};
                        linera_views::increment_counter(
                            linera_views::SAVE_VIEW_COUNTER,
                            stringify!(TestView),
                            &self.context().base_key(),
                        );
                        let mut batch = Batch::new();
                        self.register.flush(&mut batch)?;
                        self.collection.flush(&mut batch)?;
                        self.context().write_batch(batch).await?;
                        Ok(())
                    }
                    async fn write_delete(self) -> Result<(), linera_views::views::ViewError> {
                        use linera_views::{common::Context, batch::Batch, views::View};
                        let context = self.context().clone();
                        let batch = Batch::build(move |batch| {
                            Box::pin(async move {
                                self.register.delete(batch);
                                self.collection.delete(batch);
                                Ok(())
                            })
                        })
                        .await?;
                        context.write_batch(batch).await?;
                        Ok(())
                    }
                }
            );

            assert_eq!(output.to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_generate_crypto_hash_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            let output = generate_crypto_hash_code(input);

            let SpecificContextInfo {
                context,
                constraints,
                generics,
                ..
            } = context;

            let expected = quote!(
                #[async_trait::async_trait]
                impl #generics linera_views::views::CryptoHashView<#context> for TestView #generics
                #constraints
                {
                    async fn crypto_hash(
                        &self
                    ) -> Result<linera_base::crypto::CryptoHash, linera_views::views::ViewError>
                    {
                        use linera_base::crypto::{BcsHashable, CryptoHash};
                        use linera_views::{
                            batch::Batch,
                            generic_array::GenericArray,
                            sha3::{digest::OutputSizeUser, Sha3_256},
                            views::HashableView,
                        };
                        use serde::{Serialize, Deserialize};
                        #[derive(Serialize, Deserialize)]
                        struct TestViewHash(GenericArray<u8, <Sha3_256 as OutputSizeUser>::OutputSize>);
                        impl BcsHashable for TestViewHash {}
                        let hash = self.hash().await?;
                        Ok(CryptoHash::new(&TestViewHash(hash)))
                    }
                }
            );

            assert_eq!(output.to_string(), expected.to_string());
        }
    }

    #[test]
    fn test_generate_graphql_code() {
        for context in SpecificContextInfo::test_cases() {
            let SpecificContextInfo {
                attribute,
                context,
                constraints,
                generics,
                generics_with_lifetime,
            } = context;

            let explicit_context_cases = if attribute.is_some() {
                [true, false].iter()
            } else {
                [true].iter()
            }
            .copied();

            for with_explicit_context in explicit_context_cases {
                let maybe_context = with_explicit_context.then(|| quote! { #context, });

                let input: ItemStruct = parse_quote!(
                    #attribute
                    struct TestView #generics {
                        raw: String,
                        register: RegisterView<#maybe_context Option<usize>>,
                        collection: CollectionView<#maybe_context String, SomeOtherView<#context>>,
                        set: SetView<#maybe_context HashSet<usize>>,
                        log: LogView<#maybe_context usize>,
                        queue: QueueView<#maybe_context usize>,
                        map: MapView<#maybe_context String, usize>
                    }
                );

                let output = generate_graphql_code(input);

                let expected = quote! {
                    pub struct TestViewCollectionEntry #generics_with_lifetime
                    #constraints
                    {
                        string: String,
                        guard: linera_views::collection_view::ReadGuardedView<
                            'a,
                            SomeOtherView<#context>
                        >,
                    }
                    #[async_graphql::Object]
                    impl #generics_with_lifetime TestViewCollectionEntry #generics_with_lifetime
                    #constraints
                    {
                        async fn string(&self) -> &String {
                            &self.string
                        }
                        async fn some_other_view(&self) -> &SomeOtherView<#context> {
                            use std::ops::Deref;
                            self.guard.deref()
                        }
                    }
                    #[async_graphql::Object]
                    impl #generics TestView #generics
                    #constraints
                    {
                        async fn raw(&self) -> &String {
                            &self.raw
                        }

                        async fn register(&self) -> &Option<usize> {
                            self.register.get()
                        }

                        async fn collection(
                            &self,
                            string: String,
                        ) -> Result<TestViewCollectionEntry #generics, async_graphql::Error> {
                            Ok(TestViewCollectionEntry {
                                string: string.clone(),
                                guard: self.collection.try_load_entry(&string).await?,
                            })
                        }

                        async fn set(&self) -> Result<Vec<HashSet<usize>>, async_graphql::Error> {
                            Ok(self.set.indices().await?)
                        }

                        async fn log(
                            &self,
                            start: Option<usize>,
                            end: Option<usize>
                        ) -> Result<Vec<usize>, async_graphql::Error> {
                            let range = std::ops::Range {
                                start: start.unwrap_or(0),
                                end: end.unwrap_or(self.log.count()),
                            };
                            Ok(self.log.read(range).await?)
                        }

                        async fn queue(
                            &self,
                            count: Option<usize>
                        ) -> Result<Vec<usize>, async_graphql::Error> {
                            let count = count.unwrap_or_else(|| self.queue.count());
                            Ok(self.queue.read_front(count).await?)
                        }

                        async fn map(
                            &self,
                            string: String
                        ) -> Result<Option<usize>, async_graphql::Error> {
                            Ok(self.map.get(&string).await?)
                        }

                        async fn map_keys(
                            &self,
                            count: Option<u64>
                        ) -> Result<Vec<String>, async_graphql::Error> {
                            let count = count.unwrap_or(u64::MAX).try_into().unwrap_or(usize::MAX);
                            let mut keys = vec![];
                            if count == 0 {
                                return Ok(keys);
                            }
                            self.map.for_each_index_while(|key| {
                                keys.push(key);
                                Ok(keys.len() < count)
                            }).await?;
                            Ok(keys)
                        }
                    }
                };

                assert_eq_no_whitespace(output.to_string(), expected.to_string())
            }
        }
    }

    pub struct SpecificContextInfo {
        attribute: Option<TokenStream2>,
        context: Type,
        generics: TokenStream2,
        generics_with_lifetime: TokenStream2,
        constraints: TokenStream2,
    }

    impl SpecificContextInfo {
        pub fn empty() -> Self {
            SpecificContextInfo {
                attribute: None,
                context: syn::parse_str("C").unwrap(),
                generics: quote! { <C> },
                generics_with_lifetime: quote! { <'a, C> },
                constraints: quote! {
                    where
                        C: linera_views::common::Context + Send + Sync + Clone + 'static,
                        linera_views::views::ViewError: From<C::Error>,
                },
            }
        }

        pub fn new(context: &str) -> Self {
            SpecificContextInfo {
                attribute: Some(quote! { #[view(context = #context)] }),
                context: syn::parse_str(context).unwrap(),
                generics: quote! {},
                generics_with_lifetime: quote! { <'a,> },
                constraints: quote! {},
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
