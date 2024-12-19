// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-views`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, parse_quote, punctuated::Punctuated, Attribute, ItemStruct, Lit, LitStr,
    MetaNameValue, Token, Type, TypePath, WhereClause,
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
) -> (Type, WhereClause) {
    let context;
    let constraints;

    if let Some(context_literal) = custom_attribute(attributes, "context") {
        context = context_literal.parse().expect("Invalid context");
        constraints = empty_where_clause();
    } else {
        context = Type::Path(TypePath {
            qself: None,
            path: template_vect
                .first()
                .expect("failed to find the first generic parameter")
                .clone()
                .into(),
        });
        constraints = parse_quote! {
            where
                #context: linera_views::context::Context + Send + Sync + Clone + 'static,
        };
    }

    (context, constraints)
}

/// Returns an empty [`WhereClause`].
fn empty_where_clause() -> WhereClause {
    WhereClause {
        where_token: Token![where](Span::call_site()),
        predicates: Punctuated::new(),
    }
}

fn get_extended_entry(e: Type) -> TokenStream2 {
    let syn::Type::Path(typepath) = e else {
        panic!("The type should be a path");
    };
    let path_segment = typepath.path.segments.into_iter().next().unwrap();
    let ident = path_segment.ident;
    let arguments = path_segment.arguments;
    quote! { #ident :: #arguments }
}

fn generate_view_code(input: ItemStruct, root: bool) -> TokenStream2 {
    let struct_name = input.ident;
    let (impl_generics, type_generics, maybe_where_clause) = input.generics.split_for_impl();
    let template_vect = get_seq_parameter(input.generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut where_clause = maybe_where_clause
        .cloned()
        .unwrap_or_else(empty_where_clause);
    where_clause
        .predicates
        .extend(context_constraints.predicates);

    let mut name_quotes = Vec::new();
    let mut rollback_quotes = Vec::new();
    let mut flush_quotes = Vec::new();
    let mut test_flush_quotes = Vec::new();
    let mut clear_quotes = Vec::new();
    let mut has_pending_changes_quotes = Vec::new();
    let mut num_init_keys_quotes = Vec::new();
    let mut pre_load_keys_quotes = Vec::new();
    let mut post_load_keys_quotes = Vec::new();
    for (idx, e) in input.fields.into_iter().enumerate() {
        let name = e.clone().ident.unwrap();
        let test_flush_ident = format_ident!("deleted{}", idx);
        let idx_lit = syn::LitInt::new(&idx.to_string(), Span::call_site());
        let g = get_extended_entry(e.ty.clone());
        name_quotes.push(quote! { #name });
        rollback_quotes.push(quote! { self.#name.rollback(); });
        flush_quotes.push(quote! { let #test_flush_ident = self.#name.flush(batch)?; });
        test_flush_quotes.push(quote! { #test_flush_ident });
        clear_quotes.push(quote! { self.#name.clear(); });
        has_pending_changes_quotes.push(quote! {
            if self.#name.has_pending_changes().await {
                return true;
            }
        });
        num_init_keys_quotes.push(quote! { #g :: NUM_INIT_KEYS });
        pre_load_keys_quotes.push(quote! {
            let index = #idx_lit;
            let base_key = context.derive_tag_key(linera_views::views::MIN_VIEW_TAG, &index)?;
            keys.extend(#g :: pre_load(&context.clone_with_base_key(base_key))?);
        });
        post_load_keys_quotes.push(quote! {
            let index = #idx_lit;
            let pos_next = pos + #g :: NUM_INIT_KEYS;
            let base_key = context.derive_tag_key(linera_views::views::MIN_VIEW_TAG, &index)?;
            let #name = #g :: post_load(context.clone_with_base_key(base_key), &values[pos..pos_next])?;
            pos = pos_next;
        });
    }
    let first_name_quote = name_quotes
        .first()
        .expect("list of names should be non-empty");

    let load_metrics = if root && cfg!(feature = "metrics") {
        quote! {
            #[cfg(not(target_arch = "wasm32"))]
            linera_views::metrics::increment_counter(
                &linera_views::metrics::LOAD_VIEW_COUNTER,
                stringify!(#struct_name),
                &context.base_key(),
            );
            #[cfg(not(target_arch = "wasm32"))]
            use linera_views::metrics::prometheus_util::MeasureLatency as _;
            let _latency = linera_views::metrics::LOAD_VIEW_LATENCY.measure_latency();
        }
    } else {
        quote! {}
    };

    quote! {
        #[linera_views::async_trait]
        impl #impl_generics linera_views::views::View<#context> for #struct_name #type_generics
        #where_clause
        {
            const NUM_INIT_KEYS: usize = #(#num_init_keys_quotes)+*;

            fn context(&self) -> &#context {
                use linera_views::views::View;
                self.#first_name_quote.context()
            }

            fn pre_load(context: &#context) -> Result<Vec<Vec<u8>>, linera_views::views::ViewError> {
                use linera_views::context::Context as _;
                let mut keys = Vec::new();
                #(#pre_load_keys_quotes)*
                Ok(keys)
            }

            fn post_load(context: #context, values: &[Option<Vec<u8>>]) -> Result<Self, linera_views::views::ViewError> {
                use linera_views::context::Context as _;
                let mut pos = 0;
                #(#post_load_keys_quotes)*
                Ok(Self {#(#name_quotes),*})
            }

            async fn load(context: #context) -> Result<Self, linera_views::views::ViewError> {
                use linera_views::context::Context as _;
                #load_metrics
                if Self::NUM_INIT_KEYS == 0 {
                    Self::post_load(context, &[])
                } else {
                    let keys = Self::pre_load(&context)?;
                    let values = context.read_multi_values_bytes(keys).await?;
                    Self::post_load(context, &values)
                }
            }


            fn rollback(&mut self) {
                #(#rollback_quotes)*
            }

            async fn has_pending_changes(&self) -> bool {
                #(#has_pending_changes_quotes)*
                false
            }

            fn flush(&mut self, batch: &mut linera_views::batch::Batch) -> Result<bool, linera_views::views::ViewError> {
                use linera_views::views::View;
                #(#flush_quotes)*
                Ok( #(#test_flush_quotes)&&* )
            }

            fn clear(&mut self) {
                #(#clear_quotes)*
            }
        }
    }
}

fn generate_save_delete_view_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let (impl_generics, type_generics, maybe_where_clause) = input.generics.split_for_impl();
    let template_vect = get_seq_parameter(input.generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut where_clause = maybe_where_clause
        .cloned()
        .unwrap_or_else(empty_where_clause);
    where_clause
        .predicates
        .extend(context_constraints.predicates);

    let mut flushes = Vec::new();
    let mut deletes = Vec::new();
    for e in input.fields {
        let name = e.clone().ident.unwrap();
        flushes.push(quote! { self.#name.flush(&mut batch)?; });
        deletes.push(quote! { self.#name.delete(batch); });
    }

    let increment_counter = if cfg!(feature = "metrics") {
        quote! {
            #[cfg(not(target_arch = "wasm32"))]
            linera_views::metrics::increment_counter(
                &linera_views::metrics::SAVE_VIEW_COUNTER,
                stringify!(#struct_name),
                &self.context().base_key(),
            );
        }
    } else {
        quote! {}
    };

    quote! {
        #[linera_views::async_trait]
        impl #impl_generics linera_views::views::RootView<#context> for #struct_name #type_generics
        #where_clause
        {
            async fn save(&mut self) -> Result<(), linera_views::views::ViewError> {
                use linera_views::{context::Context, batch::Batch, views::View};
                #increment_counter
                let mut batch = Batch::new();
                #(#flushes)*
                if !batch.is_empty() {
                    self.context().write_batch(batch).await?;
                }
                Ok(())
            }
        }
    }
}

fn generate_hash_view_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let (impl_generics, type_generics, maybe_where_clause) = input.generics.split_for_impl();
    let template_vect = get_seq_parameter(input.generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut where_clause = maybe_where_clause
        .cloned()
        .unwrap_or_else(empty_where_clause);
    where_clause
        .predicates
        .extend(context_constraints.predicates);

    let mut field_hashes_mut = Vec::new();
    let mut field_hashes = Vec::new();
    for e in input.fields {
        let name = e.clone().ident.unwrap();
        field_hashes_mut.push(quote! { hasher.write_all(self.#name.hash_mut().await?.as_ref())?; });
        field_hashes.push(quote! { hasher.write_all(self.#name.hash().await?.as_ref())?; });
    }

    quote! {
        #[linera_views::async_trait]
        impl #impl_generics linera_views::views::HashableView<#context> for #struct_name #type_generics
        #where_clause
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
    let (impl_generics, type_generics, maybe_where_clause) = input.generics.split_for_impl();
    let template_vect = get_seq_parameter(input.generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let mut where_clause = maybe_where_clause
        .cloned()
        .unwrap_or_else(empty_where_clause);
    where_clause
        .predicates
        .extend(context_constraints.predicates);

    let hash_type = syn::Ident::new(&format!("{}Hash", struct_name), Span::call_site());
    quote! {
        #[linera_views::async_trait]
        impl #impl_generics linera_views::views::CryptoHashView<#context>
            for #struct_name #type_generics
        #where_clause
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
                impl<'de> BcsHashable<'de> for #hash_type {}
                let hash = self.hash().await?;
                Ok(CryptoHash::new(&#hash_type(hash)))
            }

            async fn crypto_hash_mut(&mut self) -> Result<linera_base::crypto::CryptoHash, linera_views::views::ViewError> {
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
                impl<'de> BcsHashable<'de> for #hash_type {}
                let hash = self.hash_mut().await?;
                Ok(CryptoHash::new(&#hash_type(hash)))
            }
        }
    }
}

fn generate_clonable_view_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());

    let (context, context_constraints) = context_and_constraints(&input.attrs, &template_vect);

    let clone_unchecked_quotes = input.fields.iter().map(|field| {
        let name = &field.ident;
        quote! { #name: self.#name.clone_unchecked()?, }
    });

    quote! {
        impl #generics linera_views::views::ClonableView<#context> for #struct_name #generics
        #context_constraints
        {
            fn clone_unchecked(&mut self) -> Result<Self, linera_views::views::ViewError> {
                Ok(Self {
                    #(#clone_unchecked_quotes)*
                })
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

#[proc_macro_derive(ClonableView, attributes(view))]
pub fn derive_clonable_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    generate_clonable_view_code(input).into()
}

#[cfg(test)]
pub mod tests {

    use quote::quote;
    use syn::{parse_quote, AngleBracketedGenericArguments};

    use crate::*;

    fn pretty(tokens: TokenStream2) -> String {
        prettyplease::unparse(
            &syn::parse2::<syn::File>(tokens).expect("failed to parse test output"),
        )
    }

    #[test]
    fn test_generate_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(
                format!(
                    "test_generate_view_code{}_{}",
                    if cfg!(feature = "metrics") {
                        "_metrics"
                    } else {
                        ""
                    },
                    context.name,
                ),
                pretty(generate_view_code(input, true))
            );
        }
    }

    #[test]
    fn test_generate_hash_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(
                format!("test_generate_hash_view_code_{}", context.name),
                pretty(generate_hash_view_code(input))
            );
        }
    }

    #[test]
    fn test_generate_save_delete_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(
                format!(
                    "test_generate_save_delete_view_code{}_{}",
                    if cfg!(feature = "metrics") {
                        "_metrics"
                    } else {
                        ""
                    },
                    context.name,
                ),
                pretty(generate_save_delete_view_code(input))
            );
        }
    }

    #[test]
    fn test_generate_crypto_hash_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(pretty(generate_crypto_hash_code(input)));
        }
    }

    #[test]
    fn test_generate_clonable_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(pretty(generate_clonable_view_code(input)));
        }
    }

    #[derive(Clone)]
    pub struct SpecificContextInfo {
        name: String,
        attribute: Option<TokenStream2>,
        context: Type,
        generics: AngleBracketedGenericArguments,
        where_clause: Option<TokenStream2>,
    }

    impl SpecificContextInfo {
        pub fn empty() -> Self {
            SpecificContextInfo {
                name: "C".to_string(),
                attribute: None,
                context: syn::parse_str("C").unwrap(),
                generics: parse_quote! { <C> },
                where_clause: None,
            }
        }

        pub fn new(context: &str) -> Self {
            SpecificContextInfo {
                name: context.replace([':', '<', '>'], "_"),
                attribute: Some(quote! { #[view(context = #context)] }),
                context: syn::parse_str(context).unwrap(),
                generics: parse_quote! { <> },
                where_clause: None,
            }
        }

        /// Sets the `where_clause` to a dummy value for test cases with a where clause.
        ///
        /// Also adds a `MyParam` generic type parameter to the `generics` field, which is the type
        /// constrained by the dummy predicate in the `where_clause`.
        pub fn with_dummy_where_clause(mut self) -> Self {
            self.generics.args.push(parse_quote! { MyParam });
            self.where_clause = Some(quote! {
                where MyParam: Send + Sync + 'static,
            });
            self.name.push_str("_with_where");

            self
        }

        pub fn test_cases() -> impl Iterator<Item = Self> {
            Some(Self::empty())
                .into_iter()
                .chain(
                    [
                        "CustomContext",
                        "custom::path::to::ContextType",
                        "custom::GenericContext<T>",
                    ]
                    .into_iter()
                    .map(Self::new),
                )
                .flat_map(|case| [case.clone(), case.with_dummy_where_clause()])
        }

        pub fn test_view_input(&self) -> ItemStruct {
            let SpecificContextInfo {
                attribute,
                context,
                generics,
                where_clause,
                ..
            } = self;

            parse_quote! {
                #attribute
                struct TestView #generics
                #where_clause
                {
                    register: RegisterView<#context, usize>,
                    collection: CollectionView<#context, usize, RegisterView<#context, usize>>,
                }
            }
        }
    }
}
