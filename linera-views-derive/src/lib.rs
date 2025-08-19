// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-views`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{parse_macro_input, parse_quote, ItemStruct, Type};

#[derive(Debug, deluxe::ParseAttributes)]
#[deluxe(attributes(view))]
struct StructAttrs {
    context: Option<syn::Type>,
}

struct Constraints<'a> {
    input_constraints: Vec<&'a syn::WherePredicate>,
    impl_generics: syn::ImplGenerics<'a>,
    type_generics: syn::TypeGenerics<'a>,
}

impl<'a> Constraints<'a> {
    fn get(item: &'a syn::ItemStruct) -> Self {
        let (impl_generics, type_generics, maybe_where_clause) = item.generics.split_for_impl();
        let input_constraints = maybe_where_clause
            .map(|w| w.predicates.iter())
            .into_iter()
            .flatten()
            .collect();

        Self {
            input_constraints,
            impl_generics,
            type_generics,
        }
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
    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);

    let attrs: StructAttrs = deluxe::parse_attributes(&input).unwrap();
    let context = attrs.context.unwrap_or_else(|| {
        let ident = &input
            .generics
            .type_params()
            .next()
            .expect("no `context` given and no type parameters")
            .ident;
        parse_quote! { #ident }
    });

    let struct_name = &input.ident;
    let field_types: Vec<_> = input.fields.iter().map(|field| &field.ty).collect();

    let mut name_quotes = Vec::new();
    let mut rollback_quotes = Vec::new();
    let mut flush_quotes = Vec::new();
    let mut test_flush_quotes = Vec::new();
    let mut clear_quotes = Vec::new();
    let mut has_pending_changes_quotes = Vec::new();
    let mut num_init_keys_quotes = Vec::new();
    let mut pre_load_keys_quotes = Vec::new();
    let mut post_load_keys_quotes = Vec::new();
    for (idx, e) in input.fields.iter().enumerate() {
        let name = e.ident.clone().unwrap();
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
            let base_key = context.base_key().derive_tag_key(linera_views::views::MIN_VIEW_TAG, &index)?;
            keys.extend(#g :: pre_load(&context.clone_with_base_key(base_key))?);
        });
        post_load_keys_quotes.push(quote! {
            let index = #idx_lit;
            let pos_next = pos + #g :: NUM_INIT_KEYS;
            let base_key = context.base_key().derive_tag_key(linera_views::views::MIN_VIEW_TAG, &index)?;
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
                &context.base_key().bytes,
            );
            #[cfg(not(target_arch = "wasm32"))]
            use linera_views::metrics::prometheus_util::MeasureLatency as _;
            let _latency = linera_views::metrics::LOAD_VIEW_LATENCY.measure_latency();
        }
    } else {
        quote! {}
    };

    quote! {
        impl #impl_generics linera_views::views::View for #struct_name #type_generics
        where
            #context: linera_views::context::Context,
            #(#input_constraints,)*
            #(#field_types: linera_views::views::View<Context = #context>,)*
        {
            const NUM_INIT_KEYS: usize = #(<#field_types as linera_views::views::View>::NUM_INIT_KEYS)+*;

            type Context = #context;

            fn context(&self) -> &#context {
                use linera_views::views::View;
                self.#first_name_quote.context()
            }

            fn pre_load(context: &#context) -> Result<Vec<Vec<u8>>, linera_views::ViewError> {
                use linera_views::context::Context as _;
                let mut keys = Vec::new();
                #(#pre_load_keys_quotes)*
                Ok(keys)
            }

            fn post_load(context: #context, values: &[Option<Vec<u8>>]) -> Result<Self, linera_views::ViewError> {
                use linera_views::context::Context as _;
                let mut pos = 0;
                #(#post_load_keys_quotes)*
                Ok(Self {#(#name_quotes),*})
            }

            async fn load(context: #context) -> Result<Self, linera_views::ViewError> {
                use linera_views::{context::Context as _, store::ReadableKeyValueStore as _};
                #load_metrics
                if Self::NUM_INIT_KEYS == 0 {
                    Self::post_load(context, &[])
                } else {
                    let keys = Self::pre_load(&context)?;
                    let values = context.store().read_multi_values_bytes(keys).await?;
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

            fn flush(&mut self, batch: &mut linera_views::batch::Batch) -> Result<bool, linera_views::ViewError> {
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

fn generate_root_view_code(input: ItemStruct) -> TokenStream2 {
    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);
    let struct_name = &input.ident;

    let increment_counter = if cfg!(feature = "metrics") {
        quote! {
            #[cfg(not(target_arch = "wasm32"))]
            linera_views::metrics::increment_counter(
                &linera_views::metrics::SAVE_VIEW_COUNTER,
                stringify!(#struct_name),
                &self.context().base_key().bytes,
            );
        }
    } else {
        quote! {}
    };

    quote! {
        impl #impl_generics linera_views::views::RootView for #struct_name #type_generics
        where
            #(#input_constraints,)*
            Self: linera_views::views::View,
        {
            async fn save(&mut self) -> Result<(), linera_views::ViewError> {
                use linera_views::{context::Context, batch::Batch, store::WritableKeyValueStore as _, views::View};
                #increment_counter
                let mut batch = Batch::new();
                self.flush(&mut batch)?;
                if !batch.is_empty() {
                    self.context().store().write_batch(batch).await?;
                }
                Ok(())
            }
        }
    }
}

fn generate_hash_view_code(input: ItemStruct) -> TokenStream2 {
    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);
    let struct_name = &input.ident;

    let field_types = input.fields.iter().map(|field| &field.ty);
    let mut field_hashes_mut = Vec::new();
    let mut field_hashes = Vec::new();
    for e in &input.fields {
        let name = e.ident.as_ref().unwrap();
        field_hashes_mut.push(quote! { hasher.write_all(self.#name.hash_mut().await?.as_ref())?; });
        field_hashes.push(quote! { hasher.write_all(self.#name.hash().await?.as_ref())?; });
    }

    quote! {
        impl #impl_generics linera_views::views::HashableView for #struct_name #type_generics
        where
            #(#field_types: linera_views::views::HashableView,)*
            #(#input_constraints,)*
            Self: linera_views::views::View,
        {
            type Hasher = linera_views::sha3::Sha3_256;

            async fn hash_mut(&mut self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::ViewError> {
                use linera_views::views::{Hasher, HashableView};
                use std::io::Write;
                let mut hasher = Self::Hasher::default();
                #(#field_hashes_mut)*
                Ok(hasher.finalize())
            }

            async fn hash(&self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::ViewError> {
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
    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);
    let field_types = input.fields.iter().map(|field| &field.ty);
    let struct_name = &input.ident;
    let hash_type = syn::Ident::new(&format!("{struct_name}Hash"), Span::call_site());
    quote! {
        impl #impl_generics linera_views::views::CryptoHashView
        for #struct_name #type_generics
        where
            #(#field_types: linera_views::views::HashableView,)*
            #(#input_constraints,)*
            Self: linera_views::views::View,
        {
            async fn crypto_hash(&self) -> Result<linera_base::crypto::CryptoHash, linera_views::ViewError> {
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

            async fn crypto_hash_mut(&mut self) -> Result<linera_base::crypto::CryptoHash, linera_views::ViewError> {
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
    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);
    let struct_name = &input.ident;

    let mut clone_constraints = vec![];
    let mut clone_fields = vec![];

    for field in &input.fields {
        let name = &field.ident;
        let ty = &field.ty;
        clone_constraints.push(quote! { #ty: ClonableView });
        clone_fields.push(quote! { #name: self.#name.clone_unchecked() });
    }

    quote! {
        impl #impl_generics linera_views::views::ClonableView for #struct_name #type_generics
        where
            #(#input_constraints,)*
            #(#clone_constraints,)*
            Self: linera_views::views::View,
        {
            fn clone_unchecked(&mut self) -> Self {
                Self {
                    #(#clone_fields,)*
                }
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
    stream.extend(generate_root_view_code(input));
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
    stream.extend(generate_root_view_code(input.clone()));
    stream.extend(generate_hash_view_code(input.clone()));
    stream.extend(generate_crypto_hash_code(input));
    stream.into()
}

#[proc_macro_derive(HashableRootView, attributes(view))]
#[cfg(test)]
pub fn derive_hashable_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let mut stream = generate_view_code(input.clone(), true);
    stream.extend(generate_root_view_code(input.clone()));
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
    fn test_generate_root_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(
                format!(
                    "test_generate_root_view_code{}_{}",
                    if cfg!(feature = "metrics") {
                        "_metrics"
                    } else {
                        ""
                    },
                    context.name,
                ),
                pretty(generate_root_view_code(input))
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
                context: syn::parse_quote! { C },
                generics: syn::parse_quote! { <C> },
                where_clause: None,
            }
        }

        pub fn new(context: syn::Type) -> Self {
            let name = quote! { #context };
            SpecificContextInfo {
                name: format!("{name}")
                    .replace(' ', "")
                    .replace([':', '<', '>'], "_"),
                attribute: Some(quote! { #[view(context = #context)] }),
                context,
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
                        syn::parse_quote! { CustomContext },
                        syn::parse_quote! { custom::path::to::ContextType },
                        syn::parse_quote! { custom::GenericContext<T> },
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
