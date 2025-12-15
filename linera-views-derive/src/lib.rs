// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-views`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::{parse_macro_input, parse_quote, Error, ItemStruct, Type};

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

fn get_extended_entry(e: Type) -> Result<TokenStream2, Error> {
    let syn::Type::Path(typepath) = &e else {
        return Err(Error::new_spanned(e, "Expected a path type"));
    };
    let Some(path_segment) = typepath.path.segments.first() else {
        return Err(Error::new_spanned(&typepath.path, "Path has no segments"));
    };
    let ident = &path_segment.ident;
    let arguments = &path_segment.arguments;
    Ok(quote! { #ident :: #arguments })
}

fn generate_view_code(input: ItemStruct, root: bool) -> Result<TokenStream2, Error> {
    // Validate that all fields are named
    for field in &input.fields {
        if field.ident.is_none() {
            return Err(Error::new_spanned(field, "All fields must be named."));
        }
    }

    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);

    let attrs: StructAttrs = deluxe::parse_attributes(&input)
        .map_err(|e| Error::new_spanned(&input, format!("Failed to parse attributes: {e}")))?;
    let context = attrs.context.or_else(|| {
        input.generics.type_params().next().map(|param| {
            let ident = &param.ident;
            parse_quote! { #ident }
        })
    }).ok_or_else(|| {
        Error::new_spanned(
            &input,
            "Missing context: either add a generic type parameter or specify the context with #[view(context = YourContextType)]"
        )
    })?;

    let struct_name = &input.ident;
    let field_types: Vec<_> = input.fields.iter().map(|field| &field.ty).collect();

    let mut name_quotes = Vec::new();
    let mut rollback_quotes = Vec::new();
    let mut pre_save_quotes = Vec::new();
    let mut delete_view_quotes = Vec::new();
    let mut clear_quotes = Vec::new();
    let mut has_pending_changes_quotes = Vec::new();
    let mut num_init_keys_quotes = Vec::new();
    let mut pre_load_keys_quotes = Vec::new();
    let mut post_load_keys_quotes = Vec::new();
    let num_fields = input.fields.len();
    for (idx, e) in input.fields.iter().enumerate() {
        let name = e.ident.clone().unwrap();
        let delete_view_ident = format_ident!("deleted{}", idx);
        let g = get_extended_entry(e.ty.clone())?;
        name_quotes.push(quote! { #name });
        rollback_quotes.push(quote! { self.#name.rollback(); });
        pre_save_quotes.push(quote! { let #delete_view_ident = self.#name.pre_save(batch)?; });
        delete_view_quotes.push(quote! { #delete_view_ident });
        clear_quotes.push(quote! { self.#name.clear(); });
        has_pending_changes_quotes.push(quote! {
            if self.#name.has_pending_changes().await {
                return true;
            }
        });
        num_init_keys_quotes.push(quote! { #g :: NUM_INIT_KEYS });

        let derive_key_logic = if num_fields < 256 {
            let idx_u8 = idx as u8;
            quote! {
                let __linera_reserved_index = #idx_u8;
                let __linera_reserved_base_key = context.base_key().derive_tag_key(linera_views::views::MIN_VIEW_TAG, &__linera_reserved_index)?;
            }
        } else {
            assert!(num_fields < 65536);
            let idx_u16 = idx as u16;
            quote! {
                let __linera_reserved_index = #idx_u16;
                let __linera_reserved_base_key = context.base_key().derive_tag_key(linera_views::views::MIN_VIEW_TAG, &__linera_reserved_index)?;
            }
        };

        pre_load_keys_quotes.push(quote! {
            #derive_key_logic
            keys.extend(#g :: pre_load(&context.clone_with_base_key(__linera_reserved_base_key))?);
        });
        post_load_keys_quotes.push(quote! {
            #derive_key_logic
            let __linera_reserved_pos_next = __linera_reserved_pos + #g :: NUM_INIT_KEYS;
            let #name = #g :: post_load(context.clone_with_base_key(__linera_reserved_base_key), &values[__linera_reserved_pos..__linera_reserved_pos_next])?;
            __linera_reserved_pos = __linera_reserved_pos_next;
        });
    }

    // derive_key_logic above adds one byte to the key as a tag, and then either one or two more
    // bytes for field indices, depending on how many fields there are. Thus, we need to trim 2
    // bytes if there are less than 256 child fields (then the field index fits within one byte),
    // or 3 bytes if there are more.
    let trim_key_logic = if num_fields < 256 {
        quote! {
            let __bytes_to_trim = 2;
        }
    } else {
        quote! {
            let __bytes_to_trim = 3;
        }
    };

    let first_name_quote = name_quotes.first().ok_or(Error::new_spanned(
        &input,
        "Struct must have at least one field",
    ))?;

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

    Ok(quote! {
        impl #impl_generics linera_views::views::View for #struct_name #type_generics
        where
            #context: linera_views::context::Context,
            #(#input_constraints,)*
            #(#field_types: linera_views::views::View<Context = #context>,)*
        {
            const NUM_INIT_KEYS: usize = #(<#field_types as linera_views::views::View>::NUM_INIT_KEYS)+*;

            type Context = #context;

            fn context(&self) -> #context {
                use linera_views::{context::Context as _};
                #trim_key_logic
                let context = self.#first_name_quote.context();
                context.clone_with_trimmed_key(__bytes_to_trim)
            }

            fn pre_load(context: &#context) -> Result<Vec<Vec<u8>>, linera_views::ViewError> {
                use linera_views::context::Context as _;
                let mut keys = Vec::new();
                #(#pre_load_keys_quotes)*
                Ok(keys)
            }

            fn post_load(context: #context, values: &[Option<Vec<u8>>]) -> Result<Self, linera_views::ViewError> {
                use linera_views::context::Context as _;
                let mut __linera_reserved_pos = 0;
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
                    let values = context.store().read_multi_values_bytes(&keys).await?;
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

            fn pre_save(&self, batch: &mut linera_views::batch::Batch) -> Result<bool, linera_views::ViewError> {
                #(#pre_save_quotes)*
                Ok( #(#delete_view_quotes)&&* )
            }

            fn post_save(&mut self) {
                #(self.#name_quotes.post_save();)*
            }

            fn clear(&mut self) {
                #(#clear_quotes)*
            }
        }
    })
}

fn generate_root_view_code(input: ItemStruct) -> TokenStream2 {
    let Constraints {
        input_constraints,
        impl_generics,
        type_generics,
    } = Constraints::get(&input);
    let struct_name = &input.ident;

    let metrics_code = if cfg!(feature = "metrics") {
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

    let write_batch_with_metrics = if cfg!(feature = "metrics") {
        quote! {
            if !batch.is_empty() {
                #[cfg(not(target_arch = "wasm32"))]
                let start = std::time::Instant::now();
                self.context().store().write_batch(batch).await?;
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                    linera_views::metrics::SAVE_VIEW_LATENCY
                        .with_label_values(&[stringify!(#struct_name)])
                        .observe(latency_ms);
                }
            }
        }
    } else {
        quote! {
            if !batch.is_empty() {
                self.context().store().write_batch(batch).await?;
            }
        }
    };

    quote! {
        impl #impl_generics linera_views::views::RootView for #struct_name #type_generics
        where
            #(#input_constraints,)*
            Self: linera_views::views::View,
        {
            async fn save(&mut self) -> Result<(), linera_views::ViewError> {
                use linera_views::{context::Context as _, batch::Batch, store::WritableKeyValueStore as _, views::View as _};
                #metrics_code
                let mut batch = Batch::new();
                self.pre_save(&mut batch)?;
                #write_batch_with_metrics
                self.post_save();
                Ok(())
            }
        }
    }
}

fn generate_hash_view_code(input: ItemStruct) -> Result<TokenStream2, Error> {
    // Validate that all fields are named
    for field in &input.fields {
        if field.ident.is_none() {
            return Err(Error::new_spanned(field, "All fields must be named."));
        }
    }

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

    Ok(quote! {
        impl #impl_generics linera_views::views::HashableView for #struct_name #type_generics
        where
            #(#field_types: linera_views::views::HashableView,)*
            #(#input_constraints,)*
            Self: linera_views::views::View,
        {
            type Hasher = linera_views::sha3::Sha3_256;

            async fn hash_mut(&mut self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::ViewError> {
                use linera_views::views::Hasher as _;
                use std::io::Write as _;
                let mut hasher = Self::Hasher::default();
                #(#field_hashes_mut)*
                Ok(hasher.finalize())
            }

            async fn hash(&self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::ViewError> {
                use linera_views::views::Hasher as _;
                use std::io::Write as _;
                let mut hasher = Self::Hasher::default();
                #(#field_hashes)*
                Ok(hasher.finalize())
            }
        }
    })
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
                    generic_array::GenericArray,
                    sha3::{digest::OutputSizeUser, Sha3_256},
                    views::HashableView as _,
                };
                #[derive(serde::Serialize, serde::Deserialize)]
                struct #hash_type(GenericArray<u8, <Sha3_256 as OutputSizeUser>::OutputSize>);
                impl<'de> BcsHashable<'de> for #hash_type {}
                let hash = self.hash().await?;
                Ok(CryptoHash::new(&#hash_type(hash)))
            }

            async fn crypto_hash_mut(&mut self) -> Result<linera_base::crypto::CryptoHash, linera_views::ViewError> {
                use linera_base::crypto::{BcsHashable, CryptoHash};
                use linera_views::{
                    generic_array::GenericArray,
                    sha3::{digest::OutputSizeUser, Sha3_256},
                    views::HashableView as _,
                };
                #[derive(serde::Serialize, serde::Deserialize)]
                struct #hash_type(GenericArray<u8, <Sha3_256 as OutputSizeUser>::OutputSize>);
                impl<'de> BcsHashable<'de> for #hash_type {}
                let hash = self.hash_mut().await?;
                Ok(CryptoHash::new(&#hash_type(hash)))
            }
        }
    }
}

fn generate_clonable_view_code(input: ItemStruct) -> Result<TokenStream2, Error> {
    // Validate that all fields are named
    for field in &input.fields {
        if field.ident.is_none() {
            return Err(Error::new_spanned(field, "All fields must be named."));
        }
    }

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
        clone_fields.push(quote! { #name: self.#name.clone_unchecked()? });
    }

    Ok(quote! {
        impl #impl_generics linera_views::views::ClonableView for #struct_name #type_generics
        where
            #(#input_constraints,)*
            #(#clone_constraints,)*
            Self: linera_views::views::View,
        {
            fn clone_unchecked(&mut self) -> Result<Self, linera_views::ViewError> {
                Ok(Self {
                    #(#clone_fields,)*
                })
            }
        }
    })
}

fn to_token_stream(input: Result<TokenStream2, Error>) -> TokenStream {
    match input {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

#[proc_macro_derive(View, attributes(view))]
pub fn derive_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    let input = generate_view_code(input, false);
    to_token_stream(input)
}

fn derive_hash_view_token_stream2(input: ItemStruct) -> Result<TokenStream2, Error> {
    let mut stream = generate_view_code(input.clone(), false)?;
    stream.extend(generate_hash_view_code(input)?);
    Ok(stream)
}

#[proc_macro_derive(HashableView, attributes(view))]
pub fn derive_hash_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let stream = derive_hash_view_token_stream2(input);
    to_token_stream(stream)
}

fn derive_root_view_token_stream2(input: ItemStruct) -> Result<TokenStream2, Error> {
    let mut stream = generate_view_code(input.clone(), true)?;
    stream.extend(generate_root_view_code(input));
    Ok(stream)
}

#[proc_macro_derive(RootView, attributes(view))]
pub fn derive_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let stream = derive_root_view_token_stream2(input);
    to_token_stream(stream)
}

fn derive_crypto_hash_view_token_stream2(input: ItemStruct) -> Result<TokenStream2, Error> {
    let mut stream = generate_view_code(input.clone(), false)?;
    stream.extend(generate_hash_view_code(input.clone())?);
    stream.extend(generate_crypto_hash_code(input));
    Ok(stream)
}

#[proc_macro_derive(CryptoHashView, attributes(view))]
pub fn derive_crypto_hash_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let stream = derive_crypto_hash_view_token_stream2(input);
    to_token_stream(stream)
}

fn derive_crypto_hash_root_view_token_stream2(input: ItemStruct) -> Result<TokenStream2, Error> {
    let mut stream = generate_view_code(input.clone(), true)?;
    stream.extend(generate_root_view_code(input.clone()));
    stream.extend(generate_hash_view_code(input.clone())?);
    stream.extend(generate_crypto_hash_code(input));
    Ok(stream)
}

#[proc_macro_derive(CryptoHashRootView, attributes(view))]
pub fn derive_crypto_hash_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let stream = derive_crypto_hash_root_view_token_stream2(input);
    to_token_stream(stream)
}

#[cfg(test)]
fn derive_hashable_root_view_token_stream2(input: ItemStruct) -> Result<TokenStream2, Error> {
    let mut stream = generate_view_code(input.clone(), true)?;
    stream.extend(generate_root_view_code(input.clone()));
    stream.extend(generate_hash_view_code(input)?);
    Ok(stream)
}

#[proc_macro_derive(HashableRootView, attributes(view))]
#[cfg(test)]
pub fn derive_hashable_root_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let stream = derive_hashable_root_view_token_stream2(input);
    to_token_stream(stream)
}

#[proc_macro_derive(ClonableView, attributes(view))]
pub fn derive_clonable_view(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    match generate_clonable_view_code(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
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
                pretty(generate_view_code(input, true).unwrap())
            );
        }
    }

    #[test]
    fn test_generate_hash_view_code() {
        for context in SpecificContextInfo::test_cases() {
            let input = context.test_view_input();
            insta::assert_snapshot!(
                format!("test_generate_hash_view_code_{}", context.name),
                pretty(generate_hash_view_code(input).unwrap())
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
            insta::assert_snapshot!(pretty(generate_clonable_view_code(input).unwrap()));
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

    // Failure scenario tests
    #[test]
    fn test_tuple_struct_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C>(RegisterView<C, u64>);
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("All fields must be named"));
    }

    #[test]
    fn test_empty_struct_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {}
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Struct must have at least one field"));
    }

    #[test]
    fn test_missing_context_no_generics_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView {
                register: RegisterView<CustomContext, u64>,
            }
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Missing context"));
    }

    #[test]
    fn test_missing_context_empty_generics_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<> {
                register: RegisterView<CustomContext, u64>,
            }
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Missing context"));
    }

    #[test]
    fn test_non_path_type_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {
                field: fn() -> i32,
            }
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Expected a path type"));
    }

    #[test]
    fn test_unnamed_field_in_hash_view_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C>(RegisterView<C, u64>);
        };
        let result = generate_hash_view_code(input);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("All fields must be named"));
    }

    #[test]
    fn test_unnamed_field_in_clonable_view_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C>(RegisterView<C, u64>);
        };
        let result = generate_clonable_view_code(input);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("All fields must be named"));
    }

    #[test]
    fn test_array_type_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {
                field: [u8; 32],
            }
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Expected a path type"));
    }

    #[test]
    fn test_reference_type_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {
                field: &'static str,
            }
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Expected a path type"));
    }

    #[test]
    fn test_pointer_type_failure() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {
                field: *const i32,
            }
        };
        let result = generate_view_code(input, false);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Expected a path type"));
    }

    #[test]
    fn test_generate_root_view_code_with_empty_struct() {
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {}
        };
        // Root view generation depends on view generation, so this should fail at the view level
        let result = generate_view_code(input.clone(), true);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Struct must have at least one field"));
    }

    #[test]
    fn test_generate_functions_behavior_differences() {
        // Some generation functions validate field types while others don't
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {
                field: fn() -> i32,
            }
        };

        // View code generation validates field types and should fail
        let view_result = generate_view_code(input.clone(), false);
        assert!(view_result.is_err());
        let error_msg = view_result.unwrap_err().to_string();
        assert!(error_msg.contains("Expected a path type"));

        // Hash view generation doesn't validate field types in the same way
        let hash_result = generate_hash_view_code(input.clone());
        assert!(hash_result.is_ok());

        // Crypto hash code generation also succeeds
        let _result = generate_crypto_hash_code(input);
    }

    #[test]
    fn test_crypto_hash_code_generation_failure() {
        // Crypto hash code generation should succeed as it doesn't validate field types directly
        let input: ItemStruct = parse_quote! {
            struct TestView<C> {
                register: RegisterView<C, usize>,
            }
        };
        let _result = generate_crypto_hash_code(input);
    }
}
