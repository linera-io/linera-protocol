extern crate proc_macro;
extern crate syn;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

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

fn generate_view_code(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());
    let first_generic = template_vect
        .get(0)
        .expect("failed to find the first generic parameter");

    let mut names = Vec::new();
    let mut loades = Vec::new();
    let mut rollbackes = Vec::new();
    let mut flushes = Vec::new();
    let mut deletes = Vec::new();
    let mut cleares = Vec::new();
    for (idx, e) in input.fields.into_iter().enumerate() {
        let name = e.clone().ident.unwrap();
        let idx_lit = syn::LitInt::new(&idx.to_string(), Span::call_site());
        let type_ident = get_type_field(e).expect("Failed to find the type");
        loades.push(quote! {
            let index = #idx_lit;
            let base_key = context.derive_key(&index)?;
            let #name = #type_ident::load(context.clone_with_base_key(base_key)).await?;
        });
        names.push(quote! { #name });
        rollbackes.push(quote! { self.#name.rollback(); });
        flushes.push(quote! { self.#name.flush(batch)?; });
        deletes.push(quote! { self.#name.delete(batch); });
        cleares.push(quote! { self.#name.clear(); });
    }
    let first_name = names.get(0).expect("list of names should be non-empty");

    let quote_o = quote! {
        #[async_trait::async_trait]
        impl #generics View<#first_generic> for #struct_name #generics
        where
            #first_generic: Context + Send + Sync + Clone + 'static,
            linera_views::views::ViewError: From<#first_generic::Error>,
        {
            fn context(&self) -> &#first_generic {
                self.#first_name.context()
            }

            async fn load(context: #first_generic) -> Result<Self, linera_views::views::ViewError> {
                #(#loades)*
                Ok(Self {#(#names),*})
            }

            fn rollback(&mut self) {
                #(#rollbackes)*
            }

            fn flush(&mut self, batch: &mut linera_views::common::Batch) -> Result<(), linera_views::views::ViewError> {
                #(#flushes)*
                Ok(())
            }

            fn delete(self, batch: &mut linera_views::common::Batch) {
                #(#deletes)*
            }

            fn clear(&mut self) {
                #(#cleares)*
            }
        }
    };
    //    println!("quote_o={}", quote_o);
    TokenStream::from(quote_o)
}

fn generate_container_view_code(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());
    let first_generic = template_vect
        .get(0)
        .expect("failed to find the first generic parameter");

    let flushes = input
        .fields
        .clone()
        .into_iter()
        .map(|e| {
            let name = e.ident.unwrap();
            quote! {
                self.#name.flush(&mut batch)?;
            }
        })
        .collect::<Vec<_>>();

    let deletes = input
        .fields
        .into_iter()
        .map(|e| {
            let name = e.ident.unwrap();
            quote! {
                self.#name.delete(batch);
            }
        })
        .collect::<Vec<_>>();

    let quote_o = quote! {
        #[async_trait::async_trait]
        impl #generics ContainerView<#first_generic> for #struct_name #generics
        where
            #first_generic: Context + Send + Sync + Clone + 'static,
            linera_views::views::ViewError: From<#first_generic::Error>,
        {
            async fn save(&mut self) -> Result<(), linera_views::views::ViewError> {
                use linera_views::common::Batch;
                let mut batch = Batch::default();
                #(#flushes)*

                self.context().write_batch(batch).await?;
                Ok(())
            }

            async fn write_delete(self) -> Result<(), linera_views::views::ViewError> {
                use linera_views::common::Batch;
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
    };
    //    println!("quote_o={}", quote_o);
    TokenStream::from(quote_o)
}

fn generate_hash_view_code(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());
    let first_generic = template_vect
        .get(0)
        .expect("failed to find the first generic parameter");

    let field_hash = input
        .fields
        .into_iter()
        .map(|e| {
            let name = e.ident.unwrap();
            quote! { hasher.write_all(self.#name.hash().await?.as_ref())?; }
        })
        .collect::<Vec<_>>();

    let quote_o = quote! {
        #[async_trait::async_trait]
        impl #generics HashView<#first_generic> for #struct_name #generics
        where
            #first_generic: Context + Send + Sync + Clone + 'static,
            linera_views::views::ViewError: From<#first_generic::Error>,
        {
            type Hasher = linera_views::sha2::Sha512;

            async fn hash(&mut self) -> Result<<Self::Hasher as linera_views::views::Hasher>::Output, linera_views::views::ViewError> {
                use linera_views::views::{Hasher, HashView};
                use std::io::Write;
                let mut hasher = Self::Hasher::default();
                #(#field_hash)*
                Ok(hasher.finalize())
            }
        }
    };
    //    println!("quote_o={}", quote_o);
    TokenStream::from(quote_o)
}

fn generate_hash_func_code(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);

    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());
    let first_generic = template_vect
        .get(0)
        .expect("failed to find the first generic parameter");

    let hash_type = syn::Ident::new(&format!("{}Hash", struct_name), Span::call_site());
    let quote_o = quote! {
        #[async_trait::async_trait]
        impl #generics HashFunc<#first_generic> for #struct_name #generics
        where
            #first_generic: Context + Send + Sync + Clone + 'static,
            linera_views::views::ViewError: From<#first_generic::Error>,
        {
            async fn hash_value(&mut self) -> Result<linera_base::crypto::HashValue, linera_views::views::ViewError> {
                use linera_views::generic_array::GenericArray;
                use linera_views::common::Batch;
                use linera_base::crypto::{BcsSignable, HashValue};
                use linera_views::views::HashView;
                use serde::{Serialize, Deserialize};
                use linera_views::sha2::{Sha512, Digest};
                #[derive(Serialize, Deserialize)]
                struct #hash_type(GenericArray<u8, <Sha512 as Digest>::OutputSize>);
                impl BcsSignable for #hash_type {}
                let hash = self.hash().await?;
                Ok(HashValue::new(&#hash_type(hash)))
            }
        }
    };
    //    println!("quote_o={}", quote_o);
    TokenStream::from(quote_o)
}

#[proc_macro_derive(View)]
pub fn derive_view(input: TokenStream) -> TokenStream {
    generate_view_code(input)
}

#[proc_macro_derive(ContainerView)]
pub fn derive_container_view(input: TokenStream) -> TokenStream {
    generate_container_view_code(input)
}

#[proc_macro_derive(HashView)]
pub fn derive_hash_view(input: TokenStream) -> TokenStream {
    generate_hash_view_code(input)
}

#[proc_macro_derive(HashFunc)]
pub fn derive_hash_func(input: TokenStream) -> TokenStream {
    generate_hash_func_code(input)
}
