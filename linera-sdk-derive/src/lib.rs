use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
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

fn generate_service_code(input: ItemStruct) -> TokenStream2 {
    let struct_name = input.ident;
    let generics = input.generics;
    let template_vect = get_seq_parameter(generics.clone());
    let first_generic = template_vect
        .get(0)
        .expect("failed to find the first generic parameter");

    quote! {
        #[async_trait::async_trait]
        impl #generics linera_sdk::Service for #struct_name #generics
        where
            C: linera_views::common::Context + Send + Sync + Clone + 'static,
            linera_views::views::ViewError: From<#first_generic::Error>,
        {
            type Error = Error;
            type Storage = linera_sdk::ViewStateStorage<Self>;

            async fn query_application(
                self: std::sync::Arc<Self>,
                _context: &linera_sdk::QueryContext,
                argument: &[u8],
            ) -> Result<Vec<u8>, Self::Error> {
                use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
                let graphql_request: Request = serde_json::from_slice(argument).map_err(|_| Error::InvalidQuery)?;
                let schema = Schema::build(self.clone(), EmptyMutation, EmptySubscription).finish();
                let res = schema.execute(graphql_request).await;
                Ok(serde_json::to_vec(&res).unwrap())
            }
        }

        /// An error that can occur during the contract execution.
        #[derive(Debug, thiserror::Error, Eq, PartialEq)]
        pub enum Error {
            /// Invalid query argument; Counter application only supports JSON encoded GraphQL queries.
            #[error(
                "Invalid query argument; Counter application only supports JSON encoded GraphQL queries"
            )]
            InvalidQuery,
        }
    }
}

#[proc_macro_derive(Service)]
pub fn derive_service(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemStruct);
    generate_service_code(input).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    #[rustfmt::skip]
    fn test_generate_graphql_code() {
        let input: ItemStruct = parse_quote!(
            struct TestState<C> {
                pub value: RegisterView<C, u64>
            }
        );

        let output = generate_service_code(input);

        let expected = quote!(
            #[async_trait::async_trait]
            impl<C> linera_sdk::Service for TestState<C>
            where
                C: linera_views::common::Context + Send + Sync + Clone + 'static,
                linera_views::views::ViewError: From<C::Error>,
            {
                type Error = Error;
                type Storage = linera_sdk::ViewStateStorage<Self>;

                async fn query_application(
                    self: std::sync::Arc<Self>,
                    _context: &linera_sdk::QueryContext,
                    argument: &[u8],
                ) -> Result<Vec<u8>, Self::Error> {
                    use async_graphql::{EmptyMutation, EmptySubscription, Request, Schema};
                    let graphql_request: Request = serde_json::from_slice(argument).map_err(|_| Error::InvalidQuery)?;
                    let schema = Schema::build(self.clone(), EmptyMutation, EmptySubscription).finish();
                    let res = schema.execute(graphql_request).await;
                    Ok(serde_json::to_vec(&res).unwrap())
                }
            }

            /// An error that can occur during the contract execution.
            #[derive(Debug, thiserror::Error, Eq, PartialEq)]
            pub enum Error {
                /// Invalid query argument; Counter application only supports JSON encoded GraphQL queries.
                #[error(
                    "Invalid query argument; Counter application only supports JSON encoded GraphQL queries"
                )]
                InvalidQuery,
            }
        );

        assert_eq!(output.to_string(), expected.to_string())
    }
}
