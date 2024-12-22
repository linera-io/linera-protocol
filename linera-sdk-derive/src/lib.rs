// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-sdk`.

mod utils;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::{
    parse_macro_input, Fields, ItemEnum,
    __private::{quote::quote, TokenStream2},
};

use crate::utils::{concat, snakify};

#[proc_macro_derive(GraphQLMutationRoot)]
pub fn derive_mutation_root(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemEnum);
    generate_mutation_root_code(input, "linera_sdk").into()
}

#[proc_macro_derive(GraphQLMutationRootInCrate)]
pub fn derive_mutation_root_in_crate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemEnum);
    generate_mutation_root_code(input, "crate").into()
}

fn generate_mutation_root_code(input: ItemEnum, crate_root: &str) -> TokenStream2 {
    let crate_root = Ident::new(crate_root, Span::call_site());
    let enum_name = input.ident;
    let mutation_root_name = concat(&enum_name, "MutationRoot");
    let mut methods = vec![];

    for variant in input.variants {
        let variant_name = &variant.ident;
        let function_name = snakify(variant_name);
        match variant.fields {
            Fields::Named(named) => {
                let mut fields = vec![];
                let mut field_names = vec![];
                for field in named.named {
                    let name = field.ident.expect("named fields always have names");
                    let ty = field.ty;
                    fields.push(quote! {#name: #ty});
                    field_names.push(name);
                }
                methods.push(quote! {
                    async fn #function_name(&self, #(#fields,)*) -> Vec<u8> {
                        #crate_root::bcs::to_bytes(&#enum_name::#variant_name { #(#field_names,)* })
                            .unwrap()
                    }
                });
            }
            Fields::Unnamed(unnamed) => {
                let mut fields = vec![];
                let mut field_names = vec![];
                for (i, field) in unnamed.unnamed.iter().enumerate() {
                    let name = concat(&syn::parse_str::<Ident>("field").unwrap(), &i.to_string());
                    let ty = &field.ty;
                    fields.push(quote! {#name: #ty});
                    field_names.push(name);
                }
                methods.push(quote! {
                    async fn #function_name(&self, #(#fields,)*) -> Vec<u8> {
                        #crate_root::bcs::to_bytes(&#enum_name::#variant_name ( #(#field_names,)* ))
                            .unwrap()
                    }
                });
            }
            Fields::Unit => {
                methods.push(quote! {
                    async fn #function_name(&self) -> Vec<u8> {
                        #crate_root::bcs::to_bytes(&#enum_name::#variant_name).unwrap()
                    }
                });
            }
        };
    }

    quote! {
        /// Mutation root
        pub struct #mutation_root_name;

        #[async_graphql::Object]
        impl #mutation_root_name {
            #(#methods)*
        }

        impl #crate_root::graphql::GraphQLMutationRoot for #enum_name {
            type MutationRoot = #mutation_root_name;

            fn mutation_root() -> Self::MutationRoot {
                #mutation_root_name
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use syn::{parse_quote, ItemEnum, __private::quote::quote};

    use crate::generate_mutation_root_code;

    fn assert_eq_no_whitespace(mut actual: String, mut expected: String) {
        // Intentionally left here for debugging purposes
        println!("{}", actual);

        actual.retain(|c| !c.is_whitespace());
        expected.retain(|c| !c.is_whitespace());

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_derive_mutation_root() {
        let operation: ItemEnum = parse_quote! {
            enum SomeOperation {
                TupleVariant(String),
                StructVariant {
                    a: u32,
                    b: u64
                },
                EmptyVariant
            }
        };

        let output = generate_mutation_root_code(operation, "linera_sdk");

        let expected = quote! {
            /// Mutation root
            pub struct SomeOperationMutationRoot;

            #[async_graphql::Object]
            impl SomeOperationMutationRoot {
                async fn tuple_variant(&self, field0: String,) -> Vec<u8> {
                    linera_sdk::bcs::to_bytes(&SomeOperation::TupleVariant(field0,)).unwrap()
                }
                async fn struct_variant(&self, a: u32, b: u64,) -> Vec<u8> {
                    linera_sdk::bcs::to_bytes(&SomeOperation::StructVariant { a, b, }).unwrap()
                }
                async fn empty_variant(&self) -> Vec<u8> {
                    linera_sdk::bcs::to_bytes(&SomeOperation::EmptyVariant).unwrap()
                }
            }

            impl linera_sdk::graphql::GraphQLMutationRoot for SomeOperation {
                type MutationRoot = SomeOperationMutationRoot;

                fn mutation_root() -> Self::MutationRoot {
                    SomeOperationMutationRoot
                }
            }
        };

        assert_eq_no_whitespace(output.to_string(), expected.to_string());
    }
}
