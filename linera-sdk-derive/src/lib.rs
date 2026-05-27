// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-sdk`.

#![deny(missing_docs)]

mod stable_enum;
mod utils;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::{
    __private::{quote::quote, TokenStream2},
    parse_macro_input, Fields, ItemEnum,
};

use crate::utils::{concat, snakify};

/// Derives `GraphQLMutationRoot` for an operation enum, generating a GraphQL mutation root
/// whose mutations each schedule the corresponding operation. SDK paths in the generated code
/// are resolved against the `linera_sdk` crate.
#[proc_macro_derive(GraphQLMutationRoot)]
pub fn derive_mutation_root(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemEnum);
    generate_mutation_root_code(input, "linera_sdk").into()
}

/// Like the `GraphQLMutationRoot` derive, but resolves SDK paths against `crate` instead of
/// `linera_sdk`. Used within the `linera-sdk` crate itself.
#[proc_macro_derive(GraphQLMutationRootInCrate)]
pub fn derive_mutation_root_in_crate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemEnum);
    generate_mutation_root_code(input, "crate").into()
}

/// Derive `linera_sdk::formats::StableEnum` for an `enum`. Expands to:
///
/// * `serde::Serialize` / `serde::Deserialize` impls in which the variant tag
///   is the first 4 bytes of `Keccak-256(variant_name)` (read big-endian as
///   `u32`), with the top 5 bits masked to `00001` so the ULEB128 encoding is
///   always exactly 4 bytes; and
/// * a `linera_sdk::formats::StableEnumTrace` impl exposing the per-variant
///   tags and a `trace_all_variants` method that drives
///   `serde_reflection::Tracer` without caller-supplied samples.
#[proc_macro_derive(StableEnum)]
pub fn derive_stable_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemEnum);
    stable_enum::generate_all(&input, stable_enum::CrateRoot::LineraSdk)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Same as [`StableEnum`] but referring to the trait through `crate::...`
/// instead of `::linera_sdk::...`. Used inside the `linera-sdk` crate itself
/// (and only there).
#[proc_macro_derive(StableEnumInCrate)]
pub fn derive_stable_enum_in_crate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemEnum);
    stable_enum::generate_all(&input, stable_enum::CrateRoot::Crate)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
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
                    async fn #function_name(&self, #(#fields,)*) -> [u8; 0] {
                        let operation = #enum_name::#variant_name {
                            #(#field_names,)*
                        };

                        self.runtime.schedule_operation(&operation);

                        []
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
                    async fn #function_name(&self, #(#fields,)*) -> [u8; 0] {
                        let operation = #enum_name::#variant_name(
                            #(#field_names,)*
                        );

                        self.runtime.schedule_operation(&operation);

                        []
                    }
                });
            }
            Fields::Unit => {
                methods.push(quote! {
                    async fn #function_name(&self) -> [u8; 0] {
                        let operation = #enum_name::#variant_name;

                        self.runtime.schedule_operation(&operation);

                        []
                    }
                });
            }
        };
    }

    quote! {
        /// Mutation root
        pub struct #mutation_root_name<Application>
        where
            Application: #crate_root::Service,
            #crate_root::ServiceRuntime<Application>: Send + Sync,
        {
            runtime: ::std::sync::Arc<#crate_root::ServiceRuntime<Application>>,
        }

        #[async_graphql::Object]
        impl<Application> #mutation_root_name<Application>
        where
            Application: #crate_root::Service,
            #crate_root::ServiceRuntime<Application>: Send + Sync,
        {
            #(#methods)*
        }

        impl<Application> #crate_root::graphql::GraphQLMutationRoot<Application> for #enum_name
        where
            Application: #crate_root::Service,
            #crate_root::ServiceRuntime<Application>: Send + Sync,
        {
            type MutationRoot = #mutation_root_name<Application>;

            fn mutation_root(
                runtime: ::std::sync::Arc<#crate_root::ServiceRuntime<Application>>,
            ) -> Self::MutationRoot {
                #mutation_root_name { runtime }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use syn::{__private::quote::quote, parse_quote, ItemEnum};

    use crate::generate_mutation_root_code;

    fn assert_eq_no_whitespace(mut actual: String, mut expected: String) {
        // Intentionally left here for debugging purposes
        println!("{actual}");

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
            pub struct SomeOperationMutationRoot<Application>
            where
                Application: linera_sdk::Service,
                linera_sdk::ServiceRuntime<Application>: Send + Sync,
            {
                runtime: ::std::sync::Arc<linera_sdk::ServiceRuntime<Application>>,
            }

            #[async_graphql::Object]
            impl<Application> SomeOperationMutationRoot<Application>
            where
                Application: linera_sdk::Service,
                linera_sdk::ServiceRuntime<Application>: Send + Sync,
            {
                async fn tuple_variant(&self, field0: String,) -> [u8; 0] {
                    let operation = SomeOperation::TupleVariant(field0,);
                    self.runtime.schedule_operation(&operation);
                    []
                }

                async fn struct_variant(&self, a: u32, b: u64,) -> [u8; 0] {
                    let operation = SomeOperation::StructVariant { a, b, };
                    self.runtime.schedule_operation(&operation);
                    []
                }

                async fn empty_variant(&self) -> [u8; 0] {
                    let operation = SomeOperation::EmptyVariant;
                    self.runtime.schedule_operation(&operation);
                    []
                }
            }

            impl<Application> linera_sdk::graphql::GraphQLMutationRoot<Application>
                for SomeOperation
            where
                Application: linera_sdk::Service,
                linera_sdk::ServiceRuntime<Application>: Send + Sync,
            {
                type MutationRoot = SomeOperationMutationRoot<Application>;

                fn mutation_root(
                    runtime: ::std::sync::Arc<linera_sdk::ServiceRuntime<Application>>,
                ) -> Self::MutationRoot {
                    SomeOperationMutationRoot { runtime }
                }
            }
        };

        assert_eq_no_whitespace(output.to_string(), expected.to_string());
    }
}
