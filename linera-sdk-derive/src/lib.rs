// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The procedural macros for the crate `linera-sdk`.

mod utils;

use proc_macro::TokenStream;
use proc_macro2::{Ident, Span};
use syn::{
    parse_macro_input, Data, DeriveInput, Fields, ItemEnum, ItemStruct,
    __private::{quote::quote, TokenStream2},
};

use crate::utils::{concat, snakify};

#[proc_macro_derive(GraphQLMutationRoot)]
pub fn derive_mutation_root(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    generate_mutation_root_code_dispatch(input, "linera_sdk").into()
}

#[proc_macro_derive(GraphQLMutationRootInCrate)]
pub fn derive_mutation_root_in_crate(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    generate_mutation_root_code_dispatch(input, "crate").into()
}

fn generate_mutation_root_code_dispatch(input: DeriveInput, crate_root: &str) -> TokenStream2 {
    match input.data {
        Data::Enum(data_enum) => {
            let item_enum = ItemEnum {
                attrs: input.attrs,
                vis: input.vis,
                enum_token: data_enum.enum_token,
                ident: input.ident,
                generics: input.generics,
                brace_token: data_enum.brace_token,
                variants: data_enum.variants,
            };
            generate_mutation_root_code_for_enum(item_enum, crate_root)
        }
        Data::Struct(data_struct) => {
            let item_struct = ItemStruct {
                attrs: input.attrs,
                vis: input.vis,
                struct_token: data_struct.struct_token,
                ident: input.ident,
                generics: input.generics,
                fields: data_struct.fields,
                semi_token: data_struct.semi_token,
            };
            generate_mutation_root_code_for_struct(item_struct, crate_root)
        }
        Data::Union(_) => {
            panic!("GraphQLMutationRoot can only be derived for enums and structs, not unions")
        }
    }
}

fn generate_mutation_root_code_for_enum(input: ItemEnum, crate_root: &str) -> TokenStream2 {
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

fn generate_mutation_root_code_for_struct(input: ItemStruct, crate_root: &str) -> TokenStream2 {
    let crate_root = Ident::new(crate_root, Span::call_site());
    let struct_name = &input.ident;
    let mutation_root_name = concat(struct_name, "MutationRoot");

    // Convert struct name to snake_case for the method name
    let method_name = snakify(struct_name);

    let method = match input.fields {
        Fields::Named(named) => {
            let mut fields = vec![];
            let mut field_names = vec![];
            for field in named.named {
                let name = field.ident.expect("named fields always have names");
                let ty = field.ty;
                fields.push(quote! {#name: #ty});
                field_names.push(name);
            }
            quote! {
                async fn #method_name(&self, #(#fields,)*) -> [u8; 0] {
                    let operation = #struct_name {
                        #(#field_names,)*
                    };

                    self.runtime.schedule_operation(&operation);

                    []
                }
            }
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
            quote! {
                async fn #method_name(&self, #(#fields,)*) -> [u8; 0] {
                    let operation = #struct_name(
                        #(#field_names,)*
                    );

                    self.runtime.schedule_operation(&operation);

                    []
                }
            }
        }
        Fields::Unit => {
            quote! {
                async fn #method_name(&self) -> [u8; 0] {
                    let operation = #struct_name;

                    self.runtime.schedule_operation(&operation);

                    []
                }
            }
        }
    };

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
            #method
        }

        impl<Application> #crate_root::graphql::GraphQLMutationRoot<Application> for #struct_name
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
pub mod tests {
    use syn::{parse_quote, ItemEnum, ItemStruct, __private::quote::quote};

    use crate::{generate_mutation_root_code_for_enum, generate_mutation_root_code_for_struct};

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

        let output = generate_mutation_root_code_for_enum(operation, "linera_sdk");

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

    #[test]
    fn test_derive_mutation_root_struct() {
        let operation: ItemStruct = parse_quote! {
            struct SomeOperation {
                a: u32,
                b: String,
            }
        };

        let output = generate_mutation_root_code_for_struct(operation, "linera_sdk");

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
                async fn some_operation(&self, a: u32, b: String,) -> [u8; 0] {
                    let operation = SomeOperation { a, b, };
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

    #[test]
    fn test_derive_mutation_root_unit_struct() {
        let operation: ItemStruct = parse_quote! {
            struct SomeOperation;
        };

        let output = generate_mutation_root_code_for_struct(operation, "linera_sdk");

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
                async fn some_operation(&self) -> [u8; 0] {
                    let operation = SomeOperation;
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
