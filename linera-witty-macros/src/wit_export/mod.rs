// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to export host functions to a Wasm guest instance.

// TODO(#1683): Remove feature flags by generating runtime agnostic code
#![cfg(with_wit_export)]

mod caller_type_parameter;
mod function_information;

use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::{quote, ToTokens};
use syn::{
    parse_quote, punctuated::Punctuated, token::Paren, Generics, Ident, ItemImpl, LitStr, Type,
    TypePath, TypeTuple,
};

use self::caller_type_parameter::CallerTypeParameter;
pub(crate) use self::function_information::{ok_type_inside_result, FunctionInformation};
use super::wit_interface;
use crate::util::AttributeParameters;

/// Returns the code generated for exporting host functions to guest Wasm instances.
///
/// The generated code implements the `linera_witty::ExportTo` trait for the Wasm runtimes enabled
/// through feature flags. The trait implementation exports the host functions in the input `impl`
/// block to a provided Wasm guest instance.
pub fn generate(implementation: &ItemImpl, parameters: AttributeParameters) -> TokenStream {
    WitExportGenerator::new(implementation, parameters).generate()
}

/// A helper type for generation of the code to export host functions to Wasm guest instances.
///
/// Code generating is done in two phases. First the necessary pieces are collected and stored in
/// this type. Then, they are used to generate the final code.
pub struct WitExportGenerator<'input> {
    parameters: AttributeParameters,
    namespace: LitStr,
    type_name: &'input Ident,
    caller_type_parameter: Option<CallerTypeParameter<'input>>,
    generics: &'input Generics,
    implementation: &'input ItemImpl,
    functions: Vec<FunctionInformation<'input>>,
}

impl<'input> WitExportGenerator<'input> {
    /// Collects the pieces necessary for code generation from the inputs.
    pub fn new(implementation: &'input ItemImpl, parameters: AttributeParameters) -> Self {
        let type_name = type_name(implementation);
        let namespace = parameters.namespace(type_name);
        let caller_type_parameter = CallerTypeParameter::extract_from(&implementation.generics);
        let caller = caller_type_parameter
            .as_ref()
            .map(CallerTypeParameter::caller);
        let functions = implementation
            .items
            .iter()
            .map(|item| FunctionInformation::from_item(item, caller))
            .collect();

        WitExportGenerator {
            parameters,
            namespace,
            type_name,
            caller_type_parameter,
            generics: &implementation.generics,
            implementation,
            functions,
        }
    }

    /// Consumes the collected pieces to generate the final code.
    pub fn generate(mut self) -> TokenStream {
        let implementation = self.implementation;
        let wasmer = self.generate_for_wasmer();
        let wasmtime = self.generate_for_wasmtime();
        let mock_instance = self.generate_for_mock_instance();
        let wit_interface = self.generate_wit_interface();

        quote! {
            #implementation
            #wasmer
            #wasmtime
            #mock_instance
            #wit_interface
        }
    }

    /// Generates the code to export functions using the Wasmer runtime.
    fn generate_for_wasmer(&mut self) -> Option<TokenStream> {
        #[cfg(with_wasmer)]
        {
            let user_data_type = self.user_data_type();
            let export_target = quote! { linera_witty::wasmer::InstanceBuilder<#user_data_type> };
            let target_caller_type: Type = parse_quote! {
                linera_witty::wasmer::FunctionEnvMut<
                    '_,
                    linera_witty::wasmer::InstanceSlot<#user_data_type>,
                >
            };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_wasmer(&self.namespace, self.type_name, &target_caller_type)
            });

            Some(self.generate_for(export_target, &target_caller_type, exported_functions))
        }
        #[cfg(not(with_wasmer))]
        {
            None
        }
    }

    /// Generates the code to export functions using the Wasmtime runtime.
    fn generate_for_wasmtime(&mut self) -> Option<TokenStream> {
        #[cfg(with_wasmtime)]
        {
            let user_data_type = self.user_data_type();
            let export_target = quote! { linera_witty::wasmtime::Linker<#user_data_type> };
            let target_caller_type: Type =
                parse_quote! { linera_witty::wasmtime::Caller<'_, #user_data_type> };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_wasmtime(&self.namespace, self.type_name, &target_caller_type)
            });

            Some(self.generate_for(export_target, &target_caller_type, exported_functions))
        }
        #[cfg(not(with_wasmtime))]
        {
            None
        }
    }

    /// Generates the code to export functions to a mock instance for testing.
    fn generate_for_mock_instance(&mut self) -> Option<TokenStream> {
        #[cfg(with_testing)]
        {
            let user_data_type = self.user_data_type();
            let export_target = quote! { linera_witty::MockInstance<#user_data_type> };
            let target_caller_type: Type =
                parse_quote! { linera_witty::MockInstance<#user_data_type> };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_mock_instance(
                    &self.namespace,
                    self.type_name,
                    &target_caller_type,
                )
            });

            Some(self.generate_for(export_target, &target_caller_type, exported_functions))
        }
        #[cfg(not(with_testing))]
        {
            None
        }
    }

    /// Generates the implementation of `ExportTo` for the `export_target` including the
    /// `exported_functions`.
    fn generate_for(
        &self,
        export_target: TokenStream,
        target_caller_type: &Type,
        exported_functions: impl Iterator<Item = TokenStream>,
    ) -> TokenStream {
        let (impl_generics, self_type, where_clause) = self.prepare_generics(target_caller_type);

        quote! {
            impl #impl_generics linera_witty::ExportTo<#export_target> for #self_type
            #where_clause
            {
                fn export_to(
                    target: &mut #export_target,
                ) -> Result<(), linera_witty::RuntimeError> {
                    #( #exported_functions )*
                    Ok(())
                }
            }
        }
    }

    /// Specializes the [`CallerTypeParameter`] (if present) with the `target_caller_type`, and
    /// returns the split generics part used in the implementation blocks.
    fn prepare_generics(&self, target_caller_type: &Type) -> (TokenStream, Type, TokenStream) {
        let mut self_type = (*self.implementation.self_ty).clone();

        if let Some(caller_type_parameter) = self.caller_type_parameter {
            caller_type_parameter.specialize_type(&mut self_type, target_caller_type.clone());

            let (impl_generics, _type_generics, where_clause) = caller_type_parameter
                .specialize_and_split_generics(self.generics.clone(), target_caller_type.clone());

            (impl_generics, self_type, where_clause)
        } else {
            let (impl_generics, _type_generics, where_clause) = self.generics.split_for_impl();

            (
                impl_generics.to_token_stream(),
                self_type,
                where_clause.to_token_stream(),
            )
        }
    }

    /// Returns the type to use for the custom user data.
    fn user_data_type(&self) -> Type {
        self.caller_type_parameter
            .as_ref()
            .and_then(CallerTypeParameter::user_data)
            .cloned()
            .unwrap_or_else(|| {
                // Unit type
                Type::Tuple(TypeTuple {
                    paren_token: Paren::default(),
                    elems: Punctuated::new(),
                })
            })
    }

    /// Generates the implementation of `WitInterface` for the type.
    fn generate_wit_interface(&self) -> TokenStream {
        let self_type = &self.implementation.self_ty;
        let type_name = self.type_name;

        let wit_interface_implementation = wit_interface::generate(
            self.parameters.package_name(),
            self.parameters.interface_name(type_name),
            &self.functions,
        );

        let (impl_generics, _type_generics, where_clause) =
            self.implementation.generics.split_for_impl();

        quote! {
            impl #impl_generics linera_witty::wit_generation::WitInterface for #self_type
            #where_clause
            {
                #wit_interface_implementation
            }
        }
    }
}

/// Returns the type name of the type the `impl` block is for.
pub fn type_name(implementation: &ItemImpl) -> &Ident {
    let Type::Path(TypePath {
        qself: None,
        path: path_name,
    }) = &*implementation.self_ty
    else {
        abort!(
            implementation.self_ty,
            "`#[wit_export]` must be used on `impl` blocks",
        );
    };

    &path_name
        .segments
        .last()
        .unwrap_or_else(|| {
            abort!(implementation.self_ty, "Missing type name identifier");
        })
        .ident
}
