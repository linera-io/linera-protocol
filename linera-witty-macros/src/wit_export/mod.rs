// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to export host functions to a Wasm guest instance.

#![cfg(any(feature = "mock-instance", feature = "wasmer", feature = "wasmtime"))]

mod function_information;

use self::function_information::FunctionInformation;
use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::quote;
use syn::{Generics, Ident, ItemImpl, LitStr, Type, TypePath};

/// Returns the code generated for exporting host functions to guest Wasm instances.
///
/// The generated code implements the `linera_witty::ExportTo` trait for the Wasm runtimes enabled
/// through feature flags. The trait implementation exports the host functions in the input `impl`
/// block to a provided Wasm guest instance.
pub fn generate(implementation: &ItemImpl, namespace: &LitStr) -> TokenStream {
    WitExportGenerator::new(implementation, namespace).generate()
}

/// A helper type for generation of the code to export host functions to Wasm guest instances.
///
/// Code generating is done in two phases. First the necessary pieces are collected and stored in
/// this type. Then, they are used to generate the final code.
pub struct WitExportGenerator<'input> {
    type_name: &'input Ident,
    caller_type_parameter: CallerTypeParameter<'input>,
    implementation: &'input ItemImpl,
    functions: Vec<FunctionInformation<'input>>,
    namespace: &'input LitStr,
}

impl<'input> WitExportGenerator<'input> {
    /// Collects the pieces necessary for code generation from the inputs.
    pub fn new(implementation: &'input ItemImpl, namespace: &'input LitStr) -> Self {
        let type_name = type_name(implementation);
        let caller_type_parameter = CallerTypeParameter::new(&implementation.generics);
        let functions = implementation
            .items
            .iter()
            .map(|item| FunctionInformation::from_item(item, caller_type_parameter.caller()))
            .collect();

        WitExportGenerator {
            type_name,
            caller_type_parameter,
            implementation,
            functions,
            namespace,
        }
    }

    /// Consumes the collected pieces to generate the final code.
    pub fn generate(mut self) -> TokenStream {
        let implementation = self.implementation;
        let wasmer = self.generate_for_wasmer();
        let wasmtime = self.generate_for_wasmtime();
        let mock_instance = self.generate_for_mock_instance();

        quote! {
            #implementation
            #wasmer
            #wasmtime
            #mock_instance
        }
    }

    /// Generates the code to export functions using the Wasmer runtime.
    fn generate_for_wasmer(&mut self) -> Option<TokenStream> {
        #[cfg(feature = "wasmer")]
        {
            let export_target = quote! { linera_witty::wasmer::InstanceBuilder<()> };
            let target_caller_type = quote! {
                linera_witty::wasmer::FunctionEnvMut<'_, linera_witty::wasmer::InstanceSlot<()>>
            };
            let exported_functions = self
                .functions
                .iter()
                .map(|function| function.generate_for_wasmer(self.namespace, self.type_name));

            Some(self.generate_for(export_target, target_caller_type, exported_functions))
        }
        #[cfg(not(feature = "wasmer"))]
        {
            None
        }
    }

    /// Generates the code to export functions using the Wasmtime runtime.
    fn generate_for_wasmtime(&mut self) -> Option<TokenStream> {
        #[cfg(feature = "wasmtime")]
        {
            let export_target = quote! { linera_witty::wasmtime::Linker<()> };
            let target_caller_type = quote! { linera_witty::wasmtime::Caller<'_, ()> };
            let exported_functions = self
                .functions
                .iter()
                .map(|function| function.generate_for_wasmtime(self.namespace, self.type_name));

            Some(self.generate_for(export_target, target_caller_type, exported_functions))
        }
        #[cfg(not(feature = "wasmtime"))]
        {
            None
        }
    }

    /// Generates the code to export functions to a mock instance for testing.
    fn generate_for_mock_instance(&mut self) -> Option<TokenStream> {
        #[cfg(feature = "mock-instance")]
        {
            let export_target = quote! { linera_witty::MockInstance<()> };
            let target_caller_type = quote! { linera_witty::MockInstance<()> };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_mock_instance(self.namespace, self.type_name)
            });

            Some(self.generate_for(export_target, target_caller_type, exported_functions))
        }
        #[cfg(not(feature = "mock-instance"))]
        {
            None
        }
    }

    /// Generates the implementation of `ExportTo` for the `export_target` including the
    /// `exported_functions`.
    fn generate_for(
        &self,
        export_target: TokenStream,
        target_caller_type: TokenStream,
        exported_functions: impl Iterator<Item = TokenStream>,
    ) -> TokenStream {
        let type_name = &self.type_name;
        let caller_type_parameter = self
            .caller_type_parameter
            .caller()
            .map(|_| quote! { <#target_caller_type> });

        quote! {
            impl linera_witty::ExportTo<#export_target> for #type_name #caller_type_parameter {
                fn export_to(
                    target: &mut #export_target,
                ) -> Result<(), linera_witty::RuntimeError> {
                    #( #exported_functions )*
                    Ok(())
                }
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
            abort!(implementation.self_ty, "Missing type name identifier",);
        })
        .ident
}

/// Information on the  generic type parameter to use for the caller parameter, if present.
#[derive(Clone, Copy, Debug)]
enum CallerTypeParameter<'input> {
    NotPresent,
    WithoutUserData(&'input Ident),
}

impl<'input> CallerTypeParameter<'input> {
    /// Parses a type's [`Generics`] to determine if a caller type parameter should be used.
    pub fn new(generics: &'input Generics) -> Self {
        if generics.type_params().count() > 1 {
            abort!(
                generics.params,
                "`#[wit_export]` supports only one generic type parameter \
                which is assumed to be the caller instance"
            );
        }

        match generics.type_params().next() {
            None => CallerTypeParameter::NotPresent,
            Some(parameter) => CallerTypeParameter::WithoutUserData(&parameter.ident),
        }
    }

    /// Returns the [`Ident`]ifier of the generic type parameter used for the caller.
    pub fn caller(&self) -> Option<&'input Ident> {
        match self {
            CallerTypeParameter::NotPresent => None,
            CallerTypeParameter::WithoutUserData(caller) => Some(caller),
        }
    }
}
