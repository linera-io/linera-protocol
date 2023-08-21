// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to export host functions to a Wasm guest instance.

mod function_information;

use self::function_information::FunctionInformation;
use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::quote;
use syn::{Ident, ItemImpl, LitStr, Type, TypePath};

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
    implementation: &'input ItemImpl,
    functions: Vec<FunctionInformation<'input>>,
    namespace: &'input LitStr,
}

impl<'input> WitExportGenerator<'input> {
    /// Collects the pieces necessary for code generation from the inputs.
    pub fn new(implementation: &'input ItemImpl, namespace: &'input LitStr) -> Self {
        let type_name = type_name(implementation);
        let functions = implementation
            .items
            .iter()
            .map(FunctionInformation::from)
            .collect();

        WitExportGenerator {
            type_name,
            implementation,
            functions,
            namespace,
        }
    }

    /// Consumes the collected pieces to generate the final code.
    pub fn generate(mut self) -> TokenStream {
        let implementation = self.implementation;

        quote! {
            #implementation
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
            "`#[wit_export]` must be used on `impl` blocks of non-generic types",
        );
    };

    path_name.get_ident().unwrap_or_else(|| {
        abort!(
            implementation.self_ty,
            "`#[wit_export]` must be used on `impl` blocks of non-generic types",
        );
    })
}
