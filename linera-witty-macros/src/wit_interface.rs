// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to generate WIT snippets for an interface.

use std::iter;

use heck::ToKebabCase;
use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::quote;
use syn::{FnArg, Ident, LitStr, Pat, PatIdent, PatType, ReturnType, Type};

#[cfg(with_wit_export)]
use super::wit_export;
use super::wit_import;

/// Returns the code generated for implementing `WitInterface` to generate WIT snippets for an
/// interface.
pub fn generate<'input, Functions>(
    wit_package: &LitStr,
    wit_name: LitStr,
    functions: Functions,
) -> TokenStream
where
    Functions: IntoIterator,
    Functions::IntoIter: Clone,
    FunctionInformation<'input>: From<Functions::Item>,
{
    let functions = functions.into_iter().map(FunctionInformation::from);

    let dependencies = functions
        .clone()
        .flat_map(|function| function.dependencies().cloned().collect::<Vec<_>>());

    let wit_functions = functions.map(|function| function.wit_declaration());

    quote! {
        type Dependencies = linera_witty::HList![#( #dependencies ),*];

        fn wit_package() -> &'static str {
            #wit_package
        }

        fn wit_name() -> &'static str {
            #wit_name
        }

        fn wit_functions() -> Vec<String> {
            vec![
                #( #wit_functions ),*
            ]
        }
    }
}

/// Information from a function relevant to deriving the [`WitInterface`] trait.
pub struct FunctionInformation<'input> {
    name: &'input Ident,
    parameter_names: Vec<&'input Ident>,
    parameter_types: Vec<&'input Type>,
    output_type: Option<Type>,
}

impl<'input> FunctionInformation<'input> {
    /// Creates a new [`FunctionInformation`] instance from its signature.
    pub fn new(
        name: &'input Ident,
        inputs: impl IntoIterator<Item = &'input FnArg>,
        output: ReturnType,
    ) -> Self {
        let (parameter_names, parameter_types) = inputs
            .into_iter()
            .map(|argument| {
                let FnArg::Typed(PatType { pat, ty, .. }) = argument else {
                    abort!(argument, "`self` is not supported in imported functions");
                };

                let Pat::Ident(PatIdent { ident, .. }) = pat.as_ref() else {
                    abort!(
                        pat,
                        "Only named parameters are supported in imported functions"
                    );
                };

                (ident, ty.as_ref())
            })
            .unzip();

        let output_type = match output {
            ReturnType::Default => None,
            ReturnType::Type(_arrow, return_type) => Some(*return_type),
        };

        FunctionInformation {
            name,
            parameter_names,
            parameter_types,
            output_type,
        }
    }

    /// Returns the types used in the function signature.
    pub fn dependencies(&self) -> impl Iterator<Item = &'_ Type> {
        self.parameter_types
            .clone()
            .into_iter()
            .chain(&self.output_type)
    }

    /// Returns a [`LitStr`] with the kebab-case WIT name of the function.
    pub fn wit_name(&self) -> LitStr {
        LitStr::new(&self.name.to_string().to_kebab_case(), self.name.span())
    }

    /// Returns the code to generate a [`String`] with the WIT declaration of the function.
    pub fn wit_declaration(&self) -> TokenStream {
        let wit_name = self.wit_name();

        let parameters = self.parameter_names.iter().zip(&self.parameter_types).map(
            |(parameter_name, parameter_type)| {
                let parameter_wit_name = LitStr::new(
                    &parameter_name.to_string().to_kebab_case(),
                    parameter_name.span(),
                );

                quote! {
                    #parameter_wit_name.into(),
                    ": ".into(),
                    <#parameter_type as linera_witty::WitType>::wit_type_name()
                }
            },
        );

        let commas = iter::repeat_n(
            Some(quote! { ", ".into(), }),
            self.parameter_names.len().saturating_sub(1),
        )
        .chain(Some(None));

        let output = self
            .output_type
            .as_ref()
            .map(|output_type| {
                quote! { " -> ".into(), <#output_type as linera_witty::WitType>::wit_type_name() }
            })
            .into_iter();

        quote! {
            [
                std::borrow::Cow::Borrowed("    "),
                #wit_name.into(),
                ": func(".into(),
                #( #parameters, #commas )*
                ")".into(),
                #( #output, )*
                ";".into(),
            ]
            .as_slice()
            .join("")
        }
    }
}

impl<'input> From<&'_ wit_import::FunctionInformation<'input>> for FunctionInformation<'input> {
    fn from(imported_function: &'_ wit_import::FunctionInformation<'input>) -> Self {
        let signature = &imported_function.function.sig;

        FunctionInformation::new(
            &signature.ident,
            &signature.inputs,
            signature.output.clone(),
        )
    }
}

#[cfg(with_wit_export)]
impl<'input> From<&'_ wit_export::FunctionInformation<'input>> for FunctionInformation<'input> {
    fn from(exported_function: &'_ wit_export::FunctionInformation<'input>) -> Self {
        let signature = &exported_function.function.sig;

        let inputs = signature
            .inputs
            .iter()
            .skip(if exported_function.is_reentrant { 1 } else { 0 });

        let mut output = signature.output.clone();

        if exported_function.call_early_return.is_some() {
            let ReturnType::Type(_arrow, return_type) = &mut output else {
                abort!(output, "Missing `Result` in fallible function return type");
            };

            let Some(actual_output) = wit_export::ok_type_inside_result(&*return_type).cloned()
            else {
                abort!(
                    output,
                    "Missing `Ok` result type in fallible function return type"
                );
            };

            if is_unit_type(&actual_output) {
                output = ReturnType::Default;
            } else {
                *return_type = Box::new(actual_output);
            }
        }

        FunctionInformation::new(&signature.ident, inputs, output)
    }
}

/// Returns `true` if `the_type` is the unit `()` type.
#[cfg(with_wit_export)]
pub fn is_unit_type(the_type: &Type) -> bool {
    matches!(the_type, Type::Tuple(tuple) if tuple.elems.is_empty())
}
