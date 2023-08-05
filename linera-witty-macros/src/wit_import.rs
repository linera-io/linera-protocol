// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to import functions from a Wasm guest module.

use proc_macro2::{Span, TokenStream};
use proc_macro_error::abort;
use quote::{quote, quote_spanned, ToTokens};
use syn::{spanned::Spanned, FnArg, Ident, ReturnType, TraitItem, TraitItemMethod};

/// Pieces of information extracted from a function's definition.
struct FunctionInformation<'input> {
    function: &'input TraitItemMethod,
    parameter_definitions: TokenStream,
    parameter_bindings: TokenStream,
    return_type: TokenStream,
    interface: TokenStream,
    instance_constraint: TokenStream,
}

impl<'input> FunctionInformation<'input> {
    /// Extracts the necessary information from the `function` and stores it in a new
    /// [`FunctionInformation`] instance.
    pub fn new(function: &'input TraitItemMethod) -> Self {
        let (parameter_definitions, parameter_bindings, parameter_types) =
            Self::parse_parameters(function.sig.inputs.iter());

        let return_type = match &function.sig.output {
            ReturnType::Default => quote_spanned! { function.sig.output.span() => () },
            ReturnType::Type(_, return_type) => return_type.to_token_stream(),
        };

        let interface = quote_spanned! { function.sig.span() =>
            <(linera_witty::HList![#parameter_types], #return_type)
                as linera_witty::ImportedFunctionInterface>
        };

        let instance_constraint = quote_spanned! { function.sig.span() =>
            linera_witty::InstanceWithFunction<
                #interface::GuestParameters,
                #interface::GuestResults,
            >
        };

        FunctionInformation {
            function,
            parameter_definitions,
            parameter_bindings,
            return_type,
            interface,
            instance_constraint,
        }
    }

    /// Parses a function's parameters and returns the pieces constructed from the parameters.
    ///
    /// Returns the parameter definitions (the name and type pairs), the parameter bindings (the
    /// names) and the parameter types.
    fn parse_parameters(
        function_inputs: impl Iterator<Item = &'input FnArg>,
    ) -> (TokenStream, TokenStream, TokenStream) {
        let parameters = function_inputs.map(|input| match input {
            FnArg::Typed(parameter) => parameter,
            FnArg::Receiver(receiver) => abort!(
                receiver.self_token,
                "Imported interfaces can not have `self` parameters"
            ),
        });

        let mut parameter_definitions = quote! {};
        let mut parameter_bindings = quote! {};
        let mut parameter_types = quote! {};

        for parameter in parameters {
            let parameter_binding = &parameter.pat;
            let parameter_type = &parameter.ty;

            parameter_definitions.extend(quote! { #parameter, });
            parameter_bindings.extend(quote! { #parameter_binding, });
            parameter_types.extend(quote! { #parameter_type, });
        }

        (parameter_definitions, parameter_bindings, parameter_types)
    }

    /// Returns the name of the function.
    pub fn name(&self) -> &Ident {
        &self.function.sig.ident
    }

    /// Returns the code span of the function.
    pub fn span(&self) -> Span {
        self.function.span()
    }
}

impl<'input> From<&'input TraitItem> for FunctionInformation<'input> {
    fn from(item: &'input TraitItem) -> Self {
        match item {
            TraitItem::Method(function) => FunctionInformation::new(function),
            TraitItem::Const(const_item) => abort!(
                const_item.ident,
                "Const items are not supported in imported traits"
            ),
            TraitItem::Type(type_item) => abort!(
                type_item.ident,
                "Type items are not supported in imported traits"
            ),
            TraitItem::Macro(macro_item) => abort!(
                macro_item.mac.path,
                "Macro items are not supported in imported traits"
            ),
            _ => abort!(item, "Only function items are supported in imported traits"),
        }
    }
}
