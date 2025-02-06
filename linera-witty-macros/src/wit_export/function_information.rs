// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Extraction of information and generation of code related to a single exported host function.

use heck::ToKebabCase;
use proc_macro2::{Span, TokenStream};
use proc_macro_error::abort;
use quote::{quote, quote_spanned, ToTokens};
use syn::{
    spanned::Spanned, FnArg, GenericArgument, GenericParam, Ident, ImplItem, ImplItemFn, LitStr,
    PatType, Path, PathArguments, PathSegment, ReturnType, Signature, Token, Type, TypePath,
    TypeReference,
};

/// Pieces of information extracted from a function's definition.
pub struct FunctionInformation<'input> {
    pub(crate) function: &'input ImplItemFn,
    pub(crate) is_reentrant: bool,
    pub(crate) call_early_return: Option<Token![?]>,
    wit_name: String,
    parameter_bindings: TokenStream,
    interface_type: TokenStream,
}

impl<'input> FunctionInformation<'input> {
    /// Parses a function definition from an [`ImplItem`] and collects pieces of information into a
    /// [`FunctionInformation`] instance.
    pub fn from_item(item: &'input ImplItem, caller_type_parameter: Option<&'input Ident>) -> Self {
        match item {
            ImplItem::Fn(function) => FunctionInformation::new(function, caller_type_parameter),
            ImplItem::Const(const_item) => abort!(
                const_item.ident,
                "Const items are not supported in exported types"
            ),
            ImplItem::Type(type_item) => abort!(
                type_item.ident,
                "Type items are not supported in exported types"
            ),
            ImplItem::Macro(macro_item) => abort!(
                macro_item.mac.path,
                "Macro items are not supported in exported types"
            ),
            _ => abort!(item, "Only function items are supported in exported types"),
        }
    }

    /// Parses a function definition and collects pieces of information into a
    /// [`FunctionInformation`] instance.
    pub fn new(function: &'input ImplItemFn, caller_type: Option<&'input Ident>) -> Self {
        let wit_name = function.sig.ident.to_string().to_kebab_case();
        let is_reentrant = Self::is_reentrant(&function.sig)
            || Self::uses_caller_parameter(&function.sig, caller_type);
        let (parameter_bindings, parameter_types) =
            Self::parse_parameters(is_reentrant, function.sig.inputs.iter());
        let (results, is_fallible) = Self::parse_output(&function.sig.output);

        let interface_type = quote_spanned! { function.sig.span() =>
            (linera_witty::HList![#parameter_types], #results)
        };

        FunctionInformation {
            function,
            is_reentrant,
            call_early_return: is_fallible.then(|| Token![?](Span::call_site())),
            wit_name,
            parameter_bindings,
            interface_type,
        }
    }

    /// Checks if a function should be considered as a reentrant function.
    ///
    /// A reentrant function has a generic type parameter that's used as the type of the first
    /// parameter.
    fn is_reentrant(signature: &Signature) -> bool {
        if signature.generics.params.len() != 1 {
            return false;
        }

        let Some(GenericParam::Type(generic_type)) = signature.generics.params.first() else {
            return false;
        };

        Self::first_parameter_is_caller(signature, &generic_type.ident)
    }

    /// Checks if a function uses a `caller_type` in the first parameter.
    ///
    /// If it does, the function is assumed to be reentrant.
    fn uses_caller_parameter(signature: &Signature, caller_type: Option<&Ident>) -> bool {
        if let Some(caller_type) = caller_type {
            Self::first_parameter_is_caller(signature, caller_type)
        } else {
            false
        }
    }

    /// Checks if the type of a function's first parameter is the `caller_type`.
    fn first_parameter_is_caller(signature: &Signature, caller_type: &Ident) -> bool {
        let Some(first_parameter) = signature.inputs.first() else {
            return false;
        };

        let FnArg::Typed(PatType {
            ty: first_parameter_type,
            ..
        }) = first_parameter
        else {
            abort!(
                first_parameter,
                "`self` parameters aren't supported by Witty"
            );
        };

        let Type::Reference(TypeReference {
            mutability: Some(_),
            elem: referenced_type,
            ..
        }) = &**first_parameter_type
        else {
            return false;
        };

        let Type::Path(TypePath { path, .. }) = &**referenced_type else {
            return false;
        };

        path.is_ident(caller_type)
    }

    /// Parses a function's parameters and returns the generated code with a list of bindings to the
    /// parameters and a list of the parameters types.
    fn parse_parameters(
        is_reentrant: bool,
        inputs: impl Iterator<Item = &'input FnArg> + Clone,
    ) -> (TokenStream, TokenStream) {
        let parameters = inputs
            .skip(if is_reentrant { 1 } else { 0 })
            .map(|input| match input {
                FnArg::Typed(parameter) => parameter,
                FnArg::Receiver(receiver) => abort!(
                    receiver.self_token,
                    "Exported interfaces can not have `self` parameters"
                ),
            });

        let bindings = parameters.clone().map(|parameter| &parameter.pat);
        let types = parameters.map(|parameter| &parameter.ty);

        (quote! { #( #bindings ),* }, quote! { #( #types ),* })
    }

    /// Parses a function's return type, returning the type to use as the WIT result and whether
    /// the function is fallible.
    fn parse_output(output: &ReturnType) -> (TokenStream, bool) {
        match output {
            ReturnType::Default => (quote_spanned! { output.span() => () }, false),
            ReturnType::Type(_, return_type) => match ok_type_inside_result(return_type) {
                Some(inner_type) => (inner_type.to_token_stream(), true),
                None => (return_type.to_token_stream(), false),
            },
        }
    }

    /// Generates the code to export a host function using the Wasmer runtime.
    #[cfg(with_wasmer)]
    pub fn generate_for_wasmer(
        &self,
        namespace: &LitStr,
        type_name: &Ident,
        caller: &Type,
    ) -> TokenStream {
        let input_to_guest_parameters = quote! {
            linera_witty::wasmer::WasmerParameters::from_wasmer(input)
        };
        let guest_results_to_output = quote! {
            linera_witty::wasmer::WasmerResults::into_wasmer(guest_results)
        };
        let output_results_trait = quote! { linera_witty::wasmer::WasmerResults };

        self.generate(
            namespace,
            type_name,
            caller,
            input_to_guest_parameters,
            guest_results_to_output,
            output_results_trait,
        )
    }

    /// Generates the code to export a host function using the Wasmtime runtime.
    #[cfg(with_wasmtime)]
    pub fn generate_for_wasmtime(
        &self,
        namespace: &LitStr,
        type_name: &Ident,
        caller: &Type,
    ) -> TokenStream {
        let input_to_guest_parameters = quote! {
            linera_witty::wasmtime::WasmtimeParameters::from_wasmtime(input)
        };
        let guest_results_to_output = quote! {
            linera_witty::wasmtime::WasmtimeResults::into_wasmtime(guest_results)
        };
        let output_results_trait = quote! { linera_witty::wasmtime::WasmtimeResults };

        self.generate(
            namespace,
            type_name,
            caller,
            input_to_guest_parameters,
            guest_results_to_output,
            output_results_trait,
        )
    }

    /// Generates the code to export a host function using a mock Wasm instance for testing.
    #[cfg(with_testing)]
    pub fn generate_for_mock_instance(
        &self,
        namespace: &LitStr,
        type_name: &Ident,
        caller: &Type,
    ) -> TokenStream {
        let input_to_guest_parameters = quote! { input };
        let guest_results_to_output = quote! { guest_results };
        let output_results_trait = quote! { linera_witty::MockResults };

        self.generate(
            namespace,
            type_name,
            caller,
            input_to_guest_parameters,
            guest_results_to_output,
            output_results_trait,
        )
    }

    /// Generates the code to export using a host function.
    fn generate(
        &self,
        namespace: &LitStr,
        type_name: &Ident,
        caller: &Type,
        input_to_guest_parameters: TokenStream,
        guest_results_to_output: TokenStream,
        output_results_trait: TokenStream,
    ) -> TokenStream {
        let wit_name = &self.wit_name;
        let interface_type = &self.interface_type;
        let host_parameters = &self.parameter_bindings;
        let call_early_return = &self.call_early_return;
        let function_name = &self.function.sig.ident;
        let caller_parameter = self.is_reentrant.then(|| quote! { &mut caller, });

        let output_type = quote_spanned! { self.function.sig.output.span() =>
            <
                <
                    #interface_type as linera_witty::ExportedFunctionInterface
                >::GuestResults as #output_results_trait
            >::Results
        };

        quote_spanned! { self.function.span() =>
            linera_witty::ExportFunction::export(
                target,
                #namespace,
                #wit_name,
                #[allow(clippy::type_complexity)]
                |mut caller: #caller, input| -> Result<#output_type, linera_witty::RuntimeError> {
                    type Interface = #interface_type;

                    let guest_parameters = #input_to_guest_parameters;
                    let (linera_witty::hlist_pat![#host_parameters], result_storage) =
                        <Interface as linera_witty::ExportedFunctionInterface>::lift_parameters(
                            guest_parameters,
                            &linera_witty::InstanceWithMemory::memory(&mut caller)?,
                        )?;

                    #[allow(clippy::let_unit_value)]
                    let host_results = #type_name::#function_name(
                        #caller_parameter
                        #host_parameters
                    ) #call_early_return;
                    let guest_results =
                        <Interface as linera_witty::ExportedFunctionInterface>::lower_results(
                            host_results,
                            result_storage,
                            &mut linera_witty::InstanceWithMemory::memory(&mut caller)?,
                        )?;

                    #[allow(clippy::unit_arg)]
                    Ok(#guest_results_to_output)
                }
            )?;
        }
    }
}

/// Returns the type inside the `Ok` variant of the `maybe_result_type`.
///
/// The type is only considered if it's a [`Result`] type with `RuntimeError` as its error variant.
pub(crate) fn ok_type_inside_result(maybe_result_type: &Type) -> Option<&Type> {
    let Type::Path(TypePath { qself: None, path }) = maybe_result_type else {
        return None;
    };

    let (ok_type, error_type) = result_type_arguments(path)?;

    if let Type::Path(TypePath { qself: None, path }) = error_type {
        if !path.is_ident("RuntimeError") {
            return None;
        }
    } else {
        return None;
    }

    Some(ok_type)
}

/// Returns the generic type arguments of the [`Result`] type in `result_path`.
fn result_type_arguments(result_path: &Path) -> Option<(&Type, &Type)> {
    if !type_is_result(result_path) {
        return None;
    }

    let PathArguments::AngleBracketed(type_arguments) = &result_path.segments.last()?.arguments
    else {
        return None;
    };

    if type_arguments.args.len() != 2 {
        return None;
    }

    let mut arguments = type_arguments.args.iter();

    let GenericArgument::Type(ok_type) = arguments.next()? else {
        return None;
    };

    let GenericArgument::Type(error_type) = arguments.next()? else {
        return None;
    };

    Some((ok_type, error_type))
}

/// Checks if `result_path` is a [`Result`] type.
fn type_is_result(result_path: &Path) -> bool {
    let segment_count = result_path.segments.len();

    if segment_count == 1 {
        result_path.leading_colon.is_none() && path_matches_segments(result_path, &["Result"])
    } else if result_path.segments.len() == 3 {
        path_matches_segments(result_path, &["std", "result", "Result"])
    } else {
        false
    }
}

/// Checks if `path` matches the provided path `segments`.
fn path_matches_segments(path: &Path, segments: &[&str]) -> bool {
    if path.segments.len() != segments.len() {
        return false;
    }

    for (index, (segment, expected)) in path.segments.iter().zip(segments).enumerate() {
        let with_type_parameters = index == segments.len() - 1;

        if !is_path_segment(segment, expected, with_type_parameters) {
            return false;
        }
    }

    true
}

/// Checks if `segment` is the `expected_identifier` and if it should have generic type parameters.
fn is_path_segment(
    segment: &PathSegment,
    expected_identifier: &str,
    with_type_parameters: bool,
) -> bool {
    let arguments_are_correct = if with_type_parameters {
        matches!(segment.arguments, PathArguments::AngleBracketed(_))
    } else {
        matches!(segment.arguments, PathArguments::None)
    };

    segment.ident == expected_identifier && arguments_are_correct
}
