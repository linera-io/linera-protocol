// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Generation of code to export host functions to a Wasm guest instance.

#![cfg(any(feature = "mock-instance", feature = "wasmer", feature = "wasmtime"))]

mod function_information;

use self::function_information::FunctionInformation;
use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::quote;
use syn::{
    punctuated::Punctuated, token::Paren, AngleBracketedGenericArguments, AssocType,
    GenericArgument, Generics, Ident, ItemImpl, LitStr, PathArguments, PathSegment,
    PredicateType, Token, TraitBound, TraitBoundModifier, Type, TypeParamBound, TypePath,
    TypeTuple, WhereClause, WherePredicate,
};

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
            .map(|item| {
                FunctionInformation::from_item(item, caller_type_parameter.caller())
            })
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
            let user_data_type = self.user_data_type();
            let export_target =
                quote! { linera_witty::wasmer::InstanceBuilder<#user_data_type> };
            let target_caller_type = quote! {
                linera_witty::wasmer::FunctionEnvMut<
                    '_,
                    linera_witty::wasmer::InstanceSlot<#user_data_type>,
                >
            };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_wasmer(
                    self.namespace,
                    self.type_name,
                    &target_caller_type,
                )
            });

            Some(self.generate_for(
                export_target,
                &target_caller_type,
                exported_functions,
            ))
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
            let user_data_type = self.user_data_type();
            let export_target =
                quote! { linera_witty::wasmtime::Linker<#user_data_type> };
            let target_caller_type =
                quote! { linera_witty::wasmtime::Caller<'_, #user_data_type> };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_wasmtime(
                    self.namespace,
                    self.type_name,
                    &target_caller_type,
                )
            });

            Some(self.generate_for(
                export_target,
                &target_caller_type,
                exported_functions,
            ))
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
            let user_data_type = self.user_data_type();
            let export_target = quote! { linera_witty::MockInstance<#user_data_type> };
            let target_caller_type =
                quote! { linera_witty::MockInstance<#user_data_type> };
            let exported_functions = self.functions.iter().map(|function| {
                function.generate_for_mock_instance(
                    self.namespace,
                    self.type_name,
                    &target_caller_type,
                )
            });

            Some(self.generate_for(
                export_target,
                &target_caller_type,
                exported_functions,
            ))
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
        target_caller_type: &TokenStream,
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

    /// Returns the type to use for the custom user data.
    fn user_data_type(&self) -> Type {
        self.caller_type_parameter
            .user_data()
            .cloned()
            .unwrap_or_else(|| {
                // Unit type
                Type::Tuple(TypeTuple {
                    paren_token: Paren::default(),
                    elems: Punctuated::new(),
                })
            })
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
    WithUserData {
        caller: &'input Ident,
        user_data: &'input Type,
    },
}

impl<'input> CallerTypeParameter<'input> {
    /// Parses a type's [`Generics`] to determine if a caller type parameter should be used.
    pub fn new(generics: &'input Generics) -> Self {
        let caller_type_parameter = Self::extract_caller_type_parameter(generics);
        let user_data_type = caller_type_parameter
            .and_then(|caller| Self::extract_user_data_type(generics, caller));

        match (caller_type_parameter, user_data_type) {
            (None, None) => CallerTypeParameter::NotPresent,
            (Some(caller), None) => CallerTypeParameter::WithoutUserData(caller),
            (Some(caller), Some(user_data)) => {
                CallerTypeParameter::WithUserData { caller, user_data }
            }
            (None, Some(_)) => unreachable!("Missing caller type parameter"),
        }
    }

    /// Extracts the [`Ident`]ifier used for the caller type parameter, if present.
    fn extract_caller_type_parameter(
        generics: &'input Generics,
    ) -> Option<&'input Ident> {
        if generics.type_params().count() > 1 {
            abort!(
                generics.params,
                "`#[wit_export]` supports only one generic type parameter \
                which is assumed to be the caller instance"
            );
        }

        generics
            .type_params()
            .next()
            .map(|parameter| &parameter.ident)
    }

    /// Extracts the [`Ident`]ifier used for the caller type parameter, if present.
    fn extract_user_data_type(
        generics: &'input Generics,
        caller_parameter: &Ident,
    ) -> Option<&'input Type> {
        Self::extract_caller_bounds(generics.where_clause.as_ref()?, caller_parameter)?
            .filter_map(Self::extract_caller_bound_path)
            .filter_map(Self::extract_caller_bound_arguments)
            .filter_map(Self::extract_caller_user_data_type_argument)
            .next()
    }

    /// Extracts the type bounds inside a `where` clause specific to the generic
    /// `caller_parameter`.
    fn extract_caller_bounds(
        where_clause: &'input WhereClause,
        caller_parameter: &Ident,
    ) -> Option<impl Iterator<Item = &'input TypeParamBound> + 'input> {
        where_clause
            .predicates
            .iter()
            .filter_map(|predicate| match predicate {
                WherePredicate::Type(PredicateType {
                    bounded_ty: Type::Path(TypePath { qself: None, path }),
                    bounds,
                    ..
                }) if path.is_ident(caller_parameter) => Some(bounds.iter()),
                _ => None,
            })
            .next()
    }

    /// Extracts the path from a trait `bound`.
    fn extract_caller_bound_path(
        bound: &'input TypeParamBound,
    ) -> Option<impl Iterator<Item = &'input PathSegment> + 'input> {
        match bound {
            TypeParamBound::Trait(TraitBound {
                paren_token: None,
                modifier: TraitBoundModifier::None,
                lifetimes: None,
                path,
            }) => Some(path.segments.iter()),
            _ => None,
        }
    }

    /// Extracts the generic arguments inside the caller parameter path's `segments`.
    fn extract_caller_bound_arguments(
        segments: impl Iterator<Item = &'input PathSegment>,
    ) -> Option<&'input Punctuated<GenericArgument, Token![,]>> {
        let mut segments = segments.peekable();

        if matches!(
            segments.peek(),
            Some(PathSegment { ident, arguments: PathArguments::None })
                if ident == "linera_witty",
        ) {
            segments.next();
        }

        match segments.next()? {
            PathSegment {
                ident,
                arguments:
                    PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                        colon2_token: None,
                        args,
                        ..
                    }),
            } if ident == "Instance" => {
                if segments.next().is_some() {
                    return None;
                }

                Some(args)
            }
            _ => None,
        }
    }

    /// Extracts the custom user data [`Type`] from the caller bound's generic `arguments`.
    fn extract_caller_user_data_type_argument(
        arguments: &'input Punctuated<GenericArgument, Token![,]>,
    ) -> Option<&'input Type> {
        if arguments.len() != 1 {
            abort!(
                arguments,
                "Caller type parameter should have a user data type. \
                E.g. `Caller: linera_witty::Instance<UserData = CustomData>`"
            );
        }

        match arguments
            .iter()
            .next()
            .expect("Missing argument in arguments list")
        {
            GenericArgument::AssocType(AssocType {
                ident,
                generics: None,
                ty: user_data,
                ..
            }) if ident == "UserData" => Some(user_data),
            _ => abort!(
                arguments,
                "Caller type parameter should have a user data type. \
                E.g. `Caller: linera_witty::Instance<UserData = CustomData>`"
            ),
        }
    }

    /// Returns the [`Ident`]ifier of the generic type parameter used for the caller.
    pub fn caller(&self) -> Option<&'input Ident> {
        match self {
            CallerTypeParameter::NotPresent => None,
            CallerTypeParameter::WithoutUserData(caller) => Some(caller),
            CallerTypeParameter::WithUserData { caller, .. } => Some(caller),
        }
    }

    /// Returns the type used for custom user data, if there is a caller type parameter.
    pub fn user_data(&self) -> Option<&'input Type> {
        match self {
            CallerTypeParameter::NotPresent => None,
            CallerTypeParameter::WithoutUserData(_) => None,
            CallerTypeParameter::WithUserData { user_data, .. } => Some(user_data),
        }
    }
}
