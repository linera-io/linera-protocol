// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of a shared generic type parameter for the caller.

use proc_macro_error::abort;
use syn::{
    punctuated::Punctuated, AngleBracketedGenericArguments, AssocType, GenericArgument, Generics,
    Ident, PathArguments, PathSegment, PredicateType, Token, TraitBound, TraitBoundModifier, Type,
    TypeParamBound, TypePath, WhereClause, WherePredicate,
};

/// Information on the  generic type parameter to use for the caller parameter, if present.
#[derive(Clone, Copy, Debug)]
pub enum CallerTypeParameter<'input> {
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
        let user_data_type =
            caller_type_parameter.and_then(|caller| Self::extract_user_data_type(generics, caller));

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
    fn extract_caller_type_parameter(generics: &'input Generics) -> Option<&'input Ident> {
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
