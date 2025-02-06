// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of a shared generic type parameter for the caller.

use std::collections::HashMap;

use proc_macro2::TokenStream;
use proc_macro_error::abort;
use quote::ToTokens;
use syn::{
    punctuated::Punctuated, AngleBracketedGenericArguments, AssocType, GenericArgument, Generics,
    Ident, PathArguments, PathSegment, PredicateType, Token, TraitBound, TraitBoundModifier, Type,
    TypeParam, TypeParamBound, TypePath, WhereClause, WherePredicate,
};

use crate::util::{Specialization, Specializations};

/// Information on the generic type parameter to use for the caller parameter, if present.
#[derive(Clone, Copy, Debug)]
pub struct CallerTypeParameter<'input> {
    caller: &'input Ident,
    user_data: Option<&'input Type>,
}

impl<'input> CallerTypeParameter<'input> {
    /// Parses a type's [`Generics`] to determine if a caller type parameter should be used.
    pub fn extract_from(generics: &'input Generics) -> Option<Self> {
        let where_bounds = Self::parse_bounds_from_where_clause(generics.where_clause.as_ref());

        generics
            .type_params()
            .filter_map(|parameter| Self::try_from_parameter(parameter, &where_bounds))
            .next()
    }

    /// Parses the bounds present in an optional `where_clause`.
    ///
    /// Returns a map between constrained types and its predicates.
    fn parse_bounds_from_where_clause(
        where_clause: Option<&'input WhereClause>,
    ) -> HashMap<&'input Ident, Vec<&'input TypeParamBound>> {
        where_clause
            .into_iter()
            .flat_map(|where_clause| where_clause.predicates.iter())
            .filter_map(|predicate| match predicate {
                WherePredicate::Type(predicate) => Self::extract_predicate_bounds(predicate),
                _ => None,
            })
            .collect()
    }

    /// Extracts the constrained type and its bounds from a predicate.
    ///
    /// Returns [`None`] if the predicate does not apply to a type that's a single identifier
    /// (which could be a generic type parameter).
    fn extract_predicate_bounds(
        predicate: &'input PredicateType,
    ) -> Option<(&'input Ident, Vec<&'input TypeParamBound>)> {
        let target_identifier = Self::extract_identifier(&predicate.bounded_ty)?;

        Some((target_identifier, predicate.bounds.iter().collect()))
    }

    /// Extracts the [`Ident`] that forms the `candidate_type`, if the [`Type`] is a single
    /// identifier.
    fn extract_identifier(candidate_type: &'input Type) -> Option<&'input Ident> {
        let Type::Path(TypePath { qself: None, path }) = candidate_type else {
            return None;
        };

        if path.leading_colon.is_some() || path.segments.len() != 1 {
            return None;
        }

        let segment = path.segments.first()?;

        if !matches!(&segment.arguments, PathArguments::None) {
            return None;
        }

        Some(&segment.ident)
    }

    /// Attempts to create a [`CallerTypeParameter`] from a generic [`TypeParam`].
    ///
    /// Succeeds if and only if the `where_bounds` map contains a predicate for the `parameter`.
    fn try_from_parameter(
        parameter: &'input TypeParam,
        where_bounds: &HashMap<&'input Ident, Vec<&'input TypeParamBound>>,
    ) -> Option<Self> {
        let caller = &parameter.ident;

        let bounds = where_bounds
            .get(caller)
            .into_iter()
            .flatten()
            .copied()
            .chain(parameter.bounds.iter());

        let instance_bound_path_segment = bounds
            .filter_map(Self::extract_trait_bound_path)
            .filter_map(Self::extract_instance_bound_path_segment)
            .next()?;

        let user_data =
            Self::extract_instance_bound_arguments(&instance_bound_path_segment.arguments)
                .and_then(Self::extract_instance_bound_user_data);

        Some(CallerTypeParameter { caller, user_data })
    }

    /// Extracts the path from a trait `bound`.
    fn extract_trait_bound_path(
        bound: &'input TypeParamBound,
    ) -> Option<impl Iterator<Item = &'input PathSegment> + Clone + 'input> {
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

    /// Extracts the [`PathSegment`] with the generic type parameters that could contain the user
    /// data type.
    fn extract_instance_bound_path_segment(
        segments: impl Iterator<Item = &'input PathSegment> + Clone,
    ) -> Option<&'input PathSegment> {
        Self::extract_aliased_instance_bound_path_segment(segments.clone())
            .or_else(|| Self::extract_direct_instance_bound_path_segment(segments))
    }

    /// Extracts the [`PathSegment`] for the identifier that uses an `InstanceFor..` trait alias
    /// generated by Witty.
    fn extract_aliased_instance_bound_path_segment(
        mut segments: impl Iterator<Item = &'input PathSegment>,
    ) -> Option<&'input PathSegment> {
        let segment = segments.next()?;

        if segment.ident.to_string().starts_with("InstanceFor") && segments.next().is_none() {
            Some(segment)
        } else {
            None
        }
    }

    /// Extracts the [`PathSegment`] for the identifier that uses an `Instance` trait directly.
    fn extract_direct_instance_bound_path_segment(
        segments: impl Iterator<Item = &'input PathSegment>,
    ) -> Option<&'input PathSegment> {
        let mut segments = segments.peekable();

        if matches!(
            segments.peek(),
            Some(PathSegment { ident, arguments: PathArguments::None })
                if ident == "linera_witty",
        ) {
            segments.next();
        }

        let segment = segments.next()?;

        if segment.ident == "Instance" && segments.next().is_none() {
            Some(segment)
        } else {
            None
        }
    }

    /// Extracts the generic arguments from `arguments`.
    fn extract_instance_bound_arguments(
        arguments: &'input PathArguments,
    ) -> Option<&'input Punctuated<GenericArgument, Token![,]>> {
        match arguments {
            PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                colon2_token: None,
                args,
                ..
            }) => Some(args),
            _ => None,
        }
    }

    /// Extracts the custom user data [`Type`] from the caller bound's generic `arguments`.
    fn extract_instance_bound_user_data(
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
    pub fn caller(&self) -> &'input Ident {
        self.caller
    }

    /// Returns the type used for custom user data, if there is a caller type parameter.
    pub fn user_data(&self) -> Option<&'input Type> {
        self.user_data
    }

    /// Specializes the [`Generics`] to replace the [`CallerTypeParameter`] with the concrete
    /// `caller_type`, and splits it into the parts used in implementation blocks.
    pub fn specialize_and_split_generics(
        &self,
        mut generics: Generics,
        caller_type: Type,
    ) -> (TokenStream, TokenStream, TokenStream) {
        let specializations = self.build_specializations(caller_type);

        specializations.apply_to_generics(&mut generics);

        let (impl_generics, type_generics, where_clause) =
            specializations.split_generics_from(&generics);

        (
            impl_generics,
            type_generics.into_token_stream(),
            where_clause.into_token_stream(),
        )
    }

    /// Specializes the [`CallerTypeParameter`] in the `target_type` with the concrete
    /// `caller_type`.
    pub fn specialize_type(&self, target_type: &mut Type, caller_type: Type) {
        self.build_specializations(caller_type)
            .apply_to_type(target_type);
    }

    /// Builds the [`Specializations`] instance to replace the [`CallerTypeParameter`] with the
    /// concrete `caller_type`.
    fn build_specializations(&self, caller_type: Type) -> Specializations {
        Specializations::from_iter(Some(Specialization::new(self.caller.clone(), caller_type)))
    }
}
