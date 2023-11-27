// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Specialization of types before deriving traits for them.

use quote::ToTokens;
use std::{
    fmt::{self, Debug, Formatter},
    mem,
};
use syn::{
    parse::{self, Parse, ParseStream},
    AngleBracketedGenericArguments, AssocConst, AssocType, Constraint, Data, DataEnum, DataStruct,
    DataUnion, DeriveInput, Field, Fields, GenericArgument, Ident, Path, PathArguments,
    PredicateType, QSelf, ReturnType, Token, Type, TypeArray, TypeBareFn, TypeGroup, TypeImplTrait,
    TypeParamBound, TypeParen, TypePath, TypePtr, TypeReference, TypeSlice, WhereClause,
    WherePredicate,
};

/// A single specialization of a generic type parameter.
struct Specialization {
    /// The type parameter to be specialized.
    type_parameter: Ident,
    /// The type to use as the specialized argument.
    specialized_type: Type,
}

impl Parse for Specialization {
    fn parse(input: ParseStream) -> parse::Result<Self> {
        let type_parameter = input.parse()?;
        let _: Token![=] = input.parse()?;
        let specialized_type = input.parse()?;

        Ok(Specialization {
            type_parameter,
            specialized_type,
        })
    }
}

impl Specialization {
    /// Replaces a type parameter in the [`DeriveInput`] with a specialized type.
    ///
    /// Note that the specialization is only done to the `where` clause and the type's fields. The
    /// types generic parameters needs to be changed separately (see
    /// [`Specializatons::specialize_type_generics`].
    pub fn apply_to(&self, input: &mut DeriveInput) {
        self.remove_from_where_clause(input.generics.where_clause.as_mut());
        self.change_types_in_where_clause(input.generics.where_clause.as_mut());
        self.change_types_in_fields(&mut input.data);
    }

    /// Removes from a [`WhereClause`] all predicates for the [`Self::type_parameter`] that this
    /// specialization targets.
    fn remove_from_where_clause(&self, maybe_where_clause: Option<&mut WhereClause>) {
        if let Some(WhereClause { predicates, .. }) = maybe_where_clause {
            let original_predicates = mem::take(predicates);

            predicates.extend(original_predicates.into_iter().filter(
                |predicate| match predicate {
                    WherePredicate::Type(PredicateType { bounded_ty, .. }) => !matches!(
                        bounded_ty,
                        Type::Path(TypePath { qself: None, path })
                            if path.is_ident(&self.type_parameter),
                    ),
                    _ => true,
                },
            ));
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside the
    /// predicates of the [`WhereClause`].
    fn change_types_in_where_clause(&self, maybe_where_clause: Option<&mut WhereClause>) {
        let type_predicates = maybe_where_clause
            .map(|where_clause| where_clause.predicates.iter_mut())
            .into_iter()
            .flatten()
            .filter_map(|predicate| match predicate {
                WherePredicate::Type(type_predicate) => Some(type_predicate),
                _ => None,
            });

        for predicate in type_predicates {
            self.change_types_in_type(&mut predicate.bounded_ty);
            self.change_types_in_bounds(predicate.bounds.iter_mut());
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside the
    /// fields of the [`DeriveInput`]'s [`Data`].
    fn change_types_in_fields(&self, data: &mut Data) {
        let fields: Box<dyn Iterator<Item = &mut Field>> = match data {
            Data::Struct(DataStruct {
                fields: Fields::Named(fields),
                ..
            })
            | Data::Union(DataUnion { fields, .. }) => Box::new(fields.named.iter_mut()),
            Data::Struct(DataStruct {
                fields: Fields::Unnamed(fields),
                ..
            }) => Box::new(fields.unnamed.iter_mut()),
            Data::Enum(DataEnum { variants, .. }) => Box::new(
                variants
                    .iter_mut()
                    .flat_map(|variant| variant.fields.iter_mut()),
            ),
            _ => Box::new(None.into_iter()),
        };

        for Field { ty, .. } in fields {
            self.change_types_in_type(ty);
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside the
    /// generic type parameter bounds.
    fn change_types_in_bounds<'bound>(
        &self,
        bounds: impl Iterator<Item = &'bound mut TypeParamBound>,
    ) {
        for bound in bounds {
            if let TypeParamBound::Trait(trait_bound) = bound {
                self.change_types_in_path(&mut trait_bound.path);
            }
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside the
    /// provided [`Type`].
    pub fn change_types_in_type(&self, the_type: &mut Type) {
        match the_type {
            Type::Array(TypeArray { elem, .. })
            | Type::Group(TypeGroup { elem, .. })
            | Type::Paren(TypeParen { elem, .. })
            | Type::Ptr(TypePtr { elem, .. })
            | Type::Reference(TypeReference { elem, .. })
            | Type::Slice(TypeSlice { elem, .. }) => self.change_types_in_type(elem.as_mut()),
            Type::BareFn(TypeBareFn { inputs, output, .. }) => self.change_types_in_function_type(
                inputs.iter_mut().map(|bare_fn_arg| &mut bare_fn_arg.ty),
                output,
            ),
            Type::ImplTrait(TypeImplTrait { bounds, .. }) => {
                self.change_types_in_bounds(bounds.iter_mut())
            }
            Type::Path(TypePath { qself: None, path }) if path.is_ident(&self.type_parameter) => {
                *the_type = self.specialized_type.clone();
            }
            Type::Path(TypePath { qself, path }) => {
                if let Some(QSelf { ty, .. }) = qself {
                    self.change_types_in_type(ty);
                }
                self.change_types_in_path(path);
            }
            _ => {}
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside the
    /// [`Path`]'s type arguments.
    fn change_types_in_path(&self, path: &mut Path) {
        for segment in path.segments.iter_mut() {
            match &mut segment.arguments {
                PathArguments::None => {}
                PathArguments::AngleBracketed(angle_bracketed) => {
                    self.change_types_in_angle_bracketed_generic_arguments(angle_bracketed)
                }
                PathArguments::Parenthesized(function_type) => self.change_types_in_function_type(
                    function_type.inputs.iter_mut(),
                    &mut function_type.output,
                ),
            }
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside the
    /// [`AngleBracketedGenericArguments`] of a [`PathSegment`][`syn::PathSegment`].
    fn change_types_in_angle_bracketed_generic_arguments(
        &self,
        arguments: &mut AngleBracketedGenericArguments,
    ) {
        for argument in arguments.args.iter_mut() {
            match argument {
                GenericArgument::Type(the_type) => self.change_types_in_type(the_type),
                GenericArgument::AssocType(AssocType { generics, ty, .. }) => {
                    if let Some(arguments) = generics {
                        self.change_types_in_angle_bracketed_generic_arguments(arguments);
                    }
                    self.change_types_in_type(ty);
                }
                GenericArgument::AssocConst(AssocConst {
                    generics: Some(arguments),
                    ..
                }) => {
                    self.change_types_in_angle_bracketed_generic_arguments(arguments);
                }
                GenericArgument::Constraint(Constraint {
                    generics, bounds, ..
                }) => {
                    if let Some(arguments) = generics {
                        self.change_types_in_angle_bracketed_generic_arguments(arguments);
                    }
                    self.change_types_in_bounds(bounds.iter_mut());
                }
                _ => {}
            }
        }
    }

    /// Replaces the [`Self::type_parameter`] with the [`Self::specialized_type`] inside a
    /// function's input parameter [`Type`]s and output [`ReturnType`].
    /// [`Path`]'s type arguments.
    fn change_types_in_function_type<'input>(
        &self,
        inputs: impl Iterator<Item = &'input mut Type>,
        output: &mut ReturnType,
    ) {
        for ty in inputs {
            self.change_types_in_type(ty);
        }

        if let ReturnType::Type(_, return_type) = output {
            self.change_types_in_type(return_type.as_mut());
        }
    }
}

impl Debug for Specialization {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter
            .debug_struct("Specialization")
            .field("type_parameter", &self.type_parameter)
            .field(
                "specialized_type",
                &self.specialized_type.to_token_stream().to_string(),
            )
            .finish()
    }
}
