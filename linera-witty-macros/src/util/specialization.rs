// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Specialization of types before deriving traits for them.

#[cfg(test)]
#[path = "../unit_tests/specialization.rs"]
mod tests;

use std::{
    collections::HashSet,
    fmt::{self, Debug, Formatter},
    mem,
};

use proc_macro2::{Span, TokenStream};
use proc_macro_error::abort;
use quote::ToTokens;
use syn::{
    parse::{self, Parse, ParseStream, Parser},
    punctuated::Punctuated,
    spanned::Spanned,
    AngleBracketedGenericArguments, AssocConst, AssocType, Attribute, Constraint, Data, DataEnum,
    DataStruct, DataUnion, DeriveInput, Field, Fields, GenericArgument, GenericParam, Generics,
    Ident, MacroDelimiter, Meta, MetaList, Path, PathArguments, PredicateType, QSelf, ReturnType,
    Token, Type, TypeArray, TypeBareFn, TypeGroup, TypeImplTrait, TypeParamBound, TypeParen,
    TypePath, TypePtr, TypeReference, TypeSlice, TypeTuple, WhereClause, WherePredicate,
};

/// Collected specializations to apply before deriving a trait for a type.
#[derive(Debug)]
pub struct Specializations(Vec<Specialization>);

impl FromIterator<Specialization> for Specializations {
    fn from_iter<I>(specializations: I) -> Self
    where
        I: IntoIterator<Item = Specialization>,
    {
        Specializations(specializations.into_iter().collect())
    }
}

impl Specializations {
    /// Changes the [`DeriveInput`] based on the specializations requested through the
    /// `witty_specialize_with` attributes.
    ///
    /// The [`DeriveInput`] is changed so that its `where` clause and field types are specialized.
    /// Returns the[`Specializations`] instance created from parsing the `witty_specialize_with`
    /// attributes from the [`DeriveInput`].
    pub fn prepare_derive_input(input: &mut DeriveInput) -> Self {
        let this: Self = Self::parse_specialization_attributes(&input.attrs).collect();

        for specialization in &this.0 {
            specialization.apply_to_derive_input(input);
        }

        this
    }

    /// Creates a list of [`Specialization`]s based on the `witty_specialize_with` attributes found
    /// in the provided `attributes`.
    fn parse_specialization_attributes(
        attributes: &[Attribute],
    ) -> impl Iterator<Item = Specialization> {
        let mut specializations = Vec::new();

        let abort_with_error = |span: Span| -> ! {
            abort!(
                span,
                "Failed to parse Witty specialization attribute. \
                Expected: `#[witty_specialize_with(TypeParam = Type, ...)]`."
            );
        };

        for attribute in attributes {
            match &attribute.meta {
                Meta::List(MetaList {
                    path,
                    delimiter,
                    tokens,
                }) if path.is_ident("witty_specialize_with")
                    && matches!(delimiter, MacroDelimiter::Paren(_)) =>
                {
                    let parser = Punctuated::<Specialization, Token![,]>::parse_separated_nonempty;
                    specializations.push(
                        parser
                            .parse2(tokens.clone())
                            .unwrap_or_else(|_| abort_with_error(attribute.span()))
                            .into_iter(),
                    );
                }
                _ => {}
            }
        }

        specializations.into_iter().flatten()
    }

    /// Specializes the types in the [`Generics`] representation.
    #[cfg(with_wit_export)]
    pub fn apply_to_generics(&self, generics: &mut Generics) {
        for specialization in &self.0 {
            specialization.apply_to_generics(generics);
        }
    }

    /// Specializes the types in the `target_type`, either itself or its type parameters.
    #[cfg(with_wit_export)]
    pub fn apply_to_type(&self, target_type: &mut Type) {
        for specialization in &self.0 {
            specialization.change_types_in_type(target_type);
        }
    }

    /// Retrieves the information related to generics from the provided [`Generics`] after
    /// applying the specializations from this instance.
    ///
    /// Returns the generic parameters for the `impl` block, the generic arguments for the target
    /// type, and the where clause to use.
    pub fn split_generics_from<'generics>(
        &self,
        generics: &'generics Generics,
    ) -> (
        TokenStream,
        Option<AngleBracketedGenericArguments>,
        Option<&'generics WhereClause>,
    ) {
        let (_original_generic_parameters, original_type_generics, where_clause) =
            generics.split_for_impl();
        let type_generics = self.specialize_type_generics(original_type_generics);
        let generic_parameters = self.clean_generic_parameters(generics.clone());

        (
            generic_parameters.into_token_stream(),
            type_generics,
            where_clause,
        )
    }

    /// Specializes the target type's generic parameters.
    fn specialize_type_generics(
        &self,
        type_generics: impl ToTokens,
    ) -> Option<AngleBracketedGenericArguments> {
        let mut generic_type = syn::parse_quote!(TheType #type_generics);

        for specialization in &self.0 {
            specialization.change_types_in_type(&mut generic_type);
        }

        match generic_type {
            Type::Path(TypePath {
                qself: None,
                path:
                    Path {
                        leading_colon: None,
                        segments,
                    },
            }) if segments.len() == 1 => {
                let segment = segments
                    .into_iter()
                    .next()
                    .expect("Missing custom type's path");
                assert_eq!(segment.ident, "TheType");

                match segment.arguments {
                    PathArguments::None => None,
                    PathArguments::AngleBracketed(arguments) => Some(arguments),
                    PathArguments::Parenthesized(_) => {
                        unreachable!("Custom type has unexpected function type parameters")
                    }
                }
            }
            _ => unreachable!("Parsed custom type literal is incorrect"),
        }
    }

    /// Returns the generic parameters from the [`Generics`] information after applying the
    /// specializations from this instance.
    fn clean_generic_parameters(&self, mut generics: Generics) -> TokenStream {
        let original_generic_types = mem::take(&mut generics.params);
        let parameters_to_remove: HashSet<Ident> = self
            .0
            .iter()
            .map(|specialization| specialization.type_parameter.clone())
            .collect();

        generics
            .params
            .extend(
                original_generic_types
                    .into_iter()
                    .filter(|generic_type| match generic_type {
                        GenericParam::Lifetime(_) | GenericParam::Const(_) => true,
                        GenericParam::Type(type_parameter) => {
                            !parameters_to_remove.contains(&type_parameter.ident)
                        }
                    }),
            );

        let (generic_parameters, _incorrect_type_generics, _unaltered_where_clause) =
            generics.split_for_impl();

        generic_parameters.into_token_stream()
    }
}

/// A single specialization of a generic type parameter.
pub struct Specialization {
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
    /// Creates a new specialization for the `type_parameter`, to specialize it into the
    /// `specialized_type`.
    #[cfg(with_wit_export)]
    pub fn new(type_parameter: Ident, specialized_type: Type) -> Self {
        Specialization {
            type_parameter,
            specialized_type,
        }
    }

    /// Replaces a type parameter in the [`DeriveInput`] with a specialized type.
    ///
    /// Note that the specialization is only done to the `where` clause and the type's fields. The
    /// types generic parameters needs to be changed separately (see
    /// [`Specializations::specialize_type_generics`].
    pub fn apply_to_derive_input(&self, input: &mut DeriveInput) {
        self.apply_to_generics(&mut input.generics);
        self.change_types_in_fields(&mut input.data);
    }

    /// Replaces a type parameter in the [`Generics`] representation with a specialized type.
    pub fn apply_to_generics(&self, generics: &mut Generics) {
        self.remove_from_where_clause(generics.where_clause.as_mut());
        self.change_types_in_where_clause(generics.where_clause.as_mut());
    }

    /// Removes from a [`WhereClause`] all predicates for the [`Self::type_parameter`] that this
    /// specialization targets.
    fn remove_from_where_clause(&self, maybe_where_clause: Option<&mut WhereClause>) {
        if let Some(WhereClause { predicates, .. }) = maybe_where_clause {
            let original_predicates = mem::take(predicates);

            predicates.extend(
                original_predicates
                    .into_iter()
                    .filter(|predicate| !self.affects_predicate(predicate)),
            );
        }
    }

    /// Returns [`true`] if this [`Specialization`] affects the `predicate`.
    fn affects_predicate(&self, predicate: &WherePredicate) -> bool {
        let WherePredicate::Type(PredicateType {
            bounded_ty: Type::Path(type_path),
            ..
        }) = predicate
        else {
            return false;
        };
        let mut type_path = type_path;

        while let Some(inner_type) = &type_path.qself {
            type_path = match &*inner_type.ty {
                Type::Path(path) => path,
                _ => return false,
            };
        }

        let Some(segment) = type_path.path.segments.first() else {
            return false;
        };

        segment.ident == self.type_parameter && matches!(segment.arguments, PathArguments::None)
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
            Type::Tuple(TypeTuple { elems, .. }) => {
                for element in elems {
                    self.change_types_in_type(element);
                }
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
