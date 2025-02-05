// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to process [`Fields`] from `struct`s and `enum` variants.

use std::{borrow::Cow, ops::Deref};

use heck::ToKebabCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{spanned::Spanned, Field, Fields, Ident, LitStr, Meta, MetaList, Type};

/// A helper type with information about a list of [`Fields`].
pub struct FieldsInformation<'input> {
    kind: FieldsKind,
    fields: Vec<FieldInformation<'input>>,
}

impl<'input> FieldsInformation<'input> {
    /// Returns an iterator over the [`FieldInformation`] of the non-skipped fields.
    pub fn non_skipped_fields(
        &self,
    ) -> impl Iterator<Item = &FieldInformation<'input>> + Clone + '_ {
        self.fields.iter().filter(|field| !field.is_skipped())
    }

    /// Returns an iterator over the names of the fields.
    pub fn names(&self) -> impl Iterator<Item = &Ident> + '_ {
        self.fields.iter().map(FieldInformation::name)
    }

    /// Returns an iterator over the types of the non-skipped fields.
    pub fn types(&self) -> impl Iterator<Item = &Type> + Clone + '_ {
        self.non_skipped_fields().map(FieldInformation::field_type)
    }

    /// Returns an iterator over the WIT compatible names of the non-skipped fields.
    pub fn wit_names(&self) -> impl Iterator<Item = &LitStr> + '_ {
        self.non_skipped_fields().map(FieldInformation::wit_name)
    }

    /// Returns an iterator over the code to obtain the WIT type names of the non-skipped fields.
    pub fn wit_type_names(&self) -> impl Iterator<Item = TokenStream> + '_ {
        self.non_skipped_fields()
            .map(FieldInformation::wit_type_name)
    }

    /// Returns the code with a pattern to match a heterogeneous list using the `field_names` as
    /// bindings.
    pub fn hlist_type(&self) -> TokenStream {
        let field_types = self.types();
        quote! { linera_witty::HList![#( #field_types ),*] }
    }

    /// Returns the code with a pattern to match a heterogeneous list using the `field_names` as
    /// bindings.
    ///
    /// This function receives `field_names` instead of a `Fields` instance because some fields might
    /// not have names, so binding names must be created for them.
    pub fn hlist_bindings(&self) -> TokenStream {
        let field_names = self.non_skipped_fields().map(FieldInformation::name);
        quote! { linera_witty::hlist_pat![#( #field_names ),*] }
    }

    /// Returns the code that creates a heterogeneous list with the field values.
    ///
    /// Assumes that the bindings were obtained using [`Self::hlist_bindings`] or
    /// [`Self::construction`].
    pub fn hlist_value(&self) -> TokenStream {
        let field_names = self.non_skipped_fields().map(FieldInformation::name);
        quote! { linera_witty::hlist![#( #field_names ),*] }
    }

    /// Returns the code that creates bindings with default values for the skipped fields.
    pub fn bindings_for_skipped_fields(&self) -> TokenStream {
        self.fields
            .iter()
            .filter(|field| field.is_skipped())
            .map(FieldInformation::name)
            .map(|field_name| quote! { let #field_name = Default::default(); })
            .collect()
    }

    /// Returns the code with the body to construct the container of the fields.
    ///
    /// Assumes all the fields have appropriate bindings set up with the names from
    /// [`Self::names`].
    pub fn construction(&self) -> TokenStream {
        let names = self.names();

        match self.kind {
            FieldsKind::Unit => quote! {},
            FieldsKind::Named => quote! { { #( #names ),* } },
            FieldsKind::Unnamed => quote! { ( #( #names ),* ) },
        }
    }

    /// Returns the code with the body pattern to destructure the container of the fields.
    ///
    /// Does not include bindings for skipped fields.
    pub fn destructuring(&self) -> TokenStream {
        match self.kind {
            FieldsKind::Unit => quote! {},
            FieldsKind::Named => {
                let bindings = self.non_skipped_fields().map(FieldInformation::name);
                let has_skipped_fields = self.fields.iter().any(|field| field.is_skipped());
                let ignored_fields = has_skipped_fields.then(|| quote! { .. });

                quote! { { #( #bindings, )* #ignored_fields } }
            }
            FieldsKind::Unnamed => {
                let bindings = self.fields.iter().map(|field| {
                    if field.is_skipped() {
                        Cow::Owned(Ident::new("_", field.name.span()))
                    } else {
                        Cow::Borrowed(field.name())
                    }
                });

                quote! { ( #( #bindings ),* ) }
            }
        }
    }
}

impl<'input> From<&'input Fields> for FieldsInformation<'input> {
    fn from(fields: &'input Fields) -> Self {
        FieldsInformation {
            kind: fields.into(),
            fields: fields
                .iter()
                .enumerate()
                .map(FieldInformation::from)
                .collect(),
        }
    }
}

/// A helper type with information about a [`Field`].
pub struct FieldInformation<'input> {
    field: &'input Field,
    name: Cow<'input, Ident>,
    wit_name: LitStr,
    is_skipped: bool,
}

impl FieldInformation<'_> {
    /// Returns the name to use for this field.
    pub fn name(&self) -> &Ident {
        &self.name
    }

    /// Returns the type of this field.
    pub fn field_type(&self) -> &Type {
        &self.field.ty
    }

    /// Returns the string literal with the WIT compatible name.
    pub fn wit_name(&self) -> &LitStr {
        &self.wit_name
    }

    /// Returns the code to obtain the field's WIT type name.
    pub fn wit_type_name(&self) -> TokenStream {
        let field_type = &self.field.ty;

        quote! { <#field_type as linera_witty::WitType>::wit_type_name() }
    }

    /// Returns `true` if this field was marked to be skipped.
    pub fn is_skipped(&self) -> bool {
        self.is_skipped
    }
}

impl Deref for FieldInformation<'_> {
    type Target = Field;

    fn deref(&self) -> &Self::Target {
        self.field
    }
}

impl<'input> From<(usize, &'input Field)> for FieldInformation<'input> {
    fn from((index, field): (usize, &'input Field)) -> Self {
        let name = field
            .ident
            .as_ref()
            .map(Cow::Borrowed)
            .unwrap_or_else(|| Cow::Owned(format_ident!("field{index}")));

        let wit_name = LitStr::new(
            &field
                .ident
                .as_ref()
                .map(Ident::to_string)
                .unwrap_or_else(|| format!("inner{index}"))
                .to_kebab_case(),
            field.span(),
        );

        let is_skipped = field.attrs.iter().any(|attribute| {
            matches!(
                &attribute.meta,
                Meta::List(MetaList { path, tokens, ..})
                    if path.is_ident("witty") && tokens.to_string() == "skip"
            )
        });

        FieldInformation {
            field,
            name,
            wit_name,
            is_skipped,
        }
    }
}

/// The kind of a [`Fields`] list.
#[derive(Clone, Copy, Debug)]
pub enum FieldsKind {
    Unit,
    Named,
    Unnamed,
}

impl<'input> From<&'input Fields> for FieldsKind {
    fn from(fields: &'input Fields) -> Self {
        match fields {
            Fields::Unit => FieldsKind::Unit,
            Fields::Named(_) => FieldsKind::Named,
            Fields::Unnamed(_) => FieldsKind::Unnamed,
        }
    }
}
