// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types to process [`Fields`] from `struct`s and `enum` variants.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use std::{borrow::Cow, ops::Deref};
use syn::{Field, Fields, Ident};

/// A helper type with information about a list of [`Fields`].
pub struct FieldsInformation<'input> {
    kind: FieldsKind,
    fields: Vec<FieldInformation<'input>>,
}

impl FieldsInformation<'_> {
    /// Returns an iterator over the names of the fields.
    pub fn names(&self) -> impl Iterator<Item = &Ident> + '_ {
        self.fields.iter().map(FieldInformation::name)
    }

    /// Returns the code with a pattern to match a heterogenous list using the `field_names` as
    /// bindings.
    pub fn hlist_type(&self) -> TokenStream {
        let field_types = self.fields.iter().map(|field| &field.ty);
        quote! { linera_witty::HList![#( #field_types ),*] }
    }

    /// Returns the code with a pattern to match a heterogenous list using the `field_names` as
    /// bindings.
    ///
    /// This function receives `field_names` instead of a `Fields` instance because some fields might
    /// not have names, so binding names must be created for them.
    pub fn hlist_bindings(&self) -> TokenStream {
        let field_names = self.fields.iter().map(FieldInformation::name);
        quote! { linera_witty::hlist_pat![#( #field_names ),*] }
    }

    /// Returns the code that creates a heterogeneous list with the field values.
    ///
    /// Assumes that the bindings were obtained using [`Self::hlist_bindings`] or
    /// [`Self::construction`].
    pub fn hlist_value(&self) -> TokenStream {
        let field_names = self.fields.iter().map(FieldInformation::name);
        quote! { linera_witty::hlist![#( #field_names ),*] }
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
        let names = self.fields.iter().map(FieldInformation::name);

        match self.kind {
            FieldsKind::Unit => quote! {},
            FieldsKind::Named => quote! { { #( #names, )* } },
            FieldsKind::Unnamed => quote! { ( #( #names ),* ) },
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
}

impl FieldInformation<'_> {
    /// Returns the name to use for this field.
    pub fn name(&self) -> &Ident {
        &self.name
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

        FieldInformation { field, name }
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
