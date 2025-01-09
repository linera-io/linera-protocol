// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `witty_specialize_with` attribute.

use proc_macro2::Span;
use quote::{quote, ToTokens};
use syn::{parse_quote, DeriveInput, Ident};

use super::{super::apply_specialization_attribute, Specialization, Specializations};

/// Checks that the [`DeriveInput`] of a `struct` is changed.
#[test]
fn derive_input_changes() {
    let mut input: DeriveInput = parse_quote! {
        #[witty_specialize_with(First = u8, Second = Vec<bool>)]
        #[witty_specialize_with(Third = (String, i32))]
        struct Dummy<'lifetime, First, Second, Third>
        where
            Option<First>: From<u8>,
            Box<[bool]>: From<Second>,
            Third: Display,
        {
            list_of_first: Vec<First>,
            second: Second,
            third: Third,
        }
    };

    let specializations = apply_specialization_attribute(&mut input);

    let expected_specializations = vec![
        Specialization {
            type_parameter: Ident::new("First", Span::call_site()),
            specialized_type: parse_quote!(u8),
        },
        Specialization {
            type_parameter: Ident::new("Second", Span::call_site()),
            specialized_type: parse_quote!(Vec<bool>),
        },
        Specialization {
            type_parameter: Ident::new("Third", Span::call_site()),
            specialized_type: parse_quote!((String, i32)),
        },
    ];

    assert_eq!(specializations.0, expected_specializations);

    let expected_changed_input = quote! {
        #[witty_specialize_with(First = u8, Second = Vec<bool>)]
        #[witty_specialize_with(Third = (String, i32))]
        struct Dummy<'lifetime, First, Second, Third>
        where
            Option<u8>: From<u8>,
            Box<[bool]>: From<Vec<bool> >
        {
            list_of_first: Vec<u8>,
            second: Vec<bool>,
            third: (String, i32),
        }
    };

    assert_eq!(
        input.to_token_stream().to_string(),
        expected_changed_input.to_string()
    );
}

/// Checks that [`Specialization`] generates correctly specialized [`Generics`].
#[test]
fn generics_are_specialized() {
    let specializations = Specializations(vec![
        Specialization {
            type_parameter: Ident::new("First", Span::call_site()),
            specialized_type: parse_quote!(u8),
        },
        Specialization {
            type_parameter: Ident::new("Second", Span::call_site()),
            specialized_type: parse_quote!(Vec<bool>),
        },
        Specialization {
            type_parameter: Ident::new("Third", Span::call_site()),
            specialized_type: parse_quote!((String, i32)),
        },
    ]);

    let generics_source: DeriveInput = parse_quote! {
        pub struct Dummy<'lifetime, First, Second, Third, Fourth>
        where
            Option<u8>: From<u8>,
            Box<[bool]>: From<Vec<bool>>;
    };

    let (impl_generics, type_generics, where_clause) =
        specializations.split_generics_from(&generics_source.generics);

    let expected_impl_generics = quote! { <'lifetime, Fourth> };
    let expected_type_generics = quote! { <'lifetime, u8, Vec<bool>, (String, i32), Fourth> };
    let expected_where_clause =
        quote! { where Option<u8>: From<u8>, Box<[bool]>: From<Vec<bool> > };

    assert_eq!(
        impl_generics.to_string(),
        expected_impl_generics.to_string()
    );
    assert_eq!(
        type_generics.to_token_stream().to_string(),
        expected_type_generics.to_string()
    );
    assert_eq!(
        where_clause.to_token_stream().to_string(),
        expected_where_clause.to_string()
    );
}

impl PartialEq for Specialization {
    fn eq(&self, other: &Self) -> bool {
        self.type_parameter == other.type_parameter
            && self.specialized_type.to_token_stream().to_string()
                == other.specialized_type.to_token_stream().to_string()
    }
}
