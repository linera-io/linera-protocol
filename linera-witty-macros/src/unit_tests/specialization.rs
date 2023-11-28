// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the `witty_specialize_with` attribute.

use super::{super::apply_specialization_attribute, Specialization};
use proc_macro2::Span;
use quote::{quote, ToTokens};
use syn::{parse_quote, DeriveInput, Ident};

/// Check that the [`DeriveInput`] of a `struct` is changed.
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

impl PartialEq for Specialization {
    fn eq(&self, other: &Self) -> bool {
        self.type_parameter == other.type_parameter
            && self.specialized_type.to_token_stream().to_string()
                == other.specialized_type.to_token_stream().to_string()
    }
}
