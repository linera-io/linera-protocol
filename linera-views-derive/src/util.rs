// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span};
use syn::Type;

/// Extracts the first `Ident` in a type and convert to snake case.
pub fn snakify(r#type: &Type) -> Ident {
    transform_type_to_ident(r#type, |s: String| s.to_case(Case::Snake))
}

/// Convert a string into an identifier
pub fn string_to_ident(s: &str) -> Ident {
    syn::Ident::new(s, Span::call_site())
}

pub fn get_graphql_pair_for_underscore(id: &Ident) -> (Ident, String) {
    let id_str = format!("{}", id);
    let id_underscore = format!("_{}", id);
    (string_to_ident(&id_underscore), id_str)
}

/// Extends an identifier with a suffix.
pub fn concat(ident: &Ident, suffix: &'static str) -> Ident {
    transform_non_keyword_ident(ident, |s: String| format!("{}{}", s, suffix))
}

/// Applies a string transformation (`transform`) to the input `Type`
/// and transform it to an `Ident` corresponding to the first segment
/// of the `TypePath`.
fn transform_type_to_ident<Transform>(r#type: &Type, transform: Transform) -> Ident
where
    Transform: FnOnce(String) -> String,
{
    let type_name = match r#type {
        Type::Path(path) => path
            .path
            .segments
            .first()
            .expect("type path should have at least one segment."),
        _ => panic!("Expected type to be path"),
    };
    let type_ident = type_name.ident.clone();
    transform_non_keyword_ident(&type_ident, transform)
}

/// Applies a string transformation (`transform`) to the input `Ident`-
/// However, it will not apply the transform to rust keywords.
fn transform_non_keyword_ident<Transform>(ident: &Ident, transform: Transform) -> Ident
where
    Transform: FnOnce(String) -> String,
{
    let is_keyword = syn::parse_str::<Ident>(&ident.to_string()).is_err();
    if is_keyword {
        ident.clone()
    } else {
        Ident::new(&transform(ident.to_string()), Span::call_site())
    }
}
