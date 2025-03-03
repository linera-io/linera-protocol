// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use convert_case::{Case, Casing};
use proc_macro2::{Ident, Span};

/// Extracts the first `Ident` in a type and convert to snake case.
pub fn snakify(ident: &Ident) -> Ident {
    transform_non_keyword_ident(ident, |s: String| s.to_case(Case::Snake))
}

/// Extends an identifier with a suffix.
pub fn concat(ident: &Ident, suffix: &str) -> Ident {
    transform_non_keyword_ident(ident, |s: String| format!("{}{}", s, suffix))
}

/// Applies a string transformation (`transform`) to the input `Ident`.
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
