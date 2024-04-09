// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Trait and helper types allow registering a compile-time list of [`WitType`]s.

use std::collections::BTreeMap;

use frunk::{HCons, HNil};

use super::WitType;

/// Marker trait to prevent [`RegisterWitTypes`] to be implemented for other types.
pub trait Sealed {}

/// Trait to register a compile-time list of [`WitType`]s into a [`BTreeMap`].
pub trait RegisterWitTypes: Sealed {
    /// Registers this list of [`WitType`]s into `wit_types`.
    fn register_wit_types(wit_types: &mut BTreeMap<String, String>);
}

impl Sealed for HNil {}
impl<Head, Tail> Sealed for HCons<Head, Tail>
where
    Head: WitType,
    Tail: Sealed,
{
}

impl RegisterWitTypes for HNil {
    fn register_wit_types(_wit_types: &mut BTreeMap<String, String>) {}
}

impl<Head, Tail> RegisterWitTypes for HCons<Head, Tail>
where
    Head: WitType,
    Tail: RegisterWitTypes,
{
    fn register_wit_types(wit_types: &mut BTreeMap<String, String>) {
        let head_name = Head::wit_type_name();

        if !wit_types.contains_key(&*head_name) {
            wit_types.insert(
                head_name.into_owned(),
                Head::wit_type_declaration().into_owned(),
            );

            Head::Dependencies::register_wit_types(wit_types);
        }

        Tail::register_wit_types(wit_types);
    }
}
