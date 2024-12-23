// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for slice types.

use std::borrow::Cow;

use frunk::HList;

use crate::WitType;

impl<T> WitType for [T]
where
    T: WitType,
{
    const SIZE: u32 = 8;

    type Layout = HList![i32, i32];
    type Dependencies = HList![T];

    fn wit_type_name() -> Cow<'static, str> {
        format!("list<{}>", T::wit_type_name()).into()
    }

    fn wit_type_declaration() -> Cow<'static, str> {
        // The native `list` type doesn't need to be declared
        "".into()
    }
}
