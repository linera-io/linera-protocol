// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Option`] type.

use crate::{Layout, Merge, WitType};
use frunk::{HCons, HNil};

impl<T> WitType for Option<T>
where
    T: WitType,
    HNil: Merge<T::Layout>,
    <HNil as Merge<T::Layout>>::Output: Layout,
{
    const SIZE: u32 = {
        let padding = <T::Layout as Layout>::ALIGNMENT - 1;

        1 + padding + T::SIZE
    };

    type Layout = HCons<i8, <HNil as Merge<T::Layout>>::Output>;
}
