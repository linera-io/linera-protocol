// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for other standard primitive types.

use crate::WitType;
use frunk::HList;

impl WitType for bool {
    const SIZE: u32 = 1;

    type Layout = HList![i8];
}

impl<'t, T> WitType for &'t T
where
    T: WitType,
{
    const SIZE: u32 = T::SIZE;

    type Layout = T::Layout;
}
