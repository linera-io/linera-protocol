// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`Vec`] type.

use crate::WitType;
use frunk::HList;

impl<T> WitType for Vec<T>
where
    T: WitType,
{
    const SIZE: u32 = 8;

    type Layout = HList![i32, i32];
}
