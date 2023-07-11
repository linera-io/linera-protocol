// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for integer primitives.

use crate::WitType;
use frunk::HList;

macro_rules! impl_wit_traits {
    ($integer:ty, 1) => {
        impl WitType for $integer {
            const SIZE: u32 = 1;

            type Layout = HList![$integer];
        }
    };

    ($integer:ty, $size:expr) => {
        impl_wit_traits!($integer, $size, ($integer));
    };

    (
        $integer:ty,
        $size:expr,
        ($( $simple_types:ty ),*)
    ) => {
        impl WitType for $integer {
            const SIZE: u32 = $size;

            type Layout = HList![$( $simple_types ),*];
        }
    };
}

impl_wit_traits!(u8, 1);
impl_wit_traits!(i8, 1);
impl_wit_traits!(u16, 2);
impl_wit_traits!(i16, 2);
impl_wit_traits!(u32, 4);
impl_wit_traits!(i32, 4);
impl_wit_traits!(u64, 8);
impl_wit_traits!(i64, 8);
impl_wit_traits!(u128, 16, (u64, u64));
impl_wit_traits!(i128, 16, (i64, i64));
