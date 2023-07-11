// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for float primitives.

use crate::WitType;
use frunk::HList;

macro_rules! impl_wit_traits {
    ($float:ty, $size:expr) => {
        impl WitType for $float {
            const SIZE: u32 = $size;

            type Layout = HList![$float];
        }
    };
}

impl_wit_traits!(f32, 4);
impl_wit_traits!(f64, 8);
