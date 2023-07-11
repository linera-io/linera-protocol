// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for the [`PhantomData`] type.

use crate::WitType;
use frunk::HList;
use std::marker::PhantomData;

impl<T> WitType for PhantomData<T> {
    const SIZE: u32 = 0;

    type Layout = HList![];
}
