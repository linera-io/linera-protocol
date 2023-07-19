// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types declared in this crate.

use crate::{GuestPointer, WitType};
use frunk::HList;

impl WitType for GuestPointer {
    const SIZE: u32 = u32::SIZE;

    type Layout = HList![i32];
}
