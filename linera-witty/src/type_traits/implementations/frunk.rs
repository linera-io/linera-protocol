// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Implementations of the custom traits for types from the standard library.

use crate::{Layout, WitType};
use frunk::{HCons, HNil};
use std::ops::Add;

impl WitType for HNil {
    const SIZE: u32 = 0;

    type Layout = HNil;
}

impl<Head, Tail> WitType for HCons<Head, Tail>
where
    Head: WitType,
    Tail: WitType,
    Head::Layout: Add<Tail::Layout>,
    <Head::Layout as Add<Tail::Layout>>::Output: Layout,
{
    const SIZE: u32 = Head::SIZE + Tail::SIZE;

    type Layout = <Head::Layout as Add<Tail::Layout>>::Output;
}
