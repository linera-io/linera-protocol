// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Helper types and functions that aren't specific to WIT or WebAssembly.

use either::Either;
use frunk::{HCons, HNil};

/// Merging of two heterogeneous lists, resulting in a new heterogeneous list where every element is
/// of type `Either<Left, Right>`, where `Left` is an element from the current list and `Right` is
/// an element from the `Other` list.
pub trait Merge<Other>: Sized {
    /// The resulting heterogeneous list with elements of both input lists.
    type Output;
}

impl Merge<HNil> for HNil {
    type Output = HNil;
}

impl<Head, Tail> Merge<HNil> for HCons<Head, Tail>
where
    Tail: Merge<HNil>,
{
    type Output = HCons<Either<Head, ()>, <Tail as Merge<HNil>>::Output>;
}

impl<Head, Tail> Merge<HCons<Head, Tail>> for HNil
where
    HNil: Merge<Tail>,
{
    type Output = HCons<Either<(), Head>, <HNil as Merge<Tail>>::Output>;
}

impl<LeftHead, LeftTail, RightHead, RightTail> Merge<HCons<RightHead, RightTail>>
    for HCons<LeftHead, LeftTail>
where
    LeftTail: Merge<RightTail>,
{
    type Output = HCons<Either<LeftHead, RightHead>, <LeftTail as Merge<RightTail>>::Output>;
}
