// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of the memory layout of complex types as a sequence of fundamental WIT types.

use frunk::{hlist::HList, HCons, HNil};

use super::{element::LayoutElement, FlatLayout};
use crate::primitive_types::MaybeFlatType;

/// Marker trait to prevent [`LayoutElement`] to be implemented for other types.
pub trait Sealed {}

/// Representation of the memory layout of complex types as a sequence of fundamental WIT types.
pub trait Layout: Sealed + HList {
    /// The alignment boundary required for the layout.
    const ALIGNMENT: u32;

    /// Result of flattening this layout.
    type Flat: FlatLayout;

    /// Flattens this layout into a layout consisting of native WebAssembly types.
    ///
    /// The resulting flat layout does not have any empty items.
    fn flatten(self) -> Self::Flat;
}

impl Sealed for HNil {}
impl<Head, Tail> Sealed for HCons<Head, Tail>
where
    Head: LayoutElement,
    Tail: Layout,
{
}

impl Layout for HNil {
    const ALIGNMENT: u32 = 1;

    type Flat = HNil;

    fn flatten(self) -> Self::Flat {
        HNil
    }
}

impl<Head, Tail> Layout for HCons<Head, Tail>
where
    Head: LayoutElement,
    Tail: Layout,
{
    const ALIGNMENT: u32 = if Head::ALIGNMENT > Tail::ALIGNMENT {
        Head::ALIGNMENT
    } else {
        Tail::ALIGNMENT
    };

    type Flat = <Head::Flat as MaybeFlatType>::Flatten<Tail>;

    fn flatten(self) -> Self::Flat {
        self.head.flatten().flatten(self.tail)
    }
}
