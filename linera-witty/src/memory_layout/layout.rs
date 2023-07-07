// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of the memory layout of complex types as a sequence of fundamental WIT types.

use super::element::LayoutElement;
use frunk::{hlist::HList, HCons, HNil};

/// Marker trait to prevent [`LayoutElement`] to be implemented for other types.
pub trait Sealed {}

/// Representation of the memory layout of complex types as a sequence of fundamental WIT types.
pub trait Layout: Sealed + Default + HList {
    /// The alignment boundary required for the layout.
    const ALIGNMENT: u32;
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
}
