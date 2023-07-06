// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Representation of a single element in a memory layout type.
//!
//! This is analogous to what [`MaybeFlatType`] is to [`crate::primitive_types::FlatType`]. Empty
//! slots (represented by the `()` unit type) make it easier to generate code for zero sized types.

use crate::primitive_types::{MaybeFlatType, SimpleType};

/// Marker trait to prevent [`LayoutElement`] to be implemented for other types.
pub trait Sealed {}

/// Representation of a single element in a memory layout type.
pub trait LayoutElement: Sealed + Default + Sized {
    /// The alignment boundary of the element type.
    const ALIGNMENT: u32;
    /// If the element is a zero sized type.
    const IS_EMPTY: bool;

    /// The flattened representation of this element.
    type Flat: MaybeFlatType;

    /// Converts the element into its flattened representation.
    fn flatten(self) -> Self::Flat;
}

impl Sealed for () {}
impl<T> Sealed for T where T: SimpleType {}

impl LayoutElement for () {
    const ALIGNMENT: u32 = 1;
    const IS_EMPTY: bool = true;

    type Flat = ();

    fn flatten(self) -> Self::Flat {}
}

impl<T> LayoutElement for T
where
    T: SimpleType,
{
    const ALIGNMENT: u32 = <T as SimpleType>::ALIGNMENT;
    const IS_EMPTY: bool = false;

    type Flat = <T as SimpleType>::Flat;

    fn flatten(self) -> Self::Flat {
        <T as SimpleType>::flatten(self)
    }
}
