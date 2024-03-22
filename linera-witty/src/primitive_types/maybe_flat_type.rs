// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A representation of either a [`FlatType`] or nothing, represented by the unit (`()`) type.

use frunk::HCons;

use super::flat_type::FlatType;
use crate::{memory_layout::FlatLayout, Layout};

/// A marker trait for [`FlatType`]s and the unit type, which uses no storage space.
pub trait MaybeFlatType: Default + Sized {
    /// Result of flattening the layout made up of the current element followed by `Tail`.
    type Flatten<Tail: Layout>: FlatLayout;

    /// Flattens a layout that starts with this [`MaybeFlatType`] followed by a provided `Tail`.
    fn flatten<Tail>(self, tail: Tail) -> Self::Flatten<Tail>
    where
        Tail: Layout;
}

impl MaybeFlatType for () {
    type Flatten<Tail: Layout> = Tail::Flat;

    fn flatten<Tail>(self, tail: Tail) -> Self::Flatten<Tail>
    where
        Tail: Layout,
    {
        tail.flatten()
    }
}

impl<AnyFlatType> MaybeFlatType for AnyFlatType
where
    AnyFlatType: FlatType,
{
    type Flatten<Tail: Layout> = HCons<Self, Tail::Flat>;

    fn flatten<Tail>(self, tail: Tail) -> Self::Flatten<Tail>
    where
        Tail: Layout,
    {
        HCons {
            head: self.flatten(),
            tail: tail.flatten(),
        }
    }
}
